package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFrameHasContent(t *testing.T) {
	cases := []struct {
		name  string
		frame string
		want  bool
	}{
		{"content delta", `{"choices":[{"index":0,"delta":{"content":"hi"}}]}`, true},
		{"reasoning delta", `{"choices":[{"index":0,"delta":{"reasoning_content":"thinking"}}]}`, true},
		{"tool call delta", `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"x"}}]}}]}`, true},
		{"empty tool calls", `{"choices":[{"index":0,"delta":{"tool_calls":[]}}]}`, false},
		{"null content", `{"choices":[{"index":0,"delta":{"content":null}}]}`, false},
		{"finish only", `{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`, false},
		{"aggregate message", `{"choices":[{"message":{"role":"assistant","content":"hello"}}]}`, true},
		{"anthropic text delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}`, true},
		{"anthropic thinking delta", `{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"hmm"}}`, true},
		{"anthropic message start", `{"type":"message_start","message":{"id":"m1","content":[]}}`, false},
		{"done frame", `data: [DONE]`, false},
		{"heartbeat", `:heartbeat`, false},
		{"data prefixed json", "data: " + `{"choices":[{"delta":{"content":"x"}}]}` + "\n\n", true},
		{"unparsable", `not json at all`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := frameHasContent([]byte(tc.frame)); got != tc.want {
				t.Fatalf("frameHasContent(%q) = %v, want %v", tc.frame, got, tc.want)
			}
		})
	}
}

func TestIsThrottledStatus(t *testing.T) {
	if !isThrottledStatus(http.StatusBadRequest, nil) {
		t.Fatal("400 with no body should be retryable")
	}
	if isThrottledStatus(http.StatusBadRequest, []byte(`{"error":"bad param"}`)) {
		t.Fatal("400 carrying a message is a real request error")
	}
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests} {
		if isThrottledStatus(status, nil) {
			t.Fatalf("HTTP %d belongs to the host's cooldown logic", status)
		}
	}
}

// sseStreamScript replays a fixed body per upstream stream, then reports done.
type sseStreamScript struct {
	id     string
	chunks []string
}

func TestStreamReplacesEmptyAttempt(t *testing.T) {
	scripts := []sseStreamScript{
		{id: "upstream-empty", chunks: []string{
			"data: " + `{"id":"empty-attempt","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n",
			"data: [DONE]\n\n",
		}},
		{id: "upstream-good", chunks: []string{
			"data: " + `{"id":"real-attempt","choices":[{"index":0,"delta":{"content":"hello"}}]}` + "\n\n",
			"data: [DONE]\n\n",
		}},
	}
	var opens atomic.Int32
	var closes, streamCloses atomic.Int32
	var emitted []string
	done := make(chan struct{})
	var emittedOnce atomic.Int32

	testHost(t, func(method string, request any) (json.RawMessage, error) {
		switch method {
		case "host.http.do_stream":
			index := int(opens.Add(1)) - 1
			return json.Marshal(hostHTTPStreamOpen{StatusCode: http.StatusOK, StreamID: scripts[index].id})
		case "host.http.stream_read":
			id := request.(map[string]any)["stream_id"].(string)
			for _, script := range scripts {
				if script.id != id {
					continue
				}
				body := strings.Join(script.chunks, "")
				return json.Marshal(hostHTTPStreamChunk{Payload: []byte(body), Done: true})
			}
			return json.Marshal(hostHTTPStreamChunk{Error: "unknown stream " + id})
		case "host.http.stream_close":
			closes.Add(1)
			return json.RawMessage(`{}`), nil
		case "host.stream.emit":
			payload := request.(map[string]any)["payload"].([]byte)
			emitted = append(emitted, string(payload))
			if emittedOnce.Add(1) == 1 {
				close(done)
			}
			return json.RawMessage(`{}`), nil
		case "host.stream.close":
			streamCloses.Add(1)
			return json.RawMessage(`{}`), nil
		default:
			t.Fatalf("unexpected callback %s", method)
			return nil, nil
		}
	})

	raw, err := executorExecuteStream(mustMarshal(t, streamRequest()))
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil || !env.OK {
		t.Fatalf("stream was not accepted: %s", raw)
	}
	waitForStreamClose(t, &streamCloses)

	if got := opens.Load(); got != 2 {
		t.Fatalf("upstream opened %d times, want 2 (one retry)", got)
	}
	joined := strings.Join(emitted, "")
	if !strings.Contains(joined, "hello") {
		t.Fatalf("retry content never reached the client: %v", emitted)
	}
	if strings.Contains(joined, "empty-attempt") {
		t.Fatalf("the replaced attempt leaked frames to the client: %v", emitted)
	}
	if got := closes.Load(); got != 2 {
		t.Fatalf("upstream streams closed %d times, want 2", got)
	}
	if got := streamCloses.Load(); got != 1 {
		t.Fatalf("client stream closed %d times, want exactly 1", got)
	}
}

func TestStreamExhaustedStillCompletesProtocol(t *testing.T) {
	empty := "data: " + `{"id":"last-attempt","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n"
	var opens, streamCloses atomic.Int32
	var emitted []string
	testHost(t, func(method string, request any) (json.RawMessage, error) {
		switch method {
		case "host.http.do_stream":
			opens.Add(1)
			return json.Marshal(hostHTTPStreamOpen{StatusCode: http.StatusOK, StreamID: "upstream"})
		case "host.http.stream_read":
			return json.Marshal(hostHTTPStreamChunk{Payload: []byte(empty), Done: true})
		case "host.http.stream_close":
			return json.RawMessage(`{}`), nil
		case "host.stream.emit":
			emitted = append(emitted, string(request.(map[string]any)["payload"].([]byte)))
			return json.RawMessage(`{}`), nil
		case "host.stream.close":
			streamCloses.Add(1)
			return json.RawMessage(`{}`), nil
		default:
			t.Fatalf("unexpected callback %s", method)
			return nil, nil
		}
	})

	if _, err := executorExecuteStream(mustMarshal(t, streamRequest())); err != nil {
		t.Fatal(err)
	}
	waitForStreamClose(t, &streamCloses)

	if got := opens.Load(); got != maxThrottleAttempts {
		t.Fatalf("upstream opened %d times, want the retry cap %d", got, maxThrottleAttempts)
	}
	joined := strings.Join(emitted, "")
	if !strings.Contains(joined, "last-attempt") {
		t.Fatalf("the final attempt's held frames were dropped: %v", emitted)
	}
	// CPA writes the terminating [DONE] itself, so the plugin's last attempt only
	// has to hand over what it held back and close the client stream once.
	if got := streamCloses.Load(); got != 1 {
		t.Fatalf("client stream closed %d times, want exactly 1", got)
	}
}

func TestExecuteRetriesEmptyAggregate(t *testing.T) {
	bodies := []string{
		"data: " + `{"id":"a1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n",
		"data: " + `{"id":"a2","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n",
	}
	var calls atomic.Int32
	testHost(t, func(method string, request any) (json.RawMessage, error) {
		switch method {
		case "host.http.do_stream":
			index := int(calls.Add(1)) - 1
			return json.Marshal(hostHTTPStreamOpen{StatusCode: http.StatusOK, StreamID: fmt.Sprintf("upstream-%d", index)})
		case "host.http.stream_read":
			index := strings.TrimPrefix(request.(map[string]any)["stream_id"].(string), "upstream-")
			return json.Marshal(hostHTTPStreamChunk{Payload: []byte(bodies[mustAtoi(t, index)]), Done: true})
		case "host.http.stream_close":
			return json.RawMessage(`{}`), nil
		default:
			t.Fatalf("unexpected callback %s", method)
			return nil, nil
		}
	})

	raw, err := executorExecute(mustMarshal(t, streamRequest()))
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil || !env.OK {
		t.Fatalf("expected success after one retry, got %s", raw)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("upstream called %d times, want 2", got)
	}
	if payload := decodedPayload(t, env.Result); !strings.Contains(payload, "answer") {
		t.Fatalf("retry payload lost: %s", payload)
	}
}

func TestExecuteRetriesSilentBadRequest(t *testing.T) {
	var calls, closes atomic.Int32
	testHost(t, func(method string, request any) (json.RawMessage, error) {
		switch method {
		case "host.http.do_stream":
			if calls.Add(1) == 1 {
				return json.Marshal(hostHTTPStreamOpen{StatusCode: http.StatusBadRequest, StreamID: "upstream"})
			}
			return json.Marshal(hostHTTPStreamOpen{StatusCode: http.StatusOK, StreamID: "upstream"})
		case "host.http.stream_read":
			if calls.Load() == 1 {
				return json.Marshal(hostHTTPStreamChunk{Done: true})
			}
			return json.Marshal(hostHTTPStreamChunk{Payload: []byte("data: " +
				`{"choices":[{"index":0,"delta":{"content":"recovered"}}]}` + "\n\ndata: [DONE]\n\n"), Done: true})
		case "host.http.stream_close":
			closes.Add(1)
			return json.RawMessage(`{}`), nil
		default:
			t.Fatalf("unexpected callback %s", method)
			return nil, nil
		}
	})

	raw, err := executorExecute(mustMarshal(t, streamRequest()))
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil || !env.OK {
		t.Fatalf("expected success after retrying the silent 400, got %s", raw)
	}
	if payload := decodedPayload(t, env.Result); !strings.Contains(payload, "recovered") {
		t.Fatalf("a silent 400 was not retried: %s", payload)
	}
	if got := closes.Load(); got < 2 {
		t.Fatalf("upstream streams closed %d times, want one per attempt", got)
	}
}

func TestExecuteDoesNotRetryExplainedError(t *testing.T) {
	var calls atomic.Int32
	testHost(t, func(method string, request any) (json.RawMessage, error) {
		switch method {
		case "host.http.do_stream":
			calls.Add(1)
			return json.Marshal(hostHTTPStreamOpen{StatusCode: http.StatusBadRequest, StreamID: "upstream"})
		case "host.http.stream_read":
			return json.Marshal(hostHTTPStreamChunk{Payload: []byte(`{"error_msg":"the param is invalid"}`), Done: true})
		case "host.http.stream_close":
			return json.RawMessage(`{}`), nil
		default:
			t.Fatalf("unexpected callback %s", method)
			return nil, nil
		}
	})

	raw, err := executorExecute(mustMarshal(t, streamRequest()))
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil || env.OK {
		t.Fatalf("expected an error envelope, got %s", raw)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("an explained 400 was retried %d times, want 1", got)
	}
	if !strings.Contains(string(raw), "the param is invalid") {
		t.Fatalf("the upstream error body was dropped instead of surfaced: %s", raw)
	}
}

func TestAggregateAndPayloadProbes(t *testing.T) {
	if aggregatedHasContent([]byte(`{"choices":[{"message":{"content":""}}]}`)) {
		t.Fatal("an empty completion must read as no content")
	}
	if !aggregatedHasContent([]byte(`{"choices":[{"message":{"reasoning_content":"step 1"}}]}`)) {
		t.Fatal("reasoning output is content")
	}
	if !aggregatedHasContent([]byte(`{"choices":[{"message":{"tool_calls":[{"id":"c"}]}}]}`)) {
		t.Fatal("a tool call is content")
	}
	if aggregatedHasContent([]byte(`{`)) == false {
		t.Fatal("an unparsable payload must not be swallowed as empty")
	}
}

// decodedPayload unwraps the base64 completion the plugin hands to the host.
func decodedPayload(t *testing.T, result json.RawMessage) string {
	t.Helper()
	var response struct {
		Payload []byte `json:"Payload"`
	}
	if err := json.Unmarshal(result, &response); err != nil {
		t.Fatalf("decode executor response: %v", err)
	}
	return string(response.Payload)
}

func mustAtoi(t *testing.T, raw string) int {
	t.Helper()
	value, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("bad stream id %q: %v", raw, err)
	}
	return value
}

func streamRequest() executorRequest {
	req := executorRequest{StreamID: "client"}
	req.Model = "model"
	req.Payload = []byte(`{"messages":[{"role":"user","content":"hi"}]}`)
	req.StorageJSON = []byte(`{"access_key_id":"fake","secret_access_key":"fake","security_token":"fake"}`)
	return req
}

func waitForStreamClose(t *testing.T, counter *atomic.Int32) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && counter.Load() < 1 {
		time.Sleep(20 * time.Millisecond)
	}
	if counter.Load() < 1 {
		t.Fatal("the client stream was never closed")
	}
}
