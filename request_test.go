package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestBuildAgentBodyPreservesClientRequest verifies the OpenAI-compatible body
// keeps client fields, forces the upstream model and stream flag, and adds
// stream_options only when streaming.
func TestBuildAgentBodyPreservesClientRequest(t *testing.T) {
	cfg := defaultConfig()
	cfg.APIMode = "agent"
	cfg.ModelMap = map[string]string{"alias-model": "upstream-model"}

	req := executorRequest{}
	req.Model = "alias-model"
	req.Payload = []byte(`{
		"model": "alias-model",
		"messages": [{"role":"user","content":"hi"}],
		"temperature": 0.3
	}`)

	body, errBuild := buildAgentBody(cfg, req, true)
	if errBuild != nil {
		t.Fatalf("buildAgentBody: %v", errBuild)
	}
	var decoded map[string]any
	if errUnmarshal := json.Unmarshal(body, &decoded); errUnmarshal != nil {
		t.Fatalf("body is not valid JSON: %v", errUnmarshal)
	}
	if decoded["model"] != "upstream-model" {
		t.Fatalf("model = %v, want the mapped upstream model", decoded["model"])
	}
	if decoded["stream"] != true {
		t.Fatalf("stream = %v, want true", decoded["stream"])
	}
	if decoded["temperature"] != 0.3 {
		t.Fatalf("temperature = %v, want 0.3 (client field must be preserved)", decoded["temperature"])
	}
	if _, ok := decoded["stream_options"]; !ok {
		t.Fatal("stream_options must be set when streaming")
	}

	body, errBuild = buildAgentBody(cfg, req, false)
	if errBuild != nil {
		t.Fatalf("buildAgentBody non-streaming: %v", errBuild)
	}
	// A fresh map is required: json.Unmarshal merges into a non-nil map, so
	// reusing the previous one would leave stream_options behind.
	decoded = map[string]any{}
	if errUnmarshal := json.Unmarshal(body, &decoded); errUnmarshal != nil {
		t.Fatalf("body is not valid JSON: %v", errUnmarshal)
	}
	if decoded["stream"] != false {
		t.Fatalf("stream = %v, want false", decoded["stream"])
	}
	if _, ok := decoded["stream_options"]; ok {
		t.Fatal("stream_options must not be set when not streaming")
	}
}

func TestBuildAgentBodyRejectsMissingMessages(t *testing.T) {
	cfg := defaultConfig()
	cfg.APIMode = "agent"
	req := executorRequest{}
	req.Payload = []byte(`{"model":"m"}`)
	if _, errBuild := buildAgentBody(cfg, req, true); errBuild == nil {
		t.Fatal("expected an error when the client body has no messages array")
	}
	badReq := executorRequest{}
	badReq.Payload = []byte(`not json`)
	if _, errBuild := buildAgentBody(cfg, badReq, true); errBuild == nil {
		t.Fatal("expected an error for a non-JSON client body")
	}
}

// TestBuildNativeBodyFoldsRolesAndSetsProtocolFields verifies the proprietary
// request shape: typed context blocks, a client-generated chat_id, the
// is_delta_response flag and the configured agent id.
func TestBuildNativeBodyFoldsRolesAndSetsProtocolFields(t *testing.T) {
	cfg := defaultConfig()
	cfg.APIMode = "native"
	cfg.AgentID = "Pangu_Doer_in_CodeArts"

	req := executorRequest{}
	req.Payload = []byte(`{
		"messages": [
			{"role":"system","content":"be terse"},
			{"role":"user","content":"hello"},
			{"role":"assistant","content":"hi"}
		]
	}`)

	body, errBuild := buildNativeBody(cfg, req, true)
	if errBuild != nil {
		t.Fatalf("buildNativeBody: %v", errBuild)
	}
	var decoded struct {
		ChatID          string `json:"chat_id"`
		Client          string `json:"client"`
		ModelID         string `json:"model_id"`
		AgentID         string `json:"agent_id"`
		IsDeltaResponse bool   `json:"is_delta_response"`
		Messages        []struct {
			Type    string `json:"type"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if errUnmarshal := json.Unmarshal(body, &decoded); errUnmarshal != nil {
		t.Fatalf("body is not valid JSON: %v", errUnmarshal)
	}
	if len(decoded.Messages) != 3 {
		t.Fatalf("messages = %d, want 3", len(decoded.Messages))
	}
	for _, message := range decoded.Messages {
		if message.Type != "text" {
			t.Fatalf("message type = %q, want text", message.Type)
		}
	}
	// Role information is folded into the block text so conversational structure
	// survives the flattened protocol.
	if !strings.Contains(decoded.Messages[0].Content, "be terse") ||
		!strings.HasPrefix(decoded.Messages[0].Content, "System instruction:") {
		t.Fatalf("system message not folded correctly: %q", decoded.Messages[0].Content)
	}
	if !strings.HasPrefix(decoded.Messages[2].Content, "Assistant:") {
		t.Fatalf("assistant message not folded correctly: %q", decoded.Messages[2].Content)
	}
	if decoded.Client != "IDE" {
		t.Fatalf("client = %q, want IDE", decoded.Client)
	}
	if decoded.ModelID != cfg.DefaultModelID {
		t.Fatalf("model_id = %q, want %q", decoded.ModelID, cfg.DefaultModelID)
	}
	if decoded.AgentID != "Pangu_Doer_in_CodeArts" {
		t.Fatalf("agent_id = %q, want the configured agent", decoded.AgentID)
	}
	if !decoded.IsDeltaResponse {
		t.Fatal("is_delta_response must be true")
	}
	// The chat id is generated client side and must not contain dashes.
	if decoded.ChatID == "" || strings.Contains(decoded.ChatID, "-") {
		t.Fatalf("chat_id = %q, want a dash-free generated id", decoded.ChatID)
	}
}

// TestBuildNativeBodySkipsEmptyMessages makes sure blank content does not
// produce empty context blocks.
func TestBuildNativeBodySkipsEmptyMessages(t *testing.T) {
	cfg := defaultConfig()
	cfg.APIMode = "native"
	req := executorRequest{}
	req.Payload = []byte(`{"messages":[{"role":"user","content":"  "},{"role":"user","content":"real"}]}`)

	body, errBuild := buildNativeBody(cfg, req, true)
	if errBuild != nil {
		t.Fatalf("buildNativeBody: %v", errBuild)
	}
	var decoded struct {
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
	}
	if errUnmarshal := json.Unmarshal(body, &decoded); errUnmarshal != nil {
		t.Fatalf("body is not valid JSON: %v", errUnmarshal)
	}
	if len(decoded.Messages) != 1 || decoded.Messages[0].Content != "real" {
		t.Fatalf("messages = %+v, want a single non-empty block", decoded.Messages)
	}
}

func TestDecodeMessageContentHandlesBothShapes(t *testing.T) {
	if got := decodeMessageContent(json.RawMessage(`"plain text"`)); got != "plain text" {
		t.Fatalf("string content = %q", got)
	}
	got := decodeMessageContent(json.RawMessage(`[{"type":"text","text":"a"},{"type":"text","text":"b"}]`))
	if got != "a\nb" {
		t.Fatalf("array content = %q, want %q", got, "a\nb")
	}
	if got := decodeMessageContent(nil); got != "" {
		t.Fatalf("nil content = %q, want empty", got)
	}
}

// TestBaseUpstreamHeadersMatchProtocol checks the headers the upstream expects,
// including the ones that participate in the request signature.
func TestBaseUpstreamHeadersMatchProtocol(t *testing.T) {
	cfg := defaultConfig()
	cfg.Language = "zh-cn"
	cfg.PluginName = "snap_vscode"
	cfg.PluginVersion = "26.3.6"
	cfg.ClientVersion = ""
	cfg.Heartbeat = true
	cfg.ExtraHeaders = map[string]string{"x-snap-zone": "green"}

	req := executorRequest{}
	req.Model = "some-model"
	headers := baseUpstreamHeaders(cfg, req)

	want := map[string]string{
		"Content-Type":     "application/json",
		"Accept":           "text/event-stream",
		"client_version":   "Vscode_26.3.6",
		"Agent-Type":       "ChatAgent",
		"X-Language":       "zh-cn",
		"plugin-name":      "snap_vscode",
		"plugin-version":   "26.3.6",
		"heartbeat-enable": "true",
		"x-snap-zone":      "green",
		"x-model-id":       "some-model",
	}
	for key, expected := range want {
		if headers[key] != expected {
			t.Errorf("header %q = %q, want %q", key, headers[key], expected)
		}
	}
	if headers["x-snap-traceid"] == "" {
		t.Error("x-snap-traceid must be set")
	}
	if strings.Contains(headers["x-snap-traceid"], "-") {
		t.Errorf("x-snap-traceid = %q, want a dash-free uuid", headers["x-snap-traceid"])
	}
}

// TestBaseUpstreamHeadersOmitsHeartbeatWhenDisabled covers the config toggle.
func TestBaseUpstreamHeadersOmitsHeartbeatWhenDisabled(t *testing.T) {
	cfg := defaultConfig()
	cfg.Heartbeat = false
	headers := baseUpstreamHeaders(cfg, executorRequest{})
	if _, ok := headers["heartbeat-enable"]; ok {
		t.Fatal("heartbeat-enable must be omitted when heartbeat is disabled")
	}
}

// TestUpstreamModelMapping covers alias resolution and fallback.
func TestUpstreamModelMapping(t *testing.T) {
	cfg := defaultConfig()
	cfg.DefaultModelID = "default-model"
	cfg.ModelMap = map[string]string{"alias": "upstream-id", "blank": "  "}

	if got := cfg.upstreamModel("alias"); got != "upstream-id" {
		t.Errorf("mapped model = %q, want upstream-id", got)
	}
	if got := cfg.upstreamModel("untouched"); got != "untouched" {
		t.Errorf("unmapped model = %q, want it passed through unchanged", got)
	}
	if got := cfg.upstreamModel("blank"); got != "blank" {
		t.Errorf("blank mapping = %q, want the original model", got)
	}
	if got := cfg.upstreamModel(""); got != "default-model" {
		t.Errorf("empty model = %q, want the default", got)
	}
}
