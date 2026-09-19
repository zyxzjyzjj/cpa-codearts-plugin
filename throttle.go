package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
)

// contentProbe matches the shapes that carry model output across the protocols
// this plugin can emit: OpenAI chat chunks (incremental and final), the
// aggregate completion, and Anthropic content block deltas. Fields that the
// upstream sends as JSON null must not count, hence the pointer types.
type contentProbe struct {
	Type    string `json:"type"`
	Choices []struct {
		Delta struct {
			Content          *string         `json:"content"`
			ReasoningContent *string         `json:"reasoning_content"`
			Reasoning        *string         `json:"reasoning"`
			ToolCalls        json.RawMessage `json:"tool_calls"`
			FunctionCall     json.RawMessage `json:"function_call"`
			ReasoningDelta   *string         `json:"reasoning_delta"`
			SignatureDelta   *string         `json:"signature_delta"`
		} `json:"delta"`
		Message struct {
			Content          *string         `json:"content"`
			ReasoningContent *string         `json:"reasoning_content"`
			ToolCalls        json.RawMessage `json:"tool_calls"`
		} `json:"message"`
	} `json:"choices"`
	Delta struct {
		Type        string  `json:"type"`
		Text        *string `json:"text"`
		Thinking    *string `json:"thinking"`
		PartialJSON *string `json:"partial_json"`
	} `json:"delta"`
}

// hasContent reports whether any of those carriers holds a non-empty value.
//
// Tool-call fragments count as content even when their name is still empty,
// because a streamed call is being assembled and retrying would duplicate it.
func (p *contentProbe) hasContent() bool {
	if p.Type == "content_block_delta" {
		if nonEmptyString(p.Delta.Text) || nonEmptyString(p.Delta.Thinking) {
			return true
		}
		// Argument JSON for a tool use block arrives as partial_json.
		if p.Delta.PartialJSON != nil && *p.Delta.PartialJSON != "" {
			return true
		}
	}
	for _, choice := range p.Choices {
		if nonEmptyString(choice.Delta.Content) || nonEmptyString(choice.Delta.ReasoningContent) ||
			nonEmptyString(choice.Delta.Reasoning) || nonEmptyString(choice.Delta.ReasoningDelta) ||
			nonEmptyString(choice.Delta.SignatureDelta) || hasToolPayload(choice.Delta.ToolCalls) ||
			len(choice.Delta.FunctionCall) > 0 {
			return true
		}
		if nonEmptyString(choice.Message.Content) || nonEmptyString(choice.Message.ReasoningContent) ||
			hasToolPayload(choice.Message.ToolCalls) {
			return true
		}
	}
	return false
}

func nonEmptyString(value *string) bool {
	return value != nil && *value != ""
}

// hasToolPayload reports whether a tool_calls array actually lists a call. An
// empty array or an explicit null is not content.
func hasToolPayload(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" || trimmed == "[]" {
		return false
	}
	var calls []json.RawMessage
	if err := json.Unmarshal(raw, &calls); err != nil {
		return true
	}
	return len(calls) > 0
}

// frameHasContent reports whether one SSE frame carries model output. Frames
// before the first content frame are the ones a retry can still replace, so this
// is deliberately cheap and forgiving: an unparsable frame counts as content,
// because withholding it is the worse failure.
func frameHasContent(frame []byte) bool {
	payload := sseDataFrame(frame)
	if len(bytes.TrimSpace(payload)) == 0 {
		// Not a data frame: an event line, comment or heartbeat. Nothing to show
		// the client yet, so it does not end the hold-back window.
		return false
	}
	if bytes.Equal(bytes.TrimSpace(payload), []byte("[DONE]")) {
		return false
	}
	var probe contentProbe
	if err := json.Unmarshal(payload, &probe); err != nil {
		return true
	}
	return probe.hasContent()
}

// aggregatedHasContent reports whether a non-streaming completion payload carries
// model output.
func aggregatedHasContent(payload []byte) bool {
	var probe contentProbe
	if err := json.Unmarshal(payload, &probe); err != nil {
		return true
	}
	return probe.hasContent()
}

// isThrottledStatus reports the status-line form of upstream congestion: a 400
// with no body at all. Other statuses are real answers (auth, quota, rate limit)
// that the host handles with its own cooldown logic, and a 400 carrying a message
// is a genuine request error that retrying cannot fix.
func isThrottledStatus(status int, body []byte) bool {
	return status == http.StatusBadRequest && len(bytes.TrimSpace(body)) == 0
}

// sseDataFrame extracts the JSON payload from one SSE frame, accepting both the
// raw frames the agent mode forwards and the "data:" prefixed lines the upstream
// produces.
func sseDataFrame(frame []byte) []byte {
	var lines []string
	for _, line := range strings.Split(string(frame), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "" || strings.HasPrefix(trimmed, ":"):
			continue
		case strings.HasPrefix(trimmed, "data:"):
			lines = append(lines, strings.TrimSpace(strings.TrimPrefix(trimmed, "data:")))
		case strings.HasPrefix(trimmed, "event:"), strings.HasPrefix(trimmed, "id:"),
			strings.HasPrefix(trimmed, "retry:"):
			continue
		default:
			lines = append(lines, trimmed)
		}
	}
	if len(lines) == 0 {
		return nil
	}
	return []byte(strings.Join(lines, "\n"))
}
