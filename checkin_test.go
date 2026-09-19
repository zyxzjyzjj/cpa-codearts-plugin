package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// TestCheckinTaskRequiresURL documents that a checkin task without a captured
// URL fails with an actionable message instead of calling something arbitrary.
func TestCheckinTaskRequiresURL(t *testing.T) {
	errRun := runCheckinTask(ScheduleTask{ID: "ci", Type: TaskCheckin})
	if errRun == nil {
		t.Fatal("a checkin task without checkin_url must fail")
	}
	if !strings.Contains(errRun.Error(), "checkin_url") {
		t.Fatalf("the error should name the missing field, got %q", errRun.Error())
	}
}

// TestCheckinTaskWithoutCredentialIsRefused verifies the claim is never sent
// unsigned, because an unsigned claim cannot be attributed to the account.
func TestCheckinTaskWithoutCredentialIsRefused(t *testing.T) {
	task := ScheduleTask{
		ID:            "ci",
		Type:          TaskCheckin,
		CheckinURL:    "https://example.invalid/claim",
		CheckinMethod: "POST",
	}
	// Without a host there is no credential inventory, so firstCredential fails.
	errRun := runCheckinTask(task)
	if errRun == nil {
		t.Fatal("a checkin task without a usable credential must fail")
	}
	if !strings.Contains(errRun.Error(), "credential") {
		t.Fatalf("the error should mention the missing credential, got %q", errRun.Error())
	}
}

// TestCheckinConfigDefaults pins the normalisation of the checkin fields so a
// minimal task entry behaves predictably.
func TestCheckinConfigDefaults(t *testing.T) {
	cfg := ScheduleConfig{Tasks: []ScheduleTask{{
		ID:         "daily-claim",
		Type:       TaskCheckin,
		Cron:       "0 9 * * *",
		CheckinURL: "  /v1/benefit/claim  ",
	}}}
	cfg.normalize()
	task := cfg.Tasks[0]
	if task.CheckinMethod != "POST" {
		t.Errorf("checkin_method = %q, want POST by default", task.CheckinMethod)
	}
	if task.CheckinURL != "/v1/benefit/claim" {
		t.Errorf("checkin_url = %q, want it trimmed", task.CheckinURL)
	}
	if task.Type != TaskCheckin {
		t.Errorf("type = %q, want checkin", task.Type)
	}
	if errCron := validateCron(task.Cron); errCron != nil {
		t.Errorf("cron invalid: %v", errCron)
	}
}

// TestCheckinTaskFromYAML exercises the real decode path for a fully specified
// checkin task.
func TestCheckinTaskFromYAML(t *testing.T) {
	yamlDoc := `
enabled: true
base_url: "https://snap-access.cn-north-4.myhuaweicloud.com"
schedule:
  enabled: true
  tasks:
    - id: "daily-checkin"
      type: "checkin"
      cron: "0 9 * * *"
      checkin_method: "POST"
      checkin_url: "/v1/activity/benefit/daily/claim"
      checkin_body: '{"activityCode":"daily_sign_in"}'
      checkin_headers:
        x-snap-traceid: "fixed"
      checkin_success_marker: '"code":"0"'
      checkin_already_marker: "already"
`
	request, _ := json.Marshal(map[string]any{
		"config_yaml": encodeBase64([]byte(yamlDoc)),
	})
	cfg, errParse := parseConfig(request)
	if errParse != nil {
		t.Fatalf("parseConfig: %v", errParse)
	}
	tasks := cfg.scheduleTasks()
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(tasks))
	}
	task := tasks[0]
	if task.Type != TaskCheckin {
		t.Fatalf("type = %q, want checkin", task.Type)
	}
	if task.CheckinURL != "/v1/activity/benefit/daily/claim" {
		t.Errorf("checkin_url = %q", task.CheckinURL)
	}
	if task.CheckinBody != `{"activityCode":"daily_sign_in"}` {
		t.Errorf("checkin_body = %q", task.CheckinBody)
	}
	if task.CheckinHeaders["x-snap-traceid"] != "fixed" {
		t.Errorf("checkin_headers = %v", task.CheckinHeaders)
	}
	if task.CheckinSuccessMarker != `"code":"0"` {
		t.Errorf("checkin_success_marker = %q", task.CheckinSuccessMarker)
	}
	if task.CheckinAlreadyMarker != "already" {
		t.Errorf("checkin_already_marker = %q", task.CheckinAlreadyMarker)
	}
}

// TestRecordTaskResultKeepsRicherOutcome ensures a task-specific result is not
// overwritten by the generic "ok" placeholder.
func TestRecordTaskResultKeepsRicherOutcome(t *testing.T) {
	const id = "test-record-result"
	recordTaskResult(id, "already claimed today")
	scheduler.record(id, nowTime(), nil)

	states := scheduler.snapshot()
	state, ok := states[id]
	if !ok {
		t.Fatalf("task %q was not recorded", id)
	}
	if state.LastResult != "already claimed today" {
		t.Fatalf("last_result = %q, want the specific outcome preserved", state.LastResult)
	}
	if state.LastError != "" {
		t.Fatalf("last_error = %q, want empty for a success", state.LastError)
	}
}

// TestBenefitsRouteExplainsUnconfiguredState verifies the operator gets
// actionable guidance instead of a bare failure when no checkin task exists.
func TestBenefitsAndCheckinExplainUnconfiguredState(t *testing.T) {
	cfg := defaultConfig()
	cfg.Schedule = ScheduleConfig{Enabled: true, DisableDefaults: true, Tasks: nil}
	currentConfig.Store(cfg)
	defer currentConfig.Store(defaultConfig())

	resp := handleBenefits()
	if resp.StatusCode != 200 {
		t.Fatalf("benefits status = %d, want 200", resp.StatusCode)
	}
	var described map[string]any
	if errUnmarshal := json.Unmarshal(resp.Body, &described); errUnmarshal != nil {
		t.Fatalf("benefits body is not JSON: %v", errUnmarshal)
	}
	if described["configured"] != false {
		t.Errorf("configured = %v, want false", described["configured"])
	}
	if described["explanation"] == nil {
		t.Error("the explanation should always be present")
	}
	if len(described["how_to_capture"].([]any)) == 0 {
		t.Error("capture instructions should be present when nothing is configured")
	}

	checkin := handleCheckin(pluginapiManagementRequest(`{}`))
	var out map[string]any
	if errUnmarshal := json.Unmarshal(checkin.Body, &out); errUnmarshal != nil {
		t.Fatalf("checkin body is not JSON: %v", errUnmarshal)
	}
	if out["configured"] != false {
		t.Errorf("checkin configured = %v, want false", out["configured"])
	}
	steps, _ := out["how_to_enable"].([]any)
	if len(steps) == 0 {
		t.Error("checkin should explain how to enable the task")
	}
}

// nowTime is a tiny indirection so the test does not need the time package for
// a single call site.
func nowTime() time.Time { return time.Now() }

// pluginapiManagementRequest builds a ManagementRequest with a JSON body.
func pluginapiManagementRequest(body string) pluginapi.ManagementRequest {
	return pluginapi.ManagementRequest{Method: "POST", Path: "/v0/management/codearts-provider/checkin", Body: []byte(body)}
}

// checkinAccessKeys returns the Access= field of each recorded signature.
func checkinAccessKeys(t *testing.T, bodies []string) []string {
	t.Helper()
	var keys []string
	for _, raw := range bodies {
		match := regexp.MustCompile(`Access=([^,]+)`).FindStringSubmatch(raw)
		if match == nil {
			t.Fatalf("no access key in signature header: %q", raw)
		}
		keys = append(keys, match[1])
	}
	return keys
}

// checkinStub installs a host that holds two CodeArts credentials and answers the
// claim endpoint. claimFor decides the response per access key, and the returned
// slice records which key signed each claim in call order.
func checkinStub(t *testing.T, claimFor func(accessKey string) hostHTTPResponse) (func(), *[]string) {
	t.Helper()
	storages := map[string]string{
		"idx-a": `{"codearts_provider_credential":{"access_key_id":"ak-a","secret_access_key":"sk-a"}}`,
		"idx-b": `{"codearts_provider_credential":{"access_key_id":"ak-b","secret_access_key":"sk-b"}}`,
	}
	keys := &[]string{}
	restore := setHostCall(func(method string, request any) (json.RawMessage, error) {
		switch method {
		case "host.auth.list":
			return json.Marshal(map[string]any{"files": []pluginapi.HostAuthFileEntry{
				{AuthIndex: "idx-a", Name: "a.json", Provider: providerID, Label: "alpha"},
				{AuthIndex: "idx-b", Name: "b.json", Provider: providerID, Label: "beta"},
			}})
		case "host.auth.get":
			index := request.(map[string]any)["auth_index"].(string)
			return json.Marshal(map[string]any{"json": json.RawMessage(storages[index])})
		case "host.http.do":
			headers := request.(map[string]any)["headers"].(map[string][]string)
			match := regexp.MustCompile(`Access=([^,]+)`).FindStringSubmatch(headers["Authorization"][0])
			if match == nil {
				t.Fatalf("claim request carried no access key: %q", headers["Authorization"][0])
			}
			*keys = append(*keys, match[1])
			return json.Marshal(claimFor(match[1]))
		default:
			return nil, fmt.Errorf("unexpected callback %s", method)
		}
	})
	return restore, keys
}

func claimSucceeded(string) hostHTTPResponse {
	return hostHTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"error_msg":"success"}`)}
}

func TestCheckinClaimsEveryAccountWhenAsked(t *testing.T) {
	restore, keys := checkinStub(t, claimSucceeded)
	defer restore()

	task := ScheduleTask{ID: "claim-all", Type: TaskCheckin, CheckinURL: "https://benefit.test/claim",
		CheckinMethod: "POST", CheckinSuccessMarker: `"error_msg":"success"`, CheckinAllAccounts: true}
	if err := runCheckinTask(task); err != nil {
		t.Fatal(err)
	}
	if len(*keys) != 2 || (*keys)[0] != "ak-a" || (*keys)[1] != "ak-b" {
		t.Fatalf("claims signed by %v, want one per stored credential", *keys)
	}
	if result := scheduler.snapshot()["claim-all"].LastResult; !strings.Contains(result, "2 claimed") {
		t.Fatalf("task result lost the per-account summary: %q", result)
	}
}

func TestCheckinDefaultsToSingleCredential(t *testing.T) {
	restore, keys := checkinStub(t, claimSucceeded)
	defer restore()

	task := ScheduleTask{ID: "claim-one", Type: TaskCheckin, CheckinURL: "https://benefit.test/claim", CheckinMethod: "POST"}
	if err := runCheckinTask(task); err != nil {
		t.Fatal(err)
	}
	if len(*keys) != 1 {
		t.Fatalf("a check-in without checkin_all_accounts claimed %d times, want 1", len(*keys))
	}
}

func TestCheckinTargetsOneAccount(t *testing.T) {
	restore, keys := checkinStub(t, claimSucceeded)
	defer restore()

	// A label is accepted as well as an auth index, because the panel shows labels.
	task := ScheduleTask{ID: "claim-pick", Type: TaskCheckin, CheckinURL: "https://benefit.test/claim",
		CheckinMethod: "POST", CheckinAuthIndex: "beta"}
	if err := runCheckinTask(task); err != nil {
		t.Fatal(err)
	}
	if len(*keys) != 1 || (*keys)[0] != "ak-b" {
		t.Fatalf("claims signed by %v, want only ak-b", *keys)
	}
}

func TestCheckinReportsPartialFailureWithoutHidingSuccess(t *testing.T) {
	restore, keys := checkinStub(t, func(accessKey string) hostHTTPResponse {
		if accessKey == "ak-b" {
			// A 200 without the success marker is a rejected claim, not a silent win.
			return hostHTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"error_msg":"not eligible"}`)}
		}
		return claimSucceeded(accessKey)
	})
	defer restore()

	task := ScheduleTask{ID: "claim-partial", Type: TaskCheckin, CheckinURL: "https://benefit.test/claim",
		CheckinMethod: "POST", CheckinSuccessMarker: `"error_msg":"success"`, CheckinAllAccounts: true}
	if err := runCheckinTask(task); err != nil {
		t.Fatalf("a partial success must not fail the whole task: %v", err)
	}
	if len(*keys) != 2 {
		t.Fatalf("the second account was skipped after the first answered: %v", *keys)
	}
	result := scheduler.snapshot()["claim-partial"].LastResult
	if !strings.Contains(result, "1 claimed") || !strings.Contains(result, "1 failed") || !strings.Contains(result, "beta") {
		t.Fatalf("partial outcome not reported per account: %q", result)
	}
}

func TestCheckinUnknownAuthIndexIsRefused(t *testing.T) {
	restore, keys := checkinStub(t, claimSucceeded)
	defer restore()

	task := ScheduleTask{ID: "claim-missing", Type: TaskCheckin, CheckinURL: "https://benefit.test/claim",
		CheckinMethod: "POST", CheckinAuthIndex: "does-not-exist"}
	err := runCheckinTask(task)
	if err == nil || !strings.Contains(err.Error(), "checkin_auth_index") {
		t.Fatalf("want a refusal naming checkin_auth_index, got %v", err)
	}
	if len(*keys) != 0 {
		t.Fatalf("an unmatched auth index still claimed %d times", len(*keys))
	}
}
