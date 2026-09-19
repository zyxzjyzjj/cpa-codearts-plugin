package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// This file implements the plugin's scheduled-task facility.
//
// The CLIProxyAPI plugin ABI has no cron capability of its own: the host never
// calls a plugin on a timer. A plugin that needs periodic work must schedule it
// itself, which is what the reference workbuddy plugin does as well. Here the
// schedule is driven by real cron expressions so operators can express anything
// from "every 10 minutes" to "09:00 on weekdays" without a plugin release.

// TaskType classifies a scheduled task.
type TaskType string

const (
	// TaskTokenRenew renews the temporary AK/SK on every stored credential.
	// The upstream credential lasts 24h and the official extension renews
	// hourly, so this is the built-in default.
	TaskTokenRenew TaskType = "token_renew"
	// TaskQuotaRefresh fetches and caches the subscription/quota snapshot for
	// every account, so the panel and quota API answer from cache.
	TaskQuotaRefresh TaskType = "quota_refresh"
	// TaskHTTP performs a configurable request against the CodeArts gateway.
	// It exists so a deployment can schedule an endpoint this plugin does not
	// model natively yet, without waiting for a plugin change.
	TaskHTTP TaskType = "http"
	// TaskCheckin claims the daily benefit.
	//
	// The CodeArts Doer extension implements no check-in call of its own: the
	// activity/benefit page (the "Wish Wall", which the extension opens from its
	// server-provided wishWallUrl) is a server-hosted web app, and the claim
	// happens inside that page against an endpoint that never appears in the
	// extension bundle. This task therefore drives a URL you supply, captured
	// once from that page, and then scheduled here.
	TaskCheckin TaskType = "checkin"
)

// taskState is the last observed outcome of one task, guarded by the scheduler
// mutex so the management API can report it.
type taskState struct {
	LastRunAt  time.Time
	LastResult string
	LastError  string
	RunCount   int64
	ErrorCount int64
	Running    bool
}

// Scheduler owns the cron runner and the task registry.
type Scheduler struct {
	mu      sync.Mutex
	runner  *cron.Cron
	entries map[string]cron.EntryID
	states  map[string]*taskState
	// running tracks per-task execution so a slow task cannot overlap itself.
	running map[string]bool
	started bool
	// location is the timezone cron expressions are evaluated in.
	location *time.Location
}

var scheduler = &Scheduler{
	states:  map[string]*taskState{},
	running: map[string]bool{},
}

// startScheduler (re)builds the cron runner from configuration. It is safe to
// call repeatedly: an existing runner is stopped first, which is how
// plugin.reconfigure applies schedule changes without a restart.
func startScheduler(cfg *Config) {
	if scheduler == nil {
		return
	}
	scheduler.stop()

	tasks := cfg.scheduleTasks()
	if !cfg.Schedule.Enabled || len(tasks) == 0 {
		if cfg.Schedule.Enabled {
			logInfo("scheduler enabled but no tasks are configured", nil)
		}
		return
	}

	location := time.Local
	if name := strings.TrimSpace(cfg.Schedule.Timezone); name != "" {
		if parsed, errLoad := time.LoadLocation(name); errLoad == nil {
			location = parsed
		} else {
			logWarn("unknown schedule timezone, falling back to the host local zone", map[string]any{
				"timezone": name,
				"error":    errLoad.Error(),
			})
		}
	}

	// cronSpec normalises every expression to the 6-field seconds-first form, so
	// the runner must use the matching parser rather than the default 5-field one.
	runner := cron.New(cron.WithLocation(location), cron.WithParser(newCronParser()))
	scheduler.mu.Lock()
	scheduler.runner = runner
	scheduler.entries = map[string]cron.EntryID{}
	scheduler.location = location
	for _, task := range tasks {
		if !task.isEnabled() {
			continue
		}
		task := task
		spec, errSpec := cronSpec(task.Cron)
		if errSpec != nil {
			logWarn("skipping scheduled task with an invalid cron expression", map[string]any{
				"task":  task.ID,
				"cron":  task.Cron,
				"error": errSpec.Error(),
			})
			continue
		}
		entryID, errAdd := runner.AddFunc(spec, func() {
			runTask(task)
		})
		if errAdd != nil {
			logWarn("skipping scheduled task that cron rejected", map[string]any{
				"task":  task.ID,
				"cron":  task.Cron,
				"error": errAdd.Error(),
			})
			continue
		}
		scheduler.entries[task.ID] = entryID
		if _, ok := scheduler.states[task.ID]; !ok {
			scheduler.states[task.ID] = &taskState{}
		}
	}
	scheduler.started = true
	scheduler.mu.Unlock()

	runner.Start()

	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if task.isEnabled() {
			ids = append(ids, task.ID)
		}
	}
	logInfo("scheduler started", map[string]any{
		"tasks":    strings.Join(ids, ","),
		"timezone": location.String(),
	})
}

// stop halts the cron runner. It is invoked on reconfigure and on quiesce.
func (s *Scheduler) stop() {
	if s == nil {
		return
	}
	s.mu.Lock()
	runner := s.runner
	s.runner = nil
	s.entries = map[string]cron.EntryID{}
	s.started = false
	s.mu.Unlock()
	if runner != nil {
		// Stop returns a context that fires once running jobs finish; waiting on
		// it here would block the host's reconfigure call, so the runner is
		// simply stopped and in-flight jobs are left to complete on their own.
		runner.Stop()
	}
}

// nextRuns reports the upcoming fire times per configured task.
func (s *Scheduler) nextRuns() map[string]string {
	s.mu.Lock()
	runner := s.runner
	entries := make(map[string]cron.EntryID, len(s.entries))
	for id, entry := range s.entries {
		entries[id] = entry
	}
	s.mu.Unlock()

	out := map[string]string{}
	if runner == nil {
		return out
	}
	for id, entry := range entries {
		out[id] = runner.Entry(entry).Next.Format(time.RFC3339)
	}
	return out
}

// snapshot returns the per-task state for the management API.
func (s *Scheduler) snapshot() map[string]taskState {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]taskState, len(s.states))
	for id, state := range s.states {
		out[id] = *state
	}
	return out
}

// record stores the outcome of one task run.
func (s *Scheduler) record(taskID string, started time.Time, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.states[taskID]
	if state == nil {
		state = &taskState{}
		s.states[taskID] = state
	}
	state.LastRunAt = started
	state.RunCount++
	if err != nil {
		state.LastError = err.Error()
		state.ErrorCount++
		state.LastResult = ""
	} else {
		state.LastError = ""
		// A task may have recorded a richer outcome (for example "already
		// claimed today"); only fill in a placeholder when it did not.
		if state.LastResult == "" {
			state.LastResult = "ok"
		}
	}
}

// setRunning marks a task as in flight so the management API can show it and
// concurrent ticks are suppressed.
func (s *Scheduler) setRunning(taskID string, running bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if running && s.running[taskID] {
		return false
	}
	s.running[taskID] = running
	return true
}

// runTask executes one task, guarding against overlap and recording the result.
func runTask(task ScheduleTask) {
	if !scheduler.setRunning(task.ID, true) {
		logWarn("scheduled task is still running, skipping this tick", map[string]any{"task": task.ID})
		return
	}
	defer scheduler.setRunning(task.ID, false)

	started := time.Now()
	errRun := executeTask(task)
	scheduler.record(task.ID, started, errRun)

	fields := map[string]any{
		"task":       task.ID,
		"type":       string(task.Type),
		"duration_s": time.Since(started).Seconds(),
	}
	if errRun != nil {
		fields["error"] = errRun.Error()
		logWarn("scheduled task failed", fields)
		return
	}
	logInfo("scheduled task completed", fields)
}

// executeTask dispatches one task by type.
func executeTask(task ScheduleTask) error {
	switch task.Type {
	case TaskTokenRenew:
		return runTokenRenewTask()
	case TaskQuotaRefresh:
		return runQuotaRefreshTask()
	case TaskHTTP:
		return runHTTPTask(task)
	case TaskCheckin:
		return runCheckinTask(task)
	default:
		return fmt.Errorf("unknown task type %q", task.Type)
	}
}

// runTokenRenewTask renews every stored credential.
func runTokenRenewTask() error {
	files, errList := hostAuthList()
	if errList != nil {
		return fmt.Errorf("list auth files: %w", errList)
	}
	renewed, failed := 0, 0
	for _, file := range files {
		if normalizeProvider(file.Provider) != providerID && normalizeProvider(file.Type) != providerID {
			continue
		}
		storage, errGet := hostAuthGet(file.AuthIndex)
		if errGet != nil {
			failed++
			continue
		}
		cred, errCred := credentialFromStorage(storage)
		if errCred != nil || !cred.valid() {
			failed++
			continue
		}
		// A credential without a security token is a permanent key pair; there
		// is nothing to renew.
		if strings.TrimSpace(cred.SecurityToken) == "" {
			continue
		}
		if errRenew := renewCredential(cred); errRenew != nil {
			failed++
			logWarn("scheduled token renewal failed", map[string]any{
				"account": file.AuthIndex,
				"error":   errRenew.Error(),
			})
			continue
		}
		renewed++
	}
	if failed > 0 && renewed == 0 {
		return fmt.Errorf("renewed 0 credentials, %d failed", failed)
	}
	return nil
}

// runQuotaRefreshTask refreshes the cached quota snapshot for every account.
func runQuotaRefreshTask() error {
	files, errList := hostAuthList()
	if errList != nil {
		return fmt.Errorf("list auth files: %w", errList)
	}
	refreshed := 0
	var lastErr error
	for _, file := range files {
		if normalizeProvider(file.Provider) != providerID && normalizeProvider(file.Type) != providerID {
			continue
		}
		storage, errGet := hostAuthGet(file.AuthIndex)
		if errGet != nil {
			lastErr = errGet
			continue
		}
		cred, _ := credentialFromStorage(storage)
		if !cred.valid() {
			continue
		}
		if _, errFetch := fetchQuotaSnapshot(file.AuthIndex, cred); errFetch != nil {
			lastErr = errFetch
			continue
		}
		refreshed++
	}
	if refreshed == 0 && lastErr != nil {
		return fmt.Errorf("refreshed 0 accounts: %w", lastErr)
	}
	return nil
}

// runHTTPTask performs a configured request against the CodeArts gateway. It is
// the escape hatch for scheduling an endpoint the plugin does not model.
//
// When Sign is true the request is signed with the first available credential,
// which is what an authenticated upstream call requires. Response bodies are
// never logged verbatim because they may carry account data.
func runHTTPTask(task ScheduleTask) error {
	cfg := config()
	method := strings.ToUpper(strings.TrimSpace(task.Method))
	if method == "" {
		method = http.MethodGet
	}
	target := strings.TrimSpace(task.URL)
	if target == "" {
		if !strings.HasPrefix(task.Path, "/") {
			task.Path = "/" + task.Path
		}
		target = strings.TrimRight(cfg.BaseURL, "/") + task.Path
	}
	if target == "" {
		return fmt.Errorf("task has neither url nor path")
	}

	var body []byte
	if strings.TrimSpace(task.Body) != "" {
		body = []byte(task.Body)
	}

	headers := map[string]string{
		"Accept":       "application/json",
		"Content-Type": "application/json",
		"X-Language":   cfg.Language,
		"plugin-name":  cfg.PluginName,
	}
	if !cfg.IsConfidential {
		headers["is_confidential"] = "false"
	}
	for key, value := range task.Headers {
		headers[key] = value
	}

	if task.Sign {
		cred, errCred := firstCredential()
		if errCred != nil {
			return fmt.Errorf("no credential available to sign the task request: %w", errCred)
		}
		signed, errSign := signRequest(method, target, headers, body, cred, cfg.SignHost)
		if errSign != nil {
			return fmt.Errorf("sign task request: %w", errSign)
		}
		headers = signed
	} else if cfg.PluginVersion != "" {
		headers["plugin-version"] = cfg.PluginVersion
	}

	response, errDo := hostHTTPDo(method, target, headers, body)
	if errDo != nil {
		return fmt.Errorf("task request failed: %w", errDo)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("task request returned HTTP %d: %s", response.StatusCode, truncate(string(response.Body), 300))
	}

	scheduler.mu.Lock()
	state := scheduler.states[task.ID]
	if state != nil {
		state.LastResult = fmt.Sprintf("HTTP %d, %d bytes", response.StatusCode, len(response.Body))
	}
	scheduler.mu.Unlock()
	return nil
}

// firstCredential returns the first usable stored credential, used by scheduled
// tasks that need to sign a request but are not bound to a specific account.
func firstCredential() (*credential, error) {
	files, errList := hostAuthList()
	if errList != nil {
		return nil, errList
	}
	for _, file := range files {
		if normalizeProvider(file.Provider) != providerID && normalizeProvider(file.Type) != providerID {
			continue
		}
		storage, errGet := hostAuthGet(file.AuthIndex)
		if errGet != nil {
			continue
		}
		cred, errCred := credentialFromStorage(storage)
		if errCred == nil && cred.valid() {
			return cred, nil
		}
	}
	return nil, fmt.Errorf("no usable %s credential is configured", providerID)
}

// cronSpec normalises a cron expression for robfig/cron.
//
// Both the standard 5-field form ("minute hour dom month dow") and the 6-field
// form with a leading seconds field are accepted, so operators can paste either
// style. The parser used later is derived from the field count.
func cronSpec(expression string) (string, error) {
	trimmed := strings.TrimSpace(expression)
	if trimmed == "" {
		return "", fmt.Errorf("cron expression is empty")
	}
	switch len(strings.Fields(trimmed)) {
	case 5:
		// Standard cron has no seconds field; add one so the shared parser works.
		return "0 " + trimmed, nil
	case 6:
		return trimmed, nil
	default:
		return "", fmt.Errorf("cron expression must have 5 or 6 fields, got %d", len(strings.Fields(trimmed)))
	}
}

// newCronParser builds the parser used for validation and by the runner. It
// accepts the 6-field (seconds-first) form produced by cronSpec.
func newCronParser() cron.Parser {
	return cron.NewParser(
		cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)
}

// validateCron reports whether an expression is usable, for config validation.
func validateCron(expression string) error {
	spec, errSpec := cronSpec(expression)
	if errSpec != nil {
		return errSpec
	}
	_, errParse := newCronParser().Parse(spec)
	return errParse
}

// describeTasks renders the configured tasks for the management API.
func describeTasks(cfg *Config) []map[string]any {
	nextRuns := scheduler.nextRuns()
	states := scheduler.snapshot()
	tasks := cfg.scheduleTasks()

	out := make([]map[string]any, 0, len(tasks))
	for _, task := range tasks {
		entry := map[string]any{
			"id":      task.ID,
			"type":    string(task.Type),
			"cron":    task.Cron,
			"enabled": task.isEnabled(),
		}
		if task.Path != "" {
			entry["path"] = task.Path
		}
		if task.Method != "" {
			entry["method"] = task.Method
		}
		if task.Sign {
			entry["sign"] = true
		}
		if next, ok := nextRuns[task.ID]; ok {
			entry["next_run"] = next
		}
		if errCron := validateCron(task.Cron); errCron != nil {
			entry["cron_error"] = errCron.Error()
		}
		if state, ok := states[task.ID]; ok {
			if !state.LastRunAt.IsZero() {
				entry["last_run"] = state.LastRunAt.Format(time.RFC3339)
			}
			if state.LastResult != "" {
				entry["last_result"] = state.LastResult
			}
			if state.LastError != "" {
				entry["last_error"] = state.LastError
			}
			entry["run_count"] = state.RunCount
			entry["error_count"] = state.ErrorCount
		}
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool {
		return fmt.Sprint(out[i]["id"]) < fmt.Sprint(out[j]["id"])
	})
	return out
}

// triggerTask runs one configured task immediately, out of band from cron. It is
// used by the management API so an operator can verify a task without waiting.
func triggerTask(taskID string) error {
	cfg := config()
	for _, task := range cfg.scheduleTasks() {
		if task.ID == taskID {
			go runTask(task)
			return nil
		}
	}
	return fmt.Errorf("unknown task %q", taskID)
}

// ---------------------------------------------------------------------------
// host auth callbacks used by the scheduler
// ---------------------------------------------------------------------------

// hostAuthList returns the host's credential inventory.
func hostAuthList() ([]pluginapi.HostAuthFileEntry, error) {
	result, errCall := hostCall("host.auth.list", map[string]any{})
	if errCall != nil {
		return nil, errCall
	}
	var payload struct {
		Files []pluginapi.HostAuthFileEntry `json:"files"`
	}
	if errUnmarshal := json.Unmarshal(result, &payload); errUnmarshal != nil {
		return nil, fmt.Errorf("decode host auth list: %w", errUnmarshal)
	}
	return payload.Files, nil
}

// hostAuthGet reads one credential's physical JSON payload by auth index.
func hostAuthGet(authIndex string) ([]byte, error) {
	result, errCall := hostCall("host.auth.get", map[string]any{"auth_index": authIndex})
	if errCall != nil {
		return nil, errCall
	}
	var payload struct {
		AuthIndex string          `json:"auth_index"`
		Name      string          `json:"name"`
		Path      string          `json:"path"`
		JSON      json.RawMessage `json:"json"`
	}
	if errUnmarshal := json.Unmarshal(result, &payload); errUnmarshal != nil {
		return nil, fmt.Errorf("decode host auth get: %w", errUnmarshal)
	}
	return payload.JSON, nil
}

// hostAuthSave writes a credential back through the host so the runtime record
// is refreshed as well.
func hostAuthSave(name string, payload []byte) error {
	_, errCall := hostCall("host.auth.save", map[string]any{
		"name": name,
		"json": json.RawMessage(payload),
	})
	return errCall
}

// renewCredential performs the token/renew exchange for one credential and
// persists the refreshed payload.
func renewCredential(cred *credential) error {
	cfg := config()
	if strings.TrimSpace(cred.RefreshToken) != "" || cred.OAuthContext != nil {
		updated, _, _, errRefresh := oauthRefreshCredential(cfg, cred)
		if errRefresh != nil {
			return errRefresh
		}
		return persistRenewedCredential(cred, updated)
	}
	renewBody, errMarshal := json.Marshal(map[string]any{
		"access":           cred.AccessKeyID,
		"securitytoken":    cred.SecurityToken,
		"duration_seconds": 24 * 60 * 60,
	})
	if errMarshal != nil {
		return errMarshal
	}
	endpoint := strings.TrimRight(cfg.BaseURL, "/") + "/snap-manager/v1/token/renew"
	headers, errSign := signRequest(http.MethodPost, endpoint, map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
	}, renewBody, cred, cfg.SignHost)
	if errSign != nil {
		return fmt.Errorf("sign renew request: %w", errSign)
	}
	response, errDo := hostHTTPDo(http.MethodPost, endpoint, headers, renewBody)
	if errDo != nil {
		return errDo
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("token renew returned HTTP %d", response.StatusCode)
	}
	updated, errDecode := decodeCredentialResponse(response.Body)
	if errDecode != nil {
		return errDecode
	}
	updated.DomainID = firstNonEmptyString(updated.DomainID, cred.DomainID)
	updated.UserName = firstNonEmptyString(updated.UserName, cred.UserName)
	updated.UserID = firstNonEmptyString(updated.UserID, cred.UserID)
	updated.LoginType = firstNonEmptyString(updated.LoginType, cred.LoginType, "WEB")
	return persistRenewedCredential(cred, &updated)
}

func persistRenewedCredential(previous, updated *credential) error {
	if previous == nil || updated == nil || !updated.valid() {
		return fmt.Errorf("refreshed credential is incomplete")
	}
	storage, errMarshal2 := json.Marshal(map[string]any{storageKey: *updated})
	if errMarshal2 != nil {
		return errMarshal2
	}
	// Persist under the same auth file name the host uses for this credential.
	files, errList := hostAuthList()
	if errList != nil {
		return errList
	}
	for _, file := range files {
		if normalizeProvider(file.Provider) != providerID && normalizeProvider(file.Type) != providerID {
			continue
		}
		storageExisting, errGet := hostAuthGet(file.AuthIndex)
		if errGet != nil {
			continue
		}
		existing, _ := credentialFromStorage(storageExisting)
		if existing == nil || existing.AccessKeyID != previous.AccessKeyID {
			continue
		}
		wrap, errWrap := mergeCredentialFile(storageExisting, storage)
		if errWrap != nil {
			return errWrap
		}
		if file.Name == "" {
			return fmt.Errorf("credential %s has no file name to save to", file.AuthIndex)
		}
		return hostAuthSave(file.Name, wrap)
	}
	return nil
}

// mergeCredentialFile rewrites the plugin-owned credential field inside an auth
// file payload while preserving every host-owned key (priority, note, weight,
// disable flags, ...) that the host may have set.
func mergeCredentialFile(existing []byte, storage []byte) ([]byte, error) {
	doc := map[string]any{}
	if len(existing) > 0 {
		if errUnmarshal := json.Unmarshal(existing, &doc); errUnmarshal != nil {
			return nil, fmt.Errorf("existing auth file is not a JSON object: %w", errUnmarshal)
		}
	}
	var credWrapper map[string]any
	if errUnmarshal := json.Unmarshal(storage, &credWrapper); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	for key, value := range credWrapper {
		doc[key] = value
	}
	doc["type"] = providerID
	return json.MarshalIndent(doc, "", "  ")
}

// shutdownScheduler is called on plugin.quiesce, which the host invokes while
// its own runtime is still healthy.
func shutdownScheduler() {
	scheduler.stop()
	_ = context.Background()
}

// ---------------------------------------------------------------------------
// check-in task
// ---------------------------------------------------------------------------

// runCheckinTask claims the daily benefit.
//
// The extension itself never performs this call: it opens a server-hosted
// activity page. The claim endpoint is therefore configuration, captured once
// from that page, and this task schedules it. Because the upstream contract is
// not published, the result is judged by explicit markers rather than by the
// HTTP status alone, and an "already claimed" answer is reported as success so a
// twice-a-day schedule does not look like a failure.
func runCheckinTask(task ScheduleTask) error {
	cfg := config()
	target := strings.TrimSpace(task.CheckinURL)
	if target == "" {
		return fmt.Errorf("checkin task %q has no checkin_url; capture it from the activity page and set it in config", task.ID)
	}
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		// A relative path is resolved against the configured gateway so the
		// common case stays short in config.
		path := target
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		target = strings.TrimRight(cfg.BaseURL, "/") + path
	}

	method := strings.ToUpper(strings.TrimSpace(task.CheckinMethod))
	if method == "" {
		method = http.MethodPost
	}

	var body []byte
	if strings.TrimSpace(task.CheckinBody) != "" {
		body = []byte(task.CheckinBody)
	}

	headers := map[string]string{
		"Accept":       "application/json",
		"Content-Type": "application/json",
		"X-Language":   cfg.Language,
		"plugin-name":  cfg.PluginName,
	}
	if cfg.PluginVersion != "" {
		headers["plugin-version"] = cfg.PluginVersion
	}
	for key, value := range task.CheckinHeaders {
		if trimmed := strings.TrimSpace(key); trimmed != "" {
			headers[trimmed] = value
		}
	}

	// The claim is authenticated exactly like every other upstream call.
	accounts, errSelect := checkinCredentials(task)
	if errSelect != nil {
		return errSelect
	}

	claimed, already, failures := 0, 0, make([]string, 0, len(accounts))
	for _, account := range accounts {
		outcome, errClaim := claimForCredential(task, cfg, method, target, headers, body, account.credential)
		if errClaim != nil {
			failures = append(failures, account.label+": "+errClaim.Error())
			logWarn("check-in failed for account", map[string]any{
				"task": task.ID, "account": account.label, "error": errClaim.Error(),
			})
			continue
		}
		if outcome == "already" {
			already++
		} else {
			claimed++
		}
	}

	switch {
	case claimed+already == 0:
		recordTaskResult(task.ID, fmt.Sprintf("0 of %d account(s) claimed", len(accounts)))
		return fmt.Errorf("check-in failed for every account: %s", strings.Join(failures, "; "))
	case len(failures) > 0:
		// Some accounts claimed and some did not. Report both: failing the task
		// outright would hide the claims that did land, and staying silent would
		// hide the account that needs attention.
		recordTaskResult(task.ID, fmt.Sprintf("%d claimed, %d already, %d failed: %s",
			claimed, already, len(failures), strings.Join(failures, "; ")))
	default:
		recordTaskResult(task.ID, fmt.Sprintf("%d claimed, %d already (%d account(s))", claimed, already, len(accounts)))
	}
	return nil
}

// checkinAccount pairs a credential with the label used in logs and task results.
type checkinAccount struct {
	label      string
	credential *credential
}

// checkinCredentials chooses which accounts the claim runs for. Without an
// explicit choice it keeps the historical one-credential behaviour: a claim
// endpoint is not necessarily per account, and repeating an unknown request for
// every stored credential is exactly the traffic pattern upstream throttles.
func checkinCredentials(task ScheduleTask) ([]checkinAccount, error) {
	files, errList := hostAuthList()
	if errList != nil {
		// Phrased around the credential on purpose: an unreadable inventory means
		// the claim cannot be attributed to any account, which is what an operator
		// needs to hear.
		return nil, fmt.Errorf("no usable %s credential could be resolved: %w", providerID, errList)
	}
	wanted := strings.TrimSpace(task.CheckinAuthIndex)
	selected := make([]checkinAccount, 0, len(files))
	for _, file := range files {
		if normalizeProvider(file.Provider) != providerID && normalizeProvider(file.Type) != providerID {
			continue
		}
		if wanted != "" && !strings.EqualFold(file.AuthIndex, wanted) &&
			!strings.EqualFold(file.Name, wanted) && !strings.EqualFold(file.Label, wanted) {
			continue
		}
		storage, errGet := hostAuthGet(file.AuthIndex)
		if errGet != nil {
			continue
		}
		cred, errCred := credentialFromStorage(storage)
		if errCred != nil || !cred.valid() {
			continue
		}
		selected = append(selected, checkinAccount{
			label:      firstNonEmptyString(file.Label, file.Name, file.AuthIndex),
			credential: cred,
		})
	}
	if len(selected) == 0 {
		if wanted != "" {
			return nil, fmt.Errorf("no usable %s credential matches checkin_auth_index %q", providerID, wanted)
		}
		return nil, fmt.Errorf("no usable %s credential is configured", providerID)
	}
	if wanted == "" && !task.CheckinAllAccounts {
		return selected[:1], nil
	}
	return selected, nil
}

// claimForCredential performs one claim with one credential and judges the result
// from the configured markers rather than the status code alone. It returns the
// outcome label: "claimed" or "already".
func claimForCredential(task ScheduleTask, cfg *Config, method, target string, headers map[string]string, body []byte, cred *credential) (string, error) {
	signed, errSign := signRequest(method, target, headers, body, cred, cfg.SignHost)
	if errSign != nil {
		return "", fmt.Errorf("sign check-in request: %w", errSign)
	}

	response, errDo := hostHTTPDo(method, target, signed, body)
	if errDo != nil {
		return "", fmt.Errorf("check-in request failed: %w", errDo)
	}

	text := string(response.Body)
	switch {
	case response.StatusCode < 200 || response.StatusCode >= 300:
		return "", fmt.Errorf("returned HTTP %d: %s", response.StatusCode, truncate(text, 300))
	case task.CheckinAlreadyMarker != "" && strings.Contains(text, task.CheckinAlreadyMarker):
		// Already claimed today: a successful, idempotent outcome.
		logInfo("check-in already claimed", map[string]any{"task": task.ID})
		return "already", nil
	case task.CheckinSuccessMarker != "" && !strings.Contains(text, task.CheckinSuccessMarker):
		// A 200 without the success marker means the claim did not happen, for
		// example a rejected or expired request. Reporting success here would
		// hide a real problem.
		return "", fmt.Errorf("returned HTTP %d but the response did not contain the success marker %q: %s",
			response.StatusCode, task.CheckinSuccessMarker, truncate(text, 300))
	default:
		logInfo("check-in completed", map[string]any{
			"task":   task.ID,
			"status": response.StatusCode,
		})
		return "claimed", nil
	}
}

// recordTaskResult stores a human-readable outcome for the management API.
func recordTaskResult(taskID, result string) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	state := scheduler.states[taskID]
	if state == nil {
		state = &taskState{}
		scheduler.states[taskID] = state
	}
	state.LastResult = result
}
