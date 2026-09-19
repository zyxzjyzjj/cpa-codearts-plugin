package main

import (
	"encoding/json"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the plugin-owned configuration delivered by the host as the
// `plugins.configs.<pluginID>` YAML subtree. The host only parses `enabled` and
// `priority` itself and forwards everything else verbatim.
type Config struct {
	// BaseURL is the CodeArts Doer regional API endpoint. Both the native chat
	// API and the OpenAI-compatible agent API are served from this host.
	BaseURL string `yaml:"base_url" json:"base_url"`
	// BenefitBaseURL is the open gateway that serves the 限时福利 (free tier)
	// catalogue and its daily claim. Its models are absent from every agent
	// catalogue and need the maas_type: benefit header on chat requests.
	BenefitBaseURL string `yaml:"benefit_base_url" json:"benefit_base_url"`
	// WebLoginBase is the web console used to start the browser login flow.
	WebLoginBase string `yaml:"web_login_base" json:"web_login_base"`
	// OAuthTokenURL and OAuthIdentityURL are the Huawei STS endpoints used by
	// the 26.9.x PKCE/DPoP login. Overrides are primarily for private clouds and
	// integration tests; public Huawei Cloud uses the defaults below.
	OAuthTokenURL    string `yaml:"oauth_token_url" json:"oauth_token_url"`
	OAuthIdentityURL string `yaml:"oauth_identity_url" json:"oauth_identity_url"`
	// APIMode selects the upstream protocol: "agent" uses the OpenAI-compatible
	// /api/v2/chat/completions endpoint, "native" uses the proprietary
	// /v1/chat/chat endpoint with its own SSE framing.
	APIMode string `yaml:"api_mode" json:"api_mode"`
	// PluginName is sent as the `plugin-name` header so the upstream can apply
	// the same policy bundle as the official IDE plugin.
	PluginName string `yaml:"plugin_name" json:"plugin_name"`
	// PluginVersion is sent as `plugin-version` and `client_version`.
	PluginVersion string `yaml:"plugin_version" json:"plugin_version"`
	// Language is sent as the X-Language header ("zh-cn" or "en-us").
	Language string `yaml:"language" json:"language"`
	// ClientVersion overrides the `client_version` header. When empty it is
	// derived from PluginVersion.
	ClientVersion string `yaml:"client_version" json:"client_version"`
	// IDEBaseURL is the web console used to open the browser login flow when
	// WebLoginBase is not set.
	IDEBaseURL string `yaml:"ide_base_url" json:"ide_base_url"`
	// AgentID is the CodeArts agent UUID or alias used by the native protocol.
	AgentID string `yaml:"agent_id" json:"agent_id"`
	// DefaultModelID is the upstream model_id sent when the requested model has
	// no explicit upstream mapping.
	DefaultModelID string `yaml:"default_model_id" json:"default_model_id"`
	// ModelMap maps client-facing model IDs to upstream model IDs.
	ModelMap map[string]string `yaml:"model_map" json:"model_map"`
	// Models is the static model list advertised to CLIProxyAPI.
	Models []ModelConfig `yaml:"models" json:"models"`
	// DiscoverModels queries the signed Agent Center detail endpoints per account.
	DiscoverModels bool     `yaml:"discover_models" json:"discover_models"`
	ModelAgentIDs  []string `yaml:"model_agent_ids" json:"model_agent_ids"`
	// RequestTimeoutSeconds bounds a single upstream request.
	RequestTimeoutSeconds int `yaml:"request_timeout_seconds" json:"request_timeout_seconds"`
	// LoginTimeoutSeconds bounds the interactive browser login flow.
	LoginTimeoutSeconds int `yaml:"login_timeout_seconds" json:"login_timeout_seconds"`
	// LoginCallbackBind is the local interface the short-lived OAuth callback
	// listener binds to. Keep the default 127.0.0.1 for remote/container CPA:
	// the operator relays the final localhost URL through the management panel.
	LoginCallbackBind string `yaml:"login_callback_bind" json:"login_callback_bind"`
	// LoginCallbackPort pins the callback port instead of using an ephemeral one.
	// A fixed port is only needed for an advanced directly reachable callback.
	LoginCallbackPort int `yaml:"login_callback_port" json:"login_callback_port"`
	// LoginCallbackBase replaces the callback URL host:port handed to the browser,
	// for example "http://192.168.1.10:40605". Leave empty for the recommended
	// localhost + manual relay flow; no Docker/public port mapping is required.
	LoginCallbackBase string `yaml:"login_callback_base" json:"login_callback_base"`
	// Heartbeat enables the upstream SSE heartbeat comment frames.
	Heartbeat bool `yaml:"heartbeat" json:"heartbeat"`
	// SignHost adds the `host` header to the signed header set. The official
	// extension does not sign `host`, so the default is false; enable it for
	// deployments whose gateway requires host to be covered by the signature.
	SignHost bool `yaml:"sign_host" json:"sign_host"`
	// InsistMissingCredentials lets the executor proceed with an unsigned
	// request when no credential is available. Useful only for debugging.
	InsistMissingCredentials bool `yaml:"insist_missing_credentials" json:"insist_missing_credentials"`
	// IsConfidential sets the `is_confidential` header used for KIA mode.
	IsConfidential bool `yaml:"is_confidential" json:"is_confidential"`
	// ExtraHeaders are added to every upstream request and are included in the
	// request signature.
	ExtraHeaders map[string]string `yaml:"extra_headers" json:"extra_headers"`

	// Schedule configures the plugin's own periodic tasks. The CLIProxyAPI
	// plugin ABI exposes no cron facility, so a plugin that needs periodic work
	// schedules it internally; this block drives that scheduler.
	Schedule ScheduleConfig `yaml:"schedule" json:"schedule"`

	// Scheduler configures credential selection for this provider.
	Scheduler SchedulerConfig `yaml:"scheduler" json:"scheduler"`
}

// SchedulerConfig controls how the plugin picks among credential candidates.
//
// CodeArts Doer applies per-account rate limits and publishes no per-credential
// quota, so spreading requests across accounts is the useful policy.
type SchedulerConfig struct {
	// Strategy is round-robin (default), preferred or host.
	//   round-robin - rotate across candidates so requests spread out.
	//   preferred   - always use PreferredAuthID when it is a candidate.
	//   host        - never pick; let the host's built-in scheduler decide.
	Strategy string `yaml:"strategy" json:"strategy"`
	// PreferredAuthID is the auth index or ID to prefer under the preferred
	// strategy, for example "codearts-provider-me.json".
	PreferredAuthID string `yaml:"preferred_auth_id" json:"preferred_auth_id"`
	// Delegate names a built-in strategy to fall back to when Strategy is host:
	// "fill-first", "round-robin", or empty for the host default.
	Delegate string `yaml:"delegate" json:"delegate"`
}

// ScheduleConfig controls the built-in cron scheduler.
type ScheduleConfig struct {
	// Enabled starts the scheduler. When false no task runs automatically.
	Enabled bool `yaml:"enabled" json:"enabled"`
	// Timezone is the IANA zone cron expressions are evaluated in. Empty means
	// the host process local zone.
	Timezone string `yaml:"timezone" json:"timezone"`
	// Tasks is the task list. When it is empty but Enabled is true, the default
	// task set below is used.
	Tasks []ScheduleTask `yaml:"tasks" json:"tasks"`
	// DisableDefaults suppresses the built-in default tasks when Tasks is empty.
	DisableDefaults bool `yaml:"disable_defaults" json:"disable_defaults"`
}

// ScheduleTask is one periodic job.
type ScheduleTask struct {
	// ID is the stable task identifier used by the management API.
	ID string `yaml:"id" json:"id"`
	// Type selects the task implementation: token_renew, quota_refresh, http or checkin.
	Type TaskType `yaml:"type" json:"type"`
	// Cron is a 5-field (minute-first) or 6-field (second-first) cron expression.
	Cron string `yaml:"cron" json:"cron"`
	// Enabled turns this single task on or off. Defaults to on when omitted.
	Enabled *bool `yaml:"enabled" json:"enabled"`

	// Method, URL and Path are used by the http task type. Path is resolved
	// against base_url.
	Method string `yaml:"method" json:"method"`
	URL    string `yaml:"url" json:"url"`
	Path   string `yaml:"path" json:"path"`
	// Body is the optional request body for the http task type.
	Body string `yaml:"body" json:"body"`
	// Headers are extra request headers for the http task type.
	Headers map[string]string `yaml:"headers" json:"headers"`
	// Sign signs the http task request with the first available credential.
	Sign bool `yaml:"sign" json:"sign"`

	// --- checkin task type ---

	// CheckinMethod is the HTTP method used to claim the daily benefit.
	CheckinMethod string `yaml:"checkin_method" json:"checkin_method"`
	// CheckinURL is the absolute claim endpoint captured from the activity page.
	CheckinURL string `yaml:"checkin_url" json:"checkin_url"`
	// CheckinBody is the request body sent when claiming.
	CheckinBody string `yaml:"checkin_body" json:"checkin_body"`
	// CheckinHeaders are extra headers for the claim request.
	CheckinHeaders map[string]string `yaml:"checkin_headers" json:"checkin_headers"`
	// CheckinSuccessMarker is a substring that must appear in a successful
	// response. It exists to distinguish a real claim from a no-op, so the task
	// reports an accurate outcome instead of assuming success from HTTP 200.
	CheckinSuccessMarker string `yaml:"checkin_success_marker" json:"checkin_success_marker"`
	// CheckinAlreadyMarker is a substring indicating the benefit was already
	// claimed today. Matching it counts as success rather than a failure.
	CheckinAlreadyMarker string `yaml:"checkin_already_marker" json:"checkin_already_marker"`
	// CheckinAllAccounts runs the claim once per stored credential. The benefit
	// allowance is granted per Huawei Cloud account, so a deployment with several
	// accounts only collects one account's worth without this.
	CheckinAllAccounts bool `yaml:"checkin_all_accounts" json:"checkin_all_accounts"`
	// CheckinAuthIndex restricts the claim to one credential, matched against the
	// auth index, file name or label. Empty means "not restricted".
	CheckinAuthIndex string `yaml:"checkin_auth_index" json:"checkin_auth_index"`
}

// isEnabled reports whether the task is on. An omitted flag means enabled, so a
// minimal task entry does what the operator expects.
func (t ScheduleTask) isEnabled() bool {
	return t.Enabled == nil || *t.Enabled
}

// defaultScheduleTasks is the built-in schedule used when tasks are not
// specified. Token renewal mirrors the official extension's hourly renewal
// cadence; the quota refresh keeps the subscription view warm for the panel and
// the quota API.
func defaultScheduleTasks() []ScheduleTask {
	on := true
	return []ScheduleTask{
		{
			ID:      "token-renew",
			Type:    TaskTokenRenew,
			Cron:    "17 * * * *",
			Enabled: &on,
		},
		{
			ID:      "quota-refresh",
			Type:    TaskQuotaRefresh,
			Cron:    "*/30 * * * *",
			Enabled: &on,
		},
	}
}

// ModelConfig describes one advertised model.
type ModelConfig struct {
	ID              string `yaml:"id" json:"id"`
	Name            string `yaml:"name" json:"name"`
	DisplayName     string `yaml:"display_name" json:"display_name"`
	Description     string `yaml:"description" json:"description"`
	ContextLength   int64  `yaml:"context_length" json:"context_length"`
	MaxOutputTokens int64  `yaml:"max_output_tokens" json:"max_output_tokens"`
	SupportsImages  bool   `yaml:"supports_images" json:"supports_images"`
	// Benefit marks a 限时福利 model: it is absent from the agent catalogue and
	// the chat endpoint answers "The model is not registered" unless the request
	// carries a signed maas_type: benefit header.
	Benefit bool `yaml:"benefit" json:"benefit"`
}

// defaultConfig returns the built-in defaults. The upstream model list is
// tenant-specific and fetched dynamically by the official IDE plugin, so these
// defaults are a starting point that users are expected to adjust.
func defaultConfig() *Config {
	return &Config{
		BaseURL:               "https://snap-access.cn-north-4.myhuaweicloud.com",
		BenefitBaseURL:        "https://opengw.developer.huaweicloud.com",
		WebLoginBase:          "https://codearts.huaweicloud.com",
		OAuthTokenURL:         codeArtsOAuthTokenURL,
		OAuthIdentityURL:      codeArtsOAuthIdentityURL,
		APIMode:               "agent",
		PluginName:            "snap_vscode",
		PluginVersion:         "26.9.101",
		Language:              "en-us",
		AgentID:               "Pangu_Doer_in_CodeArts",
		DefaultModelID:        "GLM-5.2",
		RequestTimeoutSeconds: 600,
		LoginTimeoutSeconds:   300,
		Heartbeat:             true,
		Models:                defaultModels(),
		DiscoverModels:        true,
	}
}

func defaultModels() []ModelConfig {
	return []ModelConfig{
		{
			ID:              "GLM-5.2",
			Name:            "GLM-5.2",
			DisplayName:     "GLM-5.2 (CodeArts)",
			Description:     "Huawei CodeArts Doer chat model served through the CodeArts Doer gateway.",
			ContextLength:   202752,
			MaxOutputTokens: 131072,
		},
	}
}

// parseConfig decodes the lifecycle request and applies it over the defaults.
func parseConfig(request []byte) (*Config, error) {
	cfg := defaultConfig()
	if len(request) == 0 {
		return cfg, nil
	}
	// config_yaml is a []byte on the host side, so the JSON wire form is a
	// base64 string. Declaring the field as []byte makes encoding/json decode it
	// for us, matching the host's own rpcLifecycleRequest.
	var payload struct {
		ConfigYAML []byte `json:"config_yaml"`
	}
	if errUnmarshal := json.Unmarshal(request, &payload); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	raw := payload.ConfigYAML
	if len(strings.TrimSpace(string(raw))) == 0 {
		return cfg, nil
	}
	if errUnmarshal := yaml.Unmarshal(raw, cfg); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	cfg.normalize()
	return cfg, nil
}

// normalize fills in derived values and trims user input.
func (c *Config) normalize() {
	if c == nil {
		return
	}
	c.BaseURL = strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	c.WebLoginBase = strings.TrimRight(strings.TrimSpace(c.WebLoginBase), "/")
	c.OAuthTokenURL = strings.TrimSpace(c.OAuthTokenURL)
	c.OAuthIdentityURL = strings.TrimSpace(c.OAuthIdentityURL)
	c.PluginName = strings.TrimSpace(c.PluginName)
	c.PluginVersion = strings.TrimSpace(c.PluginVersion)
	c.ClientVersion = strings.TrimSpace(c.ClientVersion)
	c.Language = strings.TrimSpace(c.Language)
	c.AgentID = strings.TrimSpace(c.AgentID)
	c.DefaultModelID = strings.TrimSpace(c.DefaultModelID)
	c.IDEBaseURL = strings.TrimRight(strings.TrimSpace(c.IDEBaseURL), "/")

	switch strings.ToLower(c.APIMode) {
	case "native":
		c.APIMode = "native"
	default:
		c.APIMode = "agent"
	}
	if c.WebLoginBase == "" {
		c.WebLoginBase = c.IDEBaseURL
	}
	if c.OAuthTokenURL == "" {
		c.OAuthTokenURL = codeArtsOAuthTokenURL
	}
	if c.OAuthIdentityURL == "" {
		c.OAuthIdentityURL = codeArtsOAuthIdentityURL
	}
	if c.ClientVersion == "" && c.PluginVersion != "" {
		c.ClientVersion = "Vscode_" + c.PluginVersion
	}
	if c.Language == "" {
		c.Language = "en-us"
	}
	if c.RequestTimeoutSeconds <= 0 {
		c.RequestTimeoutSeconds = 600
	}
	if c.LoginTimeoutSeconds <= 0 {
		c.LoginTimeoutSeconds = 300
	}
	c.LoginCallbackBind = strings.TrimSpace(c.LoginCallbackBind)
	if c.LoginCallbackBind == "" {
		c.LoginCallbackBind = "127.0.0.1"
	}
	if c.LoginCallbackPort < 0 || c.LoginCallbackPort > 65535 {
		c.LoginCallbackPort = 0
	}
	c.LoginCallbackBase = strings.TrimRight(strings.TrimSpace(c.LoginCallbackBase), "/")
	if len(c.Models) == 0 {
		c.Models = defaultModels()
	}
	for index := range c.Models {
		model := &c.Models[index]
		model.ID = strings.TrimSpace(model.ID)
		model.Name = strings.TrimSpace(model.Name)
		if model.Name == "" {
			model.Name = model.ID
		}
		model.DisplayName = strings.TrimSpace(model.DisplayName)
		if model.DisplayName == "" {
			model.DisplayName = model.ID
		}
		if model.ContextLength <= 0 {
			model.ContextLength = 128000
		}
		if model.MaxOutputTokens <= 0 {
			model.MaxOutputTokens = 8192
		}
	}
	if c.DefaultModelID == "" {
		c.DefaultModelID = c.Models[0].ID
	}
	c.Schedule.normalize()
	c.Scheduler.normalize()
}

// normalize cleans the scheduler configuration.
func (s *SchedulerConfig) normalize() {
	s.Strategy = strings.ToLower(strings.TrimSpace(s.Strategy))
	s.PreferredAuthID = strings.TrimSpace(s.PreferredAuthID)
	s.Delegate = strings.ToLower(strings.TrimSpace(s.Delegate))
	switch s.Delegate {
	case "", "fill-first", "round-robin":
	default:
		// An unknown delegate would make the host ignore the pick; dropping it
		// falls back to the host default instead.
		s.Delegate = ""
	}
}

// normalize fills schedule defaults and validates cron expressions.
func (s *ScheduleConfig) normalize() {
	s.Timezone = strings.TrimSpace(s.Timezone)
	if len(s.Tasks) == 0 {
		if s.DisableDefaults {
			return
		}
		s.Tasks = defaultScheduleTasks()
	}
	for index := range s.Tasks {
		task := &s.Tasks[index]
		task.ID = strings.TrimSpace(task.ID)
		task.Type = TaskType(strings.ToLower(strings.TrimSpace(string(task.Type))))
		task.Cron = strings.TrimSpace(task.Cron)
		task.Method = strings.ToUpper(strings.TrimSpace(task.Method))
		task.URL = strings.TrimSpace(task.URL)
		task.Path = strings.TrimSpace(task.Path)
		if task.Type == "" {
			// An entry with only a path is treated as an HTTP task, which makes
			// the escape hatch the path of least resistance. This must happen
			// before the id is derived, or the derived id would be empty.
			task.Type = TaskHTTP
		}
		if task.ID == "" {
			// A path-only entry gets a stable, readable id from its path so the
			// management API can address it.
			if trimmed := strings.Trim(task.Path, "/"); trimmed != "" && task.Type == TaskHTTP {
				task.ID = strings.ReplaceAll(trimmed, "/", "-")
			} else {
				task.ID = string(task.Type)
			}
		}
		task.CheckinMethod = strings.ToUpper(strings.TrimSpace(task.CheckinMethod))
		if task.CheckinMethod == "" {
			task.CheckinMethod = "POST"
		}
		task.CheckinURL = strings.TrimSpace(task.CheckinURL)
		task.CheckinSuccessMarker = strings.TrimSpace(task.CheckinSuccessMarker)
		task.CheckinAlreadyMarker = strings.TrimSpace(task.CheckinAlreadyMarker)
		if task.Cron == "" {
			task.Cron = "0 * * * *"
		}
	}
}

// scheduleTasks returns the effective task list.
func (c *Config) scheduleTasks() []ScheduleTask {
	if c == nil {
		return defaultScheduleTasks()
	}
	if len(c.Schedule.Tasks) > 0 {
		return c.Schedule.Tasks
	}
	if c.Schedule.DisableDefaults {
		return nil
	}
	return defaultScheduleTasks()
}

func config() *Config {
	if cfg := currentConfig.Load(); cfg != nil {
		return cfg
	}
	return defaultConfig()
}

// upstreamModel resolves the client-facing model ID to the upstream model_id.
func (c *Config) upstreamModel(model string) string {
	model = strings.TrimSpace(model)
	if mapped, ok := c.ModelMap[model]; ok {
		if trimmed := strings.TrimSpace(mapped); trimmed != "" {
			return trimmed
		}
	}
	if model != "" {
		return model
	}
	return c.DefaultModelID
}

// isBenefitModel reports whether the upstream model ID routes through 限时福利.
// Configured models answer from their `benefit` flag; discovered ones from the
// catalogue they arrived through. Matched case-insensitively against both id and
// name, mirroring how the official client routes these models by name.
func (c *Config) isBenefitModel(upstream string) bool {
	target := strings.ToLower(strings.TrimSpace(upstream))
	if target == "" {
		return false
	}
	if c != nil {
		for _, model := range c.Models {
			if !model.Benefit {
				continue
			}
			if strings.ToLower(strings.TrimSpace(model.ID)) == target ||
				strings.ToLower(strings.TrimSpace(model.Name)) == target {
				return true
			}
		}
	}
	return isKnownBenefitModel(target)
}

func (c *Config) requestTimeout() time.Duration {
	return time.Duration(c.RequestTimeoutSeconds) * time.Second
}

func (c *Config) loginTimeout() time.Duration {
	return time.Duration(c.LoginTimeoutSeconds) * time.Second
}
