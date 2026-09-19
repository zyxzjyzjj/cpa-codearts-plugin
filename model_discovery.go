package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// These are the Act and Plan agents used by extension 26.3.6's
// setSpecialAgent/getActAndPlantModel. Model IDs are model_alias, not model_id.
var agentModeCatalogIDs = []string{"0a31170db80141e3b4119c2680f42af0", "c497c5d68d5a4d8fb7d58ef84df5f685"}

// The endpoints that actually answer "which models can this account use":
// the built-in catalogue the IDE's model picker reads, and the 限时福利 catalogue
// the free-tier channel serves from its own host.
const (
	epBuiltinModels = "/v1/model/builtin"
	epBenefitConfig = "/api/v1/gateway/config"
)

// benefitModelIDs remembers which upstream model IDs arrived through the 限时福利
// catalogue, because chat must then carry the signed maas_type: benefit header.
// It is keyed per upstream ID rather than per account: the header is what routes
// the request, and the same model name means the same channel.
var benefitModelIDs = struct {
	sync.Mutex
	ids map[string]bool
}{ids: make(map[string]bool)}

func markBenefitModels(models []ModelConfig) {
	benefitModelIDs.Lock()
	defer benefitModelIDs.Unlock()
	for _, model := range models {
		if !model.Benefit {
			continue
		}
		for _, name := range []string{model.ID, model.Name} {
			if trimmed := strings.ToLower(strings.TrimSpace(name)); trimmed != "" {
				benefitModelIDs.ids[trimmed] = true
			}
		}
	}
}

func isKnownBenefitModel(upstream string) bool {
	benefitModelIDs.Lock()
	defer benefitModelIDs.Unlock()
	return benefitModelIDs.ids[strings.ToLower(strings.TrimSpace(upstream))]
}

type modelCacheEntry struct {
	models  []ModelConfig
	expires time.Time
}

var discoveredModels = struct {
	sync.Mutex
	entries map[string]modelCacheEntry
}{entries: make(map[string]modelCacheEntry)}

func modelsForAuth(raw []byte) ([]byte, error) {
	var req struct {
		pluginapi.AuthModelRequest
		HostCallbackID string `json:"host_callback_id"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	if req.AuthProvider != "" && normalizeProvider(req.AuthProvider) != providerID {
		return okEnvelope(pluginapi.ModelResponse{})
	}
	cfg := config()
	models := cfg.Models
	cred, err := credentialFromStorage(req.StorageJSON)
	if cfg.DiscoverModels && err == nil && cred.valid() {
		found, err := discoverAccountModels(cfg, cred, req.HostCallbackID)
		if err == nil {
			models = found
			// Explicit configured aliases remain usable alongside discovered models.
			seen := map[string]bool{}
			for _, m := range models {
				seen[m.ID] = true
			}
			for _, m := range cfg.Models {
				if target := cfg.ModelMap[m.ID]; target != "" && seen[target] && !seen[m.ID] {
					models = append(models, m)
					seen[m.ID] = true
				}
			}
		} else {
			logWarn("model discovery failed; using configured models", map[string]any{"auth_id": req.AuthID, "error": err.Error()})
		}
	}
	return okEnvelope(pluginapi.ModelResponse{Provider: providerID, Models: infosForModels(models)})
}

func discoverAccountModels(cfg *Config, cred *credential, callbackID string) ([]ModelConfig, error) {
	// The configured agent ids stay in the key: two accounts that query different
	// agents must never be served each other's catalogue from cache.
	agentKey := strings.Join(cfg.ModelAgentIDs, ",")
	if cfg.APIMode == "native" {
		agentKey = cfg.AgentID
	}
	key := sha256Hex([]byte(cfg.BaseURL + "\x00" + cfg.BenefitBaseURL + "\x00" + cred.AccessKeyID +
		"\x00" + cred.DomainID + "\x00" + agentKey))
	discoveredModels.Lock()
	entry, ok := discoveredModels.entries[key]
	discoveredModels.Unlock()
	if ok && time.Now().Before(entry.expires) {
		markBenefitModels(entry.models)
		return append([]ModelConfig(nil), entry.models...), nil
	}

	var models []ModelConfig
	seen := map[string]bool{}
	appendModels := func(found []ModelConfig) {
		for _, model := range found {
			id := strings.ToLower(strings.TrimSpace(model.ID))
			if id == "" || seen[id] {
				continue
			}
			seen[id] = true
			models = append(models, model)
		}
	}

	// The built-in catalogue is what the IDE's own model picker reads, and the
	// 限时福利 catalogue rides on a separate host. The agent-center query is kept
	// as a fallback for tenants whose models are attached to custom agents.
	builtin, errBuiltin := fetchBuiltinModels(cfg, cred, callbackID)
	if errBuiltin != nil {
		logWarn("builtin model catalogue unavailable", map[string]any{"error": errBuiltin.Error()})
	}
	appendModels(builtin)

	benefit, errBenefit := fetchBenefitModels(cfg, cred, callbackID)
	if errBenefit != nil {
		logWarn("benefit model catalogue unavailable", map[string]any{"error": errBenefit.Error()})
	}
	appendModels(benefit)

	if len(models) == 0 {
		agent, errAgent := discoverAgentCenterModels(cfg, cred, callbackID)
		if errAgent != nil {
			return nil, errAgent
		}
		appendModels(agent)
	}
	if len(models) == 0 {
		return nil, fmt.Errorf("no catalogues returned an enabled model")
	}

	markBenefitModels(models)
	discoveredModels.Lock()
	for k, v := range discoveredModels.entries {
		if time.Now().After(v.expires) {
			delete(discoveredModels.entries, k)
		}
	}
	if len(discoveredModels.entries) < 512 {
		discoveredModels.entries[key] = modelCacheEntry{models: append([]ModelConfig(nil), models...), expires: time.Now().Add(5 * time.Minute)}
	}
	discoveredModels.Unlock()
	return models, nil
}

// fetchBuiltinModels reads the built-in model catalogue. The official client asks
// for it with Agent-Type: PromptCenter; without that header the gateway answers a
// different agent's view and the list comes back empty.
func fetchBuiltinModels(cfg *Config, cred *credential, callbackID string) ([]ModelConfig, error) {
	endpoint := strings.TrimRight(cfg.BaseURL, "/") + epBuiltinModels
	response, errCall := signedGet(cfg, cred, callbackID, endpoint, map[string]string{"Agent-Type": "PromptCenter"})
	if errCall != nil {
		return nil, errCall
	}
	var catalogue struct {
		BuiltinModels []struct {
			ModelID       string `json:"model_id"`
			ModelName     string `json:"model_name"`
			Enable        *bool  `json:"enable"`
			ContextWindow int64  `json:"context_window"`
			MaxTokens     int64  `json:"max_tokens"`
			SupportImages bool   `json:"supports_images"`
			Description   string `json:"model_desc"`
		} `json:"builtinModels"`
	}
	if errUnmarshal := json.Unmarshal(response.Body, &catalogue); errUnmarshal != nil {
		return nil, fmt.Errorf("invalid builtin model response: %w", errUnmarshal)
	}
	var models []ModelConfig
	for _, entry := range catalogue.BuiltinModels {
		if entry.Enable != nil && !*entry.Enable {
			continue
		}
		id := firstNonEmptyString(entry.ModelID, entry.ModelName)
		if id == "" {
			continue
		}
		models = append(models, ModelConfig{ID: id, Name: firstNonEmptyString(entry.ModelName, id),
			DisplayName: firstNonEmptyString(entry.ModelName, id), Description: entry.Description,
			ContextLength: entry.ContextWindow, MaxOutputTokens: entry.MaxTokens, SupportsImages: entry.SupportImages})
	}
	return models, nil
}

// fetchBenefitModels reads the 限时福利 catalogue from the open gateway. These
// models are absent from every agent catalogue and their chat requests must carry
// the maas_type: benefit header, so the entries are tagged as they are parsed.
func fetchBenefitModels(cfg *Config, cred *credential, callbackID string) ([]ModelConfig, error) {
	if strings.TrimSpace(cfg.BenefitBaseURL) == "" {
		return nil, nil
	}
	endpoint := strings.TrimRight(cfg.BenefitBaseURL, "/") + epBenefitConfig
	response, errCall := signedGet(cfg, cred, callbackID, endpoint, nil)
	if errCall != nil {
		return nil, errCall
	}
	var catalogue struct {
		ErrorCode string `json:"error_code"`
		ErrorMsg  string `json:"error_msg"`
		Result    struct {
			Models []struct {
				ModelID       string `json:"model_id"`
				ModelName     string `json:"model_name"`
				ContextWindow int64  `json:"context_window"`
				MaxTokens     int64  `json:"max_tokens"`
			} `json:"models"`
		} `json:"result"`
	}
	if errUnmarshal := json.Unmarshal(response.Body, &catalogue); errUnmarshal != nil {
		return nil, fmt.Errorf("invalid benefit gateway config: %w", errUnmarshal)
	}
	if catalogue.ErrorCode != "" && catalogue.ErrorCode != "0000" {
		return nil, fmt.Errorf("benefit gateway returned %s: %s", catalogue.ErrorCode, catalogue.ErrorMsg)
	}
	var models []ModelConfig
	for _, entry := range catalogue.Result.Models {
		id := firstNonEmptyString(entry.ModelID, entry.ModelName)
		if id == "" {
			continue
		}
		models = append(models, ModelConfig{ID: id, Name: firstNonEmptyString(entry.ModelName, id),
			DisplayName: firstNonEmptyString(entry.ModelName, id), ContextLength: entry.ContextWindow,
			MaxOutputTokens: entry.MaxTokens, Benefit: true})
	}
	return models, nil
}

// signedGet performs one signed GET against an upstream endpoint with optional
// extra protocol headers.
func signedGet(cfg *Config, cred *credential, callbackID, endpoint string, extra map[string]string) (*hostHTTPResponse, error) {
	headers := baseUpstreamHeaders(cfg, executorRequest{})
	headers["Accept"] = "application/json"
	for key, value := range extra {
		headers[key] = value
	}
	signed, errSign := signRequest(http.MethodGet, endpoint, headers, nil, cred, cfg.SignHost)
	if errSign != nil {
		return nil, errSign
	}
	return hostHTTPDoContext(callbackID, http.MethodGet, endpoint, signed, nil)
}

// discoverAgentCenterModels queries the agent centre for the models attached to
// the configured agents.
func discoverAgentCenterModels(cfg *Config, cred *credential, callbackID string) ([]ModelConfig, error) {
	ids := cfg.ModelAgentIDs
	if len(ids) == 0 {
		ids = agentModeCatalogIDs
		if cfg.APIMode == "native" {
			ids = []string{cfg.AgentID}
		}
	}
	var models []ModelConfig
	seen := map[string]bool{}
	var lastErr error
	for _, id := range ids {
		endpoint := strings.TrimRight(cfg.BaseURL, "/") + "/v1/agent-center/agents/detail?agent_id=" + url.QueryEscape(id)
		headers := baseUpstreamHeaders(cfg, executorRequest{})
		headers["Agent-Type"] = "AgentCenter"
		headers["Accept"] = "application/json"
		signed, err := signRequest(http.MethodGet, endpoint, headers, nil, cred, cfg.SignHost)
		if err != nil {
			return nil, err
		}
		response, err := hostHTTPDoContext(callbackID, http.MethodGet, endpoint, signed, nil)
		if err != nil {
			lastErr = err
			continue
		}
		if response.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("Agent Center returned HTTP %d", response.StatusCode)
			continue
		}
		found, err := parseAgentModels(response.Body)
		if err != nil {
			lastErr = err
			continue
		}
		for _, m := range found {
			if !seen[m.ID] {
				seen[m.ID] = true
				models = append(models, m)
			}
		}
	}
	if len(models) == 0 {
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, fmt.Errorf("Agent Center returned no enabled models")
	}
	return models, nil
}

func parseAgentModels(body []byte) ([]ModelConfig, error) {
	var response struct {
		GPTs struct {
			Models []struct {
				Alias      string `json:"model_alias"`
				ID         string `json:"model_id"`
				Name       string `json:"model_name"`
				Parameters struct {
					Enabled        *bool `json:"enabled"`
					ContextWindow  int64 `json:"context_window"`
					MaxTokens      int64 `json:"max_tokens"`
					SupportsImages bool  `json:"supports_images"`
				} `json:"model_parameters"`
			} `json:"models"`
		} `json:"gpts"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("invalid Agent Center response: %w", err)
	}
	var models []ModelConfig
	for _, m := range response.GPTs.Models {
		if m.Parameters.Enabled != nil && !*m.Parameters.Enabled {
			continue
		}
		id := firstNonEmptyString(m.Alias, m.ID)
		if id == "" {
			continue
		}
		models = append(models, ModelConfig{ID: id, Name: id, DisplayName: firstNonEmptyString(m.Name, id),
			ContextLength: m.Parameters.ContextWindow, MaxOutputTokens: m.Parameters.MaxTokens, SupportsImages: m.Parameters.SupportsImages})
	}
	return models, nil
}
