package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func testEnvelopeResult[T any](t *testing.T, raw []byte, err error) T {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	if !env.OK {
		t.Fatalf("RPC failed: %+v", env.Error)
	}
	var out T
	if err := json.Unmarshal(env.Result, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func testHost(t *testing.T, fn func(string, any) (json.RawMessage, error)) {
	t.Helper()
	t.Cleanup(setHostCall(fn))
}

func TestCredentialPersistReloadAndRenew(t *testing.T) {
	cred := credential{AccessKeyID: "fake-ak", SecretAccessKey: "fake-sk", SecurityToken: "fake-sts", DomainID: "tenant", UserName: "user", UserID: "id", LoginType: "WEB"}
	doc, err := buildAuthFileDocument(cred)
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range [][]byte{doc, []byte(`{"accessKeyId":"fake-ak","secretAccessKey":"fake-sk","accessToken":"fake-sts"}`)} {
		req, _ := json.Marshal(pluginapi.AuthParseRequest{Provider: providerID, FileName: "account.json", RawJSON: payload})
		raw, err := authParse(req)
		parsed := testEnvelopeResult[pluginapi.AuthParseResponse](t, raw, err)
		if !parsed.Handled {
			t.Fatal("persisted/imported credential was not handled")
		}
		stored, err := credentialFromStorage(parsed.Auth.StorageJSON)
		if err != nil || !stored.valid() || stored.SecurityToken != cred.SecurityToken {
			t.Fatal("credential did not round trip")
		}
	}
	testHost(t, func(method string, request any) (json.RawMessage, error) {
		if method != "host.http.do" {
			return json.RawMessage(`{}`), nil
		}
		req := request.(map[string]any)
		if !strings.HasSuffix(req["url"].(string), "/token/renew") {
			t.Fatal("wrong renew endpoint")
		}
		return json.Marshal(hostHTTPResponse{StatusCode: 200, Body: []byte(`{"credential":{"access":"renewed-ak","secret":"renewed-sk","securitytoken":"renewed-sts","expires_at":"2030-01-01T00:00:00Z"}}`)})
	})
	req, _ := json.Marshal(pluginapi.AuthRefreshRequest{StorageJSON: doc})
	raw, err := authRefresh(req)
	renewed := testEnvelopeResult[pluginapi.AuthRefreshResponse](t, raw, err)
	newDoc, err := mergeCredentialFile(doc, renewed.Auth.StorageJSON)
	if err != nil {
		t.Fatal(err)
	}
	req, _ = json.Marshal(pluginapi.AuthParseRequest{Provider: providerID, FileName: "account.json", RawJSON: newDoc})
	raw, err = authParse(req)
	parsed := testEnvelopeResult[pluginapi.AuthParseResponse](t, raw, err)
	stored, _ := credentialFromStorage(parsed.Auth.StorageJSON)
	if !parsed.Handled || stored.AccessKeyID != "renewed-ak" || stored.UserID != "id" {
		t.Fatal("renewed credential/identity lost on reload")
	}
}

// TestBuildLoginURLMatchesExtension pins the OAuth PKCE URL used by CodeArts
// Agent 26.9.101.
func TestBuildLoginURLMatchesExtension(t *testing.T) {
	cfg := defaultConfig()
	ctx := &oauthLoginContext{PKCEPair: oauthPKCEPair{
		CodeVerifier:        "verifier",
		CodeChallenge:       "challenge-value",
		CodeChallengeMethod: codeArtsOAuthPKCEMethod,
	}}
	got := buildLoginURL(cfg, "f4415ec8-6377-43f7-b909-dc90d177aa50", "http://127.0.0.1:40605/oauth/callback", ctx)
	want := "https://codearts.huaweicloud.com/portal/authorize" +
		"?theme=2&locale=en&uri_scheme=vscode-codebot&client_id=vscode-codebot&port=40605" +
		"&code_challenge=challenge-value&code_challenge_method=SHA-256" +
		"&ticket_id=f4415ec8-6377-43f7-b909-dc90d177aa50" +
		"&auth_callback_url=http%3A%2F%2F127.0.0.1%3A40605%2Foauth%2Fcallback" +
		"&plugin-name=snap_vscode&plugin-version=26.9.101"
	if got != want {
		t.Fatalf("login URL mismatch:\n got %s\nwant %s", got, want)
	}
	parsed, errParse := url.Parse(got)
	if errParse != nil {
		t.Fatal(errParse)
	}
	if parsed.Path != "/portal/authorize" {
		t.Fatalf("wrong login path: %s", parsed.Path)
	}
	if callback := parsed.Query().Get("auth_callback_url"); callback != "http://127.0.0.1:40605/oauth/callback" {
		t.Fatalf("OAuth callback URL did not survive encoding: %q", callback)
	}
	if parsed.Query().Has("state") {
		t.Fatalf("authorization URL must match the official portal protocol: %s", got)
	}
	if parsed.Query().Get("plugin-name") != "snap_vscode" || parsed.Query().Get("client_id") != codeArtsOAuthClientID {
		t.Fatalf("plugin identity or idea type changed: %s", got)
	}
}

func TestConsumeHostOAuthCallback(t *testing.T) {
	authDir := t.TempDir()
	session := &loginSession{state: "plugin-state", expires: time.Now().Add(time.Minute)}
	path := filepath.Join(authDir, ".oauth-"+providerID+"-"+session.state+".oauth")
	if errWrite := os.WriteFile(path, []byte(`{"code":"host-code","state":"plugin-state","error":""}`), 0600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errConsume := session.consumeHostOAuthCallback(authDir); errConsume != nil {
		t.Fatal(errConsume)
	}
	if !session.received || session.authorizationCode != "host-code" || session.callbackAt.IsZero() {
		t.Fatalf("host callback was not imported: %+v", session)
	}
	if _, errStat := os.Stat(path); !os.IsNotExist(errStat) {
		t.Fatalf("consumed callback file still exists: %v", errStat)
	}
}

func TestOAuthPKCEDPoPExchangeAndPersistence(t *testing.T) {
	ctx, errContext := newOAuthLoginContext()
	if errContext != nil {
		t.Fatal(errContext)
	}
	if len(ctx.PKCEPair.CodeVerifier) != 128 || ctx.PKCEPair.CodeChallengeMethod != "SHA-256" {
		t.Fatalf("unexpected PKCE context: %+v", ctx.PKCEPair)
	}
	digest := sha256.Sum256([]byte(ctx.PKCEPair.CodeVerifier))
	if want := base64.RawURLEncoding.EncodeToString(digest[:]); ctx.PKCEPair.CodeChallenge != want {
		t.Fatalf("PKCE challenge mismatch: got %q want %q", ctx.PKCEPair.CodeChallenge, want)
	}
	proof, errProof := dpopProof(ctx, http.MethodPost, codeArtsOAuthTokenURL)
	if errProof != nil {
		t.Fatal(errProof)
	}
	parts := strings.Split(proof, ".")
	if len(parts) != 3 {
		t.Fatalf("DPoP proof is not a JWT: %q", proof)
	}
	headerRaw, _ := base64.RawURLEncoding.DecodeString(parts[0])
	var header struct {
		Alg string   `json:"alg"`
		Typ string   `json:"typ"`
		JWK oauthJWK `json:"jwk"`
	}
	if json.Unmarshal(headerRaw, &header) != nil || header.Alg != "ES256" || header.Typ != "dpop+jwt" || header.JWK.D != "" {
		t.Fatalf("invalid/public-key-leaking DPoP header: %s", headerRaw)
	}

	profile, _ := json.Marshal(oauthIdentity{AccountID: "account", PrincipalID: "principal", PrincipalURN: "iam::account:test-user"})
	claims, _ := json.Marshal(map[string]string{"user_profile": base64.RawURLEncoding.EncodeToString(profile)})
	refreshToken := "e30." + base64.RawURLEncoding.EncodeToString(claims) + ".signature"
	testHost(t, func(method string, request any) (json.RawMessage, error) {
		if method != "host.http.do" {
			return nil, fmt.Errorf("unexpected host method %s", method)
		}
		req := request.(map[string]any)
		if req["method"] != http.MethodPost || req["url"] != codeArtsOAuthTokenURL {
			t.Fatalf("wrong OAuth token request: %#v", req)
		}
		headers := req["headers"].(map[string][]string)
		if len(headers["DPoP"]) != 1 || len(strings.Split(headers["DPoP"][0], ".")) != 3 {
			t.Fatal("OAuth exchange omitted DPoP proof")
		}
		form, _ := url.ParseQuery(string(req["body"].([]byte)))
		if form.Get("grant_type") != "authorization_code" || form.Get("code") != "one-time-code" || form.Get("code_verifier") != ctx.PKCEPair.CodeVerifier {
			t.Fatalf("wrong OAuth token form: %v", form)
		}
		body, _ := json.Marshal(map[string]any{
			"credentials": map[string]string{
				"access_key_id": "oauth-ak", "secret_access_key": "oauth-sk", "security_token": "oauth-sts", "expiration": "2030-01-01T00:00:00Z",
			},
			"refresh_token": refreshToken,
		})
		return json.Marshal(hostHTTPResponse{StatusCode: http.StatusOK, Body: body})
	})
	cred, status, message, errExchange := oauthExchangeAuthorizationCode(defaultConfig(), "one-time-code", "http://127.0.0.1:40605/oauth/callback", ctx)
	if errExchange != nil || status != 200 || message != "" || cred.UserName != "test-user" || cred.DomainID != "account" || cred.RefreshToken == "" {
		t.Fatalf("OAuth exchange failed: cred=%+v status=%d message=%q err=%v", cred, status, message, errExchange)
	}
	doc, _ := buildAuthFileDocument(*cred)
	stored, errStored := credentialFromStorage(doc)
	if errStored != nil || stored.OAuthContext == nil || stored.OAuthContext.DPoPKeyPair.PrivateKeyJWK.D == "" || stored.RefreshToken != refreshToken {
		t.Fatalf("OAuth refresh state did not persist: %+v err=%v", stored, errStored)
	}
}

func TestOAuthRefreshReusesProofContextAndRefreshToken(t *testing.T) {
	ctx, errContext := newOAuthLoginContext()
	if errContext != nil {
		t.Fatal(errContext)
	}
	profile, _ := json.Marshal(oauthIdentity{AccountID: "account", PrincipalID: "principal", PrincipalURN: "iam::account:test-user"})
	claims, _ := json.Marshal(map[string]string{"user_profile": base64.RawURLEncoding.EncodeToString(profile)})
	refreshToken := "e30." + base64.RawURLEncoding.EncodeToString(claims) + ".signature"
	existing := &credential{
		AccessKeyID: "old-ak", SecretAccessKey: "old-sk", SecurityToken: "old-sts",
		DomainID: "account", UserID: "principal", UserName: "test-user", LoginType: "WEB",
		RefreshToken: refreshToken, OAuthContext: ctx,
	}
	testHost(t, func(method string, request any) (json.RawMessage, error) {
		if method != "host.http.do" {
			return nil, fmt.Errorf("unexpected host method %s", method)
		}
		req := request.(map[string]any)
		headers := req["headers"].(map[string][]string)
		form, _ := url.ParseQuery(string(req["body"].([]byte)))
		if form.Get("grant_type") != "refresh_token" || form.Get("refresh_token") != refreshToken || form.Get("code_verifier") != ctx.PKCEPair.CodeVerifier {
			t.Fatalf("wrong OAuth refresh form: %v", form)
		}
		if len(headers["DPoP"]) != 1 {
			t.Fatal("OAuth refresh omitted DPoP proof")
		}
		body := []byte(`{"credentials":{"access_key_id":"new-ak","secret_access_key":"new-sk","security_token":"new-sts","expiration":"2030-01-02T00:00:00Z"}}`)
		return json.Marshal(hostHTTPResponse{StatusCode: http.StatusOK, Body: body})
	})
	updated, status, message, errRefresh := oauthRefreshCredential(defaultConfig(), existing)
	if errRefresh != nil || status != http.StatusOK || message != "" {
		t.Fatalf("OAuth refresh failed: status=%d message=%q err=%v", status, message, errRefresh)
	}
	if updated.AccessKeyID != "new-ak" || updated.RefreshToken != refreshToken || updated.OAuthContext != ctx || updated.UserName != existing.UserName {
		t.Fatalf("OAuth refresh lost credential context: %+v", updated)
	}
}

// TestLoginCallbackMatchesExtensionBehaviour covers the new authorization-code
// callback while retaining the legacy secret fallback.
func TestLoginCallbackMatchesExtensionBehaviour(t *testing.T) {
	s := &loginSession{expires: time.Now().Add(time.Minute), callbackURL: "http://127.0.0.1:40000/oauth/callback"}
	w := httptest.NewRecorder()
	s.handleCallback(w, httptest.NewRequest("GET", "http://127.0.0.1/oauth/callback?code=authorization-code&state=portal-state", nil))
	if w.Code != 200 || s.authorizationCode != "authorization-code" || !s.received {
		t.Fatalf("callback rejected: %d", w.Code)
	}
	if s.callbackAt.IsZero() {
		t.Fatal("callback arrival was not recorded")
	}

	// A second callback must not be able to replace the bound secret, and it must
	// still be answered (the browser is showing the page).
	w = httptest.NewRecorder()
	s.handleCallback(w, httptest.NewRequest("GET", "http://127.0.0.1/oauth/callback?code=other", nil))
	if w.Code != 200 || s.authorizationCode != "authorization-code" {
		t.Fatalf("a second callback replaced the code: %d %q", w.Code, s.authorizationCode)
	}

	plain := &loginSession{expires: time.Now().Add(time.Minute), callbackURL: "http://127.0.0.1:40001/oauth/callback"}
	w = httptest.NewRecorder()
	plain.handleCallback(w, httptest.NewRequest("GET", "http://127.0.0.1/oauth/callback", nil))
	if w.Code != http.StatusBadRequest || plain.received {
		t.Fatalf("code-less OAuth callback accepted: %d", w.Code)
	}

	// The old secret callback remains accepted during migration.
	posted := &loginSession{expires: time.Now().Add(time.Minute)}
	w = httptest.NewRecorder()
	posted.handleCallback(w, httptest.NewRequest("POST", "http://127.0.0.1/authentication", strings.NewReader(`{"secret":"body-secret"}`)))
	if w.Code != 200 || posted.secret != "body-secret" {
		t.Fatalf("POST callback rejected: %d %q", w.Code, posted.secret)
	}

	// Preflight must succeed so a browser-side fetch can reach us.
	w = httptest.NewRecorder()
	s.handleCallback(w, httptest.NewRequest("OPTIONS", "http://127.0.0.1/oauth/callback", nil))
	if w.Code != http.StatusNoContent {
		t.Fatalf("preflight rejected: %d", w.Code)
	}

	s.expires = time.Now().Add(-time.Minute)
	w = httptest.NewRecorder()
	s.handleCallback(w, httptest.NewRequest("GET", "http://127.0.0.1/oauth/callback?code=late", nil))
	if w.Code != http.StatusGone {
		t.Fatal("expired callback accepted")
	}
}

func TestLoginTicketExchangeBindsTicketAndSecret(t *testing.T) {
	s := &loginSession{ticketID: "test-ticket", secret: "web-secret"}
	testHost(t, func(method string, request any) (json.RawMessage, error) {
		req := request.(map[string]any)
		u, _ := url.Parse(req["url"].(string))
		if u.Query().Get("ticket_id") != s.ticketID || u.Query().Get("secret") != s.secret {
			t.Fatal("ticket/secret mismatch")
		}
		return json.Marshal(hostHTTPResponse{StatusCode: 200, Body: []byte(`{"credential":{"access":"a","secret":"s","securitytoken":"t"},"user_id":"user"}`)})
	})
	cred, retry, status, message, err := s.exchangeTicket(defaultConfig())
	if err != nil || retry || status != 200 || message != "" || !cred.valid() || cred.UserID != "user" {
		t.Fatalf("exchange failed: %v status=%d message=%q", err, status, message)
	}
	if _, err := decodeCredentialResponse([]byte(`{"credential":{"access":"a"}}`)); err == nil {
		t.Fatal("incomplete renewal accepted")
	}
}

// TestTicketExchangeKeepsPollingAndReportsStatus pins the "never silently
// stuck" behaviour: a non-200 ticket answer stays retryable and its status and
// body excerpt are kept for the panel instead of aborting the flow.
func TestTicketExchangeKeepsPollingAndReportsStatus(t *testing.T) {
	s := &loginSession{ticketID: "test-ticket", secret: "web-secret", callbackURL: "http://127.0.0.1:40000/authentication"}
	testHost(t, func(method string, request any) (json.RawMessage, error) {
		return json.Marshal(hostHTTPResponse{StatusCode: http.StatusForbidden, Body: []byte(`{"error_code":"TM.00020003","message":"ticket not ready"}`)})
	})
	cred, retry, status, message, err := s.exchangeTicket(defaultConfig())
	if cred != nil || !retry || err != nil {
		t.Fatalf("a rejected ticket exchange must stay retryable: cred=%v retry=%v err=%v", cred, retry, err)
	}
	if status != http.StatusForbidden || !strings.Contains(message, "ticket not ready") {
		t.Fatalf("exchange diagnostics lost: status=%d message=%q", status, message)
	}

	// The reported progress must distinguish "waiting for the browser" from
	// "exchanging the ticket", so a hanging flow is explainable.
	if got := s.progress(); !strings.Contains(got, "等待浏览器") {
		t.Fatalf("pending flow did not report what it waits for: %q", got)
	}
	s.mu.Lock()
	s.received = true
	s.attempts = 1
	s.lastStatus = status
	s.lastMessage = message
	s.mu.Unlock()
	if got := s.progress(); !strings.Contains(got, "HTTP 403") || !strings.Contains(got, "ticket not ready") {
		t.Fatalf("exchange failure was not reported: %q", got)
	}
}

func TestAccountModelDiscoveryIsolation(t *testing.T) {
	cfg := defaultConfig()
	cfg.ModelAgentIDs = []string{"agent"}
	cfg.BaseURL = "https://discovery.test"
	requests := 0
	testHost(t, func(method string, request any) (json.RawMessage, error) {
		requests++
		req := request.(map[string]any)
		u, _ := url.Parse(req["url"].(string))
		if req["host_callback_id"] != "callback" {
			t.Fatal("callback context lost")
		}
		headers := req["headers"].(map[string][]string)
		tenant := "tenant-a"
		if strings.Contains(headers["Authorization"][0], "Access=account-b") {
			tenant = "tenant-b"
		}
		switch u.Path {
		case "/v1/model/builtin":
			if headers["Agent-Type"][0] != "PromptCenter" {
				t.Fatal("the built-in catalogue must be asked with Agent-Type: PromptCenter")
			}
			return json.Marshal(hostHTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"builtinModels":[` +
				`{"model_id":"` + tenant + `","model_name":"Test","enable":true,"supports_images":true,"context_window":200000,"max_tokens":16000},` +
				`{"model_id":"disabled","enable":false}]}`)})
		case "/api/v1/gateway/config":
			return json.Marshal(hostHTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"error_code":"0000","result":{"models":[` +
				`{"model_id":"` + tenant + `-free","model_name":"Free","context_window":1048576,"max_tokens":393216}]}}`)})
		default:
			t.Fatalf("unexpected discovery path %s", u.Path)
			return nil, nil
		}
	})
	a := &credential{AccessKeyID: "account-a", SecretAccessKey: "test"}
	b := &credential{AccessKeyID: "account-b", SecretAccessKey: "test"}
	first, err := discoverAccountModels(cfg, a, "callback")
	if err != nil {
		t.Fatal(err)
	}
	second, err := discoverAccountModels(cfg, b, "callback")
	if err != nil {
		t.Fatal(err)
	}
	third, err := discoverAccountModels(cfg, a, "callback")
	if err != nil {
		t.Fatal(err)
	}
	// requests stays at four: two accounts x (built-in + benefit), with the third
	// call answered from cache, and each account only ever sees its own catalogue.
	if requests != 4 || len(first) != 2 || len(second) != 2 || len(third) != 2 {
		t.Fatalf("discovery did not isolate accounts or cache results: requests=%d first=%v second=%v third=%v",
			requests, idsOfModels(first), idsOfModels(second), idsOfModels(third))
	}
	if first[0].ID != "tenant-a" || !first[0].SupportsImages || second[0].ID != "tenant-b" {
		t.Fatal("built-in catalogue entries lost their fields or crossed accounts")
	}
	for _, pair := range []struct {
		models  []ModelConfig
		benefit string
	}{{first, "tenant-a-free"}, {second, "tenant-b-free"}} {
		if pair.models[1].ID != pair.benefit || !pair.models[1].Benefit {
			t.Fatalf("the 限时福利 entry was not tagged: %+v", pair.models[1])
		}
	}
	if !cfg.isBenefitModel("tenant-a-free") {
		t.Fatal("discovered benefit models must be routable without config entries")
	}
	if cfg.isBenefitModel("tenant-a") {
		t.Fatal("built-in catalogue models must not carry the benefit header")
	}
}

func TestAgentCenterDiscoveryIsTheFallback(t *testing.T) {
	cfg := defaultConfig()
	cfg.ModelAgentIDs = []string{"agent"}
	cfg.BaseURL = "https://discovery.test"
	var paths []string
	testHost(t, func(method string, request any) (json.RawMessage, error) {
		req := request.(map[string]any)
		u, _ := url.Parse(req["url"].(string))
		paths = append(paths, u.Path)
		switch u.Path {
		case "/v1/model/builtin", "/api/v1/gateway/config":
			return json.Marshal(hostHTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"builtinModels":[],"result":{"models":[]}}`)})
		case "/v1/agent-center/agents/detail":
			if u.Query().Get("agent_id") != "agent" {
				t.Fatal("wrong agent id queried")
			}
			headers := req["headers"].(map[string][]string)
			if headers["Agent-Type"][0] != "AgentCenter" {
				t.Fatal("agent centre must be asked with Agent-Type: AgentCenter")
			}
			return json.Marshal(hostHTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"gpts":{"models":[` +
				`{"model_alias":"tenant-a","model_id":"internal-id","model_name":"Test","model_parameters":{"enabled":true,"supports_images":true,"context_window":200000,"max_tokens":16000}},` +
				`{"model_alias":"disabled","model_parameters":{"enabled":false}}]}}`)})
		default:
			t.Fatalf("unexpected discovery path %s", u.Path)
			return nil, nil
		}
	})
	models, err := discoverAccountModels(cfg, &credential{AccessKeyID: "account-fallback", SecretAccessKey: "test"}, "callback")
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 1 || models[0].ID != "tenant-a" || !models[0].SupportsImages {
		t.Fatalf("agent-centre fallback lost its aliases: %+v", models)
	}
	for _, want := range []string{"/v1/model/builtin", "/api/v1/gateway/config", "/v1/agent-center/agents/detail"} {
		if !strings.Contains(strings.Join(paths, ","), want) {
			t.Fatalf("discovery never queried %s: %v", want, paths)
		}
	}
}

func idsOfModels(models []ModelConfig) []string {
	out := make([]string, 0, len(models))
	for _, model := range models {
		out = append(out, model.ID)
	}
	return out
}

func TestNativeAggregateRetainsFinalUsage(t *testing.T) {
	cfg := defaultConfig()
	cfg.APIMode = "native"
	body := []byte("data: {\"delta\":{\"content\":\"hello\"}}\r\n\r\ndata: {\"prompt_tokens\":11,\"completion_tokens\":7,\"total_tokens\":18}")
	raw, err := aggregateUpstream(cfg, "model", body)
	if err != nil {
		t.Fatal(err)
	}
	var result openAICompletion
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if result.Usage == nil || result.Usage.TotalTokens != 18 || result.Choices[0].Message.Content != "hello" {
		t.Fatalf("lost final content/usage: %s", raw)
	}
}

func TestAgentCRLFSplitAndSingleDone(t *testing.T) {
	r := newStreamRenderer(defaultConfig(), "model", protocolOpenAI)
	var output []byte
	for _, part := range []string{"data: {\"choices\":[]}\r", "\n\r", "\ndata: [DONE]\r\n\r\n"} {
		for _, frame := range r.feed([]byte(part)) {
			output = append(output, frame...)
		}
	}
	for _, frame := range r.finish() {
		output = append(output, frame...)
	}
	if strings.Count(string(output), "[DONE]") != 1 || !strings.Contains(string(output), `"choices"`) {
		t.Fatalf("bad stream: %s", output)
	}
}

func TestHostStreamUsesJSONChunks(t *testing.T) {
	var emitted [][]byte
	testHost(t, func(method string, request any) (json.RawMessage, error) {
		if method != "host.stream.emit" {
			t.Fatalf("unexpected callback %s", method)
		}
		emitted = append(emitted, request.(map[string]any)["payload"].([]byte))
		return json.RawMessage(`{}`), nil
	})
	renderer := newStreamRenderer(defaultConfig(), "model", protocolOpenAI)
	upstream := "data: {\"choices\":[]}\n\ndata: [DONE]\n\n:heartbeat\n\n"
	for _, frame := range renderer.feed([]byte(upstream)) {
		if err := hostStreamEmit("test", renderer.hostPayload(frame)); err != nil {
			t.Fatal(err)
		}
	}
	for _, frame := range renderer.finish() {
		if err := hostStreamEmit("test", renderer.hostPayload(frame)); err != nil {
			t.Fatal(err)
		}
	}
	if len(emitted) != 1 || !json.Valid(emitted[0]) {
		t.Fatalf("CPA requires bare JSON chunks for chat completions, got %q", emitted)
	}
}

// claudeEvent is one decoded Anthropic SSE event.
type claudeEvent struct {
	Name string
	Data map[string]any
}

// parseClaudeEvents decodes an Anthropic SSE stream, failing on any malformed
// block so a partially written frame cannot pass unnoticed.
func parseClaudeEvents(t *testing.T, stream []byte) []claudeEvent {
	t.Helper()
	var events []claudeEvent
	for _, block := range strings.Split(string(stream), "\n\n") {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		var name, data string
		for _, line := range strings.Split(block, "\n") {
			line = strings.TrimSpace(line)
			switch {
			case strings.HasPrefix(line, "event:"):
				name = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			case strings.HasPrefix(line, "data:"):
				data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			}
		}
		if name == "" || data == "" {
			t.Fatalf("malformed SSE block: %q", block)
		}
		payload := map[string]any{}
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			t.Fatalf("invalid JSON in %s event: %v", name, err)
		}
		if payload["type"] != name && name != "error" {
			t.Fatalf("event name %q does not match payload type %v", name, payload["type"])
		}
		events = append(events, claudeEvent{Name: name, Data: payload})
	}
	return events
}

func eventByType(events []claudeEvent, name string) []claudeEvent {
	var out []claudeEvent
	for _, event := range events {
		if event.Name == name {
			out = append(out, event)
		}
	}
	return out
}

func nestedMap(t *testing.T, value any, key string) map[string]any {
	t.Helper()
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("value is not an object: %#v", value)
	}
	nested, ok := object[key].(map[string]any)
	if !ok {
		t.Fatalf("key %q is not an object: %#v", key, object)
	}
	return nested
}

func TestClaudeStreamFramesAreSelfContainedSSE(t *testing.T) {
	var emitted [][]byte
	testHost(t, func(method string, request any) (json.RawMessage, error) {
		if method != "host.stream.emit" {
			t.Fatalf("unexpected callback %s", method)
		}
		emitted = append(emitted, request.(map[string]any)["payload"].([]byte))
		return json.RawMessage(`{}`), nil
	})
	renderer := newStreamRenderer(defaultConfig(), "model", protocolClaude)
	upstream := strings.Join([]string{
		`data: {"id":"chatcmpl-9","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":"hello"},"finish_reason":null}]}`,
		``,
		`data: {"id":"chatcmpl-9","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		``,
		`data: {"id":"chatcmpl-9","object":"chat.completion.chunk","model":"m","choices":[],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n")
	for _, frame := range renderer.feed([]byte(upstream)) {
		if err := hostStreamEmit("test", renderer.hostPayload(frame)); err != nil {
			t.Fatal(err)
		}
	}
	for _, frame := range renderer.finish() {
		if err := hostStreamEmit("test", renderer.hostPayload(frame)); err != nil {
			t.Fatal(err)
		}
	}
	stream := bytes.Join(emitted, nil)
	if strings.Contains(string(stream), "data: [DONE]") || strings.Contains(string(stream), `"object":"chat.completion.chunk"`) {
		t.Fatalf("openai framing leaked into the claude stream:\n%s", stream)
	}
	events := parseClaudeEvents(t, stream)
	if len(events) == 0 || events[0].Name != "message_start" {
		t.Fatalf("stream must open with message_start: %s", stream)
	}
	if last := events[len(events)-1].Name; last != "message_stop" {
		t.Fatalf("stream must end with message_stop, got %s: %s", last, stream)
	}
	if count := len(eventByType(events, "message_start")); count != 1 {
		t.Fatalf("message_start emitted %d times: %s", count, stream)
	}
	message := nestedMap(t, events[0].Data["message"], "usage")
	if message["input_tokens"] != float64(0) {
		t.Fatalf("unexpected message_start usage: %s", stream)
	}
	textSeen := false
	for _, event := range eventByType(events, "content_block_delta") {
		delta := nestedMap(t, event.Data, "delta")
		if delta["type"] == "text_delta" && delta["text"] == "hello" {
			textSeen = true
		}
	}
	if !textSeen {
		t.Fatalf("text delta missing: %s", stream)
	}
	stops := eventByType(events, "content_block_stop")
	if len(stops) != 1 {
		t.Fatalf("expected exactly one content_block_stop: %s", stream)
	}
	deltas := eventByType(events, "message_delta")
	if len(deltas) != 1 {
		t.Fatalf("expected exactly one message_delta: %s", stream)
	}
	delta := nestedMap(t, deltas[0].Data, "delta")
	if delta["stop_reason"] != "end_turn" {
		t.Fatalf("unexpected stop_reason: %s", stream)
	}
	usage := nestedMap(t, deltas[0].Data, "usage")
	if usage["input_tokens"] != float64(11) || usage["output_tokens"] != float64(7) {
		t.Fatalf("usage was not carried into message_delta: %s", stream)
	}
}

// TestClaudeStreamToolCallBlocks checks that streamed tool calls become
// sequential tool_use content blocks with their arguments delivered once.
func TestClaudeStreamToolCallBlocks(t *testing.T) {
	renderer := newStreamRenderer(defaultConfig(), "model", protocolClaude)
	upstream := strings.Join([]string{
		`data: {"id":"c1","model":"m","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call1","type":"function","function":{"name":"lookup","arguments":"{\"q\":"}}]},"finish_reason":null}]}`,
		``,
		`data: {"id":"c1","model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"hi\"}"}}]},"finish_reason":null}]}`,
		``,
		`data: {"id":"c1","model":"m","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		``,
		`data: {"id":"c1","model":"m","choices":[],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n")
	var output []byte
	for _, frame := range renderer.feed([]byte(upstream)) {
		output = append(output, frame...)
	}
	for _, frame := range renderer.finish() {
		output = append(output, frame...)
	}
	events := parseClaudeEvents(t, output)
	starts := eventByType(events, "content_block_start")
	if len(starts) != 1 {
		t.Fatalf("expected one content_block_start: %s", output)
	}
	block := nestedMap(t, starts[0].Data, "content_block")
	if block["type"] != "tool_use" || block["id"] != "call1" || block["name"] != "lookup" {
		t.Fatalf("unexpected tool block: %s", output)
	}
	if input, ok := block["input"].(map[string]any); !ok || len(input) != 0 {
		t.Fatalf("tool block must start with an empty input object: %s", output)
	}
	argumentsSeen := false
	for _, event := range eventByType(events, "content_block_delta") {
		delta := nestedMap(t, event.Data, "delta")
		if delta["type"] != "input_json_delta" {
			continue
		}
		if delta["partial_json"] != `{"q":"hi"}` {
			t.Fatalf("tool arguments were not reassembled: %s", output)
		}
		argumentsSeen = true
	}
	if !argumentsSeen {
		t.Fatalf("tool arguments were never delivered: %s", output)
	}
	if len(eventByType(events, "content_block_stop")) != 1 {
		t.Fatalf("expected one content_block_stop: %s", output)
	}
	deltas := eventByType(events, "message_delta")
	if len(deltas) != 1 || nestedMap(t, deltas[0].Data, "delta")["stop_reason"] != "tool_use" {
		t.Fatalf("expected stop_reason=tool_use: %s", output)
	}
}

// TestClaudeStreamErrorFrameIsAnthropicShaped keeps upstream failures readable
// for Anthropic clients instead of leaking an OpenAI error object.
func TestClaudeStreamErrorFrameIsAnthropicShaped(t *testing.T) {
	renderer := newStreamRenderer(defaultConfig(), "model", protocolClaude)
	output := renderer.feed([]byte("data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\"}}\n\ndata: [DONE]\n\n"))
	if len(output) != 1 {
		t.Fatalf("error frame produced %d events", len(output))
	}
	events := parseClaudeEvents(t, output[0])
	if len(events) != 1 || events[0].Name != "error" {
		t.Fatalf("unexpected error event: %s", output[0])
	}
	detail := nestedMap(t, events[0].Data, "error")
	if detail["message"] != "boom" {
		t.Fatalf("unexpected error payload: %s", output[0])
	}
}

func TestAnthropicMessageFromCompletion(t *testing.T) {
	body := []byte(`{"id":"chatcmpl-1","object":"chat.completion","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"hi","reasoning_content":"think","tool_calls":[{"index":0,"id":"call1","type":"function","function":{"name":"lookup","arguments":"{\"q\":1}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":4,"total_tokens":14,"prompt_tokens_details":{"cached_tokens":6}}}`)
	raw, err := anthropicMessageFromCompletion(body)
	if err != nil {
		t.Fatal(err)
	}
	var message struct {
		Type    string `json:"type"`
		Role    string `json:"role"`
		Content []struct {
			Type     string          `json:"type"`
			Text     string          `json:"text"`
			Thinking string          `json:"thinking"`
			ID       string          `json:"id"`
			Name     string          `json:"name"`
			Input    json.RawMessage `json:"input"`
		} `json:"content"`
		StopReason string `json:"stop_reason"`
		Usage      struct {
			InputTokens      int64 `json:"input_tokens"`
			OutputTokens     int64 `json:"output_tokens"`
			CacheReadTokens  int64 `json:"cache_read_input_tokens"`
			CacheWriteTokens int64 `json:"cache_creation_input_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &message); err != nil {
		t.Fatal(err)
	}
	if message.Type != "message" || message.Role != "assistant" || message.StopReason != "tool_use" {
		t.Fatalf("unexpected envelope: %s", raw)
	}
	if len(message.Content) != 3 || message.Content[0].Type != "thinking" || message.Content[1].Text != "hi" {
		t.Fatalf("unexpected content blocks: %s", raw)
	}
	tool := message.Content[2]
	if tool.Type != "tool_use" || tool.Name != "lookup" || string(tool.Input) != `{"q":1}` {
		t.Fatalf("unexpected tool block: %s", raw)
	}
	// Anthropic input_tokens exclude cache reads.
	if message.Usage.InputTokens != 4 || message.Usage.OutputTokens != 4 || message.Usage.CacheReadTokens != 6 {
		t.Fatalf("unexpected usage: %s", raw)
	}
	if !strings.Contains(string(raw), `"content":[`) {
		t.Fatalf("content must always be an array: %s", raw)
	}
}

func TestUsageIgnoresOtherProviders(t *testing.T) {
	before := usage.snapshot()["requests"]
	raw, err := usageHandle([]byte(`{"Provider":"other-provider","Model":"unrelated","Detail":{"TotalTokens":999}}`))
	_ = testEnvelopeResult[map[string]any](t, raw, err)
	if usage.snapshot()["requests"] != before {
		t.Fatal("other subscriptions were counted as CodeArts usage")
	}
}

func TestAgentPreservesToolConversationAndImages(t *testing.T) {
	input := `{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,test"}}]},{"role":"assistant","content":null,"tool_calls":[{"id":"call1","type":"function","function":{"name":"lookup","arguments":"{}"}}]},{"role":"tool","tool_call_id":"call1","content":"ok"}],"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],"tool_choice":"auto"}`
	req := executorRequest{}
	req.Model = "model"
	req.Payload = []byte(input)
	body, err := buildAgentBody(defaultConfig(), req, true)
	if err != nil {
		t.Fatal(err)
	}
	var before, after map[string]json.RawMessage
	json.Unmarshal([]byte(input), &before)
	json.Unmarshal(body, &after)
	for _, key := range []string{"messages", "tools", "tool_choice"} {
		var a, b any
		json.Unmarshal(before[key], &a)
		json.Unmarshal(after[key], &b)
		ja, _ := json.Marshal(a)
		jb, _ := json.Marshal(b)
		if string(ja) != string(jb) {
			t.Fatalf("agent changed %s", key)
		}
	}
	if _, err := buildNativeBody(defaultConfig(), req, true); err == nil {
		t.Fatal("native silently discarded tools/images")
	}
}

func TestStreamRejectsUpstreamBeforeAccepting(t *testing.T) {
	for _, status := range []int{401, 403, 429, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			closed := false
			testHost(t, func(method string, request any) (json.RawMessage, error) {
				switch method {
				case "host.http.do_stream":
					return json.Marshal(hostHTTPStreamOpen{StatusCode: status, StreamID: "upstream"})
				case "host.http.stream_close":
					closed = true
					return json.RawMessage(`{}`), nil
				default:
					t.Fatalf("unexpected async callback %s", method)
					return nil, nil
				}
			})
			req := executorRequest{StreamID: "client"}
			req.Model = "model"
			req.Payload = []byte(`{"messages":[{"role":"user","content":"hi"}]}`)
			req.StorageJSON = []byte(`{"access_key_id":"fake","secret_access_key":"fake"}`)
			request, _ := json.Marshal(req)
			raw, err := executorExecuteStream(request)
			if err != nil {
				t.Fatal(err)
			}
			var env envelope
			json.Unmarshal(raw, &env)
			if env.OK || env.Error == nil || env.Error.HTTPStatus != status || !closed {
				t.Fatalf("lost HTTP %d: %s", status, raw)
			}
		})
	}
}

// TestStreamTimeoutTerminatesOnce covers the lifecycle of a stream that times
// out while its reader is also failing: CPA must see exactly one error frame and
// one close, not a duplicate pair.
func TestStreamTimeoutTerminatesOnce(t *testing.T) {
	previous := currentConfig.Load()
	short := defaultConfig()
	short.RequestTimeoutSeconds = 1
	short.BaseURL = "https://timeout.test"
	currentConfig.Store(short)
	t.Cleanup(func() {
		if previous != nil {
			currentConfig.Store(previous)
			return
		}
		currentConfig.Store(defaultConfig())
	})

	var errorEmits, streamCloses, upstreamCloses atomic.Int32
	upstreamClosed := make(chan struct{})
	testHost(t, func(method string, request any) (json.RawMessage, error) {
		switch method {
		case "host.http.do_stream":
			return json.Marshal(hostHTTPStreamOpen{StatusCode: http.StatusOK, StreamID: "upstream"})
		case "host.http.stream_read":
			// The real host unblocks the reader with an error once the stream is
			// closed, which is what the timeout path does to it.
			<-upstreamClosed
			return nil, fmt.Errorf("stream closed")
		case "host.http.stream_close":
			if upstreamCloses.Add(1) == 1 {
				close(upstreamClosed)
			}
			return json.RawMessage(`{}`), nil
		case "host.stream.emit":
			if _, hasError := request.(map[string]any)["error"]; hasError {
				errorEmits.Add(1)
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

	req := executorRequest{StreamID: "client"}
	req.Model = "model"
	req.Payload = []byte(`{"messages":[{"role":"user","content":"hi"}]}`)
	req.StorageJSON = []byte(`{"access_key_id":"fake","secret_access_key":"fake","security_token":"fake"}`)
	raw, err := executorExecuteStream(mustMarshal(t, req))
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil || !env.OK {
		t.Fatalf("stream was not accepted: %s", raw)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && streamCloses.Load() < 1 {
		time.Sleep(50 * time.Millisecond)
	}
	// Give a duplicate close/error a chance to appear before asserting.
	time.Sleep(250 * time.Millisecond)
	if got := upstreamCloses.Load(); got != 1 {
		t.Fatalf("upstream stream closed %d times, want 1", got)
	}
	if got := streamCloses.Load(); got != 1 {
		t.Fatalf("client stream closed %d times, want 1", got)
	}
	if got := errorEmits.Load(); got != 1 {
		t.Fatalf("error frames emitted %d times, want 1", got)
	}
	if active, _ := activeStreams.Load("client"); active != nil {
		t.Fatal("stream was left registered after it ended")
	}
}

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestLoginSessionsAreBounded checks that abandoned login flows cannot grow the
// session table without limit, and that the newest flow stays addressable.
func TestLoginSessionsAreBounded(t *testing.T) {
	stopLoginSessions()
	t.Cleanup(stopLoginSessions)

	var last pluginapi.AuthLoginStartResponse
	for i := 0; i < maxLoginSessions+4; i++ {
		raw, err := authLoginStart([]byte(`{}`))
		last = testEnvelopeResult[pluginapi.AuthLoginStartResponse](t, raw, err)
	}
	loginMu.Lock()
	size := len(loginSessions)
	_, newestPresent := loginSessions[last.State]
	loginMu.Unlock()
	if size > maxLoginSessions {
		t.Fatalf("login table holds %d sessions, want at most %d", size, maxLoginSessions)
	}
	if !newestPresent {
		t.Fatal("the newest login flow was evicted")
	}
	raw, err := authLoginPoll(mustMarshal(t, pluginapi.AuthLoginPollRequest{State: last.State}))
	polled := testEnvelopeResult[pluginapi.AuthLoginPollResponse](t, raw, err)
	if polled.Status != pluginapi.AuthLoginStatusPending {
		t.Fatalf("newest flow is not pollable: %+v", polled)
	}
}

// TestModelDiscoveryFallsBackToConfiguredModels covers the failure and empty
// result paths the Agent Center endpoint can produce.
func TestModelDiscoveryFallsBackToConfiguredModels(t *testing.T) {
	cfg := defaultConfig()
	cfg.DiscoverModels = true
	cfg.Models = []ModelConfig{{ID: "configured-model", Name: "configured-model", ContextLength: 128000, MaxOutputTokens: 8192}}
	cfg.ModelAgentIDs = []string{"agent-a", "agent-b"}
	saved := currentConfig.Load()
	currentConfig.Store(cfg)
	t.Cleanup(func() {
		if saved != nil {
			currentConfig.Store(saved)
			return
		}
		currentConfig.Store(defaultConfig())
	})
	discoveredModels.Lock()
	discoveredModels.entries = map[string]modelCacheEntry{}
	discoveredModels.Unlock()

	storage, err := json.Marshal(map[string]any{storageKey: credential{AccessKeyID: "ak", SecretAccessKey: "sk", SecurityToken: "sts"}})
	if err != nil {
		t.Fatal(err)
	}
	request := mustMarshal(t, map[string]any{
		"AuthProvider": providerID,
		"AuthID":       "codearts-provider-test.json",
		"StorageJSON":  storage,
	})

	t.Run("every agent fails", func(t *testing.T) {
		testHost(t, func(method string, request any) (json.RawMessage, error) {
			if method != "host.http.do" {
				t.Fatalf("unexpected callback %s", method)
			}
			return nil, fmt.Errorf("gateway unreachable")
		})
		raw, err := modelsForAuth(request)
		response := testEnvelopeResult[pluginapi.ModelResponse](t, raw, err)
		if len(response.Models) != 1 || response.Models[0].ID != "configured-model" {
			t.Fatalf("discovery failure did not fall back to configured models: %+v", response.Models)
		}
	})

	t.Run("agent returns no usable model", func(t *testing.T) {
		testHost(t, func(method string, request any) (json.RawMessage, error) {
			if method != "host.http.do" {
				t.Fatalf("unexpected callback %s", method)
			}
			return json.Marshal(hostHTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"gpts":{"models":[{"model_alias":"off","model_parameters":{"enabled":false}}]}}`)})
		})
		raw, err := modelsForAuth(request)
		response := testEnvelopeResult[pluginapi.ModelResponse](t, raw, err)
		if len(response.Models) != 1 || response.Models[0].ID != "configured-model" {
			t.Fatalf("empty discovery did not fall back to configured models: %+v", response.Models)
		}
	})

	t.Run("one agent fails and one succeeds", func(t *testing.T) {
		var calls atomic.Int32
		testHost(t, func(method string, request any) (json.RawMessage, error) {
			if method != "host.http.do" {
				t.Fatalf("unexpected callback %s", method)
			}
			if calls.Add(1) == 1 {
				return nil, fmt.Errorf("first agent unavailable")
			}
			return json.Marshal(hostHTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"gpts":{"models":[{"model_alias":"partial-model","model_name":"Partial","model_parameters":{"enabled":true,"context_window":1000,"max_tokens":100}}]}}`)})
		})
		raw, err := modelsForAuth(request)
		response := testEnvelopeResult[pluginapi.ModelResponse](t, raw, err)
		if len(response.Models) != 1 || response.Models[0].ID != "partial-model" {
			t.Fatalf("partial discovery did not use the reachable agent: %+v", response.Models)
		}
	})
}

// TestRefreshDeadlineScheduling documents when the host is asked to renew.
func TestRefreshDeadlineScheduling(t *testing.T) {
	permanent := &credential{AccessKeyID: "ak", SecretAccessKey: "sk"}
	if deadline := refreshDeadline(permanent); time.Until(deadline) < 29*24*time.Hour {
		t.Fatalf("permanent key pair was scheduled for renewal at %s", deadline)
	}
	expired := &credential{AccessKeyID: "ak", SecretAccessKey: "sk", SecurityToken: "sts", ExpiresAt: time.Now().Add(-time.Hour).Format(time.RFC3339)}
	if deadline := refreshDeadline(expired); time.Until(deadline) > 10*time.Minute {
		t.Fatalf("an already expired credential was not renewed promptly: %s", deadline)
	}
	expiredOAuth := &credential{AccessKeyID: "ak", SecretAccessKey: "sk", SecurityToken: "sts", ExpiresAt: time.Now().Add(-time.Hour).Format(time.RFC3339), RefreshToken: "refresh", OAuthContext: &oauthLoginContext{}}
	if deadline := refreshDeadline(expiredOAuth); time.Until(deadline) > 2*time.Minute {
		t.Fatalf("an expired OAuth credential was not renewed promptly: %s", deadline)
	}
	fresh := &credential{AccessKeyID: "ak", SecretAccessKey: "sk", SecurityToken: "sts", ExpiresAt: time.Now().Add(24 * time.Hour).Format(time.RFC3339)}
	until := time.Until(refreshDeadline(fresh))
	if until < 22*time.Hour || until > 23*time.Hour {
		t.Fatalf("a 24h credential should be renewed about an hour before expiry, got %s", until)
	}
	unparsable := &credential{AccessKeyID: "ak", SecretAccessKey: "sk", SecurityToken: "sts", ExpiresAt: "2030-01-01 00:00:00"}
	until = time.Until(refreshDeadline(unparsable))
	if until < 55*time.Minute || until > time.Hour {
		t.Fatalf("an unparsable expiry should fall back to hourly renewal, got %s", until)
	}
}
