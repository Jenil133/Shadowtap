package index

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/internal/index/indextest"
)

func TestTemplatesAreValidJSON(t *testing.T) {
	order, bodies, err := Templates()
	if err != nil {
		t.Fatal(err)
	}
	if len(order) != 7 || order[0].Kind != KindILMPolicy || order[0].Name != ILMPolicy {
		t.Fatalf("order = %+v", order)
	}
	var patterns []string
	for _, tt := range order {
		var m map[string]any
		if err := json.Unmarshal(bodies[tt], &m); err != nil {
			t.Errorf("%s %s (%s): %v", tt.Kind, tt.Name, tt.File, err)
			continue
		}
		switch tt.Kind {
		case KindILMPolicy:
			expectFields(t, m, map[string]string{
				"policy.phases.hot.actions.rollover.max_age":                "1d",
				"policy.phases.hot.actions.rollover.max_primary_shard_size": "10gb",
				"policy.phases.delete.min_age":                              "7d",
			})
			if field(m, "policy.phases.delete.actions.delete") == nil {
				t.Error("delete phase has no delete action")
			}
		case KindComponentTemplate:
			if field(m, "template") == nil {
				t.Errorf("%s has no template", tt.Name)
			}
		case KindIndexTemplate:
			if field(m, "data_stream") == nil {
				t.Errorf("%s is not a data stream template", tt.Name)
			}
			for _, p := range field(m, "index_patterns").([]any) {
				patterns = append(patterns, p.(string))
			}
			composed, _ := json.Marshal(field(m, "composed_of"))
			if string(composed) != `["shadowtap-settings","shadowtap-common"]` {
				t.Errorf("%s composed_of = %s", tt.Name, composed)
			}
		}
	}
	// Every default data stream must be matched by exactly one template.
	for _, name := range DefaultIndices().All() {
		n := 0
		for _, p := range patterns {
			if matchPattern(p, name) {
				n++
			}
		}
		if n != 1 {
			t.Errorf("%s matches %d index templates, want 1", name, n)
		}
	}
}

// matchPattern matches an index pattern with one trailing wildcard.
func matchPattern(pattern, name string) bool {
	if prefix, ok := strings.CutSuffix(pattern, "*"); ok {
		return strings.HasPrefix(name, prefix)
	}
	return pattern == name
}

// mappingType returns the mapping type of a field in the template body.
func mappingType(t *testing.T, body []byte, path string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatal(err)
	}
	props := field(m, "template.mappings.properties")
	var cur any = props
	for i, seg := range strings.Split(path, ".") {
		pm, _ := cur.(map[string]any)
		f, ok := pm[seg].(map[string]any)
		if !ok {
			t.Fatalf("%s: segment %q not mapped", path, seg)
		}
		if i == len(strings.Split(path, "."))-1 {
			return f
		}
		cur = f["properties"]
	}
	return nil
}

func TestTemplateMappings(t *testing.T) {
	_, bodies, err := Templates()
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string][]byte{}
	for tt, b := range bodies {
		byName[tt.Name] = b
	}
	common := byName["shadowtap-common"]
	for _, path := range []string{"trace.id", "span.id", "parent.id", "service.name", "propagation.trace.id"} {
		if got := mappingType(t, common, path)["type"]; got != "keyword" {
			t.Errorf("common %s type = %v, want keyword", path, got)
		}
	}
	for _, path := range []string{"attributes", "resource.attributes"} {
		if got := mappingType(t, common, path)["type"]; got != "flattened" {
			t.Errorf("common %s type = %v, want flattened", path, got)
		}
	}
	var cm map[string]any
	_ = json.Unmarshal(common, &cm)
	if field(cm, "template.mappings.dynamic") != false {
		t.Error("common mappings are dynamic; unknown fields would explode the mapping")
	}
	for _, name := range []string{"traces-shadowtap", "logs-shadowtap"} {
		if got := mappingType(t, byName[name], "@timestamp")["type"]; got != "date_nanos" {
			t.Errorf("%s @timestamp type = %v, want date_nanos", name, got)
		}
	}
	for _, name := range []string{"shadowtap-captures", "shadowtap-diffs"} {
		if got := mappingType(t, byName[name], "@timestamp")["type"]; got != "date" {
			t.Errorf("%s @timestamp type = %v, want date", name, got)
		}
	}
	for _, path := range []string{"http.request.body.content", "http.response.body.content", "url.original"} {
		f := mappingType(t, byName["shadowtap-captures"], path)
		if f["index"] != false || f["doc_values"] != false {
			t.Errorf("captures %s = %v, want index and doc_values false", path, f)
		}
	}
	if got := mappingType(t, byName["traces-shadowtap"], "event.duration")["type"]; got != "long" {
		t.Errorf("traces event.duration type = %v", got)
	}
	if got := mappingType(t, byName["traces-shadowtap"], "links.trace.id")["type"]; got != "keyword" {
		t.Errorf("traces links.trace.id type = %v", got)
	}
	if got := mappingType(t, byName["shadowtap-diffs"], "replay.trace.id")["type"]; got != "keyword" {
		t.Errorf("diffs replay.trace.id type = %v", got)
	}
	if got := mappingType(t, byName["shadowtap-diffs"], "replay.http.response.status_code")["type"]; got != "integer" {
		t.Errorf("diffs replay status type = %v", got)
	}
	if got := mappingType(t, byName["shadowtap-captures"], "tap.name")["type"]; got != "keyword" {
		t.Errorf("captures tap.name type = %v", got)
	}
	var settings map[string]any
	_ = json.Unmarshal(byName["shadowtap-settings"], &settings)
	if field(settings, "template.settings.index.lifecycle.name") != ILMPolicy {
		t.Errorf("settings lifecycle = %v", field(settings, "template.settings.index.lifecycle.name"))
	}
}

func TestBootstrapIsIdempotent(t *testing.T) {
	es := newFakeES(t)
	client, err := NewElasticsearchClient(ESConfig{URLs: []string{es.URL()}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := Bootstrap(ctx, client, DefaultIndices()); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"PUT /_ilm/policy/" + ILMPolicy,
		"PUT /_component_template/shadowtap-settings",
		"PUT /_component_template/shadowtap-common",
		"PUT /_index_template/traces-shadowtap",
		"PUT /_index_template/logs-shadowtap",
		"PUT /_index_template/shadowtap-captures",
		"PUT /_index_template/shadowtap-diffs",
	}
	for _, name := range DefaultIndices().All() {
		want = append(want, "GET /_data_stream/"+name, "PUT /_data_stream/"+name)
	}
	got := requestLines(es.Requests())
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("first bootstrap requests:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if streams := es.DataStreams(); len(streams) != 4 {
		t.Errorf("data streams = %v", streams)
	}
	// Every PUT body is the embedded file, byte for byte.
	_, bodies, _ := Templates()
	for tt, body := range bodies {
		found := false
		for _, r := range es.Requests() {
			if r.Method == http.MethodPut && strings.HasSuffix(r.Path, "/"+tt.Name) && string(r.Body) == string(body) {
				found = true
			}
		}
		if !found {
			t.Errorf("%s %s body not sent as embedded", tt.Kind, tt.Name)
		}
	}

	// Running again re-puts the templates (a PUT is idempotent) and creates
	// no data stream: they already exist.
	if err := Bootstrap(ctx, client, DefaultIndices()); err != nil {
		t.Fatal(err)
	}
	second := requestLines(es.Requests())[len(want):]
	for _, line := range second {
		if strings.HasPrefix(line, "PUT /_data_stream/") {
			t.Errorf("second bootstrap created a data stream: %s", line)
		}
	}
	if n := len(second); n != len(want)-4 {
		t.Errorf("second bootstrap made %d requests, want %d", n, len(want)-4)
	}
}

func requestLines(reqs []indextest.Request) []string {
	out := make([]string, 0, len(reqs))
	for _, r := range reqs {
		out = append(out, r.Method+" "+r.Path)
	}
	return out
}

func TestBootstrapRace(t *testing.T) {
	// Another replica created the data stream between our GET and PUT: the
	// resource_already_exists_exception is success.
	es := newFakeES(t)
	es.SetRequestHook(func(r *http.Request) (int, string) {
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/_data_stream/") {
			return http.StatusBadRequest, `{"error":{"type":"resource_already_exists_exception","reason":"data_stream already exists"},"status":400}`
		}
		return 0, ""
	})
	client, err := NewElasticsearchClient(ESConfig{URLs: []string{es.URL()}})
	if err != nil {
		t.Fatal(err)
	}
	if err := Bootstrap(context.Background(), client, DefaultIndices()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
}

func TestBootstrapErrors(t *testing.T) {
	if err := Bootstrap(context.Background(), nil, DefaultIndices()); err == nil {
		t.Error("nil client accepted")
	}
	tests := []struct {
		name string
		hook func(r *http.Request) (int, string)
		want string
	}{
		{
			name: "policy rejected",
			hook: func(r *http.Request) (int, string) {
				if strings.HasPrefix(r.URL.Path, "/_ilm/") {
					return http.StatusForbidden, `{"error":{"type":"security_exception","reason":"no permission"}}`
				}
				return 0, ""
			},
			want: "put ILM policy shadowtap: elasticsearch returned 403",
		},
		{
			name: "template rejected",
			hook: func(r *http.Request) (int, string) {
				if strings.HasPrefix(r.URL.Path, "/_index_template/logs") {
					return http.StatusBadRequest, `{"error":{"type":"illegal_argument_exception","reason":"bad mapping"}}`
				}
				return 0, ""
			},
			want: "put index template logs-shadowtap: elasticsearch returned 400: {\"error\"",
		},
		{
			name: "data stream lookup fails",
			hook: func(r *http.Request) (int, string) {
				if strings.HasPrefix(r.URL.Path, "/_data_stream/") && r.Method == http.MethodGet {
					return http.StatusInternalServerError, `{"error":"boom"}`
				}
				return 0, ""
			},
			want: "get data stream traces-shadowtap-default: elasticsearch returned 500",
		},
		{
			name: "data stream creation fails for another reason",
			hook: func(r *http.Request) (int, string) {
				if strings.HasPrefix(r.URL.Path, "/_data_stream/") && r.Method == http.MethodPut {
					return http.StatusBadRequest, `{"error":{"type":"illegal_argument_exception","reason":"no matching index template"}}`
				}
				return 0, ""
			},
			want: "create data stream traces-shadowtap-default: elasticsearch returned 400",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			es := newFakeES(t)
			es.SetRequestHook(tt.hook)
			client, err := NewElasticsearchClient(ESConfig{URLs: []string{es.URL()}})
			if err != nil {
				t.Fatal(err)
			}
			err = Bootstrap(context.Background(), client, DefaultIndices())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Bootstrap = %v, want %q", err, tt.want)
			}
			var esErr *ESError
			if !errors.As(err, &esErr) {
				t.Errorf("error does not wrap *ESError: %v", err)
			}
		})
	}
	t.Run("unreachable", func(t *testing.T) {
		es := newFakeES(t)
		url := es.URL()
		es.Close()
		client, err := NewElasticsearchClient(ESConfig{URLs: []string{url}})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := Bootstrap(ctx, client, DefaultIndices()); err == nil {
			t.Error("Bootstrap succeeded against a closed server")
		}
	})
}

func TestNewElasticsearchClient(t *testing.T) {
	if _, err := NewElasticsearchClient(ESConfig{}); err == nil {
		t.Error("no URLs accepted")
	}
	if _, err := NewElasticsearchClient(ESConfig{URLs: []string{"http://localhost:9200"}, TLS: ESTLS{CAFile: "/nonexistent/ca.pem"}}); err == nil {
		t.Error("missing CA file accepted")
	}
	if _, err := NewElasticsearchClient(ESConfig{URLs: []string{"http://localhost:9200"}, TLS: ESTLS{CertFile: "/nonexistent/c.pem", KeyFile: "/nonexistent/k.pem"}}); err == nil {
		t.Error("missing client certificate accepted")
	}
	// Credentials reach the server: basic auth, and the API key winning.
	var seen []string
	es := newFakeES(t)
	es.SetRequestHook(func(r *http.Request) (int, string) {
		seen = append(seen, r.Header.Get("Authorization"))
		return 0, ""
	})
	for _, cfg := range []ESConfig{
		{URLs: []string{es.URL()}, Username: "elastic", Password: "secret"},
		{URLs: []string{es.URL()}, Username: "elastic", Password: "secret", APIKey: "a2V5"},
	} {
		client, err := NewElasticsearchClient(cfg)
		if err != nil {
			t.Fatal(err)
		}
		res, err := client.Ping()
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
	}
	if len(seen) != 2 || !strings.HasPrefix(seen[0], "Basic ") || !strings.EqualFold(seen[1], "ApiKey a2V5") {
		t.Errorf("authorization headers = %q", seen)
	}
}
