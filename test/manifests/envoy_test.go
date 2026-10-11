// Package manifests checks the shipped Envoy configuration against Envoy's
// API, so a misspelled field or a wrong type fails here rather than when Envoy
// or Istio loads it.
package manifests

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	bootstrapv3 "github.com/envoyproxy/go-control-plane/envoy/config/bootstrap/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/config/trace/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	upstreamhttpv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"
)

const (
	extProcFilter  = "envoy.filters.http.ext_proc"
	serviceKey     = "x-shadowtap-service"
	istioTapTarget = "outbound|9002||shadowtap-tap.shadowtap.svc.cluster.local"
)

// strict rejects unknown fields; Any payloads resolve through the blank
// imports above.
var strict = protojson.UnmarshalOptions{}

func readJSON(t *testing.T, rel string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", rel))
	if err != nil {
		t.Fatal(err)
	}
	js, err := yaml.YAMLToJSON(data)
	if err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
	return js
}

// checkExtProc asserts the settings every Shadowtap ext_proc filter needs,
// and the ones that differ between observe and inline mode.
func checkExtProc(t *testing.T, where string, p *extprocv3.ExternalProcessor, cluster string, observe bool) {
	t.Helper()
	if err := p.ValidateAll(); err != nil {
		t.Errorf("%s: %v", where, err)
	}
	if !p.GetFailureModeAllow() {
		t.Errorf("%s: failure_mode_allow must be true so a tap outage never breaks traffic", where)
	}
	if got := p.GetGrpcService().GetEnvoyGrpc().GetClusterName(); got != cluster {
		t.Errorf("%s: cluster %q, want %q", where, got, cluster)
	}
	md := p.GetGrpcService().GetInitialMetadata()
	if len(md) != 1 || md[0].GetKey() != serviceKey || md[0].GetValue() == "" {
		t.Errorf("%s: initial_metadata must carry %s", where, serviceKey)
	}
	m := p.GetProcessingMode()
	if m.GetRequestHeaderMode() != extprocv3.ProcessingMode_SEND || m.GetResponseHeaderMode() != extprocv3.ProcessingMode_SEND {
		t.Errorf("%s: both header modes must be SEND", where)
	}
	if observe {
		if !p.GetObservabilityMode() {
			t.Errorf("%s: observe mode needs observability_mode", where)
		}
		// Observability mode only sends STREAMED (or GRPC/NONE) bodies.
		if m.GetRequestBodyMode() != extprocv3.ProcessingMode_STREAMED || m.GetResponseBodyMode() != extprocv3.ProcessingMode_STREAMED {
			t.Errorf("%s: observe mode bodies must be STREAMED", where)
		}
		return
	}
	if p.GetObservabilityMode() || !p.GetAllowModeOverride() {
		t.Errorf("%s: inline mode needs allow_mode_override and no observability_mode", where)
	}
	if m.GetRequestBodyMode() != extprocv3.ProcessingMode_BUFFERED_PARTIAL || m.GetResponseBodyMode() != extprocv3.ProcessingMode_BUFFERED_PARTIAL {
		// BUFFERED would answer oversized bodies with an error.
		t.Errorf("%s: inline mode bodies must be BUFFERED_PARTIAL", where)
	}
	if p.GetMessageTimeout().AsDuration() <= 0 {
		t.Errorf("%s: inline mode needs a message_timeout", where)
	}
}

func TestLocalEnvoyBootstrap(t *testing.T) {
	var b bootstrapv3.Bootstrap
	if err := strict.Unmarshal(readJSON(t, "deploy/local/envoy.yaml"), &b); err != nil {
		t.Fatalf("envoy.yaml does not match the Envoy API: %v", err)
	}
	if err := b.ValidateAll(); err != nil {
		t.Fatalf("envoy.yaml: %v", err)
	}

	modes := map[string]bool{} // listener -> observe
	for _, l := range b.GetStaticResources().GetListeners() {
		for _, fc := range l.GetFilterChains() {
			for _, f := range fc.GetFilters() {
				var hcm hcmv3.HttpConnectionManager
				if err := f.GetTypedConfig().UnmarshalTo(&hcm); err != nil {
					t.Fatalf("listener %s: %v", l.GetName(), err)
				}
				filters := hcm.GetHttpFilters()
				if l.GetName() == "control" {
					// The control listener measures the request path without the
					// tap: only the router.
					if len(filters) != 1 || filters[0].GetName() != "envoy.filters.http.router" {
						t.Errorf("listener control: want only the router, got %d filters", len(filters))
					}
					continue
				}
				if len(filters) < 2 || filters[0].GetName() != extProcFilter || filters[len(filters)-1].GetName() != "envoy.filters.http.router" {
					t.Errorf("listener %s: want ext_proc first and the router last", l.GetName())
					continue
				}
				var p extprocv3.ExternalProcessor
				if err := filters[0].GetTypedConfig().UnmarshalTo(&p); err != nil {
					t.Fatalf("listener %s: %v", l.GetName(), err)
				}
				observe := p.GetObservabilityMode()
				modes[l.GetName()] = observe
				checkExtProc(t, "listener "+l.GetName(), &p, "shadowtap_tap", observe)
			}
		}
	}
	if len(modes) != 2 || !modes["observe"] || modes["inline"] {
		t.Errorf("want an observe and an inline listener, got %v", modes)
	}

	for _, c := range b.GetStaticResources().GetClusters() {
		if c.GetName() != "shadowtap_tap" {
			continue
		}
		var opts upstreamhttpv3.HttpProtocolOptions
		if err := c.GetTypedExtensionProtocolOptions()["envoy.extensions.upstreams.http.v3.HttpProtocolOptions"].UnmarshalTo(&opts); err != nil {
			t.Fatalf("cluster shadowtap_tap: %v", err)
		}
		if opts.GetExplicitHttpConfig().GetHttp2ProtocolOptions() == nil {
			t.Error("cluster shadowtap_tap must use HTTP/2: ext_proc is a gRPC stream")
		}
		return
	}
	t.Error("no shadowtap_tap cluster")
}

func TestIstioEnvoyFilters(t *testing.T) {
	for file, observe := range map[string]bool{
		"deploy/istio/envoyfilter-observe.yaml": true,
		"deploy/istio/envoyfilter-inline.yaml":  false,
	} {
		t.Run(filepath.Base(file), func(t *testing.T) {
			var ef struct {
				Kind string `json:"kind"`
				Spec struct {
					ConfigPatches []struct {
						ApplyTo string `json:"applyTo"`
						Match   struct {
							Context string `json:"context"`
						} `json:"match"`
						Patch struct {
							Operation string          `json:"operation"`
							Value     json.RawMessage `json:"value"`
						} `json:"patch"`
					} `json:"configPatches"`
				} `json:"spec"`
			}
			if err := json.Unmarshal(readJSON(t, file), &ef); err != nil {
				t.Fatal(err)
			}
			if ef.Kind != "EnvoyFilter" || len(ef.Spec.ConfigPatches) != 1 {
				t.Fatalf("want one EnvoyFilter patch, got kind %q with %d patches", ef.Kind, len(ef.Spec.ConfigPatches))
			}
			patch := ef.Spec.ConfigPatches[0]
			if patch.ApplyTo != "HTTP_FILTER" || patch.Match.Context != "SIDECAR_INBOUND" || patch.Patch.Operation != "INSERT_BEFORE" {
				t.Errorf("patch targets %s/%s/%s, want HTTP_FILTER/SIDECAR_INBOUND/INSERT_BEFORE",
					patch.ApplyTo, patch.Match.Context, patch.Patch.Operation)
			}
			var filter hcmv3.HttpFilter
			if err := strict.Unmarshal(patch.Patch.Value, &filter); err != nil {
				t.Fatalf("patch value does not match the Envoy API: %v", err)
			}
			if filter.GetName() != extProcFilter {
				t.Errorf("filter name %q", filter.GetName())
			}
			var p extprocv3.ExternalProcessor
			if err := filter.GetTypedConfig().UnmarshalTo(&p); err != nil {
				t.Fatal(err)
			}
			checkExtProc(t, file, &p, istioTapTarget, observe)
		})
	}
}
