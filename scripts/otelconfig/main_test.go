package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const rendered = `# comments only
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: otel-collector
  namespace: shadowtap
data:
  relay.yaml: |
    receivers:
      otlp:
        protocols:
          grpc:
            endpoint: 0.0.0.0:4317
    exporters:
      kafka/spans:
        brokers: ["${env:KAFKA_BROKERS:-kafka:9092}"]
        topic: shadowtap.otel.spans.v1
    service:
      pipelines:
        traces:
          receivers: [otlp]
          exporters: [kafka/spans]
  notes.txt: |
    service: not a collector config, just a word
  other.yaml: |
    receivers: {}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: certs
data:
  ca.crt: "-----BEGIN CERTIFICATE-----"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: otel-collector
spec:
  template:
    spec:
      initContainers:
        - name: wait
          image: busybox:1.36
      containers:
        - name: collector
          image: otel/opentelemetry-collector-contrib:0.141.0
          args: ["--config=/conf/relay.yaml"]
---
`

func TestExtract(t *testing.T) {
	configs, image, err := extract([]byte(rendered))
	if err != nil {
		t.Fatal(err)
	}
	if image != "otel/opentelemetry-collector-contrib:0.141.0" {
		t.Errorf("image = %q", image)
	}
	if len(configs) != 1 {
		t.Fatalf("got %d configs, want 1: %+v", len(configs), configs)
	}
	c := configs[0]
	if c.ConfigMap != "otel-collector" || c.Key != "relay.yaml" {
		t.Errorf("config from %s/%s", c.ConfigMap, c.Key)
	}
	if !strings.Contains(c.Config, "shadowtap.otel.spans.v1") || !strings.HasPrefix(c.Config, "receivers:") {
		t.Errorf("config not verbatim:\n%s", c.Config)
	}
	if got := c.fileName(); got != "otel-collector--relay.yaml" {
		t.Errorf("fileName = %q", got)
	}
}

func TestExtractEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		manifests string
		configs   int
		image     string
		wantErr   bool
	}{
		{"empty", "", 0, "", false},
		{"no collector", "kind: Service\nmetadata: {name: x}\nspec: {}\n", 0, "", false},
		{"invalid yaml", "kind: [\n", 0, "", true},
		{"image in a list under a different kind", "kind: Pod\nspec:\n  containers:\n    - image: ghcr.io/x/opentelemetry-collector-k8s:1.0\n", 0, "ghcr.io/x/opentelemetry-collector-k8s:1.0", false},
		{"service without pipelines section is still a config", "kind: ConfigMap\nmetadata: {name: c}\ndata:\n  c.yaml: \"exporters: {debug: {}}\\nservice: {}\\n\"\n", 1, "", false},
		{"scalar document", "just a string\n", 0, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configs, image, err := extract([]byte(tt.manifests))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if len(configs) != tt.configs || image != tt.image {
				t.Errorf("got %d configs, image %q; want %d, %q", len(configs), image, tt.configs, tt.image)
			}
		})
	}
}

func TestRunWritesFilesAndPrintsImage(t *testing.T) {
	out := filepath.Join(t.TempDir(), "configs")
	var stdout, stderr bytes.Buffer
	if err := run([]string{"-out", out}, strings.NewReader(rendered), &stdout, &stderr); err != nil {
		t.Fatalf("run: %v (%s)", err, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "otel/opentelemetry-collector-contrib:0.141.0" {
		t.Errorf("stdout = %q", got)
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "otel-collector--relay.yaml" {
		t.Fatalf("files = %v", entries)
	}
	data, err := os.ReadFile(filepath.Join(out, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "kafka/spans") {
		t.Errorf("written config:\n%s", data)
	}

	for _, tt := range []struct {
		name  string
		args  []string
		input string
	}{
		{"missing -out", nil, rendered},
		{"no config", []string{"-out", out}, "kind: Service\n"},
		{"bad yaml", []string{"-out", out}, "kind: [\n"},
	} {
		if err := run(tt.args, strings.NewReader(tt.input), &stdout, &stderr); err == nil {
			t.Errorf("%s: no error", tt.name)
		}
	}
}
