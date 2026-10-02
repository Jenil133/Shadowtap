package config

import (
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"
)

// The configuration files shipped under deploy/ must load under the strict
// schema, so a renamed field cannot silently break a deployment.
func TestShippedConfigsLoad(t *testing.T) {
	root := filepath.Join("..", "..", "deploy")
	getenv := env(map[string]string{EnvScrubKey: testKey})

	t.Run("local", func(t *testing.T) {
		cfg, err := LoadTap(filepath.Join(root, "local", "tap.yaml"), getenv)
		if err != nil {
			t.Fatal(err)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		if cfg.Sampling.DefaultRate != 1 {
			t.Errorf("local default_rate = %v, want 1", cfg.Sampling.DefaultRate)
		}
	})

	t.Run("kubernetes configmap", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join(root, "k8s", "base", "configmap.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		var cm struct {
			Kind string            `json:"kind"`
			Data map[string]string `json:"data"`
		}
		if err := yaml.Unmarshal(data, &cm); err != nil {
			t.Fatal(err)
		}
		doc, ok := cm.Data["tap.yaml"]
		if cm.Kind != "ConfigMap" || !ok {
			t.Fatalf("configmap has no tap.yaml (kind %q)", cm.Kind)
		}
		cfg, err := LoadTap(writeFile(t, "tap.yaml", doc), getenv)
		if err != nil {
			t.Fatal(err)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		if cfg.Sampling.DefaultRate >= 1 {
			t.Errorf("production default_rate = %v; expected a sample", cfg.Sampling.DefaultRate)
		}
	})
}
