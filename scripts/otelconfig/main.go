// Command otelconfig pulls the OpenTelemetry Collector configuration out of
// rendered Kubernetes manifests, so scripts/validate-otel-collector.sh can
// hand it to the Collector's own validate command.
//
// It reads manifests (the output of kubectl kustomize) on stdin, writes every
// ConfigMap value that is a Collector configuration to a file under -out, and
// prints the image of the first container that runs the Collector, so the
// configuration is validated by the exact Collector build that will load it.
//
//	kubectl kustomize deploy/otel-collector | go run ./scripts/otelconfig -out /tmp/otel
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"
)

// collectorImageMarker is the substring every Collector distribution image
// name carries (otel/opentelemetry-collector, …-contrib, …-k8s and vendor builds).
const collectorImageMarker = "opentelemetry-collector"

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "otelconfig: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("otelconfig", flag.ContinueOnError)
	fs.SetOutput(stderr)
	out := fs.String("out", "", "directory to write the Collector configurations to (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("-out is required")
	}
	manifests, err := io.ReadAll(stdin)
	if err != nil {
		return fmt.Errorf("read manifests: %w", err)
	}
	configs, image, err := extract(manifests)
	if err != nil {
		return err
	}
	if len(configs) == 0 {
		return errors.New("no Collector configuration found: no ConfigMap value has a service section with receivers or exporters")
	}
	if err := os.MkdirAll(*out, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", *out, err)
	}
	for _, c := range configs {
		path := filepath.Join(*out, c.fileName())
		if err := os.WriteFile(path, []byte(c.Config), 0o600); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
	}
	_, err = fmt.Fprintln(stdout, image)
	return err
}

// collectorConfig is one Collector configuration found in a ConfigMap.
type collectorConfig struct {
	ConfigMap string // metadata.name
	Key       string // the data key
	Config    string // the value, verbatim
}

// unsafeFileChars covers everything a ConfigMap name or key may hold that a
// file name should not.
var unsafeFileChars = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// fileName is <configmap>--<key>, made safe for a file system.
func (c collectorConfig) fileName() string {
	return unsafeFileChars.ReplaceAllString(c.ConfigMap, "_") + "--" + unsafeFileChars.ReplaceAllString(c.Key, "_")
}

// docSeparator splits a multi-document YAML stream.
var docSeparator = regexp.MustCompile(`(?m)^---[ \t]*$`)

// extract returns the Collector configurations in the manifests, sorted by
// ConfigMap name and key, and the first Collector container image (empty when
// no container runs one).
func extract(manifests []byte) ([]collectorConfig, string, error) {
	var (
		configs []collectorConfig
		image   string
	)
	for i, doc := range docSeparator.Split(string(manifests), -1) {
		js, err := yaml.YAMLToJSON([]byte(doc))
		if err != nil {
			return nil, "", fmt.Errorf("document %d: %w", i+1, err)
		}
		var obj map[string]any
		if err := json.Unmarshal(js, &obj); err != nil || obj == nil {
			continue // not a mapping: an empty or comment-only document
		}
		if image == "" {
			image = findImage(obj)
		}
		if obj["kind"] != "ConfigMap" {
			continue
		}
		name, _ := nested(obj, "metadata", "name").(string)
		data, _ := obj["data"].(map[string]any)
		for key, v := range data {
			value, ok := v.(string)
			if !ok || !isCollectorConfig(value) {
				continue
			}
			configs = append(configs, collectorConfig{ConfigMap: name, Key: key, Config: value})
		}
	}
	sort.Slice(configs, func(i, j int) bool {
		if configs[i].ConfigMap != configs[j].ConfigMap {
			return configs[i].ConfigMap < configs[j].ConfigMap
		}
		return configs[i].Key < configs[j].Key
	})
	return configs, image, nil
}

// isCollectorConfig reports whether value parses as a Collector configuration:
// a mapping with a service section and receivers or exporters. Other
// ConfigMap values (scripts, certificates, unrelated YAML) are left alone.
func isCollectorConfig(value string) bool {
	js, err := yaml.YAMLToJSON([]byte(value))
	if err != nil {
		return false
	}
	var cfg map[string]any
	if err := json.Unmarshal(js, &cfg); err != nil || cfg == nil {
		return false
	}
	if _, ok := cfg["service"].(map[string]any); !ok {
		return false
	}
	_, hasReceivers := cfg["receivers"]
	_, hasExporters := cfg["exporters"]
	return hasReceivers || hasExporters
}

// findImage returns the first "image" value under v that names the Collector,
// walking mappings and lists in key order so the result is deterministic.
func findImage(v any) string {
	switch node := v.(type) {
	case map[string]any:
		if img, ok := node["image"].(string); ok && strings.Contains(img, collectorImageMarker) {
			return img
		}
		keys := make([]string, 0, len(node))
		for k := range node {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if img := findImage(node[k]); img != "" {
				return img
			}
		}
	case []any:
		for _, item := range node {
			if img := findImage(item); img != "" {
				return img
			}
		}
	}
	return ""
}

// nested returns obj[path[0]][path[1]]… or nil.
func nested(obj map[string]any, path ...string) any {
	var cur any = obj
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[p]
	}
	return cur
}
