package manifests

// The wiring between the tap, Kafka and the other Shadowtap workloads as the manifests
// describe it. Topic names, the broker address, record size limits, mesh membership and the
// replay Job's inputs each appear in several files; they must agree with the tap's own
// configuration, or the deployment breaks in a way no single file reveals.

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/config"
	"github.com/Jenil133/Shadowtap/internal/diff"
)

const (
	kafkaTopics  = "deploy/k8s/kafka-topic.yaml"
	tapNamespace = "deploy/k8s/base/namespace.yaml"
	localTapYAML = "deploy/local/tap.yaml"
	// scrubKeyEnvValue stands in for the Secret when loading the tap's configuration.
	scrubKeyEnvValue = "0123456789abcdef0123456789abcdef"
)

// strimziBootstrap matches the bootstrap address of a Strimzi cluster:
// <cluster>-kafka-bootstrap.<namespace>.svc[.cluster.local]:<port>.
var strimziBootstrap = regexp.MustCompile(`^([a-z0-9-]+)-kafka-bootstrap\.([a-z0-9-]+)\.svc(\.cluster\.local)?:([0-9]+)$`)

// bootstrapHost finds Kafka bootstrap addresses inside any string, such as a ConfigMap
// value or a container argument.
// The leading group keeps a placeholder default such as ${env:KAFKA_BROKERS:-shadowtap-kafka-bootstrap...}
// from contributing its "-" to the address.
var bootstrapHost = regexp.MustCompile(`(?:^|[^a-z0-9-])([a-z0-9]+(?:-[a-z0-9]+)*-kafka-bootstrap\.[a-z0-9-]+\.svc(?:\.cluster\.local)?:[0-9]+)`)

type kafkaTopic struct {
	Kind     string `json:"kind"`
	Metadata meta   `json:"metadata"`
	Spec     struct {
		TopicName  string         `json:"topicName"`
		Partitions int            `json:"partitions"`
		Config     map[string]any `json:"config"`
	} `json:"spec"`
}

// intSetting reads a numeric topic setting, which YAML may hold as a number or a string.
func intSetting(t *testing.T, where string, settings map[string]any, key string) int {
	t.Helper()
	v, ok := settings[key]
	if !ok {
		t.Fatalf("%s: no %s setting", where, key)
	}
	switch n := v.(type) {
	case float64: // every JSON number decodes as float64
		if n != float64(int(n)) {
			t.Fatalf("%s: %s = %v is not an integer", where, key, v)
		}
		return int(n)
	case string:
		i, err := strconv.Atoi(strings.TrimSpace(n))
		if err != nil {
			t.Fatalf("%s: %s = %q is not a number", where, key, n)
		}
		return i
	}
	t.Fatalf("%s: %s = %v is not a number", where, key, v)
	return 0
}

// loadTapConfig loads the ConfigMap's tap.yaml the way the pod does, with the scrub key
// coming from the environment, and returns the ConfigMap's name with it.
func loadTapConfig(t *testing.T) (string, config.Tap) {
	t.Helper()
	var cm struct {
		Metadata meta              `json:"metadata"`
		Data     map[string]string `json:"data"`
	}
	decode(t, tapConfigMap, &cm)
	path := filepath.Join(t.TempDir(), "tap.yaml")
	if err := os.WriteFile(path, []byte(cm.Data["tap.yaml"]), 0o600); err != nil {
		t.Fatal(err)
	}
	getenv := func(k string) string {
		if k == config.EnvScrubKey {
			return scrubKeyEnvValue
		}
		return ""
	}
	cfg, err := config.LoadTap(path, getenv)
	if err != nil {
		t.Fatalf("%s: %v", tapConfigMap, err)
	}
	return cm.Metadata.Name, cfg
}

// loadKafkaTopics returns the Strimzi topics by topic name.
func loadKafkaTopics(t *testing.T) map[string]kafkaTopic {
	t.Helper()
	topics := map[string]kafkaTopic{}
	for _, doc := range readDocs(t, kafkaTopics) {
		var kt kafkaTopic
		if err := json.Unmarshal(doc, &kt); err != nil {
			t.Fatalf("%s: %v", kafkaTopics, err)
		}
		if kt.Kind != "KafkaTopic" {
			t.Fatalf("%s: unexpected kind %q", kafkaTopics, kt.Kind)
		}
		name := kt.Spec.TopicName
		if name == "" {
			name = kt.Metadata.Name
		}
		topics[name] = kt
	}
	if len(topics) == 0 {
		t.Fatalf("%s: no KafkaTopic", kafkaTopics)
	}
	return topics
}

// The production ConfigMap must start a tap once the Secret is present, and sample rather
// than capture everything.
func TestTapConfigMapValidates(t *testing.T) {
	_, cfg := loadTapConfig(t)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("%s does not validate: %v", tapConfigMap, err)
	}
	if cfg.Sampling.DefaultRate <= 0 || cfg.Sampling.DefaultRate >= 1 {
		t.Errorf("sampling.default_rate %v; production should sample a fraction of traffic", cfg.Sampling.DefaultRate)
	}
	if cfg.Kafka.Topic != capture.DefaultTopic {
		t.Errorf("kafka.topic %q, want %q (what shadowtap tail, replay and the indexer read by default)", cfg.Kafka.Topic, capture.DefaultTopic)
	}
	if len(cfg.Kafka.Brokers) != 1 {
		t.Errorf("kafka.brokers %v, want the single bootstrap address", cfg.Kafka.Brokers)
	}
}

// Records the tap fits to kafka.max_record_bytes must be accepted by the topic, and the
// topic must keep the tap's timestamps for replay to seek on.
func TestCapturesTopicMatchesTheTap(t *testing.T) {
	_, cfg := loadTapConfig(t)
	topics := loadKafkaTopics(t)

	t.Run("strimzi", func(t *testing.T) {
		kt, ok := topics[cfg.Kafka.Topic]
		if !ok {
			t.Fatalf("%s: no KafkaTopic for %s", kafkaTopics, cfg.Kafka.Topic)
		}
		where := kafkaTopics + " " + kt.Metadata.Name
		if limit := intSetting(t, where, kt.Spec.Config, "max.message.bytes"); limit <= cfg.Kafka.MaxRecordBytes {
			t.Errorf("%s: max.message.bytes %d is not above the tap's kafka.max_record_bytes %d", where, limit, cfg.Kafka.MaxRecordBytes)
		}
		if ts := fmt.Sprint(kt.Spec.Config["message.timestamp.type"]); ts != "CreateTime" {
			t.Errorf("%s: message.timestamp.type %q, want CreateTime so replay can seek on request start times", where, ts)
		}
		if _, ok := topics[diff.DefaultTopic]; !ok {
			t.Errorf("%s: no KafkaTopic for %s, which shadowtap replay --publish-diffs writes", kafkaTopics, diff.DefaultTopic)
		}
		var cluster, namespace string
		for name, other := range topics {
			c := other.Metadata.Labels["strimzi.io/cluster"]
			if cluster == "" {
				cluster, namespace = c, other.Metadata.Namespace
			}
			if c != cluster || other.Metadata.Namespace != namespace {
				t.Errorf("%s: topic %s belongs to %s/%s, others to %s/%s", kafkaTopics, name, other.Metadata.Namespace, c, namespace, cluster)
			}
		}
	})

	t.Run("compose", func(t *testing.T) {
		local, err := config.LoadTap(filepath.Join(repoRoot, localTapYAML), func(k string) string {
			if k == config.EnvScrubKey {
				return scrubKeyEnvValue
			}
			return ""
		})
		if err != nil {
			t.Fatal(err)
		}
		var compose struct {
			Services map[string]struct {
				Command json.RawMessage `json:"command"`
			} `json:"services"`
		}
		decode(t, composeFile, &compose)
		found := false
		for svc, s := range compose.Services {
			args := commandArgs(t, s.Command)
			// The init service runs a shell script that creates every topic with
			// one helper call per topic; its --config settings apply to all.
			script := strings.Join(args, "\n")
			if !containsPair(args, "--topic", local.Kafka.Topic) && !createsTopic(script, local.Kafka.Topic) {
				continue
			}
			found = true
			settings := map[string]any{}
			for _, m := range configSetting.FindAllStringSubmatch(script, -1) {
				settings[m[1]] = m[2]
			}
			where := composeFile + " " + svc
			if limit := intSetting(t, where, settings, "max.message.bytes"); limit <= local.Kafka.MaxRecordBytes {
				t.Errorf("%s: max.message.bytes %d is not above kafka.max_record_bytes %d in %s", where, limit, local.Kafka.MaxRecordBytes, localTapYAML)
			}
			if settings["message.timestamp.type"] != "CreateTime" {
				t.Errorf("%s: message.timestamp.type %v, want CreateTime", where, settings["message.timestamp.type"])
			}
		}
		if !found {
			t.Errorf("%s: no service creates the topic %s", composeFile, local.Kafka.Topic)
		}
	})
}

// commandArgs decodes a compose command, given as a list or as one string.
func commandArgs(t *testing.T, raw json.RawMessage) []string {
	t.Helper()
	if len(raw) == 0 {
		return nil
	}
	var list []string
	if err := json.Unmarshal(raw, &list); err == nil {
		return list
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("%s: command %s is neither a list nor a string", composeFile, raw)
	}
	return strings.Fields(s)
}

func containsPair(args []string, flag, value string) bool {
	for i, a := range args {
		if a == flag && i+1 < len(args) && args[i+1] == value {
			return true
		}
	}
	return false
}

// stringValues returns every string in a decoded YAML document, in no particular order.
func stringValues(v any, out *[]string) {
	switch node := v.(type) {
	case string:
		*out = append(*out, node)
	case map[string]any:
		for _, child := range node {
			stringValues(child, out)
		}
	case []any:
		for _, child := range node {
			stringValues(child, out)
		}
	}
}

// Every workload that talks to Kafka names the same bootstrap Service, in the namespace
// the topics live in, and that address has the shape Strimzi gives a bootstrap Service.
func TestKafkaBootstrapAgreesAcrossManifests(t *testing.T) {
	_, cfg := loadTapConfig(t)
	tapBroker := cfg.Kafka.Brokers[0]
	m := strimziBootstrap.FindStringSubmatch(tapBroker)
	if m == nil {
		t.Fatalf("%s: kafka.brokers %q is not a Strimzi bootstrap address (<cluster>-kafka-bootstrap.<ns>.svc:<port>)", tapConfigMap, tapBroker)
	}
	namespace := m[2]

	topics := loadKafkaTopics(t)
	for name, kt := range topics {
		if kt.Metadata.Namespace != namespace {
			t.Errorf("%s: topic %s is in namespace %q, the tap connects to a cluster in %q", kafkaTopics, name, kt.Metadata.Namespace, namespace)
		}
	}

	seen := map[string][]string{} // address -> files
	walkRepo(t, func(rel string, _ os.DirEntry) {
		if !strings.HasPrefix(rel, "deploy/") || strings.HasPrefix(rel, "deploy/local/") || !strings.HasSuffix(rel, ".yaml") {
			return
		}
		for _, doc := range readDocs(t, rel) {
			var v any
			if err := json.Unmarshal(doc, &v); err != nil {
				t.Fatalf("%s: %v", rel, err)
			}
			var strs []string
			stringValues(v, &strs)
			for _, s := range strs {
				for _, m := range bootstrapHost.FindAllStringSubmatch(s, -1) {
					seen[m[1]] = append(seen[m[1]], rel)
				}
			}
		}
	})
	if len(seen[tapBroker]) == 0 {
		t.Fatalf("%s not found in any manifest", tapBroker)
	}
	for addr, files := range seen {
		if addr != tapBroker {
			t.Errorf("%v connect to Kafka at %s, the tap to %s", files, addr, tapBroker)
		}
	}
}

// The policies in deploy/istio only hold when both the tap and the captured workloads carry
// a sidecar: without one on the tap, nothing terminates mTLS or enforces the
// AuthorizationPolicy; without one on demo-api, there is no ext_proc filter to patch.
func TestMeshMembership(t *testing.T) {
	w := loadTap(t)
	for _, tt := range []struct{ file, want string }{
		{tapNamespace, w.namespace},
		{demoNamespace, "demo"},
	} {
		var ns typedObject
		decode(t, tt.file, &ns)
		if ns.Kind != "Namespace" || ns.Metadata.Name != tt.want {
			t.Errorf("%s: want Namespace %s, got %s %s", tt.file, tt.want, ns.Kind, ns.Metadata.Name)
		}
		if ns.Metadata.Labels["istio-injection"] != "enabled" {
			t.Errorf("%s: istio-injection=%q, want enabled", tt.file, ns.Metadata.Labels["istio-injection"])
		}
	}
	if v, ok := w.podLabels["sidecar.istio.io/inject"]; ok && v != "true" {
		t.Errorf("%s: pod label sidecar.istio.io/inject=%q turns the sidecar off", tapDeployment, v)
	}
	var demo deployment
	decode(t, "deploy/k8s/demo/deployment.yaml", &demo)
	if v, ok := demo.Spec.Template.Metadata.Labels["sidecar.istio.io/inject"]; ok && v != "true" {
		t.Errorf("deploy/k8s/demo/deployment.yaml: pod label sidecar.istio.io/inject=%q turns the sidecar off", v)
	}
}

type jobContainer struct {
	Name string   `json:"name"`
	Args []string `json:"args"`
	Env  []struct {
		Name      string `json:"name"`
		ValueFrom *struct {
			SecretKeyRef *struct {
				Name string `json:"name"`
				Key  string `json:"key"`
			} `json:"secretKeyRef"`
		} `json:"valueFrom"`
	} `json:"env"`
	VolumeMounts []struct {
		Name      string `json:"name"`
		MountPath string `json:"mountPath"`
	} `json:"volumeMounts"`
}

type job struct {
	Kind     string `json:"kind"`
	Metadata meta   `json:"metadata"`
	Spec     struct {
		Template struct {
			Metadata meta `json:"metadata"`
			Spec     struct {
				Containers []jobContainer `json:"containers"`
				Volumes    []struct {
					Name      string `json:"name"`
					ConfigMap *struct {
						Name string `json:"name"`
					} `json:"configMap"`
				} `json:"volumes"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
}

// argValue returns the value of a --flag=value argument, or "".
func argValue(args []string, flag string) string {
	for _, a := range args {
		if v, ok := strings.CutPrefix(a, flag+"="); ok {
			return v
		}
	}
	return ""
}

// The replay Job must read the same captures, scrub with the same key and settings, publish
// to a topic that exists and target Services that exist, all of which it names by hand.
func TestReplayJobWiring(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(repoRoot, replayManifestsGlob))
	if err != nil {
		t.Fatal(err)
	}
	var jobs []job
	services := map[string]bool{} // "<name>.<namespace>:<port>"
	for _, f := range files {
		rel, _ := filepath.Rel(repoRoot, f)
		for _, doc := range readDocs(t, rel) {
			var obj typedObject
			if err := json.Unmarshal(doc, &obj); err != nil {
				t.Fatalf("%s: %v", rel, err)
			}
			switch obj.Kind {
			case "Job":
				var j job
				if err := json.Unmarshal(doc, &j); err != nil {
					t.Fatalf("%s: %v", rel, err)
				}
				jobs = append(jobs, j)
			case "Service":
				var svc service
				if err := json.Unmarshal(doc, &svc); err != nil {
					t.Fatalf("%s: %v", rel, err)
				}
				for _, p := range svc.Spec.Ports {
					services[fmt.Sprintf("%s.%s:%d", svc.Metadata.Name, svc.Metadata.Namespace, p.Port)] = true
				}
			}
		}
	}
	if len(jobs) == 0 {
		t.Skipf("no Job among %s", replayManifestsGlob)
	}
	w := loadTap(t)
	cmName, cfg := loadTapConfig(t)
	var secret struct {
		Metadata   meta              `json:"metadata"`
		StringData map[string]string `json:"stringData"`
	}
	decode(t, scrubSecretExample, &secret)
	topics := loadKafkaTopics(t)

	for _, j := range jobs {
		where := "Job " + j.Metadata.Name
		if j.Metadata.Namespace != w.namespace {
			// The ConfigMap and Secret it mounts live in the tap's namespace.
			t.Errorf("%s: namespace %q, want %q", where, j.Metadata.Namespace, w.namespace)
		}
		if len(j.Spec.Template.Spec.Containers) != 1 {
			t.Fatalf("%s: want one container, got %d", where, len(j.Spec.Template.Spec.Containers))
		}
		c := j.Spec.Template.Spec.Containers[0]
		if len(c.Args) == 0 || c.Args[0] != "replay" {
			t.Errorf("%s: args %v do not start with the replay subcommand", where, c.Args)
		}
		if got := argValue(c.Args, "--brokers"); got != cfg.Kafka.Brokers[0] {
			t.Errorf("%s: --brokers=%q, the tap publishes to %q", where, got, cfg.Kafka.Brokers[0])
		}
		if topic := argValue(c.Args, "--topic"); topic != "" && topic != cfg.Kafka.Topic {
			t.Errorf("%s: --topic=%q, the tap publishes to %q", where, topic, cfg.Kafka.Topic)
		}

		// Scrubbing with the tap's settings and key.
		tapConfig := argValue(c.Args, "--tap-config")
		if tapConfig == "" {
			t.Errorf("%s: no --tap-config; replay would scrub with default settings and could diff pseudonyms that disagree", where)
		}
		mounted := false
		for _, m := range c.VolumeMounts {
			if m.MountPath != filepath.Dir(tapConfig) {
				continue
			}
			for _, v := range j.Spec.Template.Spec.Volumes {
				if v.Name == m.Name && v.ConfigMap != nil && v.ConfigMap.Name == cmName {
					mounted = true
				}
			}
		}
		if tapConfig != "" && (!mounted || filepath.Base(tapConfig) != "tap.yaml") {
			t.Errorf("%s: --tap-config=%q is not the tap ConfigMap %s mounted into the container", where, tapConfig, cmName)
		}
		keyFromSecret := false
		for _, e := range c.Env {
			if e.Name != config.EnvScrubKey || e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
				continue
			}
			ref := e.ValueFrom.SecretKeyRef
			if _, ok := secret.StringData[ref.Key]; ref.Name == secret.Metadata.Name && ok {
				keyFromSecret = true
			}
		}
		if !keyFromSecret {
			t.Errorf("%s: %s does not come from Secret %s; replay must use the tap's key", where, config.EnvScrubKey, secret.Metadata.Name)
		}

		// Outputs and targets.
		if slicesContains(c.Args, "--publish-diffs") {
			topic := argValue(c.Args, "--diffs-topic")
			if topic == "" {
				topic = diff.DefaultTopic
			}
			if _, ok := topics[topic]; !ok {
				t.Errorf("%s: publishes diffs to %s, which %s does not create", where, topic, kafkaTopics)
			}
		}
		for _, flag := range []string{"--target", "--baseline"} {
			raw := argValue(c.Args, flag)
			if raw == "" {
				if flag == "--target" {
					t.Errorf("%s: no %s", where, flag)
				}
				continue
			}
			u, err := url.Parse(raw)
			if err != nil || u.Host == "" {
				t.Errorf("%s: %s=%q is not a URL", where, flag, raw)
				continue
			}
			host, port := u.Hostname(), u.Port()
			if port == "" {
				port = "80"
			}
			key := strings.TrimSuffix(host, ".svc.cluster.local") + ":" + port
			if !services[key] {
				t.Errorf("%s: %s=%s does not name a Service shipped with the replay manifests (%v)", where, flag, raw, sortedKeys(services))
			}
		}
		if j.Spec.Template.Metadata.Annotations["sidecar.istio.io/inject"] == "false" && w.podLabels["sidecar.istio.io/inject"] == "true" {
			// Documented trade-off, checked so it stays deliberate: without a sidecar the
			// Job reaches Kafka and the targets in plaintext, outside the mesh's policies.
			t.Logf("%s runs without a sidecar: its Kafka and target connections are plaintext and bypass mesh policies", where)
		}
	}
}

func slicesContains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sortStrings(keys)
	return keys
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// configSetting matches "--config key=value" in a kafka-topics.sh invocation.
var configSetting = regexp.MustCompile(`--config\s+([A-Za-z0-9.]+)=(\S+)`)

// createsTopic reports whether a topic-init script creates topic: a line that
// calls the script's create helper or kafka-topics.sh --topic with that name.
func createsTopic(script, topic string) bool {
	for line := range strings.SplitSeq(script, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		for i, f := range fields {
			if (f == "create" || f == "--topic") && i+1 < len(fields) && fields[i+1] == topic {
				return true
			}
		}
	}
	return false
}
