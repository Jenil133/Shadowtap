package manifests

// The Kubernetes and Istio manifests are read as plain documents (the Istio API module is
// not a dependency) and cross-checked against each other: the policies that lock the tap
// down must select the tap's pods, name the tap's real ports and admit exactly the
// namespaces whose sidecars carry a Shadowtap EnvoyFilter, or capture silently stops.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"sigs.k8s.io/yaml"

	"github.com/Jenil133/Shadowtap/internal/config"
	"github.com/Jenil133/Shadowtap/internal/scrub"
)

// Paths under the repository root.
const (
	tapKustomization    = "deploy/k8s/base/kustomization.yaml"
	tapDeployment       = "deploy/k8s/base/deployment.yaml"
	tapService          = "deploy/k8s/base/service.yaml"
	tapConfigMap        = "deploy/k8s/base/configmap.yaml"
	tapPDB              = "deploy/k8s/base/pdb.yaml"
	tapNetworkPolicy    = "deploy/k8s/base/networkpolicy.yaml"
	tapPeerAuth         = "deploy/istio/peerauthentication.yaml"
	tapAuthzPolicy      = "deploy/istio/authorizationpolicy.yaml"
	demoNamespace       = "deploy/k8s/demo/namespace.yaml"
	scrubSecretExample  = "deploy/k8s/secret.example.yaml"
	replayManifestsGlob = "deploy/k8s/replay/*.yaml"
	envoyFilterGlob     = "deploy/istio/envoyfilter-*.yaml"
)

// Container port names in the tap Deployment and Service.
const (
	extProcPortName = "grpc-extproc"
	adminPortName   = "http-admin"
)

// Admin endpoints served by internal/admin.
const (
	healthPath  = "/healthz"
	readyPath   = "/readyz"
	metricsPath = "/metrics"
)

// Ports of the Istio sidecar that scrapers reach directly (not through the app).
var sidecarPorts = map[int]string{15020: "istio-agent merged metrics and probes", 15090: "Envoy stats"}

// extProcMethod is the gRPC path Envoy calls on the tap.
var extProcMethod = "/" + extprocv3.ExternalProcessor_ServiceDesc.ServiceName + "/Process"

type meta struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

type selector struct {
	MatchLabels map[string]string `json:"matchLabels"`
}

type probe struct {
	HTTPGet *struct {
		Path string `json:"path"`
		Port any    `json:"port"` // a number or a container port name
	} `json:"httpGet"`
}

type container struct {
	Name  string   `json:"name"`
	Args  []string `json:"args"`
	Ports []struct {
		Name          string `json:"name"`
		ContainerPort int    `json:"containerPort"`
	} `json:"ports"`
	StartupProbe   *probe `json:"startupProbe"`
	ReadinessProbe *probe `json:"readinessProbe"`
	LivenessProbe  *probe `json:"livenessProbe"`
	VolumeMounts   []struct {
		Name      string `json:"name"`
		MountPath string `json:"mountPath"`
	} `json:"volumeMounts"`
}

type deployment struct {
	Kind     string `json:"kind"`
	Metadata meta   `json:"metadata"`
	Spec     struct {
		Selector selector `json:"selector"`
		Template struct {
			Metadata meta `json:"metadata"`
			Spec     struct {
				TerminationGracePeriodSeconds int         `json:"terminationGracePeriodSeconds"`
				Containers                    []container `json:"containers"`
				Volumes                       []struct {
					Name      string `json:"name"`
					ConfigMap *struct {
						Name string `json:"name"`
					} `json:"configMap"`
				} `json:"volumes"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
}

type service struct {
	Metadata meta `json:"metadata"`
	Spec     struct {
		Selector map[string]string `json:"selector"`
		Ports    []struct {
			Name        string `json:"name"`
			Port        int    `json:"port"`
			TargetPort  any    `json:"targetPort"`
			AppProtocol string `json:"appProtocol"`
		} `json:"ports"`
	} `json:"spec"`
}

type peerAuthentication struct {
	Kind     string `json:"kind"`
	Metadata meta   `json:"metadata"`
	Spec     struct {
		Selector selector `json:"selector"`
		MTLS     struct {
			Mode string `json:"mode"`
		} `json:"mtls"`
		PortLevelMTLS map[string]struct {
			Mode string `json:"mode"`
		} `json:"portLevelMtls"`
	} `json:"spec"`
}

type authorizationPolicy struct {
	Kind     string `json:"kind"`
	Metadata meta   `json:"metadata"`
	Spec     struct {
		Selector selector `json:"selector"`
		Action   string   `json:"action"`
		Rules    []struct {
			From []struct {
				Source struct {
					Namespaces []string `json:"namespaces"`
					Principals []string `json:"principals"`
				} `json:"source"`
			} `json:"from"`
			To []struct {
				Operation struct {
					Ports   []string `json:"ports"`
					Methods []string `json:"methods"`
					Paths   []string `json:"paths"`
				} `json:"operation"`
			} `json:"to"`
		} `json:"rules"`
	} `json:"spec"`
}

type networkPolicy struct {
	Kind     string `json:"kind"`
	Metadata meta   `json:"metadata"`
	Spec     struct {
		PodSelector selector `json:"podSelector"`
		PolicyTypes []string `json:"policyTypes"`
		Ingress     []struct {
			From []struct {
				NamespaceSelector *selector `json:"namespaceSelector"`
				PodSelector       *selector `json:"podSelector"`
				IPBlock           *struct {
					CIDR string `json:"cidr"`
				} `json:"ipBlock"`
			} `json:"from"`
			Ports []struct {
				Port     any    `json:"port"`
				Protocol string `json:"protocol"`
			} `json:"ports"`
		} `json:"ingress"`
		Egress []json.RawMessage `json:"egress"`
	} `json:"spec"`
}

type envoyFilter struct {
	Kind     string `json:"kind"`
	Metadata meta   `json:"metadata"`
	Spec     struct {
		WorkloadSelector struct {
			Labels map[string]string `json:"labels"`
		} `json:"workloadSelector"`
		ConfigPatches []struct {
			Patch struct {
				Value struct {
					TypedConfig struct {
						GrpcService struct {
							EnvoyGrpc struct {
								ClusterName string `json:"cluster_name"`
							} `json:"envoy_grpc"`
						} `json:"grpc_service"`
					} `json:"typed_config"`
				} `json:"value"`
			} `json:"patch"`
		} `json:"configPatches"`
	} `json:"spec"`
}

type typedObject struct {
	Kind     string `json:"kind"`
	Metadata meta   `json:"metadata"`
}

// docSeparator splits a multi-document YAML file.
var docSeparator = regexp.MustCompile(`(?m)^---[ \t]*$`)

// readDocs returns every non-empty document of a YAML file as JSON.
func readDocs(t *testing.T, rel string) []json.RawMessage {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", rel))
	if err != nil {
		t.Fatal(err)
	}
	var docs []json.RawMessage
	for _, doc := range docSeparator.Split(string(data), -1) {
		js, err := yaml.YAMLToJSON([]byte(doc))
		if err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		if string(js) == "null" { // comments only
			continue
		}
		docs = append(docs, js)
	}
	return docs
}

// decode reads a single-document YAML file into v.
func decode(t *testing.T, rel string, v any) {
	t.Helper()
	docs := readDocs(t, rel)
	if len(docs) != 1 {
		t.Fatalf("%s: want one document, got %d", rel, len(docs))
	}
	if err := json.Unmarshal(docs[0], v); err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
}

// subset reports whether every label in want is present in have with the same value.
func subset(want, have map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

// tap is the tap workload as the manifests describe it.
type tap struct {
	namespace string
	deploy    deployment
	svc       service
	ports     map[string]int // container port name -> number
	podLabels map[string]string
}

func (w tap) port(t *testing.T, name string) int {
	t.Helper()
	p, ok := w.ports[name]
	if !ok {
		t.Fatalf("%s: no container port named %s", tapDeployment, name)
	}
	return p
}

// portNumber resolves a port given as a number or as a container port name.
func (w tap) portNumber(v any) (int, bool) {
	switch p := v.(type) {
	case float64:
		return int(p), true
	case string:
		if n, err := strconv.Atoi(p); err == nil {
			return n, true
		}
		n, ok := w.ports[p]
		return n, ok
	}
	return 0, false
}

func loadTap(t *testing.T) tap {
	t.Helper()
	var k struct {
		Namespace string `json:"namespace"`
	}
	decode(t, tapKustomization, &k)
	if k.Namespace == "" {
		t.Fatalf("%s: no namespace", tapKustomization)
	}
	w := tap{namespace: k.Namespace, ports: map[string]int{}}
	decode(t, tapDeployment, &w.deploy)
	decode(t, tapService, &w.svc)
	w.podLabels = w.deploy.Spec.Template.Metadata.Labels
	for _, c := range w.deploy.Spec.Template.Spec.Containers {
		for _, p := range c.Ports {
			w.ports[p.Name] = p.ContainerPort
		}
	}
	for _, name := range []string{extProcPortName, adminPortName} {
		if _, ok := w.ports[name]; !ok {
			t.Fatalf("%s: no container port named %s", tapDeployment, name)
		}
	}
	return w
}

// loadEnvoyFilters returns every shipped EnvoyFilter by file.
func loadEnvoyFilters(t *testing.T) map[string]envoyFilter {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", envoyFilterGlob))
	if err != nil || len(files) == 0 {
		t.Fatalf("no EnvoyFilters match %s: %v", envoyFilterGlob, err)
	}
	out := map[string]envoyFilter{}
	for _, f := range files {
		rel, _ := filepath.Rel(filepath.Join("..", ".."), f)
		var ef envoyFilter
		decode(t, rel, &ef)
		if ef.Kind != "EnvoyFilter" || ef.Metadata.Namespace == "" || len(ef.Spec.WorkloadSelector.Labels) == 0 {
			t.Fatalf("%s: want a namespaced EnvoyFilter with a workloadSelector", rel)
		}
		out[rel] = ef
	}
	return out
}

// Every selector that is meant to pick the tap's pods must match the pod template.
func TestTapSelectorsMatchThePods(t *testing.T) {
	w := loadTap(t)
	var pa peerAuthentication
	decode(t, tapPeerAuth, &pa)
	var ap authorizationPolicy
	decode(t, tapAuthzPolicy, &ap)
	var np networkPolicy
	decode(t, tapNetworkPolicy, &np)
	var pdb struct {
		Spec struct {
			Selector selector `json:"selector"`
		} `json:"spec"`
	}
	decode(t, tapPDB, &pdb)

	for _, tt := range []struct {
		file   string
		labels map[string]string
	}{
		{tapDeployment, w.deploy.Spec.Selector.MatchLabels},
		{tapService, w.svc.Spec.Selector},
		{tapPDB, pdb.Spec.Selector.MatchLabels},
		{tapNetworkPolicy, np.Spec.PodSelector.MatchLabels},
		{tapPeerAuth, pa.Spec.Selector.MatchLabels},
		{tapAuthzPolicy, ap.Spec.Selector.MatchLabels},
	} {
		if len(tt.labels) == 0 {
			t.Errorf("%s: empty selector would match every pod in the namespace", tt.file)
			continue
		}
		if !subset(tt.labels, w.podLabels) {
			t.Errorf("%s: selector %v does not match the tap pod labels %v", tt.file, tt.labels, w.podLabels)
		}
	}
	for _, tt := range []struct {
		file, ns string
	}{{tapPeerAuth, pa.Metadata.Namespace}, {tapAuthzPolicy, ap.Metadata.Namespace}} {
		if tt.ns != w.namespace {
			t.Errorf("%s: namespace %q, want the tap's %q", tt.file, tt.ns, w.namespace)
		}
	}
}

// The tap only accepts mTLS on the ext_proc port, so every stream carries the caller's
// mesh identity for the AuthorizationPolicy; only the admin port may stay permissive, for
// scrapers outside the mesh.
func TestPeerAuthenticationIsStrictForExtProc(t *testing.T) {
	w := loadTap(t)
	var pa peerAuthentication
	decode(t, tapPeerAuth, &pa)
	if pa.Kind != "PeerAuthentication" {
		t.Fatalf("kind %q", pa.Kind)
	}
	if pa.Spec.MTLS.Mode != "STRICT" {
		t.Errorf("mtls.mode %q, want STRICT", pa.Spec.MTLS.Mode)
	}
	extProc, admin := w.port(t, extProcPortName), w.port(t, adminPortName)
	for port, m := range pa.Spec.PortLevelMTLS {
		n, err := strconv.Atoi(port)
		if err != nil {
			t.Errorf("portLevelMtls key %q is not a port number", port)
			continue
		}
		known := false
		for _, p := range w.ports {
			known = known || p == n
		}
		if !known {
			t.Errorf("portLevelMtls names port %d, which the tap does not expose", n)
		}
		if n == extProc && m.Mode != "STRICT" {
			t.Errorf("portLevelMtls[%d] = %s: the ext_proc port must stay STRICT", n, m.Mode)
		}
		if n == admin && m.Mode != "PERMISSIVE" && m.Mode != "STRICT" {
			t.Errorf("portLevelMtls[%d] = %q", n, m.Mode)
		}
	}
}

// Only sidecars from namespaces that carry a Shadowtap EnvoyFilter may open ext_proc
// streams; the admin port is read-only.
func TestAuthorizationPolicyAdmitsCapturedNamespacesOnly(t *testing.T) {
	w := loadTap(t)
	var ap authorizationPolicy
	decode(t, tapAuthzPolicy, &ap)
	if ap.Kind != "AuthorizationPolicy" {
		t.Fatalf("kind %q", ap.Kind)
	}
	if ap.Spec.Action != "ALLOW" {
		t.Fatalf("action %q, want ALLOW (unmatched requests are then denied)", ap.Spec.Action)
	}
	extProc := strconv.Itoa(w.port(t, extProcPortName))
	admin := strconv.Itoa(w.port(t, adminPortName))

	allowedNamespaces := map[string]bool{}
	extProcAllowed := false
	for i, rule := range ap.Spec.Rules {
		if len(rule.From) == 0 && len(rule.To) == 0 {
			t.Errorf("rule %d is empty and allows everything", i)
			continue
		}
		authenticated := false
		for _, f := range rule.From {
			if len(f.Source.Namespaces) > 0 || len(f.Source.Principals) > 0 {
				authenticated = true
			}
			for _, ns := range f.Source.Namespaces {
				allowedNamespaces[ns] = true
			}
		}
		for _, to := range rule.To {
			op := to.Operation
			if len(op.Ports) == 0 {
				t.Errorf("rule %d: an operation without ports applies to every port", i)
			}
			for _, port := range op.Ports {
				switch port {
				case extProc:
					if !authenticated {
						t.Errorf("rule %d opens the ext_proc port without a source namespace or principal", i)
					}
					if len(op.Paths) == 0 || contains(op.Paths, extProcMethod) {
						extProcAllowed = true
					}
				case admin:
					if len(op.Methods) != 1 || op.Methods[0] != "GET" {
						t.Errorf("rule %d: admin port methods %v, want [GET] only", i, op.Methods)
					}
					for _, p := range op.Paths {
						if p != metricsPath && p != healthPath && p != readyPath {
							t.Errorf("rule %d: admin path %q is not an admin endpoint", i, p)
						}
					}
					if len(op.Paths) == 0 {
						t.Errorf("rule %d: admin port without paths", i)
					}
				default:
					t.Errorf("rule %d allows port %s, which the tap does not expose", i, port)
				}
			}
		}
	}
	if !extProcAllowed {
		t.Errorf("no rule allows %s on port %s", extProcMethod, extProc)
	}
	for file, ef := range loadEnvoyFilters(t) {
		if !allowedNamespaces[ef.Metadata.Namespace] {
			t.Errorf("%s captures namespace %q, which the AuthorizationPolicy does not admit", file, ef.Metadata.Namespace)
		}
	}
}

// Without the mesh, the NetworkPolicy is what keeps the ext_proc port closed to the rest of
// the cluster; it must still let the demo namespace in and never restrict egress, which the
// tap needs for DNS, istiod and Kafka.
func TestNetworkPolicyAdmitsCapturedNamespaces(t *testing.T) {
	w := loadTap(t)
	var np networkPolicy
	decode(t, tapNetworkPolicy, &np)
	if np.Kind != "NetworkPolicy" {
		t.Fatalf("kind %q", np.Kind)
	}
	if len(np.Spec.PolicyTypes) != 1 || np.Spec.PolicyTypes[0] != "Ingress" || len(np.Spec.Egress) != 0 {
		t.Errorf("policyTypes %v with %d egress rules; want Ingress only so egress to DNS, istiod and Kafka stays open",
			np.Spec.PolicyTypes, len(np.Spec.Egress))
	}
	var demo typedObject
	decode(t, demoNamespace, &demo)
	if demo.Kind != "Namespace" {
		t.Fatalf("%s: kind %q", demoNamespace, demo.Kind)
	}

	extProc, admin := w.port(t, extProcPortName), w.port(t, adminPortName)
	demoReachesExtProc, adminGuarded := false, false
	for i, rule := range np.Spec.Ingress {
		if len(rule.From) == 0 {
			t.Errorf("ingress rule %d has no from: open to the whole cluster", i)
		}
		if len(rule.Ports) == 0 {
			t.Errorf("ingress rule %d has no ports: opens every port", i)
		}
		for _, p := range rule.Ports {
			n, ok := w.portNumber(p.Port)
			if !ok {
				t.Errorf("ingress rule %d: port %v is neither a number nor a tap port name", i, p.Port)
				continue
			}
			if p.Protocol != "" && p.Protocol != "TCP" {
				t.Errorf("ingress rule %d: port %d protocol %s", i, n, p.Protocol)
			}
			isTapPort := false
			for _, tp := range w.ports {
				isTapPort = isTapPort || tp == n
			}
			if _, sidecar := sidecarPorts[n]; !isTapPort && !sidecar {
				t.Errorf("ingress rule %d opens port %d, which neither the tap nor its sidecar serves", i, n)
			}
			for _, f := range rule.From {
				if f.NamespaceSelector == nil || len(f.NamespaceSelector.MatchLabels) == 0 {
					t.Errorf("ingress rule %d: port %d is open to a source without a labelled namespaceSelector", i, n)
					continue
				}
				if n == extProc && subset(f.NamespaceSelector.MatchLabels, demo.Metadata.Labels) {
					demoReachesExtProc = true
				}
				if n == admin {
					adminGuarded = true
				}
				if n == extProc && f.PodSelector == nil && subset(f.NamespaceSelector.MatchLabels, w.podLabels) {
					t.Errorf("ingress rule %d: namespaceSelector %v would match pod labels, is it meant as a podSelector?", i, f.NamespaceSelector.MatchLabels)
				}
			}
		}
	}
	if !demoReachesExtProc {
		t.Errorf("namespace %s (labels %v) cannot reach the ext_proc port %d", demo.Metadata.Name, demo.Metadata.Labels, extProc)
	}
	if !adminGuarded {
		t.Errorf("no ingress rule covers the admin port %d", admin)
	}
}

// Ports, probe paths and the EnvoyFilter's cluster name must agree with the binary's
// configuration: a drift here breaks capture or health checks at deploy time only.
func TestTapPortsAgreeAcrossManifests(t *testing.T) {
	w := loadTap(t)
	extProc, admin := w.port(t, extProcPortName), w.port(t, adminPortName)

	// The ConfigMap's tap.yaml listens where the Deployment exposes.
	var cm struct {
		Metadata meta              `json:"metadata"`
		Data     map[string]string `json:"data"`
	}
	decode(t, tapConfigMap, &cm)
	path := filepath.Join(t.TempDir(), "tap.yaml")
	if err := os.WriteFile(path, []byte(cm.Data["tap.yaml"]), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadTap(path, func(string) string { return "" })
	if err != nil {
		t.Fatalf("%s: %v", tapConfigMap, err)
	}
	for _, tt := range []struct {
		name, addr string
		want       int
	}{{"listen.grpc", cfg.Listen.GRPC, extProc}, {"listen.http", cfg.Listen.HTTP, admin}} {
		if got := listenPort(tt.addr); got != tt.want {
			t.Errorf("%s %q listens on %d, the Deployment exposes %d", tt.name, tt.addr, got, tt.want)
		}
	}

	// The tap container reads that ConfigMap from where it is mounted.
	var tapContainer *container
	for i := range w.deploy.Spec.Template.Spec.Containers {
		if c := &w.deploy.Spec.Template.Spec.Containers[i]; c.Name == "tap" {
			tapContainer = c
		}
	}
	if tapContainer == nil {
		t.Fatalf("%s: no container named tap", tapDeployment)
	}
	configArg := ""
	for i, a := range tapContainer.Args {
		if a == "--config" && i+1 < len(tapContainer.Args) {
			configArg = tapContainer.Args[i+1]
		}
	}
	mounted := false
	for _, m := range tapContainer.VolumeMounts {
		if filepath.Dir(configArg) != m.MountPath {
			continue
		}
		for _, v := range w.deploy.Spec.Template.Spec.Volumes {
			if v.Name == m.Name && v.ConfigMap != nil && v.ConfigMap.Name == cm.Metadata.Name {
				mounted = true
			}
		}
	}
	if !mounted || filepath.Base(configArg) != "tap.yaml" {
		t.Errorf("--config %q is not the ConfigMap %s mounted into the tap container", configArg, cm.Metadata.Name)
	}

	// Probes hit the admin server's endpoints on the admin port.
	for _, tt := range []struct {
		name string
		p    *probe
		path string
	}{
		{"startupProbe", tapContainer.StartupProbe, healthPath},
		{"livenessProbe", tapContainer.LivenessProbe, healthPath},
		{"readinessProbe", tapContainer.ReadinessProbe, readyPath},
	} {
		if tt.p == nil || tt.p.HTTPGet == nil {
			t.Errorf("%s: no httpGet %s", tapDeployment, tt.name)
			continue
		}
		if port, ok := w.portNumber(tt.p.HTTPGet.Port); !ok || port != admin {
			t.Errorf("%s port %v, want the admin port %d", tt.name, tt.p.HTTPGet.Port, admin)
		}
		if tt.p.HTTPGet.Path != tt.path {
			t.Errorf("%s path %q, want %s", tt.name, tt.p.HTTPGet.Path, tt.path)
		}
	}
	ann := w.deploy.Spec.Template.Metadata.Annotations
	if ann["prometheus.io/scrape"] != "true" || ann["prometheus.io/port"] != strconv.Itoa(admin) || ann["prometheus.io/path"] != metricsPath {
		t.Errorf("prometheus.io annotations %v do not point at %s on port %d", ann, metricsPath, admin)
	}

	// The Service fronts both ports, and the EnvoyFilters name its gRPC port.
	var grpcServicePort int
	for _, p := range w.svc.Spec.Ports {
		target, ok := w.portNumber(p.TargetPort)
		if !ok {
			t.Errorf("%s: targetPort %v of port %s does not resolve", tapService, p.TargetPort, p.Name)
			continue
		}
		switch target {
		case extProc:
			if p.AppProtocol != "grpc" {
				t.Errorf("%s: port %s appProtocol %q, want grpc so Istio speaks HTTP/2 to the tap", tapService, p.Name, p.AppProtocol)
			}
			grpcServicePort = p.Port
		case admin:
			if p.AppProtocol != "http" {
				t.Errorf("%s: port %s appProtocol %q, want http", tapService, p.Name, p.AppProtocol)
			}
		default:
			t.Errorf("%s: port %s targets %d, which the tap does not expose", tapService, p.Name, target)
		}
	}
	if grpcServicePort == 0 {
		t.Fatalf("%s: no port targets the ext_proc container port %d", tapService, extProc)
	}
	wantCluster := fmt.Sprintf("outbound|%d||%s.%s.svc.cluster.local", grpcServicePort, w.svc.Metadata.Name, w.namespace)
	if wantCluster != istioTapTarget {
		t.Errorf("Service renders cluster %q but the EnvoyFilter tests expect %q", wantCluster, istioTapTarget)
	}
	for file, ef := range loadEnvoyFilters(t) {
		for _, p := range ef.Spec.ConfigPatches {
			if got := p.Patch.Value.TypedConfig.GrpcService.EnvoyGrpc.ClusterName; got != wantCluster {
				t.Errorf("%s: cluster_name %q, want %q", file, got, wantCluster)
			}
		}
	}
}

// listenPort returns the port of a host:port listen address, or -1.
func listenPort(addr string) int {
	i := strings.LastIndex(addr, ":")
	if i < 0 {
		return -1
	}
	n, err := strconv.Atoi(addr[i+1:])
	if err != nil {
		return -1
	}
	return n
}

// The sidecar must outlive the tap's shutdown so the final Kafka flush gets through, and
// Istio requires its drain to end before the pod's grace period does.
func TestTapTerminationBudget(t *testing.T) {
	w := loadTap(t)
	raw, ok := w.deploy.Spec.Template.Metadata.Annotations["proxy.istio.io/config"]
	if !ok {
		t.Fatal("pod template has no proxy.istio.io/config annotation")
	}
	var proxy struct {
		HoldApplicationUntilProxyStarts bool   `json:"holdApplicationUntilProxyStarts"`
		TerminationDrainDuration        string `json:"terminationDrainDuration"`
	}
	if err := yaml.Unmarshal([]byte(raw), &proxy); err != nil {
		t.Fatalf("proxy.istio.io/config: %v", err)
	}
	if !proxy.HoldApplicationUntilProxyStarts {
		t.Error("holdApplicationUntilProxyStarts must be true: the tap pings Kafka through the sidecar at startup")
	}
	drain, err := time.ParseDuration(proxy.TerminationDrainDuration)
	if err != nil {
		t.Fatalf("terminationDrainDuration %q: %v", proxy.TerminationDrainDuration, err)
	}
	// The budget in cmd/shadowtap-tap: 12s gRPC stop + 7s drain + 7s flush + 2s admin.
	const tapShutdownBudget = 28 * time.Second
	grace := time.Duration(w.deploy.Spec.Template.Spec.TerminationGracePeriodSeconds) * time.Second
	if drain < tapShutdownBudget {
		t.Errorf("terminationDrainDuration %v is shorter than the tap's shutdown budget %v: the flush could find the sidecar gone", drain, tapShutdownBudget)
	}
	if drain >= grace {
		t.Errorf("terminationDrainDuration %v must be below terminationGracePeriodSeconds %v", drain, grace)
	}
	if grace < tapShutdownBudget+2*time.Second {
		t.Errorf("terminationGracePeriodSeconds %v leaves no margin over the shutdown budget %v", grace, tapShutdownBudget)
	}
}

// Applying the example Secret by mistake must not yield a working tap with a guessable key.
func TestSecretExampleCannotStartATap(t *testing.T) {
	var s struct {
		Kind       string            `json:"kind"`
		Metadata   meta              `json:"metadata"`
		StringData map[string]string `json:"stringData"`
	}
	decode(t, scrubSecretExample, &s)
	if s.Kind != "Secret" {
		t.Fatalf("kind %q", s.Kind)
	}
	key, ok := s.StringData["scrub-key"]
	if !ok {
		t.Fatal("no scrub-key entry")
	}
	if len(key) >= scrub.MinKeyBytes {
		t.Errorf("placeholder key has %d bytes; it must stay below %d so the tap refuses it", len(key), scrub.MinKeyBytes)
	}
	var w tap
	decode(t, tapDeployment, &w.deploy)
	for _, c := range w.deploy.Spec.Template.Spec.Containers {
		_ = c // the Deployment reads SHADOWTAP_SCRUB_KEY from this Secret; its name is checked below
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", tapDeployment))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "name: "+s.Metadata.Name) {
		t.Errorf("%s does not reference Secret %q", tapDeployment, s.Metadata.Name)
	}
}

// Replayed traffic must not be captured again: no replay target may carry the labels an
// EnvoyFilter selects, or a replay would feed its own requests back into the topic.
func TestReplayTargetsAreNotCaptured(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", replayManifestsGlob))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Skipf("no manifests match %s", replayManifestsGlob)
	}
	filters := loadEnvoyFilters(t)
	checked := 0
	for _, f := range files {
		rel, _ := filepath.Rel(filepath.Join("..", ".."), f)
		for _, doc := range readDocs(t, rel) {
			var d deployment
			if err := json.Unmarshal(doc, &d); err != nil {
				t.Fatalf("%s: %v", rel, err)
			}
			if d.Kind != "Deployment" {
				continue
			}
			checked++
			for file, ef := range filters {
				if ef.Metadata.Namespace == d.Metadata.Namespace && subset(ef.Spec.WorkloadSelector.Labels, d.Spec.Template.Metadata.Labels) {
					t.Errorf("%s: Deployment %s matches the workloadSelector of %s; replays against it would be captured", rel, d.Metadata.Name, file)
				}
			}
		}
	}
	if checked == 0 {
		t.Skip("no Deployments among the replay manifests")
	}
}

// Every kustomization under deploy/ renders (kubectl kustomize is offline), when kubectl is
// available. CI runs the same check with the Makefile's explicit list.
func TestKustomizationsRender(t *testing.T) {
	kubectl, err := exec.LookPath("kubectl")
	if err != nil {
		t.Skip("kubectl not on PATH")
	}
	root := filepath.Join("..", "..", "deploy")
	var dirs []string
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && d.Name() == "kustomization.yaml" {
			dirs = append(dirs, filepath.Dir(path))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) == 0 {
		t.Fatal("no kustomization.yaml under deploy/")
	}
	for _, dir := range dirs {
		t.Run(filepath.ToSlash(strings.TrimPrefix(dir, "../../")), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, kubectl, "kustomize", dir).CombinedOutput()
			if err != nil {
				t.Fatalf("kubectl kustomize %s: %v\n%s", dir, err, out)
			}
			if !strings.Contains(string(out), "kind:") {
				t.Errorf("kubectl kustomize %s rendered no resources", dir)
			}
		})
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
