package manifests

// Tooling coherence. The Makefile, the CI workflow, the compose stack, the Dockerfile and
// the Kubernetes manifests each keep their own list of binaries, images, fuzz targets or
// kustomizations. Cross-checking the lists here means one that falls behind fails go test
// instead of silently leaving a fuzz target unfuzzed, an image unbuilt or a kustomization
// unrendered.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

const (
	makefilePath = "Makefile"
	dockerfile   = "Dockerfile"
	ciWorkflow   = ".github/workflows/ci.yml"
	composeFile  = "deploy/local/compose.yaml"
	// imagePrefix is the registry path every Shadowtap image is published under.
	imagePrefix = "ghcr.io/jenil133/"
)

// repoRoot is the repository root relative to this package.
var repoRoot = filepath.Join("..", "..")

// skipDirs hold no source: build output, e2e artifacts and local notes.
var skipDirs = map[string]bool{".git": true, "bin": true, ".e2e": true, "dist": true, "phases": true, "testdata": true}

var (
	fuzzFunc = regexp.MustCompile(`(?m)^func (Fuzz[A-Za-z0-9_]*)\(`)
	// Recipe lines only (tab-indented), so the macro's own comment does not count.
	buildImageCall = regexp.MustCompile(`(?m)^\t\$\(call build_image,([^,)]+),([^)]+)\)`)
	dockerfileCmd  = regexp.MustCompile(`(?m)^ARG CMD=(\S+)`)
	packageMain    = regexp.MustCompile(`(?m)^package main\b`)
)

func readRepoFile(t *testing.T, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot, rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// makeVariable returns the words assigned to a Makefile variable with :=, ?= or =, with
// backslash continuations joined.
func makeVariable(t *testing.T, name string) []string {
	t.Helper()
	src := strings.ReplaceAll(readRepoFile(t, makefilePath), "\\\n", " ")
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + `[ \t]*[:?]?=[ \t]*(.*)$`)
	m := re.FindStringSubmatch(src)
	if m == nil {
		t.Fatalf("%s: no variable %s", makefilePath, name)
	}
	return strings.Fields(m[1])
}

// dockerImages returns the images make docker builds: image name -> Go package.
func dockerImages(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, m := range buildImageCall.FindAllStringSubmatch(readRepoFile(t, makefilePath), -1) {
		out[strings.TrimSpace(m[1])] = strings.TrimSpace(m[2])
	}
	if len(out) == 0 {
		t.Fatalf("%s: the docker target has no $(call build_image,...) lines", makefilePath)
	}
	return out
}

// walkRepo calls fn with the repository-relative, slash-separated path of every file under
// the tree, skipping skipDirs.
func walkRepo(t *testing.T, fn func(rel string, d os.DirEntry)) {
	t.Helper()
	err := filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != repoRoot && skipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		fn(filepath.ToSlash(rel), d)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// diffSets reports the elements of want missing from got and the extra ones in got.
func diffSets(want, got []string) (missing, extra []string) {
	for _, w := range want {
		if !slices.Contains(got, w) {
			missing = append(missing, w)
		}
	}
	for _, g := range got {
		if !slices.Contains(want, g) {
			extra = append(extra, g)
		}
	}
	return missing, extra
}

// `go test -fuzz` runs one target per invocation, so make fuzz works from a list; a Fuzz
// function missing from it is never fuzzed, and a listed one that is gone fails make fuzz.
func TestMakefileListsEveryFuzzTarget(t *testing.T) {
	var inTree []string
	walkRepo(t, func(rel string, _ os.DirEntry) {
		if !strings.HasSuffix(rel, "_test.go") {
			return
		}
		src := readRepoFile(t, rel)
		for _, m := range fuzzFunc.FindAllStringSubmatch(src, -1) {
			inTree = append(inTree, "./"+filepath.ToSlash(filepath.Dir(rel))+":"+m[1])
		}
	})
	sort.Strings(inTree)
	if len(inTree) == 0 {
		t.Fatal("no Fuzz functions found in the tree")
	}
	listed := makeVariable(t, "FUZZ_TARGETS")
	missing, extra := diffSets(inTree, listed)
	for _, m := range missing {
		t.Errorf("fuzz target %s is not in FUZZ_TARGETS (%s)", m, makefilePath)
	}
	for _, e := range extra {
		t.Errorf("FUZZ_TARGETS lists %s, which is not in the tree", e)
	}
}

// make kustomize (and CI through it) renders a fixed list, so a renamed or deleted
// directory fails there rather than drops out of validation; a new kustomization must
// join the list, which is what this checks.
func TestMakefileListsEveryKustomization(t *testing.T) {
	var inTree []string
	walkRepo(t, func(rel string, d os.DirEntry) {
		if strings.HasPrefix(rel, "deploy/") && d.Name() == "kustomization.yaml" {
			inTree = append(inTree, filepath.ToSlash(filepath.Dir(rel)))
		}
	})
	sort.Strings(inTree)
	if len(inTree) == 0 {
		t.Fatal("no kustomization.yaml under deploy/")
	}
	listed := makeVariable(t, "KUSTOMIZATIONS")
	missing, _ := diffSets(inTree, listed)
	for _, m := range missing {
		t.Errorf("%s/kustomization.yaml is not in KUSTOMIZATIONS (%s)", m, makefilePath)
	}
}

// Every command under cmd/ and examples/demo/cmd/ is built by make build, and every listed
// command exists and is a main package.
func TestMakefileBuildsEveryCommand(t *testing.T) {
	var inTree []string
	for _, pattern := range []string{"cmd/*", "examples/demo/cmd/*"} {
		dirs, err := filepath.Glob(filepath.Join(repoRoot, pattern))
		if err != nil {
			t.Fatal(err)
		}
		for _, dir := range dirs {
			if info, err := os.Stat(dir); err != nil || !info.IsDir() {
				continue
			}
			rel, _ := filepath.Rel(repoRoot, dir)
			inTree = append(inTree, "./"+filepath.ToSlash(rel))
		}
	}
	sort.Strings(inTree)
	cmds := makeVariable(t, "CMDS")
	missing, extra := diffSets(inTree, cmds)
	for _, m := range missing {
		t.Errorf("%s is not in CMDS (%s)", m, makefilePath)
	}
	for _, e := range extra {
		t.Errorf("CMDS lists %s, which is not a command directory", e)
	}
	for _, pkg := range cmds {
		files, err := filepath.Glob(filepath.Join(repoRoot, filepath.FromSlash(pkg), "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		isMain := false
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			data, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			isMain = isMain || packageMain.Match(data)
		}
		if !isMain {
			t.Errorf("CMDS entry %s is not a main package", pkg)
		}
	}
}

// buildArgs decodes compose build args, which may be a mapping or a list of KEY=VALUE.
func buildArgs(t *testing.T, raw json.RawMessage) map[string]string {
	t.Helper()
	out := map[string]string{}
	if len(raw) == 0 {
		return out
	}
	var asMap map[string]string
	if err := json.Unmarshal(raw, &asMap); err == nil {
		return asMap
	}
	var asList []string
	if err := json.Unmarshal(raw, &asList); err != nil {
		t.Fatalf("%s: build args %s are neither a mapping nor a list", composeFile, raw)
	}
	for _, kv := range asList {
		k, v, _ := strings.Cut(kv, "=")
		out[k] = v
	}
	return out
}

// imageName splits a Shadowtap image reference into its name and tag; ok is false for
// images from elsewhere.
func imageName(ref string) (name, tag string, ok bool) {
	if !strings.HasPrefix(ref, imagePrefix) {
		return "", "", false
	}
	name, tag, _ = strings.Cut(strings.TrimPrefix(ref, imagePrefix), ":")
	return name, tag, true
}

// imageRefs returns every "image" value in the YAML documents of a file, in order.
func imageRefs(t *testing.T, rel string) []string {
	t.Helper()
	var refs []string
	var visit func(v any)
	visit = func(v any) {
		switch node := v.(type) {
		case map[string]any:
			for k, child := range node {
				if s, ok := child.(string); ok && k == "image" {
					refs = append(refs, s)
					continue
				}
				visit(child)
			}
		case []any:
			for _, child := range node {
				visit(child)
			}
		}
	}
	for _, doc := range readDocs(t, rel) {
		var v any
		if err := json.Unmarshal(doc, &v); err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		visit(v)
	}
	return refs
}

// The same images, built from the same packages, everywhere: make docker, the CI matrix,
// the compose stack and the Kubernetes manifests.
func TestImagesAgreeAcrossTooling(t *testing.T) {
	images := dockerImages(t)
	cmds := makeVariable(t, "CMDS")
	for name, pkg := range images {
		if !slices.Contains(cmds, pkg) {
			t.Errorf("make docker builds %s from %s, which is not in CMDS", name, pkg)
		}
	}
	if prefix := makeVariable(t, "IMAGE_PREFIX"); len(prefix) != 1 || prefix[0]+"/" != imagePrefix {
		t.Errorf("IMAGE_PREFIX %v, want %s", prefix, strings.TrimSuffix(imagePrefix, "/"))
	}
	defaultTag := makeVariable(t, "IMAGE_TAG")
	if len(defaultTag) != 1 {
		t.Fatalf("IMAGE_TAG %v, want one word", defaultTag)
	}

	t.Run("dockerfile", func(t *testing.T) {
		m := dockerfileCmd.FindStringSubmatch(readRepoFile(t, dockerfile))
		if m == nil {
			t.Fatalf("%s: no ARG CMD default", dockerfile)
		}
		if !slices.Contains(cmds, m[1]) {
			t.Errorf("%s: default CMD %s is not in CMDS", dockerfile, m[1])
		}
	})

	t.Run("ci matrix", func(t *testing.T) {
		var wf struct {
			Jobs map[string]struct {
				Strategy struct {
					Matrix struct {
						Include []struct {
							Image string `json:"image"`
							Cmd   string `json:"cmd"`
						} `json:"include"`
					} `json:"matrix"`
				} `json:"strategy"`
			} `json:"jobs"`
		}
		decode(t, ciWorkflow, &wf)
		job, ok := wf.Jobs["docker"]
		if !ok {
			t.Fatalf("%s: no docker job", ciWorkflow)
		}
		inCI := map[string]string{}
		for _, inc := range job.Strategy.Matrix.Include {
			inCI[inc.Image] = inc.Cmd
		}
		for name, pkg := range images {
			if got, ok := inCI[name]; !ok {
				t.Errorf("%s: the docker matrix does not build %s", ciWorkflow, name)
			} else if got != pkg {
				t.Errorf("%s: the docker matrix builds %s from %s, make docker from %s", ciWorkflow, name, got, pkg)
			}
		}
		for name := range inCI {
			if _, ok := images[name]; !ok {
				t.Errorf("%s: the docker matrix builds %s, which make docker does not", ciWorkflow, name)
			}
		}
	})

	t.Run("compose", func(t *testing.T) {
		var compose struct {
			Services map[string]struct {
				Image string `json:"image"`
				Build *struct {
					Args json.RawMessage `json:"args"`
				} `json:"build"`
			} `json:"services"`
		}
		decode(t, composeFile, &compose)
		for svc, s := range compose.Services {
			name, tag, ok := imageName(s.Image)
			if !ok {
				continue
			}
			pkg, known := images[name]
			if !known {
				t.Errorf("%s: service %s uses image %s, which make docker does not build", composeFile, svc, name)
				continue
			}
			if tag != defaultTag[0] {
				t.Errorf("%s: service %s uses tag %q; make docker tags %q", composeFile, svc, tag, defaultTag[0])
			}
			if s.Build == nil {
				t.Errorf("%s: service %s has no build section, so make up cannot build %s", composeFile, svc, name)
				continue
			}
			if cmd := buildArgs(t, s.Build.Args)["CMD"]; cmd != pkg {
				t.Errorf("%s: service %s builds %s from %q, make docker from %q", composeFile, svc, name, cmd, pkg)
			}
		}
	})

	t.Run("kubernetes", func(t *testing.T) {
		checked := 0
		walkRepo(t, func(rel string, _ os.DirEntry) {
			if !strings.HasPrefix(rel, "deploy/") || strings.HasPrefix(rel, "deploy/local/") || !strings.HasSuffix(rel, ".yaml") {
				return
			}
			for _, ref := range imageRefs(t, rel) {
				name, _, ok := imageName(ref)
				if !ok {
					continue
				}
				checked++
				if _, known := images[name]; !known {
					t.Errorf("%s: image %s is not built by make docker or the CI matrix", rel, ref)
				}
			}
		})
		if checked == 0 {
			t.Error("no Shadowtap image referenced by any manifest under deploy/")
		}
	})
}

// The workflow runs the Makefile's checks rather than its own copies, so the lists above
// are the only ones to maintain.
func TestCIRunsTheTooling(t *testing.T) {
	var wf struct {
		Jobs map[string]struct {
			Steps []struct {
				Run string `json:"run"`
			} `json:"steps"`
		} `json:"jobs"`
	}
	decode(t, ciWorkflow, &wf)
	var runs []string
	for _, job := range wf.Jobs {
		for _, step := range job.Steps {
			runs = append(runs, step.Run)
		}
	}
	all := strings.Join(runs, "\n")
	for _, tt := range []struct{ want, why string }{
		{"go test -race", "the race detector must run in CI"},
		{"make manifests", "renders every kustomization and runs this package"},
		{"make fuzz", "runs every fuzz target and fails on a stale FUZZ_TARGETS"},
		{"make collector", "validates the Collector configuration with the Collector"},
		{"make e2e", "the only test with a real Envoy and a real Kafka"},
	} {
		if !strings.Contains(all, tt.want) {
			t.Errorf("%s: no step runs %q (%s)", ciWorkflow, tt.want, tt.why)
		}
	}
}
