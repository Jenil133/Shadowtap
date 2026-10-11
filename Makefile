# Shadowtap developer tasks. `make` (or `make help`) lists them.
# Works with GNU Make 3.81 (macOS) and newer.

GO           ?= go
DOCKER       ?= docker
KUBECTL      ?= kubectl
COMPOSE      ?= $(DOCKER) compose -f deploy/local/compose.yaml
BIN_DIR      ?= bin
COVERPROFILE ?= coverage.out
FUZZTIME     ?= 30s
IMAGE_PREFIX ?= ghcr.io/jenil133
IMAGE_TAG    ?= dev
VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# Every binary; make build compiles them all. The ones that ship as images are built by
# make docker and the CI docker matrix under the same names the manifests and compose use:
# ghcr.io/jenil133/{shadowtap-tap,shadowtap,shadowtap-indexer,shadowtap-demo-api}.
CMDS    := ./cmd/shadowtap-tap ./cmd/shadowtap ./cmd/shadowtap-indexer ./examples/demo/cmd/demo-api ./examples/demo/cmd/loadgen
LDFLAGS := -X main.version=$(VERSION)

# Every fuzz target in the tree as <package>:<FuzzName>. `go test -fuzz` runs one target per
# invocation, so make fuzz loops over this list, and make fuzz-list fails when the list and
# the tree disagree: a target added without an entry here would otherwise never be fuzzed.
FUZZ_TARGETS := \
	./internal/tracectx:FuzzParseTraceparent \
	./internal/tracectx:FuzzParseB3 \
	./internal/scrub:FuzzScrubJSON \
	./internal/scrub:FuzzScrubText \
	./internal/scrub:FuzzScrubPath \
	./internal/scrub:FuzzScrubHeaderValue \
	./internal/tap:FuzzServiceFromAuthority \
	./internal/tap:FuzzProcess \
	./internal/kafka:FuzzParseStart \
	./internal/diff:FuzzDiffJSON \
	./internal/diff:FuzzDiffText \
	./internal/diff:FuzzPointerPattern \
	./internal/diff:FuzzRouteTemplate \
	./examples/demo/api:FuzzHandler

# Kustomizations that must render. Listed rather than discovered, so a renamed or deleted
# directory fails make kustomize (and CI) instead of silently dropping out of validation.
KUSTOMIZATIONS := deploy/k8s/base deploy/k8s/demo deploy/k8s/replay deploy/k8s/indexer deploy/istio deploy/otel-collector

# $(call build_image,<name>,<package>) builds one image from the shared Dockerfile.
define build_image
	$(DOCKER) build --build-arg CMD=$(2) --build-arg VERSION=$(VERSION) -t $(IMAGE_PREFIX)/$(1):$(IMAGE_TAG) .
endef

.DEFAULT_GOAL := help

.PHONY: help build test race cover fuzz fuzz-list vet fmt lint tidy docker kustomize manifests collector up down logs e2e e2e-native replay clean

help: ## List the available targets
	@echo "Usage: make <target> [VAR=value]"
	@echo
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-11s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo
	@echo "Variables: VERSION=$(VERSION) FUZZTIME=$(FUZZTIME) IMAGE_PREFIX=$(IMAGE_PREFIX) IMAGE_TAG=$(IMAGE_TAG)"

build: ## Build all binaries into bin/
	@mkdir -p $(BIN_DIR)
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/ $(CMDS)

test: ## Run the tests
	$(GO) test ./...

race: ## Run the tests with the race detector, uncached
	$(GO) test -race -count=1 ./...

cover: ## Write a coverage profile and print the per-function summary
	$(GO) test -count=1 -covermode=atomic -coverprofile=$(COVERPROFILE) ./...
	$(GO) tool cover -func=$(COVERPROFILE)

fuzz: fuzz-list ## Run every fuzz target for FUZZTIME each (default 30s)
	@for t in $(FUZZ_TARGETS); do \
		pkg=$${t%%:*}; name=$${t##*:}; \
		echo "$(GO) test -run='^$$' -fuzz='^$$name$$' -fuzztime=$(FUZZTIME) $$pkg"; \
		$(GO) test -run='^$$' -fuzz="^$${name}\$$" -fuzztime=$(FUZZTIME) "$$pkg" || exit 1; \
	done

fuzz-list: ## Fail when FUZZ_TARGETS and the fuzz targets in the tree differ
	@status=0; \
	found=$$(grep -rHo '^func Fuzz[A-Za-z0-9_]*' --include='*_test.go' . | sed -E 's#^(\./)?(.*)/[^/]+\.go:func #./\2:#' | sort -u); \
	for t in $$found; do \
		case " $(FUZZ_TARGETS) " in \
		*" $$t "*) ;; \
		*) echo "fuzz target $$t is not listed in FUZZ_TARGETS (Makefile)"; status=1 ;; \
		esac; \
	done; \
	for t in $(FUZZ_TARGETS); do \
		pkg=$${t%%:*}; name=$${t##*:}; \
		$(GO) test -list="^$${name}\$$" "$$pkg" 2>/dev/null | grep -qx "$$name" || { echo "fuzz target $$name not found in $$pkg"; status=1; }; \
	done; \
	[ "$$status" -eq 0 ] && echo "FUZZ_TARGETS matches the tree"; exit $$status

vet: ## Run go vet
	$(GO) vet ./...

fmt: ## Fail if any Go file is not gofmt-formatted
	@out="$$(gofmt -l .)"; \
	if [ -n "$$out" ]; then \
		echo "These files need gofmt (run: gofmt -w .):"; echo "$$out"; exit 1; \
	fi

lint: vet ## go vet, plus staticcheck when it is installed
	@if command -v staticcheck >/dev/null 2>&1; then \
		echo "staticcheck ./..."; staticcheck ./...; \
	else \
		echo "staticcheck not installed, skipped (go install honnef.co/go/tools/cmd/staticcheck@latest)"; \
	fi

tidy: ## Tidy go.mod and go.sum
	$(GO) mod tidy

docker: ## Build the tap, CLI, indexer and demo-api images (IMAGE_PREFIX/<name>:IMAGE_TAG)
	$(call build_image,shadowtap-tap,./cmd/shadowtap-tap)
	$(call build_image,shadowtap,./cmd/shadowtap)
	$(call build_image,shadowtap-indexer,./cmd/shadowtap-indexer)
	$(call build_image,shadowtap-demo-api,./examples/demo/cmd/demo-api)

kustomize: ## Render every kustomization under deploy/ (needs kubectl)
	@for dir in $(KUSTOMIZATIONS); do \
		[ -f "$$dir/kustomization.yaml" ] || { echo "$$dir/kustomization.yaml is missing"; exit 1; }; \
		echo "$(KUBECTL) kustomize $$dir"; \
		$(KUBECTL) kustomize "$$dir" >/dev/null || exit 1; \
	done

manifests: kustomize ## Render the kustomizations and check the manifests against Envoy's API and each other
	$(GO) test -count=1 ./test/manifests

collector: ## Validate the OpenTelemetry Collector configuration with the Collector image (needs docker)
	scripts/validate-otel-collector.sh deploy/otel-collector

up: ## Start the local stack (Kafka, tap, demo-api, Envoy) and wait until it is up
	$(COMPOSE) up -d --build --wait

down: ## Stop the local stack and delete its data
	$(COMPOSE) down -v --remove-orphans

logs: ## Follow the local stack's logs (SERVICE=tap for one service)
	$(COMPOSE) logs -f --tail=100 $(SERVICE)

e2e: ## Run the end-to-end test against the local stack (needs Docker)
	bash scripts/e2e-local.sh

e2e-native: ## Run the end-to-end test without Docker (needs envoy on PATH)
	bash scripts/e2e-native.sh

REPLAY_FROM ?= -15m
REPLAY_SPEED ?= 4

replay: build ## Replay the local stack's captures to the v2 candidate with the v1 baseline (after make up and traffic)
	@mkdir -p .e2e
	SHADOWTAP_SCRUB_KEY=local-dev-scrub-key-not-for-production $(BIN_DIR)/shadowtap replay \
		--brokers localhost:29092 --from $(REPLAY_FROM) --to now --speed $(REPLAY_SPEED) \
		--target http://localhost:8082 --baseline http://localhost:8081 \
		--tap-config deploy/local/tap.yaml --report-html .e2e/replay.html --publish-diffs

clean: ## Remove build, coverage and e2e output
	rm -rf $(BIN_DIR) .e2e $(COVERPROFILE)
