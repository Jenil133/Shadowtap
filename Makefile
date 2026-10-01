# Shadowtap developer tasks. `make` (or `make help`) lists them.
# Works with GNU Make 3.81 (macOS) and newer.

GO           ?= go
DOCKER       ?= docker
COMPOSE      ?= $(DOCKER) compose -f deploy/local/compose.yaml
BIN_DIR      ?= bin
COVERPROFILE ?= coverage.out
FUZZTIME     ?= 30s
IMAGE_PREFIX ?= shadowtap
IMAGE_TAG    ?= dev
VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

CMDS    := ./cmd/shadowtap-tap ./cmd/shadowtap ./examples/demo/cmd/demo-api ./examples/demo/cmd/loadgen
LDFLAGS := -X main.version=$(VERSION)

# $(call fuzz_target,<package>,<FuzzName>) fuzzes one target; `go test -fuzz` accepts only one
# per run. It first checks that the target exists, because `go test -fuzz` passes silently
# when the pattern matches nothing. -run='^$' skips the package's unit tests.
define fuzz_target
	@$(GO) test -list='^$(2)$$' $(1) | grep -qx '$(2)' || { echo "fuzz target $(2) not found in $(1)" >&2; exit 1; }
	$(GO) test -run='^$$' -fuzz='^$(2)$$' -fuzztime=$(FUZZTIME) $(1)
endef

.DEFAULT_GOAL := help

.PHONY: help build test race cover fuzz vet fmt lint tidy docker up down logs e2e e2e-native clean

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

fuzz: ## Run every fuzz target for FUZZTIME each (default 30s)
	$(call fuzz_target,./internal/tracectx,FuzzParseTraceparent)
	$(call fuzz_target,./internal/tracectx,FuzzParseB3)
	$(call fuzz_target,./internal/scrub,FuzzScrubJSON)
	$(call fuzz_target,./internal/scrub,FuzzScrubText)
	$(call fuzz_target,./internal/scrub,FuzzScrubPath)
	$(call fuzz_target,./internal/scrub,FuzzScrubHeaderValue)
	$(call fuzz_target,./internal/tap,FuzzServiceFromAuthority)
	$(call fuzz_target,./internal/tap,FuzzProcess)
	$(call fuzz_target,./internal/kafka,FuzzParseStart)
	$(call fuzz_target,./examples/demo/api,FuzzHandler)

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

docker: ## Build the shadowtap-tap and demo-api images (shadowtap/<name>:dev)
	$(DOCKER) build --build-arg CMD=./cmd/shadowtap-tap --build-arg VERSION=$(VERSION) \
		-t $(IMAGE_PREFIX)/shadowtap-tap:$(IMAGE_TAG) .
	$(DOCKER) build --build-arg CMD=./examples/demo/cmd/demo-api --build-arg VERSION=$(VERSION) \
		-t $(IMAGE_PREFIX)/demo-api:$(IMAGE_TAG) .

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

clean: ## Remove build, coverage and e2e output
	rm -rf $(BIN_DIR) .e2e $(COVERPROFILE)
