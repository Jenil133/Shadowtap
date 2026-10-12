# Getting started

Everything below runs from the repository root. Go 1.26 is required; Docker is needed only for the
local stack, and an `envoy` binary only for the Docker-free end-to-end run.

## Build and test

```sh
make build        # bin/shadowtap-tap, bin/shadowtap, bin/shadowtap-indexer, bin/demo-api, bin/loadgen
make test         # unit and in-process integration tests (no Docker)
make race         # the same with the race detector
make fuzz FUZZTIME=10s
```

## End to end without Docker

A real Envoy (`brew install envoy` or [func-e](https://func-e.io)) fronts the demo service with the
tap behind both ext_proc modes, Kafka is an in-memory broker, and the replay runs against a
candidate with deliberate regressions:

```sh
make e2e-native
```

The run leaves its artifacts under `.e2e/native/`: the captured records, the load generator's
list of every personal value it sent, the replay reports and the Envoy statistics.

## The local stack

Kafka, Envoy, the tap, the demo service, the OpenTelemetry Collector, the indexer, Elasticsearch
and Kibana:

```sh
make up
```

Send traffic through Envoy (observe mode on `:10000`, inline mode on `:10001`, no tap on `:10002`)
and read the captures back:

```sh
bin/loadgen --target http://localhost:10000 --requests 200 --pii-out .e2e/pii.txt
bin/shadowtap tail --brokers localhost:29092 --from earliest --timeout 5s --limit 5
bin/shadowtap tail --brokers localhost:29092 --from earliest --timeout 5s --check-pii
```

`--check-pii` scans every stored record, including URL-decoded and JSON-decoded views, and exits
with status 3 if any detector still matches.

Replay the captured window against the candidate build (`v2`, on `:8082`) with the baseline
(`v1`, on `:8081`) for noise suppression, four times faster than it was recorded:

```sh
make replay
```

The candidate carries three deliberate regressions, so the replay names them and the gate fails:
`shadowtap replay` exits with status 1, which is what a release pipeline would act on.

Follow one captured request across Envoy, the three demo services, the tap and the stored record
(the indexer needs a few seconds to catch up after the traffic):

```sh
TRACE=$(bin/shadowtap tail --brokers localhost:29092 --from earliest --timeout 5s --limit 1 --format json | python3 -c 'import json,sys; print(json.loads(sys.stdin.readline())["trace_id"])')
bin/shadowtap trace --es http://localhost:9200 "$TRACE"
```

`make e2e-observability` runs that whole chain as a test. Kibana is on http://localhost:5601;
`deploy/kibana/import.sh` loads the data views and the dashboard. Stop everything and delete the
data with:

```sh
make down
```

## Kubernetes and Istio

The manifests render without a cluster:

```sh
make kustomize
```

| Kustomization | What it installs |
|---|---|
| `deploy/k8s/base` | the tap: Deployment, Service, ConfigMap, NetworkPolicy, PodDisruptionBudget |
| `deploy/k8s/indexer` | the indexer, reading Kafka and writing Elasticsearch |
| `deploy/otel-collector` | the OpenTelemetry Collector, OTLP in and Kafka out |
| `deploy/istio` | the `EnvoyFilter` that streams a workload's traffic to the tap, mTLS and authorization policies, the tracing provider |
| `deploy/k8s/demo` | the demo service in a captured namespace |
| `deploy/k8s/replay` | baseline and candidate deployments and a replay Job |

Install them in that order with `kubectl apply -k`, after creating the scrub-key Secret described in
`deploy/k8s/secret.example.yaml` and the Kafka topics in `deploy/k8s/kafka-topic.yaml`.

## Configuration

| File | For |
|---|---|
| `deploy/local/tap.yaml` | every tap setting with its default, documented inline |
| `deploy/local/indexer.yaml` | every indexer setting |
| `deploy/k8s/base/configmap.yaml` | the production tap configuration (5 % sampling, rate limited) |

Secrets never live in these files: the scrub key comes from `SHADOWTAP_SCRUB_KEY` or a mounted
file, Kafka and Elasticsearch credentials from their `SHADOWTAP_*` environment variables.
