#!/usr/bin/env bash
#
# Validates the OpenTelemetry Collector configuration shipped under deploy/otel-collector with
# the Collector's own `validate` command, run from the Collector image the manifests deploy,
# so a misspelled receiver, processor or exporter setting fails here and not at pod start.
#
# The kustomization is rendered; scripts/otelconfig then writes every ConfigMap value that is
# a Collector configuration (a YAML mapping with a service: section and receivers: or
# exporters:) to a file and names the image of the container that runs the Collector. Values
# such as ${env:KAFKA_BROKERS} resolve from the environment of the validate run: give them
# defaults (${env:KAFKA_BROKERS:-kafka:9092}) or export them before running this.
#
# Usage: scripts/validate-otel-collector.sh [dir]   (default: deploy/otel-collector)
#
# Requires kubectl (to render), go (to run scripts/otelconfig) and docker (to run the
# Collector). OTEL_COLLECTOR_IMAGE overrides the image found in the manifests.

set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

dir=${1:-deploy/otel-collector}
default_image=otel/opentelemetry-collector-contrib:latest

log() { printf '==> %s\n' "$*"; }
warn() { printf 'WARN: %s\n' "$*" >&2; }
die() {
	printf 'ERROR: %s\n' "$*" >&2
	exit 1
}

[ -f "$dir/kustomization.yaml" ] || die "$dir/kustomization.yaml not found"
for tool in kubectl go docker; do
	command -v "$tool" >/dev/null 2>&1 || die "$tool is required"
done

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

log "Rendering $dir"
kubectl kustomize "$dir" >"$work/rendered.yaml"

image=$(go run ./scripts/otelconfig -out "$work/configs" <"$work/rendered.yaml") ||
	die "no Collector configuration could be extracted from $dir"
if [ -n "${OTEL_COLLECTOR_IMAGE:-}" ]; then
	image=$OTEL_COLLECTOR_IMAGE
elif [ -z "$image" ]; then
	image=$default_image
	warn "no Collector container image in the rendered manifests; validating with $image"
fi

status=0
for f in "$work"/configs/*; do
	[ -e "$f" ] || die "scripts/otelconfig wrote no configuration files"
	name=${f##*/}
	log "Validating ${name%%--*}/${name#*--} with $image"
	# Mounted read-only under the path the Collector image documents; the container runs the
	# validate subcommand only and exits.
	if docker run --rm -v "$f:/etc/otelcol-contrib/config.yaml:ro" "$image" validate --config=/etc/otelcol-contrib/config.yaml; then
		log "${name%%--*}/${name#*--}: OK"
	else
		printf 'FAIL: %s/%s is not a valid Collector configuration for %s\n' "${name%%--*}" "${name#*--}" "$image"
		status=1
	fi
done
[ "$status" -eq 0 ] || exit "$status"
log "Collector configuration OK"
