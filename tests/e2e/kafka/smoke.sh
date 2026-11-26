#!/bin/sh
set -eu

here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo=$(CDPATH='' cd -- "$here/../../.." && pwd)
run_dir=$(mktemp -d "${TMPDIR:-/tmp}/chaos-kafka-XXXXXXXX")
project=$(basename "$run_dir" | tr '[:upper:]' '[:lower:]')

compose() {
    docker compose -f "$here/compose.yaml" -p "$project" "$@"
}

cleanup() {
    status=$?
    trap - EXIT
    if [ "$status" -ne 0 ]; then
        compose --profile '*' logs --no-color --tail 80 >&2 || true
    fi
    compose --profile '*' down --volumes --remove-orphans || status=$?
    rmdir "$run_dir" || status=$?
    exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [ -z "${CHAOS_PROXY_IMAGE:-}" ]; then
    docker build --tag chaos-proxy:kafka-smoke "$repo"
fi

compose up --detach --wait --wait-timeout 90

kafka() {
    tool=$1
    shift
    compose run --rm --no-deps -T client timeout 45 "/opt/kafka/bin/$tool.sh" \
        --bootstrap-server adapter:19092 "$@"
}

topic=chaos-proxy-smoke
payload=kafka-through-chaos-proxy

metadata=$(kafka kafka-broker-api-versions)
if ! printf '%s\n' "$metadata" | grep -q '^adapter:19092 (id:'; then
    printf 'Expected Kafka metadata to advertise adapter:19092.\n%s\n' "$metadata" >&2
    exit 1
fi

kafka kafka-topics --create --topic "$topic" --partitions 1 --replication-factor 1
printf '%s\n' "$payload" | kafka kafka-console-producer --topic "$topic"
received=$(kafka kafka-console-consumer --topic "$topic" --from-beginning \
    --max-messages 1 --timeout-ms 15000)

if [ "$received" != "$payload" ]; then
    printf 'Expected message "%s", received "%s".\n' "$payload" "$received" >&2
    exit 1
fi

proxy_metrics() {
    compose exec -T broker wget -q -Y off -T 5 -O - http://proxy:9090/metrics
}

metric_value() {
    printf '%s\n' "$2" | awk -v metric="$1" '$1 == metric {print $2}'
}

metrics_before=$(proxy_metrics)
delay_count=$(metric_value 'chaos_proxy_injections_total{direction="request",fault_type="delay"}' "$metrics_before")
if [ -z "$delay_count" ] || [ "$delay_count" -le 0 ]; then
    printf 'Expected the proxy to inject a request delay for Kafka traffic.\n' >&2
    exit 1
fi

reset_metric='chaos_proxy_tcp_connections_total{outcome="reset"}'
reset_before=$(metric_value "$reset_metric" "$metrics_before")
compose up --detach --wait --wait-timeout 90 adapter-reset
if rejected=$(compose run --rm --no-deps -T client timeout 12 \
    /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server adapter-reset:19092 2>&1); then
    printf 'Expected Kafka bootstrap to fail through adapter-reset:19092.\n%s\n' "$rejected" >&2
    exit 1
fi
metrics_after=$(proxy_metrics)
reset_after=$(metric_value "$reset_metric" "$metrics_after")
if [ -z "$reset_before" ] || [ -z "$reset_after" ] || [ "$reset_after" -le "$reset_before" ]; then
    printf 'Expected the proxy TCP reset counter to increase. Before: %s. After: %s.\n%s\n' \
        "$reset_before" "$reset_after" "$rejected" >&2
    exit 1
fi

printf 'Kafka smoke test passed. Metadata advertised adapter:19092, the consumer received "%s", %s request delays were injected, and the reset counter increased from %s to %s.\n' \
    "$received" "$delay_count" "$reset_before" "$reset_after"
