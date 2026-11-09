#!/usr/bin/env bash
set -euo pipefail

bench_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
backend=${1:-both}
if [[ $# -gt 0 ]]; then shift; fi
case "$backend" in weave|electric|drasi|weave-embedded|weave-remote) backends=("$backend");; topology) backends=(weave-embedded weave-remote);; both) backends=(weave electric);; all) backends=(weave electric drasi);; *) echo "usage: $0 weave|electric|drasi|both|all|topology|weave-embedded|weave-remote [runner flags]" >&2; exit 2;; esac
for bench_arg in "$@"; do
  case "$bench_arg" in --backend|--backend=*|-backend|-backend=*|--grpc|--grpc=*|-grpc|-grpc=*|--electric|--electric=*|-electric|-electric=*|--drasi|--drasi=*|-drasi|-drasi=*|--drasi-events|--drasi-events=*|-drasi-events|-drasi-events=*)
    echo "run.sh owns backend/endpoints; configure workloads only" >&2; exit 2;;
  esac
done

# Every invocation owns new containers/volumes. Cleanup cannot target the demo
# Compose project or a caller-supplied existing project name.
bench_run="$(date -u +%Y%m%dt%H%M%Sz)-$$"
bench_results="$bench_root/results/$bench_run"
mkdir -p "$bench_results"
bench_project=""
bench_sampler=""
cleanup() {
  if [[ -n "$bench_sampler" ]]; then kill -TERM "$bench_sampler" 2>/dev/null || true; wait "$bench_sampler" 2>/dev/null || true; bench_sampler=""; fi
  if [[ -n "$bench_project" ]]; then
    docker compose -p "$bench_project" -f "$bench_root/compose.yaml" --profile '*' logs --no-color >"$bench_results/$bench_backend/services.log" 2>&1 || true
    docker compose -p "$bench_project" -f "$bench_root/compose.yaml" --profile '*' down --volumes --remove-orphans >&2 || true
    bench_project=""
  fi
}
# Streaming docker stats emits terminal clear-screen codes even into files.
# One-shot samples are valid JSONL. Trap termination to reap our own subprocess.
sample_resources() {
  local bench_stats_pid=""
  trap 'if [[ -n "$bench_stats_pid" ]]; then kill -TERM "$bench_stats_pid" 2>/dev/null || true; wait "$bench_stats_pid" 2>/dev/null || true; fi; exit 0' TERM INT
  while :; do
    docker stats --no-stream --format '{{json .}}' "$@" &
    bench_stats_pid=$!
    wait "$bench_stats_pid" || return
    bench_stats_pid=""
  done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker compose -p weave-query-benchmark-build -f "$bench_root/compose.yaml" build weave runner >&2
for bench_backend in "${backends[@]}"; do
  if [[ "$bench_backend" == drasi ]]; then
    docker compose -p weave-query-benchmark-build -f "$bench_root/compose.yaml" build drasi >&2
  fi
done
bench_failed=0
for bench_backend in "${backends[@]}"; do
  bench_project="weave-qbench-$bench_run-$bench_backend"
  mkdir -p "$bench_results/$bench_backend"
  compose=(docker compose -p "$bench_project" -f "$bench_root/compose.yaml")
  if [[ "${BENCH_DIAGNOSTICS:-0}" == 1 ]]; then
    if [[ "$bench_backend" != weave ]]; then echo "diagnostics currently supports weave only" >&2; exit 2; fi
    export BENCH_DIAGNOSTICS_DIR="$bench_results/$bench_backend/diagnostics"
    mkdir -p "$BENCH_DIAGNOSTICS_DIR"
    compose+=(-f "$bench_root/compose.diagnostics.yaml")
  fi
  "${compose[@]}" up -d --wait postgres >&2
  # Share built images across the independently named per-backend projects.
  "${compose[@]}" run --rm --no-deps runner seed --backend "$bench_backend" "$@" >&2
  "${compose[@]}" --profile "$bench_backend" up -d --no-build "$bench_backend" >&2
  bench_ready="http://$bench_backend:8081/ready"
  if [[ "$bench_backend" == electric ]]; then bench_ready=http://electric:3000/v1/health; fi
  if [[ "$bench_backend" == drasi ]]; then bench_ready=http://drasi:8080/health; fi
  "${compose[@]}" run --rm --no-deps runner wait --url "$bench_ready" >&2
  bench_generator="${bench_project}-load"
  bench_container=$("${compose[@]}" run -d --no-deps --name "$bench_generator" runner run --backend "$bench_backend" --grpc "$bench_backend:9090" "$@")
  bench_engine=$("${compose[@]}" ps -q "$bench_backend")
  bench_postgres=$("${compose[@]}" ps -q postgres)
  bench_resources=("$bench_engine" "$bench_postgres" "$bench_container")
  if [[ "$bench_backend" == weave-remote ]]; then bench_resources+=("$("${compose[@]}" ps -q weave)"); fi
  docker inspect "${bench_resources[@]}" >"$bench_results/$bench_backend/containers.json"
  "${compose[@]}" config >"$bench_results/$bench_backend/compose.yaml"
  git -C "$bench_root" rev-parse HEAD >"$bench_results/$bench_backend/revision.txt"
  git -C "$bench_root" diff --stat >>"$bench_results/$bench_backend/revision.txt"
  git -C "$bench_root" status --short >>"$bench_results/$bench_backend/revision.txt"
  sample_resources "${bench_resources[@]}" >"$bench_results/$bench_backend/resources.jsonl" 2>"$bench_results/$bench_backend/resources-errors.log" &
  bench_sampler=$!
  bench_status=$(docker wait "$bench_container")
  docker logs "$bench_container" >"$bench_results/$bench_backend/result.json" 2>"$bench_results/$bench_backend/runner.log"
  if [[ "$bench_status" != 0 ]]; then bench_failed=1; echo "$bench_backend failed; see $bench_results/$bench_backend" >&2; fi
  cleanup
done
echo "Results: $bench_results"
exit "$bench_failed"
