#!/usr/bin/env bash
# Run the JMH harness across every backend cell, merge the per-cell JSONs into one all.json covering all clients, and print a summary.
# Requires Docker (and jq for the merge). Upload benchmarks/results/all.json to https://jmh.morethan.io for charts.
#
#   benchmarks/run.sh                                              # everything (the full cross-product — hours)
#   benchmarks/run.sh ThroughputBench.get -p concurrency=64 -f 1 -wi 3 -i 3   # scope one workload/params (minutes)
#
# All arguments are forwarded verbatim to each cell's `Jmh/run`, so you can pass a benchmark regex, -p params, and -f/-wi/-i settings. A
# filter that doesn't match a cell (e.g. -p client=zio-redis only exists in the zio cell) leaves that cell out of the merged result; any
# OTHER failure (compile error, Docker, a benchmark exception) is reported and fails the run so partial results never look complete.
# per-cell sbt failures are classified by hand below (inside `if`, so they don't trip -e); merge/jq/column failures should still abort
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

args="$*"
cells=(benchmarksZio:zio benchmarksCe:ce benchmarksOx:ox benchmarksKyo3_8_3:kyo)
failed=0

rm -f benchmarks/results/*.json
for entry in "${cells[@]}"; do
  proj="${entry%%:*}"
  out="${entry##*:}"
  log="$(mktemp)"
  echo ">>> $proj/Jmh/run $args"
  # tee so JMH's iteration output streams live while we still capture it to classify the exit (pipefail keeps sbt's status)
  if sbt -batch "$proj/Jmh/run $args -rf json -rff benchmarks/results/$out.json" 2>&1 | tee "$log"; then
    :
  elif grep -qiE "No matching benchmarks|Unknown parameter" "$log"; then
    echo ">> $proj skipped (a client/benchmark filter does not match this cell)"
  else
    echo ">> $proj FAILED (see output above)"
    failed=1
  fi
  rm -f "$log"
done

"$root/benchmarks/merge-results.sh"

all="$root/benchmarks/results/all.json"
echo
echo "==== summary — $all (upload to https://jmh.morethan.io) ===="
jq -r '
  (["benchmark","client","conc","size","score","±error","unit"] | @tsv),
  (.[] | [
    (.benchmark | sub("sage.benchmarks.";"")),
    .params.client,
    (.params.concurrency // "-"),
    (.params.valueSize // "-"),
    (.primaryMetric.score | floor),
    (.primaryMetric.scoreError | if . == "NaN" then "-" else floor end),
    .primaryMetric.scoreUnit
  ] | @tsv)
' "$all" | column -t

if [ "$failed" -ne 0 ]; then
  echo
  echo "ERROR: one or more cells failed above — results in all.json are PARTIAL." >&2
  exit 1
fi
