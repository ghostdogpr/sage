#!/usr/bin/env bash
# Merge the per-cell JMH JSONs written by `sbt benchAll` into one all.json covering every client (5 sage backends + zio-redis + redis4cats +
# lettuce + rediscala + jedis). Cells use the same benchmark class and method names and a unique `client` parameter. The resulting all.json
# can be compared as one run or uploaded to https://jmh.morethan.io.
set -euo pipefail
cd "$(dirname "$0")/results"

cells=(zio.json ce.json ox.json pekko.json kyo.json)
present=()
for f in "${cells[@]}"; do [ -s "$f" ] && present+=("$f"); done
if [ ${#present[@]} -eq 0 ]; then
  echo "no per-cell JSONs found in $(pwd) — run 'sbt benchAll' first" >&2
  exit 1
fi

jq -s 'add' "${present[@]}" > all.json
echo "wrote $(pwd)/all.json from: ${present[*]}"
