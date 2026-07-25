#!/usr/bin/env bash
# Runs the benchmark matrix one player at a time.
#
# Every (engine, scale, shape) gets its own `go test` process. That is not
# pedantry: nestory registers entity types process-globally, so a second
# configuration cannot exist alongside the first — and running two engines in
# one process would let the first one's heap, GC state and warmed caches shift
# the second one's numbers. One process, one player, one configuration.
#
# Usage:
#   ./run.sh                     # every category, default scales
#   ./run.sh graph               # one category
#   NESTORY_SCALES="1000 10000" ./run.sh graph
#
# Raw `go test -bench` output goes to results/<category>/<engine>-<shape>-<scale>.txt.
set -uo pipefail

cd "$(dirname "$0")"

CATEGORIES=${1:-graph}
SCALES=${NESTORY_SCALES:-"1 10 100 1000 10000 100000 1000000"}
SHAPES=${NESTORY_SHAPES:-"single wide"}
BENCHTIME=${NESTORY_BENCHTIME:-300ms}
RESULTS=${NESTORY_RESULTS:-results}

# Engines per category. A name absent from a list is one that cannot express
# that workload, and the report says which and why.
graph_engines="Nestory NestoryUnsafe Memdb SQLite Bolt Bunt BuntMem Badger BadgerMem"
point_engines="Nestory NestoryUnsafe NestoryGet Memdb SQLite SQLiteMem Bolt Bunt BuntMem Badger BadgerMem Redis Mongo"

for category in $CATEGORIES; do
    engines_var="${category}_engines"
    engines=${!engines_var:-}
    if [ -z "$engines" ]; then
        echo "unknown category: $category" >&2
        exit 1
    fi

    mkdir -p "$RESULTS/$category"
    for shape in $SHAPES; do
        for scale in $SCALES; do
            for engine in $engines; do
                case $category in
                    graph) pattern="^BenchmarkGraphLoad_${engine}\$" ;;
                    point) pattern="^BenchmarkPoint(Read|Write)_${engine}\$" ;;
                    *) echo "no pattern for $category" >&2; exit 1 ;;
                esac
                out="$RESULTS/$category/${engine}-${shape}-${scale}.txt"
                printf '%-14s %-7s n=%-8s ' "$engine" "$shape" "$scale"

                if NESTORY_BENCH_SCALE="$scale" NESTORY_BENCH_SHAPE="$shape" \
                    go test ./compare/ -run='^$' -bench="$pattern" \
                    -benchmem -benchtime="$BENCHTIME" >"$out" 2>&1; then
                    lines=$(grep -E '^Benchmark' "$out")
                    if [ -n "$lines" ]; then
                        echo
                        echo "$lines" | awk '{printf "  %-34s %12s ns/op %10s B/op %8s allocs/op\n", $1, $3, $5, $7}'
                    else
                        echo "skipped"
                    fi
                else
                    echo "FAILED (see $out)"
                fi
            done
        done
    done
done
