#!/usr/bin/env bash
# compare.sh — benchmark fork (built from source) vs oracle (com.cerner/clara-rules JAR)
#
# Usage:
#   cd /path/to/clara-rules
#   bash bench/compare.sh
#   BENCH_N=10000 bash bench/compare.sh      # quick smoke
#   BENCH_ONLY=01,11,50 bash bench/compare.sh
#
# Requirements: clojure CLI, Java 21+

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FORK_OUT="$REPO_DIR/bench/results-fork.txt"
ORACLE_OUT="$REPO_DIR/bench/results-oracle.txt"
ORACLE_VERSION="${ORACLE_VERSION:-0.24.0}"

export BENCH_N="${BENCH_N:-100000}"
export BENCH_ITERS="${BENCH_ITERS:-2}"
# Only export BENCH_ONLY if non-empty (empty string would filter out all scenarios)
if [[ -n "${BENCH_ONLY:-}" ]]; then export BENCH_ONLY; else unset BENCH_ONLY 2>/dev/null || true; fi

echo "========================================================================"
echo "  Clara Rules Benchmark Comparison"
echo "  Fork:   $(git -C "$REPO_DIR" rev-parse --short HEAD) (built from source)"
echo "  Oracle: com.cerner/clara-rules $ORACLE_VERSION (released JAR)"
echo "  N=$BENCH_N  ITERS=$BENCH_ITERS"
echo "========================================================================"

# ── 1. Compile Java sources for fork ────────────────────────────────────────
echo ""
echo "==> [1/4] Compiling Java sources (fork)..."
(cd "$REPO_DIR" && clojure -T:build javac 2>&1 | tail -3) || true

# ── 2. Run fork benchmarks ───────────────────────────────────────────────────
echo ""
echo "==> [2/4] Running benchmarks on FORK..."
(cd "$REPO_DIR" && clojure -M:bench 2>/dev/null | tee "$FORK_OUT")

# ── 3. Run oracle benchmarks (using released JAR, no source checkout needed) ─
echo ""
echo "==> [3/4] Running benchmarks on ORACLE JAR ($ORACLE_VERSION)..."
(cd "$REPO_DIR" && \
  BENCH_N="$BENCH_N" BENCH_ITERS="$BENCH_ITERS" BENCH_ONLY="$BENCH_ONLY" \
  clojure \
    -Sdeps "{:paths [\"bench\"]
             :deps {org.clojure/clojure {:mvn/version \"1.12.4\"}
                    com.cerner/clara-rules {:mvn/version \"$ORACLE_VERSION\"}
                    prismatic/schema {:mvn/version \"1.4.1\"}
                    org.clojure/core.cache {:mvn/version \"1.1.234\"}}}" \
    -J-Xmx8g -J-server \
    -M -m clara.bench 2>/dev/null | tee "$ORACLE_OUT")

# ── 4. Print comparison table ────────────────────────────────────────────────
echo ""
echo "========================================================================"
echo "  COMPARISON: Fork vs Oracle $ORACLE_VERSION  (lower = faster)"
echo "========================================================================"
echo ""
printf "  %-6s %-52s %10s %10s %9s\n" "ID" "Scenario" "Fork ms" "Oracle ms" "Speedup"
printf "  %s\n" "$(printf '%.0s-' {1..83})"

python3 - "$FORK_OUT" "$ORACLE_OUT" <<'PYEOF'
import sys, re

def parse(path):
    out = {}
    try:
        for line in open(path):
            m = re.match(r'\s+\[(\d+)\]\s+(.*?)\s+mean=\s*([\d.]+)', line)
            if m:
                out[m.group(1)] = {'label': m.group(2).strip(), 'mean': float(m.group(3))}
    except FileNotFoundError:
        pass
    return out

fork   = parse(sys.argv[1])
oracle = parse(sys.argv[2])
ids    = sorted(set(fork) | set(oracle))

tf = to = cnt = 0.0
for i in ids:
    f = fork.get(i,   {'label':'?','mean':0})
    o = oracle.get(i, {'label':'?','mean':0})
    lbl = f['label'] if f['label'] != '?' else o['label']
    fm, om = f['mean'], o['mean']
    sp = (om / fm) if fm > 0 and om > 0 else 0.0
    mark = ' <' if sp > 1.05 else (' >' if sp < 0.95 else '  ')
    print(f"  [{i}] {lbl:<52s} {fm:>9.1f}  {om:>9.1f}  {sp:>6.2f}x{mark}")
    if fm > 0 and om > 0:
        tf += fm; to += om; cnt += 1

print()
if cnt > 0:
    sp = to / tf
    print(f"  {'TOTAL':>60s}  {tf:>9.1f}  {to:>9.1f}  {sp:>6.2f}x")
    print()
    if sp > 1.0:
        print(f"  Fork is {sp:.2f}x FASTER overall ({(sp-1)*100:.0f}% faster than oracle release)")
    else:
        print(f"  Fork is {1/sp:.2f}x SLOWER overall vs oracle release")
PYEOF

echo ""
echo "  < = fork faster   > = fork slower"
echo "  Results: $FORK_OUT"
echo "           $ORACLE_OUT"
