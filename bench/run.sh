#!/usr/bin/env bash
#
# Benchmark: logfwd vs vector vs filebeat
#
# All agents send to a logfwd blackhole HTTP sink, which counts lines.
# This gives apples-to-apples throughput comparison with identical I/O.
#
# Usage:
#   ./bench/run.sh                    # 5M lines, all agents found on PATH
#   ./bench/run.sh --lines 1000000    # custom line count
#   ./bench/run.sh --agents logfwd    # only run logfwd
#
# Prerequisites:
#   - logfwd built: cargo build --release
#   - vector (optional): https://vector.dev/docs/setup/installation/
#   - filebeat (optional): https://www.elastic.co/beats/filebeat

set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

LINES=5000000
AGENTS=""
BENCH_DIR=$(mktemp -d)
LOGFWD="${LOGFWD:-./target/release/logfwd}"
VECTOR="${VECTOR:-vector}"
FILEBEAT="${FILEBEAT:-filebeat}"
DIAG_PORT=19876
BLACKHOLE_PORT=19877
BLACKHOLE_ADDR="127.0.0.1:$BLACKHOLE_PORT"

trap 'cleanup' EXIT

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    rm -rf "$BENCH_DIR"
}

PIDS=()

# ---------------------------------------------------------------------------
# Parse args
# ---------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        --lines) LINES="$2"; shift 2 ;;
        --agents) AGENTS="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

has_cmd() { command -v "$1" &>/dev/null; }

# Poll blackhole /stats until lines reaches expected count (or stabilizes).
wait_blackhole_done() {
    local expected="$1"
    local prev=0 stable=0
    for _ in $(seq 1 600); do  # 60s max
        sleep 0.1
        local lines
        lines=$(curl -s "http://${BLACKHOLE_ADDR}/stats" 2>/dev/null \
            | grep -o '"lines":[0-9]*' | cut -d: -f2) || true
        if [[ -z "$lines" ]]; then continue; fi
        if [[ "$lines" -ge "$expected" ]]; then
            echo "$lines"
            return 0
        fi
        if [[ "$lines" -eq "$prev" && "$lines" -gt 0 ]]; then
            stable=$((stable + 1))
            if [[ "$stable" -ge 30 ]]; then  # 3s stable = done
                echo "$lines"
                return 0
            fi
        else
            stable=0
        fi
        prev="$lines"
    done
    echo "$prev"
    return 1
}

# Start blackhole, wait for it to be ready.
start_blackhole() {
    "$LOGFWD" --blackhole "$BLACKHOLE_ADDR" 2>/dev/null &
    local pid=$!
    PIDS+=("$pid")
    BLACKHOLE_PID=$pid
    for _ in $(seq 1 50); do
        if curl -s "http://${BLACKHOLE_ADDR}/stats" &>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    echo "ERROR: blackhole failed to start" >&2
    return 1
}

# Stop and restart blackhole (resets counters).
restart_blackhole() {
    if [[ -n "${BLACKHOLE_PID:-}" ]]; then
        kill "$BLACKHOLE_PID" 2>/dev/null || true
        wait "$BLACKHOLE_PID" 2>/dev/null || true
        PIDS=("${PIDS[@]/$BLACKHOLE_PID/}")
    fi
    start_blackhole
}

fmt_rate() {
    local lines="$1" ms="$2"
    if [[ "$ms" -eq 0 ]]; then echo "N/A"; return; fi
    local lps=$(( lines * 1000 / ms ))
    if [[ "$lps" -ge 1000000 ]]; then
        awk "BEGIN { printf \"%.2fM lines/sec\", $lps / 1000000.0 }"
    else
        awk "BEGIN { printf \"%.0fK lines/sec\", $lps / 1000.0 }"
    fi
}

now_ms() {
    if [[ "$(uname)" == "Darwin" ]]; then
        python3 -c "import time; print(int(time.time()*1000))"
    else
        date +%s%3N
    fi
}

# ---------------------------------------------------------------------------
# Generate test data
# ---------------------------------------------------------------------------

DATA_FILE="$BENCH_DIR/test.jsonl"
echo "=== Generating $LINES JSON log lines ==="
"$LOGFWD" --generate-json "$LINES" "$DATA_FILE" 2>&1 | grep -v "^$"
FILE_SIZE=$(stat -f%z "$DATA_FILE" 2>/dev/null || stat -c%s "$DATA_FILE")
echo "  File: $DATA_FILE ($(awk "BEGIN { printf \"%.1f\", $FILE_SIZE / 1048576.0 }") MB)"
echo

# ---------------------------------------------------------------------------
# Determine which agents to run
# ---------------------------------------------------------------------------

if [[ -z "$AGENTS" ]]; then
    AGENTS="logfwd"
    has_cmd "$VECTOR" && AGENTS="$AGENTS,vector"
    has_cmd "$FILEBEAT" && AGENTS="$AGENTS,filebeat"
fi

echo "=== Agents: $AGENTS ==="
echo "=== Blackhole sink: http://$BLACKHOLE_ADDR ==="
echo

# ---------------------------------------------------------------------------
# Results table
# ---------------------------------------------------------------------------

declare -a RESULT_NAMES=()
declare -a RESULT_LINES=()
declare -a RESULT_MS=()
declare -a RESULT_RATES=()

# ---------------------------------------------------------------------------
# Benchmark: logfwd
# ---------------------------------------------------------------------------

bench_logfwd() {
    echo "--- logfwd ---"
    restart_blackhole

    local cfg="$BENCH_DIR/logfwd.yaml"
    cat > "$cfg" <<YAML
server:
  diagnostics: "127.0.0.1:$DIAG_PORT"
pipelines:
  bench:
    inputs:
      - type: file
        path: "$DATA_FILE"
        format: json
    transform: "SELECT * FROM logs"
    outputs:
      - type: http
        endpoint: "http://$BLACKHOLE_ADDR"
        format: json
YAML

    local start
    start=$(now_ms)
    "$LOGFWD" --config "$cfg" 2>/dev/null &
    local pid=$!
    PIDS+=("$pid")

    local lines_done
    lines_done=$(wait_blackhole_done "$LINES") || true
    local end
    end=$(now_ms)
    local elapsed=$(( end - start ))

    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    PIDS=("${PIDS[@]/$pid/}")

    local rate
    rate=$(fmt_rate "${lines_done:-0}" "$elapsed")
    echo "  Lines: ${lines_done:-0} / $LINES"
    echo "  Time:  ${elapsed}ms"
    echo "  Rate:  $rate"
    echo

    RESULT_NAMES+=("logfwd")
    RESULT_LINES+=("${lines_done:-0}")
    RESULT_MS+=("$elapsed")
    RESULT_RATES+=("$rate")
}

# ---------------------------------------------------------------------------
# Benchmark: vector
# ---------------------------------------------------------------------------

bench_vector() {
    echo "--- vector ---"
    restart_blackhole

    local cfg="$BENCH_DIR/vector.yaml"
    local vector_data="$BENCH_DIR/vector_data"
    mkdir -p "$vector_data"

    cat > "$cfg" <<YAML
data_dir: "$vector_data"
sources:
  bench_in:
    type: file
    include:
      - "$DATA_FILE"
    read_from: beginning
    ignore_checkpoints: true
sinks:
  bench_out:
    type: http
    inputs: ["bench_in"]
    uri: "http://$BLACKHOLE_ADDR"
    encoding:
      codec: json
    method: post
YAML

    local start
    start=$(now_ms)
    "$VECTOR" --config "$cfg" --quiet 2>/dev/null &
    local pid=$!
    PIDS+=("$pid")

    local lines_done
    lines_done=$(wait_blackhole_done "$LINES") || true
    local end
    end=$(now_ms)
    local elapsed=$(( end - start ))

    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    PIDS=("${PIDS[@]/$pid/}")

    local rate
    rate=$(fmt_rate "${lines_done:-0}" "$elapsed")
    echo "  Lines: ${lines_done:-0} / $LINES"
    echo "  Time:  ${elapsed}ms"
    echo "  Rate:  $rate"
    echo

    RESULT_NAMES+=("vector")
    RESULT_LINES+=("${lines_done:-0}")
    RESULT_MS+=("$elapsed")
    RESULT_RATES+=("$rate")
}

# ---------------------------------------------------------------------------
# Benchmark: filebeat
# ---------------------------------------------------------------------------

bench_filebeat() {
    echo "--- filebeat ---"
    restart_blackhole

    local cfg="$BENCH_DIR/filebeat.yaml"
    local fb_data="$BENCH_DIR/fb_data"
    local fb_logs="$BENCH_DIR/fb_logs"
    mkdir -p "$fb_data" "$fb_logs"

    # Filebeat sends to /_bulk which the blackhole handles with ES-compat response
    cat > "$cfg" <<YAML
filebeat.inputs:
  - type: log
    enabled: true
    paths:
      - "$DATA_FILE"

output.elasticsearch:
  hosts: ["http://$BLACKHOLE_ADDR"]
  index: "bench"

path.data: "$fb_data"
path.logs: "$fb_logs"
logging.level: error
YAML

    local start
    start=$(now_ms)
    "$FILEBEAT" -e -c "$cfg" 2>/dev/null &
    local pid=$!
    PIDS+=("$pid")

    local lines_done
    lines_done=$(wait_blackhole_done "$LINES") || true
    local end
    end=$(now_ms)
    local elapsed=$(( end - start ))

    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    PIDS=("${PIDS[@]/$pid/}")

    local rate
    rate=$(fmt_rate "${lines_done:-0}" "$elapsed")
    echo "  Lines: ${lines_done:-0} / $LINES"
    echo "  Time:  ${elapsed}ms"
    echo "  Rate:  $rate"
    echo

    RESULT_NAMES+=("filebeat")
    RESULT_LINES+=("${lines_done:-0}")
    RESULT_MS+=("$elapsed")
    RESULT_RATES+=("$rate")
}

# ---------------------------------------------------------------------------
# Run benchmarks
# ---------------------------------------------------------------------------

IFS=',' read -ra AGENT_LIST <<< "$AGENTS"

for agent in "${AGENT_LIST[@]}"; do
    case "$agent" in
        logfwd)   bench_logfwd ;;
        vector)   bench_vector ;;
        filebeat) bench_filebeat ;;
        *) echo "Unknown agent: $agent"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

echo "==========================================="
echo "  RESULTS ($LINES lines, $(awk "BEGIN { printf \"%.1f\", $FILE_SIZE / 1048576.0 }") MB)"
echo "==========================================="
printf "  %-12s %12s %10s %20s\n" "Agent" "Lines" "Time" "Throughput"
printf "  %-12s %12s %10s %20s\n" "-----" "-----" "----" "----------"

for i in "${!RESULT_NAMES[@]}"; do
    printf "  %-12s %12s %8sms %20s\n" \
        "${RESULT_NAMES[$i]}" \
        "${RESULT_LINES[$i]}" \
        "${RESULT_MS[$i]}" \
        "${RESULT_RATES[$i]}"
done
echo "==========================================="

# Show relative performance vs first agent
if [[ ${#RESULT_MS[@]} -gt 1 ]]; then
    echo
    base_ms="${RESULT_MS[0]}"
    base_name="${RESULT_NAMES[0]}"
    for i in "${!RESULT_NAMES[@]}"; do
        if [[ "$i" -eq 0 ]]; then continue; fi
        if [[ "${RESULT_MS[$i]}" -gt 0 ]]; then
            ratio=$(awk "BEGIN { printf \"%.1f\", ${RESULT_MS[$i]} / $base_ms }")
            echo "  $base_name is ${ratio}x vs ${RESULT_NAMES[$i]}"
        fi
    done
fi
