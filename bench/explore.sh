#!/usr/bin/env bash
# Exploratory benchmark: scanner + SQL transform across many dimensions.
# Generates synthetic data, runs logfwd with various SQL queries, measures throughput.
set -euo pipefail

LOGFWD="./target/release/logfwd"
TMPDIR=$(mktemp -d)
RESULTS="$TMPDIR/results.csv"
echo "dataset,lines,query,time_ms,lines_per_sec,mb_per_sec" > "$RESULTS"

# --- Data generation helpers ---

generate_simple_json() {
    local n=$1 out=$2
    "$LOGFWD" --generate-json "$n" "$out" 2>/dev/null
}

generate_wide_json() {
    # JSON with 50 fields per line
    local n=$1 out=$2
    python3 -c "
import json, sys
for i in range($n):
    d = {
        'timestamp': f'2024-01-15T10:30:00.{i%1000:03d}Z',
        'level': ['INFO','DEBUG','WARN','ERROR'][i%4],
        'message': f'request handled GET /api/v1/users/{10000+(i*7)%90000}',
        'duration_ms': 1 + (i*13) % 500,
        'request_id': f'{i:016x}',
        'service': 'myapp',
        'host': f'node-{i%10}',
        'pod': f'app-{i%100:04d}',
        'namespace': ['default','kube-system','monitoring','logging'][i%4],
        'container': 'main',
        'trace_id': f'{i*17:032x}',
        'span_id': f'{i*31:016x}',
        'user_id': f'user-{i%1000}',
        'org_id': f'org-{i%50}',
        'region': ['us-east-1','us-west-2','eu-west-1','ap-southeast-1'][i%4],
        'az': f'az-{i%3}',
        'instance_type': ['m5.large','c5.xlarge','r5.2xlarge'][i%3],
        'status_code': [200,201,400,404,500][i%5],
        'method': ['GET','POST','PUT','DELETE','PATCH'][i%5],
        'path': f'/api/v{1+i%3}/{[\"users\",\"orders\",\"products\",\"auth\"][i%4]}/{i%10000}',
        'response_bytes': 100 + (i*37) % 10000,
        'request_bytes': 50 + (i*19) % 5000,
        'latency_p50_ms': 1 + (i*7) % 100,
        'latency_p99_ms': 10 + (i*11) % 1000,
        'error_count': i % 20 == 0 and 1 or 0,
        'retry_count': i % 50 == 0 and (i%3) or 0,
        'cache_hit': i % 3 == 0,
        'db_query_count': (i*3) % 10,
        'db_query_ms': (i*7) % 200,
        'upstream_service': ['auth-svc','user-svc','order-svc','product-svc'][i%4],
        'upstream_latency_ms': (i*11) % 300,
        'version': f'v{1+i%5}.{i%10}.{i%20}',
        'build': f'build-{i%100:04d}',
        'environment': ['production','staging','development'][i%3],
        'feature_flags': f'flag-{i%10},flag-{i%20}',
        'correlation_id': f'corr-{i:012x}',
        'parent_span_id': f'{i*43:016x}',
        'sampled': i % 2 == 0,
        'priority': ['low','medium','high','critical'][i%4],
        'category': ['access','error','audit','metric','trace'][i%5],
        'source_ip': f'10.{i%256}.{(i//256)%256}.{(i//65536)%256}',
        'dest_ip': f'172.16.{i%256}.{(i//256)%256}',
        'port': 8000 + i % 100,
        'protocol': ['http','https','grpc'][i%3],
        'tls_version': ['TLS1.2','TLS1.3'][i%2],
        'cipher_suite': ['AES-128-GCM','AES-256-GCM','CHACHA20'][i%3],
        'cert_cn': f'*.example-{i%10}.com',
        'tags': f'env:{[\"prod\",\"stg\",\"dev\"][i%3]},team:{[\"platform\",\"infra\",\"app\"][i%3]}'
    }
    print(json.dumps(d, separators=(',',':')))
" > "$out"
}

generate_nested_json() {
    # JSON with nested objects and arrays
    local n=$1 out=$2
    python3 -c "
import json
for i in range($n):
    d = {
        'timestamp': f'2024-01-15T10:30:00.{i%1000:03d}Z',
        'level': ['INFO','DEBUG','WARN','ERROR'][i%4],
        'message': f'request {i}',
        'http': {
            'method': ['GET','POST','PUT'][i%3],
            'status': [200,400,500][i%3],
            'url': f'/api/v1/resource/{i%1000}',
            'headers': {'content-type': 'application/json', 'x-request-id': f'{i:016x}'}
        },
        'timing': {
            'total_ms': 1 + (i*13) % 500,
            'db_ms': (i*7) % 100,
            'cache_ms': (i*3) % 50,
            'render_ms': (i*11) % 200
        },
        'tags': [f'tag-{j}' for j in range(i%5)],
        'metadata': {'version': f'v{i%10}', 'build': i%1000}
    }
    print(json.dumps(d, separators=(',',':')))
" > "$out"
}

generate_high_cardinality() {
    # Every field is unique (worst case for dictionary encoding)
    local n=$1 out=$2
    python3 -c "
import json, uuid, random, string
for i in range($n):
    d = {
        'timestamp': f'2024-01-15T10:{(i//3600)%24:02d}:{(i//60)%60:02d}.{i%1000:03d}Z',
        'id': str(i),
        'uuid': f'{i:032x}',
        'message': ''.join(random.choices(string.ascii_lowercase + ' ', k=50+i%200)),
        'value': random.random() * 1000,
        'counter': i,
        'source': f'host-{i}-{random.randint(0,999999)}'
    }
    print(json.dumps(d, separators=(',',':')))
" > "$out"
}

# --- Benchmark runner ---

run_bench() {
    local dataset=$1 lines=$2 query=$3 datafile=$4
    local config="$TMPDIR/bench.yaml"

    cat > "$config" << YAML
input:
  type: file
  path: $datafile
  format: json
transform: "$query"
output:
  type: stdout
  format: json
YAML

    local start_ms=$(python3 -c "import time; print(int(time.time()*1000))")
    "$LOGFWD" --config "$config" > /dev/null 2>/dev/null || true
    local end_ms=$(python3 -c "import time; print(int(time.time()*1000))")
    local elapsed=$((end_ms - start_ms))

    local file_mb=$(python3 -c "import os; print(f'{os.path.getsize(\"$datafile\")/1048576:.1f}')")
    local lps=0
    local mbps="0"
    if [ "$elapsed" -gt 0 ]; then
        lps=$((lines * 1000 / elapsed))
        mbps=$(python3 -c "print(f'{float($file_mb) / ($elapsed/1000):.1f}')")
    fi

    echo "$dataset,$lines,$query,$elapsed,$lps,$mbps" >> "$RESULTS"
    printf "  %-20s %8d lines  %-55s %6dms  %8d lines/sec  %s MB/s\n" \
        "$dataset" "$lines" "$(echo $query | head -c 55)" "$elapsed" "$lps" "$mbps"
}

# ============================================================
echo "=== Exploratory Benchmark ==="
echo "=== $(date) ==="
echo ""

# --- Generate datasets ---
echo "Generating datasets..."
for n in 10000 100000 1000000; do
    generate_simple_json $n "$TMPDIR/simple_${n}.jsonl"
    echo "  simple_${n}: $(du -h "$TMPDIR/simple_${n}.jsonl" | cut -f1)"
done

generate_wide_json 100000 "$TMPDIR/wide_100k.jsonl"
echo "  wide_100k: $(du -h "$TMPDIR/wide_100k.jsonl" | cut -f1)"

generate_nested_json 100000 "$TMPDIR/nested_100k.jsonl"
echo "  nested_100k: $(du -h "$TMPDIR/nested_100k.jsonl" | cut -f1)"

generate_high_cardinality 100000 "$TMPDIR/highcard_100k.jsonl"
echo "  highcard_100k: $(du -h "$TMPDIR/highcard_100k.jsonl" | cut -f1)"

echo ""

# --- Dimension 1: Data size scaling ---
echo "=== Dimension 1: Data Size Scaling (passthrough) ==="
for n in 10000 100000 1000000; do
    run_bench "simple" $n "SELECT * FROM logs" "$TMPDIR/simple_${n}.jsonl"
done
echo ""

# --- Dimension 2: Query complexity ---
echo "=== Dimension 2: Query Complexity (100K lines) ==="
QUERIES=(
    "SELECT * FROM logs"
    "SELECT timestamp_str, level_str, message_str FROM logs"
    "SELECT * FROM logs WHERE level_str = 'ERROR'"
    "SELECT * FROM logs WHERE level_str IN ('ERROR', 'WARN')"
    "SELECT * FROM logs WHERE duration_ms_int > 100"
    "SELECT * FROM logs WHERE level_str = 'ERROR' AND duration_ms_int > 200"
    "SELECT level_str, COUNT(*) as cnt FROM logs GROUP BY level_str"
    "SELECT service_str, level_str, COUNT(*) FROM logs GROUP BY service_str, level_str"
    "SELECT level_str, AVG(duration_ms_int) as avg_dur, MAX(duration_ms_int) as max_dur FROM logs GROUP BY level_str"
    "SELECT *, CASE WHEN duration_ms_int > 200 THEN 'slow' ELSE 'fast' END as speed_str FROM logs"
    "SELECT * FROM logs WHERE message_str LIKE '%users%'"
    "SELECT * FROM logs ORDER BY duration_ms_int DESC LIMIT 100"
)
for q in "${QUERIES[@]}"; do
    run_bench "simple" 100000 "$q" "$TMPDIR/simple_100000.jsonl"
done
echo ""

# --- Dimension 3: Field count (wide vs narrow) ---
echo "=== Dimension 3: Field Count ==="
run_bench "simple(6f)" 100000 "SELECT * FROM logs" "$TMPDIR/simple_100000.jsonl"
run_bench "wide(50f)" 100000 "SELECT * FROM logs" "$TMPDIR/wide_100k.jsonl"
run_bench "nested" 100000 "SELECT * FROM logs" "$TMPDIR/nested_100k.jsonl"
run_bench "highcard" 100000 "SELECT * FROM logs" "$TMPDIR/highcard_100k.jsonl"
echo ""

# --- Dimension 4: Projection pushdown ---
echo "=== Dimension 4: Projection Pushdown (wide dataset) ==="
run_bench "wide" 100000 "SELECT * FROM logs" "$TMPDIR/wide_100k.jsonl"
run_bench "wide" 100000 "SELECT timestamp_str, level_str FROM logs" "$TMPDIR/wide_100k.jsonl"
run_bench "wide" 100000 "SELECT timestamp_str, level_str, message_str, duration_ms_int FROM logs" "$TMPDIR/wide_100k.jsonl"
run_bench "wide" 100000 "SELECT * FROM logs WHERE level_str = 'ERROR'" "$TMPDIR/wide_100k.jsonl"
echo ""

# --- Dimension 5: Aggregation queries ---
echo "=== Dimension 5: Aggregations (100K lines) ==="
run_bench "simple" 100000 "SELECT COUNT(*) FROM logs" "$TMPDIR/simple_100000.jsonl"
run_bench "simple" 100000 "SELECT level_str, COUNT(*) FROM logs GROUP BY level_str" "$TMPDIR/simple_100000.jsonl"
run_bench "simple" 100000 "SELECT level_str, AVG(duration_ms_int), MIN(duration_ms_int), MAX(duration_ms_int) FROM logs GROUP BY level_str" "$TMPDIR/simple_100000.jsonl"
run_bench "wide" 100000 "SELECT region_str, namespace_str, COUNT(*) FROM logs GROUP BY region_str, namespace_str" "$TMPDIR/wide_100k.jsonl"
run_bench "wide" 100000 "SELECT service_str, method_str, AVG(duration_ms_int), COUNT(*) FROM logs GROUP BY service_str, method_str" "$TMPDIR/wide_100k.jsonl"
echo ""

# --- Dimension 6: Heavy transforms ---
echo "=== Dimension 6: Heavy Transforms (100K lines) ==="
run_bench "simple" 100000 "SELECT *, duration_ms_int * 1000 as duration_us FROM logs" "$TMPDIR/simple_100000.jsonl"
run_bench "simple" 100000 "SELECT *, UPPER(level_str) as level_upper FROM logs" "$TMPDIR/simple_100000.jsonl"
run_bench "simple" 100000 "SELECT *, LENGTH(message_str) as msg_len FROM logs" "$TMPDIR/simple_100000.jsonl"
run_bench "simple" 100000 "SELECT *, CONCAT(level_str, ':', service_str) as tag FROM logs" "$TMPDIR/simple_100000.jsonl"
echo ""

# --- Summary ---
echo "=== Results saved to $RESULTS ==="
echo ""
echo "=== Top 5 Fastest ==="
sort -t, -k5 -rn "$RESULTS" | head -6
echo ""
echo "=== Top 5 Slowest ==="
sort -t, -k5 -n "$RESULTS" | head -6

# Cleanup
# rm -rf "$TMPDIR"  # Keep for inspection
echo ""
echo "Data dir: $TMPDIR"
