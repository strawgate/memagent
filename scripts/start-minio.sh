#!/usr/bin/env bash
# scripts/start-minio.sh
#
# Start a local MinIO instance via Docker for S3 benchmarking and integration
# testing.  MinIO exposes the S3-compatible API on port 9000 and the web
# console on port 9001.
#
# Usage:
#   ./scripts/start-minio.sh          # Start MinIO
#   ./scripts/start-minio.sh stop     # Stop and remove the container
#   ./scripts/start-minio.sh status   # Check if MinIO is running
#
# Environment:
#   MINIO_ROOT_USER     (default: minioadmin)
#   MINIO_ROOT_PASSWORD (default: minioadmin)
#   MINIO_PORT          S3 API port (default: 9000)
#   MINIO_CONSOLE_PORT  Web console port (default: 9001)
#   MINIO_DATA_DIR      Host path for data (default: ~/.logfwd-minio)

set -euo pipefail

CONTAINER_NAME="logfwd-minio"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9001}"
MINIO_DATA_DIR="${MINIO_DATA_DIR:-${HOME}/.logfwd-minio}"
MINIO_IMAGE="minio/minio:latest"

cmd="${1:-start}"

case "$cmd" in
  start)
    echo "Starting MinIO container '${CONTAINER_NAME}'..."
    mkdir -p "${MINIO_DATA_DIR}"

    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
      echo "MinIO is already running."
      echo "  S3 endpoint: http://localhost:${MINIO_PORT}"
      echo "  Console:     http://localhost:${MINIO_CONSOLE_PORT}"
      exit 0
    fi

    # Remove stopped container if present.
    docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true

    docker run \
      --detach \
      --name "${CONTAINER_NAME}" \
      --publish "${MINIO_PORT}:9000" \
      --publish "${MINIO_CONSOLE_PORT}:9001" \
      --env "MINIO_ROOT_USER=${MINIO_ROOT_USER}" \
      --env "MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}" \
      --volume "${MINIO_DATA_DIR}:/data" \
      "${MINIO_IMAGE}" \
      server /data --console-address ":9001"

    echo "Waiting for MinIO to be ready..."
    for i in $(seq 1 30); do
      if curl -sf "http://localhost:${MINIO_PORT}/minio/health/live" > /dev/null 2>&1; then
        echo "MinIO is ready."
        break
      fi
      if [ "$i" -eq 30 ]; then
        echo "ERROR: MinIO did not become ready in time." >&2
        docker logs "${CONTAINER_NAME}" >&2
        exit 1
      fi
      sleep 1
    done

    echo ""
    echo "MinIO started successfully."
    echo "  S3 endpoint:        http://localhost:${MINIO_PORT}"
    echo "  Console URL:        http://localhost:${MINIO_CONSOLE_PORT}"
    echo "  Access key:         ${MINIO_ROOT_USER}"
    echo "  Secret key:         ${MINIO_ROOT_PASSWORD}"
    echo ""
    echo "Run benchmarks with:"
    echo "  MINIO_ENDPOINT=http://localhost:${MINIO_PORT} \\"
    echo "  AWS_ACCESS_KEY_ID=${MINIO_ROOT_USER} \\"
    echo "  AWS_SECRET_ACCESS_KEY=${MINIO_ROOT_PASSWORD} \\"
    echo "  cargo bench --bench s3_input -p logfwd-bench --features s3"
    ;;

  stop)
    echo "Stopping MinIO container '${CONTAINER_NAME}'..."
    docker rm -f "${CONTAINER_NAME}" 2>/dev/null && echo "Stopped." || echo "Container not found."
    ;;

  status)
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
      echo "MinIO is running."
      echo "  S3 endpoint: http://localhost:${MINIO_PORT}"
      echo "  Console:     http://localhost:${MINIO_CONSOLE_PORT}"
    else
      echo "MinIO is not running."
      exit 1
    fi
    ;;

  *)
    echo "Usage: $0 [start|stop|status]" >&2
    exit 1
    ;;
esac
