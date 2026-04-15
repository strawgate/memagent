#!/usr/bin/env python3

from __future__ import annotations

import argparse
import socketserver
import threading
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture newline-delimited TCP output into a file.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


class CaptureHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        while True:
            chunk = self.request.recv(65536)
            if not chunk:
                return
            with self.server.lock:
                with self.server.output_path.open("ab") as handle:
                    handle.write(chunk)


class CaptureServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], handler_cls: type[CaptureHandler], output_path: Path):
        super().__init__(server_address, handler_cls)
        self.output_path = output_path
        self.lock = threading.Lock()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "captured.ndjson"
    output_path.write_text("", encoding="utf-8")

    with CaptureServer((args.host, args.port), CaptureHandler, output_path) as server:
        print(f"capture-tcp listening on {args.host}:{args.port}, writing to {output_path}", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
