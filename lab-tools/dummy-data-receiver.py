#!/usr/bin/env python3
"""Receive only Task 1.5's fixed dummy payload on the isolated lab interface."""

import argparse
import hashlib
import json
import threading
import uuid
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


DUMMY_DATA = (
    b"CCDC TRAINING DATA ONLY\n"
    b"record_id,customer,amount\n"
    b"DEMO-001,Fictional Customer A,0\n"
    b"DEMO-002,Fictional Customer B,0\n"
)
LAB_SOURCES = {"linux": "172.29.242.30", "windows": "172.29.242.40"}


class Receiver(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, log_path, hold_seconds):
        super().__init__(address, Handler)
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.touch(mode=0o600, exist_ok=True)
        self.hold_seconds = hold_seconds
        self.receipts = deque(maxlen=40)
        self.receipt_lock = threading.Lock()
        self.upload_slots = threading.BoundedSemaphore(8)
        self.stopping = threading.Event()

    def server_close(self):
        self.stopping.set()
        super().server_close()


class Handler(BaseHTTPRequestHandler):
    server_version = "CCDC-Dummy-Receiver/1.0"

    def allowed(self, platform=None):
        peer = self.client_address[0]
        if self.server.server_address[0] == "127.0.0.1":
            return peer == "127.0.0.1"
        return peer == LAB_SOURCES.get(platform) if platform else peer in LAB_SOURCES.values()

    def send_json(self, status, payload):
        encoded = (json.dumps(payload, indent=2) + "\n").encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):
        if not self.allowed():
            self.send_json(403, {"error": "Only the two lab VMs are allowed."})
        elif self.path == "/health":
            self.send_json(200, {"mode": "dummy-data-only", "observation_seconds": self.server.hold_seconds})
        elif self.path in ("/receipts/linux", "/receipts/windows"):
            platform = self.path.rsplit("/", 1)[1]
            if not self.allowed(platform):
                self.send_json(403, {"error": "Use the receipt page for your assigned VM."})
                return
            with self.server.receipt_lock:
                receipts = [r for r in self.server.receipts if r["platform"] == platform][-10:]
            self.send_json(200, {"receipts_since_receiver_start": receipts})
        else:
            self.send_json(404, {"error": "Unknown exercise endpoint."})

    def do_POST(self):
        if self.path not in ("/upload/linux", "/upload/windows"):
            self.send_json(404, {"error": "Unknown exercise endpoint."})
            return
        platform = self.path.rsplit("/", 1)[1]
        if not self.allowed(platform):
            self.send_json(403, {"error": "Upload only from the assigned lab VM."})
            return
        # Reject arbitrary uploads, including files that are merely small or similarly named.
        if self.headers.get("Transfer-Encoding") or self.headers.get("Content-Length") != str(len(DUMMY_DATA)):
            self.send_json(400, {"error": "Only the unmodified supplied dummy file is accepted."})
            return
        if not self.server.upload_slots.acquire(blocking=False):
            self.send_json(503, {"error": "Exercise observation slots are busy; retry later."})
            return
        try:
            self.connection.settimeout(5)
            data = self.rfile.read(len(DUMMY_DATA))
            if data != DUMMY_DATA:
                self.send_json(400, {"error": "Only the unmodified supplied dummy file is accepted."})
                return
            receipt = {
                "receipt_id": str(uuid.uuid4()),
                "received_utc": datetime.now(timezone.utc).isoformat(),
                "platform": platform,
                "source_ip": self.client_address[0],
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
                "dummy_data": data.decode(),
            }
            with self.server.receipt_lock:
                with self.server.log_path.open("a", encoding="utf-8") as log:
                    log.write(json.dumps(receipt) + "\n")
                self.server.receipts.append(receipt)
            print(json.dumps({"received": receipt["receipt_id"], "platform": platform}), flush=True)
            # The data has arrived. Delay only the acknowledgement to keep a visible
            # connection while students inspect the sender; no background sender is installed.
            self.server.stopping.wait(self.server.hold_seconds)
            self.send_json(201, receipt)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass  # A defender stopping curl does not undo an already recorded receipt.
        finally:
            self.server.upload_slots.release()

    def log_message(self, fmt, *args):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bind", choices=("172.29.242.1", "127.0.0.1"), default="172.29.242.1")
    parser.add_argument("--port", type=int, default=18081)
    parser.add_argument("--hold-seconds", type=int, choices=range(121), default=120, metavar="0..120")
    parser.add_argument("--log", type=Path, default=Path.home() / ".local/state/ccdc-task15/receipts.jsonl")
    args = parser.parse_args()
    server = Receiver((args.bind, args.port), args.log, args.hold_seconds)
    print(f"Dummy-only receiver: {args.bind}:{server.server_port}; receipts: {args.log}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
