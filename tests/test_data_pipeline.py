import asyncio
import csv
import json
import os
import tempfile
import time
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import websockets

from data_pipeline import DataPipeline


class _MockHandler(BaseHTTPRequestHandler):
    rest_payload = []
    csv_payload = []
    graphql_payload = []

    def _write_json(self, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        if self.path == "/rest":
            self._write_json(self.rest_payload)
            return
        if self.path == "/csv":
            rows = ["timestamp,value,unit"]
            for row in self.csv_payload:
                rows.append(f"{row['timestamp']},{row['value']},{row['unit']}")
            body = "\n".join(rows).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):  # noqa: N802
        if self.path == "/graphql":
            self._write_json({"data": {"items": self.graphql_payload}})
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):  # noqa: A003
        return


@contextmanager
def run_http_server(rest_payload, csv_payload, graphql_payload):
    _MockHandler.rest_payload = rest_payload
    _MockHandler.csv_payload = csv_payload
    _MockHandler.graphql_payload = graphql_payload
    server = ThreadingHTTPServer(("127.0.0.1", 0), _MockHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


async def _run_ws_server(messages, stop_event, port_holder):
    async def handler(websocket):
        for message in messages:
            await websocket.send(json.dumps(message))
            await asyncio.sleep(0.01)

    server = await websockets.serve(handler, "127.0.0.1", 0)
    port_holder.append(server.sockets[0].getsockname()[1])
    await stop_event.wait()
    server.close()
    await server.wait_closed()


class DataPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_pipeline_fetches_and_normalizes_all_source_types(self):
        rest_rows = [{"timestamp": "2026-01-01T10:00:00Z", "value": 10, "unit": "usd"}]
        csv_rows = [{"timestamp": "2026-01-01T10:01:00Z", "value": 11, "unit": "usd"}]
        graphql_rows = [{"timestamp": "2026-01-01T10:02:00Z", "value": 12, "unit": "usd"}]
        file_rows = [{"timestamp": "2026-01-01T10:03:00Z", "value": 13, "unit": "usd"}]
        ws_rows = [{"timestamp": "2026-01-01T10:04:00Z", "value": 14, "unit": "usd"}]

        stop_event = asyncio.Event()
        ws_port = []
        ws_task = asyncio.create_task(_run_ws_server(ws_rows, stop_event, ws_port))
        while not ws_port:
            await asyncio.sleep(0.01)

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "source.json"
            file_path.write_text(json.dumps(file_rows), encoding="utf-8")

            with run_http_server(rest_rows, csv_rows, graphql_rows) as http_port:
                config = {
                    "sources": [
                        {"name": "rest", "type": "rest", "url": f"http://127.0.0.1:{http_port}/rest"},
                        {"name": "csv", "type": "csv", "url": f"http://127.0.0.1:{http_port}/csv"},
                        {
                            "name": "graphql",
                            "type": "graphql",
                            "url": f"http://127.0.0.1:{http_port}/graphql",
                            "query": "{ items { timestamp value unit } }",
                            "data_path": "items",
                        },
                        {"name": "file", "type": "file", "path": str(file_path)},
                        {
                            "name": "websocket",
                            "type": "websocket",
                            "url": f"ws://127.0.0.1:{ws_port[0]}",
                            "max_messages": 1,
                        },
                    ]
                }
                result = await DataPipeline(config).run()

        stop_event.set()
        await ws_task

        self.assertEqual(5, len(result["records"]))
        self.assertEqual(["rest", "csv", "graphql", "file", "websocket"], [r["source"] for r in result["records"]])
        self.assertEqual(0, result["stats"]["errors"])

    async def test_pipeline_handles_timeout_and_continues(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "source.json"
            file_path.write_text(
                json.dumps([{"timestamp": "2026-01-01T10:00:00Z", "value": 1, "unit": "usd"}]),
                encoding="utf-8",
            )
            config = {
                "sources": [
                    {"name": "broken", "type": "rest", "url": "http://127.0.0.1:9/unreachable"},
                    {"name": "file", "type": "file", "path": str(file_path)},
                ]
            }
            result = await DataPipeline(config, request_timeout=0.2).run()

        self.assertEqual(1, len(result["records"]))
        self.assertEqual(1, result["stats"]["errors"])

    async def test_incremental_updates_fetch_only_new_records(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "source.json"
            state_path = Path(tmpdir) / "state.json"
            file_path.write_text(
                json.dumps(
                    [
                        {"timestamp": "2026-01-01T10:00:00Z", "value": 1, "unit": "usd"},
                        {"timestamp": "2026-01-01T10:01:00Z", "value": 2, "unit": "usd"},
                    ]
                ),
                encoding="utf-8",
            )

            config = {"sources": [{"name": "file", "type": "file", "path": str(file_path)}]}
            pipeline = DataPipeline(config, incremental_store=str(state_path))
            first = await pipeline.run()
            self.assertEqual(2, len(first["records"]))

            file_path.write_text(
                json.dumps(
                    [
                        {"timestamp": "2026-01-01T10:00:00Z", "value": 1, "unit": "usd"},
                        {"timestamp": "2026-01-01T10:01:00Z", "value": 2, "unit": "usd"},
                        {"timestamp": "2026-01-01T10:02:00Z", "value": 3, "unit": "usd"},
                    ]
                ),
                encoding="utf-8",
            )

            second = await pipeline.run()
            self.assertEqual(1, len(second["records"]))
            self.assertEqual(3, second["records"][0]["value"])

    async def test_pipeline_under_15_seconds_for_1000_records(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "source.csv"
            with file_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=["timestamp", "value", "unit"])
                writer.writeheader()
                for i in range(1000):
                    writer.writerow(
                        {
                            "timestamp": f"2026-01-01T10:{i % 60:02d}:{i % 60:02d}Z",
                            "value": i,
                            "unit": "usd",
                        }
                    )

            config = {"sources": [{"name": "file", "type": "file", "path": str(file_path)}]}
            start = time.perf_counter()
            result = await DataPipeline(config).run()
            elapsed = time.perf_counter() - start

        self.assertEqual(1000, len(result["records"]))
        self.assertLess(elapsed, 15.0)


if __name__ == "__main__":
    unittest.main()
