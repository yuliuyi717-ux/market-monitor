import asyncio
import csv
import json
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import aiohttp
import websockets


class DataPipeline:
    def __init__(self, config: dict[str, Any], incremental_store: str | None = None, request_timeout: float = 5.0):
        self.config = config
        self.incremental_store = Path(incremental_store) if incremental_store else None
        self.request_timeout = request_timeout

    def _load_incremental_state(self) -> dict[str, str]:
        if not self.incremental_store or not self.incremental_store.exists():
            return {}
        try:
            return json.loads(self.incremental_store.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def _save_incremental_state(self, state: dict[str, str]) -> None:
        if not self.incremental_store:
            return
        self.incremental_store.parent.mkdir(parents=True, exist_ok=True)
        self.incremental_store.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")

    async def run(self, output_path: str | None = None) -> dict[str, Any]:
        sources = self.config.get("sources", [])
        tasks = [self._fetch_source(source) for source in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        records_by_source: dict[str, list[dict[str, Any]]] = {}
        records: list[dict[str, Any]] = []
        errors = 0

        for source, result in zip(sources, results):
            source_name = source["name"]
            if isinstance(result, Exception):
                errors += 1
                records_by_source[source_name] = []
                continue
            normalized = [self._normalize_record(source_name, item, source.get("mapping"), source.get("unit")) for item in result]
            records_by_source[source_name] = normalized
            records.extend(normalized)

        state = self._load_incremental_state()
        filtered_records = self._apply_incremental_filter(records_by_source, state)
        filtered_records.sort(key=lambda item: item["timestamp"])

        output = {
            "records": filtered_records,
            "stats": {
                "records": len(filtered_records),
                "sources": len(sources),
                "errors": errors,
                "source_breakdown": {name: len(items) for name, items in records_by_source.items()},
            },
        }

        if output_path:
            Path(output_path).write_text(json.dumps(output, indent=2), encoding="utf-8")

        return output

    def _apply_incremental_filter(
        self, records_by_source: dict[str, list[dict[str, Any]]], state: dict[str, str]
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for source_name, records in records_by_source.items():
            if not records:
                continue
            last_seen = state.get(source_name)
            for record in records:
                if not last_seen or record["timestamp"] > last_seen:
                    out.append(record)
            state[source_name] = max(record["timestamp"] for record in records)
        self._save_incremental_state(state)
        return out

    def _normalize_record(
        self, source_name: str, record: dict[str, Any], mapping: dict[str, str] | None, default_unit: str | None
    ) -> dict[str, Any]:
        field_map = mapping or {}
        timestamp_key = field_map.get("timestamp", "timestamp")
        value_key = field_map.get("value", "value")
        unit_key = field_map.get("unit", "unit")

        timestamp = self._normalize_timestamp(record.get(timestamp_key))
        value = record.get(value_key)
        unit = record.get(unit_key, default_unit)
        return {"source": source_name, "timestamp": timestamp, "value": value, "unit": unit}

    def _normalize_timestamp(self, value: Any) -> str:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat().replace("+00:00", "Z")
        if isinstance(value, str) and value:
            if value.endswith("Z"):
                return value
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        raise ValueError("timestamp is required")

    async def _fetch_source(self, source: dict[str, Any]) -> list[dict[str, Any]]:
        source_type = source["type"]
        if source_type == "rest":
            return await self._fetch_rest(source)
        if source_type == "csv":
            return await self._fetch_csv(source)
        if source_type == "graphql":
            return await self._fetch_graphql(source)
        if source_type == "file":
            return await self._fetch_file(source)
        if source_type == "websocket":
            return await self._fetch_websocket(source)
        raise ValueError(f"unsupported source type: {source_type}")

    async def _fetch_rest(self, source: dict[str, Any]) -> list[dict[str, Any]]:
        timeout = aiohttp.ClientTimeout(total=self.request_timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(source["url"]) as response:
                response.raise_for_status()
                payload = await response.json()
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            data_path = source.get("data_path")
            if data_path:
                return self._extract_path(payload, data_path)
        raise ValueError("rest payload must be list or include data_path")

    async def _fetch_csv(self, source: dict[str, Any]) -> list[dict[str, Any]]:
        if source.get("url"):
            timeout = aiohttp.ClientTimeout(total=self.request_timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(source["url"]) as response:
                    response.raise_for_status()
                    content = await response.text()
        else:
            content = Path(source["path"]).read_text(encoding="utf-8")
        reader = csv.DictReader(StringIO(content))
        return [dict(row) for row in reader]

    async def _fetch_graphql(self, source: dict[str, Any]) -> list[dict[str, Any]]:
        timeout = aiohttp.ClientTimeout(total=self.request_timeout)
        payload = {"query": source.get("query"), "variables": source.get("variables", {})}
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(source["url"], json=payload) as response:
                response.raise_for_status()
                body = await response.json()
        data = body.get("data", {})
        return self._extract_path(data, source.get("data_path", ""))

    async def _fetch_file(self, source: dict[str, Any]) -> list[dict[str, Any]]:
        path = Path(source["path"])
        if path.suffix.lower() == ".csv":
            content = path.read_text(encoding="utf-8")
            return [dict(row) for row in csv.DictReader(StringIO(content))]
        content = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(content, list):
            raise ValueError("file source json must be list")
        return content

    async def _fetch_websocket(self, source: dict[str, Any]) -> list[dict[str, Any]]:
        messages = []
        max_messages = int(source.get("max_messages", 10))
        timeout = float(source.get("timeout", self.request_timeout))

        async with websockets.connect(source["url"], open_timeout=timeout) as websocket:
            for _ in range(max_messages):
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    break
                messages.append(json.loads(raw))
        return messages

    def _extract_path(self, payload: dict[str, Any], path: str) -> list[dict[str, Any]]:
        current: Any = payload
        for token in [token for token in path.split(".") if token]:
            if not isinstance(current, dict) or token not in current:
                return []
            current = current[token]
        if isinstance(current, list):
            return current
        return []
