"""Incremental CISA KEV feed adapter using Huffman Cyber Observatory's feed client.

The source endpoint is fixed in Cyber Observatory code. No user-supplied URL,
API key, hosted service, or external model is involved.
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from openswarm.events import PlatformEvent
from openswarm.ingestion import MAX_BATCH_EVENTS, SourceBatch

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SHA256 = re.compile(r"^[a-f0-9]{64}$")


def _text(
    record: dict[str, Any], field: str, *, required: bool = False, limit: int = 2048
) -> str | None:
    value = record.get(field)
    if value is None:
        if required:
            raise ValueError(f"CISA KEV field {field} is required")
        return None
    if not isinstance(value, str) or len(value) > limit:
        raise ValueError(f"CISA KEV field {field} is invalid")
    cleaned = value.strip()
    if not cleaned and required:
        raise ValueError(f"CISA KEV field {field} is required")
    return cleaned or None


def _date(record: dict[str, Any]) -> str:
    value = _text(record, "dateAdded", required=True, limit=10)
    if not _DATE.fullmatch(value):
        raise ValueError("CISA KEV dateAdded must use YYYY-MM-DD")
    datetime.strptime(value, "%Y-%m-%d")
    return value


class CisaKevSource:
    """Poll CISA KEV via HCO and emit idempotent vulnerability events.

    By default, the first poll establishes a baseline without flooding the
    event journal with the historical catalog. Set initial_backfill=True to
    ingest the full catalog in bounded batches. Subsequent feed changes emit
    records on or after the last observed date; deterministic event IDs make
    same-day replays harmless.
    """

    def __init__(self, *, initial_backfill: bool = False, fetcher=None) -> None:
        if not isinstance(initial_backfill, bool):
            raise TypeError("initial_backfill must be a boolean")
        self.initial_backfill = initial_backfill
        self.fetcher = fetcher

    def _fetch(self) -> dict[str, Any]:
        if self.fetcher is not None:
            return self.fetcher()
        try:
            from huffman_cyber.feeds.kev import fetch_kev
        except ImportError as exc:
            raise RuntimeError(
                "install Huffman Cyber Observatory to enable the CISA KEV source"
            ) from exc
        return fetch_kev()

    @staticmethod
    def _checkpoint(value: str | None) -> dict[str, Any] | None:
        if value is None:
            return None
        try:
            state = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("CISA KEV checkpoint is malformed") from exc
        if (
            not isinstance(state, dict)
            or state.get("mode") not in {"date", "full", "delta"}
            or not isinstance(state.get("feed_sha256"), str)
            or not _SHA256.fullmatch(state["feed_sha256"])
        ):
            raise ValueError("CISA KEV checkpoint has an invalid shape")
        if state["mode"] == "date":
            if not isinstance(state.get("last_date"), str) or not _DATE.fullmatch(
                state["last_date"]
            ):
                raise ValueError("CISA KEV date checkpoint is invalid")
        else:
            if (
                not isinstance(state.get("offset"), int)
                or isinstance(state.get("offset"), bool)
                or state["offset"] < 0
            ):
                raise ValueError("CISA KEV batch offset is invalid")
            if state["mode"] == "delta" and (
                not isinstance(state.get("last_date"), str)
                or not _DATE.fullmatch(state["last_date"])
            ):
                raise ValueError("CISA KEV delta checkpoint is invalid")
        return state

    @staticmethod
    def _encode(state: dict[str, Any]) -> str:
        return json.dumps(state, sort_keys=True, separators=(",", ":"))

    def poll(self, checkpoint: str | None) -> SourceBatch:
        snapshot = self._fetch()
        if not isinstance(snapshot, dict):
            raise ValueError("HCO KEV client returned an invalid snapshot")
        feed_hash = snapshot.get("sha256")
        catalog = snapshot.get("catalog")
        vulnerabilities = catalog.get("vulnerabilities") if isinstance(catalog, dict) else None
        if (
            not isinstance(feed_hash, str)
            or not _SHA256.fullmatch(feed_hash)
            or not isinstance(vulnerabilities, list)
        ):
            raise ValueError("HCO KEV snapshot does not match the expected schema")
        dates = [_date(item) for item in vulnerabilities if isinstance(item, dict)]
        if len(dates) != len(vulnerabilities):
            raise ValueError("CISA KEV catalog contains a non-object record")
        latest_date = max(dates, default="1970-01-01")
        state = self._checkpoint(checkpoint)

        if state is None:
            if not self.initial_backfill:
                return SourceBatch(
                    [],
                    self._encode(
                        {"mode": "date", "last_date": latest_date, "feed_sha256": feed_hash}
                    ),
                )
            mode, offset, last_date = "full", 0, None
        elif state["mode"] == "date":
            if state["feed_sha256"] == feed_hash:
                return SourceBatch([], checkpoint)
            mode, offset, last_date = "delta", 0, state["last_date"]
        else:
            if state["feed_sha256"] == feed_hash:
                mode = state["mode"]
                offset = state["offset"]
                last_date = state.get("last_date")
            elif state["mode"] == "full":
                mode, offset, last_date = "full", 0, None
            else:
                mode, offset, last_date = "delta", 0, state["last_date"]

        records = (
            vulnerabilities
            if mode == "full"
            else [
                item for item in vulnerabilities
                if _date(item) >= last_date
            ]
        )
        chunk = records[offset : offset + MAX_BATCH_EVENTS]
        events = []
        source_url = snapshot.get("source_url")
        for item in chunk:
            cve_id = _text(item, "cveID", required=True, limit=32)
            if not re.fullmatch(r"CVE-\d{4}-\d{4,}", cve_id):
                raise ValueError("CISA KEV record contains an invalid CVE identifier")
            added = _date(item)
            event_id = str(uuid5(NAMESPACE_URL, f"cisa-kev:{cve_id}:{added}"))
            events.append(
                PlatformEvent(
                    event_id=event_id,
                    event_type="intelligence.vulnerability.kev_added",
                    source="huffman-cyber-observatory:cisa-kev",
                    classification="internal",
                    occurred_at=f"{added}T00:00:00Z",
                    evidence_refs=[f"cisa-kev:{cve_id}", f"sha256:{feed_hash}"],
                    payload={
                        "cve_id": cve_id,
                        "vendor_project": _text(item, "vendorProject", required=True, limit=256),
                        "product": _text(item, "product", required=True, limit=256),
                        "vulnerability_name": _text(
                            item, "vulnerabilityName", required=True, limit=512
                        ),
                        "date_added": added,
                        "short_description": _text(item, "shortDescription", limit=2048),
                        "required_action": _text(item, "requiredAction", limit=2048),
                        "due_date": _text(item, "dueDate", limit=10),
                        "known_ransomware_campaign_use": _text(
                            item, "knownRansomwareCampaignUse", limit=32
                        ),
                        "notes": _text(item, "notes", limit=2048),
                        "feed_sha256": feed_hash,
                        "feed_source_url": source_url,
                        "source": "CISA Known Exploited Vulnerabilities Catalog",
                    },
                )
            )

        next_offset = offset + len(chunk)
        if next_offset < len(records):
            next_state = {
                "mode": mode,
                "feed_sha256": feed_hash,
                "offset": next_offset,
            }
            if mode == "delta":
                next_state["last_date"] = last_date
        else:
            next_state = {
                "mode": "date",
                "last_date": max(latest_date, last_date or "1970-01-01"),
                "feed_sha256": feed_hash,
            }
        return SourceBatch(events, self._encode(next_state))
