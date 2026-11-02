"""Synthetic CISA KEV polling demonstration without external network access."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from openswarm.adapters.cisa_kev import CisaKevSource
from openswarm.events import SQLiteEventBus
from openswarm.ingestion import IngestionSupervisor


def _record(cve_id: str, date_added: str) -> dict[str, str]:
    return {
        "cveID": cve_id,
        "vendorProject": "Synthetic Vendor",
        "product": "Synthetic Product",
        "vulnerabilityName": "Synthetic vulnerability record",
        "dateAdded": date_added,
        "shortDescription": "Synthetic test data only.",
        "requiredAction": "Review vendor mitigation guidance.",
        "dueDate": "2026-12-01",
        "knownRansomwareCampaignUse": "Unknown",
        "notes": "",
    }


def _snapshot(records: list[dict[str, str]], marker: str) -> dict[str, object]:
    return {
        "sha256": hashlib.sha256(marker.encode()).hexdigest(),
        "source_url": "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json",
        "catalog": {"vulnerabilities": records},
    }


def main() -> None:
    feeds = [
        _snapshot([_record("CVE-2026-10001", "2026-11-01")], "initial"),
        _snapshot(
            [
                _record("CVE-2026-10001", "2026-11-01"),
                _record("CVE-2026-10002", "2026-11-02"),
            ],
            "updated",
        ),
    ]
    source = CisaKevSource(fetcher=lambda: feeds.pop(0))
    with tempfile.TemporaryDirectory(prefix="openswarm-ingestion-") as directory:
        database = Path(directory) / "events.sqlite3"
        bus = SQLiteEventBus(database)
        supervisor = IngestionSupervisor(bus)
        start = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
        supervisor.register_source(
            "hco.cisa-kev", source, interval_seconds=300, now=start
        )
        baseline = supervisor.run_source_once("hco.cisa-kev", now=start)
        update = supervisor.run_source_once(
            "hco.cisa-kev", force=True, now=start + timedelta(minutes=5)
        )
        with sqlite3.connect(database) as db:
            events = db.execute(
                "SELECT event_type,payload_json FROM platform_events ORDER BY sequence"
            ).fetchall()
        print(
            json.dumps(
                {
                    "baseline_status": baseline.status,
                    "baseline_events": baseline.event_count,
                    "update_status": update.status,
                    "update_events": update.event_count,
                    "cve_ids": [
                        json.loads(payload)["cve_id"] for _kind, payload in events
                    ],
                    "external_network_used": False,
                    "mode": "synthetic_feed_fixture",
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
