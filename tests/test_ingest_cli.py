from __future__ import annotations

import json
import sys

from openswarm.ingest_cli import main
from openswarm.ingestion import SourceBatch


class FakeKevSource:
    def __init__(self, *, initial_backfill=False):
        self.initial_backfill = initial_backfill

    def poll(self, checkpoint):
        return SourceBatch([], "synthetic-checkpoint")


def test_ingest_cli_once_uses_local_database_and_emits_status(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.setattr("openswarm.ingest_cli.CisaKevSource", FakeKevSource)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "openswarm-ingest-cisa-kev",
            "--events-db",
            str(tmp_path / "events.sqlite3"),
            "--once",
        ],
    )

    main()

    result = json.loads(capsys.readouterr().out)
    assert result["source_id"] == "hco.cisa-kev"
    assert result["status"] == "succeeded"
    assert result["event_count"] == 0


def test_ingest_cli_status_reports_persistent_source_state(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.setattr("openswarm.ingest_cli.CisaKevSource", FakeKevSource)
    database = tmp_path / "events.sqlite3"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "openswarm-ingest-cisa-kev",
            "--events-db",
            str(database),
            "--status",
        ],
    )

    main()
    first = json.loads(capsys.readouterr().out)
    assert first[0]["source_id"] == "hco.cisa-kev"

    main()
    second = json.loads(capsys.readouterr().out)
    assert second[0]["source_id"] == "hco.cisa-kev"
    assert second[0]["interval_seconds"] == 300
