from __future__ import annotations

import json

import pytest

from openswarm.adapters.cisa_kev import CisaKevSource


def record(cve, added, *, notes=""):
    return {
        "cveID": cve,
        "vendorProject": "Example Vendor",
        "product": "Example Product",
        "vulnerabilityName": "Example Vulnerability",
        "dateAdded": added,
        "shortDescription": "Synthetic test record.",
        "requiredAction": "Apply the vendor mitigation.",
        "dueDate": "2026-12-01",
        "knownRansomwareCampaignUse": "Unknown",
        "notes": notes,
    }


def snapshot(records, digest):
    return {
        "sha256": digest * 64,
        "source_url": "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json",
        "source_date": "2026-11-02",
        "catalog": {"vulnerabilities": records},
    }


def test_default_first_poll_establishes_baseline_without_historical_flood():
    source = CisaKevSource(fetcher=lambda: snapshot(
        [record("CVE-2026-10001", "2026-11-01")], "a"
    ))

    batch = source.poll(None)

    assert batch.events == []
    state = json.loads(batch.checkpoint)
    assert state["mode"] == "date"
    assert state["last_date"] == "2026-11-01"


def test_changed_feed_emits_same_day_and_newer_records_with_stable_ids():
    feeds = [
        snapshot([record("CVE-2026-10001", "2026-11-01")], "a"),
        snapshot(
            [
                record("CVE-2026-10001", "2026-11-01"),
                record("CVE-2026-10002", "2026-11-01"),
                record("CVE-2026-10003", "2026-11-02"),
            ],
            "b",
        ),
    ]
    source = CisaKevSource(fetcher=lambda: feeds.pop(0))
    baseline = source.poll(None)
    batch = source.poll(baseline.checkpoint)

    assert [item.payload["cve_id"] for item in batch.events] == [
        "CVE-2026-10001", "CVE-2026-10002", "CVE-2026-10003"
    ]
    assert all(item.event_type == "intelligence.vulnerability.kev_added" for item in batch.events)
    assert all(item.classification == "internal" for item in batch.events)
    assert len({item.event_id for item in batch.events}) == 3
    assert json.loads(batch.checkpoint)["last_date"] == "2026-11-02"


def test_unchanged_feed_does_not_reemit_events():
    feed = snapshot([record("CVE-2026-10001", "2026-11-01")], "a")
    source = CisaKevSource(fetcher=lambda: feed)
    baseline = source.poll(None)

    batch = source.poll(baseline.checkpoint)

    assert batch.events == []
    assert batch.checkpoint == baseline.checkpoint


def test_initial_backfill_is_bounded_and_resumes_from_checkpoint():
    records = [
        record(f"CVE-2026-{10000 + index}", "2026-10-01")
        for index in range(1001)
    ]
    feed = snapshot(records, "c")
    source = CisaKevSource(initial_backfill=True, fetcher=lambda: feed)

    first = source.poll(None)
    second = source.poll(first.checkpoint)

    assert len(first.events) == 1000
    assert json.loads(first.checkpoint)["offset"] == 1000
    assert len(second.events) == 1
    assert json.loads(second.checkpoint)["mode"] == "date"


def test_invalid_feed_record_is_rejected():
    source = CisaKevSource(
        fetcher=lambda: snapshot([record("NOT-A-CVE", "2026-11-01")], "d")
    )
    baseline = source.poll(None)
    changed = snapshot([record("NOT-A-CVE", "2026-11-02")], "e")
    source.fetcher = lambda: changed

    with pytest.raises(ValueError, match="invalid CVE"):
        source.poll(baseline.checkpoint)


def test_malformed_checkpoint_is_rejected():
    source = CisaKevSource(fetcher=lambda: snapshot([], "f"))
    with pytest.raises(ValueError, match="checkpoint"):
        source.poll("not-json")
