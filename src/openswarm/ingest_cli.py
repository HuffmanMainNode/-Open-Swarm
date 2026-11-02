"""Operator CLI for the optional CISA KEV ingestion source."""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict

from openswarm.adapters.cisa_kev import CisaKevSource
from openswarm.events import SQLiteEventBus
from openswarm.ingestion import IngestionSupervisor


def main() -> None:
    parser = argparse.ArgumentParser(prog="openswarm-ingest-cisa-kev")
    parser.add_argument("--events-db", default=".openswarm/events.sqlite3")
    parser.add_argument("--interval", type=int, default=300)
    parser.add_argument("--initial-backfill", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--idle-sleep", type=float, default=1.0)
    args = parser.parse_args()

    bus = SQLiteEventBus(args.events_db)
    supervisor = IngestionSupervisor(bus)
    supervisor.register_source(
        "hco.cisa-kev",
        CisaKevSource(initial_backfill=args.initial_backfill),
        interval_seconds=args.interval,
    )
    if args.status:
        print(json.dumps([asdict(item) for item in supervisor.status()], indent=2))
    elif args.once:
        result = supervisor.run_source_once("hco.cisa-kev", force=True)
        print(json.dumps(asdict(result), indent=2))
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
        )
        try:
            supervisor.run_forever(idle_sleep_seconds=args.idle_sleep)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
