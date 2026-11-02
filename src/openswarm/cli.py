from __future__ import annotations

import argparse
import json

from openswarm.models import Task, TaskStatus
from openswarm.queue import TaskQueue
from openswarm.runtime import SwarmRuntime, Worker


def main() -> None:
    parser = argparse.ArgumentParser(prog="openswarm")
    parser.add_argument("--db", default=".openswarm/state.sqlite3")
    sub = parser.add_subparsers(dest="command", required=True)
    submit = sub.add_parser("submit")
    submit.add_argument("--worker", default="general")
    submit.add_argument("--adapter", choices=["mock", "ollama"], default="mock")
    submit.add_argument("--model")
    submit.add_argument("--task", required=True)
    submit.add_argument("--capability", action="append", default=[])
    submit.add_argument("--requires-approval", action="store_true")
    approve = sub.add_parser("approve", help="approve a task waiting for human approval")
    approve.add_argument("task_id")
    reject = sub.add_parser("reject", help="reject a task waiting for human approval")
    reject.add_argument("task_id")
    sub.add_parser("run-once")
    tasks = sub.add_parser("tasks")
    tasks.add_argument("--status", choices=[status.value for status in TaskStatus])
    tasks.add_argument("--limit", type=int, default=100)
    audit = sub.add_parser("audit")
    audit.add_argument("--limit", type=int, default=100)
    sub.add_parser("demo")
    args = parser.parse_args()
    queue = TaskQueue(args.db)
    if args.command == "submit":
        task = queue.submit(
            Task(
                worker=args.worker,
                prompt=args.task,
                adapter=args.adapter,
                model=args.model,
                capabilities=args.capability,
                requires_approval=args.requires_approval,
            )
        )
        print(task.task_id)
    elif args.command == "approve":
        print(json.dumps(queue.approve(args.task_id).to_dict(), indent=2))
    elif args.command == "reject":
        print(json.dumps(queue.reject(args.task_id).to_dict(), indent=2))
    elif args.command == "run-once":
        task = SwarmRuntime(queue).run_once()
        print(json.dumps(task.to_dict() if task else {"status": "idle"}, indent=2))
    elif args.command == "tasks":
        status = TaskStatus(args.status) if args.status else None
        print(json.dumps([task.to_dict() for task in queue.list(status, args.limit)], indent=2))
    elif args.command == "audit":
        print(json.dumps(queue.audit(args.limit), indent=2))
    elif args.command == "demo":
        submitted = queue.submit(
            Task(worker="general", prompt="Describe a safe agent handoff")
        )
        result = SwarmRuntime(queue, workers=[Worker("general")]).run_once()
        print(
            json.dumps(
                {
                    "submitted": submitted.task_id,
                    "result": result.to_dict() if result else None,
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
