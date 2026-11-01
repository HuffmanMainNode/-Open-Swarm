from dataclasses import dataclass, field
from typing import Callable
from openswarm.adapters.mock import MockAdapter
from openswarm.adapters.ollama import OllamaAdapter
from openswarm.models import Task, TaskStatus
from openswarm.policy import Policy
from openswarm.queue import TaskQueue

@dataclass(slots=True)
class Worker:
    name: str
    capabilities: set[str] = field(default_factory=set)
    max_concurrency: int = 1

class SwarmRuntime:
    def __init__(self, queue: TaskQueue, policy: Policy | None = None,
                 workers: list[Worker] | None = None,
                 approval: Callable[[Task], bool] | None = None):
        self.queue = queue
        self.policy = policy or Policy()
        self.workers = {w.name: w for w in (workers or [Worker("general")])}
        if any(w.max_concurrency < 1 for w in self.workers.values()):
            raise ValueError("worker max_concurrency must be positive")
        self.approval = approval or (lambda task: False)
        self.adapters = {"mock": MockAdapter(), "ollama": OllamaAdapter()}

    def register_adapter(self, name: str, adapter: object) -> None:
        if not name or not callable(getattr(adapter, "generate", None)):
            raise ValueError("adapter must implement generate")
        self.adapters[name] = adapter

    def run_once(self) -> Task | None:
        task = self.queue.claim()
        if task is None:
            return None
        worker = self.workers.get(task.worker)
        if worker is None:
            return self.queue.finish(task, TaskStatus.BLOCKED, error=f"unknown worker: {task.worker}")
        approved = self.approval(task) if task.requires_approval else False
        allowed, reason = self.policy.check(task, worker.capabilities, approved)
        if not allowed:
            return self.queue.finish(task, TaskStatus.BLOCKED, error=reason)
        adapter = self.adapters.get(task.adapter)
        if adapter is None:
            return self.queue.finish(task, TaskStatus.BLOCKED, error=f"unknown adapter: {task.adapter}")
        try:
            output = adapter.generate(task.prompt, task.model, timeout_seconds=60)
            allowed, reason = self.policy.check_output(output)
            if not allowed:
                return self.queue.finish(task, TaskStatus.BLOCKED, error=reason)
            return self.queue.finish(task, TaskStatus.SUCCEEDED, result=output)
        except Exception as exc:
            status = TaskStatus.QUEUED if task.attempts < task.max_attempts else TaskStatus.FAILED
            return self.queue.finish(task, status, error=str(exc))
