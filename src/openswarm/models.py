from __future__ import annotations
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

class TaskStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED = "blocked"

@dataclass(slots=True)
class Task:
    worker: str
    prompt: str
    adapter: str = "mock"
    model: str | None = None
    capabilities: list[str] = field(default_factory=list)
    requires_approval: bool = False
    max_attempts: int = 2
    task_id: str = field(default_factory=lambda: str(uuid4()))
    status: TaskStatus = TaskStatus.QUEUED
    attempts: int = 0
    result: str | None = None
    error: str | None = None
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["status"] = self.status.value
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Task:
        data = dict(value)
        data["status"] = TaskStatus(data.get("status", "queued"))
        return cls(**data)

@dataclass(slots=True)
class AuditEvent:
    event_type: str
    task_id: str | None
    detail: dict[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: str(uuid4()))
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
