import os
import sqlite3
import stat

import pytest

from openswarm.models import Task, TaskStatus
from openswarm.queue import (
    InvalidTaskStateError,
    LeaseOwnershipError,
    QueueFullError,
    TaskQueue,
)


def test_submit_claim_finish_and_audit(tmp_path):
    queue = TaskQueue(tmp_path / "state.sqlite3")
    task = queue.submit(Task(worker="general", prompt="hello"))
    claimed = queue.claim()
    assert claimed.task_id == task.task_id and claimed.status == TaskStatus.RUNNING
    queue.finish(claimed, TaskStatus.SUCCEEDED, result="done")
    assert queue.get(task.task_id).result == "done"
    assert {event["event_type"] for event in queue.audit()} >= {
        "task.submitted", "task.claimed", "task.succeeded"
    }


def test_queue_capacity(tmp_path):
    queue = TaskQueue(tmp_path / "state.sqlite3", max_queued=1)
    queue.submit(Task(worker="general", prompt="one"))
    with pytest.raises(QueueFullError):
        queue.submit(Task(worker="general", prompt="two"))


def test_queue_persists_after_reopen(tmp_path):
    path = tmp_path / "state.sqlite3"
    task = TaskQueue(path).submit(Task(worker="general", prompt="persist"))
    assert TaskQueue(path).get(task.task_id).prompt == "persist"


def _expire_lease(path, task_id):
    with sqlite3.connect(path) as db:
        db.execute(
            "UPDATE tasks SET updated_at=? WHERE task_id=?",
            ("2000-01-01T00:00:00+00:00", task_id),
        )


def test_poison_task_fails_after_max_attempts_across_crashes(tmp_path):
    path = tmp_path / "state.sqlite3"
    queue = TaskQueue(path)
    task = queue.submit(Task(worker="general", prompt="crash", max_attempts=2))
    first = queue.claim(lease_seconds=1)
    assert first.attempts == 1
    _expire_lease(path, task.task_id)
    second = queue.claim(lease_seconds=1)
    assert second.attempts == 2
    _expire_lease(path, task.task_id)
    assert queue.claim(lease_seconds=1) is None
    assert queue.get(task.task_id).status == TaskStatus.FAILED
    assert any(event["event_type"] == "task.lease_exhausted" for event in queue.audit())


def test_stale_worker_cannot_overwrite_new_lease_result(tmp_path):
    path = tmp_path / "state.sqlite3"
    queue = TaskQueue(path)
    task = queue.submit(Task(worker="general", prompt="slow"))
    stale_worker = queue.claim(lease_seconds=1)
    _expire_lease(path, task.task_id)
    current_worker = queue.claim(lease_seconds=1)
    assert stale_worker.lease_id != current_worker.lease_id
    with pytest.raises(LeaseOwnershipError):
        queue.finish(stale_worker, TaskStatus.SUCCEEDED, result="stale")
    queue.finish(current_worker, TaskStatus.SUCCEEDED, result="current")
    assert queue.get(task.task_id).result == "current"
    assert any(event["event_type"] == "task.finish_rejected" for event in queue.audit())


def test_second_worker_cannot_claim_same_active_task(tmp_path):
    queue = TaskQueue(tmp_path / "state.sqlite3")
    queue.submit(Task(worker="general", prompt="one"))
    assert queue.claim() is not None
    assert queue.claim() is None


def test_approval_required_task_waits_until_approved(tmp_path):
    queue = TaskQueue(tmp_path / "state.sqlite3")
    task = queue.submit(
        Task(worker="general", prompt="approval", requires_approval=True)
    )
    assert task.status == TaskStatus.PENDING_APPROVAL
    assert queue.claim() is None
    approved = queue.approve(task.task_id)
    assert approved.status == TaskStatus.QUEUED and approved.approved
    assert queue.claim().task_id == task.task_id
    assert any(event["event_type"] == "task.approved" for event in queue.audit())


def test_rejected_task_never_enters_queue(tmp_path):
    queue = TaskQueue(tmp_path / "state.sqlite3")
    task = queue.submit(
        Task(worker="general", prompt="reject", requires_approval=True)
    )
    rejected = queue.reject(task.task_id)
    assert rejected.status == TaskStatus.REJECTED
    assert queue.claim() is None
    with pytest.raises(InvalidTaskStateError):
        queue.approve(task.task_id)


def test_task_listing_uses_newest_first_for_all_filters(tmp_path):
    queue = TaskQueue(tmp_path / "state.sqlite3")
    first = queue.submit(Task(worker="general", prompt="first"))
    second = queue.submit(Task(worker="general", prompt="second"))
    assert [task.task_id for task in queue.list()] == [second.task_id, first.task_id]
    assert [task.task_id for task in queue.list(TaskStatus.QUEUED)] == [
        second.task_id, first.task_id
    ]



@pytest.mark.skipif(os.name != "posix", reason="POSIX permission bits are unavailable")
def test_default_state_directory_and_database_are_private(tmp_path):
    database = tmp_path / ".openswarm" / "state.sqlite3"
    TaskQueue(database)
    assert stat.S_IMODE(database.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(database.stat().st_mode) == 0o600


@pytest.mark.skipif(os.name != "posix", reason="POSIX symlink semantics required")
def test_queue_refuses_symlink_database(tmp_path):
    target = tmp_path / "target.sqlite3"
    target.touch()
    link = tmp_path / "state.sqlite3"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="must not be a symlink"):
        TaskQueue(link)
