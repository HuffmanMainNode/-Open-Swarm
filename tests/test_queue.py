import pytest
from openswarm.models import Task, TaskStatus
from openswarm.queue import QueueFullError, TaskQueue

def test_submit_claim_finish_and_audit(tmp_path):
    q = TaskQueue(tmp_path / "state.sqlite3")
    t = q.submit(Task(worker="general", prompt="hello"))
    claimed = q.claim()
    assert claimed.task_id == t.task_id and claimed.status == TaskStatus.RUNNING
    q.finish(claimed, TaskStatus.SUCCEEDED, result="done")
    assert q.get(t.task_id).result == "done"
    assert {e["event_type"] for e in q.audit()} >= {
        "task.submitted", "task.claimed", "task.succeeded"}

def test_queue_capacity(tmp_path):
    q = TaskQueue(tmp_path / "state.sqlite3", max_queued=1)
    q.submit(Task(worker="general", prompt="one"))
    with pytest.raises(QueueFullError):
        q.submit(Task(worker="general", prompt="two"))

def test_queue_persists_after_reopen(tmp_path):
    path = tmp_path / "state.sqlite3"
    task = TaskQueue(path).submit(Task(worker="general", prompt="persist"))
    assert TaskQueue(path).get(task.task_id).prompt == "persist"
