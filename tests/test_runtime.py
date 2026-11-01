from openswarm.models import Task, TaskStatus
from openswarm.queue import TaskQueue
from openswarm.runtime import SwarmRuntime, Worker

def test_mock_worker_executes_task(tmp_path):
    q = TaskQueue(tmp_path / "runtime.sqlite3")
    submitted = q.submit(Task(worker="research", prompt="summarize public data"))
    result = SwarmRuntime(q, workers=[Worker("research")]).run_once()
    assert result.task_id == submitted.task_id
    assert result.status == TaskStatus.SUCCEEDED
    assert "[mock response]" in result.result

def test_unknown_worker_is_blocked(tmp_path):
    q = TaskQueue(tmp_path / "runtime.sqlite3")
    q.submit(Task(worker="missing", prompt="hello"))
    assert SwarmRuntime(q).run_once().status == TaskStatus.BLOCKED

def test_missing_capability_is_blocked(tmp_path):
    q = TaskQueue(tmp_path / "runtime.sqlite3")
    q.submit(Task(worker="general", prompt="fetch", capabilities=["network"]))
    assert SwarmRuntime(q).run_once().status == TaskStatus.BLOCKED
