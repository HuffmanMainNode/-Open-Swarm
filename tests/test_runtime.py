from openswarm.models import Task, TaskStatus
from openswarm.queue import TaskQueue
from openswarm.runtime import SwarmRuntime, Worker


def test_mock_worker_executes_task(tmp_path):
    queue = TaskQueue(tmp_path / "runtime.sqlite3")
    submitted = queue.submit(Task(worker="research", prompt="summarize public data"))
    result = SwarmRuntime(queue, workers=[Worker("research")]).run_once()
    assert result.task_id == submitted.task_id
    assert result.status == TaskStatus.SUCCEEDED
    assert "[mock response]" in result.result


def test_unknown_worker_is_blocked(tmp_path):
    queue = TaskQueue(tmp_path / "runtime.sqlite3")
    queue.submit(Task(worker="missing", prompt="hello"))
    assert SwarmRuntime(queue).run_once().status == TaskStatus.BLOCKED


def test_missing_capability_is_blocked(tmp_path):
    queue = TaskQueue(tmp_path / "runtime.sqlite3")
    queue.submit(Task(worker="general", prompt="fetch", capabilities=["network"]))
    assert SwarmRuntime(queue).run_once().status == TaskStatus.BLOCKED


def test_unapproved_task_is_not_claimed_or_run(tmp_path):
    queue = TaskQueue(tmp_path / "runtime.sqlite3")
    task = queue.submit(
        Task(worker="general", prompt="wait for operator", requires_approval=True)
    )
    assert SwarmRuntime(queue).run_once() is None
    assert queue.get(task.task_id).status == TaskStatus.PENDING_APPROVAL


def test_approved_task_runs(tmp_path):
    queue = TaskQueue(tmp_path / "runtime.sqlite3")
    task = queue.submit(
        Task(worker="general", prompt="approved", requires_approval=True)
    )
    queue.approve(task.task_id)
    result = SwarmRuntime(queue).run_once()
    assert result.status == TaskStatus.SUCCEEDED
