from openswarm.models import Task
from openswarm.policy import Policy

def test_capability_gate_fails_closed():
    ok, reason = Policy().check(Task(worker="x", prompt="do",
                                     capabilities=["network"]), set())
    assert not ok and "network" in reason

def test_approval_gate():
    task = Task(worker="x", prompt="deploy", requires_approval=True)
    assert not Policy().check(task, set())[0]
    assert Policy().check(task, set(), approved=True)[0]

def test_output_limit():
    assert not Policy(max_output_chars=3).check_output("long")[0]
