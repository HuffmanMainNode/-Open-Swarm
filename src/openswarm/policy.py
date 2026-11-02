from dataclasses import dataclass

from openswarm.models import Task


@dataclass(slots=True)
class Policy:
    max_prompt_chars: int = 12000
    max_output_chars: int = 20000
    allowed_adapters: tuple[str, ...] = ("mock", "ollama")
    denied_prompt_markers: tuple[str, ...] = ("<|system_override|>",)

    def check(
        self, task: Task, worker_capabilities: set[str], approved: bool = False
    ) -> tuple[bool, str]:
        if not task.prompt.strip():
            return False, "empty task prompt"
        if len(task.prompt) > self.max_prompt_chars:
            return False, "prompt exceeds configured size limit"
        if task.adapter not in self.allowed_adapters:
            return False, f"adapter is not allowed: {task.adapter}"
        if any(marker in task.prompt for marker in self.denied_prompt_markers):
            return False, "prompt contains a denied control marker"
        missing = set(task.capabilities) - worker_capabilities
        if missing:
            return False, f"worker lacks capabilities: {', '.join(sorted(missing))}"
        if task.requires_approval and not approved:
            return False, "human approval required"
        return True, "allowed"

    def check_output(self, output: str) -> tuple[bool, str]:
        if len(output) > self.max_output_chars:
            return False, "model output exceeds configured size limit"
        return True, "allowed"
