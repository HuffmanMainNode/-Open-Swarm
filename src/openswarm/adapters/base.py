from typing import Protocol

class ModelAdapter(Protocol):
    def generate(self, prompt: str, model: str | None = None,
                 timeout_seconds: int = 60) -> str: ...
