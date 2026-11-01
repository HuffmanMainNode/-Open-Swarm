class MockAdapter:
    def generate(self, prompt: str, model: str | None = None,
                 timeout_seconds: int = 60) -> str:
        del model, timeout_seconds
        return f"[mock response] Received {len(prompt)} characters. Preview: {prompt[:240]}"
