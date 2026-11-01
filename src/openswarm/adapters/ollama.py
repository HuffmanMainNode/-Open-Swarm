import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

class OllamaAdapter:
    def __init__(self, base_url: str = "http://127.0.0.1:11434"):
        local = base_url.startswith(("http://127.0.0.1", "http://localhost", "http://[::1]"))
        if not local and not base_url.startswith("https://"):
            raise ValueError("Ollama URL must be localhost HTTP or remote HTTPS")
        self.base_url = base_url.rstrip("/")

    def generate(self, prompt: str, model: str | None = None,
                 timeout_seconds: int = 60) -> str:
        if not model:
            raise ValueError("an Ollama model name is required")
        body = json.dumps({"model": model, "prompt": prompt, "stream": False}).encode()
        request = Request(self.base_url + "/api/generate", data=body,
                          headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                raw = response.read(2_000_001)
        except (HTTPError, URLError, TimeoutError) as exc:
            raise RuntimeError(f"Ollama request failed: {exc}") from exc
        if len(raw) > 2_000_000:
            raise RuntimeError("Ollama response exceeded 2 MB")
        result = json.loads(raw).get("response")
        if not isinstance(result, str):
            raise RuntimeError("Ollama response did not contain text")
        return result
