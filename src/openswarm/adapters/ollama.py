from __future__ import annotations

import json
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

_LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}


class OllamaAdapter:
    def __init__(self, base_url: str = "http://127.0.0.1:11434") -> None:
        parsed = urlsplit(base_url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("Ollama URL scheme must be http or https")
        if not parsed.hostname:
            raise ValueError("Ollama URL must include a hostname")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("Ollama URL must not contain credentials")
        if parsed.fragment:
            raise ValueError("Ollama URL must not contain a fragment")
        if parsed.scheme == "http" and parsed.hostname.lower() not in _LOCAL_HOSTS:
            raise ValueError("plain HTTP is allowed only for loopback Ollama endpoints")
        if parsed.query:
            raise ValueError("Ollama base URL must not contain a query")
        if parsed.path not in {"", "/"}:
            raise ValueError("Ollama base URL must not contain an API path")
        try:
            port = parsed.port
            if port == 0:
                raise ValueError("Ollama URL port must be positive")
        except ValueError as exc:
            raise ValueError("Ollama URL contains an invalid port") from exc
        self.base_url = base_url.rstrip("/")

    def generate(
        self, prompt: str, model: str | None = None, timeout_seconds: int = 60
    ) -> str:
        if not model:
            raise ValueError("an Ollama model name is required")
        body = json.dumps({"model": model, "prompt": prompt, "stream": False}).encode()
        request = Request(
            self.base_url + "/api/generate",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            # The constructor restricts schemes and rejects non-loopback HTTP.
            with urlopen(request, timeout=timeout_seconds) as response:  # nosec B310
                raw = response.read(2_000_001)
        except (HTTPError, URLError, TimeoutError) as exc:
            raise RuntimeError(f"Ollama request failed: {exc}") from exc
        if len(raw) > 2_000_000:
            raise RuntimeError("Ollama response exceeded 2 MB")
        result = json.loads(raw).get("response")
        if not isinstance(result, str):
            raise RuntimeError("Ollama response did not contain text")
        return result
