import pytest

from openswarm.adapters.ollama import OllamaAdapter


@pytest.mark.parametrize("url", [
    "file:///etc/passwd",
    "ftp://example.com:11434",
    "gopher://example.com",
    "http://192.168.1.20:11434",
    "http://user:pass@localhost:11434",
    "http://localhost:11434/#fragment",
    "http://localhost:99999",
])
def test_unsafe_ollama_urls_are_rejected(url):
    with pytest.raises(ValueError):
        OllamaAdapter(url)


@pytest.mark.parametrize("url", [
    "http://127.0.0.1:11434",
    "http://localhost:11434",
    "http://[::1]:11434",
    "https://ollama.example:11434",
])
def test_allowed_ollama_urls(url):
    assert OllamaAdapter(url).base_url == url


def test_remote_http_is_rejected():
    with pytest.raises(ValueError):
        OllamaAdapter("http://192.168.1.20:11434")


def test_local_http_is_allowed():
    assert OllamaAdapter("http://127.0.0.1:11434").base_url.endswith("11434")
