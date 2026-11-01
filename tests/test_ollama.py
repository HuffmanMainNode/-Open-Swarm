import pytest
from openswarm.adapters.ollama import OllamaAdapter

def test_remote_http_is_rejected():
    with pytest.raises(ValueError):
        OllamaAdapter("http://192.168.1.20:11434")

def test_local_http_is_allowed():
    assert OllamaAdapter("http://127.0.0.1:11434").base_url.endswith("11434")
