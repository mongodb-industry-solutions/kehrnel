import pytest

from kehrnel.persistence.embeddings import HttpEmbeddingAdapter


def test_http_endpoint_rejects_a_bearer_credential():
    with pytest.raises(ValueError):
        HttpEmbeddingAdapter("http://embeddings.internal:8080/embed", api_key="secret")


def test_https_endpoint_accepts_a_bearer_credential():
    adapter = HttpEmbeddingAdapter("https://embeddings.internal:8443/embed", api_key="secret")

    assert adapter.endpoint == "https://embeddings.internal:8443/embed"
    assert adapter.api_key == "secret"


def test_http_endpoint_stays_available_without_credentials():
    adapter = HttpEmbeddingAdapter("http://localhost:8080/embed")

    assert adapter.endpoint == "http://localhost:8080/embed"


def test_http_endpoint_rejects_a_credential_passed_as_a_header():
    with pytest.raises(ValueError):
        HttpEmbeddingAdapter(
            "http://embeddings.internal:8080/embed", headers={"Authorization": "Bearer secret"}
        )
