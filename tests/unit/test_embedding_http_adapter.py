import urllib.request

import pytest

from kehrnel.persistence.embeddings import HttpEmbeddingAdapter
from kehrnel.persistence.embeddings.http import _RedirectGuard


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


def test_redirect_guard_blocks_redirects_that_would_leak_the_credential():
    guard = _RedirectGuard(True, "https://embeddings.internal")
    request = urllib.request.Request("https://embeddings.internal/embed", method="POST")
    for newurl in (
        "http://embeddings.internal/embed",  # downgrades the credential to plain HTTP
        "https://elsewhere.internal/embed",  # forwards it to another origin
    ):
        with pytest.raises(ValueError):
            guard.redirect_request(request, fp=None, code=302, msg="Found", headers={}, newurl=newurl)


def test_redirect_guard_accepts_a_same_origin_https_redirect_with_the_credential():
    guard = _RedirectGuard(True, "https://embeddings.internal")
    request = urllib.request.Request("https://embeddings.internal/embed")
    # same-origin HTTPS falls through to urllib's own redirect handling: no leak
    guard.redirect_request(request, fp=None, code=302, msg="Found", headers={}, newurl="https://embeddings.internal/embed/")


def test_redirect_guard_lets_credential_free_redirects_through():
    guard = _RedirectGuard(False, "https://embeddings.internal")
    request = urllib.request.Request("https://embeddings.internal/embed")
    guard.redirect_request(request, fp=None, code=302, msg="Found", headers={}, newurl="http://elsewhere.internal/embed")
