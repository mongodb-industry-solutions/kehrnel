import pytest

from kehrnel.persistence.artifacts import FileSystemArtifactStore


@pytest.mark.asyncio
async def test_filesystem_artifact_store_is_idempotent_and_replayable(tmp_path):
    store = FileSystemArtifactStore(tmp_path)
    content = b'{"datasetJSONVersion":"1.1.0"}\n'

    first = await store.put("sha256/ab/example", content, media_type="application/json")
    second = await store.put("sha256/ab/example", content, media_type="application/json")

    assert first.uri == second.uri
    assert first.size == len(content)
    assert await store.get("sha256/ab/example") == content


@pytest.mark.asyncio
async def test_filesystem_artifact_store_rejects_traversal_and_overwrite(tmp_path):
    store = FileSystemArtifactStore(tmp_path)

    with pytest.raises(ValueError, match="unsafe"):
        await store.put("../escape", b"bad", media_type="application/octet-stream")

    await store.put("sha256/ab/example", b"first", media_type="application/octet-stream")
    with pytest.raises(ValueError, match="different bytes"):
        await store.put("sha256/ab/example", b"second", media_type="application/octet-stream")


@pytest.mark.asyncio
async def test_filesystem_artifact_store_stat_reports_streamed_digest(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "objects")
    await store.put("sha256/aa/value", b"content", media_type="application/octet-stream")

    location = await store.stat("sha256/aa/value")

    assert location.size == 7
    assert location.metadata["sha256"] == "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73"
