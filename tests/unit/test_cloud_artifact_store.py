import hashlib
import io

import pytest

from kehrnel.persistence.artifacts import S3ArtifactStore


class MissingObject(Exception):
    response = {"Error": {"Code": "404"}}


class PreconditionFailed(Exception):
    response = {"Error": {"Code": "PreconditionFailed"}}


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.race_writer = None

    def head_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise MissingObject()
        value = self.objects[(Bucket, Key)]
        return {"ContentLength": len(value["Body"]), "Metadata": value["Metadata"]}

    def put_object(self, **kwargs):
        key = (kwargs["Bucket"], kwargs["Key"])
        if self.race_writer is not None:
            # Simulate another writer creating the key between head_object and put_object.
            self.objects[key] = self.race_writer
            self.race_writer = None
        if kwargs.get("IfNoneMatch") == "*" and key in self.objects:
            raise PreconditionFailed()
        self.objects[key] = kwargs

    def get_object(self, *, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)]["Body"])}

    def generate_presigned_url(self, operation, *, Params, ExpiresIn, HttpMethod):
        return f"https://signed.example/{operation}/{Params['Bucket']}/{Params['Key']}?expires={ExpiresIn}&method={HttpMethod}"


@pytest.mark.asyncio
async def test_s3_store_is_immutable_and_prefix_scoped():
    client = FakeS3()
    store = S3ArtifactStore("trial-data", prefix="tenant-a", client=client, server_side_encryption="AES256")

    location = await store.put("sha256/aa/value", b"content", media_type="application/octet-stream")
    repeated = await store.put("sha256/aa/value", b"content", media_type="application/octet-stream")

    assert location.uri == "s3://trial-data/tenant-a/sha256/aa/value"
    assert repeated == location
    assert await store.get("sha256/aa/value") == b"content"
    assert (await store.stat("sha256/aa/value")).metadata["sha256"] == location.metadata["sha256"]
    assert client.objects[("trial-data", "tenant-a/sha256/aa/value")]["ServerSideEncryption"] == "AES256"
    with pytest.raises(ValueError):
        await store.put("sha256/aa/value", b"different", media_type="application/octet-stream")
    with pytest.raises(ValueError):
        await store.get("../escape")


@pytest.mark.asyncio
async def test_s3_put_losing_a_race_never_overwrites_the_winner():
    client = FakeS3()
    store = S3ArtifactStore("trial-data", prefix="tenant-a", client=client)
    key = "sha256/aa/raced"
    # The other writer lands first, with different bytes, after our existence check.
    client.race_writer = {
        "Bucket": "trial-data",
        "Key": "tenant-a/sha256/aa/raced",
        "Body": b"someone-else",
        "Metadata": {"sha256": hashlib.sha256(b"someone-else").hexdigest()},
    }

    with pytest.raises(ValueError):
        await store.put(key, b"ours", media_type="application/octet-stream")

    assert client.objects[("trial-data", "tenant-a/sha256/aa/raced")]["Body"] == b"someone-else"


@pytest.mark.asyncio
async def test_s3_put_hashes_the_stored_object_before_accepting_it_as_identical():
    client = FakeS3()
    store = S3ArtifactStore("trial-data", prefix="tenant-a", client=client)
    key = "sha256/aa/forged"
    await store.put(key, b"content", media_type="application/octet-stream")
    # A presigned upload can swap the bytes while keeping the declared length and
    # the sha256 metadata, so the store must hash the object before accepting it.
    client.objects[("trial-data", "tenant-a/sha256/aa/forged")]["Body"] = b"altered"

    with pytest.raises(ValueError):
        await store.put(key, b"content", media_type="application/octet-stream")

    # The same holds when another writer wins the race with forged metadata.
    client.race_writer = {
        "Bucket": "trial-data",
        "Key": "tenant-a/sha256/aa/raced",
        "Body": b"evil",
        "Metadata": {"sha256": hashlib.sha256(b"ours").hexdigest()},
    }
    with pytest.raises(ValueError):
        await store.put("sha256/aa/raced", b"ours", media_type="application/octet-stream")

    assert client.objects[("trial-data", "tenant-a/sha256/aa/raced")]["Body"] == b"evil"


@pytest.mark.asyncio
async def test_s3_direct_transfer_targets_are_prefix_scoped_and_checksum_bound():
    client = FakeS3()
    store = S3ArtifactStore("trial-data", prefix="tenant-a", client=client)
    digest = "a" * 64

    upload = await store.create_upload(
        f"sha256/aa/{digest}",
        media_type="application/x-sas-xport",
        size=123,
        sha256=digest,
        expires_in=600,
    )
    download = await store.create_download(f"sha256/aa/{digest}", expires_in=600)

    assert upload["method"] == "PUT"
    assert upload["headers"]["x-amz-meta-sha256"] == digest
    assert upload["headers"]["If-None-Match"] == "*"
    assert "tenant-a/sha256/aa/" in upload["url"]
    assert download["method"] == "GET"
    assert "tenant-a/sha256/aa/" in download["url"]
