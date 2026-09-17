import io

import pytest

from kehrnel.persistence.artifacts import S3ArtifactStore


class MissingObject(Exception):
    response = {"Error": {"Code": "404"}}


class FakeS3:
    def __init__(self):
        self.objects = {}

    def head_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise MissingObject()
        value = self.objects[(Bucket, Key)]
        return {"ContentLength": len(value["Body"]), "Metadata": value["Metadata"]}

    def put_object(self, **kwargs):
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs

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
