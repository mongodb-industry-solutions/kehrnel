import os
from pathlib import Path

import pytest

from kehrnel.contracts.package_digest import (
    PackageFileEntry,
    PackageFileManifest,
    PackageLayoutError,
    build_package_file_manifest,
    canonical_json_bytes,
    compute_package_digest,
)


def _write_pack(root: Path, line_ending: str = "\n") -> None:
    root.mkdir()
    (root / "manifest.json").write_bytes(
        ("{\"id\":\"example\",\"ui\":{\"summary\":\"Demo\"}}" + line_ending).encode()
    )
    (root / "strategy.py").write_text("VALUE = 1\n", encoding="utf-8")


def test_package_digest_is_root_and_mtime_independent_but_byte_sensitive(tmp_path):
    left = tmp_path / "left"
    right = tmp_path / "right"
    _write_pack(left)
    _write_pack(right)
    assert compute_package_digest(left) == compute_package_digest(right)

    os.utime(right / "strategy.py", (1, 1))
    assert compute_package_digest(left) == compute_package_digest(right)

    (right / "strategy.py").write_text("VALUE = 2\n", encoding="utf-8")
    assert compute_package_digest(left) != compute_package_digest(right)


def test_line_endings_and_presentation_metadata_are_exact_hashed_bytes(tmp_path):
    lf = tmp_path / "lf"
    crlf = tmp_path / "crlf"
    _write_pack(lf, "\n")
    _write_pack(crlf, "\r\n")
    assert compute_package_digest(lf) != compute_package_digest(crlf)

    before = compute_package_digest(lf)
    (lf / "manifest.json").write_text(
        '{"id":"example","ui":{"summary":"Changed presentation"}}\n',
        encoding="utf-8",
    )
    assert compute_package_digest(lf) != before


def test_explicit_cache_and_vcs_exclusions_do_not_change_digest(tmp_path):
    pack = tmp_path / "pack"
    _write_pack(pack)
    before = compute_package_digest(pack)
    (pack / "__pycache__").mkdir()
    (pack / "__pycache__" / "strategy.pyc").write_bytes(b"cache")
    (pack / ".git").mkdir()
    (pack / ".git" / "index").write_bytes(b"vcs")
    (pack / ".DS_Store").write_bytes(b"finder")
    assert compute_package_digest(pack) == before


def test_manifest_paths_are_posix_sorted_and_hash_policy_is_explicit(tmp_path):
    pack = tmp_path / "pack"
    _write_pack(pack)
    nested = pack / "nested"
    nested.mkdir()
    (nested / "z.json").write_text("{}", encoding="utf-8")
    manifest = build_package_file_manifest(pack)
    paths = [entry.path for entry in manifest.files]
    assert paths == sorted(paths, key=lambda value: value.encode("utf-8"))
    assert all("\\" not in value for value in paths)
    assert manifest.presentation_metadata == "included"
    assert manifest.byte_policy == "exact"


def test_symlinks_secret_like_files_and_special_files_fail_closed(tmp_path):
    pack = tmp_path / "pack"
    _write_pack(pack)
    (pack / "link.py").symlink_to(pack / "strategy.py")
    with pytest.raises(PackageLayoutError, match="symlink"):
        compute_package_digest(pack)
    (pack / "link.py").unlink()

    (pack / ".env.local").write_text("DO_NOT_READ", encoding="utf-8")
    with pytest.raises(PackageLayoutError, match="secret-like"):
        compute_package_digest(pack)
    (pack / ".env.local").unlink()

    for secret_name in (".ENV", ".envrc", "Credentials.json"):
        (pack / secret_name).write_text("DO_NOT_READ", encoding="utf-8")
        with pytest.raises(PackageLayoutError, match="secret-like"):
            compute_package_digest(pack)
        (pack / secret_name).unlink()

    if hasattr(os, "mkfifo"):
        os.mkfifo(pack / "pipe")
        with pytest.raises(PackageLayoutError, match="special file"):
            compute_package_digest(pack)


def test_complete_layout_is_rejected_before_any_file_is_read(tmp_path, monkeypatch):
    pack = tmp_path / "pack"
    _write_pack(pack)
    (pack / "z-link.py").symlink_to(pack / "strategy.py")

    def fail_if_read(_path):
        raise AssertionError("package content was read before layout preflight completed")

    monkeypatch.setattr(Path, "read_bytes", fail_if_read)
    with pytest.raises(PackageLayoutError, match="symlink"):
        build_package_file_manifest(pack)


def test_directory_scan_errors_fail_closed(tmp_path, monkeypatch):
    pack = tmp_path / "pack"
    _write_pack(pack)

    def failing_walk(_root, *, topdown, followlinks, onerror):
        assert topdown is True
        assert followlinks is False
        onerror(PermissionError(13, "permission denied", str(pack / "hidden")))
        return iter(())

    monkeypatch.setattr(os, "walk", failing_walk)
    with pytest.raises(PackageLayoutError, match="cannot inspect package path"):
        build_package_file_manifest(pack)


def test_package_file_manifest_rejects_noncanonical_input_contract():
    entry = PackageFileEntry(path="a.json", size=2, sha256="a" * 64)
    with pytest.raises(ValueError, match="literal_error"):
        PackageFileManifest(hashAlgorithm="md5", files=[entry])
    with pytest.raises(ValueError, match="exclusions"):
        PackageFileManifest(exclusions=[], files=[entry])
    with pytest.raises(ValueError, match="relative POSIX"):
        PackageFileEntry(path="../a.json", size=2, sha256="a" * 64)
    with pytest.raises(ValueError, match="sorted"):
        PackageFileManifest(
            files=[
                PackageFileEntry(path="z.json", size=2, sha256="a" * 64),
                entry,
            ]
        )
    with pytest.raises(ValueError, match="unique"):
        PackageFileManifest(files=[entry, entry])


def test_canonical_json_rejects_ambiguous_keys_and_floats():
    with pytest.raises(TypeError, match="ASCII"):
        canonical_json_bytes({"é": "value"})
    with pytest.raises(TypeError, match="floating-point"):
        canonical_json_bytes({"value": 1.5})
