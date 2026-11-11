#!/usr/bin/env python3
"""Cooperative POSIX checkout lock and verified pre-command recovery checkpoint.

This cannot stop programs that bypass its lock, or prove editor/agent inactivity.
Use separate worktrees and one writer per checkout. No reset/restore/clean occurs.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile


class GuardError(RuntimeError):
    pass


ENV = {**os.environ, "GIT_OPTIONAL_LOCKS": "0", "GIT_NO_LAZY_FETCH": "1"}
LOCK_NAMES = {"workspace-guard.lock", "workspace-guard.common.lock"}
MARKERS = (
    "MERGE_HEAD", "CHERRY_PICK_HEAD", "REVERT_HEAD", "BISECT_LOG",
    "rebase-apply", "rebase-merge", "sequencer",
)
REDIRECT_ENV = (
    "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR",
    "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_NAMESPACE",
)


def git(repo: Path, *args: str) -> bytes:
    result = subprocess.run(["git", "-C", str(repo), *args], env=ENV,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        raise GuardError(result.stderr.decode(errors="replace").strip())
    return result.stdout


def layout(repo: Path) -> tuple[Path, Path, Path]:
    redirected = [name for name in REDIRECT_ENV if os.environ.get(name)]
    if redirected:
        raise GuardError(f"Git environment redirects repository state: {', '.join(redirected)}")
    root = Path(os.fsdecode(git(repo, "rev-parse", "--show-toplevel").strip())).resolve()
    gitdir = Path(os.fsdecode(git(root, "rev-parse", "--absolute-git-dir").strip())).resolve()
    common = Path(os.fsdecode(git(root, "rev-parse", "--git-common-dir").strip()))
    if not common.is_absolute():
        common = root / common
    return root, gitdir, common.resolve()


def check_git_idle(gitdir: Path, common: Path) -> None:
    bases = {gitdir, common}
    if (common / "worktrees").is_dir():
        bases.update(path for path in (common / "worktrees").iterdir() if path.is_dir())
    for base in bases:
        for name in MARKERS:
            if (base / name).exists():
                raise GuardError(f"Git operation in progress: {base / name}; do not remove it.")
        for path in base.rglob("*.lock"):
            if path.name not in LOCK_NAMES:
                raise GuardError(f"Git lock exists: {path}; do not remove it.")
    if (common / "objects/info/alternates").exists():
        raise GuardError("External Git object alternates need their own backup; refused.")
    if (common / "objects").is_symlink():
        raise GuardError("External Git object directory needs its own backup; refused.")
    if any(path.is_symlink() for base in {gitdir, common} for path in base.rglob("*")):
        raise GuardError("Symlinked Git metadata needs its own backup; refused.")


@contextlib.contextmanager
def exclusive_locks(gitdir: Path, common: Path):
    """Never unlink locks: unlinking permits a second inode to be locked."""
    with contextlib.ExitStack() as stack:
        descriptors = []
        for path in (common / "workspace-guard.common.lock", gitdir / "workspace-guard.lock"):
            handle = stack.enter_context(path.open("a+"))
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise GuardError(f"Another guarded writer owns {path}") from exc
            handle.seek(0)
            handle.truncate()
            json.dump({"pid": os.getpid(), "checkout_gitdir": str(gitdir)}, handle)
            handle.flush()
            descriptors.append(handle.fileno())
        yield tuple(descriptors)


def active_writers(root: Path) -> tuple[list[dict], list[str]]:
    """Best effort PID/cwd scan, without collecting potentially sensitive argv."""
    result = subprocess.run(["ps", "-axo", "pid=,ppid=,comm="],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode:
        return [], ["Process inventory unavailable; session inactivity is unverified."]
    processes = {}
    for line in result.stdout.splitlines():
        fields = line.strip().split(None, 2)
        if len(fields) == 3:
            processes[int(fields[0])] = (int(fields[1]), fields[2])
    ancestors = {os.getpid()}
    cursor = os.getpid()
    while cursor in processes:
        cursor = processes[cursor][0]
        if cursor in ancestors:
            break
        ancestors.add(cursor)
    blockers, warnings = [], []
    for pid, (_, command) in processes.items():
        name = Path(command).name.lower()
        if pid in ancestors or not any(word in name for word in ("codex", "claude", "git", "code", "cursor")):
            continue
        cwd = None
        try:
            if sys.platform.startswith("linux"):
                cwd = Path(os.readlink(f"/proc/{pid}/cwd"))
            elif shutil.which("lsof"):
                found = subprocess.run(["lsof", "-a", "-p", str(pid), "-d", "cwd", "-Fn"],
                                       stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                       text=True, timeout=5)
                cwd = next((Path(line[1:]) for line in found.stdout.splitlines() if line.startswith("n")), None)
        except (OSError, subprocess.TimeoutExpired):
            pass
        if cwd is None:
            warnings.append(f"Could not inspect cwd of known writer PID {pid} ({name}).")
        elif cwd.resolve() == root or root in cwd.resolve().parents:
            blockers.append({"pid": pid, "process": name, "cwd": str(cwd)})
    return blockers, warnings


def workspace_entries(root: Path) -> dict[str, Path]:
    paths = set(git(root, "ls-files", "--cached", "--others", "--exclude-standard", "-z").split(b"\0"))
    entries = {}
    if (root / ".git").is_file():
        entries["worktree/.git"] = root / ".git"  # Linked-worktree gitdir pointer.
    for raw in paths - {b""}:
        rel = os.fsdecode(raw)
        path = root / rel
        if not path.exists() and not path.is_symlink():
            continue  # A deletion is captured by the tracked inventory and status.
        if path.is_dir() and not path.is_symlink():
            raise GuardError(f"Nested repository/submodule needs a separate checkpoint: {rel}")
        # Reject paths reached through a symlinked parent, never follow outside root.
        if path.parent.resolve() != root and root not in path.parent.resolve().parents:
            raise GuardError(f"Workspace path escapes checkout through a symlink: {rel}")
        entries[f"worktree/{rel}"] = path
    return entries


def metadata_entries(gitdir: Path, common: Path) -> dict[str, Path]:
    entries = {}
    roots = {"git-common": common}
    if gitdir != common:
        roots["git-worktree"] = gitdir
    for label, base in roots.items():
        for directory, dirs, files in os.walk(base, followlinks=False):
            parent = Path(directory)
            # os.walk places symlinked directories in dirs; preserve as links.
            for name in list(dirs):
                path = parent / name
                if path.is_symlink():
                    files.append(name)
                    dirs.remove(name)
            for name in files:
                if name in LOCK_NAMES:
                    continue
                path = parent / name
                entries[f"{label}/{path.relative_to(base).as_posix()}"] = path
    return entries


def fingerprint(path: Path) -> dict:
    before = path.lstat()
    if stat.S_ISLNK(before.st_mode):
        info = {"kind": "symlink", "target": os.readlink(path)}
    elif stat.S_ISREG(before.st_mode):
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
        info = {"kind": "file", "sha256": digest.hexdigest(), "size": before.st_size}
    else:
        raise GuardError(f"Cannot checkpoint a special file: {path}")
    after = path.lstat()
    if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise GuardError(f"File changed while being read: {path}")
    return {**info, "mode": stat.S_IMODE(before.st_mode)}


def inventory(root: Path, gitdir: Path, common: Path) -> tuple[dict[str, Path], dict]:
    entries = {**workspace_entries(root), **metadata_entries(gitdir, common)}
    return entries, {name: fingerprint(path) for name, path in sorted(entries.items())}


def verify_archive(archive: Path, expected: dict) -> None:
    with tarfile.open(archive, "r:") as bundle:
        members = bundle.getmembers()
        if {member.name for member in members} != set(expected):
            raise GuardError("Archive inventory differs from the checkpoint inventory.")
        for member in members:
            info = expected[member.name]
            if member.mode != info["mode"]:
                raise GuardError(f"Archive mode differs: {member.name}")
            if info["kind"] == "symlink":
                valid = member.issym() and member.linkname == info["target"]
            else:
                source = bundle.extractfile(member)
                if source is None:
                    raise GuardError(f"Archive content missing: {member.name}")
                digest = hashlib.sha256()
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(block)
                valid = member.isfile() and digest.hexdigest() == info["sha256"] and member.size == info["size"]
            if not valid:
                raise GuardError(f"Archive content differs: {member.name}")


def checkpoint(root: Path, gitdir: Path, common: Path, destination: Path) -> Path:
    destination = destination.resolve()
    if destination == root or root in destination.parents or any(destination == base or base in destination.parents for base in {gitdir, common}):
        raise GuardError("Checkpoint destination must be outside the checkout and Git metadata.")
    destination.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    pending = Path(tempfile.mkdtemp(prefix=f".{root.name}-{stamp}-pending-", dir=destination))
    os.chmod(pending, 0o700)
    try:
        check_git_idle(gitdir, common)
        status = git(root, "status", "--porcelain=v2", "-z")
        tracked = git(root, "ls-files", "--stage", "-z")
        entries, before = inventory(root, gitdir, common)
        archive = pending / "checkpoint.tar"
        with tarfile.open(archive, "w:", dereference=False) as bundle:
            for name, path in sorted(entries.items()):
                # Store each file independently, including hard-linked Git objects.
                bundle.inodes.clear()
                bundle.add(path, arcname=name, recursive=False)
        os.chmod(archive, 0o600)
        verify_archive(archive, before)
        check_git_idle(gitdir, common)
        _, after = inventory(root, gitdir, common)
        if before != after or status != git(root, "status", "--porcelain=v2", "-z") or tracked != git(root, "ls-files", "--stage", "-z"):
            raise GuardError("Workspace or Git metadata changed during backup; command was not run.")
        (pending / "status.porcelain-v2.z").write_bytes(status)
        (pending / "tracked-stage.z").write_bytes(tracked)
        (pending / "manifest.json").write_text(json.dumps({
            "format": 1, "checkout": str(root), "gitdir": str(gitdir), "common_gitdir": str(common),
            "created_utc": stamp, "files": before,
            "limitations": ["Ignored worktree files excluded.", "Locks only protect cooperating processes.",
                            "Cwd scan cannot prove that agents/editors are inactive."],
        }, indent=2) + "\n")
        for path in pending.iterdir():
            os.chmod(path, 0o600)
            with path.open("rb") as handle:
                os.fsync(handle.fileno())
        complete = destination / f"{root.name}-{stamp}-{os.getpid()}"
        pending.rename(complete)
        for directory in (complete, destination):
            fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        return complete
    except Exception:
        # Keep failed artifacts for inspection rather than delete recovery data.
        print(f"Incomplete checkpoint retained at {pending}", file=sys.stderr)
        raise


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--backup-dir", type=Path, required=True,
                        help="Private durable directory outside the checkout")
    parser.add_argument("--skip-process-check", action="store_true",
                        help="Explicitly acknowledge an unavailable process scan; locks still apply")
    parser.add_argument("command", nargs=argparse.REMAINDER,
                        help="Optional command after --; runs only after a verified checkpoint")
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    try:
        root, gitdir, common = layout(args.repo)
        check_git_idle(gitdir, common)
        with exclusive_locks(gitdir, common) as lock_fds:
            check_git_idle(gitdir, common)
            if args.skip_process_check:
                print("Process scan explicitly skipped; ensure one writer manually.", file=sys.stderr)
            else:
                blockers, warnings = active_writers(root)
                for warning in warnings:
                    print(f"Warning: {warning}", file=sys.stderr)
                if blockers:
                    raise GuardError(f"Other known writers use this checkout: {json.dumps(blockers)}")
                if warnings:
                    raise GuardError("Process scan incomplete. Verify sessions manually, then use --skip-process-check if appropriate.")
            saved = checkpoint(root, gitdir, common, args.backup_dir)
            print(f"Verified checkpoint: {saved}", flush=True)
            if not command:
                return 0
            check_git_idle(gitdir, common)
            # Recheck immediately before starting; uncooperative races remain possible.
            if not args.skip_process_check:
                blockers, warnings = active_writers(root)
                if blockers or warnings:
                    raise GuardError("Writer scan changed after checkpoint; command was not run.")
            # Inherit locks so killing only the wrapper does not free a live child.
            result = subprocess.run(command, cwd=root, pass_fds=lock_fds)
            return result.returncode if result.returncode >= 0 else 128 - result.returncode
    except (GuardError, OSError, tarfile.TarError) as exc:
        print(f"Refused: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
