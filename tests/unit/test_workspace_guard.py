"""Integration tests use only disposable local repositories; no network."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[2] / 'scripts/workspace_guard.py'
spec = importlib.util.spec_from_file_location("workspace_guard", SCRIPT)
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class WorkspaceGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="workspace-guard-test-")
        self.base = Path(self.temp.name)
        self.repo = self.base / "repo"
        self.repo.mkdir()
        self.backups = self.base / "backups"
        self.run_git("init", "-q")
        self.run_git("config", "user.name", "Guard Test")
        self.run_git("config", "user.email", "guard-test@example.invalid")
        (self.repo / "tracked.txt").write_text("committed\n")
        (self.repo / ".gitignore").write_text("ignored.txt\n")
        self.run_git("add", ".")
        self.run_git("commit", "-qm", "initial")

    def tearDown(self):
        self.temp.cleanup()

    def run_git(self, *args, repo=None):
        return subprocess.check_output(["git", "-C", str(repo or self.repo), *args])

    def run_guard(self, *args, repo=None):
        return subprocess.run([sys.executable, str(SCRIPT), "--repo", str(repo or self.repo),
                               "--backup-dir", str(self.backups), "--skip-process-check", *args],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def checkpoint_path(self, result):
        self.assertEqual(result.returncode, 0, result.stderr)
        return Path(result.stdout.split("Verified checkpoint: ", 1)[1].splitlines()[0])

    def test_checkpoint_preserves_index_stash_deleted_and_untracked_files(self):
        (self.repo / "tracked.txt").write_text("stash content\n")
        self.run_git("stash", "push", "-qm", "preserve this stash")
        stash = self.run_git("rev-parse", "refs/stash")
        (self.repo / "tracked.txt").write_text("staged content\n")
        self.run_git("add", "tracked.txt")
        (self.repo / "tracked.txt").write_text("unstaged content\n")
        (self.repo / "new \u00e9\nfile.txt").write_text("untracked content\n")
        (self.repo / "ignored.txt").write_text("ignored private content\n")
        (self.repo / "external-link").symlink_to(self.base / "outside")
        (self.repo / ".gitignore").unlink()
        original_status = self.run_git("status", "--porcelain=v2", "-z")
        original_index = (self.repo / ".git/index").read_bytes()
        saved = self.checkpoint_path(self.run_guard())
        manifest = json.loads((saved / "manifest.json").read_text())
        self.assertIn("worktree/new \u00e9\nfile.txt", manifest["files"])
        self.assertNotIn("worktree/.gitignore", manifest["files"])
        self.assertEqual(original_status, (saved / "status.porcelain-v2.z").read_bytes())
        restored = self.base / "restored"
        restored.mkdir()
        with tarfile.open(saved / "checkpoint.tar") as archive:
            # Test archives were created locally from these disposable repos.
            archive.extractall(restored, filter="fully_trusted")
        restored_repo = restored / "worktree"
        (restored / "git-common").rename(restored_repo / ".git")
        self.assertEqual((restored_repo / ".git/index").read_bytes(), original_index)
        self.assertEqual(self.run_git("show", ":tracked.txt", repo=restored_repo), b"staged content\n")
        self.assertEqual((restored_repo / "tracked.txt").read_text(), "unstaged content\n")
        self.assertEqual(self.run_git("rev-parse", "refs/stash", repo=restored_repo), stash)
        self.assertEqual(self.run_git("stash", "show", "-p", repo=restored_repo), self.run_git("stash", "show", "-p"))
        self.assertTrue((restored_repo / "external-link").is_symlink())
        self.assertEqual(self.run_git("status", "--porcelain=v2", "-z", repo=restored_repo), original_status)
        self.assertEqual(self.run_git("status", "--porcelain=v2", "-z"), original_status)

    def test_ignored_files_excluded_when_ignore_file_present(self):
        (self.repo / "ignored.txt").write_text("excluded\n")
        saved = self.checkpoint_path(self.run_guard())
        self.assertNotIn("worktree/ignored.txt", json.loads((saved / "manifest.json").read_text())["files"])

    def test_command_runs_after_verified_checkpoint_and_exit_code_preserved(self):
        result = self.run_guard("--", sys.executable, "-c",
                                "from pathlib import Path; Path('tracked.txt').write_text('changed'); raise SystemExit(7)")
        self.assertEqual(result.returncode, 7, result.stderr)
        saved = Path(result.stdout.split("Verified checkpoint: ", 1)[1].splitlines()[0])
        with tarfile.open(saved / "checkpoint.tar") as archive:
            self.assertEqual(archive.extractfile("worktree/tracked.txt").read(), b"committed\n")
        self.assertEqual((self.repo / "tracked.txt").read_text(), "changed")

    def test_git_lock_and_operation_markers_refuse_command(self):
        for marker in ("index.lock", "MERGE_HEAD", "CHERRY_PICK_HEAD", "rebase-merge"):
            with self.subTest(marker=marker):
                path = self.repo / ".git" / marker
                path.mkdir() if marker == "rebase-merge" else path.write_text("retained")
                result = self.run_guard("--", sys.executable, "-c", "open('must-not-run','w').close()")
                self.assertEqual(result.returncode, 2)
                self.assertIn("Refused:", result.stderr)
                self.assertFalse((self.repo / "must-not-run").exists())
                self.assertTrue(path.exists())
                path.rmdir() if path.is_dir() else path.unlink()

    def test_lock_is_exclusive_and_released_after_holder_exits(self):
        root, gitdir, common = guard.layout(self.repo)
        with guard.exclusive_locks(gitdir, common):
            result = self.run_guard()
            self.assertEqual(result.returncode, 2)
            self.assertIn("Another guarded writer", result.stderr)
        self.checkpoint_path(self.run_guard())
        self.assertTrue((gitdir / "workspace-guard.lock").exists())

    def test_external_destination_and_object_alternates_refused(self):
        result = subprocess.run([sys.executable, str(SCRIPT), "--repo", str(self.repo),
                                 "--backup-dir", str(self.repo / "backups"), "--skip-process-check"],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("outside", result.stderr)
        alternates = self.repo / ".git/objects/info/alternates"
        alternates.write_text(str(self.base / "other-objects") + "\n")
        result = self.run_guard()
        self.assertEqual(result.returncode, 2)
        self.assertIn("alternates", result.stderr)

    def test_changes_during_checkpoint_refuse_command_and_retain_partial_backup(self):
        root, gitdir, common = guard.layout(self.repo)
        original = guard.verify_archive
        def mutate_after_verify(archive, expected):
            original(archive, expected)
            (self.repo / "tracked.txt").write_text("concurrent writer\n")
        with guard.exclusive_locks(gitdir, common), patch.object(guard, "verify_archive", mutate_after_verify):
            with self.assertRaisesRegex(guard.GuardError, "changed during backup"):
                guard.checkpoint(root, gitdir, common, self.backups)
        self.assertTrue(any("pending" in path.name for path in self.backups.iterdir()))

    def test_linked_worktree_preserves_per_worktree_index_and_shared_metadata(self):
        linked = self.base / "linked"
        self.run_git("worktree", "add", "-q", "--detach", str(linked))
        (linked / "tracked.txt").write_text("linked staged\n")
        self.run_git("add", "tracked.txt", repo=linked)
        (linked / "tracked.txt").write_text("linked unstaged\n")
        saved = self.checkpoint_path(self.run_guard(repo=linked))
        manifest = json.loads((saved / "manifest.json").read_text())
        self.assertIn("git-worktree/index", manifest["files"])
        self.assertIn("git-common/HEAD", manifest["files"])
        self.assertIn("worktree/.git", manifest["files"])
        with tarfile.open(saved / "checkpoint.tar") as archive:
            self.assertEqual(archive.extractfile("git-worktree/index").read(),
                             Path(manifest["gitdir"], "index").read_bytes())
        root, gitdir, common = guard.layout(self.repo)
        with guard.exclusive_locks(gitdir, common):
            result = self.run_guard(repo=linked)
            self.assertEqual(result.returncode, 2)
            self.assertIn("Another guarded writer", result.stderr)

    def test_incomplete_process_inventory_requires_explicit_acknowledgment(self):
        with patch.object(guard, "active_writers", return_value=([], ["not available"])):
            self.assertEqual(guard.main(["--repo", str(self.repo), "--backup-dir", str(self.backups)]), 2)
        self.assertFalse(self.backups.exists())

    def test_command_child_retains_lock_if_wrapper_is_killed(self):
        child_script = "import os,time; from pathlib import Path; Path('child.pid').write_text(str(os.getpid())); time.sleep(20)"
        wrapper = subprocess.Popen([sys.executable, str(SCRIPT), "--repo", str(self.repo),
                                    "--backup-dir", str(self.backups), "--skip-process-check", "--",
                                    sys.executable, "-c", child_script],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        import signal
        import time
        child_pid = None
        try:
            deadline = time.monotonic() + 10
            while not (self.repo / "child.pid").exists() and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertTrue((self.repo / "child.pid").exists())
            child_pid = int((self.repo / "child.pid").read_text())
            wrapper.kill()
            wrapper.wait(timeout=5)
            blocked = self.run_guard()
            self.assertEqual(blocked.returncode, 2)
            self.assertIn("Another guarded writer", blocked.stderr)
        finally:
            if child_pid:
                os.kill(child_pid, signal.SIGTERM)
            if wrapper.poll() is None:
                wrapper.kill()
                wrapper.wait(timeout=5)
            wrapper.stdout.close()
            wrapper.stderr.close()

    def test_hardlinked_files_are_verified_and_metadata_redirects_refused(self):
        os.link(self.repo / "tracked.txt", self.repo / "hardlink.txt")
        saved = self.checkpoint_path(self.run_guard())
        with tarfile.open(saved / "checkpoint.tar") as archive:
            self.assertTrue(archive.getmember("worktree/hardlink.txt").isfile())
            self.assertTrue(archive.getmember("worktree/tracked.txt").isfile())
        with patch.dict(os.environ, {"GIT_INDEX_FILE": str(self.base / "other-index")}):
            with self.assertRaisesRegex(guard.GuardError, "redirects"):
                guard.layout(self.repo)

    def test_operation_in_other_worktree_refused_before_shared_metadata_backup(self):
        linked = self.base / "linked"
        self.run_git("worktree", "add", "-q", "--detach", str(linked))
        _, linked_gitdir, _ = guard.layout(linked)
        (linked_gitdir / "MERGE_HEAD").write_text("operation in another worktree")
        result = self.run_guard()
        self.assertEqual(result.returncode, 2)
        self.assertIn("MERGE_HEAD", result.stderr)


if __name__ == "__main__":
    unittest.main()
