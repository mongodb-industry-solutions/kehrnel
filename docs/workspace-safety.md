# One writer per checkout

Give each active Claude/Codex chat its own Git worktree and branch. Keep the
primary checkout for one foreground writer. Open the editor at that writer's
worktree; stop its background Git operations before integrating recovered work.
Another agent sharing the same checkout may still write files without running
Git. An `index.lock` covers an individual Git operation, not the whole recovery
session or filesystem changes.

OpenAI's [official worktrees documentation](https://developers.openai.com/codex/app/worktrees/)
describes independent worktree chats, detached HEAD defaults, and why a branch
cannot be checked out in two places at once. A worktree isolates files and the
index, while repository objects and references remain shared.

## Guarded checkpoint and command

The standard-library Python utility works on macOS and Linux. Set a durable
backup directory outside every repository before use. Checkpoints can contain
private tracked files and Git configuration; new checkpoint directories have
mode `0700`, and files have mode `0600`.

```sh
python3 scripts/workspace_guard.py --repo "$PWD" \
  --backup-dir "$HOME/Documents/recovery-checkpoints"

python3 scripts/workspace_guard.py --repo "$PWD" \
  --backup-dir "$HOME/Documents/recovery-checkpoints" -- git merge --no-edit origin/codex/working

# For a sequence, run a foreground shell under the guard and exit when done.
python3 scripts/workspace_guard.py --repo "$PWD" \
  --backup-dir "$HOME/Documents/recovery-checkpoints" -- zsh
```

The utility acquires an exclusive per-checkout advisory `flock` and an exclusive
shared-repository metadata lock. It refuses existing Git locks, merge/rebase/
cherry-pick/revert/bisect state, external Git object storage, symlinked Git
metadata, nested repositories, and environment variables that redirect
repository metadata. It never removes a
Git lock. Leave its own lock files in place; the OS releases ownership on exit.

Before running the command, it stores:

- Present tracked and untracked nonignored files, including symlinks and modes.
- Exact Git metadata: objects, staged blobs/index, refs, stash, reflogs and HEAD.
- NUL-delimited tracked/staged inventory and Git status to record deletions.
- A SHA-256 manifest, verified against the archive and a second source inventory.

The archive has `worktree/`, `git-common/`, and, for linked worktrees,
`git-worktree/` prefixes. A completed checkpoint is renamed only after validation
and file sync. An incomplete checkpoint is retained for inspection. A command
failure preserves the checkpoint and returns the command's exit code. The child
command inherits the locks; killing only the wrapper leaves its live child
holding them.

The process check looks for visible Git/Codex/Claude/editor executables whose cwd
is in the checkout. It excludes its own ancestors. On macOS it uses `lsof`; on
Linux it uses `/proc`. It prints executable names and PIDs, without command-line
arguments. If inspection is incomplete it refuses by default. Only after
checking sessions manually, use `--skip-process-check` to explicitly acknowledge
that limitation. That option retains both locks, Git state checks and backup
verification.

## Limits and recovery

The lock is cooperative. Editors, raw `git`, direct filesystem commands,
background children that close inherited descriptors, and other agents can
bypass it. A GUI agent may have cwd `/` while editing this repository, so process
inspection cannot prove inactivity. Separate worktrees and one writer are the
primary protection. The manifest detects observed changes during capture; it is
not an atomic filesystem snapshot.

Ignored worktree files, empty directories, extended attributes and ACLs are
excluded. Back up needed ignored `.env` files and local datasets separately.
Full Git metadata can be large; allow disk space for complete checkpoint copies.
The utility never prunes checkpoints, caches, stashes or untracked files.

Recover into a new disposable directory and inspect the manifest/status first.
For a normal checkout, extracted `git-common/` can serve as its `.git` directory.
For a linked checkout, original gitdir pointer files refer to original paths and
must be reviewed or repaired in the disposable copy. Do not extract over the live
checkout. Preserve the original checkpoint until the recovered tree is tested
and committed.

Before cleanup, enumerate exact target paths and inspect their contents. Move
unneeded data to an external archive, then validate the app, then remove only
reviewed paths. Avoid blanket source-tree deletes and `git clean`, `reset --hard`,
or `stash pop` during recovery. Apply a stash without dropping it, and resolve
against verified recovered files.

Run the utility's integration tests without network:

```sh
python3 -m unittest discover -s tests/unit -p test_workspace_guard.py -v
```
