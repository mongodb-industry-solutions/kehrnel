# Repository collaboration

Use one writer per checkout. Parallel Codex or Claude chats must use separate worktrees and branches. Keep read-only reviews read-only. Before changing Git state in this checkout, inspect active chats and local agent/editor processes, pause relevant writers, and preserve tracked edits, untracked files, index, refs and stashes in a verified external checkpoint.

Use `python3 scripts/workspace_guard.py --repo "$PWD" --backup-dir <external-durable-directory> -- <command>` for a mutating session. Its locks are cooperative; review process-check limitations in `docs/workspace-safety.md`. Never delete an existing Git lock; identify the owner first.

During recovery, use `git stash apply` and retain the original stash until validated recovery is committed. Avoid `git reset --hard`, `git clean`, and source-directory deletion. Enumerate exact cleanup paths and archive them outside the repository before removing reviewed files. Restore into an isolated copy, combine local and upstream changes, validate tests, then integrate.

Do not describe transcript replay as byte-exact recovery. Validate replayed code against the surviving tests and inspect duplicate definitions, routes, dispatch branches, and mismatched renamed imports. Refresh content-addressed catalog references after strategy changes using `scripts/refresh_catalog_digests.py`.
