#!/usr/bin/env bash
set -euo pipefail

SESSION_NAME="kehrnel-demo"
ROOT="/workspace/kehrnel"

if tmux has-session -t "${SESSION_NAME}" 2>/dev/null; then
  exec tmux attach -t "${SESSION_NAME}"
fi

tmux new-session -d -s "${SESSION_NAME}" -c "${ROOT}"
tmux rename-window -t "${SESSION_NAME}:0" "openEHR-demo"
tmux send-keys -t "${SESSION_NAME}:0.0" "cd ${ROOT} && clear && demo-help --pane runtime" C-m
tmux split-window -h -t "${SESSION_NAME}:0" -c "${ROOT}"
tmux send-keys -t "${SESSION_NAME}:0.1" "cd ${ROOT} && clear && demo-help --pane hands-on" C-m
tmux select-pane -t "${SESSION_NAME}:0.1"

exec tmux attach -t "${SESSION_NAME}"
