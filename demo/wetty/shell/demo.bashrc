export PATH="/opt/kehrnel-demo/bin:$PATH"
export KEHRNEL_DEMO_ROOT="/workspace/kehrnel"
export RUNTIME_URL="${RUNTIME_URL:-http://127.0.0.1:8080}"
export KEHRNEL_RUNTIME_URL="${KEHRNEL_RUNTIME_URL:-$RUNTIME_URL}"
export KEHRNEL_HOST="${KEHRNEL_HOST:-0.0.0.0}"
export KEHRNEL_PORT="${KEHRNEL_PORT:-8080}"
export ENV_ID="${ENV_ID:-dev}"
export DOMAIN="${DOMAIN:-openehr}"
export STRATEGY_ID="${STRATEGY_ID:-openehr.rps_dual}"
export MONGODB_DB="${MONGODB_DB:-openEHR_demo}"
export WORKDIR="${WORKDIR:-/home/demo/.kehrnel-demo/workflow-smoke}"
export SAMPLES_ROOT="${SAMPLES_ROOT:-/workspace/kehrnel/src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference}"

alias k='kehrnel'
alias ll='ls -lah'
alias croot='cd /workspace/kehrnel'

cd /workspace/kehrnel

if [[ $- == *i* ]] && [[ -z "${TMUX:-}" ]] && [[ -z "${KEHRNEL_DEMO_DISABLE_TMUX:-}" ]]; then
  exec /opt/kehrnel-demo/launch-demo-session.sh
fi
