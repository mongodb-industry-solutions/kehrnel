#!/usr/bin/env bash
set -euo pipefail

DEMO_USER="${DEMO_USER:-demo}"
DEMO_PASSWORD="${DEMO_PASSWORD:-demo}"

echo "${DEMO_USER}:${DEMO_PASSWORD}" | chpasswd

mkdir -p /home/demo/.kehrnel-demo
chown -R demo:demo /home/demo/.kehrnel-demo /workspace/kehrnel /opt/kehrnel-demo

exec /usr/sbin/sshd -D -e
