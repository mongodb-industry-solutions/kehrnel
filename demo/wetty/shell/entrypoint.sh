#!/usr/bin/env bash
set -euo pipefail

# The image provisions a single `demo` user (useradd and AllowUsers in the
# Dockerfile), so the user is fixed; only the password is configurable.
DEMO_USER=demo
DEMO_PASSWORD="${DEMO_PASSWORD:-demo}"

echo "${DEMO_USER}:${DEMO_PASSWORD}" | chpasswd

mkdir -p /home/demo/.kehrnel-demo
chown -R demo:demo /home/demo/.kehrnel-demo /workspace/kehrnel /opt/kehrnel-demo

exec /usr/sbin/sshd -D -e
