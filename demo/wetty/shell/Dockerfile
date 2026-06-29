FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV KEHRNEL_HOST=0.0.0.0
ENV KEHRNEL_PORT=8080
ENV KEHRNEL_RUNTIME_URL=http://127.0.0.1:8080
ENV RUNTIME_URL=http://127.0.0.1:8080
ENV ENV_ID=dev
ENV DOMAIN=openehr
ENV STRATEGY_ID=openehr.rps_dual
ENV REQUIRE_RUNTIME=0
ENV WORKDIR=/home/demo/.kehrnel-demo/workflow-smoke
ENV SAMPLES_ROOT=/workspace/kehrnel/src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference

WORKDIR /workspace/kehrnel

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    bash \
    build-essential \
    ca-certificates \
    curl \
    git \
    less \
    nano \
    nodejs \
    npm \
    openssh-server \
    procps \
    tmux \
 && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir uv

COPY pyproject.toml /workspace/kehrnel/pyproject.toml
COPY src /workspace/kehrnel/src
COPY examples /workspace/kehrnel/examples
COPY startKehrnel /workspace/kehrnel/startKehrnel
COPY docs/website/package*.json /workspace/kehrnel/docs/website/
COPY docs/website/docusaurus.config.js /workspace/kehrnel/docs/website/docusaurus.config.js
COPY docs/website/sidebars.js /workspace/kehrnel/docs/website/sidebars.js
COPY docs/website/src /workspace/kehrnel/docs/website/src
COPY docs/website/static /workspace/kehrnel/docs/website/static
COPY docs/website/docs /workspace/kehrnel/docs/website/docs
COPY samples/templates /workspace/kehrnel/samples/templates

RUN printf '# kehrnel\n' > /workspace/kehrnel/README.md \
 && chmod +x /workspace/kehrnel/startKehrnel

RUN cd /workspace/kehrnel/docs/website \
 && npm install \
 && npm run build

RUN uv venv --python 3.12 /workspace/kehrnel/.venv \
 && uv pip install --python /workspace/kehrnel/.venv/bin/python -e '/workspace/kehrnel[all]'

RUN useradd -m -s /bin/bash demo \
 && mkdir -p /var/run/sshd /opt/kehrnel-demo/bin /home/demo/.kehrnel-demo \
 && ssh-keygen -A \
 && printf '%s\n' \
    'Port 22' \
    'Protocol 2' \
    'PermitRootLogin no' \
    'PasswordAuthentication yes' \
    'ChallengeResponseAuthentication no' \
    'UsePAM no' \
    'X11Forwarding no' \
    'AllowUsers demo' \
    'Subsystem sftp /usr/lib/openssh/sftp-server' \
    > /etc/ssh/sshd_config

COPY demo/wetty/shell/demo.bashrc /home/demo/.bashrc
COPY demo/wetty/shell/launch-demo-session.sh /opt/kehrnel-demo/launch-demo-session.sh
COPY demo/wetty/shell/entrypoint.sh /opt/kehrnel-demo/entrypoint.sh
COPY demo/wetty/shell/bin/ /opt/kehrnel-demo/bin/

RUN printf 'if [ -f ~/.bashrc ]; then . ~/.bashrc; fi\n' > /home/demo/.bash_profile \
 && chmod +x /opt/kehrnel-demo/entrypoint.sh /opt/kehrnel-demo/launch-demo-session.sh /opt/kehrnel-demo/bin/* \
 && chown -R demo:demo /home/demo /opt/kehrnel-demo /workspace/kehrnel

EXPOSE 22 8080

ENTRYPOINT ["/opt/kehrnel-demo/entrypoint.sh"]
