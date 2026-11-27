# WeTTY CLI Demo Prototype

This folder contains a removable prototype for evaluating whether an embedded
terminal can serve as a practical demo surface for `kehrnel`.

It is intentionally isolated from the core runtime:

- no WeTTY code is wired into `src/kehrnel`
- no runtime routes depend on it
- no docs pages need to embed it for the prototype to work

## What This Prototype Includes

- a lightweight portal page with instructions and an embedded terminal
- a WeTTY service reachable through that portal page
- a dedicated shell container with:
  - a preinstalled `kehrnel` checkout
  - a guided `tmux` session
  - helper commands for the openEHR RPS Dual demo flow

## Ports

- Portal: `http://localhost:3001/`
- WeTTY direct: `http://localhost:3000/wetty/`
- Kehrnel runtime inside the demo shell: `http://localhost:8080/`

The runtime only comes up after you start it from the terminal.

## Quick Start

1. Copy the example environment file:

```bash
cp demo/wetty/.env.example demo/wetty/.env
```

2. Edit `demo/wetty/.env` if you want to provide:

- `MONGODB_URI` for local plaintext demo activation
- `BINDINGS_REF` for resolver-backed activation

3. Start the prototype:

```bash
docker compose --env-file demo/wetty/.env -f demo/wetty/docker-compose.yml up --build
```

4. Open the portal:

```text
http://localhost:3001/
```

## Guided Terminal Flow

When the shell opens, it attaches to a `tmux` session with two panes.

- Left pane: runtime and environment notes
- Right pane: hands-on CLI commands

Useful helper commands inside the terminal:

- `demo-help`
- `demo-status`
- `demo-bindings`
- `demo-assets`
- `demo-explore`
- `demo-runtime`
- `demo-smoke`
- `demo-reset`

Recommended first run:

1. In the left pane: `demo-runtime`
2. In the right pane: `demo-smoke`

If you did not provide `MONGODB_URI` or `BINDINGS_REF`, the smoke flow still
shows the local template-driven parts and skips runtime activation cleanly.

## What To Evaluate

- Is the embedded terminal intuitive enough for internal users?
- Does the guided shell reduce onboarding friction?
- Is the portal page clear enough before anyone types a command?
- Is the balance between "guided" and "free exploration" right?
- Does this feel credible enough to justify deeper portal integration?

## Security Note

This prototype is for local or isolated internal testing only.

It uses a dedicated demo shell, password-based SSH inside the local compose
network, and an embedded web terminal. That is acceptable for evaluation, but
it is not the production hardening model.

If the experience is validated, productionization should move to:

- SSO in front of the terminal
- disposable demo environments
- tighter network boundaries
- non-shared credentials
- explicit iframe and reverse-proxy controls

## Tear Down

```bash
docker compose --env-file demo/wetty/.env -f demo/wetty/docker-compose.yml down --volumes
```
