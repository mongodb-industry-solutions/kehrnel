# Strategy Pack Template

Use this folder as a starting point for new strategy packs.

Contents:
- `manifest.json`: required metadata and entrypoint declaration.
- `defaults.json` and `schema.json`: config defaults and JSON schema placeholders.
- `strategy.py`: minimal `StrategyPlugin` implementation using the shared explain metadata helper.

To use:
1. Copy this folder, rename it, and update `manifest.json` (id, domain, entrypoint).
2. Adjust schema/defaults and flesh out `strategy.py`.
3. Run `kehrnel-validate-pack /path/to/your/pack` for quick diagnostics.
