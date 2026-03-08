# Safety Monitor Agent Notes

- Use the repo-local virtualenv at `.venv` for all local validation work.
- Required local runtime is Python `3.11+`.
- Required local UI automation runtime is Node.js with `npm` and `npx`.
- Prefer a working `playwright-cli` install for browser verification; the Codex Playwright wrapper is an acceptable fallback when `npx` is present.
- Preferred commands:
  - `source .venv/bin/activate`
  - `make preflight`
  - `make controlled-pass` before deploys that touch rules/alerts
  - `make post-deploy-guard` after deploy
- Keep `origin/main` as the canonical source of truth.
- Before deploy, ensure the local repo is clean and synced with `origin/main`.
- `config.yaml` can drift on CT104 because the Rules UI writes to it live. If CT104 has runtime-only changes, merge them back into the repo before resetting the container to `origin/main`.
- CT104 SSH key path:
  - `/Users/rpias/dev/vscode-dev-env/.notes_access/ssh/ct104_root_ed25519`
