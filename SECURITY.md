# Security

## Reporting a vulnerability

Please do not open a public issue. Report it privately through GitHub's
[security advisories](https://github.com/CtrlAltDevelop/mdp-outbox/security/advisories/new),
with what you found, how to reproduce it, and what an attacker could do with
it. You will get an acknowledgement within a week and a fix, or a reasoned
answer, as soon as one is ready.

Only the latest release is supported.

## What the project does and does not protect

The pipeline reads **public** market data and holds no secrets beyond its own
database and Redis credentials. Its security boundaries are these:

- **The API has no authentication and no TLS.** It is meant to sit behind a
  gateway that provides both. Do not expose port 8000 to the internet as it is.
- **The compose credentials are demo values.** Set `POSTGRES_PASSWORD` and
  `GRAFANA_ADMIN_PASSWORD` in `.env` before running the stack anywhere shared,
  and keep Redis and TimescaleDB off public networks. The compose file does
  not publish their ports.
- **Grafana allows anonymous read-only access** in the compose file, for the
  demo. Turn `GF_AUTH_ANONYMOUS_ENABLED` off for anything else.
- **Input from sources is untrusted.** Every frame is validated into the trade
  schema; anything that fails goes to the dead-letter stream and is never
  applied. Frames are size-limited (4 MiB) at the WebSocket layer.
- **WebSocket clients are bounded**: at most 50 channels each, and a client
  that falls behind is disconnected instead of buffering without limit.

Dependencies are audited with `pip-audit` and the history is scanned for
secrets with gitleaks on every push, in CI.
