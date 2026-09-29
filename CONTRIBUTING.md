# Contributing

Thanks for considering it. This is a solo-maintained project, so the biggest
favour a contribution can do is arrive in a shape that is fast to review. This
page is mostly about that.

## Setup

```bash
uv sync
make services   # Redis on 56380 and TimescaleDB on 55433, in Docker
```

Without `make services` the suite still runs: Redis falls back to fakeredis,
and the TimescaleDB tests are skipped with a reason. Run it against the real
services before opening a pull request that touches storage or streams.

## Before opening a pull request

```bash
make check      # ruff check, ruff format --check, mypy --strict, pytest
```

CI runs the same checks on every push, and a pull request that fails one will
not be merged until it passes.

If you changed dependencies, regenerate the file CI installs from:

```bash
make requirements
```

## What makes a change easy to accept

- **One concern per pull request.** A fix and a refactor are two PRs.
- **A test that fails without the change.** For the aggregator, prefer a
  property of the output ("shuffled gives the same candles") over a list of
  expected numbers.
- **No network in tests.** An adapter change comes with frames recorded from
  the real feed, added to `tests/fixtures/`.
- **A decision worth recording gets an ADR** in `docs/adr/`: what was decided,
  what else was considered, and what it costs.
- **Conventional commit messages**, short and imperative:
  `fix: keep a closed candle closed when a stale snapshot replays`.
- **A CHANGELOG entry** under *Unreleased* for anything a user would notice.
