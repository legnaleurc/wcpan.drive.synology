# AGENTS.md

Guidance for coding agents working in this repository.

## Scope

This project provides a Synology Drive integration for `wcpan.drive`.

- The client lives under `src/wcpan/drive/synology/_client/` and talks to a running server.
- The server lives under `src/wcpan/drive/synology/_server/` and exposes the mirrored file service over HTTP.
- Shared public types and helpers live under `src/wcpan/drive/synology/`.

Treat `README.md` and `server.example.yaml` as the source of truth for
user-facing usage, configuration examples, and documented backend
compatibility notes. Treat tests as the source of truth for expected behavior.

## Commands

```bash
make test       # compile + run all tests (unittest discovery)
make lint       # ruff format check + ruff lint + pyright
make format     # ruff format & fix in place
make coverage   # run tests with coverage report
```

Run a single test file:

```bash
uv run python -m unittest tests/test_<module>.py
```

Run a specific test case:

```bash
uv run python -m unittest tests.test_<module>.TestClass.test_method
```

## Repo-specific conventions

- Use `unittest`, not `pytest`.
- Tests live in `tests/test_*.py`.
- Prefer `rg` for code search.
- Keep changes narrow and consistent with existing async `aiohttp` patterns.
- Follow the local signature style from `README.md`:
  keep subject inputs positional, and make service or infrastructure dependencies keyword-only when practical.
- Keep `AGENTS.md` focused on agent workflow; avoid repeating user-facing details already covered in `README.md`.

## Architecture Notes

Keep these as high-level orientation only:

- The server maintains a local SQLite-backed mirror of Synology Drive state.
- Startup scan, webhook processing, and explicit backfill all reconcile remote state into the local mirror.
- Path handling is split between Synology paths, client-facing virtual paths, and stored node identity.
- SQLite writes are serialized through background workers.
- Synology API access (client, types, errors) comes from the external `wcpan-synology` library (`../wcpan.synology` locally); `_server/synology.py` adapts it to the mirror. Change API behavior in that library, not here.

If you need detailed implementation behavior, read the relevant module instead of extending this file with subsystem internals.

## Configuration Notes

The YAML config schema is defined in `src/wcpan/drive/synology/_server/config.py` (parsed into `ServerConfig` in `_server/types.py`), and the example config is `server.example.yaml`.

Do not duplicate config fields or semantics here; update `README.md`, `server.example.yaml`, and tests when behavior
changes.

## Maintaining This File

Keep `AGENTS.md` short and durable.

Include:

- commands agents should run,
- stable repository layout,
- testing and style conventions,
- pointers to canonical files.

Avoid:

- detailed subsystem inventories,
- line-by-line architecture explanations,
- transient implementation details that are better learned from the code.
