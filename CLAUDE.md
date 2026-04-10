# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make test       # compile + run all tests (unittest discovery)
make lint       # ruff format check + pyright type checking
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

## Architecture

This package bridges the **wcpan.drive** framework with **Synology Drive API**. It has two components:

### Client (`src/wcpan/drive/synology/_client/`)
A thin async HTTP client that implements `wcpan.drive.core.FileService`. It delegates all file operations to a running server instance. Upload has three strategies depending on whether file size is known (empty / chunked 4 MiB / streaming).

### Server (`src/wcpan/drive/synology/_server/`)
A long-running aiohttp REST server that:
1. Mirrors a Synology Drive instance into a local SQLite database (3 tables: `nodes`, `changes`, `mounts`)
2. Serves the client API at `/api/v1/`
3. Receives Synology webhooks and applies event-driven updates to the mirror

Key subsystems under `_server/services/`:
- **Virtual ID / path system** (`paths.py`): Three service classes — `SynologyPathService` (virtual IDs ↔ Synology API refs), `LocalPathService` (node records → host filesystem), `VirtualPathService` (client virtual paths → node IDs). Synthetic root (`_`) and mount nodes (`_<name>`) are layered over real Synology file IDs.
- **Scan** (`scan.py`): `StartupScanService` runs `_scan_all_mounts` before the server is marked ready; uses Synology's `sync_id` / `max_id` mechanism for incremental BFS scans. Deferred deletion avoids false removes during reparenting.
- **Webhook processing** (`webhook.py`): Processes webhook events as a background consumer. File create/modify events may schedule delayed metadata fetch retries with back-off; moved directories trigger a subtree refresh.
- **Node sync** (`sync.py`): `NodeSyncService` — central gateway for persisting node changes from scan, webhook, and API sources. Drives media enrichment before writing.
- **Storage** (`storage.py`): SQLite read/write operations for nodes, changes, and per-mount scan checkpoints.
- **Write queue** (`workers.py`): All SQLite mutations are serialized through a single async queue → background writer. Let-it-crash: worker failure exits the process.
- **Off-main thread** (`off_main.py`): `OffMainThreadService` wraps blocking SQLite calls in a thread-pool executor with a 60 s timeout.
- **Backfill** (`backfill.py`, `run_backfill` in `main.py`): The `backfill` CLI builds `BackfillService` with the same network / storage / sync stack shape as the server, but without HTTP or webhook lifecycle.
- **Media enrichment** (`enricher.py`): Optional `pymediainfo` probing before change emission when `local_paths` is configured.
- **Network / Auth** (`network.py`): `NetworkService` — locked login/logout flow against Synology DSM; token injected into every API request.

`_server/lib/nodes.py`: `convert_file_info` normalizes Synology API rows to `NodeRecord`; used by HTTP handlers, webhooks, and backfill.

### Shared (`src/wcpan/drive/synology/`)
- `types.py` — `NodeRecord`, `MergedChange`, `ServerConfig`
- `exceptions.py` — custom exception classes
- `_lib.py` — node conversion helpers, MIME guessing, UTC utilities

## Configuration

Server is configured via YAML (see `server.example.yaml`). Key fields:
- `database_url` — SQLite URL
- `synology_url` / `username` / `password` — DSM credentials
- `mounts` — dict of `label: synology_path` (each becomes a virtual mount `_<label>`)
- `public_url` — used for webhook registration
- `local_paths` — dict for media dimension probing and local path resolution; use `{}` to disable probing

## Testing Notes

- Uses Python's built-in `unittest` — never pytest
- Tests are in `tests/test_*.py`
- Type checking via pyright; linting/formatting via ruff
