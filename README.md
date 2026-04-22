# wcpan.drive.synology

Synology Drive integration for the `wcpan.drive` framework.

This repository contains:

- a client-side `FileService` implementation that talks to a running server
- a server that mirrors Synology Drive metadata into a local SQLite database
- a Synology API adapter for the server-side mirror

## Installation

```bash
pip install wcpan-drive-synology
```

For the server component:

```bash
pip install "wcpan-drive-synology[server]"
```

## Usage

### Client

The client connects to a running `wcpan.drive.synology` server instance and
implements the `wcpan.drive.core` `FileService` interface.

```python
from wcpan.drive.synology import create_service

async with create_service(server_url="http://localhost:8080") as file_service:
    root = await file_service.get_root()
    print(f"Root: {root}")
```

### Server

The server mirrors a Synology Drive instance locally via a REST API and
webhook-driven update pipeline. Configure it with a YAML file (see
`server.example.yaml`):

```bash
# Copy and edit the example config
cp server.example.yaml server.yaml

# Start the server
wcpan.drive.synology --config server.yaml serve

# Other subcommands
wcpan.drive.synology --config server.yaml gc
wcpan.drive.synology --config server.yaml backfill /
wcpan.drive.synology --config server.yaml squash
```

At startup, the server:

- connects to Synology Drive through DSM's WebStation API
- registers a webhook at `CALLBACK_URL/api/v1/synology-webhook`
- serves a local HTTP API for the client integration
- keeps a SQLite-backed mirror in sync through scans, webhook events, and
  explicit backfill commands

`backfill PATH` operates on the server's virtual mirror paths. `/` is the
server root; mounted Synology paths appear underneath it by mount label.

#### Config file

```yaml
version: 2
host: "0.0.0.0"
port: 8080
database_url: "sqlite:////data/mirror.db"
synology:
  url: "https://nas.example.com:5001"
  username: "your-drive-user"
  password: "your-password"
  webhook:
    callback_url: "https://my-server.example.com"
mounts:
  photos: "/volume1/photos"
local_paths: {}
```

`version` is the config schema version and must match the version supported by
the current binary. The current binary accepts only version `2`. `local_paths`
is currently required by the config schema; use `{}` when local media probing
is not needed. `synology.webhook.app_id` defaults to `wcpan-drive-synology`. See
`server.example.yaml` for the full set of options.

Important fields:

- `database_url`: SQLite database location for the local mirror
- `synology.url`: DSM / Synology Drive base URL
- `mounts`: virtual mount label to Synology path mapping
- `local_paths`: optional Synology-path to local-filesystem mapping for media
  probing; required by the current schema, so use `{}` if unused
- `synology.webhook.callback_url`: externally reachable base URL used for
  webhook registration
- `synology.otp_code`: optional 2FA code when the Synology account requires it

For multipart uploads in particular, this project intentionally sends
`Content-Type` only on the file part and not on scalar form fields. That
behavior is confirmed against a real Synology server and should be preserved as
a project compatibility requirement to avoid regressions, even though
Synology's public docs do not appear to spell it out explicitly.

## Requirements

- Python >= 3.13
- aiohttp >= 3.13.0
- pycryptodome >= 3.0
- wcpan-drive-core >= 5.0.6

Server extras additionally require: dacite, pyyaml, pymediainfo, wcpan-logging.

## Development

Code style rule for function signatures:

- Keep semantic inputs positional when they are the subject of the operation.
- Make infrastructure and wiring parameters keyword-only when they act as facilities for the function or object, such as `storage`, `drive_api`, `network`, `node_sync`, `off_main`, or similar service/session dependencies.

Example:

```python
await copy_node(src, dst, node_sync=node_sync)
```

Useful commands:

```bash
make test
make lint
make format
make coverage
```

## License

MIT
