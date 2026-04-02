# wcpan.drive.synology

Synology Drive Web API integration for wcpan.drive framework.

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
webhook. Configure it with a YAML file (see `server.example.yaml`):

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

#### Config file

```yaml
host: "0.0.0.0"
port: 8080
database_url: "sqlite:////data/mirror.db"
synology_url: "https://nas.example.com:5001"
username: "your-drive-user"
password: "your-password"
folders:
  photos: "/volume1/photos"
public_url: "https://my-server.example.com"
```

See `server.example.yaml` for the full set of options.

## Requirements

- Python >= 3.13
- aiohttp >= 3.13.0
- pycryptodome >= 3.0
- wcpan-drive-core >= 5.0.6

Server extras additionally require: pyyaml, pymediainfo, wcpan-logging.

## License

MIT
