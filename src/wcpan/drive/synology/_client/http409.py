"""Parse HTTP 409 bodies from the wcpan.drive.synology server."""

from typing import Any, cast

from aiohttp import ClientResponse
from wcpan.drive.core.types import Node

from .._lib import node_from_record, node_record_from_dict


async def node_from_409(response: ClientResponse) -> Node | None:
    """If the JSON body is a node record (has ``node_id``), return a ``Node``."""
    try:
        body: Any = await response.json()
    except Exception:
        return None
    if not isinstance(body, dict) or "node_id" not in body:
        return None
    return node_from_record(node_record_from_dict(cast(dict[str, Any], body)))
