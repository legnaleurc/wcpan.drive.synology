"""Server-internal types (not part of client / shared lib surface)."""

import asyncio
from collections.abc import Callable


type WriteQueue = asyncio.Queue[Callable[[], None]]
