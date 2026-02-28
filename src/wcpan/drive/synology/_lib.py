from collections.abc import Iterable
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import PurePosixPath
from typing import NotRequired, TypedDict

from wcpan.drive.core.types import ChangeAction, Node


FOLDER_MIME_TYPE = "application/x-folder"


def path_to_id(path: str) -> str:
    """Return the SHA-256 hex digest of a Synology path string."""
    return sha256(path.encode()).hexdigest()


class SynologyFileDict(TypedDict):
    id: str
    name: str
    parent_id: str | None
    is_folder: bool
    trashed: bool
    created_time: str
    modified_time: str
    mime_type: str
    size: int
    hash: NotRequired[str]
    image_width: NotRequired[int]
    image_height: NotRequired[int]
    video_width: NotRequired[int]
    video_height: NotRequired[int]
    video_duration_ms: NotRequired[int]


def node_from_api(data: SynologyFileDict) -> Node:
    # data["id"] is the raw Synology path; Node.id is its SHA-256
    raw_path = data["id"]
    id_ = path_to_id(raw_path)
    name = data["name"]
    raw_parent = data.get("parent_id")
    parent_id = path_to_id(raw_parent) if raw_parent is not None else None

    is_folder = data["is_folder"]

    ctime = datetime.fromisoformat(data["created_time"])
    mtime = datetime.fromisoformat(data["modified_time"])

    if ctime.tzinfo is None:
        ctime = ctime.replace(tzinfo=UTC)
    if mtime.tzinfo is None:
        mtime = mtime.replace(tzinfo=UTC)

    size = data["size"]
    hash_ = data.get("hash", "")
    mime_type = "" if is_folder else data["mime_type"]
    is_trashed = data["trashed"]

    is_image = False
    is_video = False
    width = 0
    height = 0
    ms_duration = 0

    if "image_width" in data and "image_height" in data:
        is_image = True
        width = data["image_width"]
        height = data["image_height"]

    if "video_width" in data and "video_height" in data:
        is_video = True
        is_image = False
        width = data["video_width"]
        height = data["video_height"]
        ms_duration = data.get("video_duration_ms", 0)

    return Node(
        id=id_,
        name=name,
        is_trashed=is_trashed,
        ctime=ctime,
        mtime=mtime,
        parent_id=parent_id,
        is_directory=is_folder,
        mime_type=mime_type,
        hash=hash_,
        size=size,
        is_image=is_image,
        is_video=is_video,
        width=width,
        height=height,
        ms_duration=ms_duration,
        private={"path": raw_path},
    )


def normalize_changes(
    file_list: list[SynologyFileDict],
) -> Iterable[ChangeAction]:
    for file_data in file_list:
        node = node_from_api(file_data)
        yield False, node


def node_path(node: Node) -> PurePosixPath:
    """Extract the Synology filesystem path from a node's private dict."""
    if not node.private or "path" not in node.private:
        raise ValueError(f"Node {node.id!r} has no Synology path in private dict")
    return PurePosixPath(node.private["path"])


def utc_now() -> datetime:
    return datetime.now(UTC)
