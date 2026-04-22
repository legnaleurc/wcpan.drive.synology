import mimetypes
from datetime import UTC, datetime
from typing import Literal, TypedDict

from wcpan.drive.core.types import Node

from .types import MirrorMutableId, MirrorStableId, NodeRecord


FOLDER_MIME_TYPE = "application/x-directory"


class NodeRecordDict(TypedDict):
    id: str
    mutable_id: str
    parent_id: str | None
    name: str
    is_directory: bool
    ctime: str
    mtime: str
    mime_type: str
    hash: str
    size: int
    is_image: bool
    is_video: bool
    width: int
    height: int
    ms_duration: int


class RemovedChangeDict(TypedDict):
    removed: Literal[True]
    node_id: str


class UpsertChangeDict(TypedDict):
    removed: Literal[False]
    node: NodeRecordDict


type ChangeDict = RemovedChangeDict | UpsertChangeDict


def node_from_record(record: NodeRecord) -> Node:
    return Node(
        id=str(record.id),
        parent_id=str(record.parent_id) if record.parent_id is not None else None,
        name=record.name,
        is_directory=record.is_directory,
        is_trashed=False,
        ctime=record.ctime,
        mtime=record.mtime,
        mime_type=record.mime_type,
        hash=record.hash,
        size=record.size,
        is_image=record.is_image,
        is_video=record.is_video,
        width=record.width,
        height=record.height,
        ms_duration=record.ms_duration,
        private=None,
    )


def node_record_to_dict(record: NodeRecord) -> NodeRecordDict:
    return {
        "id": str(record.id),
        "mutable_id": str(record.mutable_id),
        "parent_id": str(record.parent_id) if record.parent_id is not None else None,
        "name": record.name,
        "is_directory": record.is_directory,
        "ctime": record.ctime.isoformat(),
        "mtime": record.mtime.isoformat(),
        "mime_type": record.mime_type,
        "hash": record.hash,
        "size": record.size,
        "is_image": record.is_image,
        "is_video": record.is_video,
        "width": record.width,
        "height": record.height,
        "ms_duration": record.ms_duration,
    }


def node_record_from_dict(data: NodeRecordDict) -> NodeRecord:
    ctime = datetime.fromisoformat(data["ctime"])
    mtime = datetime.fromisoformat(data["mtime"])
    if ctime.tzinfo is None:
        ctime = ctime.replace(tzinfo=UTC)
    if mtime.tzinfo is None:
        mtime = mtime.replace(tzinfo=UTC)
    parent_id = data["parent_id"]
    return NodeRecord(
        id=MirrorStableId(data["id"]),
        mutable_id=MirrorMutableId(data["mutable_id"]),
        parent_id=MirrorStableId(parent_id) if parent_id is not None else None,
        name=data["name"],
        is_directory=data["is_directory"],
        ctime=ctime,
        mtime=mtime,
        mime_type=data["mime_type"],
        hash=data["hash"],
        size=data["size"],
        is_image=data["is_image"],
        is_video=data["is_video"],
        width=data["width"],
        height=data["height"],
        ms_duration=data["ms_duration"],
    )


def guess_mime_type(name: str, *, is_directory: bool) -> str:
    if is_directory:
        return FOLDER_MIME_TYPE
    return mimetypes.guess_type(name)[0] or "application/octet-stream"


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_from_timestamp(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, UTC)
