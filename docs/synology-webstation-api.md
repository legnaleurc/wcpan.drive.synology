# Synology WebStation SynologyDrive API Reference

This document is a self-contained reference for the Synology Drive APIs exposed through DSM WebStation `webapi/entry.cgi`.

It covers only the `SYNO.SynologyDrive.*` namespaces:

- `SYNO.SynologyDrive.Files`
- `SYNO.SynologyDrive.Tasks`
- `SYNO.SynologyDrive.Webhooks`

All examples are sanitized. Server names, IDs, paths, and tokens are placeholders.

## Scope And Verification

The reference combines two sources:

- exported Synology Drive OpenAPI schemas for method fields and response shapes
- live WebStation probing for transport rules and async task behavior

Verification status:

- `SYNO.SynologyDrive.Files`
  - `get`, `list`, `search`, `create`, `update`, `move`, `delete`, `download`, `upload`: WebStation transport live-verified
- `SYNO.SynologyDrive.Tasks`
  - `list`, `get`: live-verified
  - conflict payload for `move` with `conflict_action=stop`: live-verified
- `SYNO.SynologyDrive.Webhooks`
  - namespace exists and methods are recognized: live-verified
  - `create`, `get`, `delete`: live-verified with JSON-encoded query parameters
  - field-level schemas: taken from the exported Synology schema

## Common Rules

### Entrypoint

All documented methods use:

```http
/webapi/entry.cgi
```

Authentication is via an existing DSM WebAPI session id:

- `_sid=<sid>`

This document assumes the caller already has a valid `_sid`.

### Response Envelope

The common JSON envelope is:

```json
{
  "success": true,
  "data": {}
}
```

or:

```json
{
  "success": false,
  "error": {
    "code": 401,
    "errors": {
      "message": "..."
    }
  }
}
```

### Normal Transport

For `Files`, `Tasks`, and `Webhooks`, the working WebStation transport is:

```http
GET /webapi/entry.cgi
```

Query parameters always include:

- `api=<namespace>`
- `version=<version>`
- `method=<method>`
- `_sid=<sid>`

Observed compatibility notes:

- The WebStation API index advertises `requestFormat: JSON` for these namespaces.
- In practice, the working transport is query-string RPC with JSON-encoded parameter values.
- Scalar fields such as `webhook_id` and `app_id` must be JSON-encoded as strings.
- Array-valued fields such as `files` must be JSON-stringified inside a single query parameter.

Working example:

```text
api=SYNO.SynologyDrive.Files
version=11
method=move
_sid=<sid>
to_parent_folder="id:<folder-id>"
conflict_action="stop"
files=["id:<file-id>"]
```

### Upload Transport

`SYNO.SynologyDrive.Files.upload` uses multipart upload instead of the normal query-string RPC.

Working pattern:

```http
POST /webapi/entry.cgi?api=SYNO.SynologyDrive.Files&version=11&method=upload&_sid=<sid>
Content-Type: multipart/form-data
```

Multipart fields:

- `file`
- `path`
- optional upload fields such as `type`, `conflict_action`, `mute`

Observed working `path` form for upload:

- `id:<parent-id>/<basename>`

### Path And ID Forms

Observed accepted path identifier forms on WebStation:

- `link:<permanent_link>`
- `id:<file_id>`
- `id:<file_id>/<basename>`
- `/mydrive/<relative-path>`
- `/team-folders/<team-folder-name>/<relative-path>`
- `/views/<view_id>/<relative-path>`
- `/volumes/<absolute-path>`

Observed response path behavior:

- `display_path` is the full accessible path
- `path` is a Drive-relative path in the current navigation context

This difference matters when comparing request payloads with returned metadata.

### Async Behavior

`SYNO.SynologyDrive.Files.move` returns an async task id immediately.

The initial response does not prove the move succeeded. The authoritative result is in:

- `SYNO.SynologyDrive.Tasks.get`

Observed example for a conflict with `conflict_action=stop`:

- initial move call: `success: true` with `async_task_id`
- later task result: `status: finished` and `result.errors[0].code == 1022`

## `SYNO.SynologyDrive.Files`

Namespace metadata:

- namespace: `SYNO.SynologyDrive.Files`
- advertised WebStation version: `11`
- entrypoint: `/webapi/entry.cgi`

### Method Summary

| Method | Purpose | Status |
| --- | --- | --- |
| `get` | Fetch one file or folder by path or id | live-verified |
| `list` | List children in a folder | live-verified |
| `search` | Search files | live-verified |
| `create` | Create a folder or small file | live-verified |
| `update` | Rename or update metadata | live-verified |
| `move` | Move files or folders | live-verified |
| `delete` | Delete files or folders | live-verified |
| `download` | Download files | live-verified |
| `upload` | Multipart upload | live-verified |

### Common File Metadata Shape

Most successful `Files` methods return a `FileInfo` object or an array of them.

Observed field semantics from a live `list` response:

- `file_id`: file or folder id
- `parent_id`: parent folder id
- `name`
- `type`: `file` or `dir`
- `content_type`: media or preview classification; for example a regular file can be `type: "file"` with `content_type: "image"`, while folders use `content_type: "dir"`
- `path`: path relative to the requested parent or navigation root; for `list(path="id:<folder-id>")` the child entries are returned as `/<child-name>`, not as full Drive paths
- `display_path`: full accessible Drive path such as `/team-folders/<share>/<path>`
- `size`: bytes; folders are `0`
- `hash`: content hash for files, empty string for folders
- `created_time`
- `modified_time`
- `access_time`
- `change_time`
- `capabilities`
- `version_id`: observed as a string in live payloads
- `sync_id`
- `change_id`
- `max_id`
- `removed`
- `shared`
- `starred`
- `encrypted`

Timestamp caveat from the observed payload:

- `modified_time` looks like the file content timestamp
- `created_time`, `access_time`, and `change_time` look more like Drive record or sync-event timestamps than filesystem birth / access / ctime values
- `image_metadata.time` exists in the observed payload, but its meaning is still unknown

The exported schema and live payloads also include:

- `permanent_link`
- `owner`
- `shared_with`
- `labels`
- `properties`
- `app_properties`
- `image_metadata`
- `watermark_version`
- `disable_download`
- `enable_watermark`
- `force_watermark_download`
- `support_remote`
- `transient`
- `uploaded_size`
- `last_modified_by`
- `dsm_path`
- `sync_to_device`
- `revisions`
- `content_snippet`
- `adv_shared`
- `in_disconnected_cold_tier`
- `node_locking`

### `get`

Transport:

```text
api=SYNO.SynologyDrive.Files
version=11
method=get
_sid=<sid>
path=<full-path-or-id-ref>
```

Required fields:

- `path`

Accepted path identifier forms observed on WebStation:

- `link:<permanent_link>`
- `id:<file_id>`
- `id:<file_id>/<basename>`
- `/mydrive/<relative-path>`
- `/team-folders/<team-folder-name>/<relative-path>`
- `/views/<view_id>/<relative-path>`
- `/volumes/<absolute-path>`

Response:

- `success: true`
- `data: <FileInfo>`

### `list`

Transport:

```text
api=SYNO.SynologyDrive.Files
version=11
method=list
_sid=<sid>
path=<folder-path-or-id-ref>
sort_by=name
sort_direction=asc
offset=0
limit=0
filter={"type":["file","dir"]}
extra=["sync_to_device"]
```

Required fields:

- `path`

Accepted path identifier forms observed on WebStation:

- `link:<permanent_link>`
- `id:<file_id>`
- `id:<file_id>/<basename>`
- `/mydrive/<relative-path>`
- `/team-folders/<team-folder-name>/<relative-path>`
- `/views/<view_id>/<relative-path>`
- `/volumes/<absolute-path>`

Optional query-style fields:

- `sort_by`
- `sort_direction`
- `offset`
- `limit`

Optional body-style fields carried in the form payload:

- `filter`
  - `extensions`
  - `type`
  - `label_id`
  - `starred`
- `extra`
  - currently documented value: `sync_to_device`

Response:

```json
{
  "success": true,
  "data": {
    "total": 2,
    "items": [
      {
        "file_id": "<file-id>",
        "name": "a.txt",
        "type": "file",
        "content_type": "image",
        "path": "/a.txt",
        "display_path": "/team-folders/share/folder/a.txt"
      }
    ]
  }
}
```

### `search`

`search` is available on WebStation and was live-verified as a recognized working method.

This document records method availability and transport compatibility. It does not restate the full search schema because the current integration does not depend on it.

Transport:

```text
api=SYNO.SynologyDrive.Files
version=11
method=search
_sid=<sid>
...
```

### `create`

`create` supports:

- folder creation
- small file creation via base64 body content

Transport:

```text
api=SYNO.SynologyDrive.Files
version=11
method=create
_sid=<sid>
type=folder
path=id:<parent-id>/new-folder
conflict_action=stop
mute=false
```

Required fields:

- `type`
  - valid values: `file`, `folder`
- `path`

Optional fields:

- `conflict_action`
  - `overwrite`, `autorename`, `stop`
- `mute`
- `permanent_link`
- `encrypted`
- `removed`
- `file_content`
  - base64 encoded
  - limited to about 1 MB
- `labels`
- `modified_time`
- `access_time`
- `created_time`

Response:

- `success: true`
- `data: <FileInfo>`

### `update`

`update` is the rename and metadata-update method on WebStation.

Transport:

```text
api=SYNO.SynologyDrive.Files
version=11
method=update
_sid=<sid>
path=id:<file-id>
name=new-name.txt
```

Required fields:

- `path`

Optional fields:

- `name`
- `mute`
- `encrypted`
- `removed`
- `starred`
- `labels`
- `modified_time`
- `created_time`
- `access_time`

Observed behavior:

- rename conflicts are surfaced as normal API errors
- move conflicts are not surfaced here because `move` is async

Response:

- `success: true`
- `data: <FileInfo>`

### `move`

Transport:

```text
api=SYNO.SynologyDrive.Files
version=11
method=move
_sid=<sid>
to_parent_folder=id:<folder-id>
conflict_action=stop
files=["id:<file-id>"]
```

Required fields:

- `to_parent_folder`
- `files`

Optional fields:

- `dry_run`
- `conflict_action`
  - `overwrite`, `autorename`, `skip`, `stop`, `version`

Important transport rule:

- `files` must be one JSON-stringified array field, not repeated `files=` entries

Immediate response:

```json
{
  "success": true,
  "data": {
    "async_task_id": "task-<n>"
  }
}
```

Authoritative result:

- poll `SYNO.SynologyDrive.Tasks.get(task_id)`

Observed conflict behavior for `conflict_action=stop`:

- initial move response still returns `success: true`
- the task later finishes with:
  - `result.errors[0].code == 1022`
  - `result.errors[0].message == "file operation is stopped"`
- source and destination remain unchanged after the blocked move

### `delete`

Transport:

```text
api=SYNO.SynologyDrive.Files
version=11
method=delete
_sid=<sid>
files=["id:<file-id>"]
permanent=false
```

Required fields:

- `files`

Optional fields:

- `permanent`

Transport note:

- `files` follows the same JSON-stringified array rule as `move`

Response:

- `success: true`
- async tracking may be needed depending on the operation result path

### `download`

Transport:

```text
api=SYNO.SynologyDrive.Files
version=11
method=download
_sid=<sid>
files=["id:<file-id>"]
dry_run=false
force_download=true
archive_name=download
```

Required fields:

- `files`

Optional fields:

- `dry_run`
- `decrypt`
- `force_download`
- `archive_name`

Behavior:

- one file may be returned directly
- multiple files are archived into a zip stream
- `dry_run=true` is used to validate whether the download can proceed

### `upload`

Transport:

```http
POST /webapi/entry.cgi?api=SYNO.SynologyDrive.Files&version=11&method=upload&_sid=<sid>
Content-Type: multipart/form-data
```

Multipart fields:

- required:
  - `file`
  - `path`
- optional:
  - `conflict_action`
  - `type`
  - `mute`
  - `encrypted`
  - `starred`
  - `labels`
  - `modified_time`
  - `created_time`
  - `access_time`

Accepted path identifier forms observed on WebStation:

- `link:<permanent_link>`
- `id:<file_id>`
- `id:<file_id>/<basename>`
- `/mydrive/<relative-path>`
- `/team-folders/<team-folder-name>/<relative-path>`
- `/views/<view_id>/<relative-path>`
- `/volumes/<absolute-path>`

Example:

```text
path=id:<parent-id>/report.pdf
type=file
conflict_action=stop
```

Response:

- `success: true`
- `data: <FileInfo>`

## `SYNO.SynologyDrive.Tasks`

Namespace metadata:

- namespace: `SYNO.SynologyDrive.Tasks`
- version: `1`
- entrypoint: `/webapi/entry.cgi`

This namespace is the authoritative source for async Drive operation results.

### `list`

Transport:

```text
api=SYNO.SynologyDrive.Tasks
version=1
method=list
_sid=<sid>
```

Response:

```json
{
  "success": true,
  "data": {
    "items": [
      {
        "task_id": "task-<n>",
        "status": "in_progress",
        "progress": 0,
        "result": {
          "action": "move",
          "errors": null,
          "names": ["sample.txt"],
          "params": {
            "files": [
              {
                "path": "id:<file-id>"
              }
            ]
          }
        }
      }
    ],
    "total": 1
  }
}
```

### `get`

Transport:

```text
api=SYNO.SynologyDrive.Tasks
version=1
method=get
_sid=<sid>
task_id=task-<n>
```

Success response:

```json
{
  "success": true,
  "data": {
    "task_id": "task-<n>",
    "status": "finished",
    "progress": 100,
    "result": {
      "action": "move",
      "errors": [],
      "processed_size": 123,
      "total_size": 123,
      "names": ["sample.txt"],
      "params": {
        "files": [
          {
            "path": "id:<file-id>"
          }
        ]
      },
      "targets": [
        {
          "file_id": "<file-id>",
          "parent_id": "<parent-id>",
          "file_type": "file",
          "name": "sample.txt",
          "path": "/folder/sample.txt"
        }
      ]
    }
  }
}
```

Observed invalid task response:

```json
{
  "success": false,
  "error": {
    "code": 401,
    "errors": {
      "message": "invalid task id"
    }
  }
}
```

Observed move-conflict response:

```json
{
  "success": true,
  "data": {
    "task_id": "task-<n>",
    "status": "finished",
    "progress": 100,
    "result": {
      "action": "move",
      "errors": [
        {
          "code": 1022,
          "message": "file operation is stopped",
          "context": {
            "file_type": "file",
            "name": "sample.txt",
            "path": "/folder/sample.txt"
          }
        }
      ],
      "processed_size": 0,
      "total_size": 123
    }
  }
}
```

Practical rule:

- for async file operations, task status is authoritative
- do not treat the initial `async_task_id` response as final success

## `SYNO.SynologyDrive.Webhooks`

Namespace metadata:

- namespace: `SYNO.SynologyDrive.Webhooks`
- version: `2`
- entrypoint: `/webapi/entry.cgi`

Methods recognized on WebStation:

- `create`
- `get`
- `update`
- `delete`
- `list`

Transport:

```text
api=SYNO.SynologyDrive.Webhooks
version=2
method=<create|get|update|delete|list>
_sid=<sid>
...
```

### Webhook Object

Fields:

- `webhook_id`
- `app_id`
- `type`
  - `url` or `shared_library`
- `url`
- `so_name`
- `token`
- `options`

`options` fields:

- `filter_file_ext`
- `filter_events`

### `create`

Required fields:

- `type`
- `app_id`

Additional required field for URL webhook:

- `url`

Optional fields:

- `token`
- `options`
- `so_name`

Response:

```json
{
  "success": true,
  "data": {
    "type": "url",
    "url": "https://example.invalid/webhook",
    "token": "",
    "app_id": "<app-id>",
    "webhook_id": "<webhook-id>"
  }
}
```

### `get`

Required fields:

- `webhook_id`
- `app_id`

Observed live-verified transport:

- query parameters are JSON-encoded strings
- example shape:
  - `api=SYNO.SynologyDrive.Webhooks`
  - `version=2`
  - `method=get`
  - `_sid=<sid>`
  - `webhook_id="<webhook-id>"`
  - `app_id="<app-id>"`

Response:

- `success: true`
- `data: <WebhookObject>`

### `update`

Required fields:

- `webhook_id`
- `app_id`
- `type`

Optional fields:

- `url`
- `token`
- `options`
- `so_name`

Response:

- `success: true`
- `data: <WebhookObject>`

### `delete`

Required fields:

- `webhook_id`
- `app_id`

Observed live-verified transport:

- query parameters are JSON-encoded strings
- example shape:
  - `api=SYNO.SynologyDrive.Webhooks`
  - `version=2`
  - `method=delete`
  - `_sid=<sid>`
  - `webhook_id="<webhook-id>"`
  - `app_id="<app-id>"`

Response:

```json
{
  "success": true
}
```

### `list`

Required fields:

- `app_id`

Response:

```json
{
  "success": true,
  "data": {
    "total": 1,
    "items": [
      {
        "type": "url",
        "url": "https://example.invalid/webhook",
        "token": "",
        "app_id": "<app-id>",
        "webhook_id": "<webhook-id>"
      }
    ]
  }
}
```

## Compatibility Notes For Migration

The WebStation SynologyDrive APIs cover the current integration surface:

- file metadata lookup
- folder listing
- create folder
- rename
- move
- delete
- upload
- download
- webhook management
- async task tracking

Important incompatibilities from the REST-style API shape:

1. Calls go to `/webapi/entry.cgi` with `api`, `version`, and `method`, not resource paths.
2. Authentication uses `_sid`.
3. Normal requests are query-string RPC calls.
4. Parameter values are JSON-encoded, including scalar ids like `app_id` and `webhook_id`.
5. Array params such as `files` must be JSON-stringified.
6. Upload uses multipart and places `api`, `version`, `method`, and `_sid` in the query string.
7. `move` success must be confirmed with `SYNO.SynologyDrive.Tasks.get`.
