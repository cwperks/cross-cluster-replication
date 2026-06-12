# Cluster Metadata Diff API

## Overview

A read-only diagnostic API that compares core metadata between a secondary cluster and its primary, using the existing replication handler pipeline to determine what's in sync, what's diverged, and what's expected to differ.

## Location

Lives in the CCR replication plugin. Requires:
- The remote cluster connection (to fetch the peer's cluster state)
- The replication policy (to know what's in-scope and what's intentionally excluded/stripped)
- The relationship context (which cluster is primary, what epoch)

## API

```
GET /_plugins/_replication/_cluster/<connection-name>/_metadata_diff
```

Called on the **secondary only**. The secondary has both its own local state and a connection to the primary. The primary is stateless about the secondary by design.

### Query Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `categories` | comma-separated | all | Filter to specific categories |

Valid categories in the current implementation: `templates`, `ingest_pipelines`, `indices`

The initial implementation compares these categories in provider order:
```
templates → ingest_pipelines → indices
```

### Response

```json
{
  "relationship_id": "us-west-east-dr",
  "local_role": "SECONDARY",
  "remote_metadata_version": 4521,
  "local_applied_metadata_version": 4519,
  "categories": {
    "templates": {
      "status": "compared",
      "in_sync": 4,
      "remote_only": 0,
      "local_only": 0,
      "diverged": 0
    },
    "ingest_pipelines": {
      "status": "compared",
      "in_sync": 12,
      "remote_only": 0,
      "local_only": 0,
      "diverged": 0
    },
    "indices": {
      "status": "compared",
      "in_sync": 287,
      "remote_only": 2,
      "local_only": 0,
      "diverged": 1,
      "items": {
        "remote_only": ["logs-000043", "logs-000044"],
        "diverged": [
          {
            "name": "logs-000042",
            "fields": [
              {
                "path": "settings.index.number_of_replicas",
                "local": "1",
                "remote": "2",
                "policy": "conditional"
              },
              {
                "path": "aliases.logs.is_write_index",
                "local": "false",
                "remote": "true",
                "policy": "included"
              }
            ]
          }
        ]
      }
    }
  }
}
```

## Internal Mechanics

```
1. Fetch remote cluster state via ClusterStateAction
   (same mechanism the MetadataReplicationController uses)

2. For each registered category provider:
   a. extract(remoteState) → Map<String, T>
   b. extract(localState)  → Map<String, T>
   c. Apply the replication policy:
      - Filter to replicable items only
      - Strip excluded fields (index.routing.allocation.*, index.uuid, etc.)
   d. Compare:
      - In remote but not local → remote_only
      - In local but not remote → local_only
      - In both but !handler.equal(local, remote) → diverged
      - In both and equal → in_sync

3. Aggregate and return
```

## Field-Level Diff for Indices

When an index is `diverged`, the response includes which fields differ. The `policy` field classifies each difference:

| Policy | Meaning |
|---|---|
| `included` | This field should be in sync — divergence is unexpected |
| `conditional` | This field may legitimately differ (e.g., `number_of_replicas`) |
| `stripped` | This field is excluded from replication — difference is expected |

Fields classified as `stripped` are not reported in the diff (they're always different by design). Only `included` and `conditional` fields appear.

## Per-Index Sub-Field Policy

Based on the metadata-replication design:

| Sub-field | Policy |
|---|---|
| User-facing settings (analyzers, codec, refresh_interval, ...) | `included` |
| `index.uuid` | stripped (never shown) |
| `index.routing.allocation.*` | stripped (never shown) |
| `index.number_of_replicas` | `conditional` |
| `index.number_of_shards` | `included` (bootstrap only) |
| `index.blocks.*` (user-driven) | `included` |
| Replication-imposed blocks | stripped (never shown) |
| `index.default_pipeline`, `index.final_pipeline` | `included` |
| Mappings | `included` |
| Aliases | `included` |
| `rollover_info` | `included` |
| `in_sync_allocations` | stripped (never shown) |
| `primary_terms` | stripped (never shown) |

## Interpreting the Response

- **`remote_metadata_version` > `local_applied_metadata_version`**: The controller hasn't caught up yet. `remote_only` and `diverged` items may resolve on their own once the controller applies the pending versions.
- **Versions match but `diverged` > 0**: Something is wrong. The controller applied the version but the state doesn't match. Investigate the handler for that category.
- **`local_only` items**: Either legitimately local (provenance-tagged) or something was created on the secondary that shouldn't have been.

## What This Doesn't Cover

- System index contents (plugin state) — separate, more expensive operation
- Data plane (document-level sync) — tracked by shard checkpoints
- *Why* something diverged — that's the status/health API's job
- Remediation — this is read-only diagnostic

## Relationship to Other APIs

| API | Purpose |
|---|---|
| `GET /_replication/cluster/<id>/status` | Is replication running? What's the lag? Any errors? |
| `GET /_plugins/_replication/_cluster/<connection-name>/_metadata_diff` | What's different right now? (this doc) |
| `GET /_replication/cluster/<id>/_switchover/validate` | Are all shards caught up for switchover? |
