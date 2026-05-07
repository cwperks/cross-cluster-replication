# Allow CCR auto-follow to replicate data stream backing indices

## Title

Allow CCR auto-follow to replicate data stream backing indices

## Description

## What changed

This change adds coverage for CCR auto-follow behavior with OpenSearch data streams and allows auto-follow initiated replication to target data stream backing indices.

Specifically:

- Adds an integration test that creates a leader data stream, writes a document through the data stream, and configures auto-follow for the generated `.ds-*` backing index.
- Allows `ReplicateIndexRequest` validation to accept `.ds-*` backing index names when the request is created by auto-follow.
- Keeps the existing dot-index validation for normal direct replication requests and for other dot-prefixed indices.
- Verifies that the replicated follower backing index contains the leader document.
- Characterizes the current limitation: CCR replicates the backing index, but does not recreate the follower-side data stream abstraction.

## Why

Data stream backing indices are hidden dot-prefixed indices such as `.ds-logs-foo-000001`. CCR auto-follow already discovers the backing index, but replication previously failed request validation because CCR treated every dot-prefixed index name as disallowed system-index replication.

The initial failure looked like:

```text
Validation Failed: 1: Value .ds-... must not start with '.';
2: Value .ds-... must not start with '.';
```

This change narrows the exception to auto-follow requests where both the leader and follower names are `.ds-*` backing indices.

## Current limitation

This does not provide complete data stream replication semantics yet.

After this change, CCR can replicate the backing index as a concrete index, but it does not create the follower-side data stream metadata. Searching the replicated `.ds-*` backing index works, but `_data_stream/<name>` does not exist on the follower.

Full data stream support likely needs follow-up work to replicate or reconstruct data stream metadata, templates, rollover behavior, and write-index semantics.

## Validation

- `./gradlew compileTestKotlin`
- `./gradlew integTest --tests 'org.opensearch.replication.integ.rest.UpdateAutoFollowPatternIT.test auto follow data stream backing index does not recreate follower data stream'`
