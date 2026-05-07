# CCR Standby Delete Behavior

## Goal

For a standby cluster that may be promoted quickly, the standby should converge to the leader's intended index namespace, while destructive changes remain explicit, observable, and recoverable within policy.

## Current Default CCR Behavior

CCR currently defaults to preserving follower data when a leader index is deleted:

```text
plugins.replication.replicate.delete_index = false
```

With this default, leader index deletion pauses replication and leaves the follower index in place. This is a safe generic CCR default because CCR may be used as a recovery copy rather than a strict mirror.

## Desired Standby Behavior

For a true standby profile, delete propagation should be enabled and treated as part of the expected configuration:

```text
plugins.replication.replicate.delete_index = true
```

Expected behavior:

1. If a followed leader index is deleted, the follower should stop replication and delete the corresponding follower index.
2. Replication metadata for that follower index should be cleaned up.
3. Autofollow should not permanently remember the deleted index as already handled.
4. If the leader recreates the same index name and it matches the autofollow pattern, autofollow should start a fresh replication job.
5. The recreated follower index should contain only the recreated leader index's data, not stale data from the old generation.
6. Status and logs should make the flow visible: delete observed, follower delete started, follower delete acknowledged, metadata removed, recreated leader index followed.

## Index Identity

Index names are not enough for robust behavior. Deleted and recreated indices with the same name are different index generations.

Example:

```text
leader src uuid=A -> follower src follows A
leader deletes src
leader creates src uuid=B
```

Correct standby behavior:

```text
follower src for A is deleted
follower src for B is created and follows B
```

If follower `src` from generation A still exists when leader `src` generation B appears, autofollow should not silently skip forever. It should wait for cleanup to finish, or report a clear blocked state that the same name exists from an older generation.

## Race Handling

Leader delete and recreate can happen quickly, and the follower may observe those changes through polling.

Autofollow behavior should be resilient:

- If autofollow sees leader `src` exists but follower `src` also exists, it should check whether follower `src` is an active replication of the same leader index generation.
- If follower `src` is a stale paused or deleting replication from an older leader generation and delete propagation is enabled, autofollow should not mark the new leader index as permanently failed.
- Autofollow should retry until follower cleanup completes, then start fresh replication.
- If a same-name local index exists and is not CCR-owned, autofollow should not delete it automatically; that should remain blocked.

## Temporary Reindex Indices

Major-version migration may use an in-place reindex flow:

```text
src -> tmp
delete src
create src
tmp -> src
delete tmp
```

For standby mode:

- Autofollow mirrors whatever matches the configured pattern.
- Operators should choose patterns carefully.
- Reindex temp indices should either intentionally match or intentionally not match.
- If the temp index is part of application-visible state during migration, replicate it.
- If it is only local migration scaffolding, exclude it from autofollow patterns.

## Proposed Contract

```text
In normal CCR mode, leader index deletion pauses replication and preserves follower data.

In standby mode / delete-propagation mode, leader index deletion is replicated as a follower index deletion for CCR-owned follower indices. If a leader index is recreated with the same name, autofollow treats it as a new index generation and starts a new replication after the stale follower index and replication metadata are removed.
```

## Guardrails

CCR should not delete arbitrary same-name local indices on the follower. Delete propagation should only apply to indices CCR knows it owns through replication metadata.

Longer term, delete/recreate handling should not rely only on index names. Name-only behavior is fragile because a recreated index with the same name is a different index generation.

## Suggested Tests

Direct replication test:

1. Enable `plugins.replication.replicate.delete_index=true`.
2. Start replication for `src`.
3. Delete leader `src`.
4. Assert follower `src` is deleted.
5. Recreate leader `src`.
6. Assert replication can be started again for the recreated index.

Autofollow test:

1. Enable `plugins.replication.replicate.delete_index=true`.
2. Create an autofollow pattern for `src-*`.
3. Create leader `src-1`.
4. Assert follower `src-1` appears and syncs.
5. Delete leader `src-1`.
6. Assert follower `src-1` is deleted.
7. Recreate leader `src-1`.
8. Assert autofollow creates follower `src-1` again and syncs new docs.

If the autofollow test fails, the likely bug is that autofollow does not properly handle same-name leader index replacement under delete propagation.
