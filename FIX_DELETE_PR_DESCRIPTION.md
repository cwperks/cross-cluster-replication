# Add delete/recreate characterization tests for CCR standby behavior

## What changed

Adds characterization coverage for CCR leader index delete/recreate behavior when delete propagation is explicitly enabled with `plugins.replication.replicate.delete_index=true`.

This includes:

- Direct replication coverage for deleting a leader index, deleting the follower, recreating the leader index with the same name, and starting replication again.
- Autofollow coverage for deleting a followed leader index, waiting for follower cleanup, recreating the same leader index name, and verifying autofollow starts the recreated index generation.
- Autofollow coverage for a quick delete/recreate sequence without waiting for follower cleanup.
- A design note summarizing intended standby delete behavior and guardrails.

## Findings

I originally expected one of these tests to fail as part of a TDD investigation. Locally, all added characterization tests passed. That suggests CCR can already handle the tested delete/recreate scenarios when `plugins.replication.replicate.delete_index=true`.

The likely problem area is therefore not the tested delete propagation path itself, but one of:

- delete propagation being left at the default `false`
- expectations around default CCR behavior versus standby behavior
- a different timing/path than the quick delete/recreate case covered here
- security/system-index-specific behavior

## Validation

- `./gradlew compileTestKotlin`
- `./gradlew integTest --tests 'org.opensearch.replication.integ.rest.StartReplicationIT.test direct replication delete and recreate same leader index with delete propagation enabled'`
- `./gradlew integTest --tests 'org.opensearch.replication.integ.rest.UpdateAutoFollowPatternIT.test auto follow delete and recreate same leader index with delete propagation enabled'`
- `./gradlew integTest --tests 'org.opensearch.replication.integ.rest.UpdateAutoFollowPatternIT.test auto follow quick delete and recreate same leader index with delete propagation enabled'`
