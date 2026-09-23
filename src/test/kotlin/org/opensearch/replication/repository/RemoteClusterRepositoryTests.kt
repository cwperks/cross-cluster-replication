/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.replication.repository

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.opensearch.action.NoShardAvailableActionException
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.routing.IndexRoutingTable
import org.opensearch.cluster.routing.RecoverySource
import org.opensearch.cluster.routing.RoutingTable
import org.opensearch.cluster.routing.ShardRouting
import org.opensearch.cluster.routing.ShardRoutingState
import org.opensearch.cluster.routing.TestShardRouting
import org.opensearch.cluster.routing.UnassignedInfo
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.test.OpenSearchTestCase

class RemoteClusterRepositoryTests : OpenSearchTestCase() {

    fun `test resolving an unassigned primary returns a retryable exception`() {
        val shardId = ShardId(Index("leader-index", "leader-index-uuid"), 0)
        val unassignedPrimary = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, null)
        )
        val routingTable = RoutingTable.builder()
            .add(IndexRoutingTable.builder(shardId.index).addShard(unassignedPrimary).build())
            .build()
        val clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .routingTable(routingTable)
            .build()

        assertThatThrownBy { RemoteClusterRepository.resolvePrimaryNode(clusterState, shardId) }
            .isInstanceOf(NoShardAvailableActionException::class.java)
            .hasMessageContaining("primary shard is not assigned")
    }

    fun `test resolving a primary with a missing node returns a retryable exception`() {
        val shardId = ShardId(Index("leader-index", "leader-index-uuid"), 0)
        val primary = TestShardRouting.newShardRouting(
            shardId.indexName,
            shardId.id,
            "missing-node",
            true,
            ShardRoutingState.INITIALIZING
        )
        val routingTable = RoutingTable.builder()
            .add(IndexRoutingTable.builder(shardId.index).addShard(primary).build())
            .build()
        val clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .routingTable(routingTable)
            .build()

        assertThatThrownBy { RemoteClusterRepository.resolvePrimaryNode(clusterState, shardId) }
            .isInstanceOf(NoShardAvailableActionException::class.java)
            .hasMessageContaining("primary shard node is not available")
    }
}
