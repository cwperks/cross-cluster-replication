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

package org.opensearch.replication.integ.rest

import org.opensearch.replication.MultiClusterAnnotations
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.startReplication
import org.opensearch.replication.stopReplication
import org.opensearch.replication.updateAutoFollowPattern
import org.opensearch.replication.deleteAutoFollowPattern
import org.opensearch.replication.followerStats
import org.opensearch.replication.leaderStats
import org.opensearch.replication.replicationStatus
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.assertj.core.api.Assertions.assertThat
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.RestHighLevelClient
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.common.settings.Settings
import org.opensearch.test.OpenSearchTestCase.assertBusy
import org.apache.logging.log4j.LogManager
import java.util.concurrent.TimeUnit

const val SWITCHOVER_ACTIVE = "leaderCluster"
const val SWITCHOVER_STANDBY = "followCluster"

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = SWITCHOVER_ACTIVE),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = SWITCHOVER_STANDBY)
)
class SwitchoverIT : MultiClusterRestTestCase() {

    companion object {
        private val log = LogManager.getLogger(SwitchoverIT::class.java)
    }

    fun `test switchover promotes follower to active with autofollow`() {
        val leaderClient = getClientForCluster(SWITCHOVER_ACTIVE)
        val followerClient = getClientForCluster(SWITCHOVER_STANDBY)
        createConnectionBetweenClusters(SWITCHOVER_STANDBY, SWITCHOVER_ACTIVE)

        // Enable standby mode on follower
        setStandbyMode(followerClient, true)

        // Create indices on leader with data
        createIndex(leaderClient, "index-a")
        createIndex(leaderClient, "index-b")
        bulkIndex(leaderClient, "index-a", 100)
        bulkIndex(leaderClient, "index-b", 50)

        // Set up autofollow on follower to replicate all user indices
        followerClient.updateAutoFollowPattern("source", "replicate-all", "*")

        // Wait for indices to replicate and data to sync
        assertBusy({
            assertThat(followerClient.indices().exists(GetIndexRequest("index-a"), RequestOptions.DEFAULT)).isTrue()
            assertThat(followerClient.indices().exists(GetIndexRequest("index-b"), RequestOptions.DEFAULT)).isTrue()
        }, 60, TimeUnit.SECONDS)
        assertBusy({
            assertThat(docCount(followerClient, "index-a")).isEqualTo(100)
            assertThat(docCount(followerClient, "index-b")).isEqualTo(50)
        }, 60, TimeUnit.SECONDS)

        // Log replication stats
        logReplicationStats(leaderClient, followerClient, listOf("index-a", "index-b"))

        // Verify zero replication lag before switchover
        assertZeroLag(followerClient, "index-a")
        assertZeroLag(followerClient, "index-b")

        // --- SWITCHOVER: Promote follower ---

        // 1. Remove autofollow pattern (stop picking up new indices)
        followerClient.deleteAutoFollowPattern("source", "replicate-all")

        // 2. Stop replication on each replicated index
        followerClient.stopReplication("index-a")
        followerClient.stopReplication("index-b")

        // 3. Disable standby mode — follower is now the active cluster
        setStandbyMode(followerClient, false)

        // --- VERIFY: Promoted cluster accepts writes ---
        bulkIndex(followerClient, "index-a", 50, startId = 100)
        assertThat(docCount(followerClient, "index-a")).isEqualTo(150)

        // Original data on index-b is intact
        assertThat(docCount(followerClient, "index-b")).isEqualTo(50)
        log.info("Switchover complete: follower promoted, verified writes and data integrity")
    }

    fun `test switchover with explicit start replication`() {
        val leaderClient = getClientForCluster(SWITCHOVER_ACTIVE)
        val followerClient = getClientForCluster(SWITCHOVER_STANDBY)
        createConnectionBetweenClusters(SWITCHOVER_STANDBY, SWITCHOVER_ACTIVE)

        // Enable standby mode on follower
        setStandbyMode(followerClient, true)

        // Create and populate index on leader
        createIndex(leaderClient, "switchover-test")
        bulkIndex(leaderClient, "switchover-test", 200)

        // Start explicit replication
        followerClient.startReplication(
            StartReplicationRequest("source", "switchover-test", "switchover-test"),
            waitForRestore = true
        )

        // Verify data replicated
        assertBusy({
            assertThat(docCount(followerClient, "switchover-test")).isEqualTo(200)
        }, 60, TimeUnit.SECONDS)

        // Log replication stats
        logReplicationStats(leaderClient, followerClient, listOf("switchover-test"))

        // Verify zero replication lag before switchover
        assertZeroLag(followerClient, "switchover-test")

        // --- SWITCHOVER ---
        followerClient.stopReplication("switchover-test")
        setStandbyMode(followerClient, false)

        // Promoted follower can write
        bulkIndex(followerClient, "switchover-test", 100, startId = 200)
        assertThat(docCount(followerClient, "switchover-test")).isEqualTo(300)
        log.info("Switchover complete: verified 300 total docs after promotion write")
    }

    private fun setStandbyMode(client: RestHighLevelClient, enabled: Boolean) {
        val settings = Settings.builder()
            .put("cluster.standby_mode", enabled)
            .build()
        val request = ClusterUpdateSettingsRequest().persistentSettings(settings)
        val response = client.cluster().putSettings(request, RequestOptions.DEFAULT)
        assertThat(response.isAcknowledged).isTrue()
    }

    private fun createIndex(client: RestHighLevelClient, name: String) {
        val response = client.indices().create(CreateIndexRequest(name), RequestOptions.DEFAULT)
        assertThat(response.isAcknowledged).isTrue()
    }

    private fun bulkIndex(client: RestHighLevelClient, index: String, count: Int, startId: Int = 0) {
        val batchSize = 50
        for (batch in startId until startId + count step batchSize) {
            val bulkRequest = BulkRequest()
            val end = minOf(batch + batchSize, startId + count)
            for (i in batch until end) {
                bulkRequest.add(IndexRequest(index).id(i.toString()).source(mapOf("value" to i, "data" to "doc-$i")))
            }
            val response = client.bulk(bulkRequest, RequestOptions.DEFAULT)
            assertThat(response.hasFailures()).withFailMessage("Bulk indexing failed: ${response.buildFailureMessage()}").isFalse()
        }
        client.lowLevelClient.performRequest(Request("POST", "/$index/_refresh"))
        log.info("Bulk indexed $count docs into $index (startId=$startId)")
    }

    private fun assertZeroLag(followerClient: RestHighLevelClient, index: String) {
        assertBusy({
            val status = followerClient.replicationStatus(index)
            val shardDetails = status["shard_replication_details"] as? Map<*, *>
            if (shardDetails != null) {
                for ((shard, details) in shardDetails) {
                    val taskDetails = (details as? Map<*, *>)?.get("syncing_task_details") as? Map<*, *>
                    if (taskDetails != null) {
                        val leaderCp = (taskDetails["leader_checkpoint"] as Number).toLong()
                        val followerCp = (taskDetails["follower_checkpoint"] as Number).toLong()
                        val lag = leaderCp - followerCp
                        log.info("Replication lag for [$index][$shard]: $lag (leader=$leaderCp, follower=$followerCp)")
                        assertThat(lag).withFailMessage("Replication lag for [$index][$shard] is $lag, expected 0").isEqualTo(0L)
                    }
                }
            }
        }, 30, TimeUnit.SECONDS)
        log.info("Zero replication lag confirmed for [$index]")
    }

    private fun logReplicationStats(leaderClient: RestHighLevelClient, followerClient: RestHighLevelClient, indices: List<String>) {
        try {
            val leaderStats = leaderClient.leaderStats()
            log.info("Leader stats: $leaderStats")
        } catch (e: Exception) {
            log.warn("Could not fetch leader stats: ${e.message}")
        }
        try {
            val followerStats = followerClient.followerStats()
            log.info("Follower stats: $followerStats")
        } catch (e: Exception) {
            log.warn("Could not fetch follower stats: ${e.message}")
        }
        for (index in indices) {
            try {
                val status = followerClient.replicationStatus(index)
                log.info("Replication status for [$index]: $status")
            } catch (e: Exception) {
                log.warn("Could not fetch replication status for [$index]: ${e.message}")
            }
        }
    }
}
