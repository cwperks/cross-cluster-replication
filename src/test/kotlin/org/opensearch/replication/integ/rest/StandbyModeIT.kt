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

import org.opensearch.replication.*
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.assertj.core.api.Assertions.assertThat
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.junit.Assume
import org.junit.Before
import java.util.concurrent.TimeUnit

const val STANDBY_LEADER = "leaderCluster"
const val STANDBY_FOLLOWER = "followCluster"

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = STANDBY_LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = STANDBY_FOLLOWER)
)
class StandbyModeIT : SecurityBase() {

    @Before
    fun beforeTest() {
        Assume.assumeTrue(isSecurityPropertyEnabled)
    }

    fun `test that security system index can be replicated to follower`() {
        val followerClient = getClientForCluster(STANDBY_FOLLOWER)
        createConnectionBetweenClusters(STANDBY_FOLLOWER, STANDBY_LEADER)

        // Start replication of the security index from leader to follower
        val securityIndex = ".opendistro_security"
        followerClient.startReplication(
            StartReplicationRequest("source", securityIndex, securityIndex),
            waitForRestore = true
        )

        // Verify the security index exists on the follower
        assertBusy({
            assertThat(
                followerClient.indices().exists(GetIndexRequest(securityIndex), RequestOptions.DEFAULT)
            ).isTrue()
        }, 30L, TimeUnit.SECONDS)
    }

    fun `test that follower rejects security config mutations when index is replicated`() {
        val followerClient = getClientForCluster(STANDBY_FOLLOWER)
        createConnectionBetweenClusters(STANDBY_FOLLOWER, STANDBY_LEADER)

        val securityIndex = ".opendistro_security"
        followerClient.startReplication(
            StartReplicationRequest("source", securityIndex, securityIndex),
            waitForRestore = true
        )

        // Wait for replication to be active
        assertBusy({
            assertThat(
                followerClient.indices().exists(GetIndexRequest(securityIndex), RequestOptions.DEFAULT)
            ).isTrue()
        }, 30L, TimeUnit.SECONDS)

        // Try to create a role on the follower — should fail because the index is read-only (CCR follower)
        val request = Request("PUT", "/_plugins/_security/api/roles/test_standby_role")
        request.entity = StringEntity(
            """{"cluster_permissions": ["cluster_monitor"]}""",
            ContentType.APPLICATION_JSON
        )
        val response = followerClient.lowLevelClient.performRequest(request)
        // Expect failure — either 403 (standby mode) or 409/500 (read-only follower index)
        assertThat(response.statusLine.statusCode).isNotEqualTo(200)
        assertThat(response.statusLine.statusCode).isNotEqualTo(201)
    }

    fun `test that security config changes on leader are replicated to follower`() {
        val leaderClient = getClientForCluster(STANDBY_LEADER)
        val followerClient = getClientForCluster(STANDBY_FOLLOWER)
        createConnectionBetweenClusters(STANDBY_FOLLOWER, STANDBY_LEADER)

        val securityIndex = ".opendistro_security"
        followerClient.startReplication(
            StartReplicationRequest("source", securityIndex, securityIndex),
            waitForRestore = true
        )

        // Create a new role on the leader
        val createRoleRequest = Request("PUT", "/_plugins/_security/api/roles/replicated_test_role")
        createRoleRequest.entity = StringEntity(
            """{"cluster_permissions": ["cluster_monitor"]}""",
            ContentType.APPLICATION_JSON
        )
        val createResponse = leaderClient.lowLevelClient.performRequest(createRoleRequest)
        assertThat(createResponse.statusLine.statusCode).isIn(200, 201)

        // Verify the role is replicated to the follower
        assertBusy({
            val getRoleRequest = Request("GET", "/_plugins/_security/api/roles/replicated_test_role")
            val getResponse = followerClient.lowLevelClient.performRequest(getRoleRequest)
            assertThat(getResponse.statusLine.statusCode).isEqualTo(200)
        }, 60L, TimeUnit.SECONDS)

        // Cleanup
        val deleteRequest = Request("DELETE", "/_plugins/_security/api/roles/replicated_test_role")
        leaderClient.lowLevelClient.performRequest(deleteRequest)
    }
}
