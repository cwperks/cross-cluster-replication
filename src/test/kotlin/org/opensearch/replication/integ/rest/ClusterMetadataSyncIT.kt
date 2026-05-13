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
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.assertj.core.api.Assertions.assertThat
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.RestHighLevelClient
import org.opensearch.test.OpenSearchTestCase.assertBusy
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.util.concurrent.TimeUnit

const val CMS_LEADER = "leaderCluster"
const val CMS_FOLLOWER = "followCluster"

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = CMS_LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = CMS_FOLLOWER)
)
class ClusterMetadataSyncIT : MultiClusterRestTestCase() {

    fun `test index template syncs from leader to follower`() {
        val leaderClient = getClientForCluster(CMS_LEADER)
        val followerClient = getClientForCluster(CMS_FOLLOWER)
        createConnectionBetweenClusters(CMS_FOLLOWER, CMS_LEADER)

        val templateName = "test-sync-template"

        try {
            // Create a template on the leader
            putIndexTemplate(leaderClient, templateName, "sync-test-*")

            // Verify template does NOT exist on follower yet
            assertThat(indexTemplateExists(followerClient, templateName)).isFalse()

            // Start cluster metadata sync
            startClusterMetadataSync(followerClient, "source")

            // Wait for template to sync
            assertBusy({
                assertThat(indexTemplateExists(followerClient, templateName)).isTrue()
            }, 60, TimeUnit.SECONDS)

            // Verify template content matches
            val leaderTemplate = getIndexTemplate(leaderClient, templateName)
            val followerTemplate = getIndexTemplate(followerClient, templateName)
            assertThat(followerTemplate).isEqualTo(leaderTemplate)
        } finally {
            stopClusterMetadataSync(followerClient, "source")
            deleteIndexTemplate(leaderClient, templateName)
            deleteIndexTemplate(followerClient, templateName)
        }
    }

    fun `test new template on leader syncs to follower after sync is started`() {
        val leaderClient = getClientForCluster(CMS_LEADER)
        val followerClient = getClientForCluster(CMS_FOLLOWER)
        createConnectionBetweenClusters(CMS_FOLLOWER, CMS_LEADER)

        val templateName = "test-new-template"

        try {
            // Start sync first
            startClusterMetadataSync(followerClient, "source")

            // Then create template on leader
            putIndexTemplate(leaderClient, templateName, "new-test-*")

            // Wait for it to appear on follower
            assertBusy({
                assertThat(indexTemplateExists(followerClient, templateName)).isTrue()
            }, 60, TimeUnit.SECONDS)
        } finally {
            stopClusterMetadataSync(followerClient, "source")
            deleteIndexTemplate(leaderClient, templateName)
            deleteIndexTemplate(followerClient, templateName)
        }
    }

    private fun startClusterMetadataSync(client: RestHighLevelClient, leaderAlias: String) {
        val request = Request("POST", "/_plugins/_replication/_cluster_metadata_sync")
        request.entity = StringEntity("""{"leader_alias": "$leaderAlias"}""", ContentType.APPLICATION_JSON)
        val response = client.lowLevelClient.performRequest(request)
        assertThat(response.statusLine.statusCode).isIn(200, 201)
    }

    private fun stopClusterMetadataSync(client: RestHighLevelClient, leaderAlias: String) {
        try {
            val request = Request("DELETE", "/_plugins/_replication/_cluster_metadata_sync")
            request.entity = StringEntity("""{"leader_alias": "$leaderAlias"}""", ContentType.APPLICATION_JSON)
            client.lowLevelClient.performRequest(request)
        } catch (e: ResponseException) {
            // Ignore if already stopped
        }
    }

    private fun putIndexTemplate(client: RestHighLevelClient, name: String, pattern: String) {
        val request = Request("PUT", "/_index_template/$name")
        request.setJsonEntity("""
            {
              "index_patterns": ["$pattern"],
              "template": {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"@timestamp": {"type": "date"}}}
              }
            }
        """.trimIndent())
        client.lowLevelClient.performRequest(request)
    }

    private fun indexTemplateExists(client: RestHighLevelClient, name: String): Boolean {
        return try {
            client.lowLevelClient.performRequest(Request("GET", "/_index_template/$name"))
            true
        } catch (e: ResponseException) {
            false
        }
    }

    private fun getIndexTemplate(client: RestHighLevelClient, name: String): Map<String, Any> {
        val response = client.lowLevelClient.performRequest(Request("GET", "/_index_template/$name"))
        val body = OpenSearchRestTestCase.entityAsMap(response)
        val templates = body["index_templates"] as List<Map<String, Any>>
        return templates.first()["index_template"] as Map<String, Any>
    }

    private fun deleteIndexTemplate(client: RestHighLevelClient, name: String) {
        try {
            client.lowLevelClient.performRequest(Request("DELETE", "/_index_template/$name"))
        } catch (e: ResponseException) {
            // Ignore
        }
    }
}
