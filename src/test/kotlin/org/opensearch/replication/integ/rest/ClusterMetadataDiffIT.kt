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

import org.assertj.core.api.Assertions.assertThat
import org.junit.Assert
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.PutIndexTemplateRequest
import org.opensearch.replication.MultiClusterAnnotations
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.test.rest.OpenSearchRestTestCase

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class ClusterMetadataDiffIT : MultiClusterRestTestCase() {

    fun `test diff API returns categories when clusters are connected`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        // Create an index on the leader that doesn't exist on the follower
        val indexName = "test-diff-index"
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(indexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        // Call the diff API on the follower
        val diffResponse = clusterMetadataDiff(followerClient, "source")

        // Verify response structure
        assertThat(diffResponse).containsKey("connection_name")
        assertThat(diffResponse["connection_name"]).isEqualTo("source")
        assertThat(diffResponse).containsKey("remote_metadata_version")
        assertThat(diffResponse).containsKey("local_metadata_version")
        assertThat(diffResponse).containsKey("categories")

        // The leader index should show up as remote_only in the indices category
        val categories = diffResponse["categories"] as Map<String, Any>
        assertThat(categories).containsKey("indices")
        val indicesCategory = categories["indices"] as Map<String, Any>
        assertThat(indicesCategory["remote_only"] as Int).isGreaterThanOrEqualTo(1)
    }

    fun `test diff API with category filter`() {
        val followerClient = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        // Call with only templates category
        val diffResponse = clusterMetadataDiff(followerClient, "source", "templates_v2")

        val categories = diffResponse["categories"] as Map<String, Any>
        assertThat(categories).containsKey("templates_v2")
        // Should not contain other categories
        assertThat(categories).doesNotContainKey("indices")
        assertThat(categories).doesNotContainKey("ingest_pipelines")
    }

    fun `test diff API with invalid connection returns error`() {
        val followerClient = getClientForCluster(FOLLOWER)
        try {
            clusterMetadataDiff(followerClient, "nonexistent-connection")
            Assert.fail("Diff API should fail with invalid connection")
        } catch (e: ResponseException) {
            assertThat(e.response.statusLine.statusCode).isIn(400, 500)
        }
    }

    fun `test diff API shows in_sync when clusters have same templates`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        // Create the same legacy template on both clusters
        val templateName = "test-diff-template"
        val templateRequest = PutIndexTemplateRequest(templateName)
            .patterns(listOf("test-diff-*"))
        leaderClient.indices().putTemplate(templateRequest, RequestOptions.DEFAULT)
        followerClient.indices().putTemplate(templateRequest, RequestOptions.DEFAULT)

        val diffResponse = clusterMetadataDiff(followerClient, "source", "templates")
        val categories = diffResponse["categories"] as Map<String, Any>
        val templatesCategory = categories["templates"] as Map<String, Any>
        assertThat(templatesCategory["in_sync"] as Int).isGreaterThanOrEqualTo(1)
    }

    private fun clusterMetadataDiff(
        client: org.opensearch.client.RestHighLevelClient,
        connectionName: String,
        categories: String? = null
    ): Map<String, Any> {
        var path = "/_plugins/_replication/$connectionName/_diff"
        if (categories != null) {
            path += "?categories=$categories"
        }
        val request = Request("GET", path)
        val response = client.lowLevelClient.performRequest(request)
        return OpenSearchRestTestCase.entityAsMap(response)
    }
}
