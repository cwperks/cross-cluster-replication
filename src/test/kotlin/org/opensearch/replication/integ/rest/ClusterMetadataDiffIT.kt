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
    companion object {
        private const val CONNECTION_NAME = "source"
    }

    fun `test diff API returns categories when clusters are connected`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        // Create an index on the leader that doesn't exist on the follower
        val indexName = "test-diff-index"
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(indexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        // Call the diff API on the follower
        val diffResponse = clusterMetadataDiff(followerClient)

        // Verify response structure
        assertThat(diffResponse).containsKey("connection_name")
        assertThat(diffResponse["connection_name"]).isEqualTo("source")
        assertThat(diffResponse).containsKey("remote_metadata_version")
        assertThat(diffResponse).containsKey("local_metadata_version")
        assertThat(diffResponse).containsKey("categories")

        // The leader index should show up as remote_only in the indices category
        val indicesCategory = category(diffResponse, "indices")
        assertThat(indicesCategory["remote_only"] as Int).isGreaterThanOrEqualTo(1)
        assertThat(remoteOnlyItems(indicesCategory)).contains(indexName)
    }

    fun `test diff API with category filter`() {
        val followerClient = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        // Call with only ingest_pipelines category
        val diffResponse = clusterMetadataDiff(followerClient, "ingest_pipelines")

        val categories = categories(diffResponse)
        assertThat(categories).containsKey("ingest_pipelines")
        // Should not contain other categories
        assertThat(categories).doesNotContainKey("indices")
        assertThat(categories).doesNotContainKey("templates")
        val pipelinesCategory = category(diffResponse, "ingest_pipelines")
        assertThat(pipelinesCategory["status"]).isEqualTo("compared")
    }

    fun `test diff API with multiple category filter`() {
        val followerClient = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val diffResponse = clusterMetadataDiff(followerClient, "templates,indices")

        val categories = categories(diffResponse)
        assertThat(categories).containsKeys("templates", "indices")
        assertThat(categories).doesNotContainKey("ingest_pipelines")
        assertThat(category(diffResponse, "templates")["status"]).isEqualTo("compared")
        assertThat(category(diffResponse, "indices")["status"]).isEqualTo("compared")
    }

    fun `test diff API rejects unsupported category`() {
        val followerClient = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        try {
            clusterMetadataDiff(followerClient, "templates_v2")
            Assert.fail("Diff API should reject unsupported categories")
        } catch (e: ResponseException) {
            assertThat(e.response.statusLine.statusCode).isEqualTo(400)
            assertThat(e.message).contains("unsupported categories")
        }
    }

    fun `test diff API with invalid connection returns error`() {
        val followerClient = getClientForCluster(FOLLOWER)
        try {
            clusterMetadataDiff(followerClient, connectionName = "nonexistent-connection")
            Assert.fail("Diff API should fail with invalid connection")
        } catch (e: ResponseException) {
            assertThat(e.response.statusLine.statusCode).isIn(400, 404, 500)
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

        val diffResponse = clusterMetadataDiff(followerClient, "templates")
        val templatesCategory = category(diffResponse, "templates")
        assertThat(templatesCategory["in_sync"] as Int).isGreaterThanOrEqualTo(1)
    }

    fun `test diff API detects diverged index with different replica count`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val indexName = "test-diff-replicas"

        // Create same index on both clusters with different replica counts
        val leaderRequest = CreateIndexRequest(indexName)
        leaderRequest.settings(org.opensearch.common.settings.Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 2))
        leaderClient.indices().create(leaderRequest, RequestOptions.DEFAULT)

        val followerRequest = CreateIndexRequest(indexName)
        followerRequest.settings(org.opensearch.common.settings.Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1))
        followerClient.indices().create(followerRequest, RequestOptions.DEFAULT)

        val diffResponse = clusterMetadataDiff(followerClient, "indices")
        val indicesCategory = category(diffResponse, "indices")

        assertThat(indicesCategory["diverged"] as Int).isGreaterThanOrEqualTo(1)

        // Verify field-level detail shows the replica count difference with conditional policy
        val divergedIndex = divergedItems(indicesCategory).find { it["name"] == indexName }
        assertThat(divergedIndex).isNotNull()

        val replicaField = fields(divergedIndex!!).find { (it["path"] as String).contains("number_of_replicas") }
        assertThat(replicaField).isNotNull()
        assertThat(replicaField!!["local"]).isEqualTo("1")
        assertThat(replicaField["remote"]).isEqualTo("2")
        assertThat(replicaField["policy"]).isEqualTo("conditional")
    }

    fun `test diff API detects diverged index with different mappings`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val indexName = "test-diff-mappings"

        // Create index on leader with a mapping
        val leaderRequest = CreateIndexRequest(indexName)
        leaderRequest.mapping(mapOf("properties" to mapOf(
            "title" to mapOf("type" to "text"),
            "timestamp" to mapOf("type" to "date")
        )))
        leaderClient.indices().create(leaderRequest, RequestOptions.DEFAULT)

        // Create same index on follower with a different mapping
        val followerRequest = CreateIndexRequest(indexName)
        followerRequest.mapping(mapOf("properties" to mapOf(
            "title" to mapOf("type" to "keyword"),
            "count" to mapOf("type" to "integer")
        )))
        followerClient.indices().create(followerRequest, RequestOptions.DEFAULT)

        val diffResponse = clusterMetadataDiff(followerClient, "indices")
        val indicesCategory = category(diffResponse, "indices")

        assertThat(indicesCategory["diverged"] as Int).isGreaterThanOrEqualTo(1)

        val divergedIndex = divergedItems(indicesCategory).find { it["name"] == indexName }
        assertThat(divergedIndex).isNotNull()

        val mappingField = fields(divergedIndex!!).find { (it["path"] as String) == "mappings" }
        assertThat(mappingField).isNotNull()
        assertThat(mappingField!!["policy"]).isEqualTo("included")
    }

    fun `test diff API detects ingest pipeline on remote only`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        // Create an ingest pipeline on the leader only
        val pipelineName = "test-diff-pipeline"
        val putPipelineRequest = Request("PUT", "/_ingest/pipeline/$pipelineName")
        putPipelineRequest.setJsonEntity("""
            {
              "description": "test pipeline for diff",
              "processors": [
                {
                  "set": {
                    "field": "diff_test",
                    "value": "true"
                  }
                }
              ]
            }
        """.trimIndent())
        leaderClient.lowLevelClient.performRequest(putPipelineRequest)

        val diffResponse = clusterMetadataDiff(followerClient, "ingest_pipelines")
        val pipelinesCategory = category(diffResponse, "ingest_pipelines")

        assertThat(pipelinesCategory["remote_only"] as Int).isGreaterThanOrEqualTo(1)
        assertThat(remoteOnlyItems(pipelinesCategory)).contains(pipelineName)
    }

    private fun clusterMetadataDiff(
        client: org.opensearch.client.RestHighLevelClient,
        categories: String? = null,
        connectionName: String = CONNECTION_NAME
    ): Map<String, Any> {
        var path = "/_plugins/_replication/_cluster/$connectionName/_metadata_diff"
        if (categories != null) {
            path += "?categories=$categories"
        }
        val request = Request("GET", path)
        val response = client.lowLevelClient.performRequest(request)
        val responseMap = OpenSearchRestTestCase.entityAsMap(response)
        logger.info("Diff response for [$path]: $responseMap")
        return responseMap
    }

    @Suppress("UNCHECKED_CAST")
    private fun categories(diffResponse: Map<String, Any>): Map<String, Any> {
        return diffResponse["categories"] as Map<String, Any>
    }

    @Suppress("UNCHECKED_CAST")
    private fun category(diffResponse: Map<String, Any>, name: String): Map<String, Any> {
        return categories(diffResponse)[name] as Map<String, Any>
    }

    @Suppress("UNCHECKED_CAST")
    private fun items(category: Map<String, Any>): Map<String, Any> {
        return category["items"] as Map<String, Any>
    }

    @Suppress("UNCHECKED_CAST")
    private fun remoteOnlyItems(category: Map<String, Any>): List<String> {
        return items(category)["remote_only"] as List<String>
    }

    @Suppress("UNCHECKED_CAST")
    private fun divergedItems(category: Map<String, Any>): List<Map<String, Any>> {
        return items(category)["diverged"] as List<Map<String, Any>>
    }

    @Suppress("UNCHECKED_CAST")
    private fun fields(divergedItem: Map<String, Any>): List<Map<String, Any>> {
        return divergedItem["fields"] as List<Map<String, Any>>
    }
}
