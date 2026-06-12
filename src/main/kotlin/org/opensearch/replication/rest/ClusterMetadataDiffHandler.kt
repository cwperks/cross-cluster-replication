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

package org.opensearch.replication.rest

import org.opensearch.action.admin.cluster.state.ClusterStateResponse
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.replication.action.diff.CategoryDiff
import org.opensearch.replication.action.diff.ClusterMetadataDiffResponse
import org.opensearch.replication.action.diff.IndexMetadataDiffProvider
import org.opensearch.replication.action.diff.IngestPipelineDiffProvider
import org.opensearch.replication.action.diff.LegacyTemplateDiffProvider
import org.opensearch.replication.action.diff.ClusterMetadataDiffRequest
import org.opensearch.rest.BytesRestResponse
import org.opensearch.transport.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.NamedRoute
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import java.io.IOException

class ClusterMetadataDiffHandler(
    private val clusterService: ClusterService
) : BaseRestHandler() {

    private val diffProviders = listOf(
        LegacyTemplateDiffProvider(),
        IngestPipelineDiffProvider(),
        IndexMetadataDiffProvider()
    ).associateBy { it.category }

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            NamedRoute.Builder()
                .method(RestRequest.Method.GET)
                .path("/_plugins/_replication/_cluster/{connectionName}/_metadata_diff")
                .uniqueName(ClusterMetadataDiffRequest.ACTION_NAME)
                .build()
        )
    }

    override fun getName(): String = "plugins_replication_cluster_metadata_diff"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val connectionName = request.param("connectionName")
        val categoriesParam = request.param("categories", "")
        val categories = if (categoriesParam.isBlank()) {
            ClusterMetadataDiffRequest.ALL_CATEGORIES
        } else {
            categoriesParam.split(",").map { it.trim() }.toSet()
        }
        val diffRequest = ClusterMetadataDiffRequest(connectionName, categories)
        diffRequest.validate()?.let { throw it }

        return RestChannelConsumer { channel ->
            executeDiff(diffRequest, client, channel)
        }
    }

    private fun executeDiff(request: ClusterMetadataDiffRequest, client: NodeClient, channel: RestChannel) {
        val remoteClient = client.getRemoteClusterClient(request.connectionName)
        val clusterStateRequest = remoteClient.admin().cluster().prepareState()
            .clear()
            .setMetadata(true)
            .setCustoms(true)
            .request()

        remoteClient.admin().cluster().state(
            clusterStateRequest,
            ActionListener.wrap(
                { remoteStateResponse: ClusterStateResponse ->
                    try {
                        val response = buildResponse(request, clusterService.state().metadata(), remoteStateResponse.state.metadata())
                        val builder = channel.newBuilder()
                        response.toXContent(builder, channel.request())
                        channel.sendResponse(BytesRestResponse(RestStatus.OK, builder))
                    } catch (e: Exception) {
                        channel.sendResponse(BytesRestResponse(channel, e))
                    }
                },
                { e: Exception ->
                    channel.sendResponse(BytesRestResponse(channel, e))
                }
            )
        )
    }

    private fun buildResponse(
        request: ClusterMetadataDiffRequest,
        localMetadata: Metadata,
        remoteMetadata: Metadata
    ): ClusterMetadataDiffResponse {
        val categories = mutableListOf<CategoryDiff>()
        for (provider in diffProviders.values) {
            if (provider.category in request.categories) {
                categories.add(provider.diff(localMetadata, remoteMetadata))
            }
        }

        return ClusterMetadataDiffResponse(
            connectionName = request.connectionName,
            remoteMetadataVersion = remoteMetadata.version(),
            localMetadataVersion = localMetadata.version(),
            categories = categories
        )
    }
}
