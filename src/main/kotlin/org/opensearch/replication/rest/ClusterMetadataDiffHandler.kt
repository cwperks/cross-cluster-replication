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

import org.opensearch.replication.action.diff.ClusterMetadataDiffAction
import org.opensearch.replication.action.diff.ClusterMetadataDiffRequest
import org.opensearch.transport.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class ClusterMetadataDiffHandler : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(RestRequest.Method.GET, "/_plugins/_replication/_cluster/{connectionName}/_diff")
        )
    }

    override fun getName(): String = "plugins_replication_cluster_diff"

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
        return RestChannelConsumer { channel ->
            client.execute(ClusterMetadataDiffAction.INSTANCE, diffRequest, RestToXContentListener(channel))
        }
    }
}
