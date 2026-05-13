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

import org.opensearch.core.action.ActionListener
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.persistent.PersistentTaskResponse
import org.opensearch.persistent.RemovePersistentTaskAction
import org.opensearch.persistent.StartPersistentTaskAction
import org.opensearch.replication.task.clustermetadata.ClusterMetadataSyncExecutor
import org.opensearch.replication.task.clustermetadata.ClusterMetadataSyncParams
import org.opensearch.transport.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.core.rest.RestStatus

class ClusterMetadataSyncHandler : BaseRestHandler() {

    companion object {
        const val PATH = "/_plugins/_replication/_cluster_metadata_sync"
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(RestRequest.Method.POST, PATH),
            RestHandler.Route(RestRequest.Method.DELETE, PATH)
        )
    }

    override fun getName() = "plugins_replication_cluster_metadata_sync"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val leaderAlias = parseLeaderAlias(request.contentParser())
        val taskId = "cluster_metadata_sync:$leaderAlias"

        return when (request.method()) {
            RestRequest.Method.POST -> {
                val params = ClusterMetadataSyncParams(leaderAlias)
                val startRequest = StartPersistentTaskAction.Request(taskId, ClusterMetadataSyncExecutor.TASK_NAME, params)
                RestChannelConsumer { channel ->
                    client.execute(StartPersistentTaskAction.INSTANCE, startRequest, object : ActionListener<PersistentTaskResponse> {
                        override fun onResponse(response: PersistentTaskResponse) {
                            channel.sendResponse(BytesRestResponse(RestStatus.OK, """{"acknowledged":true}"""))
                        }
                        override fun onFailure(e: Exception) {
                            channel.sendResponse(BytesRestResponse(channel, e))
                        }
                    })
                }
            }
            RestRequest.Method.DELETE -> {
                val removeRequest = RemovePersistentTaskAction.Request(taskId)
                RestChannelConsumer { channel ->
                    client.execute(RemovePersistentTaskAction.INSTANCE, removeRequest, object : ActionListener<PersistentTaskResponse> {
                        override fun onResponse(response: PersistentTaskResponse) {
                            channel.sendResponse(BytesRestResponse(RestStatus.OK, """{"acknowledged":true}"""))
                        }
                        override fun onFailure(e: Exception) {
                            channel.sendResponse(BytesRestResponse(channel, e))
                        }
                    })
                }
            }
            else -> throw IllegalArgumentException("Unsupported method: ${request.method()}")
        }
    }

    private fun parseLeaderAlias(parser: XContentParser): String {
        var leaderAlias = ""
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser)
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            when (parser.currentName()) {
                "leader_alias" -> { parser.nextToken(); leaderAlias = parser.text() }
                else -> parser.skipChildren()
            }
        }
        require(leaderAlias.isNotEmpty()) { "leader_alias is required" }
        return leaderAlias
    }
}
