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

package org.opensearch.replication.action.diff

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.transport.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.suspending
import org.opensearch.tasks.Task
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService

class TransportClusterMetadataDiffAction @Inject constructor(
    transportService: TransportService,
    val threadPool: ThreadPool,
    actionFilters: ActionFilters,
    private val client: Client,
    private val clusterService: ClusterService
) : HandledTransportAction<ClusterMetadataDiffRequest, ClusterMetadataDiffResponse>(
    ClusterMetadataDiffAction.NAME, transportService, actionFilters, ::ClusterMetadataDiffRequest
), CoroutineScope by GlobalScope {

    private val diffProviders = listOf(
        LegacyTemplateDiffProvider(),
        IngestPipelineDiffProvider(),
        IndexMetadataDiffProvider()
    ).associateBy { it.category }

    override fun doExecute(task: Task, request: ClusterMetadataDiffRequest, listener: ActionListener<ClusterMetadataDiffResponse>) {
        launch(threadPool.coroutineContext()) {
            listener.completeWith {
                val remoteClient = client.getRemoteClusterClient(request.connectionName)
                val clusterStateRequest = remoteClient.admin().cluster().prepareState()
                    .clear()
                    .setMetadata(true)
                    .setCustoms(true)
                    .request()
                val remoteState = remoteClient.suspending(
                    remoteClient.admin().cluster()::state
                )(clusterStateRequest).state

                val localMetadata = clusterService.state().metadata()
                val remoteMetadata = remoteState.metadata()

                val categories = mutableListOf<CategoryDiff>()

                for (provider in diffProviders.values) {
                    if (provider.category in request.categories) {
                        categories.add(provider.diff(localMetadata, remoteMetadata))
                    }
                }

                ClusterMetadataDiffResponse(
                    connectionName = request.connectionName,
                    remoteMetadataVersion = remoteMetadata.version(),
                    localMetadataVersion = localMetadata.version(),
                    categories = categories
                )
            }
        }
    }
}
