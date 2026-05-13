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

package org.opensearch.replication.task.clustermetadata

import kotlinx.coroutines.*
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.cluster.metadata.ComposableIndexTemplate
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.tasks.TaskId
import org.opensearch.persistent.AllocatedPersistentTask
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.replication.util.suspending
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.util.coroutineContext
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client

class ClusterMetadataSyncTask(id: Long, type: String, action: String, description: String,
                              parentTask: TaskId, headers: Map<String, String>,
                              private val executor: String,
                              private val clusterService: ClusterService,
                              private val threadPool: ThreadPool,
                              private val client: Client,
                              private val params: ClusterMetadataSyncParams) :
    AllocatedPersistentTask(id, type, action, description, parentTask, headers) {

    companion object {
        private val log = LogManager.getLogger(ClusterMetadataSyncTask::class.java)
        val POLL_INTERVAL: TimeValue = TimeValue.timeValueSeconds(30)
    }

    private val leaderAlias = params.leaderCluster
    @Volatile private var running = true

    fun run() {
        val scope = CoroutineScope(threadPool.coroutineContext(executor))
        scope.launch {
            try {
                execute()
            } catch (e: Exception) {
                log.error("Cluster metadata sync task failed for leader: $leaderAlias", e)
                markAsFailed(e)
            }
        }
    }

    private suspend fun execute() {
        log.info("Cluster metadata sync task started for leader: $leaderAlias")
        while (running && !isCancelled) {
            try {
                syncIndexTemplates()
            } catch (e: OpenSearchException) {
                log.warn("Error syncing cluster metadata from $leaderAlias: ${e.message}")
            } catch (e: Exception) {
                log.error("Unexpected error in cluster metadata sync for $leaderAlias", e)
            }
            delay(POLL_INTERVAL.millis)
        }
    }

    private suspend fun syncIndexTemplates() {
        val remoteClient = client.getRemoteClusterClient(leaderAlias)

        // Get leader cluster state metadata (templates)
        val clusterStateRequest = ClusterStateRequest().clear().metadata(true)
        val leaderState = remoteClient.suspending(remoteClient.admin().cluster()::state, injectSecurityContext = true)(clusterStateRequest)
        val leaderTemplates: Map<String, ComposableIndexTemplate> = leaderState.state.metadata().templatesV2()

        // Get follower templates
        val followerTemplates: Map<String, ComposableIndexTemplate> = clusterService.state().metadata().templatesV2()

        // Sync templates that are new or different on leader
        var updated = false
        for ((name, leaderTemplate) in leaderTemplates) {
            val followerTemplate = followerTemplates[name]
            if (followerTemplate == null || followerTemplate != leaderTemplate) {
                log.info("Syncing index template [$name] from leader [$leaderAlias]")
                updated = true
            }
        }

        if (updated) {
            clusterService.submitStateUpdateTask("cluster_metadata_sync",
                object : org.opensearch.cluster.ClusterStateUpdateTask() {
                    override fun execute(currentState: org.opensearch.cluster.ClusterState): org.opensearch.cluster.ClusterState {
                        val metadataBuilder = org.opensearch.cluster.metadata.Metadata.builder(currentState.metadata())
                        for ((name, leaderTemplate) in leaderTemplates) {
                            val followerTemplate = currentState.metadata().templatesV2()[name]
                            if (followerTemplate == null || followerTemplate != leaderTemplate) {
                                metadataBuilder.put(name, leaderTemplate)
                            }
                        }
                        return org.opensearch.cluster.ClusterState.builder(currentState)
                            .metadata(metadataBuilder).build()
                    }

                    override fun onFailure(source: String, e: Exception) {
                        log.warn("Failed to sync templates from leader [$leaderAlias]: ${e.message}")
                    }
                })
        }
    }

    override fun onCancelled() {
        running = false
    }
}
