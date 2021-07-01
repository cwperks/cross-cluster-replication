/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.elasticsearch.replication.task.index

import com.amazon.elasticsearch.replication.ReplicationException
import com.amazon.elasticsearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import com.amazon.elasticsearch.replication.action.index.block.IndexBlockUpdateType
import com.amazon.elasticsearch.replication.action.index.block.UpdateIndexBlockRequest
import com.amazon.elasticsearch.replication.action.stop.StopIndexReplicationAction
import com.amazon.elasticsearch.replication.action.stop.StopIndexReplicationRequest
import com.amazon.elasticsearch.replication.metadata.state.getReplicationStateParamsForIndex
import com.amazon.elasticsearch.replication.repository.REMOTE_SNAPSHOT_NAME
import com.amazon.elasticsearch.replication.repository.RemoteClusterRepository
import com.amazon.elasticsearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import com.amazon.elasticsearch.replication.task.CrossClusterReplicationTask
import com.amazon.elasticsearch.replication.task.ReplicationState
import com.amazon.elasticsearch.replication.task.shard.ShardReplicationExecutor
import com.amazon.elasticsearch.replication.task.shard.ShardReplicationParams
import com.amazon.elasticsearch.replication.task.shard.ShardReplicationTask
import com.amazon.elasticsearch.replication.util.suspending
import com.amazon.elasticsearch.replication.util.waitForNextChange
import com.amazon.elasticsearch.replication.util.startTask
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import org.elasticsearch.ElasticsearchTimeoutException
import org.elasticsearch.ResourceNotFoundException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterChangedEvent
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.ClusterStateListener
import org.elasticsearch.cluster.ClusterStateObserver
import org.elasticsearch.cluster.RestoreInProgress
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.index.Index
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.persistent.PersistentTasksCustomMetadata
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.elasticsearch.persistent.PersistentTasksNodeService
import org.elasticsearch.persistent.PersistentTasksService
import org.elasticsearch.tasks.TaskId
import org.elasticsearch.threadpool.ThreadPool
import java.util.function.Predicate
import java.util.stream.Collectors
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import com.amazon.elasticsearch.replication.action.index.block.UpdateIndexBlockAction
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.metadata.ReplicationOverallState
import com.amazon.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import com.amazon.elasticsearch.replication.util.suspendExecute
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.IndexSettings
import org.elasticsearch.indices.recovery.RecoveryState
import kotlin.streams.toList

class IndexReplicationTask(id: Long, type: String, action: String, description: String,
                           parentTask: TaskId,
                           executor: String,
                           clusterService: ClusterService,
                           threadPool: ThreadPool,
                           client: Client,
                           params: IndexReplicationParams,
                           private val persistentTasksService: PersistentTasksService,
                           replicationMetadataManager: ReplicationMetadataManager)
    : CrossClusterReplicationTask(id, type, action, description, parentTask, emptyMap(), executor,
                                  clusterService, threadPool, client, replicationMetadataManager), ClusterStateListener {
    private lateinit var currentTaskState : IndexReplicationState
    private lateinit var followingTaskState : IndexReplicationState

    override val remoteCluster = params.remoteCluster

    private val remoteClient = client.getRemoteClusterClient(remoteCluster)
    val remoteIndex   = params.remoteIndex
    override val followerIndexName = params.followerIndexName

    override val log = Loggers.getLogger(javaClass, Index(params.followerIndexName, ClusterState.UNKNOWN_UUID))
    private val cso = ClusterStateObserver(clusterService, log, threadPool.threadContext)
    private val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)

    private val SLEEP_TIME_BETWEEN_POLL_MS = 5000L

    override fun indicesOrShards(): List<Any> = listOf(followerIndexName)

    override suspend fun execute(initialState: PersistentTaskState?) {
        checkNotNull(initialState) { "Missing initial state" }
        followingTaskState = FollowingState(emptyMap())
        currentTaskState = initialState as IndexReplicationState
        while (scope.isActive) {
            val newState = when (currentTaskState.state) {
                ReplicationState.INIT -> {
                    addListenerToInterruptTask()
                    if (isResumed()) {
                        log.debug("Resuming tasks now.")
                        InitFollowState
                    } else {
                        setupAndStartRestore()
                    }
                }
                ReplicationState.RESTORING -> {
                    waitForRestore()
                }
                ReplicationState.INIT_FOLLOW -> {
                    startShardFollowTasks(emptyMap())
                }
                ReplicationState.FOLLOWING -> {
                    if (currentTaskState is FollowingState) {
                        followingTaskState = (currentTaskState as FollowingState)
                        addIndexBlockForReplication()
                    } else {
                        throw ReplicationException("Wrong state type: ${currentTaskState::class}")
                    }
                }
                ReplicationState.MONITORING -> {
                    pollShardTaskStatus((followingTaskState as FollowingState).shardReplicationTasks)
                }
                ReplicationState.FAILED -> {
                    stopReplicationTasks()
                    currentTaskState
                }
                ReplicationState.COMPLETED -> {
                    markAsCompleted()
                    CompletedState
                }
            }
            if (newState != currentTaskState) {
                currentTaskState = updateState(newState)
            }
            if (isCompleted) break
        }
    }

    private fun addListenerToInterruptTask() {
        clusterService.addListener(this)
    }

    private suspend fun pollShardTaskStatus(shardTasks: Map<ShardId, PersistentTask<ShardReplicationParams>>): IndexReplicationState {
        val failedShardTasks = findFailedShardTasks(shardTasks, clusterService.state())
        if (failedShardTasks.isNotEmpty())
            return FailedState(failedShardTasks, "At least one of the shard replication task has failed")
        delay(SLEEP_TIME_BETWEEN_POLL_MS)
        return MonitoringState
    }

    private fun isResumed(): Boolean {
        return  clusterService.state().routingTable.hasIndex(followerIndexName)
    }

    private suspend fun stopReplicationTasks() {
        val stopReplicationResponse = client.suspendExecute(replicationMetadata,
                StopIndexReplicationAction.INSTANCE, StopIndexReplicationRequest(followerIndexName), defaultContext = true)
        if (!stopReplicationResponse.isAcknowledged)
            throw ReplicationException("Failed to gracefully stop replication after one or more shard tasks failed. " +
                    "Replication tasks may need to be stopped manually.")
    }

    private fun findFailedShardTasks(shardTasks: Map<ShardId, PersistentTask<ShardReplicationParams>>, clusterState: ClusterState)
        :Map<ShardId, PersistentTask<ShardReplicationParams>> {

        val persistentTasks = clusterState.metadata.custom<PersistentTasksCustomMetadata>(PersistentTasksCustomMetadata.TYPE)
        val runningShardTasks = persistentTasks.findTasks(ShardReplicationExecutor.TASK_NAME, Predicate { true }).stream()
                .map { task -> task.params as ShardReplicationParams }
                .collect(Collectors.toList())
        return shardTasks.filterKeys { shardId ->
            runningShardTasks.find { task -> task.followerShardId == shardId } == null}
    }

    override suspend fun cleanup() {
        if (currentTaskState.state == ReplicationState.RESTORING)  {
            log.info("Replication stopped before restore could finish, so removing partial restore..")
            cancelRestore()
        }
        /* This is to minimise overhead of calling an additional listener as
         * it continues to be called even after the task is completed.
         */
        clusterService.removeListener(this)
    }

    private suspend fun addIndexBlockForReplication(): IndexReplicationState {
        val request = UpdateIndexBlockRequest(followerIndexName, IndexBlockUpdateType.ADD_BLOCK)
        client.suspendExecute(replicationMetadata, UpdateIndexBlockAction.INSTANCE, request, defaultContext = true)
        return MonitoringState
    }

    private suspend fun updateState(newState: IndexReplicationState) : IndexReplicationState {
        return suspendCoroutine { cont ->
            updatePersistentTaskState(newState, object : ActionListener<PersistentTask<*>> {
                override fun onFailure(e: Exception) {
                    cont.resumeWithException(e)
                }

                override fun onResponse(response: PersistentTask<*>) {
                    cont.resume(response.state as IndexReplicationState)
                }
            })
        }
    }

    private suspend fun
            startShardFollowTasks(tasks: Map<ShardId, PersistentTask<ShardReplicationParams>>): FollowingState {
        assert(clusterService.state().routingTable.hasIndex(followerIndexName)) { "Can't find index $followerIndexName" }
        val shards = clusterService.state().routingTable.indicesRouting().get(followerIndexName).shards()
        val newTasks = shards.map {
            it.value.shardId
        }.associate { shardId ->
            val task = tasks.getOrElse(shardId) {
                startReplicationTask(ShardReplicationParams(remoteCluster, ShardId(remoteIndex, shardId.id), shardId))
            }
            return@associate shardId to task
        }
        return FollowingState(newTasks)
    }

    private suspend fun cancelRestore() {
        /*
         * Should be safe to delete the retention leases here for all the shards
         * as the restore is not yet completed
         */
        val shards = clusterService.state().routingTable.indicesRouting().get(followerIndexName)?.shards()
        shards?.forEach {
            val followerShardId = it.value.shardId
            retentionLeaseHelper.removeRetentionLease(ShardId(remoteIndex, followerShardId.id), followerShardId)
        }

        /* As given here
         * (https://www.elastic.co/guide/en/elasticsearch/reference/6.8/modules-snapshots.html#_stopping_currently_running_snapshot_and_restore_operations)
         * a snapshot restore can be cancelled by deleting the indices being restored.
         */
        log.info("Deleting the index $followerIndexName")
        client.suspending(client.admin().indices()::delete, defaultContext = true)(DeleteIndexRequest(followerIndexName))
    }

    private suspend fun setupAndStartRestore(): IndexReplicationState {
        // Enable translog based fetch on the leader(remote) cluster
        val remoteClient = client.getRemoteClusterClient(remoteCluster)
        val settingsBuilder = Settings.builder().put(IndexSettings.INDEX_TRANSLOG_RETENTION_LEASE_PRUNING_ENABLED_SETTING.key, true)
        val updateSettingsRequest = remoteClient.admin().indices().prepareUpdateSettings().setSettings(settingsBuilder).setIndices(remoteIndex.name).request()
        val updateResponse = remoteClient.suspending(remoteClient.admin().indices()::updateSettings, injectSecurityContext = true)(updateSettingsRequest)
        if(!updateResponse.isAcknowledged) {
            log.error("Unable to update setting for translog pruning based on retention lease")
        }

        val restoreRequest = client.admin().cluster()
            .prepareRestoreSnapshot(RemoteClusterRepository.repoForCluster(remoteCluster), REMOTE_SNAPSHOT_NAME)
            .setIndices(remoteIndex.name)
            .request()
        if (remoteIndex.name != followerIndexName) {
            restoreRequest.renamePattern(remoteIndex.name)
                .renameReplacement(followerIndexName)
        }

        val response = client.suspending(client.admin().cluster()::restoreSnapshot, defaultContext = true)(restoreRequest)
        if (response.restoreInfo != null) {
            if (response.restoreInfo.failedShards() != 0) {
                throw ReplicationException("Restore failed: $response")
            }
            return FollowingState(emptyMap())
        }
        cso.waitForNextChange("remote restore start") { inProgressRestore(it) != null }
        return RestoreState
    }

    private suspend fun waitForRestore(): IndexReplicationState {
        var restore = inProgressRestore(clusterService.state())

        // Waiting for snapshot restore to reach a terminal stage.
        while (restore != null && restore.state() != RestoreInProgress.State.FAILURE && restore.state() != RestoreInProgress.State.SUCCESS) {
            try {
                cso.waitForNextChange("remote restore finish")
            } catch(e: ElasticsearchTimeoutException) {
                log.info("Timed out while waiting for restore to complete.")
            }
            restore = inProgressRestore(clusterService.state())
        }

        if (restore == null) {
            /**
             * At this point, we've already verified (during startRestore) that RestoreInProgress entry was
             * added in cluster state. Now if the entry is not present, we validate that no primary shard
             * is in recovery. Post validation we assume that restore is already completed and entry has been
             * cleaned up from cluster state.
             *
             * This we've observed when we trigger the replication on multiple small indices(also autofollow) simultaneously.
             */
            if (doesValidIndexExists()) {
                return InitFollowState
            } else {
                throw ResourceNotFoundException("""
                    Unable to find in progress restore for remote index: $remoteCluster:$remoteIndex. 
                    This can happen if there was a badly timed master node failure.""".trimIndent())
            }
        } else if (restore?.state() == RestoreInProgress.State.FAILURE) {
            val failureReason = restore.shards().values().find {
                it.value.state() == RestoreInProgress.State.FAILURE
            }!!.value.reason()
            throw ReplicationException("Remote restore failed: $failureReason")
        } else {
            return InitFollowState
        }
    }

    /**
     * In case the snapshot entry is not present in the cluster state, we assume that the task has
     * been successful and has been cleaned up from cluster state. With this method, we do basic
     * validation for the index before we allow the index replication to move to following state.
     * The validation done are:
     * 1. The index still exists and has been created using replication
     *    workflow i.e. index settings contains 'index.opendistro.replicated'
     * 2. There shouldn't be any primary shard in active recovery.
     */
    private fun doesValidIndexExists(): Boolean {
        try {
            client.admin().indices().prepareGetSettings(followerIndexName).get()
                    .getSetting(followerIndexName, REPLICATED_INDEX_SETTING.key) ?: return false

            val recoveries = client.admin().indices().prepareRecoveries(followerIndexName).get()
                .shardRecoveryStates().get(followerIndexName)
            val activeRecoveries = recoveries?.stream()?.filter(RecoveryState::getPrimary)?.filter {
                    r -> r.stage != RecoveryState.Stage.DONE }?.toList()
            return activeRecoveries?.size == 0
        } catch (e: Exception) {
            log.error("Error trying to validate the index. ${e.message}")
            return false
        }
    }
    private fun inProgressRestore(cs: ClusterState): RestoreInProgress.Entry? {
        return cs.custom<RestoreInProgress>(RestoreInProgress.TYPE).singleOrNull { entry ->
            entry.snapshot().repository == RemoteClusterRepository.repoForCluster(remoteCluster) &&
                entry.indices().singleOrNull { idx -> idx == followerIndexName } != null
        }
    }

    private suspend
    fun startReplicationTask(replicationParams : ShardReplicationParams) : PersistentTask<ShardReplicationParams> {
        return persistentTasksService.startTask(ShardReplicationTask.taskIdForShard(replicationParams.followerShardId),
            ShardReplicationExecutor.TASK_NAME, replicationParams)
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        log.debug("Cluster metadata listener invoked on index task...")
        if (event.metadataChanged()) {
            val replicationStateParams = getReplicationStateParamsForIndex(clusterService, followerIndexName)
            if (replicationStateParams == null) {
                if (PersistentTasksNodeService.Status(State.STARTED) == status)
                    scope.cancel("Index replication task received an interrupt.")
            } else if (replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE] == ReplicationOverallState.PAUSED.name){
                log.info("Pause state received for index $followerIndexName task")
                scope.cancel("Index replication task received a pause.")
            }
        }
    }

    override fun replicationTaskResponse(): CrossClusterReplicationTaskResponse {
        return IndexReplicationTaskResponse(currentTaskState)
    }

    class IndexReplicationTaskResponse(private val taskState : IndexReplicationState):
            CrossClusterReplicationTaskResponse(ReplicationState.COMPLETED.name), ToXContentObject {

        override fun writeTo(out: StreamOutput) {
            super.writeTo(out)
            taskState.writeTo(out)
        }

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            var responseBuilder = builder.startObject()
                    .field("index_task_status", ReplicationState.COMPLETED.name)
                    .field("following_tasks")
            return taskState.toXContent(responseBuilder, params).endObject()
        }
    }
}
