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

import org.opensearch.transport.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.persistent.AllocatedPersistentTask
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.opensearch.persistent.PersistentTasksExecutor
import org.opensearch.core.tasks.TaskId
import org.opensearch.threadpool.ThreadPool

class ClusterMetadataSyncExecutor(executor: String,
                                  private val clusterService: ClusterService,
                                  private val threadPool: ThreadPool,
                                  private val client: Client) :
    PersistentTasksExecutor<ClusterMetadataSyncParams>(TASK_NAME, executor) {

    companion object {
        const val TASK_NAME = "cluster:admin/plugins/replication/cluster_metadata_sync"
    }

    override fun nodeOperation(task: AllocatedPersistentTask, params: ClusterMetadataSyncParams, state: PersistentTaskState?) {
        if (task is ClusterMetadataSyncTask) {
            task.run()
        } else {
            task.markAsFailed(IllegalArgumentException("unknown task type : ${task::class.java}"))
        }
    }

    override fun createTask(id: Long, type: String, action: String, parentTaskId: TaskId,
                            taskInProgress: PersistentTask<ClusterMetadataSyncParams>,
                            headers: Map<String, String>): AllocatedPersistentTask {
        return ClusterMetadataSyncTask(id, type, action, getDescription(taskInProgress),
                parentTaskId, headers, executor, clusterService, threadPool, client,
                taskInProgress.params!!)
    }

    override fun getDescription(taskInProgress: PersistentTask<ClusterMetadataSyncParams>): String {
        return "cluster metadata sync task for leader: ${taskInProgress.params?.leaderCluster}"
    }
}
