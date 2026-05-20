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
import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.transport.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.ingest.IngestMetadata
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

    companion object {
        private val log = LogManager.getLogger(TransportClusterMetadataDiffAction::class.java)

        private val STRIPPED_SETTINGS = setOf(
            "index.uuid",
            "index.version.created",
            "index.version.upgraded",
            "index.creation_date",
            "index.provided_name",
            "index.resize.source.uuid",
            "index.resize.source.name"
        )

        private val STRIPPED_SETTING_PREFIXES = listOf(
            "index.routing.allocation."
        )

        private val CONDITIONAL_SETTINGS = setOf(
            "index.number_of_replicas"
        )
    }

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

                if ("templates" in request.categories) {
                    categories.add(diffLegacyTemplates(localMetadata, remoteMetadata))
                }
                if ("ingest_pipelines" in request.categories) {
                    categories.add(diffIngestPipelines(localMetadata, remoteMetadata))
                }
                if ("indices" in request.categories) {
                    categories.add(diffIndices(localMetadata, remoteMetadata))
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

    private fun diffLegacyTemplates(local: Metadata, remote: Metadata): CategoryDiff {
        val localNames = local.templates().keys.toMutableSet() as Set<String>
        val remoteNames = remote.templates().keys.toMutableSet() as Set<String>
        return diffNamedItems("templates", localNames, remoteNames) { name ->
            local.templates().get(name) == remote.templates().get(name)
        }
    }

    private fun diffIngestPipelines(local: Metadata, remote: Metadata): CategoryDiff {
        val localPipelines = local.custom<IngestMetadata>("ingest")
        val remotePipelines = remote.custom<IngestMetadata>("ingest")
        val localNames = localPipelines?.pipelines?.keys.orEmpty()
        val remoteNames = remotePipelines?.pipelines?.keys.orEmpty()
        return diffNamedItems("ingest_pipelines", localNames, remoteNames) { name ->
            localPipelines?.pipelines?.get(name)?.configAsMap == remotePipelines?.pipelines?.get(name)?.configAsMap
        }
    }

    private fun diffIndices(local: Metadata, remote: Metadata): CategoryDiff {
        val allLocalNames = local.indices().keys.toMutableSet() as Set<String>
        val allRemoteNames = remote.indices().keys.toMutableSet() as Set<String>
        val localIndices = allLocalNames.filter { isReplicable(local.index(it)) }.toSet()
        val remoteIndices = allRemoteNames.filter { isReplicable(remote.index(it)) }.toSet()

        val remoteOnly = (remoteIndices - localIndices).toList()
        val localOnly = (localIndices - remoteIndices).toList()
        val common = localIndices.intersect(remoteIndices)

        val diverged = mutableListOf<DivergedItem>()
        var inSync = 0

        for (name in common) {
            val fields = diffIndexMetadata(local.index(name), remote.index(name))
            if (fields.isEmpty()) {
                inSync++
            } else {
                diverged.add(DivergedItem(name, fields))
            }
        }

        return CategoryDiff("indices", inSync, remoteOnly, localOnly, diverged)
    }

    private fun diffIndexMetadata(local: IndexMetadata, remote: IndexMetadata): List<DiffField> {
        val fields = mutableListOf<DiffField>()

        // Compare mappings
        val localMapping = local.mapping()?.source()?.string()
        val remoteMapping = remote.mapping()?.source()?.string()
        if (localMapping != remoteMapping) {
            fields.add(DiffField("mappings", "[differs]", "[differs]", "included"))
        }

        // Compare user-facing settings (excluding stripped ones)
        val localSettings = local.settings
        val remoteSettings = remote.settings
        val allKeys = (localSettings.keySet() + remoteSettings.keySet())
            .filter { key -> !isStrippedSetting(key) }
            .toSet()

        for (key in allKeys) {
            val localVal = localSettings.get(key)
            val remoteVal = remoteSettings.get(key)
            if (localVal != remoteVal) {
                val policy = if (key in CONDITIONAL_SETTINGS) "conditional" else "included"
                fields.add(DiffField("settings.$key", localVal, remoteVal, policy))
            }
        }

        // Compare aliases
        val localAliases = local.aliases.keys.toMutableSet() as Set<String>
        val remoteAliases = remote.aliases.keys.toMutableSet() as Set<String>
        for (alias in remoteAliases - localAliases) {
            fields.add(DiffField("aliases.$alias", null, "present", "included"))
        }
        for (alias in localAliases - remoteAliases) {
            fields.add(DiffField("aliases.$alias", "present", null, "included"))
        }
        for (alias in localAliases.intersect(remoteAliases)) {
            val localAlias = local.aliases.get(alias)
            val remoteAlias = remote.aliases.get(alias)
            if (localAlias != remoteAlias) {
                fields.add(DiffField("aliases.$alias", localAlias.toString(), remoteAlias.toString(), "included"))
            }
        }

        return fields
    }

    private fun diffNamedItems(
        category: String,
        localNames: Set<String>,
        remoteNames: Set<String>,
        isEqual: (String) -> Boolean
    ): CategoryDiff {
        val remoteOnly = (remoteNames - localNames).toList()
        val localOnly = (localNames - remoteNames).toList()
        val common = localNames.intersect(remoteNames)

        val diverged = mutableListOf<DivergedItem>()
        var inSync = 0

        for (name in common) {
            if (isEqual(name)) {
                inSync++
            } else {
                diverged.add(DivergedItem(name, listOf(DiffField("content", "[differs]", "[differs]", "included"))))
            }
        }

        return CategoryDiff(category, inSync, remoteOnly, localOnly, diverged)
    }

    private fun isReplicable(indexMetadata: IndexMetadata): Boolean {
        val name = indexMetadata.index.name
        return !name.startsWith(".") && !indexMetadata.isSystem && !indexMetadata.settings.getAsBoolean("index.hidden", false)
    }

    private fun isStrippedSetting(key: String): Boolean {
        if (key in STRIPPED_SETTINGS) return true
        return STRIPPED_SETTING_PREFIXES.any { key.startsWith(it) }
    }
}
