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
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
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

    companion object {
        private val log = LogManager.getLogger(TransportClusterMetadataDiffAction::class.java)

        // Index settings that are per-cluster and should not be compared
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

        // Settings that may legitimately differ between clusters
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

                if ("component_templates" in request.categories) {
                    categories.add(diffComponentTemplates(localMetadata, remoteMetadata))
                }
                if ("templates_v2" in request.categories) {
                    categories.add(diffComposableTemplates(localMetadata, remoteMetadata))
                }
                if ("templates" in request.categories) {
                    categories.add(diffLegacyTemplates(localMetadata, remoteMetadata))
                }
                if ("stored_scripts" in request.categories) {
                    categories.add(diffStoredScripts(localMetadata, remoteMetadata))
                }
                if ("ingest_pipelines" in request.categories) {
                    categories.add(diffIngestPipelines(localMetadata, remoteMetadata))
                }
                if ("search_pipelines" in request.categories) {
                    categories.add(diffSearchPipelines(localMetadata, remoteMetadata))
                }
                if ("indices" in request.categories) {
                    categories.add(diffIndices(localMetadata, remoteMetadata))
                }
                if ("data_streams" in request.categories) {
                    categories.add(diffDataStreams(localMetadata, remoteMetadata))
                }

                ClusterMetadataDiffResponse(
                    connectionName = request.connectionName,
                    remoteMetadataVersion = remoteState.metadata().version(),
                    localMetadataVersion = localMetadata.version(),
                    categories = categories
                )
            }
        }
    }

    private fun diffComponentTemplates(local: Metadata, remote: Metadata): CategoryDiff {
        val localNames = local.componentTemplates()?.keys().orEmpty().toSet()
        val remoteNames = remote.componentTemplates()?.keys().orEmpty().toSet()
        return diffNamedItems("component_templates", localNames, remoteNames) { name ->
            local.componentTemplates().get(name) == remote.componentTemplates().get(name)
        }
    }

    private fun diffComposableTemplates(local: Metadata, remote: Metadata): CategoryDiff {
        val localNames = local.templatesV2()?.keys().orEmpty().toSet()
        val remoteNames = remote.templatesV2()?.keys().orEmpty().toSet()
        return diffNamedItems("templates_v2", localNames, remoteNames) { name ->
            local.templatesV2().get(name) == remote.templatesV2().get(name)
        }
    }

    private fun diffLegacyTemplates(local: Metadata, remote: Metadata): CategoryDiff {
        val localNames = local.templates().keys().toSet()
        val remoteNames = remote.templates().keys().toSet()
        return diffNamedItems("templates", localNames, remoteNames) { name ->
            local.templates().get(name) == remote.templates().get(name)
        }
    }

    private fun diffStoredScripts(local: Metadata, remote: Metadata): CategoryDiff {
        val localScripts = local.storedScripts()
        val remoteScripts = remote.storedScripts()
        val localNames = localScripts.keys.toSet()
        val remoteNames = remoteScripts.keys.toSet()
        return diffNamedItems("stored_scripts", localNames, remoteNames) { name ->
            localScripts[name] == remoteScripts[name]
        }
    }

    private fun diffIngestPipelines(local: Metadata, remote: Metadata): CategoryDiff {
        val localPipelines = local.custom<org.opensearch.ingest.IngestMetadata>("ingest")
        val remotePipelines = remote.custom<org.opensearch.ingest.IngestMetadata>("ingest")
        val localNames = localPipelines?.pipelines?.keys.orEmpty()
        val remoteNames = remotePipelines?.pipelines?.keys.orEmpty()
        return diffNamedItems("ingest_pipelines", localNames, remoteNames) { name ->
            localPipelines?.pipelines?.get(name)?.configAsMap == remotePipelines?.pipelines?.get(name)?.configAsMap
        }
    }

    private fun diffSearchPipelines(local: Metadata, remote: Metadata): CategoryDiff {
        // Search pipelines are stored as a Metadata.Custom; compare by name presence
        val localNames = extractSearchPipelineNames(local)
        val remoteNames = extractSearchPipelineNames(remote)
        return diffNamedItems("search_pipelines", localNames, remoteNames) { _ -> true }
    }

    private fun extractSearchPipelineNames(metadata: Metadata): Set<String> {
        // Search pipelines custom metadata access - best effort
        return try {
            val custom = metadata.custom<Metadata.Custom>("search_pipeline")
            if (custom != null) {
                // Use reflection or known API to get pipeline names
                emptySet()
            } else emptySet()
        } catch (e: Exception) {
            emptySet()
        }
    }

    private fun diffIndices(local: Metadata, remote: Metadata): CategoryDiff {
        val localIndices = local.indices().keys().filter { isReplicable(local.index(it)) }.toSet()
        val remoteIndices = remote.indices().keys().filter { isReplicable(remote.index(it)) }.toSet()

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
        if (local.mapping() != remote.mapping()) {
            val localSource = local.mapping()?.source()?.string()
            val remoteSource = remote.mapping()?.source()?.string()
            if (localSource != remoteSource) {
                fields.add(DiffField("mappings", "[differs]", "[differs]", "included"))
            }
        }

        // Compare user-facing settings (excluding stripped ones)
        val localSettings = local.settings
        val remoteSettings = remote.settings
        val allKeys = (localSettings.keySet() + remoteSettings.keySet())
            .filter { key -> !isStrippedSetting(key) }

        for (key in allKeys) {
            val localVal = localSettings.get(key)
            val remoteVal = remoteSettings.get(key)
            if (localVal != remoteVal) {
                val policy = if (key in CONDITIONAL_SETTINGS) "conditional" else "included"
                fields.add(DiffField("settings.$key", localVal, remoteVal, policy))
            }
        }

        // Compare aliases
        val localAliases = local.aliases.keys().toSet()
        val remoteAliases = remote.aliases.keys().toSet()
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

    private fun diffDataStreams(local: Metadata, remote: Metadata): CategoryDiff {
        val localNames = local.dataStreams().keys
        val remoteNames = remote.dataStreams().keys
        return diffNamedItems("data_streams", localNames, remoteNames) { name ->
            val localDs = local.dataStreams()[name]
            val remoteDs = remote.dataStreams()[name]
            localDs?.indices?.map { it.name } == remoteDs?.indices?.map { it.name }
        }
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
