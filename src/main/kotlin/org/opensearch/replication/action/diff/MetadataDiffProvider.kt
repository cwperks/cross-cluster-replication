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

import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.ingest.IngestMetadata

internal interface MetadataDiffProvider {
    val category: String

    fun diff(local: Metadata, remote: Metadata): CategoryDiff
}

internal class LegacyTemplateDiffProvider : MetadataDiffProvider {
    override val category = "templates"

    override fun diff(local: Metadata, remote: Metadata): CategoryDiff {
        return diffNamedItems(category, local.templates().keys.toSet(), remote.templates().keys.toSet()) { name ->
            local.templates()[name] == remote.templates()[name]
        }
    }
}

internal class IngestPipelineDiffProvider : MetadataDiffProvider {
    override val category = "ingest_pipelines"

    override fun diff(local: Metadata, remote: Metadata): CategoryDiff {
        val localPipelines = local.custom<IngestMetadata>("ingest")
        val remotePipelines = remote.custom<IngestMetadata>("ingest")
        return diffNamedItems(category, localPipelines?.pipelines?.keys.orEmpty(), remotePipelines?.pipelines?.keys.orEmpty()) { name ->
            localPipelines?.pipelines?.get(name)?.configAsMap == remotePipelines?.pipelines?.get(name)?.configAsMap
        }
    }
}

internal class IndexMetadataDiffProvider : MetadataDiffProvider {
    override val category = "indices"

    override fun diff(local: Metadata, remote: Metadata): CategoryDiff {
        val localIndices = local.indices().keys.filter { isReplicable(local.index(it)) }.toSet()
        val remoteIndices = remote.indices().keys.filter { isReplicable(remote.index(it)) }.toSet()

        val remoteOnly = (remoteIndices - localIndices).sorted()
        val localOnly = (localIndices - remoteIndices).sorted()
        val diverged = mutableListOf<DivergedItem>()
        var inSync = 0

        for (name in localIndices.intersect(remoteIndices).sorted()) {
            val fields = diffIndexMetadata(local.index(name), remote.index(name))
            if (fields.isEmpty()) {
                inSync++
            } else {
                diverged.add(DivergedItem(name, fields))
            }
        }

        return CategoryDiff(category, "compared", inSync, remoteOnly, localOnly, diverged)
    }

    private fun diffIndexMetadata(local: IndexMetadata, remote: IndexMetadata): List<DiffField> {
        val fields = mutableListOf<DiffField>()

        val localMapping = local.mapping()?.source()?.string()
        val remoteMapping = remote.mapping()?.source()?.string()
        if (localMapping != remoteMapping) {
            fields.add(DiffField("mappings", "[differs]", "[differs]", "included"))
        }

        val localSettings = local.settings
        val remoteSettings = remote.settings
        val allKeys = (localSettings.keySet() + remoteSettings.keySet())
            .filter { key -> !IndexMetadataDiffPolicy.isStrippedSetting(key) }
            .sorted()

        for (key in allKeys) {
            val localVal = localSettings.get(key)
            val remoteVal = remoteSettings.get(key)
            if (localVal != remoteVal) {
                fields.add(DiffField("settings.$key", localVal, remoteVal, IndexMetadataDiffPolicy.policyForSetting(key)))
            }
        }

        val localAliases = local.aliases.keys.toSet()
        val remoteAliases = remote.aliases.keys.toSet()
        for (alias in (remoteAliases - localAliases).sorted()) {
            fields.add(DiffField("aliases.$alias", null, "present", "included"))
        }
        for (alias in (localAliases - remoteAliases).sorted()) {
            fields.add(DiffField("aliases.$alias", "present", null, "included"))
        }
        for (alias in localAliases.intersect(remoteAliases).sorted()) {
            val localAlias = local.aliases[alias]
            val remoteAlias = remote.aliases[alias]
            if (localAlias != remoteAlias) {
                fields.add(DiffField("aliases.$alias", localAlias.toString(), remoteAlias.toString(), "included"))
            }
        }

        return fields
    }

    private fun isReplicable(indexMetadata: IndexMetadata): Boolean {
        val name = indexMetadata.index.name
        return !name.startsWith(".") && !indexMetadata.isSystem && !indexMetadata.settings.getAsBoolean("index.hidden", false)
    }
}

internal object IndexMetadataDiffPolicy {
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

    fun isStrippedSetting(key: String): Boolean {
        if (key in STRIPPED_SETTINGS) return true
        return STRIPPED_SETTING_PREFIXES.any { key.startsWith(it) }
    }

    fun policyForSetting(key: String): String = if (key in CONDITIONAL_SETTINGS) "conditional" else "included"
}

private fun diffNamedItems(
    category: String,
    localNames: Set<String>,
    remoteNames: Set<String>,
    isEqual: (String) -> Boolean
): CategoryDiff {
    val remoteOnly = (remoteNames - localNames).sorted()
    val localOnly = (localNames - remoteNames).sorted()
    val diverged = mutableListOf<DivergedItem>()
    var inSync = 0

    for (name in localNames.intersect(remoteNames).sorted()) {
        if (isEqual(name)) {
            inSync++
        } else {
            diverged.add(DivergedItem(name, listOf(DiffField("content", "[differs]", "[differs]", "included"))))
        }
    }

    return CategoryDiff(category, "compared", inSync, remoteOnly, localOnly, diverged)
}
