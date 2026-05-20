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

import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder

data class DiffField(val path: String, val local: String?, val remote: String?, val policy: String)

data class DivergedItem(val name: String, val fields: List<DiffField>)

data class CategoryDiff(
    val category: String,
    val inSync: Int,
    val remoteOnly: List<String>,
    val localOnly: List<String>,
    val diverged: List<DivergedItem>
)

class ClusterMetadataDiffResponse : ActionResponse, ToXContentObject {

    val connectionName: String
    val remoteMetadataVersion: Long
    val localMetadataVersion: Long
    val categories: List<CategoryDiff>

    constructor(
        connectionName: String,
        remoteMetadataVersion: Long,
        localMetadataVersion: Long,
        categories: List<CategoryDiff>
    ) : super() {
        this.connectionName = connectionName
        this.remoteMetadataVersion = remoteMetadataVersion
        this.localMetadataVersion = localMetadataVersion
        this.categories = categories
    }

    constructor(inp: StreamInput) : super(inp) {
        this.connectionName = inp.readString()
        this.remoteMetadataVersion = inp.readVLong()
        this.localMetadataVersion = inp.readVLong()
        val catCount = inp.readVInt()
        val cats = mutableListOf<CategoryDiff>()
        repeat(catCount) {
            val category = inp.readString()
            val inSync = inp.readVInt()
            val remoteOnly = inp.readStringList()
            val localOnly = inp.readStringList()
            val divCount = inp.readVInt()
            val diverged = mutableListOf<DivergedItem>()
            repeat(divCount) {
                val name = inp.readString()
                val fieldCount = inp.readVInt()
                val fields = mutableListOf<DiffField>()
                repeat(fieldCount) {
                    fields.add(DiffField(inp.readString(), inp.readOptionalString(), inp.readOptionalString(), inp.readString()))
                }
                diverged.add(DivergedItem(name, fields))
            }
            cats.add(CategoryDiff(category, inSync, remoteOnly, localOnly, diverged))
        }
        this.categories = cats
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(connectionName)
        out.writeVLong(remoteMetadataVersion)
        out.writeVLong(localMetadataVersion)
        out.writeVInt(categories.size)
        for (cat in categories) {
            out.writeString(cat.category)
            out.writeVInt(cat.inSync)
            out.writeStringCollection(cat.remoteOnly)
            out.writeStringCollection(cat.localOnly)
            out.writeVInt(cat.diverged.size)
            for (item in cat.diverged) {
                out.writeString(item.name)
                out.writeVInt(item.fields.size)
                for (field in item.fields) {
                    out.writeString(field.path)
                    out.writeOptionalString(field.local)
                    out.writeOptionalString(field.remote)
                    out.writeString(field.policy)
                }
            }
        }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("connection_name", connectionName)
        builder.field("remote_metadata_version", remoteMetadataVersion)
        builder.field("local_metadata_version", localMetadataVersion)

        builder.startObject("categories")
        for (cat in categories) {
            builder.startObject(cat.category)
            builder.field("in_sync", cat.inSync)
            builder.field("remote_only", cat.remoteOnly.size)
            builder.field("local_only", cat.localOnly.size)
            builder.field("diverged", cat.diverged.size)

            if (cat.remoteOnly.isNotEmpty() || cat.localOnly.isNotEmpty() || cat.diverged.isNotEmpty()) {
                builder.startObject("items")
                if (cat.remoteOnly.isNotEmpty()) {
                    builder.field("remote_only", cat.remoteOnly)
                }
                if (cat.localOnly.isNotEmpty()) {
                    builder.field("local_only", cat.localOnly)
                }
                if (cat.diverged.isNotEmpty()) {
                    builder.startArray("diverged")
                    for (item in cat.diverged) {
                        builder.startObject()
                        builder.field("name", item.name)
                        builder.startArray("fields")
                        for (field in item.fields) {
                            builder.startObject()
                            builder.field("path", field.path)
                            builder.field("local", field.local)
                            builder.field("remote", field.remote)
                            builder.field("policy", field.policy)
                            builder.endObject()
                        }
                        builder.endArray()
                        builder.endObject()
                    }
                    builder.endArray()
                }
                builder.endObject()
            }
            builder.endObject()
        }
        builder.endObject()

        builder.endObject()
        return builder
    }
}
