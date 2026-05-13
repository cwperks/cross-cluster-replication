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

import org.opensearch.Version
import org.opensearch.core.ParseField
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ObjectParser
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.persistent.PersistentTaskParams
import java.io.IOException

class ClusterMetadataSyncParams : PersistentTaskParams {

    lateinit var leaderCluster: String

    companion object {
        const val NAME = ClusterMetadataSyncExecutor.TASK_NAME

        private val PARSER = ObjectParser<ClusterMetadataSyncParams, Void>(NAME, true) { ClusterMetadataSyncParams() }
        init {
            PARSER.declareString(ClusterMetadataSyncParams::leaderCluster::set, ParseField("leader_cluster"))
        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser): ClusterMetadataSyncParams {
            return PARSER.parse(parser, null)
        }
    }

    private constructor()

    constructor(leaderCluster: String) {
        this.leaderCluster = leaderCluster
    }

    constructor(inp: StreamInput) : this(inp.readString())

    override fun writeTo(out: StreamOutput) {
        out.writeString(leaderCluster)
    }

    override fun getWriteableName() = NAME

    override fun getMinimalSupportedVersion(): Version = Version.V_2_0_0

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("leader_cluster", leaderCluster)
            .endObject()
    }
}
