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

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput

class ClusterMetadataDiffRequest : ActionRequest {

    val connectionName: String
    val categories: Set<String>

    companion object {
        val SUPPORTED_CATEGORIES = setOf("templates", "ingest_pipelines", "indices")
        val ALL_CATEGORIES = SUPPORTED_CATEGORIES
    }

    constructor(connectionName: String, categories: Set<String> = ALL_CATEGORIES) : super() {
        this.connectionName = connectionName
        this.categories = categories
    }

    constructor(inp: StreamInput) : super(inp) {
        this.connectionName = inp.readString()
        this.categories = inp.readSet(StreamInput::readString)
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(connectionName)
        out.writeStringCollection(categories)
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (connectionName.isBlank()) {
            validationException = ValidateActions.addValidationError("connection_name must not be empty", validationException)
        }
        val unsupportedCategories = categories - SUPPORTED_CATEGORIES
        if (unsupportedCategories.isNotEmpty()) {
            validationException = ValidateActions.addValidationError(
                "unsupported categories [${unsupportedCategories.sorted().joinToString(",")}], supported categories are [${SUPPORTED_CATEGORIES.sorted().joinToString(",")}]",
                validationException
            )
        }
        return validationException
    }
}
