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

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions

class ClusterMetadataDiffRequest(
    val connectionName: String,
    val categories: Set<String> = ALL_CATEGORIES
) {

    companion object {
        const val ACTION_NAME = "cluster:admin/plugins/replication/metadata/diff"
        val SUPPORTED_CATEGORIES = setOf("templates", "ingest_pipelines", "indices")
        val ALL_CATEGORIES = SUPPORTED_CATEGORIES
    }

    fun validate(): ActionRequestValidationException? {
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
