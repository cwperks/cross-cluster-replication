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

package org.opensearch.replication.integ.rest

import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder
import org.apache.hc.core5.http.HttpStatus
import org.apache.hc.core5.http.HttpHost
import org.apache.hc.core5.http2.HttpVersionPolicy
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder
import org.assertj.core.api.Assertions.assertThat
import org.opensearch.client.Request
import org.opensearch.client.RestClient
import org.opensearch.client.ResponseException
import org.opensearch.client.RestHighLevelClient
import org.opensearch.replication.MultiClusterAnnotations
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.startReplication
import org.opensearch.replication.stopReplication
import org.opensearch.test.OpenSearchTestCase.assertBusy
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.security.KeyFactory
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64
import java.util.concurrent.TimeUnit
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class TaskIndexReplicationIT : MultiClusterRestTestCase() {

    fun `test tasks index replication with follower in standby mode`() {
        adminCertClient(LEADER).use { leaderClient ->
            adminCertClient(FOLLOWER).use { followerClient ->
                try {
                    createConnectionBetweenClusters(FOLLOWER, LEADER)
                    createPersistedTaskResult(leaderClient)
                    updateStandbyMode(followerClient, true)

                    followerClient.startReplication(
                        StartReplicationRequest("source", TASKS_INDEX, TASKS_INDEX),
                        waitForRestore = true
                    )

                    assertBusy({
                        assertThat(count(leaderClient, TASKS_INDEX)).isGreaterThan(0)
                        assertThat(count(followerClient, TASKS_INDEX)).isEqualTo(count(leaderClient, TASKS_INDEX))
                    }, 60, TimeUnit.SECONDS)
                } finally {
                    updateStandbyMode(followerClient, false)
                    stopReplicationIfStarted(followerClient, TASKS_INDEX)
                    deleteIndexIfExists(followerClient, TASKS_INDEX)
                    deleteIndexIfExists(leaderClient, TASKS_INDEX)
                }
            }
        }
    }

    private fun createPersistedTaskResult(client: RestHighLevelClient) {
        perform(client, "PUT", TASK_SOURCE_INDEX)
        perform(client, "POST", "$TASK_SOURCE_INDEX/_doc/1?refresh=true", """{"value":1}""")

        val reindex = Request("POST", "_reindex?wait_for_completion=false")
        reindex.setJsonEntity(
            """
            {
              "source": {
                "index": "$TASK_SOURCE_INDEX"
              },
              "dest": {
                "index": "$TASK_DEST_INDEX"
              }
            }
            """.trimIndent()
        )
        val taskId = OpenSearchRestTestCase.entityAsMap(client.lowLevelClient.performRequest(reindex))["task"].toString()
        val encodedTaskId = URLEncoder.encode(taskId, StandardCharsets.UTF_8.name())

        assertBusy({
            val task = getAsMap(client.lowLevelClient, "_tasks/$encodedTaskId?wait_for_completion=true&timeout=10s")
            assertThat(task["completed"]).isEqualTo(true)
            assertThat(count(client, TASKS_INDEX)).isGreaterThan(0)
        }, 60, TimeUnit.SECONDS)
    }

    private fun updateStandbyMode(client: RestHighLevelClient, enabled: Boolean) {
        perform(
            client,
            "PUT",
            "_cluster/settings",
            """
            {
              "persistent": {
                "cluster.standby_mode": $enabled
              }
            }
            """.trimIndent()
        )
    }

    private fun count(client: RestHighLevelClient, index: String): Int {
        val response = getAsMap(client.lowLevelClient, "$index/_count")
        return response["count"].toString().toInt()
    }

    private fun stopReplicationIfStarted(client: RestHighLevelClient, index: String) {
        try {
            client.stopReplication(index)
        } catch (e: ResponseException) {
            if (e.response.statusLine.statusCode != HttpStatus.SC_BAD_REQUEST && e.response.statusLine.statusCode != HttpStatus.SC_NOT_FOUND) {
                throw e
            }
        }
    }

    private fun deleteIndexIfExists(client: RestHighLevelClient, index: String) {
        try {
            perform(client, "DELETE", index)
        } catch (e: ResponseException) {
            if (e.response.statusLine.statusCode != HttpStatus.SC_NOT_FOUND) {
                throw e
            }
        }
    }

    private fun perform(client: RestHighLevelClient, method: String, endpoint: String, body: String? = null) {
        val request = Request(method, endpoint)
        if (body != null) {
            request.setJsonEntity(body)
        }
        client.lowLevelClient.performRequest(request)
    }

    private fun adminCertClient(clusterName: String): RestHighLevelClient {
        val trustCerts = arrayOf<TrustManager>(object : X509TrustManager {
            override fun checkClientTrusted(chain: Array<out java.security.cert.X509Certificate>?, authType: String?) {}
            override fun checkServerTrusted(chain: Array<out java.security.cert.X509Certificate>?, authType: String?) {}
            override fun getAcceptedIssuers(): Array<out java.security.cert.X509Certificate> = emptyArray()
        })

        val sslContext = SSLContext.getInstance("TLS")
        sslContext.init(
            keyManagers(),
            trustCerts,
            SecureRandom()
        )
        val tlsStrategy = ClientTlsStrategyBuilder.create()
            .setSslContext(sslContext)
            .setHostnameVerifier { _, _ -> true }
            .build()
        val connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
            .setTlsStrategy(tlsStrategy)
            .build()
        val hosts = getNamedCluster(clusterName).httpHosts.map { HttpHost.create(it.toURI()) }.toTypedArray()
        val builder = RestClient.builder(*hosts).setHttpClientConfigCallback {
            it.setConnectionManager(connectionManager)
            it.setVersionPolicy(HttpVersionPolicy.FORCE_HTTP_1)
        }
        builder.setStrictDeprecationMode(false)
        return RestHighLevelClient(builder)
    }

    private fun keyManagers(): Array<javax.net.ssl.KeyManager> {
        val certificateFactory = CertificateFactory.getInstance("X.509")
        val certificate = resourceBytes(KIRK_CERT_RESOURCE).inputStream().use { certificateFactory.generateCertificate(it) }
        val privateKey = KeyFactory.getInstance("RSA").generatePrivate(PKCS8EncodedKeySpec(decodePem(KIRK_KEY_RESOURCE, "PRIVATE KEY")))
        val password = CharArray(0)
        val keyStore = KeyStore.getInstance(KeyStore.getDefaultType())
        keyStore.load(null, password)
        keyStore.setKeyEntry("kirk", privateKey, password, arrayOf(certificate))
        val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
        keyManagerFactory.init(keyStore, password)
        return keyManagerFactory.keyManagers
    }

    private fun decodePem(resourceName: String, type: String): ByteArray {
        val pem = resourceBytes(resourceName).toString(StandardCharsets.UTF_8)
            .replace("-----BEGIN $type-----", "")
            .replace("-----END $type-----", "")
            .replace("\\s".toRegex(), "")
        return Base64.getDecoder().decode(pem)
    }

    private fun resourceBytes(resourceName: String): ByteArray {
        val classLoader = TaskIndexReplicationIT::class.java.classLoader
        return requireNotNull(classLoader.getResourceAsStream(resourceName)) { "Could not load test resource $resourceName" }.use { it.readBytes() }
    }

    companion object {
        private const val TASKS_INDEX = ".tasks"
        private const val TASK_SOURCE_INDEX = "task-source-index"
        private const val TASK_DEST_INDEX = "task-dest-index"
        private const val KIRK_CERT_RESOURCE = "security/plugin/kirk.pem"
        private const val KIRK_KEY_RESOURCE = "security/plugin/kirk-key.pem"
    }
}
