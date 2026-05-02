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
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.HttpStatus
import org.apache.hc.core5.http.message.BasicHeader
import org.apache.hc.core5.http.io.entity.StringEntity
import org.apache.hc.core5.http2.HttpVersionPolicy
import org.assertj.core.api.Assertions.assertThat
import org.junit.Assume
import org.opensearch.client.Request
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.replication.MultiClusterAnnotations
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.startReplication
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager

const val STANDBY_FOLLOWER = "standbyFollowCluster"

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = STANDBY_FOLLOWER)
)
class SecurityStandbyModeIT : MultiClusterRestTestCase() {
    private val securityIndex = ".opendistro_security"

    fun `test security index replication initializes standby security plugin`() {
        Assume.assumeTrue(isSecurityPropertyEnabled)

        val leader = getNamedCluster(LEADER)
        val standby = getNamedCluster(STANDBY_FOLLOWER)
        val standbyAdminClient = standby.restClient

        createReplicatedUser(leader.lowLevelClient, "standby_user")
        createConnectionBetweenClusters(STANDBY_FOLLOWER, LEADER)

        standbyAdminClient.startReplication(StartReplicationRequest("source", securityIndex, securityIndex), waitForRestore = true)

        basicClient(STANDBY_FOLLOWER, "standby_user").use { standbyUserClient ->
            assertStandbySecurityInitialized(standbyUserClient)
        }

        createReplicatedUser(leader.lowLevelClient, "standby_user_after_start")
        basicClient(STANDBY_FOLLOWER, "standby_user_after_start").use { standbyUserClient ->
            assertStandbySecurityInitialized(standbyUserClient)
        }

        assertThat(standbyAdminClient.lowLevelClient.performRequest(Request("GET", "/_plugins/_security/api/roles")).statusLine.statusCode)
            .isEqualTo(HttpStatus.SC_OK)
        assertThatThrownByResponse {
            putJson(
                standbyAdminClient.lowLevelClient,
                "/_plugins/_security/api/roles/standby_should_reject_writes",
                """{"cluster_permissions":["cluster_monitor"]}"""
            )
        }.hasMessageContaining("standby mode")
    }

    private fun assertStandbySecurityInitialized(client: RestClient) {
        assertBusy({
            try {
                val health = client.performRequest(Request("GET", "/_cluster/health"))
                assertThat(health.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
            } catch (e: ResponseException) {
                assertThat(e.response.statusLine.statusCode).isEqualTo(HttpStatus.SC_SERVICE_UNAVAILABLE)
                assertThat(e.message).contains("OpenSearch Security not initialized")
                throw AssertionError("Security standby mode has not initialized from replicated config yet", e)
            }
        }, 90L, TimeUnit.SECONDS)
    }

    private fun createReplicatedUser(client: RestClient, username: String) {
        putJson(
            client,
            "/_plugins/_security/api/roles/standby_auth_role",
            """{"cluster_permissions":["cluster_monitor"]}"""
        )
        putJson(
            client,
            "/_plugins/_security/api/internalusers/$username",
            """{"password":"$INTEG_TEST_PASSWORD"}"""
        )
        putJson(
            client,
            "/_plugins/_security/api/rolesmapping/standby_auth_role",
            """{"users":["standby_user","standby_user_after_start"]}"""
        )
    }

    private fun putJson(client: RestClient, endpoint: String, body: String) {
        val request = Request("PUT", endpoint)
        request.entity = StringEntity(body, ContentType.APPLICATION_JSON)
        val response = client.performRequest(request)
        assertThat(response.statusLine.statusCode).isIn(HttpStatus.SC_OK, HttpStatus.SC_CREATED)
    }

    private fun assertThatThrownByResponse(call: () -> Unit): org.assertj.core.api.AbstractThrowableAssert<*, out Throwable> {
        return org.assertj.core.api.Assertions.assertThatThrownBy(call).isInstanceOf(ResponseException::class.java)
    }

    private fun basicClient(clusterName: String, username: String): RestClient {
        val cluster = getNamedCluster(clusterName)
        val trustCerts = arrayOf<TrustManager>(object : X509TrustManager {
            override fun checkClientTrusted(chain: Array<out java.security.cert.X509Certificate>?, authType: String?) {}
            override fun checkServerTrusted(chain: Array<out java.security.cert.X509Certificate>?, authType: String?) {}
            override fun getAcceptedIssuers(): Array<out java.security.cert.X509Certificate>? = null
        })
        val sslContext = SSLContext.getInstance("SSL")
        sslContext.init(null, trustCerts, java.security.SecureRandom())
        val tlsStrategy = ClientTlsStrategyBuilder.create()
            .setSslContext(sslContext)
            .setHostnameVerifier { _, _ -> true }
            .build()
        val connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
            .setTlsStrategy(tlsStrategy)
            .build()
        val credentials = "$username:$INTEG_TEST_PASSWORD"
        return RestClient.builder(*cluster.httpHosts.toTypedArray())
            .setHttpClientConfigCallback { builder ->
                builder.setConnectionManager(connectionManager)
                builder.setVersionPolicy(HttpVersionPolicy.FORCE_HTTP_1)
            }
            .setDefaultHeaders(
                arrayOf(
                    BasicHeader(
                        "Authorization",
                        "Basic " + Base64.getEncoder().encodeToString(credentials.toByteArray(StandardCharsets.UTF_8))
                    )
                )
            )
            .build()
    }
}
