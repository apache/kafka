/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.server

import integration.kafka.server.IntegrationTestUtils
import kafka.test.{ClusterConfig, ClusterInstance}
import kafka.test.annotation.ClusterTest
import kafka.test.junit.ClusterTestExtensions
import kafka.network.Processor.ListenerMetricTag
import kafka.network.Processor.NetworkProcessorMetricTag
import kafka.utils.TestUtils
import org.apache.kafka.common.message.ApiVersionsRequestData
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.network.ClientInformation.UNKNOWN_NAME_OR_VERSION
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ApiVersionsRequest, ApiVersionsResponse}
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith

import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class ApiVersionsRequestTest(cluster: ClusterInstance) extends AbstractApiVersionsRequestTest(cluster) {

  @BeforeEach
  def setup(config: ClusterConfig): Unit = {
    this.brokerPropertyOverrides(config.serverProperties())
    // These configurations are set to make `testClientInformation` predictable.
    config.serverProperties().put(KafkaConfig.NumIoThreadsProp, "1")
    config.serverProperties().put(KafkaConfig.NumNetworkThreadsProp, "1")
  }

  @ClusterTest
  def testApiVersionsRequest(): Unit = {
    val request = new ApiVersionsRequest.Builder().build()
    val apiVersionsResponse = sendApiVersionsRequest(request, cluster.clientListener())
    validateApiVersionsResponse(apiVersionsResponse)
  }

  @ClusterTest
  def testApiVersionsRequestThroughControlPlaneListener(): Unit = {
    val request = new ApiVersionsRequest.Builder().build()
    val apiVersionsResponse = sendApiVersionsRequest(request, super.controlPlaneListenerName)
    validateApiVersionsResponse(apiVersionsResponse)
  }

  @ClusterTest
  def testApiVersionsRequestWithUnsupportedVersion(): Unit = {
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build()
    val apiVersionsResponse = sendUnsupportedApiVersionRequest(apiVersionsRequest)
    assertEquals(Errors.UNSUPPORTED_VERSION.code, apiVersionsResponse.data.errorCode)
    assertFalse(apiVersionsResponse.data.apiKeys.isEmpty)
    val apiVersion = apiVersionsResponse.data.apiKeys.find(ApiKeys.API_VERSIONS.id)
    assertEquals(ApiKeys.API_VERSIONS.id, apiVersion.apiKey)
    assertEquals(ApiKeys.API_VERSIONS.oldestVersion, apiVersion.minVersion)
    assertEquals(ApiKeys.API_VERSIONS.latestVersion, apiVersion.maxVersion)
  }

  @ClusterTest
  def testApiVersionsRequestValidationV0(): Unit = {
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0.toShort)
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest, cluster.clientListener())
    validateApiVersionsResponse(apiVersionsResponse)
  }

  @ClusterTest
  def testApiVersionsRequestValidationV0ThroughControlPlaneListener(): Unit = {
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0.toShort)
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest, super.controlPlaneListenerName)
    validateApiVersionsResponse(apiVersionsResponse)
  }

  @ClusterTest
  def testClientInformationValidation(): Unit = {
    for (version <- ApiKeys.API_VERSIONS.oldestVersion to ApiKeys.API_VERSIONS.latestVersion) {
      // Invalid request for any version >= 3 because Name and Version are empty by default
      val apiVersionsRequest = new ApiVersionsRequest(
        new ApiVersionsRequestData(),
        version.toShort)
      val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest, cluster.clientListener())

      if (version >= 3)
        assertEquals(Errors.INVALID_REQUEST.code, apiVersionsResponse.data.errorCode)
      else
        assertEquals(Errors.NONE.code, apiVersionsResponse.data.errorCode)
    }
  }

  @ClusterTest
  def testClientInformation(): Unit = {
    val softwareName = "name"
    val softwareVersion = "version"

    for (version <- ApiKeys.API_VERSIONS.oldestVersion to ApiKeys.API_VERSIONS.latestVersion) {
      val socket = IntegrationTestUtils.connect(cluster.anyBrokerSocketServer(), cluster.clientListener())
      waitClientInformation(UNKNOWN_NAME_OR_VERSION, UNKNOWN_NAME_OR_VERSION, 1)

      try {
        val apiVersionsRequest = new ApiVersionsRequest(
          new ApiVersionsRequestData()
            .setClientSoftwareName(softwareName)
            .setClientSoftwareVersion(softwareVersion),
          version.toShort)
        val apiVersionsResponse = IntegrationTestUtils.sendAndReceive[ApiVersionsResponse](apiVersionsRequest, socket)
        assertEquals(Errors.NONE.code, apiVersionsResponse.data.errorCode)

        if (version >= 3) {
          waitClientInformation(softwareName, softwareVersion, 1)
          waitClientInformation(UNKNOWN_NAME_OR_VERSION, UNKNOWN_NAME_OR_VERSION, 0)
        } else {
          waitClientInformation(UNKNOWN_NAME_OR_VERSION, UNKNOWN_NAME_OR_VERSION, 1)
        }
      } finally socket.close()

      if (version >= 3) {
        waitClientInformation(softwareName, softwareVersion, 0)
      }
      waitClientInformation(UNKNOWN_NAME_OR_VERSION, UNKNOWN_NAME_OR_VERSION, 0)
    }
  }

  private def waitClientInformation(
    softwareName: String,
    softwareVersion: String,
    expectedCount: Int
  ): Unit = {
    TestUtils.retry(JTestUtils.DEFAULT_MAX_WAIT_MS) {
      clientInformationMetric(softwareName, softwareVersion) match {
        case Some(metric) =>
          assertEquals(expectedCount, metric.metricValue())

        case None =>
          fail(s"Metric for '$softwareName' and '$softwareVersion' is not defined")
      }
    }
  }

  private def clientInformationMetric(
    softwareName: String,
    softwareVersion: String
  ): Option[KafkaMetric] = {
    val metrics = cluster.anyBrokerSocketServer().metrics
    val tags = Map(
      ListenerMetricTag -> cluster.clientListener().value,
      NetworkProcessorMetricTag -> "1", // The harness is configured to have only one processor
      "clientSoftwareName" -> softwareName,
      "clientSoftwareVersion" -> softwareVersion
    )
    val metricName = metrics.metricName(
      "connections",
      "socket-server-metrics",
      "The number of connections with this client and version.",
      tags.asJava)
    Option(metrics.metric(metricName))
  }
}
