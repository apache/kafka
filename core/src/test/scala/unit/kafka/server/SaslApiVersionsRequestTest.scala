/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import integration.kafka.server.IntegrationTestHelper

import java.net.Socket
import java.util.Collections
import kafka.api.{KafkaSasl, SaslSetup}
import kafka.utils.JaasTestUtils
import org.apache.kafka.common.message.SaslHandshakeRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ApiVersionsRequest, ApiVersionsResponse, SaslHandshakeRequest, SaslHandshakeResponse}
import kafka.test.annotations.{ClusterTest, Type}
import kafka.test.{ClusterConfig, ClusterInstance}
import kafka.test.junit.ClusterForEach
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.extension.ExtendWith

import scala.jdk.CollectionConverters._


@ExtendWith(value = Array(classOf[ClusterForEach]))
class SaslApiVersionsRequestTest(helper: IntegrationTestHelper,
                                 cluster: ClusterInstance) extends AbstractApiVersionsRequestTest(helper, cluster) {

  val kafkaClientSaslMechanism = "PLAIN"
  val kafkaServerSaslMechanisms = List("PLAIN")

  private var sasl: SaslSetup = _

  @BeforeEach
  def setupSasl(config: ClusterConfig): Unit = {
    sasl = new SaslSetup() {}
    sasl.startSasl(sasl.jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), KafkaSasl, JaasTestUtils.KafkaServerContextName))
    config.saslServerProperties().putAll(sasl.kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))

    super.brokerPropertyOverrides(config.serverProperties())
  }

  @ClusterTest(securityProtocol = "SASL_PLAINTEXT", clusterType = Type.Zk)
  def testApiVersionsRequestBeforeSaslHandshakeRequest(): Unit = {
    val socket = helper.connect(cluster.brokers().asScala.head, cluster.listener())
    try {
      val apiVersionsResponse = helper.sendAndReceive[ApiVersionsResponse](
        new ApiVersionsRequest.Builder().build(0), socket)
      validateApiVersionsResponse(apiVersionsResponse, cluster.listener())
      sendSaslHandshakeRequestValidateResponse(socket)
    } finally {
      socket.close()
    }
  }

  @ClusterTest(securityProtocol = "SASL_PLAINTEXT", clusterType = Type.Zk)
  def testApiVersionsRequestAfterSaslHandshakeRequest(): Unit = {
    val socket = helper.connect(cluster.brokers().asScala.head, cluster.listener())
    try {
      sendSaslHandshakeRequestValidateResponse(socket)
      val response = helper.sendAndReceive[ApiVersionsResponse](
        new ApiVersionsRequest.Builder().build(0), socket)
      assertEquals(Errors.ILLEGAL_SASL_STATE.code, response.data.errorCode)
    } finally {
      socket.close()
    }
  }

  @ClusterTest(securityProtocol = "SASL_PLAINTEXT", clusterType = Type.Zk)
  def testApiVersionsRequestWithUnsupportedVersion(): Unit = {
    val socket = helper.connect(cluster.brokers().asScala.head, cluster.listener())
    try {
      val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0)
      val apiVersionsResponse = sendUnsupportedApiVersionRequest(apiVersionsRequest)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, apiVersionsResponse.data.errorCode)
      val apiVersionsResponse2 = helper.sendAndReceive[ApiVersionsResponse](
        new ApiVersionsRequest.Builder().build(0), socket)
      validateApiVersionsResponse(apiVersionsResponse2, cluster.listener())
      sendSaslHandshakeRequestValidateResponse(socket)
    } finally {
      socket.close()
    }
  }

  @AfterEach
  def closeSasl(): Unit = {
    sasl.closeSasl()
  }

  private def sendSaslHandshakeRequestValidateResponse(socket: Socket): Unit = {
    val request = new SaslHandshakeRequest(new SaslHandshakeRequestData().setMechanism("PLAIN"),
      ApiKeys.SASL_HANDSHAKE.latestVersion)
    val response = helper.sendAndReceive[SaslHandshakeResponse](request, socket)
    assertEquals(Errors.NONE, response.error)
    assertEquals(Collections.singletonList("PLAIN"), response.enabledMechanisms)
  }
}
