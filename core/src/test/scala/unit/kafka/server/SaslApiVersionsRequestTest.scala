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

import org.apache.kafka.common.message.SaslHandshakeRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ApiVersionsRequest, ApiVersionsResponse, SaslHandshakeRequest, SaslHandshakeResponse}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.test.api.{ClusterTest, Type}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.junit.ClusterTestExtensions
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith

import java.net.Socket
import java.util.Collections
import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class SaslApiVersionsRequestTest(cluster: ClusterInstance) extends AbstractApiVersionsRequestTest(cluster) {

  @ClusterTest(types = Array(Type.KRAFT),
    brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
    controllerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT
  )
  def testApiVersionsRequestBeforeSaslHandshakeRequest(): Unit = {
    val socket = IntegrationTestUtils.connect(cluster.brokerSocketServers().asScala.head, cluster.clientListener())
    try {
      val apiVersionsResponse = IntegrationTestUtils.sendAndReceive[ApiVersionsResponse](
        new ApiVersionsRequest.Builder().build(0), socket)
      validateApiVersionsResponse(
        apiVersionsResponse,
        enableUnstableLastVersion = !"false".equals(
          cluster.config().serverProperties().get("unstable.api.versions.enable")),
        apiVersion = 0.toShort
      )
      sendSaslHandshakeRequestValidateResponse(socket)
    } finally {
      socket.close()
    }
  }

  @ClusterTest(types = Array(Type.KRAFT),
    brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
    controllerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT
  )
  def testApiVersionsRequestAfterSaslHandshakeRequest(): Unit = {
    val socket = IntegrationTestUtils.connect(cluster.brokerSocketServers().asScala.head, cluster.clientListener())
    try {
      sendSaslHandshakeRequestValidateResponse(socket)
      val response = IntegrationTestUtils.sendAndReceive[ApiVersionsResponse](
        new ApiVersionsRequest.Builder().build(0), socket)
      assertEquals(Errors.ILLEGAL_SASL_STATE.code, response.data.errorCode)
    } finally {
      socket.close()
    }
  }

  @ClusterTest(types = Array(Type.KRAFT),
    brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
    controllerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT
  )
  def testApiVersionsRequestWithUnsupportedVersion(): Unit = {
    val socket = IntegrationTestUtils.connect(cluster.brokerSocketServers().asScala.head, cluster.clientListener())
    try {
      val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0)
      val apiVersionsResponse = sendUnsupportedApiVersionRequest(apiVersionsRequest)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, apiVersionsResponse.data.errorCode)
      val apiVersionsResponse2 = IntegrationTestUtils.sendAndReceive[ApiVersionsResponse](
        new ApiVersionsRequest.Builder().build(0), socket)
      validateApiVersionsResponse(
        apiVersionsResponse2,
        enableUnstableLastVersion = !"false".equals(
          cluster.config().serverProperties().get("unstable.api.versions.enable")),
        apiVersion = 0.toShort
      )
      sendSaslHandshakeRequestValidateResponse(socket)
    } finally {
      socket.close()
    }
  }

  private def sendSaslHandshakeRequestValidateResponse(socket: Socket): Unit = {
    val request = new SaslHandshakeRequest(new SaslHandshakeRequestData().setMechanism("PLAIN"),
      ApiKeys.SASL_HANDSHAKE.latestVersion)
    val response = IntegrationTestUtils.sendAndReceive[SaslHandshakeResponse](request, socket)
    assertEquals(Errors.NONE, response.error)
    assertEquals(Collections.singletonList("PLAIN"), response.enabledMechanisms)
  }
}
