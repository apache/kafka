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

import java.net.Socket
import java.util.Collections

import kafka.api.{KafkaSasl, SaslSetup}
import kafka.utils.JaasTestUtils
import org.apache.kafka.common.message.SaslHandshakeRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ApiVersionsRequest, ApiVersionsResponse, SaslHandshakeRequest, SaslHandshakeResponse}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Assert._
import org.junit.{After, Before, Test}

class SaslApiVersionsRequestTest extends AbstractApiVersionsRequestTest with SaslSetup {
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List("PLAIN")
  protected override val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  protected override val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))
  override def brokerCount = 1

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), KafkaSasl, JaasTestUtils.KafkaServerContextName))
    super.setUp()
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  @Test
  def testApiVersionsRequestBeforeSaslHandshakeRequest(): Unit = {
    val plaintextSocket = connect(protocol = securityProtocol)
    try {
      val apiVersionsResponse = sendApiVersionsRequest(plaintextSocket, new ApiVersionsRequest.Builder().build(0))
      ApiVersionsRequestTest.validateApiVersionsResponse(apiVersionsResponse)
      sendSaslHandshakeRequestValidateResponse(plaintextSocket)
    } finally {
      plaintextSocket.close()
    }
  }

  @Test
  def testApiVersionsRequestAfterSaslHandshakeRequest(): Unit = {
    val plaintextSocket = connect(protocol = securityProtocol)
    try {
      sendSaslHandshakeRequestValidateResponse(plaintextSocket)
      val response = sendApiVersionsRequest(plaintextSocket, new ApiVersionsRequest.Builder().build(0))
      assertEquals(Errors.ILLEGAL_SASL_STATE.code(), response.data.errorCode())
    } finally {
      plaintextSocket.close()
    }
  }

  @Test
  def testApiVersionsRequestWithUnsupportedVersion(): Unit = {
    val plaintextSocket = connect(protocol = securityProtocol)
    try {
      val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0)
      val apiVersionsResponse = sendUnsupportedApiVersionRequest(apiVersionsRequest)
      assertEquals(Errors.UNSUPPORTED_VERSION.code(), apiVersionsResponse.data.errorCode())
      val apiVersionsResponse2 = sendApiVersionsRequest(plaintextSocket, new ApiVersionsRequest.Builder().build(0))
      ApiVersionsRequestTest.validateApiVersionsResponse(apiVersionsResponse2)
      sendSaslHandshakeRequestValidateResponse(plaintextSocket)
    } finally {
      plaintextSocket.close()
    }
  }

  private def sendApiVersionsRequest(socket: Socket, request: ApiVersionsRequest): ApiVersionsResponse = {
    sendAndReceive(request, socket).asInstanceOf[ApiVersionsResponse]
  }

  private def sendSaslHandshakeRequestValidateResponse(socket: Socket): Unit = {
    val request = new SaslHandshakeRequest(new SaslHandshakeRequestData().setMechanism("PLAIN"))
    val response = sendAndReceive(request, socket).asInstanceOf[SaslHandshakeResponse]
    assertEquals(Errors.NONE, response.error)
    assertEquals(Collections.singletonList("PLAIN"), response.enabledMechanisms)
  }
}
