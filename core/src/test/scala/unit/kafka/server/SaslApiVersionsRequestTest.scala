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

import java.io.IOException
import java.net.Socket
import java.util.Collections
import org.apache.kafka.common.protocol.{ApiKeys, Errors, SecurityProtocol}
import org.apache.kafka.common.requests.{ApiVersionsRequest, ApiVersionsResponse}
import org.apache.kafka.common.requests.SaslHandshakeRequest
import org.apache.kafka.common.requests.SaslHandshakeResponse
import org.junit.Test
import org.junit.Assert._
import kafka.api.SaslTestHarness

class SaslApiVersionsRequestTest extends BaseRequestTest with SaslTestHarness {
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  override protected val kafkaClientSaslMechanism = "PLAIN"
  override protected val kafkaServerSaslMechanisms = List("PLAIN")
  override protected val saslProperties = Some(kafkaSaslProperties(kafkaClientSaslMechanism, Some(kafkaServerSaslMechanisms)))
  override protected val zkSaslEnabled = false
  override def numBrokers = 1

  @Test
  def testApiVersionsRequestBeforeSaslHandshakeRequest() {
    val plaintextSocket = connect(protocol = securityProtocol)
    try {
      val apiVersionsResponse = sendApiVersionsRequest(plaintextSocket, new ApiVersionsRequest, 0)
      ApiVersionsRequestTest.validateApiVersionsResponse(apiVersionsResponse)
      sendSaslHandshakeRequestValidateResponse(plaintextSocket)
    } finally {
      plaintextSocket.close()
    }
  }

  @Test
  def testApiVersionsRequestAfterSaslHandshakeRequest() {
    val plaintextSocket = connect(protocol = securityProtocol)
    try {
      sendSaslHandshakeRequestValidateResponse(plaintextSocket)
      try {
        sendApiVersionsRequest(plaintextSocket, new ApiVersionsRequest, 0)
        fail("Versions Request during Sasl handshake did not fail")
      } catch {
        case ioe: IOException => // expected exception
      }
    } finally {
      plaintextSocket.close()
    }
  }

  @Test
  def testApiVersionsRequestWithUnsupportedVersion() {
    val plaintextSocket = connect(protocol = securityProtocol)
    try {
      val apiVersionsResponse = sendApiVersionsRequest(plaintextSocket, new ApiVersionsRequest, Short.MaxValue)
      assertEquals(Errors.UNSUPPORTED_VERSION.code(), apiVersionsResponse.errorCode)
      val apiVersionsResponse2 = sendApiVersionsRequest(plaintextSocket, new ApiVersionsRequest, 0)
      ApiVersionsRequestTest.validateApiVersionsResponse(apiVersionsResponse2)
      sendSaslHandshakeRequestValidateResponse(plaintextSocket)
    } finally {
      plaintextSocket.close()
    }
  }

  private def sendApiVersionsRequest(socket: Socket, request: ApiVersionsRequest, version: Short): ApiVersionsResponse = {
    val response = send(request, ApiKeys.API_VERSIONS, version, socket)
    ApiVersionsResponse.parse(response)
  }

  private def sendSaslHandshakeRequestValidateResponse(socket: Socket) {
    val response = send(new SaslHandshakeRequest("PLAIN"), ApiKeys.SASL_HANDSHAKE, 0.toShort, socket)
    val handshakeResponse = SaslHandshakeResponse.parse(response)
    assertEquals(Errors.NONE.code, handshakeResponse.errorCode())
    assertEquals(Collections.singletonList("PLAIN"), handshakeResponse.enabledMechanisms())
  }
}
