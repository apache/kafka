/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.Optional
import kafka.network
import kafka.network.RequestChannel
import kafka.utils.MockTime
import org.apache.kafka.clients.{ClientResponse, RequestCompletionHandler}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.AlterConfigsResponseData
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, AlterConfigsRequest, AlterConfigsResponse, EnvelopeRequest, EnvelopeResponse, RequestContext, RequestHeader, RequestTestUtils}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentMatchers, Mockito}

import scala.jdk.CollectionConverters._

class ForwardingManagerTest {
  private val brokerToController = Mockito.mock(classOf[BrokerToControllerChannelManager])
  private val time = new MockTime()
  private val principalBuilder = new DefaultKafkaPrincipalBuilder(null, null)

  @Test
  def testResponseCorrelationIdMismatch(): Unit = {
    val forwardingManager = new ForwardingManager(brokerToController, time, Long.MaxValue)
    val requestCorrelationId = 27
    val envelopeCorrelationId = 39
    val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "client")

    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, "foo")
    val configs = List(new AlterConfigsRequest.ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")).asJava
    val requestBody = new AlterConfigsRequest.Builder(Map(
      configResource -> new AlterConfigsRequest.Config(configs)
    ).asJava, false).build()
    val (requestHeader, requestBuffer) = buildRequest(requestBody, requestCorrelationId)
    val request = buildRequest(requestHeader, requestBuffer, clientPrincipal)

    val responseBody = new AlterConfigsResponse(new AlterConfigsResponseData())
    val responseBuffer = RequestTestUtils.serializeResponseWithHeader(responseBody, requestHeader.apiVersion,
      requestCorrelationId + 1)

    Mockito.when(brokerToController.sendRequest(
      any(classOf[EnvelopeRequest.Builder]),
      any(classOf[ControllerRequestCompletionHandler]),
      ArgumentMatchers.eq(Long.MaxValue)
    )).thenAnswer(invocation => {
      val completionHandler = invocation.getArgument[RequestCompletionHandler](1)
      val response = buildEnvelopeResponse(responseBuffer, envelopeCorrelationId, completionHandler)
      response.onComplete()
    })

    var response: AbstractResponse = null
    forwardingManager.forwardRequest(request, res => response = res)

    assertNotNull(response)
    assertEquals(Map(Errors.UNKNOWN_SERVER_ERROR -> 1).asJava, response.errorCounts())
  }

  private def buildEnvelopeResponse(
    responseBuffer: ByteBuffer,
    correlationId: Int,
    completionHandler: RequestCompletionHandler
  ): ClientResponse = {
    val envelopeRequestHeader = new RequestHeader(
      ApiKeys.ENVELOPE,
      ApiKeys.ENVELOPE.latestVersion(),
      "clientId",
      correlationId
    )
    val envelopeResponse = new EnvelopeResponse(
      responseBuffer,
      Errors.NONE
    )

    new ClientResponse(
      envelopeRequestHeader,
      completionHandler,
      "1",
      time.milliseconds(),
      time.milliseconds(),
      false,
      null,
      null,
      envelopeResponse
    )
  }

  private def buildRequest(
    body: AbstractRequest,
    correlationId: Int
  ): (RequestHeader, ByteBuffer) = {
    val header = new RequestHeader(
      body.apiKey,
      body.version,
      "clientId",
      correlationId
    )
    val buffer = RequestTestUtils.serializeRequestWithHeader(header, body)

    // Fast-forward buffer to start of the request as `RequestChannel.Request` expects
    RequestHeader.parse(buffer)

    (header, buffer)
  }

  private def buildRequest(
    requestHeader: RequestHeader,
    requestBuffer: ByteBuffer,
    principal: KafkaPrincipal
  ): RequestChannel.Request = {
    val requestContext = new RequestContext(
      requestHeader,
      "1",
      InetAddress.getLocalHost,
      principal,
      new ListenerName("client"),
      SecurityProtocol.SASL_PLAINTEXT,
      ClientInformation.EMPTY,
      false,
      Optional.of(principalBuilder)
    )

    new network.RequestChannel.Request(
      processor = 1,
      context = requestContext,
      startTimeNanos = time.nanoseconds(),
      memoryPool = MemoryPool.NONE,
      buffer = requestBuffer,
      metrics = new RequestChannel.Metrics(allowDisabledApis = true),
      envelope = None
    )
  }

}
