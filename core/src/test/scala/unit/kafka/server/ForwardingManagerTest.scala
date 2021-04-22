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
import java.util.concurrent.atomic.AtomicReference

import kafka.network
import kafka.network.RequestChannel
import kafka.utils.MockTime
import org.apache.kafka.clients.{MockClient, NodeApiVersions}
import org.apache.kafka.clients.MockClient.RequestMatcher
import org.apache.kafka.common.Node
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.{AlterConfigsResponseData, ApiVersionsResponseData}
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, AlterConfigsRequest, AlterConfigsResponse, EnvelopeRequest, EnvelopeResponse, RequestContext, RequestHeader, RequestTestUtils}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito

import scala.jdk.CollectionConverters._

class ForwardingManagerTest {
  private val time = new MockTime()
  private val client = new MockClient(time)
  private val controllerNodeProvider = Mockito.mock(classOf[ControllerNodeProvider])
  private val brokerToController = new MockBrokerToControllerChannelManager(
    client, time, controllerNodeProvider, controllerApiVersions)
  private val forwardingManager = new ForwardingManagerImpl(brokerToController)
  private val principalBuilder = new DefaultKafkaPrincipalBuilder(null, null)

  private def controllerApiVersions: NodeApiVersions = {
    // The Envelope API is not yet included in the standard set of APIs
    val envelopeApiVersion = new ApiVersionsResponseData.ApiVersion()
      .setApiKey(ApiKeys.ENVELOPE.id)
      .setMinVersion(ApiKeys.ENVELOPE.oldestVersion)
      .setMaxVersion(ApiKeys.ENVELOPE.latestVersion)
    NodeApiVersions.create(List(envelopeApiVersion).asJava)
  }

  @Test
  def testResponseCorrelationIdMismatch(): Unit = {
    val requestCorrelationId = 27
    val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "client")
    val (requestHeader, requestBuffer) = buildRequest(testAlterConfigRequest, requestCorrelationId)
    val request = buildRequest(requestHeader, requestBuffer, clientPrincipal)

    val responseBody = new AlterConfigsResponse(new AlterConfigsResponseData())
    val responseBuffer = RequestTestUtils.serializeResponseWithHeader(responseBody, requestHeader.apiVersion,
      requestCorrelationId + 1)

    Mockito.when(controllerNodeProvider.get()).thenReturn(Some(new Node(0, "host", 1234)))
    val isEnvelopeRequest: RequestMatcher = request => request.isInstanceOf[EnvelopeRequest]
    client.prepareResponse(isEnvelopeRequest, new EnvelopeResponse(responseBuffer, Errors.NONE));

    val responseOpt = new AtomicReference[Option[AbstractResponse]]()
    forwardingManager.forwardRequest(request, responseOpt.set)
    brokerToController.poll()
    assertTrue(Option(responseOpt.get).isDefined)

    val response = responseOpt.get.get
    assertEquals(Map(Errors.UNKNOWN_SERVER_ERROR -> 1).asJava, response.errorCounts())
  }

  @Test
  def testUnsupportedVersions(): Unit = {
    val requestCorrelationId = 27
    val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "client")
    val (requestHeader, requestBuffer) = buildRequest(testAlterConfigRequest, requestCorrelationId)
    val request = buildRequest(requestHeader, requestBuffer, clientPrincipal)

    val responseBody = new AlterConfigsResponse(new AlterConfigsResponseData())
    val responseBuffer = RequestTestUtils.serializeResponseWithHeader(responseBody,
      requestHeader.apiVersion, requestCorrelationId)

    Mockito.when(controllerNodeProvider.get()).thenReturn(Some(new Node(0, "host", 1234)))
    val isEnvelopeRequest: RequestMatcher = request => request.isInstanceOf[EnvelopeRequest]
    client.prepareResponse(isEnvelopeRequest, new EnvelopeResponse(responseBuffer, Errors.UNSUPPORTED_VERSION));

    val responseOpt = new AtomicReference[Option[AbstractResponse]]()
    forwardingManager.forwardRequest(request, responseOpt.set)
    brokerToController.poll()
    assertEquals(None, responseOpt.get)
  }

  @Test
  def testForwardingTimeoutWaitingForControllerDiscovery(): Unit = {
    val requestCorrelationId = 27
    val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "client")
    val (requestHeader, requestBuffer) = buildRequest(testAlterConfigRequest, requestCorrelationId)
    val request = buildRequest(requestHeader, requestBuffer, clientPrincipal)

    Mockito.when(controllerNodeProvider.get()).thenReturn(None)

    val response = new AtomicReference[AbstractResponse]()
    forwardingManager.forwardRequest(request, res => res.foreach(response.set))
    brokerToController.poll()
    assertNull(response.get)

    // The controller is not discovered before reaching the retry timeout.
    // The request should fail with a timeout error.
    time.sleep(brokerToController.retryTimeoutMs)
    brokerToController.poll()
    assertNotNull(response.get)

    val alterConfigResponse = response.get.asInstanceOf[AlterConfigsResponse]
    assertEquals(Map(Errors.REQUEST_TIMED_OUT -> 1).asJava, alterConfigResponse.errorCounts)
  }

  @Test
  def testForwardingTimeoutAfterRetry(): Unit = {
    val requestCorrelationId = 27
    val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "client")
    val (requestHeader, requestBuffer) = buildRequest(testAlterConfigRequest, requestCorrelationId)
    val request = buildRequest(requestHeader, requestBuffer, clientPrincipal)

    Mockito.when(controllerNodeProvider.get()).thenReturn(Some(new Node(0, "host", 1234)))

    val response = new AtomicReference[AbstractResponse]()
    forwardingManager.forwardRequest(request, res => res.foreach(response.set))
    brokerToController.poll()
    assertNull(response.get)

    // After reaching the retry timeout, we get a disconnect. Instead of retrying,
    // we should fail the request with a timeout error.
    time.sleep(brokerToController.retryTimeoutMs)
    client.respond(testAlterConfigRequest.getErrorResponse(0, Errors.UNKNOWN_SERVER_ERROR.exception), true)
    brokerToController.poll()
    brokerToController.poll()
    assertNotNull(response.get)

    val alterConfigResponse = response.get.asInstanceOf[AlterConfigsResponse]
    assertEquals(Map(Errors.REQUEST_TIMED_OUT -> 1).asJava, alterConfigResponse.errorCounts)
  }

  @Test
  def testUnsupportedVersionFromNetworkClient(): Unit = {
    val requestCorrelationId = 27
    val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "client")
    val (requestHeader, requestBuffer) = buildRequest(testAlterConfigRequest, requestCorrelationId)
    val request = buildRequest(requestHeader, requestBuffer, clientPrincipal)

    val controllerNode = new Node(0, "host", 1234)
    Mockito.when(controllerNodeProvider.get()).thenReturn(Some(controllerNode))

    client.prepareUnsupportedVersionResponse(req => req.apiKey == requestHeader.apiKey)

    val response = new AtomicReference[AbstractResponse]()
    forwardingManager.forwardRequest(request, res => res.foreach(response.set))
    brokerToController.poll()
    assertNotNull(response.get)

    val alterConfigResponse = response.get.asInstanceOf[AlterConfigsResponse]
    assertEquals(Map(Errors.UNKNOWN_SERVER_ERROR -> 1).asJava, alterConfigResponse.errorCounts)
  }

  @Test
  def testFailedAuthentication(): Unit = {
    val requestCorrelationId = 27
    val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "client")
    val (requestHeader, requestBuffer) = buildRequest(testAlterConfigRequest, requestCorrelationId)
    val request = buildRequest(requestHeader, requestBuffer, clientPrincipal)

    val controllerNode = new Node(0, "host", 1234)
    Mockito.when(controllerNodeProvider.get()).thenReturn(Some(controllerNode))

    client.createPendingAuthenticationError(controllerNode, 50)

    val response = new AtomicReference[AbstractResponse]()
    forwardingManager.forwardRequest(request, res => res.foreach(response.set))
    brokerToController.poll()
    assertNotNull(response.get)

    val alterConfigResponse = response.get.asInstanceOf[AlterConfigsResponse]
    assertEquals(Map(Errors.UNKNOWN_SERVER_ERROR -> 1).asJava, alterConfigResponse.errorCounts)
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
      metrics = new RequestChannel.Metrics(ListenerType.CONTROLLER),
      envelope = None
    )
  }

  private def testAlterConfigRequest: AlterConfigsRequest = {
    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, "foo")
    val configs = List(new AlterConfigsRequest.ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")).asJava
    new AlterConfigsRequest.Builder(Map(
      configResource -> new AlterConfigsRequest.Config(configs)
    ).asJava, false).build()
  }

}
