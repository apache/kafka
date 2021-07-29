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

import java.nio.ByteBuffer
import java.util.Collections
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import kafka.utils.TestUtils
import org.apache.kafka.clients.{ClientResponse, ManualMetadataUpdater, Metadata, MockClient, NodeApiVersions}
import org.apache.kafka.common.Node
import org.apache.kafka.common.message.{EnvelopeResponseData, MetadataRequestData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractRequest, EnvelopeRequest, EnvelopeResponse, MetadataRequest, MetadataResponse, RequestTestUtils}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito._


class BrokerToControllerRequestThreadTest {

  @Test
  def testRetryTimeoutWhileControllerNotAvailable(): Unit = {
    val time = new MockTime()
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))
    val metadata = mock(classOf[Metadata])
    val mockClient = new MockClient(time, metadata)
    val controllerNodeProvider = mock(classOf[ControllerNodeProvider])

    when(controllerNodeProvider.get()).thenReturn(None)

    val retryTimeoutMs = 30000
    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), controllerNodeProvider,
      config, time, "", retryTimeoutMs)
    testRequestThread.started = true

    val completionHandler = new TestRequestCompletionHandler(None)
    val queueItem = BrokerToControllerQueueItem(
      time.milliseconds(),
      new MetadataRequest.Builder(new MetadataRequestData()),
      completionHandler
    )

    testRequestThread.enqueue(queueItem)
    testRequestThread.doWork()
    assertEquals(1, testRequestThread.queueSize)

    time.sleep(retryTimeoutMs)
    testRequestThread.doWork()
    assertEquals(0, testRequestThread.queueSize)
    assertTrue(completionHandler.timedOut.get)
  }

  @Test
  def testRequestsSent(): Unit = {
    // just a simple test that tests whether the request from 1 -> 2 is sent and the response callback is called
    val time = new MockTime()
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))
    val controllerId = 2

    val metadata = mock(classOf[Metadata])
    val mockClient = new MockClient(time, metadata)

    val controllerNodeProvider = mock(classOf[ControllerNodeProvider])
    val activeController = new Node(controllerId, "host", 1234)

    when(controllerNodeProvider.get()).thenReturn(Some(activeController))

    val expectedResponse = RequestTestUtils.metadataUpdateWith(2, Collections.singletonMap("a", 2))
    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), controllerNodeProvider,
      config, time, "", retryTimeoutMs = Long.MaxValue)
    testRequestThread.started = true
    mockClient.prepareResponse(expectedResponse)

    val completionHandler = new TestRequestCompletionHandler(Some(expectedResponse))
    val queueItem = BrokerToControllerQueueItem(
      time.milliseconds(),
      new MetadataRequest.Builder(new MetadataRequestData()),
      completionHandler
    )

    testRequestThread.enqueue(queueItem)
    assertEquals(1, testRequestThread.queueSize)

    // initialize to the controller
    testRequestThread.doWork()
    // send and process the request
    testRequestThread.doWork()

    assertEquals(0, testRequestThread.queueSize)
    assertTrue(completionHandler.completed.get())
  }

  @Test
  def testControllerChanged(): Unit = {
    // in this test the current broker is 1, and the controller changes from 2 -> 3 then back: 3 -> 2
    val time = new MockTime()
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))
    val oldControllerId = 1
    val newControllerId = 2

    val metadata = mock(classOf[Metadata])
    val mockClient = new MockClient(time, metadata)

    val controllerNodeProvider = mock(classOf[ControllerNodeProvider])
    val oldController = new Node(oldControllerId, "host1", 1234)
    val newController = new Node(newControllerId, "host2", 1234)

    when(controllerNodeProvider.get()).thenReturn(Some(oldController), Some(newController))

    val expectedResponse = RequestTestUtils.metadataUpdateWith(3, Collections.singletonMap("a", 2))
    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(),
      controllerNodeProvider, config, time, "", retryTimeoutMs = Long.MaxValue)
    testRequestThread.started = true

    val completionHandler = new TestRequestCompletionHandler(Some(expectedResponse))
    val queueItem = BrokerToControllerQueueItem(
      time.milliseconds(),
      new MetadataRequest.Builder(new MetadataRequestData()),
      completionHandler,
    )

    testRequestThread.enqueue(queueItem)
    mockClient.prepareResponse(expectedResponse)
    // initialize the thread with oldController
    testRequestThread.doWork()
    assertFalse(completionHandler.completed.get())

    // disconnect the node
    mockClient.setUnreachable(oldController, time.milliseconds() + 5000)
    // verify that the client closed the connection to the faulty controller
    testRequestThread.doWork()
    // should connect to the new controller
    testRequestThread.doWork()
    // should send the request and process the response
    testRequestThread.doWork()

    assertTrue(completionHandler.completed.get())
  }

  @Test
  def testNotController(): Unit = {
    val time = new MockTime()
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))
    val oldControllerId = 1
    val newControllerId = 2

    val metadata = mock(classOf[Metadata])
    val mockClient = new MockClient(time, metadata)

    val controllerNodeProvider = mock(classOf[ControllerNodeProvider])
    val port = 1234
    val oldController = new Node(oldControllerId, "host1", port)
    val newController = new Node(newControllerId, "host2", port)

    when(controllerNodeProvider.get()).thenReturn(Some(oldController), Some(newController))

    val responseWithNotControllerError = RequestTestUtils.metadataUpdateWith("cluster1", 2,
      Collections.singletonMap("a", Errors.NOT_CONTROLLER),
      Collections.singletonMap("a", 2))
    val expectedResponse = RequestTestUtils.metadataUpdateWith(3, Collections.singletonMap("a", 2))
    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), controllerNodeProvider,
      config, time, "", retryTimeoutMs = Long.MaxValue)
    testRequestThread.started = true

    val completionHandler = new TestRequestCompletionHandler(Some(expectedResponse))
    val queueItem = BrokerToControllerQueueItem(
      time.milliseconds(),
      new MetadataRequest.Builder(new MetadataRequestData()
        .setAllowAutoTopicCreation(true)),
      completionHandler
    )
    testRequestThread.enqueue(queueItem)
    // initialize to the controller
    testRequestThread.doWork()

    val oldBrokerNode = new Node(oldControllerId, "host1", port)
    assertEquals(Some(oldBrokerNode), testRequestThread.activeControllerAddress())

    // send and process the request
    mockClient.prepareResponse((body: AbstractRequest) => {
      body.isInstanceOf[MetadataRequest] &&
      body.asInstanceOf[MetadataRequest].allowAutoTopicCreation()
    }, responseWithNotControllerError)
    testRequestThread.doWork()
    assertEquals(None, testRequestThread.activeControllerAddress())
    // reinitialize the controller to a different node
    testRequestThread.doWork()
    // process the request again
    mockClient.prepareResponse(expectedResponse)
    testRequestThread.doWork()

    val newControllerNode = new Node(newControllerId, "host2", port)
    assertEquals(Some(newControllerNode), testRequestThread.activeControllerAddress())

    assertTrue(completionHandler.completed.get())
  }

  @Test
  def testEnvelopeResponseWithNotControllerError(): Unit = {
    val time = new MockTime()
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))
    val oldControllerId = 1
    val newControllerId = 2

    val metadata = mock(classOf[Metadata])
    val mockClient = new MockClient(time, metadata)
    // enable envelope API
    mockClient.setNodeApiVersions(NodeApiVersions.create(ApiKeys.ENVELOPE.id, 0.toShort, 0.toShort))

    val controllerNodeProvider = mock(classOf[ControllerNodeProvider])
    val port = 1234
    val oldController = new Node(oldControllerId, "host1", port)
    val newController = new Node(newControllerId, "host2", port)

    when(controllerNodeProvider.get()).thenReturn(Some(oldController), Some(newController))

    // create an envelopeResponse with NOT_CONTROLLER error
    val envelopeResponseWithNotControllerError = new EnvelopeResponse(
      new EnvelopeResponseData().setErrorCode(Errors.NOT_CONTROLLER.code()))

    // response for retry request after receiving NOT_CONTROLLER error
    val expectedResponse = RequestTestUtils.metadataUpdateWith(3, Collections.singletonMap("a", 2))

    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), controllerNodeProvider,
      config, time, "", retryTimeoutMs = Long.MaxValue)
    testRequestThread.started = true

    val completionHandler = new TestRequestCompletionHandler(Some(expectedResponse))
    val kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "principal", true)
    val kafkaPrincipalBuilder = new DefaultKafkaPrincipalBuilder(null, null)

    // build an EnvelopeRequest by dummy data
    val envelopeRequestBuilder = new EnvelopeRequest.Builder(ByteBuffer.allocate(0),
      kafkaPrincipalBuilder.serialize(kafkaPrincipal), "client-address".getBytes)

    val queueItem = BrokerToControllerQueueItem(
      time.milliseconds(),
      envelopeRequestBuilder,
      completionHandler
    )

    testRequestThread.enqueue(queueItem)
    // initialize to the controller
    testRequestThread.doWork()

    val oldBrokerNode = new Node(oldControllerId, "host1", port)
    assertEquals(Some(oldBrokerNode), testRequestThread.activeControllerAddress())

    // send and process the envelope request
    mockClient.prepareResponse((body: AbstractRequest) => {
      body.isInstanceOf[EnvelopeRequest]
    }, envelopeResponseWithNotControllerError)
    testRequestThread.doWork()
    // expect to reset the activeControllerAddress after finding the NOT_CONTROLLER error
    assertEquals(None, testRequestThread.activeControllerAddress())
    // reinitialize the controller to a different node
    testRequestThread.doWork()
    // process the request again
    mockClient.prepareResponse(expectedResponse)
    testRequestThread.doWork()

    val newControllerNode = new Node(newControllerId, "host2", port)
    assertEquals(Some(newControllerNode), testRequestThread.activeControllerAddress())

    assertTrue(completionHandler.completed.get())
  }

  @Test
  def testRetryTimeout(): Unit = {
    val time = new MockTime()
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))
    val controllerId = 1

    val metadata = mock(classOf[Metadata])
    val mockClient = new MockClient(time, metadata)

    val controllerNodeProvider = mock(classOf[ControllerNodeProvider])
    val controller = new Node(controllerId, "host1", 1234)

    when(controllerNodeProvider.get()).thenReturn(Some(controller))

    val retryTimeoutMs = 30000
    val responseWithNotControllerError = RequestTestUtils.metadataUpdateWith("cluster1", 2,
      Collections.singletonMap("a", Errors.NOT_CONTROLLER),
      Collections.singletonMap("a", 2))
    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), controllerNodeProvider,
      config, time, "", retryTimeoutMs)
    testRequestThread.started = true

    val completionHandler = new TestRequestCompletionHandler()
    val queueItem = BrokerToControllerQueueItem(
      time.milliseconds(),
      new MetadataRequest.Builder(new MetadataRequestData()
        .setAllowAutoTopicCreation(true)),
      completionHandler
    )

    testRequestThread.enqueue(queueItem)

    // initialize to the controller
    testRequestThread.doWork()

    time.sleep(retryTimeoutMs)

    // send and process the request
    mockClient.prepareResponse((body: AbstractRequest) => {
      body.isInstanceOf[MetadataRequest] &&
        body.asInstanceOf[MetadataRequest].allowAutoTopicCreation()
    }, responseWithNotControllerError)

    testRequestThread.doWork()

    assertTrue(completionHandler.timedOut.get())
  }

  @Test
  def testUnsupportedVersionHandling(): Unit = {
    val time = new MockTime()
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))
    val controllerId = 2

    val metadata = mock(classOf[Metadata])
    val mockClient = new MockClient(time, metadata)

    val controllerNodeProvider = mock(classOf[ControllerNodeProvider])
    val activeController = new Node(controllerId, "host", 1234)

    when(controllerNodeProvider.get()).thenReturn(Some(activeController))

    val callbackResponse = new AtomicReference[ClientResponse]()
    val completionHandler = new ControllerRequestCompletionHandler {
      override def onTimeout(): Unit = fail("Unexpected timeout exception")
      override def onComplete(response: ClientResponse): Unit = callbackResponse.set(response)
    }

    val queueItem = BrokerToControllerQueueItem(
      time.milliseconds(),
      new MetadataRequest.Builder(new MetadataRequestData()),
      completionHandler
    )

    mockClient.prepareUnsupportedVersionResponse(request => request.apiKey == ApiKeys.METADATA)

    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), controllerNodeProvider,
      config, time, "", retryTimeoutMs = Long.MaxValue)
    testRequestThread.started = true

    testRequestThread.enqueue(queueItem)
    pollUntil(testRequestThread, () => callbackResponse.get != null)
    assertNotNull(callbackResponse.get.versionMismatch)
  }

  @Test
  def testAuthenticationExceptionHandling(): Unit = {
    val time = new MockTime()
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))
    val controllerId = 2

    val metadata = mock(classOf[Metadata])
    val mockClient = new MockClient(time, metadata)

    val controllerNodeProvider = mock(classOf[ControllerNodeProvider])
    val activeController = new Node(controllerId, "host", 1234)

    when(controllerNodeProvider.get()).thenReturn(Some(activeController))

    val callbackResponse = new AtomicReference[ClientResponse]()
    val completionHandler = new ControllerRequestCompletionHandler {
      override def onTimeout(): Unit = fail("Unexpected timeout exception")
      override def onComplete(response: ClientResponse): Unit = callbackResponse.set(response)
    }

    val queueItem = BrokerToControllerQueueItem(
      time.milliseconds(),
      new MetadataRequest.Builder(new MetadataRequestData()),
      completionHandler
    )

    mockClient.createPendingAuthenticationError(activeController, 50)

    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), controllerNodeProvider,
      config, time, "", retryTimeoutMs = Long.MaxValue)
    testRequestThread.started = true

    testRequestThread.enqueue(queueItem)
    pollUntil(testRequestThread, () => callbackResponse.get != null)
    assertNotNull(callbackResponse.get.authenticationException)
  }

  @Test
  def testThreadNotStarted(): Unit = {
    // Make sure we throw if we enqueue anything while the thread is not running
    val time = new MockTime()
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))

    val metadata = mock(classOf[Metadata])
    val mockClient = new MockClient(time, metadata)

    val controllerNodeProvider = mock(classOf[ControllerNodeProvider])

    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), controllerNodeProvider,
      config, time, "", retryTimeoutMs = Long.MaxValue)

    val completionHandler = new TestRequestCompletionHandler(None)
    val queueItem = BrokerToControllerQueueItem(
      time.milliseconds(),
      new MetadataRequest.Builder(new MetadataRequestData()),
      completionHandler
    )

    assertThrows(classOf[IllegalStateException], () => testRequestThread.enqueue(queueItem))
    assertEquals(0, testRequestThread.queueSize)
  }

  private def pollUntil(
    requestThread: BrokerToControllerRequestThread,
    condition: () => Boolean,
    maxRetries: Int = 10
  ): Unit = {
    var tries = 0
    do {
      requestThread.doWork()
      tries += 1
    } while (!condition.apply() && tries < maxRetries)

    if (!condition.apply()) {
      fail(s"Condition failed to be met after polling $tries times")
    }
  }

  class TestRequestCompletionHandler(
    expectedResponse: Option[MetadataResponse] = None
  ) extends ControllerRequestCompletionHandler {
    val completed: AtomicBoolean = new AtomicBoolean(false)
    val timedOut: AtomicBoolean = new AtomicBoolean(false)

    override def onComplete(response: ClientResponse): Unit = {
      expectedResponse.foreach { expected =>
        assertEquals(expected, response.responseBody())
      }
      completed.set(true)
    }

    override def onTimeout(): Unit = {
      timedOut.set(true)
    }
  }
}
