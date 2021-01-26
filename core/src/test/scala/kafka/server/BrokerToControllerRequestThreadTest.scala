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

import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

import kafka.cluster.{Broker, EndPoint}
import kafka.utils.TestUtils
import org.apache.kafka.clients.{ClientResponse, ManualMetadataUpdater, Metadata, MockClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.feature.Features
import org.apache.kafka.common.feature.Features.emptySupportedFeatures
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, MetadataRequest, MetadataResponse, RequestTestUtils}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito._

class BrokerToControllerRequestThreadTest {

  @Test
  def testRetryTimeoutWhileControllerNotAvailable(): Unit = {
    val time = new MockTime()
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))
    val metadata = mock(classOf[Metadata])
    val mockClient = new MockClient(time, metadata)
    val metadataCache = mock(classOf[MetadataCache])
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)

    when(metadataCache.getControllerId).thenReturn(None)

    val retryTimeoutMs = 30000
    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), metadataCache,
      config, listenerName, time, "", retryTimeoutMs)

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

    val metadataCache = mock(classOf[MetadataCache])
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val activeController = new Broker(controllerId,
      Seq(new EndPoint("host", 1234, listenerName, SecurityProtocol.PLAINTEXT)), None, emptySupportedFeatures)

    when(metadataCache.getControllerId).thenReturn(Some(controllerId))
    when(metadataCache.getAliveBrokers).thenReturn(Seq(activeController))
    when(metadataCache.getAliveBroker(controllerId)).thenReturn(Some(activeController))

    val expectedResponse = RequestTestUtils.metadataUpdateWith(2, Collections.singletonMap("a", 2))
    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), metadataCache,
      config, listenerName, time, "", retryTimeoutMs = Long.MaxValue)
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

    val metadataCache = mock(classOf[MetadataCache])
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val oldController = new Broker(oldControllerId,
      Seq(new EndPoint("host1", 1234, listenerName, SecurityProtocol.PLAINTEXT)), None, Features.emptySupportedFeatures)
    val oldControllerNode = oldController.node(listenerName)
    val newController = new Broker(newControllerId,
      Seq(new EndPoint("host2", 1234, listenerName, SecurityProtocol.PLAINTEXT)), None, Features.emptySupportedFeatures)

    when(metadataCache.getControllerId).thenReturn(Some(oldControllerId), Some(newControllerId))
    when(metadataCache.getAliveBroker(oldControllerId)).thenReturn(Some(oldController))
    when(metadataCache.getAliveBroker(newControllerId)).thenReturn(Some(newController))
    when(metadataCache.getAliveBrokers).thenReturn(Seq(oldController, newController))

    val expectedResponse = RequestTestUtils.metadataUpdateWith(3, Collections.singletonMap("a", 2))
    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(),
      metadataCache, config, listenerName, time, "", retryTimeoutMs = Long.MaxValue)

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
    mockClient.setUnreachable(oldControllerNode, time.milliseconds() + 5000)
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

    val metadataCache = mock(classOf[MetadataCache])
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val port = 1234
    val oldController = new Broker(oldControllerId,
      Seq(new EndPoint("host1", port, listenerName, SecurityProtocol.PLAINTEXT)), None, Features.emptySupportedFeatures)
    val newController = new Broker(2,
      Seq(new EndPoint("host2", port, listenerName, SecurityProtocol.PLAINTEXT)), None, Features.emptySupportedFeatures)

    when(metadataCache.getControllerId).thenReturn(Some(oldControllerId), Some(newControllerId))
    when(metadataCache.getAliveBrokers).thenReturn(Seq(oldController, newController))
    when(metadataCache.getAliveBroker(oldControllerId)).thenReturn(Some(oldController))
    when(metadataCache.getAliveBroker(newControllerId)).thenReturn(Some(newController))

    val responseWithNotControllerError = RequestTestUtils.metadataUpdateWith("cluster1", 2,
      Collections.singletonMap("a", Errors.NOT_CONTROLLER),
      Collections.singletonMap("a", 2))
    val expectedResponse = RequestTestUtils.metadataUpdateWith(3, Collections.singletonMap("a", 2))
    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), metadataCache,
      config, listenerName, time, "", retryTimeoutMs = Long.MaxValue)

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
  def testRetryTimeout(): Unit = {
    val time = new MockTime()
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))
    val controllerId = 1

    val metadata = mock(classOf[Metadata])
    val mockClient = new MockClient(time, metadata)

    val metadataCache = mock(classOf[MetadataCache])
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val controller = new Broker(controllerId,
      Seq(new EndPoint("host1", 1234, listenerName, SecurityProtocol.PLAINTEXT)), None, Features.emptySupportedFeatures)

    when(metadataCache.getControllerId).thenReturn(Some(controllerId))
    when(metadataCache.getAliveBrokers).thenReturn(Seq(controller))
    when(metadataCache.getAliveBroker(controllerId)).thenReturn(Some(controller))

    val retryTimeoutMs = 30000
    val responseWithNotControllerError = RequestTestUtils.metadataUpdateWith("cluster1", 2,
      Collections.singletonMap("a", Errors.NOT_CONTROLLER),
      Collections.singletonMap("a", 2))
    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), metadataCache,
      config, listenerName, time, "", retryTimeoutMs)

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
