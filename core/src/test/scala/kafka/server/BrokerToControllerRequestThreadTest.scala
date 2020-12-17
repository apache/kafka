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

import java.util.concurrent.{CountDownLatch, LinkedBlockingDeque, TimeUnit}
import java.util.Collections
import kafka.cluster.{Broker, EndPoint}
import kafka.utils.TestUtils
import org.apache.kafka.clients.{ClientResponse, ManualMetadataUpdater, Metadata, MockClient}
import org.apache.kafka.common.feature.Features
import org.apache.kafka.common.feature.Features.emptySupportedFeatures
import org.apache.kafka.common.utils.{MockTime, SystemTime}
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, MetadataRequest, MetadataResponse, RequestTestUtils}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test
import org.mockito.Mockito._

class BrokerToControllerRequestThreadTest {

  @Test
  def testRequestsSent(): Unit = {
    // just a simple test that tests whether the request from 1 -> 2 is sent and the response callback is called
    val time = new SystemTime
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))
    val controllerId = 2

    val metadata = mock(classOf[Metadata])
    val mockClient = new MockClient(time, metadata)

    val requestQueue = new LinkedBlockingDeque[BrokerToControllerQueueItem]()
    val metadataCache = mock(classOf[MetadataCache])
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val activeController = new Broker(controllerId,
      Seq(new EndPoint("host", 1234, listenerName, SecurityProtocol.PLAINTEXT)), None, emptySupportedFeatures)

    when(metadataCache.getControllerId).thenReturn(Some(controllerId))
    when(metadataCache.getAliveBrokers).thenReturn(Seq(activeController))
    when(metadataCache.getAliveBroker(controllerId)).thenReturn(Some(activeController))

    val expectedResponse = RequestTestUtils.metadataUpdateWith(2, Collections.singletonMap("a", 2))
    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), requestQueue, metadataCache,
      config, listenerName, time, "")
    mockClient.prepareResponse(expectedResponse)

    val responseLatch = new CountDownLatch(1)
    val queueItem = BrokerToControllerQueueItem(
      new MetadataRequest.Builder(new MetadataRequestData()),
      new TestRequestCompletionHandler(expectedResponse, responseLatch),
      Long.MaxValue)
    requestQueue.put(queueItem)
    // initialize to the controller
    testRequestThread.doWork()
    // send and process the request
    testRequestThread.doWork()

    assertTrue(responseLatch.await(10, TimeUnit.SECONDS))
  }

  @Test
  def testControllerChanged(): Unit = {
    // in this test the current broker is 1, and the controller changes from 2 -> 3 then back: 3 -> 2
    val time = new SystemTime
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))
    val oldControllerId = 1
    val newControllerId = 2

    val metadata = mock(classOf[Metadata])
    val mockClient = new MockClient(time, metadata)

    val requestQueue = new LinkedBlockingDeque[BrokerToControllerQueueItem]()
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
      requestQueue, metadataCache, config, listenerName, time, "")

    val responseLatch = new CountDownLatch(1)

    val queueItem = BrokerToControllerQueueItem(
      new MetadataRequest.Builder(new MetadataRequestData()),
      new TestRequestCompletionHandler(expectedResponse, responseLatch),
      Long.MaxValue)
    requestQueue.put(queueItem)
    mockClient.prepareResponse(expectedResponse)
    // initialize the thread with oldController
    testRequestThread.doWork()
    // assert queue correctness
    assertFalse(requestQueue.isEmpty)
    assertEquals(1, requestQueue.size())
    assertEquals(queueItem, requestQueue.peek())
    // disconnect the node
    mockClient.setUnreachable(oldControllerNode, time.milliseconds() + 5000)
    // verify that the client closed the connection to the faulty controller
    testRequestThread.doWork()
    assertFalse(requestQueue.isEmpty)
    assertEquals(1, requestQueue.size())
    // should connect to the new controller
    testRequestThread.doWork()
    // should send the request and process the response
    testRequestThread.doWork()

    assertTrue(responseLatch.await(10, TimeUnit.SECONDS))
  }

  @Test
  def testNotController(): Unit = {
    val time = new SystemTime
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))
    val oldControllerId = 1
    val newControllerId = 2

    val metadata = mock(classOf[Metadata])
    val mockClient = new MockClient(time, metadata)

    val requestQueue = new LinkedBlockingDeque[BrokerToControllerQueueItem]()
    val metadataCache = mock(classOf[MetadataCache])
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val oldController = new Broker(oldControllerId,
      Seq(new EndPoint("host1", 1234, listenerName, SecurityProtocol.PLAINTEXT)), None, Features.emptySupportedFeatures)
    val newController = new Broker(2,
      Seq(new EndPoint("host2", 1234, listenerName, SecurityProtocol.PLAINTEXT)), None, Features.emptySupportedFeatures)

    when(metadataCache.getControllerId).thenReturn(Some(oldControllerId), Some(newControllerId))
    when(metadataCache.getAliveBrokers).thenReturn(Seq(oldController, newController))
    when(metadataCache.getAliveBroker(oldControllerId)).thenReturn(Some(oldController))
    when(metadataCache.getAliveBroker(newControllerId)).thenReturn(Some(newController))

    val responseWithNotControllerError = RequestTestUtils.metadataUpdateWith("cluster1", 2,
      Collections.singletonMap("a", Errors.NOT_CONTROLLER),
      Collections.singletonMap("a", 2))
    val expectedResponse = RequestTestUtils.metadataUpdateWith(3, Collections.singletonMap("a", 2))
    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), requestQueue, metadataCache,
      config, listenerName, time, "")

    val responseLatch = new CountDownLatch(1)
    val queueItem = BrokerToControllerQueueItem(
      new MetadataRequest.Builder(new MetadataRequestData()
        .setAllowAutoTopicCreation(true)),
      new TestRequestCompletionHandler(expectedResponse, responseLatch),
      Long.MaxValue)
    requestQueue.put(queueItem)
    // initialize to the controller
    testRequestThread.doWork()
    // send and process the request
    mockClient.prepareResponse((body: AbstractRequest) => {
      body.isInstanceOf[MetadataRequest] &&
      body.asInstanceOf[MetadataRequest].allowAutoTopicCreation()
    }, responseWithNotControllerError)
    testRequestThread.doWork()
    // reinitialize the controller to a different node
    testRequestThread.doWork()
    // process the request again
    mockClient.prepareResponse(expectedResponse)
    testRequestThread.doWork()

    assertTrue(responseLatch.await(10, TimeUnit.SECONDS))
  }

  @Test
  def testRequestTimeout(): Unit = {
    val time = new MockTime()
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))
    val controllerId = 1

    val metadata = mock(classOf[Metadata])
    val mockClient = new MockClient(time, metadata)

    val requestQueue = new LinkedBlockingDeque[BrokerToControllerQueueItem]()
    val metadataCache = mock(classOf[MetadataCache])
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val controller = new Broker(controllerId,
      Seq(new EndPoint("host1", 1234, listenerName, SecurityProtocol.PLAINTEXT)), None, Features.emptySupportedFeatures)

    when(metadataCache.getControllerId).thenReturn(Some(controllerId))
    when(metadataCache.getAliveBrokers).thenReturn(Seq(controller))
    when(metadataCache.getAliveBroker(controllerId)).thenReturn(Some(controller))

    val responseWithNotControllerError = RequestTestUtils.metadataUpdateWith("cluster1", 2,
      Collections.singletonMap("a", Errors.NOT_CONTROLLER),
      Collections.singletonMap("a", 2))
    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), requestQueue, metadataCache,
      config, listenerName, time, "")

    val responseLatch = new CountDownLatch(1)
    val requestTimeout = config.requestTimeoutMs.longValue()
    val queueItem = BrokerToControllerQueueItem(
      new MetadataRequest.Builder(new MetadataRequestData()
        .setAllowAutoTopicCreation(true)), new ControllerRequestCompletionHandler {
        override def onComplete(response: ClientResponse): Unit = {}

        override def onTimeout(): Unit = {
          responseLatch.countDown()
        }
      }, requestTimeout + time.milliseconds())
    requestQueue.put(queueItem)

    // initialize to the controller
    testRequestThread.doWork()
    // send and process the request
    mockClient.prepareResponse((body: AbstractRequest) => {
      // Advance time to timeout the response
      time.sleep(requestTimeout + 1)

      body.isInstanceOf[MetadataRequest] &&
        body.asInstanceOf[MetadataRequest].allowAutoTopicCreation()
    }, responseWithNotControllerError)

    testRequestThread.doWork()

    // The queued item should be timed out, instead of
    // re-enqueue by NOT_CONTROLLER error.
    assertEquals(0, requestQueue.size())

    assertTrue(responseLatch.await(10, TimeUnit.SECONDS))
  }

  class TestRequestCompletionHandler(expectedResponse: MetadataResponse,
                                     responseLatch: CountDownLatch) extends ControllerRequestCompletionHandler {
    override def onComplete(response: ClientResponse): Unit = {
      assertEquals(expectedResponse, response.responseBody())
      responseLatch.countDown()
    }

    override def onTimeout(): Unit = {
    }
  }
}
