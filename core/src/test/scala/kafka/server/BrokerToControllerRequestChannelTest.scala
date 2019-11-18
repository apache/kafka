package kafka.server

import java.util.concurrent.{CountDownLatch, LinkedBlockingDeque, TimeUnit}
import java.util.Collections

import kafka.cluster.{Broker, EndPoint}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.test.{TestUtils => ClientsTestUtils}
import org.apache.kafka.clients.{ManualMetadataUpdater, Metadata, MockClient}
import org.apache.kafka.clients.MockClient.MockMetadataUpdater
import org.apache.kafka.common.utils.SystemTime
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Assert.{assertEquals, assertFalse}
import org.junit.Test
import org.mockito.Mockito._

import scala.collection.JavaConverters._

class BrokerToControllerRequestChannelTest {

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
      Seq(new EndPoint("host", 1234, listenerName, SecurityProtocol.PLAINTEXT)), None)

    when(metadataCache.getControllerId).thenReturn(Some(controllerId))
    when(metadataCache.getAliveBrokers).thenReturn(Seq(activeController))
    when(metadataCache.getAliveBroker(controllerId)).thenReturn(Some(activeController))

    val expectedResponse = ClientsTestUtils.metadataUpdateWith(2, Collections.singletonMap("a", new Integer(2)))
    val testRequestThread = new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(), requestQueue, metadataCache,
      config, listenerName, time, "")
    mockClient.prepareResponse(expectedResponse)

    val responseLatch = new CountDownLatch(1)
    val queueItem = BrokerToControllerQueueItem(
      new MetadataRequest.Builder(new MetadataRequestData()), response => {
        assertEquals(expectedResponse, response)
        responseLatch.countDown()
      })
    requestQueue.put(queueItem)
    testRequestThread.doWork()
    responseLatch.await(10, TimeUnit.SECONDS)
  }

  @Test
  def testControllerChanged(): Unit = {
    // in this test the current broker is 1, and the controller changes from 2 -> 3 then back: 3 -> 2
    val time = new SystemTime
    val config = new KafkaConfig(TestUtils.createBrokerConfig(1, "localhost:2181"))
    val oldControllerId = 2
    val newControllerId = 2

    val metadata = mock(classOf[Metadata])
    val mockClient = spy(new MockClient(time, metadata))

    val requestQueue = new LinkedBlockingDeque[BrokerToControllerQueueItem]()
    val metadataCache = mock(classOf[MetadataCache])
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val oldController = new Broker(oldControllerId,
      Seq(new EndPoint("host1", 1234, listenerName, SecurityProtocol.PLAINTEXT)), None)
    val newController = new Broker(newControllerId,
      Seq(new EndPoint("host2", 1234, listenerName, SecurityProtocol.PLAINTEXT)), None)

    when(metadataCache.getControllerId)
      .thenReturn(Some(oldControllerId))
      .thenReturn(Some(newControllerId))
      .thenReturn(Some(oldControllerId))
    when(metadataCache.getAliveBroker(oldControllerId)).thenReturn(Some(oldController))
    when(metadataCache.getAliveBroker(newControllerId)).thenReturn(Some(newController))

    val expectedResponse = ClientsTestUtils.metadataUpdateWith(3, Collections.singletonMap("a", new Integer(2)))
    val testRequestThread = spy(new BrokerToControllerRequestThread(mockClient, new ManualMetadataUpdater(),
      requestQueue, metadataCache, config, listenerName, time, ""))

    val responseLatch = new CountDownLatch(1)

    val queueItem = BrokerToControllerQueueItem(
      new MetadataRequest.Builder(new MetadataRequestData()), response => {
        assertEquals(expectedResponse, response)
        responseLatch.countDown()
      })
    requestQueue.put(queueItem)

    testRequestThread.doWork()
    // assert queue correctness
    assertFalse(requestQueue.isEmpty)
    assertEquals(1, requestQueue.size())
    assertEquals(queueItem, requestQueue.peek())
    // verify that the client closed the connection to the faulty controller
    verify(mockClient, times(1)).close(oldController.node(listenerName).idString())

    mockClient.prepareResponse(expectedResponse)
    testRequestThread.doWork()

    requestQueue.put(queueItem)

    responseLatch.await(10, TimeUnit.SECONDS)
  }

  @Test
  def testControllerNotReady(): Unit = {
    // just a simple test that tests whether the request from 1 -> 2 is sent and the response callback is called
    val time = new MockTime()
    val brokerConfig = TestUtils.createBrokerConfig(1, "localhost:2181")
    brokerConfig.put(KafkaConfig.RequestTimeoutMsProp, 2000.asInstanceOf[Object])
    val config = new KafkaConfig(brokerConfig)
    val controllerId = 2

    val metadataUpdater = new ManualMetadataUpdater()
    val mockClient = new MockClient(time, mock(classOf[MockMetadataUpdater]))

    val requestQueue = new LinkedBlockingDeque[BrokerToControllerQueueItem]()
    val metadataCache = mock(classOf[MetadataCache])
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val activeController = new Broker(controllerId,
      Seq(new EndPoint("host", 1234, listenerName, SecurityProtocol.PLAINTEXT)), None)

    when(metadataCache.getControllerId).thenReturn(Some(controllerId))
    when(metadataCache.getAliveBrokers).thenReturn(Seq(activeController))
    when(metadataCache.getAliveBroker(controllerId)).thenReturn(Some(activeController))

    val expectedResponse = ClientsTestUtils.metadataUpdateWith(2, Collections.singletonMap("a", new Integer(2)))
    val testRequestThread = new BrokerToControllerRequestThread(mockClient, metadataUpdater, requestQueue, metadataCache,
      config, listenerName, time, "")
    mockClient.prepareResponse(expectedResponse)

    val responseLatch = new CountDownLatch(1)
    val queueItem = BrokerToControllerQueueItem(
      new MetadataRequest.Builder(new MetadataRequestData()), response => {
        assertEquals(expectedResponse, response)
        responseLatch.countDown()
      })
    requestQueue.put(queueItem)

    // controller not ready yet
    mockClient.delayReady(activeController.node(listenerName), 5000)
    testRequestThread.doWork()
    assertEquals(1, requestQueue.size())
    // some time passes and the controller becomes ready
    time.sleep(5100)
    testRequestThread.doWork()
    assertEquals(0, requestQueue.size())
    responseLatch.await(10, TimeUnit.SECONDS)
  }

  @Test
  def testBackoff(): Unit = {
    // just a simple test that tests whether the request from 1 -> 2 is sent and the response callback is called
    val time = new MockTime()
    val brokerConfig = TestUtils.createBrokerConfig(1, "localhost:2181")
    brokerConfig.put(KafkaConfig.RequestTimeoutMsProp, 2000.asInstanceOf[Object])
    val config = new KafkaConfig(brokerConfig)
    val controllerId = 2

    val metadataUpdater = spy(new ManualMetadataUpdater())
    val mockClient = new MockClient(time, mock(classOf[MockMetadataUpdater]))

    val requestQueue = new LinkedBlockingDeque[BrokerToControllerQueueItem]()
    val metadataCache = mock(classOf[MetadataCache])
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val activeController = new Broker(controllerId,
      Seq(new EndPoint("host", 1234, listenerName, SecurityProtocol.PLAINTEXT)), None)

    when(metadataCache.getControllerId)
      .thenReturn(None)
      .thenReturn(Some(controllerId))
    when(metadataCache.getAliveBrokers).thenReturn(Seq(activeController))
    when(metadataCache.getAliveBroker(controllerId)).thenReturn(Some(activeController))

    val expectedResponse = ClientsTestUtils.metadataUpdateWith(2, Collections.singletonMap("a", new Integer(2)))
    val testRequestThread = spy(new BrokerToControllerRequestThread(mockClient, metadataUpdater, requestQueue, metadataCache,
      config, listenerName, time, ""))
    mockClient.prepareResponse(expectedResponse)

    val responseLatch = new CountDownLatch(1)
    val queueItem = BrokerToControllerQueueItem(
      new MetadataRequest.Builder(new MetadataRequestData()), response => {
        assertEquals(expectedResponse, response)
        responseLatch.countDown()
      })
    requestQueue.put(queueItem)

    // controller isn't ready yet
    testRequestThread.doWork()
    assertEquals(1, requestQueue.size())
    // for the second time the metadata cache is updated with the controller but not ready yet
    testRequestThread.doWork()
    assertEquals(1, requestQueue.size())
    // third time's the charm
    testRequestThread.doWork()
    assertEquals(0, requestQueue.size())
    verify(testRequestThread).backoff()
    verify(metadataUpdater).setNodes(Seq(activeController.getNode(listenerName).get).asJava)
    responseLatch.await(10, TimeUnit.SECONDS)
  }
}
