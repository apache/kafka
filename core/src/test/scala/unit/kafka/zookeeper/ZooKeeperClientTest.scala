/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.zookeeper

import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentLinkedQueue, CountDownLatch, Executors, Semaphore, TimeUnit}
import scala.collection.Seq
import com.yammer.metrics.core.{Gauge, Meter, MetricName}
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.server.QuorumTestHarness
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.config.ZkConfigs
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.zookeeper.KeeperException.{Code, NoNodeException}
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper.client.ZKClientConfig
import org.apache.zookeeper.{CreateMode, WatchedEvent, ZooDefs}
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertFalse, assertThrows, assertTrue, fail}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo, Timeout}

import scala.jdk.CollectionConverters._

class ZooKeeperClientTest extends QuorumTestHarness {
  private val mockPath = "/foo"
  private val time = Time.SYSTEM

  private var zooKeeperClient: ZooKeeperClient = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    TestUtils.verifyNoUnexpectedThreads("@BeforeEach")
    cleanMetricsRegistry()
    super.setUp(testInfo)
    zooKeeperClient = newZooKeeperClient()
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (zooKeeperClient != null)
      zooKeeperClient.close()
    super.tearDown()
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    TestUtils.verifyNoUnexpectedThreads("@AfterEach")
  }

  @Test
  def testUnresolvableConnectString(): Unit = {
    try {
      newZooKeeperClient("some.invalid.hostname.foo.bar.local", connectionTimeoutMs = 10)
    } catch {
      case e: ZooKeeperClientTimeoutException =>
        assertEquals(Set.empty, runningZkSendThreads,  "ZooKeeper client threads still running")
    }
  }

  private def runningZkSendThreads: collection.Set[String] = Thread.getAllStackTraces.keySet.asScala
    .filter(_.isAlive)
    .map(_.getName)
    .filter(t => t.contains("SendThread()"))

  @Test
  def testConnectionTimeout(): Unit = {
    zookeeper.shutdown()
    assertThrows(classOf[ZooKeeperClientTimeoutException], () => newZooKeeperClient(
      connectionTimeoutMs = 10).close())
  }

  @Test
  def testConnection(): Unit = {
    val client = newZooKeeperClient()
    try {
      // Verify ZooKeeper event thread name. This is used in QuorumTestHarness to verify that tests have closed ZK clients
      val threads = Thread.getAllStackTraces.keySet.asScala.map(_.getName)
      assertTrue(threads.exists(_.contains(QuorumTestHarness.ZkClientEventThreadSuffix)),
        s"ZooKeeperClient event thread not found, threads=$threads")
    } finally {
      client.close()
    }
  }

  @Test
  def testConnectionViaNettyClient(): Unit = {
    // Confirm that we can explicitly set client connection configuration, which is necessary for TLS.
    // TLS connectivity itself is tested in system tests rather than here to avoid having to add TLS support
    // to kafka.zk.EmbeddedZookeeper
    val clientConfig = new ZKClientConfig()
    val propKey = ZkConfigs.ZK_CLIENT_CNXN_SOCKET_CONFIG
    val propVal = "org.apache.zookeeper.ClientCnxnSocketNetty"
    KafkaConfig.setZooKeeperClientProperty(clientConfig, propKey, propVal)
    val client = newZooKeeperClient(clientConfig = clientConfig)
    try {
      assertEquals(Some(propVal), KafkaConfig.zooKeeperClientProperty(client.clientConfig, propKey))
      // For a sanity check, make sure a bad client connection socket class name generates an exception
      val badClientConfig = new ZKClientConfig()
      KafkaConfig.setZooKeeperClientProperty(badClientConfig, propKey, propVal + "BadClassName")
      assertThrows(classOf[Exception], () => newZooKeeperClient(clientConfig = badClientConfig))
    } finally {
      client.close()
    }
  }

  @Test
  def testDeleteNonExistentZNode(): Unit = {
    val deleteResponse = zooKeeperClient.handleRequest(DeleteRequest(mockPath, -1))
    assertEquals(Code.NONODE, deleteResponse.resultCode, "Response code should be NONODE")
    assertThrows(classOf[NoNodeException], () => deleteResponse.maybeThrow())
  }

  @Test
  def testDeleteExistingZNode(): Unit = {
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals(Code.OK, createResponse.resultCode, "Response code for create should be OK")
    val deleteResponse = zooKeeperClient.handleRequest(DeleteRequest(mockPath, -1))
    assertEquals(Code.OK, deleteResponse.resultCode, "Response code for delete should be OK")
  }

  @Test
  def testExistsNonExistentZNode(): Unit = {
    val existsResponse = zooKeeperClient.handleRequest(ExistsRequest(mockPath))
    assertEquals(Code.NONODE, existsResponse.resultCode, "Response code should be NONODE")
  }

  @Test
  def testExistsExistingZNode(): Unit = {
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals(Code.OK, createResponse.resultCode, "Response code for create should be OK")
    val existsResponse = zooKeeperClient.handleRequest(ExistsRequest(mockPath))
    assertEquals(Code.OK, existsResponse.resultCode, "Response code for exists should be OK")
  }

  @Test
  def testGetDataNonExistentZNode(): Unit = {
    val getDataResponse = zooKeeperClient.handleRequest(GetDataRequest(mockPath))
    assertEquals(Code.NONODE, getDataResponse.resultCode, "Response code should be NONODE")
  }

  @Test
  def testGetDataExistingZNode(): Unit = {
    val data = bytes
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala,
      CreateMode.PERSISTENT))
    assertEquals(Code.OK, createResponse.resultCode, "Response code for create should be OK")
    val getDataResponse = zooKeeperClient.handleRequest(GetDataRequest(mockPath))
    assertEquals(Code.OK, getDataResponse.resultCode, "Response code for getData should be OK")
    assertArrayEquals(data, getDataResponse.data, "Data for getData should match created znode data")
  }

  @Test
  def testSetDataNonExistentZNode(): Unit = {
    val setDataResponse = zooKeeperClient.handleRequest(SetDataRequest(mockPath, Array.empty[Byte], -1))
    assertEquals(Code.NONODE, setDataResponse.resultCode, "Response code should be NONODE")
  }

  @Test
  def testSetDataExistingZNode(): Unit = {
    val data = bytes
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals(Code.OK, createResponse.resultCode, "Response code for create should be OK")
    val setDataResponse = zooKeeperClient.handleRequest(SetDataRequest(mockPath, data, -1))
    assertEquals(Code.OK, setDataResponse.resultCode, "Response code for setData should be OK")
    val getDataResponse = zooKeeperClient.handleRequest(GetDataRequest(mockPath))
    assertEquals(Code.OK, getDataResponse.resultCode, "Response code for getData should be OK")
    assertArrayEquals(data, getDataResponse.data, "Data for getData should match setData's data")
  }

  @Test
  def testGetAclNonExistentZNode(): Unit = {
    val getAclResponse = zooKeeperClient.handleRequest(GetAclRequest(mockPath))
    assertEquals(Code.NONODE, getAclResponse.resultCode, "Response code should be NONODE")
  }

  @Test
  def testGetAclExistingZNode(): Unit = {
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals(Code.OK, createResponse.resultCode, "Response code for create should be OK")
    val getAclResponse = zooKeeperClient.handleRequest(GetAclRequest(mockPath))
    assertEquals(Code.OK, getAclResponse.resultCode, "Response code for getAcl should be OK")
    assertEquals(ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, getAclResponse.acl, "ACL should be " + ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala)
  }

  @Test
  def testSetAclNonExistentZNode(): Unit = {
    val setAclResponse = zooKeeperClient.handleRequest(SetAclRequest(mockPath, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, -1))
    assertEquals(Code.NONODE, setAclResponse.resultCode, "Response code should be NONODE")
  }

  @Test
  def testGetChildrenNonExistentZNode(): Unit = {
    val getChildrenResponse = zooKeeperClient.handleRequest(GetChildrenRequest(mockPath, registerWatch = true))
    assertEquals(Code.NONODE, getChildrenResponse.resultCode, "Response code should be NONODE")
  }

  @Test
  def testGetChildrenExistingZNode(): Unit = {
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals(Code.OK, createResponse.resultCode, "Response code for create should be OK")
    val getChildrenResponse = zooKeeperClient.handleRequest(GetChildrenRequest(mockPath, registerWatch = true))
    assertEquals(Code.OK, getChildrenResponse.resultCode, "Response code for getChildren should be OK")
    assertEquals(Seq.empty[String], getChildrenResponse.children, "getChildren should return no children")
  }

  @Test
  def testGetChildrenExistingZNodeWithChildren(): Unit = {
    val child1 = "child1"
    val child2 = "child2"
    val child1Path = mockPath + "/" + child1
    val child2Path = mockPath + "/" + child2
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals(Code.OK, createResponse.resultCode, "Response code for create should be OK")
    val createResponseChild1 = zooKeeperClient.handleRequest(CreateRequest(child1Path, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals(Code.OK, createResponseChild1.resultCode, "Response code for create child1 should be OK")
    val createResponseChild2 = zooKeeperClient.handleRequest(CreateRequest(child2Path, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals(Code.OK, createResponseChild2.resultCode, "Response code for create child2 should be OK")

    val getChildrenResponse = zooKeeperClient.handleRequest(GetChildrenRequest(mockPath, registerWatch = true))
    assertEquals(Code.OK, getChildrenResponse.resultCode, "Response code for getChildren should be OK")
    assertEquals(Seq(child1, child2), getChildrenResponse.children.sorted, "getChildren should return two children")
  }

  @Test
  def testPipelinedGetData(): Unit = {
    val createRequests = (1 to 3).map(x => CreateRequest("/" + x, (x * 2).toString.getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    val createResponses = createRequests.map(zooKeeperClient.handleRequest)
    createResponses.foreach(createResponse => assertEquals(Code.OK, createResponse.resultCode, "Response code for create should be OK"))
    val getDataRequests = (1 to 3).map(x => GetDataRequest("/" + x))
    val getDataResponses = zooKeeperClient.handleRequests(getDataRequests)
    getDataResponses.foreach(getDataResponse => assertEquals(Code.OK, getDataResponse.resultCode,
      "Response code for getData should be OK"))
    getDataResponses.zipWithIndex.foreach { case (getDataResponse, i) =>
      assertEquals(Code.OK, getDataResponse.resultCode, "Response code for getData should be OK")
      assertEquals((i + 1) * 2, Integer.valueOf(new String(getDataResponse.data)), "Data for getData should match")
    }
  }

  @Test
  def testMixedPipeline(): Unit = {
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals(Code.OK, createResponse.resultCode, "Response code for create should be OK")
    val getDataRequest = GetDataRequest(mockPath)
    val setDataRequest = SetDataRequest("/nonexistent", Array.empty[Byte], -1)
    val responses = zooKeeperClient.handleRequests(Seq(getDataRequest, setDataRequest))
    assertEquals(Code.OK, responses.head.resultCode, "Response code for getData should be OK")
    assertArrayEquals(Array.empty[Byte], responses.head.asInstanceOf[GetDataResponse].data, "Data for getData should be empty")
    assertEquals(Code.NONODE, responses.last.resultCode, "Response code for setData should be NONODE")
  }

  @Test
  def testZNodeChangeHandlerForCreation(): Unit = {
    val znodeChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChangeHandler = new ZNodeChangeHandler {
      override def handleCreation(): Unit = {
        znodeChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    zooKeeperClient.registerZNodeChangeHandler(zNodeChangeHandler)
    val existsRequest = ExistsRequest(mockPath)
    val createRequest = CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT)
    val responses = zooKeeperClient.handleRequests(Seq(existsRequest, createRequest))
    assertEquals(Code.NONODE, responses.head.resultCode, "Response code for exists should be NONODE")
    assertEquals(Code.OK, responses.last.resultCode, "Response code for create should be OK")
    assertTrue(znodeChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS), "Failed to receive create notification")
  }

  @Test
  def testZNodeChangeHandlerForDeletion(): Unit = {
    val znodeChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChangeHandler = new ZNodeChangeHandler {
      override def handleDeletion(): Unit = {
        znodeChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    zooKeeperClient.registerZNodeChangeHandler(zNodeChangeHandler)
    val existsRequest = ExistsRequest(mockPath)
    val createRequest = CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT)
    val responses = zooKeeperClient.handleRequests(Seq(createRequest, existsRequest))
    assertEquals(Code.OK, responses.last.resultCode, "Response code for create should be OK")
    assertEquals(Code.OK, responses.head.resultCode, "Response code for exists should be OK")
    val deleteResponse = zooKeeperClient.handleRequest(DeleteRequest(mockPath, -1))
    assertEquals(Code.OK, deleteResponse.resultCode, "Response code for delete should be OK")
    assertTrue(znodeChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS), "Failed to receive delete notification")
  }

  @Test
  def testZNodeChangeHandlerForDataChange(): Unit = {
    val znodeChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChangeHandler = new ZNodeChangeHandler {
      override def handleDataChange(): Unit = {
        znodeChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    zooKeeperClient.registerZNodeChangeHandler(zNodeChangeHandler)
    val existsRequest = ExistsRequest(mockPath)
    val createRequest = CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT)
    val responses = zooKeeperClient.handleRequests(Seq(createRequest, existsRequest))
    assertEquals(Code.OK, responses.last.resultCode, "Response code for create should be OK")
    assertEquals(Code.OK, responses.head.resultCode, "Response code for exists should be OK")
    val setDataResponse = zooKeeperClient.handleRequest(SetDataRequest(mockPath, Array.empty[Byte], -1))
    assertEquals(Code.OK, setDataResponse.resultCode, "Response code for setData should be OK")
    assertTrue(znodeChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS), "Failed to receive data change notification")
  }

  @Test
  @Timeout(60)
  def testBlockOnRequestCompletionFromStateChangeHandler(): Unit = {
    // This tests the scenario exposed by KAFKA-6879 in which the expiration callback awaits
    // completion of a request which is handled by another thread

    val latch = new CountDownLatch(1)
    val stateChangeHandler = new StateChangeHandler {
      override val name = this.getClass.getName
      override def beforeInitializingSession(): Unit = {
        latch.await()
      }
    }

    zooKeeperClient.close()
    zooKeeperClient = newZooKeeperClient()
    zooKeeperClient.registerStateChangeHandler(stateChangeHandler)

    val requestThread = new Thread() {
      override def run(): Unit = {
        try
          zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
            ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
        finally
          latch.countDown()
      }
    }

    val reinitializeThread = new Thread() {
      override def run(): Unit = {
        zooKeeperClient.forceReinitialize()
      }
    }

    reinitializeThread.start()

    // sleep briefly before starting the request thread so that the initialization
    // thread is blocking on the latch
    Thread.sleep(100)
    requestThread.start()

    reinitializeThread.join()
    requestThread.join()
  }

  @Test
  def testExceptionInBeforeInitializingSession(): Unit = {
    val faultyHandler = new StateChangeHandler {
      override val name = this.getClass.getName
      override def beforeInitializingSession(): Unit = {
        throw new RuntimeException()
      }
    }

    val goodCalls = new AtomicInteger(0)
    val goodHandler = new StateChangeHandler {
      override val name = this.getClass.getName
      override def beforeInitializingSession(): Unit = {
        goodCalls.incrementAndGet()
      }
    }

    zooKeeperClient.close()
    zooKeeperClient = newZooKeeperClient()
    zooKeeperClient.registerStateChangeHandler(faultyHandler)
    zooKeeperClient.registerStateChangeHandler(goodHandler)

    zooKeeperClient.forceReinitialize()

    assertEquals(1, goodCalls.get)

    // Client should be usable even if the callback throws an error
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals(Code.OK, createResponse.resultCode, "Response code for create should be OK")
  }

  @Test
  def testZNodeChildChangeHandlerForChildChange(): Unit = {
    val zNodeChildChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChildChangeHandler = new ZNodeChildChangeHandler {
      override def handleChildChange(): Unit = {
        zNodeChildChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    val child1 = "child1"
    val child1Path = mockPath + "/" + child1
    val createResponse = zooKeeperClient.handleRequest(
      CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals(Code.OK, createResponse.resultCode, "Response code for create should be OK")
    zooKeeperClient.registerZNodeChildChangeHandler(zNodeChildChangeHandler)
    val getChildrenResponse = zooKeeperClient.handleRequest(GetChildrenRequest(mockPath, registerWatch = true))
    assertEquals(Code.OK, getChildrenResponse.resultCode, "Response code for getChildren should be OK")
    val createResponseChild1 = zooKeeperClient.handleRequest(
      CreateRequest(child1Path, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals(Code.OK, createResponseChild1.resultCode, "Response code for create child1 should be OK")
    assertTrue(zNodeChildChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS),
      "Failed to receive child change notification")
  }

  @Test
  def testZNodeChildChangeHandlerForChildChangeNotTriggered(): Unit = {
    val zNodeChildChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChildChangeHandler = new ZNodeChildChangeHandler {
      override def handleChildChange(): Unit = {
        zNodeChildChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    val child1 = "child1"
    val child1Path = mockPath + "/" + child1
    val createResponse = zooKeeperClient.handleRequest(
      CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals(Code.OK, createResponse.resultCode, "Response code for create should be OK")
    zooKeeperClient.registerZNodeChildChangeHandler(zNodeChildChangeHandler)
    val getChildrenResponse = zooKeeperClient.handleRequest(GetChildrenRequest(mockPath, registerWatch = false))
    assertEquals(Code.OK, getChildrenResponse.resultCode, "Response code for getChildren should be OK")
    val createResponseChild1 = zooKeeperClient.handleRequest(
      CreateRequest(child1Path, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals(Code.OK, createResponseChild1.resultCode, "Response code for create child1 should be OK")
    assertFalse(zNodeChildChangeHandlerCountDownLatch.await(100, TimeUnit.MILLISECONDS),
      "Child change notification received")
  }

  @Test
  def testStateChangeHandlerForAuthFailure(): Unit = {
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, "no-such-file-exists.conf")
    val stateChangeHandlerCountDownLatch = new CountDownLatch(1)
    val stateChangeHandler = new StateChangeHandler {
      override val name: String =  this.getClass.getName

      override def onAuthFailure(): Unit = {
        stateChangeHandlerCountDownLatch.countDown()
      }
    }

    val zooKeeperClient = newZooKeeperClient()
    try {
      zooKeeperClient.registerStateChangeHandler(stateChangeHandler)
      zooKeeperClient.forceReinitialize()

      assertTrue(stateChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS), "Failed to receive auth failed notification")
    } finally zooKeeperClient.close()
  }

  @Test
  def testConnectionLossRequestTermination(): Unit = {
    val batchSize = 10
    val zooKeeperClient = newZooKeeperClient(maxInFlight = 2)
    zookeeper.shutdown()
    try {
      val requests = (1 to batchSize).map(i => GetDataRequest(s"/$i"))
      val countDownLatch = new CountDownLatch(1)
      val running = new AtomicBoolean(true)
      val unexpectedResponses = new ArrayBlockingQueue[GetDataResponse](batchSize)
      val requestThread = new Thread {
        override def run(): Unit = {
          while (running.get()) {
            val responses = zooKeeperClient.handleRequests(requests)
            val suffix = responses.dropWhile(response => response.resultCode != Code.CONNECTIONLOSS)
            if (!suffix.forall(response => response.resultCode == Code.CONNECTIONLOSS))
              responses.foreach(unexpectedResponses.add)
            if (!unexpectedResponses.isEmpty || suffix.nonEmpty)
              running.set(false)
          }
          countDownLatch.countDown()
        }
      }
      requestThread.start()
      val requestThreadTerminated = countDownLatch.await(30, TimeUnit.SECONDS)
      if (!requestThreadTerminated) {
        running.set(false)
        requestThread.join(5000)
        fail("Failed to receive a CONNECTIONLOSS response code after zookeeper has shutdown.")
      } else if (!unexpectedResponses.isEmpty) {
        fail(s"Received an unexpected non-CONNECTIONLOSS response code after a CONNECTIONLOSS response code from a single batch: $unexpectedResponses")
      }
    } finally zooKeeperClient.close()
  }

  /**
    * Tests that if session expiry notification is received while a thread is processing requests,
    * session expiry is handled and the request thread completes with responses to all requests,
    * even though some requests may fail due to session expiry or disconnection.
    *
    * Sequence of events on different threads:
    *   Request thread:
    *       - Sends `maxInflightRequests` requests (these may complete before session is expired)
    *   Main thread:
    *       - Waits for at least one request to be processed (this should succeed)
    *       - Expires session by creating new client with same session id
    *       - Unblocks another `maxInflightRequests` requests before and after new client is closed (these may fail)
    *   ZooKeeperClient Event thread:
    *       - Delivers responses and session expiry (no ordering guarantee between these, both are processed asynchronously)
    *   Response executor thread:
    *       - Blocks subsequent sends by delaying response until session expiry is processed
    *   ZooKeeperClient Session Expiry Handler:
    *       - Unblocks subsequent sends
    *   Main thread:
    *       - Waits for all sends to complete. The requests sent after session expiry processing should succeed.
    */
  @Test
  def testSessionExpiry(): Unit = {
    val maxInflightRequests = 2
    val responseExecutor = Executors.newSingleThreadExecutor
    val sendSemaphore = new Semaphore(0)
    val sendCompleteSemaphore = new Semaphore(0)
    val sendSize = maxInflightRequests * 5
    @volatile var resultCodes: Seq[Code] = null
    val stateChanges = new ConcurrentLinkedQueue[String]()
    val zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, maxInflightRequests,
      time, "testGroupType", "testGroupName", new ZKClientConfig, "ZooKeeperClientTest") {
      override def send[Req <: AsyncRequest](request: Req)(processResponse: Req#Response => Unit): Unit = {
        super.send(request)( response => {
          responseExecutor.submit(new Runnable {
            override def run(): Unit = {
              sendCompleteSemaphore.release()
              sendSemaphore.acquire()
              processResponse(response)
            }
          })
        })
      }
    }
    try {
      zooKeeperClient.registerStateChangeHandler(new StateChangeHandler {
        override val name: String ="test-state-change-handler"
        override def afterInitializingSession(): Unit = {
          verifyHandlerThread()
          stateChanges.add("afterInitializingSession")
        }
        override def beforeInitializingSession(): Unit = {
          verifyHandlerThread()
          stateChanges.add("beforeInitializingSession")
          sendSemaphore.release(sendSize) // Resume remaining sends
        }
        private def verifyHandlerThread(): Unit = {
          val threadName = Thread.currentThread.getName
          assertTrue(threadName.startsWith(zooKeeperClient.reinitializeScheduler.threadNamePrefix), s"Unexpected thread + $threadName")
        }
      })

      val requestThread = new Thread {
        override def run(): Unit = {
          val requests = (1 to sendSize).map(i => GetDataRequest(s"/$i"))
          resultCodes = zooKeeperClient.handleRequests(requests).map(_.resultCode)
        }
      }
      requestThread.start()
      sendCompleteSemaphore.acquire() // Wait for request thread to start processing requests

      val anotherZkClient = createZooKeeperClientToTriggerSessionExpiry(zooKeeperClient.currentZooKeeper)
      sendSemaphore.release(maxInflightRequests) // Resume a few more sends which may fail
      anotherZkClient.close()
      sendSemaphore.release(maxInflightRequests) // Resume a few more sends which may fail

      requestThread.join(10000)
      if (requestThread.isAlive) {
        requestThread.interrupt()
        fail("Request thread did not complete")
      }
      assertEquals(Seq("beforeInitializingSession", "afterInitializingSession"), stateChanges.asScala.toSeq)

      assertEquals(resultCodes.size, sendSize)
      val connectionLostCount = resultCodes.count(_ == Code.CONNECTIONLOSS)
      assertTrue(connectionLostCount <= maxInflightRequests, s"Unexpected connection lost requests $resultCodes")
      val expiredCount = resultCodes.count(_ == Code.SESSIONEXPIRED)
      assertTrue(expiredCount <= maxInflightRequests, s"Unexpected session expired requests $resultCodes")
      assertTrue(connectionLostCount + expiredCount > 0, s"No connection lost or expired requests $resultCodes")
      assertEquals(Code.NONODE, resultCodes.head)
      assertEquals(Code.NONODE, resultCodes.last)
      assertTrue(resultCodes.forall(Set(Code.NONODE, Code.SESSIONEXPIRED, Code.CONNECTIONLOSS).contains),
        s"Unexpected result code $resultCodes")

    } finally {
      zooKeeperClient.close()
      responseExecutor.shutdownNow()
    }
    assertFalse(zooKeeperClient.reinitializeScheduler.isStarted, "Expiry executor not shutdown")
  }

  @Test
  def testSessionExpiryDuringClose(): Unit = {
    val semaphore = new Semaphore(0)
    val closeExecutor = Executors.newSingleThreadExecutor
    try {
      zooKeeperClient.reinitializeScheduler.scheduleOnce("test", () => semaphore.acquireUninterruptibly())
      zooKeeperClient.scheduleReinitialize("session-expired", "Session expired.", delayMs = 0L)
      val closeFuture = closeExecutor.submit(new Runnable {
        override def run(): Unit = {
          zooKeeperClient.close()
        }
      })
      assertFalse(closeFuture.isDone, "Close completed without shutting down expiry scheduler gracefully")
      assertTrue(zooKeeperClient.currentZooKeeper.getState.isAlive) // Client should be closed after expiry handler
      semaphore.release()
      closeFuture.get(10, TimeUnit.SECONDS)
      assertFalse(zooKeeperClient.reinitializeScheduler.isStarted, "Expiry executor not shutdown")
    } finally {
      closeExecutor.shutdownNow()
    }
  }

  @Test
  def testReinitializeAfterAuthFailure(): Unit = {
    val sessionInitializedCountDownLatch = new CountDownLatch(1)
    val changeHandler = new StateChangeHandler {
      override val name = this.getClass.getName
      override def beforeInitializingSession(): Unit = {
        sessionInitializedCountDownLatch.countDown()
      }
    }

    zooKeeperClient.close()
    @volatile var connectionStateOverride: Option[States] = None
    zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout,
      zkMaxInFlightRequests, time, "testMetricGroup", "testMetricType", new ZKClientConfig, "ZooKeeperClientTest") {
      override def connectionState: States = connectionStateOverride.getOrElse(super.connectionState)
    }
    zooKeeperClient.registerStateChangeHandler(changeHandler)

    connectionStateOverride = Some(States.CONNECTED)
    zooKeeperClient.ZooKeeperClientWatcher.process(new WatchedEvent(EventType.None, KeeperState.AuthFailed, null))
    assertFalse(sessionInitializedCountDownLatch.await(1200, TimeUnit.MILLISECONDS), "Unexpected session initialization when connection is alive")

    connectionStateOverride = Some(States.AUTH_FAILED)
    zooKeeperClient.ZooKeeperClientWatcher.process(new WatchedEvent(EventType.None, KeeperState.AuthFailed, null))
    assertTrue(sessionInitializedCountDownLatch.await(5, TimeUnit.SECONDS), "Failed to receive session initializing notification")
  }

  def isExpectedMetricName(metricName: MetricName, name: String): Boolean =
    metricName.getName == name && metricName.getGroup == "testMetricGroup" && metricName.getType == "testMetricType"

  @Test
  def testZooKeeperStateChangeRateMetrics(): Unit = {
    def checkMeterCount(name: String, expected: Long): Unit = {
      val meter = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.collectFirst {
        case (metricName, meter: Meter) if isExpectedMetricName(metricName, name) => meter
      }.getOrElse(sys.error(s"Unable to find meter with name $name"))
      assertEquals(expected, meter.count, s"Unexpected meter count for $name")
    }

    val expiresPerSecName = "ZooKeeperExpiresPerSec"
    val disconnectsPerSecName = "ZooKeeperDisconnectsPerSec"
    checkMeterCount(expiresPerSecName, 0)
    checkMeterCount(disconnectsPerSecName, 0)

    zooKeeperClient.ZooKeeperClientWatcher.process(new WatchedEvent(EventType.None, KeeperState.Expired, null))
    checkMeterCount(expiresPerSecName, 1)
    checkMeterCount(disconnectsPerSecName, 0)

    zooKeeperClient.ZooKeeperClientWatcher.process(new WatchedEvent(EventType.None, KeeperState.Disconnected, null))
    checkMeterCount(expiresPerSecName, 1)
    checkMeterCount(disconnectsPerSecName, 1)
  }

  @Test
  def testZooKeeperSessionStateMetric(): Unit = {
    def gaugeValue(name: String): Option[String] = {
      KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.collectFirst {
        case (metricName, gauge: Gauge[_]) if isExpectedMetricName(metricName, name) => gauge.value.asInstanceOf[String]
      }
    }

    assertEquals(Some(States.CONNECTED.toString), gaugeValue("SessionState"))
    assertEquals(States.CONNECTED, zooKeeperClient.connectionState)

    zooKeeperClient.close()

    assertEquals(None, gaugeValue("SessionState"))
    assertEquals(States.CLOSED, zooKeeperClient.connectionState)
  }

  private def newZooKeeperClient(connectionString: String = zkConnect,
                                 connectionTimeoutMs: Int = zkConnectionTimeout,
                                 maxInFlight: Int = zkMaxInFlightRequests,
                                 clientConfig: ZKClientConfig = new ZKClientConfig) =
    new ZooKeeperClient(connectionString, zkSessionTimeout, connectionTimeoutMs, maxInFlight, time,
      "testMetricGroup", "testMetricType", clientConfig, "ZooKeeperClientTest")

  private def cleanMetricsRegistry(): Unit = {
    val metrics = KafkaYammerMetrics.defaultRegistry
    metrics.allMetrics.keySet.forEach(m => metrics.removeMetric(m))
  }

  private def bytes = UUID.randomUUID().toString.getBytes(StandardCharsets.UTF_8)
}
