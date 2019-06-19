/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.io.IOException
import java.net.{InetAddress, Socket}
import java.util.Properties
import java.util.concurrent._

import kafka.server.{BaseRequestTest, KafkaConfig}
import kafka.utils.{CoreUtils, TestUtils}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.Assertions.intercept

import scala.collection.JavaConverters._

class DynamicConnectionQuotaTest extends BaseRequestTest {

  override def brokerCount = 1

  val topic = "test"
  val listener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
  val localAddress = InetAddress.getByName("127.0.0.1")
  var executor: ExecutorService = _

  @Before
  override def setUp(): Unit = {
    super.setUp()
    TestUtils.createTopic(zkClient, topic, brokerCount, brokerCount, servers)
  }

  @After
  override def tearDown(): Unit = {
    try {
      if (executor != null) {
        executor.shutdownNow()
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS))
      }
    } finally {
      super.tearDown()
    }
  }

  override protected def brokerPropertyOverrides(properties: Properties): Unit = {
    super.brokerPropertyOverrides(properties)
  }

  @Test
  def testDynamicConnectionQuota() {
    val maxConnectionsPerIP = 5

    def connectAndVerify() {
      val socket = connect()
      try {
        sendAndReceive(produceRequest, ApiKeys.PRODUCE, socket)
      } finally {
        socket.close()
      }
    }

    val props = new Properties
    props.put(KafkaConfig.MaxConnectionsPerIpProp, maxConnectionsPerIP.toString)
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.MaxConnectionsPerIpProp, maxConnectionsPerIP.toString))

    verifyMaxConnections(maxConnectionsPerIP, () => connectAndVerify)

    // Increase MaxConnectionsPerIpOverrides for localhost to 7
    val maxConnectionsPerIPOverride = 7
    props.put(KafkaConfig.MaxConnectionsPerIpOverridesProp, s"localhost:$maxConnectionsPerIPOverride")
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.MaxConnectionsPerIpOverridesProp, s"localhost:$maxConnectionsPerIPOverride"))

    verifyMaxConnections(maxConnectionsPerIPOverride, () => connectAndVerify)
  }

  @Test
  def testDynamicListenerConnectionQuota(): Unit = {
    val initialConnectionCount = connectionCount

    def connectAndVerify() {
      val socket = connect("PLAINTEXT")
      socket.setSoTimeout(1000)
      try {
        val response = sendAndReceive(produceRequest, ApiKeys.PRODUCE, socket)
        assertEquals(0, response.remaining)
      } finally {
        socket.close()
      }
    }

    // Reduce total broker MaxConnections to 5 at the cluster level
    val props = new Properties
    props.put(KafkaConfig.MaxConnectionsProp, "5")
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.MaxConnectionsProp, "5"))
    verifyMaxConnections(5, () => connectAndVerify)

    // Create another listener and verify listener connection limit of 5 for each listener
    val newListeners = "PLAINTEXT://localhost:0,INTERNAL://localhost:0"
    props.put(KafkaConfig.ListenersProp, newListeners)
    props.put(KafkaConfig.ListenerSecurityProtocolMapProp, "PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT")
    props.put(KafkaConfig.MaxConnectionsProp, "10")
    props.put("listener.name.internal.max.connections", "5")
    props.put("listener.name.plaintext.max.connections", "5")
    reconfigureServers(props, perBrokerConfig = true, (KafkaConfig.ListenersProp, newListeners))
    waitForListener("INTERNAL")

    var conns = (connectionCount until 5).map(_ => connect("PLAINTEXT"))
    conns ++= (5 until 10).map(_ => connect("INTERNAL"))
    conns.foreach(verifyConnection)
    conns.foreach(_.close())
    TestUtils.waitUntilTrue(() => initialConnectionCount == connectionCount, "Connections not closed")

    // Increase MaxConnections for PLAINTEXT listener to 7 at the broker level
    val maxConnectionsPlaintext = 7
    val listenerProp = s"${listener.configPrefix}${KafkaConfig.MaxConnectionsProp}"
    props.put(listenerProp, maxConnectionsPlaintext.toString)
    reconfigureServers(props, perBrokerConfig = true, (listenerProp, maxConnectionsPlaintext.toString))
    verifyMaxConnections(maxConnectionsPlaintext, () => connectAndVerify)

    // Verify that connection blocked on the limit connects successfully when an existing connection is closed
    val plaintextConnections = (connectionCount until maxConnectionsPlaintext).map(_ => connect("PLAINTEXT"))
    executor = Executors.newSingleThreadExecutor
    val future = executor.submit(CoreUtils.runnable { createAndVerifyConnection() })
    Thread.sleep(100)
    assertFalse(future.isDone)
    plaintextConnections.head.close()
    future.get(30, TimeUnit.SECONDS)
    plaintextConnections.foreach(_.close())
    TestUtils.waitUntilTrue(() => initialConnectionCount == connectionCount, "Connections not closed")

    // Verify that connections on inter-broker listener succeed even if broker max connections has been
    // reached by closing connections on another listener
    var plaintextConns = (connectionCount until 5).map(_ => connect("PLAINTEXT"))
    val internalConns = (5 until 10).map(_ => connect("INTERNAL"))
    plaintextConns.foreach(verifyConnection)
    internalConns.foreach(verifyConnection)
    plaintextConns ++= (0 until 2).map(_ => connect("PLAINTEXT"))
    TestUtils.waitUntilTrue(() => connectionCount <= 10, "Internal connections not closed")
    plaintextConns.foreach(verifyConnection)
    intercept[IOException](internalConns.foreach { socket => sendAndReceive(produceRequest, ApiKeys.PRODUCE, socket) })
    plaintextConns.foreach(_.close())
    internalConns.foreach(_.close())
    TestUtils.waitUntilTrue(() => initialConnectionCount == connectionCount, "Connections not closed")
  }

  private def reconfigureServers(newProps: Properties, perBrokerConfig: Boolean, aPropToVerify: (String, String)): Unit = {
    val initialConnectionCount = connectionCount
    val adminClient = createAdminClient()
    TestUtils.alterConfigs(servers, adminClient, newProps, perBrokerConfig).all.get()
    waitForConfigOnServer(aPropToVerify._1, aPropToVerify._2)
    adminClient.close()
    TestUtils.waitUntilTrue(() => initialConnectionCount == connectionCount, "Admin client connection not closed")
  }

  private def waitForListener(listenerName: String): Unit = {
    TestUtils.retry(maxWaitMs = 10000) {
      try {
        assertTrue(servers.head.socketServer.boundPort(ListenerName.normalised(listenerName)) > 0)
      } catch {
        case e: KafkaException => throw new AssertionError(e)
      }
    }
  }

  private def createAdminClient(): AdminClient = {
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName(securityProtocol.name))
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "10")
    val adminClient = AdminClient.create(config)
    adminClient
  }

  private def waitForConfigOnServer(propName: String, propValue: String, maxWaitMs: Long = 10000): Unit = {
    TestUtils.retry(maxWaitMs) {
      assertEquals(propValue, servers.head.config.originals.get(propName))
    }
  }

  private def produceRequest: ProduceRequest = {
    val topicPartition = new TopicPartition(topic, 0)
    val memoryRecords = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes))
    val partitionRecords = Map(topicPartition -> memoryRecords)
    ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build()
  }

  def connectionCount: Int = servers.head.socketServer.connectionCount(localAddress)

  def connect(listener: String): Socket = {
    val listenerName = ListenerName.normalised(listener)
    new Socket("localhost", servers.head.socketServer.boundPort(listenerName))
  }

  private def createAndVerifyConnection(listener: String = "PLAINTEXT"): Unit = {
    val socket = connect(listener)
    try {
      verifyConnection(socket)
    } finally {
      socket.close()
    }
  }

  private def verifyConnection(socket: Socket): Unit = {
    val request = produceRequest
    val response = sendAndReceive(request, ApiKeys.PRODUCE, socket)
    val produceResponse = ProduceResponse.parse(response, request.version)
    assertEquals(1, produceResponse.responses.size)
    val (_, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(Errors.NONE, partitionResponse.error)
  }

  private def verifyMaxConnections(maxConnections: Int, connectWithFailure: () => Unit): Unit = {
    val initialConnectionCount = connectionCount

    //create connections up to maxConnectionsPerIP - 1, leave space for one connection
    var conns = (connectionCount until (maxConnections - 1)).map(_ => connect("PLAINTEXT"))

    // produce should succeed on a new connection
    createAndVerifyConnection()

    TestUtils.waitUntilTrue(() => connectionCount == (maxConnections - 1), "produce request connection is not closed")
    conns = conns :+ connect("PLAINTEXT")

    // now try one more (should fail)
    intercept[IOException](connectWithFailure.apply())

    //close one connection
    conns.head.close()
    TestUtils.waitUntilTrue(() => connectionCount == (maxConnections - 1), "connection is not closed")
    createAndVerifyConnection()

    conns.foreach(_.close())
    TestUtils.waitUntilTrue(() => initialConnectionCount == connectionCount, "Connections not closed")
  }
}
