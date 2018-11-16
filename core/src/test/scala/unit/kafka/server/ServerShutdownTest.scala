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
package kafka.server

import java.io.{DataInputStream, File}
import java.net.{ServerSocket, UnknownHostException}
import java.util.concurrent.{Executors, TimeUnit}

import kafka.zk.ZooKeeperTestHarness
import kafka.consumer.SimpleConsumer
import kafka.utils.{CoreUtils, TestUtils}
import kafka.utils.TestUtils._
import kafka.api.FetchRequestBuilder
import kafka.message.ByteBufferMessageSet
import kafka.cluster.Broker
import kafka.controller.{ControllerChannelManager, ControllerContext, StateChangeLogger}
import kafka.log.LogManager
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.LeaderAndIsrRequest
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}
import org.apache.kafka.common.utils.Time
import org.junit.{Before, Test}
import org.junit.Assert._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class ServerShutdownTest extends ZooKeeperTestHarness {
  var config: KafkaConfig = null
  val host = "localhost"
  val topic = "test"
  val sent1 = List("hello", "there")
  val sent2 = List("more", "messages")

  @Before
  override def setUp() {
    super.setUp()
    val props = TestUtils.createBrokerConfig(0, zkConnect)
    config = KafkaConfig.fromProps(props)
  }

  @Test
  def testCleanShutdown() {

    def createProducer(server: KafkaServer): KafkaProducer[Integer, String] =
      TestUtils.createNewProducer(
        TestUtils.getBrokerListStrFromServers(Seq(server)),
        retries = 5,
        keySerializer = new IntegerSerializer,
        valueSerializer = new StringSerializer
      )

    var server = new KafkaServer(config, threadNamePrefix = Option(this.getClass.getName))
    server.startup()
    var producer = createProducer(server)

    // create topic
    createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 1, servers = Seq(server))

    // send some messages
    sent1.map(value => producer.send(new ProducerRecord(topic, 0, value))).foreach(_.get)

    // do a clean shutdown and check that offset checkpoint file exists
    server.shutdown()
    for (logDir <- config.logDirs) {
      val OffsetCheckpointFile = new File(logDir, LogManager.RecoveryPointCheckpointFile)
      assertTrue(OffsetCheckpointFile.exists)
      assertTrue(OffsetCheckpointFile.length() > 0)
    }
    producer.close()

    /* now restart the server and check that the written data is still readable and everything still works */
    server = new KafkaServer(config)
    server.startup()

    // wait for the broker to receive the update metadata request after startup
    TestUtils.waitUntilMetadataIsPropagated(Seq(server), topic, 0)

    producer = createProducer(server)
    val consumer = new SimpleConsumer(host, TestUtils.boundPort(server), 1000000, 64*1024, "")

    var fetchedMessage: ByteBufferMessageSet = null
    while (fetchedMessage == null || fetchedMessage.validBytes == 0) {
      val fetched = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).maxWait(0).build())
      fetchedMessage = fetched.messageSet(topic, 0)
    }
    assertEquals(sent1, fetchedMessage.map(m => TestUtils.readString(m.message.payload)))
    val newOffset = fetchedMessage.last.nextOffset

    // send some more messages
    sent2.map(value => producer.send(new ProducerRecord(topic, 0, value))).foreach(_.get)

    fetchedMessage = null
    while (fetchedMessage == null || fetchedMessage.validBytes == 0) {
      val fetched = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, newOffset, 10000).build())
      fetchedMessage = fetched.messageSet(topic, 0)
    }
    assertEquals(sent2, fetchedMessage.map(m => TestUtils.readString(m.message.payload)))

    consumer.close()
    producer.close()
    server.shutdown()
    CoreUtils.delete(server.config.logDirs)
    verifyNonDaemonThreadsStatus
  }

  @Test
  def testCleanShutdownWithDeleteTopicEnabled() {
    val newProps = TestUtils.createBrokerConfig(0, zkConnect)
    newProps.setProperty("delete.topic.enable", "true")
    val newConfig = KafkaConfig.fromProps(newProps)
    val server = new KafkaServer(newConfig, threadNamePrefix = Option(this.getClass.getName))
    server.startup()
    server.shutdown()
    server.awaitShutdown()
    CoreUtils.delete(server.config.logDirs)
    verifyNonDaemonThreadsStatus
  }

  @Test
  def testCleanShutdownAfterFailedStartup() {
    val newProps = TestUtils.createBrokerConfig(0, zkConnect)
    newProps.setProperty("zookeeper.connect", "some.invalid.hostname.foo.bar.local:65535")
    val newConfig = KafkaConfig.fromProps(newProps)
    verifyCleanShutdownAfterFailedStartup[UnknownHostException](newConfig)
  }

  @Test
  def testCleanShutdownAfterFailedStartupDueToCorruptLogs() {
    val server = new KafkaServer(config)
    server.startup()
    createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 1, servers = Seq(server))
    server.shutdown()
    server.awaitShutdown()
    config.logDirs.foreach { dirName =>
      val partitionDir = new File(dirName, s"$topic-0")
      partitionDir.listFiles.foreach(f => TestUtils.appendNonsenseToFile(f, TestUtils.random.nextInt(1024) + 1))
    }
    verifyCleanShutdownAfterFailedStartup[KafkaStorageException](config)
  }

  private def verifyCleanShutdownAfterFailedStartup[E <: Exception](config: KafkaConfig)(implicit exceptionClassTag: ClassTag[E]) {
    val server = new KafkaServer(config, threadNamePrefix = Option(this.getClass.getName))
    try {
      server.startup()
      fail("Expected KafkaServer setup to fail and throw exception")
    }
    catch {
      // Try to clean up carefully without hanging even if the test fails. This means trying to accurately
      // identify the correct exception, making sure the server was shutdown, and cleaning up if anything
      // goes wrong so that awaitShutdown doesn't hang
      case e: Exception =>
        assertTrue(s"Unexpected exception $e", exceptionClassTag.runtimeClass.isInstance(e))
        assertEquals(NotRunning.state, server.brokerState.currentState)
    }
    finally {
      if (server.brokerState.currentState != NotRunning.state)
        server.shutdown()
      server.awaitShutdown()
    }
    CoreUtils.delete(server.config.logDirs)
    verifyNonDaemonThreadsStatus
  }

  private[this] def isNonDaemonKafkaThread(t: Thread): Boolean = {
    !t.isDaemon && t.isAlive && t.getName.startsWith(this.getClass.getName)
  }

  def verifyNonDaemonThreadsStatus() {
    assertEquals(0, Thread.getAllStackTraces.keySet().toArray
      .map{ _.asInstanceOf[Thread] }
      .count(isNonDaemonKafkaThread))
  }

  @Test
  def testConsecutiveShutdown(){
    val server = new KafkaServer(config)
    try {
      server.startup()
      server.shutdown()
      server.awaitShutdown()
      server.shutdown()
      assertTrue(true)
    }
    catch{
      case _: Throwable => fail()
    }
  }

  // Verify that if controller is in the midst of processing a request, shutdown completes
  // without waiting for request timeout.
  @Test
  def testControllerShutdownDuringSend(): Unit = {
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)

    val controllerId = 2
    val metrics = new Metrics
    val executor = Executors.newSingleThreadExecutor
    var serverSocket: ServerSocket = null
    var controllerChannelManager: ControllerChannelManager = null

    try {
      // Set up a server to accept a connection and receive one byte from the first request. No response is sent.
      serverSocket = new ServerSocket(0)
      val receiveFuture = executor.submit(new Runnable {
        override def run(): Unit = {
          val socket = serverSocket.accept()
          new DataInputStream(socket.getInputStream).readByte()
        }
      })

      // Start a ControllerChannelManager
      val brokers = Seq(new Broker(1, "localhost", serverSocket.getLocalPort, listenerName, securityProtocol))
      val controllerConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(controllerId, zkConnect))
      val controllerContext = new ControllerContext
      controllerContext.liveBrokers = brokers.toSet
      controllerChannelManager = new ControllerChannelManager(controllerContext, controllerConfig, Time.SYSTEM,
        metrics, new StateChangeLogger(controllerId, inControllerContext = true, None))
      controllerChannelManager.startup()

      // Initiate a sendRequest and wait until connection is established and one byte is received by the peer
      val requestBuilder = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion,
        controllerId, 1, Map.empty.asJava, brokers.map(_.node(listenerName)).toSet.asJava)
      controllerChannelManager.sendRequest(1, ApiKeys.LEADER_AND_ISR, requestBuilder)
      receiveFuture.get(10, TimeUnit.SECONDS)

      // Shutdown controller. Request timeout is 30s, verify that shutdown completed well before that
      val shutdownFuture = executor.submit(new Runnable {
        override def run(): Unit = controllerChannelManager.shutdown()
      })
      shutdownFuture.get(10, TimeUnit.SECONDS)

    } finally {
      if (serverSocket != null)
        serverSocket.close()
      if (controllerChannelManager != null)
        controllerChannelManager.shutdown()
      executor.shutdownNow()
      metrics.close()
    }
  }
}
