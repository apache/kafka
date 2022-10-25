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

import kafka.utils.{CoreUtils, Exit, TestInfoUtils, TestUtils}

import java.io.{DataInputStream, File}
import java.net.ServerSocket
import java.util.Collections
import java.util.concurrent.{CancellationException, Executors, TimeUnit}
import kafka.cluster.Broker
import kafka.controller.{ControllerChannelManager, ControllerContext, StateChangeLogger}
import kafka.integration.KafkaServerTestHarness
import kafka.log.LogManager
import kafka.zookeeper.ZooKeeperClientTimeoutException
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.LeaderAndIsrRequest
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer, StringSerializer}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.BrokerState
import org.junit.jupiter.api.{BeforeEach, Disabled, TestInfo, Timeout}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Properties
import scala.collection.Seq
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

@Timeout(60)
class ServerShutdownTest extends KafkaServerTestHarness {
  val host = "localhost"
  val topic = "test"
  val sent1 = List("hello", "there")
  val sent2 = List("more", "messages")
  val propsToChangeUponRestart = new Properties()
  var priorConfig: Option[KafkaConfig] = None

  override def generateConfigs: Seq[KafkaConfig] = {
    priorConfig.foreach { config =>
      // keep the same log directory
      val originals = config.originals
      val logDirsValue = originals.get(KafkaConfig.LogDirsProp)
      if (logDirsValue != null) {
        propsToChangeUponRestart.put(KafkaConfig.LogDirsProp, logDirsValue)
      } else {
        propsToChangeUponRestart.put(KafkaConfig.LogDirProp, originals.get(KafkaConfig.LogDirProp))
      }
    }
    priorConfig = Some(KafkaConfig.fromProps(TestUtils.createBrokerConfigs(1, zkConnectOrNull).head, propsToChangeUponRestart))
    Seq(priorConfig.get)
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    // be sure to clear local variables before setting up so that anything leftover from a prior test
    // won;t impact the initial config for the current test
    priorConfig = None
    propsToChangeUponRestart.clear()
    super.setUp(testInfo)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCleanShutdown(quorum: String): Unit = {

    def createProducer(): KafkaProducer[Integer, String] =
      TestUtils.createProducer(
        bootstrapServers(),
        keySerializer = new IntegerSerializer,
        valueSerializer = new StringSerializer
      )

    def createConsumer(): KafkaConsumer[Integer, String] =
      TestUtils.createConsumer(
        bootstrapServers(),
        securityProtocol = SecurityProtocol.PLAINTEXT,
        keyDeserializer = new IntegerDeserializer,
        valueDeserializer = new StringDeserializer
      )

    var producer = createProducer()

    // create topic
    createTopic(topic)

    // send some messages
    sent1.map(value => producer.send(new ProducerRecord(topic, 0, value))).foreach(_.get)

    // do a clean shutdown and check that offset checkpoint file exists
    shutdownBroker()
    for (logDir <- config.logDirs) {
      val OffsetCheckpointFile = new File(logDir, LogManager.RecoveryPointCheckpointFile)
      assertTrue(OffsetCheckpointFile.exists)
      assertTrue(OffsetCheckpointFile.length() > 0)
    }
    producer.close()

    /* now restart the server and check that the written data is still readable and everything still works */
    restartBroker()

    // wait for the broker to receive the update metadata request after startup
    TestUtils.waitForPartitionMetadata(Seq(broker), topic, 0)

    producer = createProducer()
    val consumer = createConsumer()
    consumer.subscribe(Seq(topic).asJava)

    val consumerRecords = TestUtils.consumeRecords(consumer, sent1.size)
    assertEquals(sent1, consumerRecords.map(_.value))

    // send some more messages
    sent2.map(value => producer.send(new ProducerRecord(topic, 0, value))).foreach(_.get)

    val consumerRecords2 = TestUtils.consumeRecords(consumer, sent2.size)
    assertEquals(sent2, consumerRecords2.map(_.value))

    consumer.close()
    producer.close()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCleanShutdownAfterFailedStartup(quorum: String): Unit = {
    if (isKRaftTest()) {
      propsToChangeUponRestart.setProperty(KafkaConfig.InitialBrokerRegistrationTimeoutMsProp, "1000")
      shutdownBroker()
      shutdownKRaftController()
      verifyCleanShutdownAfterFailedStartup[CancellationException]
    } else {
      propsToChangeUponRestart.setProperty(KafkaConfig.ZkConnectionTimeoutMsProp, "50")
      propsToChangeUponRestart.setProperty(KafkaConfig.ZkConnectProp, "some.invalid.hostname.foo.bar.local:65535")
      verifyCleanShutdownAfterFailedStartup[ZooKeeperClientTimeoutException]
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testNoCleanShutdownAfterFailedStartupDueToCorruptLogs(quorum: String): Unit = {
    createTopic(topic)
    shutdownBroker()
    config.logDirs.foreach { dirName =>
      val partitionDir = new File(dirName, s"$topic-0")
      partitionDir.listFiles.foreach(f => TestUtils.appendNonsenseToFile(f, TestUtils.random.nextInt(1024) + 1))
    }

    val expectedStatusCode = Some(1)
    @volatile var receivedStatusCode = Option.empty[Int]
    @volatile var hasHaltProcedureCalled = false
    Exit.setHaltProcedure((statusCode, _) => {
      hasHaltProcedureCalled = true
      receivedStatusCode = Some(statusCode)
    }.asInstanceOf[Nothing])

    try {
      val recreateBrokerExec: Executable = () => recreateBroker(true)
      // this startup should fail with no online log dir (due to corrupted log), and exit directly without throwing exception
      assertDoesNotThrow(recreateBrokerExec)
      // JVM should exit with status code 1
      TestUtils.waitUntilTrue(() => hasHaltProcedureCalled == true && expectedStatusCode == receivedStatusCode,
        s"Expected to halt directly with the expected status code:${expectedStatusCode.get}, " +
          s"but got hasHaltProcedureCalled: $hasHaltProcedureCalled and received status code: ${receivedStatusCode.orNull}")
    } finally {
      Exit.resetHaltProcedure()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testCleanShutdownWithZkUnavailable(quorum: String): Unit = {
    shutdownZooKeeper()
    shutdownBroker()
    CoreUtils.delete(broker.config.logDirs)
    verifyNonDaemonThreadsStatus()
  }

  @Disabled
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testCleanShutdownWithKRaftControllerUnavailable(quorum: String): Unit = {
    shutdownKRaftController()
    shutdownBroker()
    CoreUtils.delete(broker.config.logDirs)
    verifyNonDaemonThreadsStatus()
  }

  private def verifyCleanShutdownAfterFailedStartup[E <: Exception](implicit exceptionClassTag: ClassTag[E]): Unit = {
    try {
      recreateBroker(startup = true)
      fail("Expected KafkaServer setup to fail and throw exception")
    } catch {
      // Try to clean up carefully without hanging even if the test fails. This means trying to accurately
      // identify the correct exception, making sure the server was shutdown, and cleaning up if anything
      // goes wrong so that awaitShutdown doesn't hang
      case e: Exception =>
        assertCause(exceptionClassTag.runtimeClass, e)
        assertEquals(if (isKRaftTest()) BrokerState.SHUTTING_DOWN else BrokerState.NOT_RUNNING, brokers.head.brokerState)
    } finally {
      shutdownBroker()
    }
  }

  private def assertCause(expectedClass: Class[_], e: Throwable): Unit = {
    var cause = e
    while (cause != null) {
      if (expectedClass.isInstance(cause)) {
        return
      }
      cause = cause.getCause
    }
    fail(s"Failed to assert cause of $e, expected cause $expectedClass")
  }

  private[this] def isNonDaemonKafkaThread(t: Thread): Boolean = {
    !t.isDaemon && t.isAlive && t.getName.startsWith(this.getClass.getName)
  }

  def verifyNonDaemonThreadsStatus(): Unit = {
    assertEquals(0, Thread.getAllStackTraces.keySet.toArray
      .map(_.asInstanceOf[Thread])
      .count(isNonDaemonKafkaThread))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testConsecutiveShutdown(quorum: String): Unit = {
    shutdownBroker()
    brokers.head.shutdown()
  }

  // Verify that if controller is in the midst of processing a request, shutdown completes
  // without waiting for request timeout. Since this involves LeaderAndIsr request, it is
  // ZK-only for now.
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testControllerShutdownDuringSend(quorum: String): Unit = {
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
      val brokerAndEpochs = Map((new Broker(1, "localhost", serverSocket.getLocalPort, listenerName, securityProtocol), 0L))
      val controllerConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(controllerId, zkConnect))
      val controllerContext = new ControllerContext
      controllerContext.setLiveBrokers(brokerAndEpochs)
      controllerChannelManager = new ControllerChannelManager(controllerContext, controllerConfig, Time.SYSTEM,
        metrics, new StateChangeLogger(controllerId, inControllerContext = true, None))
      controllerChannelManager.startup()

      // Initiate a sendRequest and wait until connection is established and one byte is received by the peer
      val requestBuilder = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion,
        controllerId, 1, 0L, Seq.empty.asJava, Collections.singletonMap(topic, Uuid.randomUuid()),
        brokerAndEpochs.keys.map(_.node(listenerName)).toSet.asJava)
      controllerChannelManager.sendRequest(1, requestBuilder)
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

  private def config: KafkaConfig = configs.head
  private def broker: KafkaBroker = brokers.head
  private def shutdownBroker(): Unit = killBroker(0) // idempotent
  private def restartBroker(): Unit = {
    shutdownBroker()
    restartDeadBrokers(reconfigure = !propsToChangeUponRestart.isEmpty)
  }
  private def recreateBroker(startup: Boolean): Unit =
    recreateBrokers(reconfigure = !propsToChangeUponRestart.isEmpty, startup = startup)
}
