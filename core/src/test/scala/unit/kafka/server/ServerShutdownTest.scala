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

import kafka.utils.{CoreUtils, TestInfoUtils, TestUtils}

import java.io.File
import java.util.concurrent.CancellationException
import kafka.integration.KafkaServerTestHarness
import kafka.log.LogManager
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer, StringSerializer}
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.server.config.{KRaftConfigs, ServerLogConfigs}
import org.junit.jupiter.api.{BeforeEach, TestInfo, Timeout}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{MethodSource, ValueSource}

import java.time.Duration
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
      val logDirsValue = originals.get(ServerLogConfigs.LOG_DIRS_CONFIG)
      if (logDirsValue != null) {
        propsToChangeUponRestart.put(ServerLogConfigs.LOG_DIRS_CONFIG, logDirsValue)
      } else {
        propsToChangeUponRestart.put(ServerLogConfigs.LOG_DIR_CONFIG, originals.get(ServerLogConfigs.LOG_DIR_CONFIG))
      }
    }
    priorConfig = Some(KafkaConfig.fromProps(TestUtils.createBrokerConfigs(1, null).head, propsToChangeUponRestart))
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testCleanShutdown(quorum: String, groupProtocol: String): Unit = {

    def createProducer(): KafkaProducer[Integer, String] =
      TestUtils.createProducer(
        bootstrapServers(),
        keySerializer = new IntegerSerializer,
        valueSerializer = new StringSerializer
      )

    def createConsumer(): Consumer[Integer, String] =
      TestUtils.createConsumer(
        bootstrapServers(),
        groupProtocolFromTestParameters(),
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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCleanShutdownAfterFailedStartup(quorum: String): Unit = {
    propsToChangeUponRestart.setProperty(KRaftConfigs.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_CONFIG, "1000")
    shutdownBroker()
    shutdownKRaftController()
    verifyCleanShutdownAfterFailedStartup[CancellationException]
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testShutdownWithKRaftControllerUnavailable(quorum: String): Unit = {
    shutdownKRaftController()
    killBroker(0, Duration.ofSeconds(1))
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
        assertEquals(BrokerState.SHUTTING_DOWN, brokers.head.brokerState)
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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testConsecutiveShutdown(quorum: String): Unit = {
    shutdownBroker()
    brokers.head.shutdown()
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
