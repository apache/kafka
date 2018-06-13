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

import kafka.zk.ZooKeeperTestHarness
import kafka.utils.{CoreUtils, TestUtils}
import kafka.utils.TestUtils._
import java.io.File

import kafka.log.LogManager
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer, StringSerializer}
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

    def createConsumer(server: KafkaServer): KafkaConsumer[Integer, String] =
      TestUtils.createNewConsumer(
        TestUtils.getBrokerListStrFromServers(Seq(server)),
        securityProtocol = SecurityProtocol.PLAINTEXT,
        keyDeserializer = new IntegerDeserializer,
        valueDeserializer = new StringDeserializer
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
    val consumer = createConsumer(server)
    consumer.subscribe(Seq(topic).asJava)

    val consumerRecords = TestUtils.consumeRecords(consumer, sent1.size)
    assertEquals(sent1, consumerRecords.map(_.value))

    // send some more messages
    sent2.map(value => producer.send(new ProducerRecord(topic, 0, value))).foreach(_.get)

    val consumerRecords2 = TestUtils.consumeRecords(consumer, sent2.size)
    assertEquals(sent2, consumerRecords2.map(_.value))

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
    verifyCleanShutdownAfterFailedStartup[IllegalArgumentException](newConfig)
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
}
