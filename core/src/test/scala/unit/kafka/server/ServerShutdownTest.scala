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
import kafka.consumer.SimpleConsumer
import kafka.producer._
import kafka.utils.{IntEncoder, TestUtils, CoreUtils}
import kafka.utils.TestUtils._
import kafka.api.FetchRequestBuilder
import kafka.message.ByteBufferMessageSet
import kafka.serializer.StringEncoder

import java.io.File

import org.junit.{Before, Test}
import org.junit.Assert._

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
    var server = new KafkaServer(config, threadNamePrefix = Option(this.getClass.getName))
    server.startup()
    var producer = TestUtils.createProducer[Int, String](TestUtils.getBrokerListStrFromServers(Seq(server)),
      encoder = classOf[StringEncoder].getName,
      keyEncoder = classOf[IntEncoder].getName)

    // create topic
    createTopic(zkUtils, topic, numPartitions = 1, replicationFactor = 1, servers = Seq(server))

    // send some messages
    producer.send(sent1.map(m => new KeyedMessage[Int, String](topic, 0, m)):_*)

    // do a clean shutdown and check that offset checkpoint file exists
    server.shutdown()
    for(logDir <- config.logDirs) {
      val OffsetCheckpointFile = new File(logDir, server.logManager.RecoveryPointCheckpointFile)
      assertTrue(OffsetCheckpointFile.exists)
      assertTrue(OffsetCheckpointFile.length() > 0)
    }
    producer.close()
    
    /* now restart the server and check that the written data is still readable and everything still works */
    server = new KafkaServer(config)
    server.startup()

    // wait for the broker to receive the update metadata request after startup
    TestUtils.waitUntilMetadataIsPropagated(Seq(server), topic, 0)

    producer = TestUtils.createProducer[Int, String](TestUtils.getBrokerListStrFromServers(Seq(server)),
      encoder = classOf[StringEncoder].getName,
      keyEncoder = classOf[IntEncoder].getName)
    val consumer = new SimpleConsumer(host, server.boundPort(), 1000000, 64*1024, "")

    var fetchedMessage: ByteBufferMessageSet = null
    while(fetchedMessage == null || fetchedMessage.validBytes == 0) {
      val fetched = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).maxWait(0).build())
      fetchedMessage = fetched.messageSet(topic, 0)
    }
    assertEquals(sent1, fetchedMessage.map(m => TestUtils.readString(m.message.payload)))
    val newOffset = fetchedMessage.last.nextOffset

    // send some more messages
    producer.send(sent2.map(m => new KeyedMessage[Int, String](topic, 0, m)):_*)

    fetchedMessage = null
    while(fetchedMessage == null || fetchedMessage.validBytes == 0) {
      val fetched = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, newOffset, 10000).build())
      fetchedMessage = fetched.messageSet(topic, 0)
    }
    assertEquals(sent2, fetchedMessage.map(m => TestUtils.readString(m.message.payload)))

    consumer.close()
    producer.close()
    server.shutdown()
    CoreUtils.rm(server.config.logDirs)
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
    CoreUtils.rm(server.config.logDirs)
    verifyNonDaemonThreadsStatus
  }

  @Test
  def testCleanShutdownAfterFailedStartup() {
    val newProps = TestUtils.createBrokerConfig(0, zkConnect)
    newProps.setProperty("zookeeper.connect", "fakehostthatwontresolve:65535")
    val newConfig = KafkaConfig.fromProps(newProps)
    val server = new KafkaServer(newConfig, threadNamePrefix = Option(this.getClass.getName))
    try {
      server.startup()
      fail("Expected KafkaServer setup to fail, throw exception")
    }
    catch {
      // Try to clean up carefully without hanging even if the test fails. This means trying to accurately
      // identify the correct exception, making sure the server was shutdown, and cleaning up if anything
      // goes wrong so that awaitShutdown doesn't hang
      case e: org.I0Itec.zkclient.exception.ZkException =>
        assertEquals(NotRunning.state, server.brokerState.currentState)
      case e: Throwable =>
        fail("Expected ZkException during Kafka server starting up but caught a different exception %s".format(e.toString))
    }
    finally {
      if (server.brokerState.currentState != NotRunning.state)
        server.shutdown()
      server.awaitShutdown()
    }
    CoreUtils.rm(server.config.logDirs)
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
      case ex: Throwable => fail()
    }
  }
}
