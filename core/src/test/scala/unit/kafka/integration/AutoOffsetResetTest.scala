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

package kafka.integration

import junit.framework.Assert._
import kafka.zk.ZooKeeperTestHarness
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.atomic.AtomicInteger
import kafka.utils.{ZKGroupTopicDirs, Logging}
import kafka.consumer.{ConsumerTimeoutException, ConsumerConfig, ConsumerConnector, Consumer}
import kafka.server.{KafkaRequestHandlers, KafkaServer, KafkaConfig}
import org.apache.log4j.{Level, Logger}
import org.scalatest.junit.JUnit3Suite
import kafka.utils.{TestUtils, TestZKUtils}

class AutoOffsetResetTest extends JUnit3Suite with ZooKeeperTestHarness with Logging {

  val zkConnect = TestZKUtils.zookeeperConnect
  val topic = "test_topic"
  val group = "default_group"
  val testConsumer = "consumer"
  val brokerPort = 9892
  val kafkaConfig = new KafkaConfig(TestUtils.createBrokerConfig(0, brokerPort))
  var kafkaServer : KafkaServer = null
  val numMessages = 10
  val largeOffset = 10000
  val smallOffset = -1
  
  val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandlers])

  override def setUp() {
    super.setUp()
    kafkaServer = TestUtils.createServer(kafkaConfig)

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)
  }

  override def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
    kafkaServer.shutdown
    super.tearDown
  }
  
  def testEarliestOffsetResetForward() = {
    val producer = TestUtils.createProducer("localhost", brokerPort)

    for(i <- 0 until numMessages) {
      producer.send(topic, TestUtils.singleMessageSet("test".getBytes()))
    }

    // update offset in zookeeper for consumer to jump "forward" in time
    val dirs = new ZKGroupTopicDirs(group, topic)
    var consumerProps = TestUtils.createConsumerProperties(zkConnect, group, testConsumer)
    consumerProps.put("autooffset.reset", "smallest")
    consumerProps.put("consumer.timeout.ms", "2000")
    val consumerConfig = new ConsumerConfig(consumerProps)
    
    TestUtils.updateConsumerOffset(consumerConfig, dirs.consumerOffsetDir + "/" + "0-0", largeOffset)
    info("Updated consumer offset to " + largeOffset)

    Thread.sleep(500)
    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    val messageStreams = consumerConnector.createMessageStreams(Map(topic -> 1))

    var threadList = List[Thread]()
    val nMessages : AtomicInteger = new AtomicInteger(0)
    for ((topic, streamList) <- messageStreams)
      for (i <- 0 until streamList.length)
        threadList ::= new Thread("kafka-zk-consumer-" + i) {
          override def run() {

            try {
              for (message <- streamList(i)) {
                nMessages.incrementAndGet
              }
            }
            catch {
              case te: ConsumerTimeoutException => info("Consumer thread timing out..")
              case _: InterruptedException => 
              case _: ClosedByInterruptException =>
              case e => throw e
            }
          }

        }


    for (thread <- threadList)
      thread.start

    threadList(0).join(2000)

    info("Asserting...")
    assertEquals(numMessages, nMessages.get)
    consumerConnector.shutdown
  }

  def testEarliestOffsetResetBackward() = {
    val producer = TestUtils.createProducer("localhost", brokerPort)

    for(i <- 0 until numMessages) {
      producer.send(topic, TestUtils.singleMessageSet("test".getBytes()))
    }

    // update offset in zookeeper for consumer to jump "forward" in time
    val dirs = new ZKGroupTopicDirs(group, topic)
    var consumerProps = TestUtils.createConsumerProperties(zkConnect, group, testConsumer)
    consumerProps.put("autooffset.reset", "smallest")
    consumerProps.put("consumer.timeout.ms", "2000")
    val consumerConfig = new ConsumerConfig(consumerProps)

    TestUtils.updateConsumerOffset(consumerConfig, dirs.consumerOffsetDir + "/" + "0-0", smallOffset)
    info("Updated consumer offset to " + smallOffset)


    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    val messageStreams = consumerConnector.createMessageStreams(Map(topic -> 1))

    var threadList = List[Thread]()
    val nMessages : AtomicInteger = new AtomicInteger(0)
    for ((topic, streamList) <- messageStreams)
      for (i <- 0 until streamList.length)
        threadList ::= new Thread("kafka-zk-consumer-" + i) {
          override def run() {

            try {
              for (message <- streamList(i)) {
                nMessages.incrementAndGet
              }
            }
            catch {
              case _: InterruptedException => 
              case _: ClosedByInterruptException =>
              case e => throw e
            }
          }

        }


    for (thread <- threadList)
      thread.start

    threadList(0).join(2000)

    info("Asserting...")
    assertEquals(numMessages, nMessages.get)
    consumerConnector.shutdown
  }

  def testLatestOffsetResetForward() = {
    val producer = TestUtils.createProducer("localhost", brokerPort)

    for(i <- 0 until numMessages) {
      producer.send(topic, TestUtils.singleMessageSet("test".getBytes()))
    }

    // update offset in zookeeper for consumer to jump "forward" in time
    val dirs = new ZKGroupTopicDirs(group, topic)
    var consumerProps = TestUtils.createConsumerProperties(zkConnect, group, testConsumer)
    consumerProps.put("autooffset.reset", "largest")
    consumerProps.put("consumer.timeout.ms", "2000")
    val consumerConfig = new ConsumerConfig(consumerProps)

    TestUtils.updateConsumerOffset(consumerConfig, dirs.consumerOffsetDir + "/" + "0-0", largeOffset)
    info("Updated consumer offset to " + largeOffset)


    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    val messageStreams = consumerConnector.createMessageStreams(Map(topic -> 1))

    var threadList = List[Thread]()
    val nMessages : AtomicInteger = new AtomicInteger(0)
    for ((topic, streamList) <- messageStreams)
      for (i <- 0 until streamList.length)
        threadList ::= new Thread("kafka-zk-consumer-" + i) {
          override def run() {

            try {
              for (message <- streamList(i)) {
                nMessages.incrementAndGet
              }
            }
            catch {
              case _: InterruptedException => 
              case _: ClosedByInterruptException =>
              case e => throw e
            }
          }

        }


    for (thread <- threadList)
      thread.start

    threadList(0).join(2000)

    info("Asserting...")

    assertEquals(0, nMessages.get)
    consumerConnector.shutdown
  }

  
}
