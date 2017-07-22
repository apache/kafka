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

import kafka.utils.{ZKGroupTopicDirs, Logging}
import kafka.consumer.{ConsumerTimeoutException, ConsumerConfig, ConsumerConnector, Consumer}
import kafka.server._
import kafka.utils.TestUtils
import kafka.serializer._
import kafka.producer.{Producer, KeyedMessage}

import org.junit.{After, Before, Test}
import org.apache.log4j.{Level, Logger}
import org.junit.Assert._

@deprecated("This test has been deprecated and it will be removed in a future release", "0.10.0.0")
class AutoOffsetResetTest extends KafkaServerTestHarness with Logging {

  def generateConfigs = List(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))

  val topic = "test_topic"
  val group = "default_group"
  val testConsumer = "consumer"
  val NumMessages = 10
  val LargeOffset = 10000
  val SmallOffset = -1
  
  val requestHandlerLogger = Logger.getLogger(classOf[kafka.server.KafkaRequestHandler])

  @Before
  override def setUp() {
    super.setUp()
    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)
  }

  @After
  override def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
    super.tearDown
  }

  @Test
  def testResetToEarliestWhenOffsetTooHigh() =
    assertEquals(NumMessages, resetAndConsume(NumMessages, "smallest", LargeOffset))

  @Test
  def testResetToEarliestWhenOffsetTooLow() =
    assertEquals(NumMessages, resetAndConsume(NumMessages, "smallest", SmallOffset))

  @Test
  def testResetToLatestWhenOffsetTooHigh() =
    assertEquals(0, resetAndConsume(NumMessages, "largest", LargeOffset))

  @Test
  def testResetToLatestWhenOffsetTooLow() =
    assertEquals(0, resetAndConsume(NumMessages, "largest", SmallOffset))

  /* Produce the given number of messages, create a consumer with the given offset policy, 
   * then reset the offset to the given value and consume until we get no new messages. 
   * Returns the count of messages received.
   */
  def resetAndConsume(numMessages: Int, resetTo: String, offset: Long): Int = {
    TestUtils.createTopic(zkUtils, topic, 1, 1, servers)

    val producer: Producer[String, Array[Byte]] = TestUtils.createProducer(
      TestUtils.getBrokerListStrFromServers(servers),
      keyEncoder = classOf[StringEncoder].getName)

    for(_ <- 0 until numMessages)
      producer.send(new KeyedMessage[String, Array[Byte]](topic, topic, "test".getBytes))

    // update offset in zookeeper for consumer to jump "forward" in time
    val dirs = new ZKGroupTopicDirs(group, topic)
    val consumerProps = TestUtils.createConsumerProperties(zkConnect, group, testConsumer)
    consumerProps.put("auto.offset.reset", resetTo)
    consumerProps.put("consumer.timeout.ms", "2000")
    consumerProps.put("fetch.wait.max.ms", "0")
    val consumerConfig = new ConsumerConfig(consumerProps)

    TestUtils.updateConsumerOffset(consumerConfig, dirs.consumerOffsetDir + "/" + "0", offset)
    info("Updated consumer offset to " + offset)
    
    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    val messageStream = consumerConnector.createMessageStreams(Map(topic -> 1))(topic).head

    var received = 0
    val iter = messageStream.iterator
    try {
      for (_ <- 0 until numMessages) {
        iter.next // will throw a timeout exception if the message isn't there
        received += 1
      }
    } catch {
      case _: ConsumerTimeoutException =>
        info("consumer timed out after receiving " + received + " messages.")
    } finally {
      producer.close()
      consumerConnector.shutdown
    }
    received
  }
  
}
