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

package kafka.log

import kafka.server.KafkaConfig
import java.io.File
import java.nio.ByteBuffer
import kafka.utils.Utils
import kafka.api.FetchRequest
import kafka.common.InvalidMessageSizeException
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.{TestZKUtils, TestUtils}
import kafka.consumer.{ZookeeperConsumerConnector, ConsumerConfig}
import org.scalatest.junit.JUnit3Suite
import kafka.integration.ProducerConsumerTestHarness
import kafka.integration.KafkaServerTestHarness
import org.apache.log4j.{Logger, Level}
import kafka.message.{NoCompressionCodec, Message, ByteBufferMessageSet}

class LogCorruptionTest extends JUnit3Suite with ProducerConsumerTestHarness with KafkaServerTestHarness with ZooKeeperTestHarness {
  val zkConnect = TestZKUtils.zookeeperConnect  
  val port = TestUtils.choosePort
  val props = TestUtils.createBrokerConfig(0, port)
  val config = new KafkaConfig(props) {
                 override val hostName = "localhost"
                 override val enableZookeeper = true
               }
  val configs = List(config)
  val topic = "test"
  val partition = 0

  def testMessageSizeTooLarge() {
    val requestHandlerLogger = Logger.getLogger(classOf[kafka.server.KafkaRequestHandlers])
    val fetcherLogger = Logger.getLogger(classOf[kafka.consumer.FetcherRunnable])

    requestHandlerLogger.setLevel(Level.FATAL)
    fetcherLogger.setLevel(Level.FATAL)

    // send some messages
    val sent1 = new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = new Message("hello".getBytes()))
    producer.send(topic, sent1)
    Thread.sleep(200)

    // corrupt the file on disk
    val logFile = new File(config.logDir + File.separator + topic + "-" + partition, Log.nameFromOffset(0))
    val byteBuffer = ByteBuffer.allocate(4)
    byteBuffer.putInt(1000) // wrong message size
    byteBuffer.rewind()
    val channel = Utils.openChannel(logFile, true)
    channel.write(byteBuffer)
    channel.force(true)
    channel.close

    Thread.sleep(500)
    // test SimpleConsumer
    val messageSet = consumer.fetch(new FetchRequest(topic, partition, 0, 10000))
    try {
      for (msg <- messageSet)
        fail("shouldn't reach here in SimpleConsumer since log file is corrupted.")
      fail("shouldn't reach here in SimpleConsumer since log file is corrupted.")
    }
    catch {
      case e: InvalidMessageSizeException => "This is good"
    }

    val messageSet2 = consumer.fetch(new FetchRequest(topic, partition, 0, 10000))
    try {
      for (msg <- messageSet2)
        fail("shouldn't reach here in SimpleConsumer since log file is corrupted.")
      fail("shouldn't reach here in SimpleConsumer since log file is corrupted.")
    }
    catch {
      case e: InvalidMessageSizeException => println("This is good")
    }

    // test ZookeeperConsumer
    val consumerConfig1 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, "group1", "consumer1", 10000))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Predef.Map(topic -> 1))
    try {
      for ((topic, messageStreams) <- topicMessageStreams1)
      for (message <- messageStreams(0))
        fail("shouldn't reach here in ZookeeperConsumer since log file is corrupted.")
      fail("shouldn't reach here in ZookeeperConsumer since log file is corrupted.")
    }
    catch {
      case e: InvalidMessageSizeException => "This is good"
      case e: Exception => "This is not bad too !"
    }

    zkConsumerConnector1.shutdown
    requestHandlerLogger.setLevel(Level.ERROR)
    fetcherLogger.setLevel(Level.ERROR)
  }
}
