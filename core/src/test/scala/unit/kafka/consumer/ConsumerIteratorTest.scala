
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

package kafka.consumer

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.Properties
import scala.collection._
import junit.framework.Assert._

import kafka.message._
import kafka.server._
import kafka.utils.TestUtils._
import kafka.utils._
import kafka.admin.AdminUtils
import org.junit.Test
import kafka.serializer._
import kafka.cluster.{Broker, Cluster}
import org.scalatest.junit.JUnit3Suite
import kafka.integration.KafkaServerTestHarness

class ConsumerIteratorTest extends JUnit3Suite with KafkaServerTestHarness {

  val numNodes = 1
  val configs =
    for(props <- TestUtils.createBrokerConfigs(numNodes))
    yield new KafkaConfig(props) {
      override val zkConnect = TestZKUtils.zookeeperConnect
    }
  val messages = new mutable.HashMap[Int, Seq[Message]]
  val topic = "topic"
  val group = "group1"
  val consumer0 = "consumer0"
  val consumedOffset = 5
  val cluster = new Cluster(configs.map(c => new Broker(c.brokerId, "localhost", c.port)))
  val queue = new LinkedBlockingQueue[FetchedDataChunk]
  val topicInfos = configs.map(c => new PartitionTopicInfo(topic,
                                                           0,
                                                           queue,
                                                           new AtomicLong(consumedOffset),
                                                           new AtomicLong(0),
                                                           new AtomicInteger(0),
                                                           ""))
  val consumerConfig = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer0))

  override def setUp() {
    super.setUp
    AdminUtils.createTopicWithAssignment(zkClient, topic, Map(0 -> Seq(configs.head.brokerId)), new Properties)
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 500)
  }

  @Test
  def testConsumerIteratorDeduplicationDeepIterator() {
    val messageStrings = (0 until 10).map(_.toString).toList
    val messages = messageStrings.map(s => new Message(s.getBytes))
    val messageSet = new ByteBufferMessageSet(DefaultCompressionCodec, new AtomicLong(0), messages:_*)

    topicInfos(0).enqueue(messageSet)
    assertEquals(1, queue.size)
    queue.put(ZookeeperConsumerConnector.shutdownCommand)

    val iter = new ConsumerIterator[String, String](queue, 
                                                    consumerConfig.consumerTimeoutMs,
                                                    new StringDecoder(), 
                                                    new StringDecoder(),
                                                    clientId = "")
    val receivedMessages = (0 until 5).map(i => iter.next.message).toList

    assertFalse(iter.hasNext)
    assertEquals(1, queue.size) // This is only the shutdown command.
    assertEquals(5, receivedMessages.size)
    val unconsumed = messageSet.filter(_.offset >= consumedOffset).map(m => Utils.readString(m.message.payload))
    assertEquals(unconsumed, receivedMessages)
  }
}
