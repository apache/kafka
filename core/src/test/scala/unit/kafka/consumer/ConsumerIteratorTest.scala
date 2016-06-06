
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

import kafka.common.LongRef

import scala.collection._
import org.junit.Assert._
import kafka.message._
import kafka.server._
import kafka.utils.TestUtils._
import kafka.utils._
import org.junit.{Before, Test}
import kafka.serializer._
import kafka.integration.KafkaServerTestHarness

class ConsumerIteratorTest extends KafkaServerTestHarness {

  val numNodes = 1

  def generateConfigs() = TestUtils.createBrokerConfigs(numNodes, zkConnect).map(KafkaConfig.fromProps)

  val messages = new mutable.HashMap[Int, Seq[Message]]
  val topic = "topic"
  val group = "group1"
  val consumer0 = "consumer0"
  val consumedOffset = 5
  val queue = new LinkedBlockingQueue[FetchedDataChunk]
  var topicInfos: Seq[PartitionTopicInfo] = null

  def consumerConfig = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer0))

  @Before
  override def setUp() {
    super.setUp()
    topicInfos = configs.map(c => new PartitionTopicInfo(topic,
      0,
      queue,
      new AtomicLong(consumedOffset),
      new AtomicLong(0),
      new AtomicInteger(0),
      ""))
    createTopic(zkUtils, topic, partitionReplicaAssignment = Map(0 -> Seq(configs.head.brokerId)), servers = servers)
  }

  @Test
  def testConsumerIteratorDeduplicationDeepIterator() {
    val messageStrings = (0 until 10).map(_.toString).toList
    val messages = messageStrings.map(s => new Message(s.getBytes))
    val messageSet = new ByteBufferMessageSet(DefaultCompressionCodec, new LongRef(0), messages:_*)

    topicInfos.head.enqueue(messageSet)
    assertEquals(1, queue.size)
    queue.put(ZookeeperConsumerConnector.shutdownCommand)

    val iter = new ConsumerIterator[String, String](queue, 
                                                    consumerConfig.consumerTimeoutMs,
                                                    new StringDecoder(), 
                                                    new StringDecoder(),
                                                    clientId = "")
    val receivedMessages = (0 until 5).map(i => iter.next.message).toList

    assertFalse(iter.hasNext)
    assertEquals(0, queue.size) // Shutdown command has been consumed.
    assertEquals(5, receivedMessages.size)
    val unconsumed = messageSet.filter(_.offset >= consumedOffset).map(m => TestUtils.readString(m.message.payload))
    assertEquals(unconsumed, receivedMessages)
  }

  @Test
  def testConsumerIteratorDecodingFailure() {
    val messageStrings = (0 until 10).map(_.toString).toList
    val messages = messageStrings.map(s => new Message(s.getBytes))
    val messageSet = new ByteBufferMessageSet(NoCompressionCodec, new LongRef(0), messages:_*)

    topicInfos.head.enqueue(messageSet)
    assertEquals(1, queue.size)

    val iter = new ConsumerIterator[String, String](queue,
      ConsumerConfig.ConsumerTimeoutMs,
      new FailDecoder(),
      new FailDecoder(),
      clientId = "")

    val receivedMessages = (0 until 5).map{ i =>
      assertTrue(iter.hasNext)
      val message = iter.next
      assertEquals(message.offset, i + consumedOffset)

      try {
        message.message // should fail
      }
      catch {
        case e: UnsupportedOperationException => // this is ok
        case e2: Throwable => fail("Unexpected exception when iterating the message set. " + e2.getMessage)
      }
    }
  }

  class FailDecoder(props: VerifiableProperties = null) extends Decoder[String] {
    def fromBytes(bytes: Array[Byte]): String = {
      throw new UnsupportedOperationException("This decoder does not work at all..")
    }
  }
}
