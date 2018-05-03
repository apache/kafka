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

package kafka.producer

import java.net.SocketTimeoutException
import java.util.Properties

import kafka.api.{ProducerRequest, ProducerResponseStatus}
import kafka.common.TopicAndPartition
import kafka.integration.KafkaServerTestHarness
import kafka.message._
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.record.{DefaultRecordBatch, DefaultRecord}
import org.junit.Test
import org.junit.Assert._

@deprecated("This test has been deprecated and it will be removed in a future release", "0.10.0.0")
class SyncProducerTest extends KafkaServerTestHarness {
  private val messageBytes =  new Array[Byte](2)
  // turning off controlled shutdown since testProducerCanTimeout() explicitly shuts down request handler pool.
  def generateConfigs = List(KafkaConfig.fromProps(TestUtils.createBrokerConfigs(1, zkConnect, false).head))

  private def produceRequest(topic: String,
    partition: Int,
    message: ByteBufferMessageSet,
    acks: Int,
    timeout: Int = SyncProducerConfig.DefaultAckTimeoutMs,
    correlationId: Int = 0,
    clientId: String = SyncProducerConfig.DefaultClientId): ProducerRequest = {
    TestUtils.produceRequest(topic, partition, message, acks, timeout, correlationId, clientId)
  }

  @Test
  def testReachableServer(): Unit = {
    val server = servers.head
    val props = TestUtils.getSyncProducerConfig(boundPort(server))

    val producer = new SyncProducer(new SyncProducerConfig(props))

    val firstStart = Time.SYSTEM.milliseconds
    var response = producer.send(produceRequest("test", 0,
      new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = new Message(messageBytes)), acks = 1))
    assertNotNull(response)
    assertTrue((Time.SYSTEM.milliseconds - firstStart) < 12000)

    val secondStart = Time.SYSTEM.milliseconds
    response = producer.send(produceRequest("test", 0,
      new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = new Message(messageBytes)), acks = 1))
    assertNotNull(response)
    assertTrue((Time.SYSTEM.milliseconds - secondStart) < 12000)

    response = producer.send(produceRequest("test", 0,
      new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = new Message(messageBytes)), acks = 1))
    assertNotNull(response)
  }

  @Test
  def testEmptyProduceRequest(): Unit = {
    val server = servers.head
    val props = TestUtils.getSyncProducerConfig(boundPort(server))


    val correlationId = 0
    val clientId = SyncProducerConfig.DefaultClientId
    val ackTimeoutMs = SyncProducerConfig.DefaultAckTimeoutMs
    val ack: Short = 1
    val emptyRequest = new kafka.api.ProducerRequest(correlationId, clientId, ack, ackTimeoutMs, collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet]())

    val producer = new SyncProducer(new SyncProducerConfig(props))
    val response = producer.send(emptyRequest)
    assertTrue(response != null)
    assertTrue(!response.hasError && response.status.isEmpty)
  }

  @Test
  def testMessageSizeTooLarge(): Unit = {
    val server = servers.head
    val props = TestUtils.getSyncProducerConfig(boundPort(server))

    val producer = new SyncProducer(new SyncProducerConfig(props))
    createTopic("test", numPartitions = 1, replicationFactor = 1)

    val message1 = new Message(new Array[Byte](configs.head.messageMaxBytes + 1))
    val messageSet1 = new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = message1)
    val response1 = producer.send(produceRequest("test", 0, messageSet1, acks = 1))

    assertEquals(1, response1.status.count(_._2.error != Errors.NONE))
    assertEquals(Errors.MESSAGE_TOO_LARGE, response1.status(TopicAndPartition("test", 0)).error)
    assertEquals(-1L, response1.status(TopicAndPartition("test", 0)).offset)

    val safeSize = configs.head.messageMaxBytes - DefaultRecordBatch.RECORD_BATCH_OVERHEAD - DefaultRecord.MAX_RECORD_OVERHEAD
    val message2 = new Message(new Array[Byte](safeSize))
    val messageSet2 = new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = message2)
    val response2 = producer.send(produceRequest("test", 0, messageSet2, acks = 1))

    assertEquals(1, response1.status.count(_._2.error != Errors.NONE))
    assertEquals(Errors.NONE, response2.status(TopicAndPartition("test", 0)).error)
    assertEquals(0, response2.status(TopicAndPartition("test", 0)).offset)
  }

  @Test
  def testMessageSizeTooLargeWithAckZero(): Unit = {
    val server = servers.head
    val props = TestUtils.getSyncProducerConfig(boundPort(server))

    props.put("request.required.acks", "0")

    val producer = new SyncProducer(new SyncProducerConfig(props))
    adminZkClient.createTopic("test", 1, 1)
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, "test", 0)

    // This message will be dropped silently since message size too large.
    producer.send(produceRequest("test", 0,
      new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = new Message(new Array[Byte](configs.head.messageMaxBytes + 1))), acks = 0))

    // Send another message whose size is large enough to exceed the buffer size so
    // the socket buffer will be flushed immediately;
    // this send should fail since the socket has been closed
    try {
      producer.send(produceRequest("test", 0,
        new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = new Message(new Array[Byte](configs.head.messageMaxBytes + 1))), acks = 0))
    } catch {
      case _ : java.io.IOException => // success
    }
  }

  @Test
  def testProduceCorrectlyReceivesResponse(): Unit = {
    val server = servers.head
    val props = TestUtils.getSyncProducerConfig(boundPort(server))

    val producer = new SyncProducer(new SyncProducerConfig(props))
    val messages = new ByteBufferMessageSet(NoCompressionCodec, new Message(messageBytes))

    // #1 - test that we get an error when partition does not belong to broker in response
    val request = TestUtils.produceRequestWithAcks(Array("topic1", "topic2", "topic3"), Array(0), messages, 1,
      timeout = SyncProducerConfig.DefaultAckTimeoutMs, clientId = SyncProducerConfig.DefaultClientId)
    val response = producer.send(request)

    assertNotNull(response)
    assertEquals(request.correlationId, response.correlationId)
    assertEquals(3, response.status.size)
    response.status.values.foreach {
      case ProducerResponseStatus(error, nextOffset, timestamp) =>
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, error)
        assertEquals(-1L, nextOffset)
        assertEquals(Message.NoTimestamp, timestamp)
    }

    // #2 - test that we get correct offsets when partition is owned by broker
    adminZkClient.createTopic("topic1", 1, 1)
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, "topic1", 0)
    adminZkClient.createTopic("topic3", 1, 1)
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, "topic3", 0)

    val response2 = producer.send(request)
    assertNotNull(response2)
    assertEquals(request.correlationId, response2.correlationId)
    assertEquals(3, response2.status.size)

    // the first and last message should have been accepted by broker
    assertEquals(Errors.NONE, response2.status(TopicAndPartition("topic1", 0)).error)
    assertEquals(Errors.NONE, response2.status(TopicAndPartition("topic3", 0)).error)
    assertEquals(0, response2.status(TopicAndPartition("topic1", 0)).offset)
    assertEquals(0, response2.status(TopicAndPartition("topic3", 0)).offset)

    // the middle message should have been rejected because broker doesn't lead partition
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION,
                        response2.status(TopicAndPartition("topic2", 0)).error)
    assertEquals(-1, response2.status(TopicAndPartition("topic2", 0)).offset)
  }

  @Test
  def testProducerCanTimeout(): Unit = {
    val timeoutMs = 500

    val server = servers.head
    val props = TestUtils.getSyncProducerConfig(boundPort(server))
    val producer = new SyncProducer(new SyncProducerConfig(props))

    val messages = new ByteBufferMessageSet(NoCompressionCodec, new Message(messageBytes))
    val request = produceRequest("topic1", 0, messages, acks = 1)

    // stop IO threads and request handling, but leave networking operational
    // any requests should be accepted and queue up, but not handled
    server.requestHandlerPool.shutdown()

    val t1 = Time.SYSTEM.milliseconds
    try {
      producer.send(request)
      fail("Should have received timeout exception since request handling is stopped.")
    } catch {
      case _: SocketTimeoutException => /* success */
    }
    val t2 = Time.SYSTEM.milliseconds
    // make sure we don't wait fewer than timeoutMs for a response
    assertTrue((t2-t1) >= timeoutMs)
  }

  @Test
  def testProduceRequestWithNoResponse(): Unit = {
    val server = servers.head

    val port = TestUtils.boundPort(server)
    val props = TestUtils.getSyncProducerConfig(port)
    val correlationId = 0
    val clientId = SyncProducerConfig.DefaultClientId
    val ackTimeoutMs = SyncProducerConfig.DefaultAckTimeoutMs
    val ack: Short = 0
    val emptyRequest = new kafka.api.ProducerRequest(correlationId, clientId, ack, ackTimeoutMs, collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet]())
    val producer = new SyncProducer(new SyncProducerConfig(props))
    val response = producer.send(emptyRequest)
    assertTrue(response == null)
  }

  @Test
  def testNotEnoughReplicas(): Unit =  {
    val topicName = "minisrtest"
    val server = servers.head
    val props = TestUtils.getSyncProducerConfig(boundPort(server))

    props.put("request.required.acks", "-1")

    val producer = new SyncProducer(new SyncProducerConfig(props))
    val topicProps = new Properties()
    topicProps.put("min.insync.replicas","2")
    adminZkClient.createTopic(topicName, 1, 1,topicProps)
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topicName, 0)

    val response = producer.send(produceRequest(topicName, 0,
      new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = new Message(messageBytes)),-1))

    assertEquals(Errors.NOT_ENOUGH_REPLICAS, response.status(TopicAndPartition(topicName, 0)).error)
  }
}
