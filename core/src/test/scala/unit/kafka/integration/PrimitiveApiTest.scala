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

import java.nio.ByteBuffer

import org.junit.Assert._
import kafka.api.{FetchRequest, FetchRequestBuilder, PartitionFetchInfo}
import kafka.server.{KafkaConfig, KafkaRequestHandler}
import kafka.producer.{KeyedMessage, Producer}
import org.apache.log4j.{Level, Logger}
import org.junit.Test

import scala.collection._
import kafka.common.{ErrorMapping, OffsetOutOfRangeException, TopicAndPartition, UnknownTopicOrPartitionException}
import kafka.utils.{StaticPartitioner, TestUtils}
import kafka.serializer.StringEncoder
import java.util.Properties

import org.apache.kafka.common.TopicPartition

/**
 * End to end tests of the primitive apis against a local server
 */
@deprecated("This test has been deprecated and it will be removed in a future release", "0.10.0.0")
class PrimitiveApiTest extends ProducerConsumerTestHarness {
  val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])

  def generateConfigs = List(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))

  @Test
  def testFetchRequestCanProperlySerialize(): Unit = {
    val request = new FetchRequestBuilder()
      .clientId("test-client")
      .maxWait(10001)
      .minBytes(4444)
      .addFetch("topic1", 0, 0, 10000)
      .addFetch("topic2", 1, 1024, 9999)
      .addFetch("topic1", 1, 256, 444)
      .build()
    val serializedBuffer = ByteBuffer.allocate(request.sizeInBytes)
    request.writeTo(serializedBuffer)
    serializedBuffer.rewind()
    val deserializedRequest = FetchRequest.readFrom(serializedBuffer)
    assertEquals(request, deserializedRequest)
  }

  @Test
  def testEmptyFetchRequest(): Unit = {
    val partitionRequests = immutable.Map[TopicAndPartition, PartitionFetchInfo]()
    val request = new FetchRequest(requestInfo = partitionRequests.toVector)
    val fetched = consumer.fetch(request)
    assertTrue(!fetched.hasError && fetched.data.isEmpty)
  }

  @Test
  def testDefaultEncoderProducerAndFetch(): Unit = {
    val topic = "test-topic"

    producer.send(new KeyedMessage[String, String](topic, "test-message"))

    val replica = servers.head.replicaManager.getReplica(new TopicPartition(topic, 0)).get
    assertTrue("HighWatermark should equal logEndOffset with just 1 replica",
               replica.logEndOffset.messageOffset > 0 && replica.logEndOffset.equals(replica.highWatermark))

    val request = new FetchRequestBuilder()
      .clientId("test-client")
      .addFetch(topic, 0, 0, 10000)
      .build()
    val fetched = consumer.fetch(request)
    assertEquals("Returned correlationId doesn't match that in request.", 0, fetched.correlationId)

    val messageSet = fetched.messageSet(topic, 0)
    assertTrue(messageSet.iterator.hasNext)

    val fetchedMessageAndOffset = messageSet.head
    assertEquals("test-message", TestUtils.readString(fetchedMessageAndOffset.message.payload, "UTF-8"))
  }

  @Test
  def testDefaultEncoderProducerAndFetchWithCompression(): Unit = {
    val topic = "test-topic"
    val props = new Properties()
    props.put("compression.codec", "gzip")

    val stringProducer1 = TestUtils.createProducer[String, String](
      TestUtils.getBrokerListStrFromServers(servers),
      encoder = classOf[StringEncoder].getName,
      keyEncoder = classOf[StringEncoder].getName,
      partitioner = classOf[StaticPartitioner].getName,
      producerProps = props)

    stringProducer1.send(new KeyedMessage[String, String](topic, "test-message"))

    val fetched = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
    val messageSet = fetched.messageSet(topic, 0)
    assertTrue(messageSet.iterator.hasNext)

    val fetchedMessageAndOffset = messageSet.head
    assertEquals("test-message", TestUtils.readString(fetchedMessageAndOffset.message.payload, "UTF-8"))
  }

  private def produceAndMultiFetch(producer: Producer[String, String]): Unit = {
    for(topic <- List("test1", "test2", "test3", "test4"))
      createTopic(topic)

    // send some messages
    val topics = List(("test4", 0), ("test1", 0), ("test2", 0), ("test3", 0));
    {
      val messages = new mutable.HashMap[String, Seq[String]]
      val builder = new FetchRequestBuilder()
      for( (topic, partition) <- topics) {
        val messageList = List("a_" + topic, "b_" + topic)
        val producerData = messageList.map(new KeyedMessage[String, String](topic, topic, _))
        messages += topic -> messageList
        producer.send(producerData:_*)
        builder.addFetch(topic, partition, 0, 10000)
      }

      val request = builder.build()
      val response = consumer.fetch(request)
      for((topic, partition) <- topics) {
        val fetched = response.messageSet(topic, partition)
        assertEquals(messages(topic), fetched.map(messageAndOffset => TestUtils.readString(messageAndOffset.message.payload)))
      }
    }

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    {
      // send some invalid offsets
      val builder = new FetchRequestBuilder()
      for((topic, partition) <- topics)
        builder.addFetch(topic, partition, -1, 10000)

      try {
        val request = builder.build()
        val response = consumer.fetch(request)
        response.data.foreach(pdata => ErrorMapping.maybeThrowException(pdata._2.error.code))
        fail("Expected exception when fetching message with invalid offset")
      } catch {
        case _: OffsetOutOfRangeException => // This is good.
      }
    }

    {
      // send some invalid partitions
      val builder = new FetchRequestBuilder()
      for((topic, _) <- topics)
        builder.addFetch(topic, -1, 0, 10000)

      try {
        val request = builder.build()
        val response = consumer.fetch(request)
        response.data.foreach(pdata => ErrorMapping.maybeThrowException(pdata._2.error.code))
        fail("Expected exception when fetching message with invalid partition")
      } catch {
        case _: UnknownTopicOrPartitionException => // This is good.
      }
    }

    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  @Test
  def testProduceAndMultiFetch(): Unit = {
    produceAndMultiFetch(producer)
  }

  private def multiProduce(producer: Producer[String, String]): Unit = {
    val topics = Map("test4" -> 0, "test1" -> 0, "test2" -> 0, "test3" -> 0)
    topics.keys.map(topic => createTopic(topic))

    val messages = new mutable.HashMap[String, Seq[String]]
    val builder = new FetchRequestBuilder()
    for((topic, partition) <- topics) {
      val messageList = List("a_" + topic, "b_" + topic)
      val producerData = messageList.map(new KeyedMessage[String, String](topic, topic, _))
      messages += topic -> messageList
      producer.send(producerData:_*)
      builder.addFetch(topic, partition, 0, 10000)
    }

    val request = builder.build()
    val response = consumer.fetch(request)
    for((topic, partition) <- topics) {
      val fetched = response.messageSet(topic, partition)
      assertEquals(messages(topic), fetched.map(messageAndOffset => TestUtils.readString(messageAndOffset.message.payload)))
    }
  }

  @Test
  def testMultiProduce(): Unit = {
    multiProduce(producer)
  }

  @Test
  def testConsumerEmptyTopic(): Unit = {
    val newTopic = "new-topic"
    createTopic(newTopic, numPartitions = 1, replicationFactor = 1)

    val fetchResponse = consumer.fetch(new FetchRequestBuilder().addFetch(newTopic, 0, 0, 10000).build())
    assertFalse(fetchResponse.messageSet(newTopic, 0).iterator.hasNext)
  }

  @Test
  def testPipelinedProduceRequests(): Unit = {
    val topics = Map("test4" -> 0, "test1" -> 0, "test2" -> 0, "test3" -> 0)
    topics.keys.map(topic => createTopic(topic))
    val props = new Properties()
    props.put("request.required.acks", "0")
    val pipelinedProducer: Producer[String, String] =
      TestUtils.createProducer[String, String](
        TestUtils.getBrokerListStrFromServers(servers),
        encoder = classOf[StringEncoder].getName,
        keyEncoder = classOf[StringEncoder].getName,
        partitioner = classOf[StaticPartitioner].getName,
        producerProps = props)

    // send some messages
    val messages = new mutable.HashMap[String, Seq[String]]
    val builder = new FetchRequestBuilder()
    for( (topic, partition) <- topics) {
      val messageList = List("a_" + topic, "b_" + topic)
      val producerData = messageList.map(new KeyedMessage[String, String](topic, topic, _))
      messages += topic -> messageList
      pipelinedProducer.send(producerData:_*)
      builder.addFetch(topic, partition, 0, 10000)
    }

    // wait until the messages are published
    TestUtils.waitUntilTrue(() => { servers.head.logManager.getLog(new TopicPartition("test1", 0)).get.logEndOffset == 2 },
                            "Published messages should be in the log")
    TestUtils.waitUntilTrue(() => { servers.head.logManager.getLog(new TopicPartition("test2", 0)).get.logEndOffset == 2 },
                            "Published messages should be in the log")
    TestUtils.waitUntilTrue(() => { servers.head.logManager.getLog(new TopicPartition("test3", 0)).get.logEndOffset == 2 },
                            "Published messages should be in the log")
    TestUtils.waitUntilTrue(() => { servers.head.logManager.getLog(new TopicPartition("test4", 0)).get.logEndOffset == 2 },
                            "Published messages should be in the log")

    val replicaId = servers.head.config.brokerId
    TestUtils.waitUntilTrue(() => { servers.head.replicaManager.getReplica(new TopicPartition("test1", 0), replicaId).get.highWatermark.messageOffset == 2 },
                            "High watermark should equal to log end offset")
    TestUtils.waitUntilTrue(() => { servers.head.replicaManager.getReplica(new TopicPartition("test2", 0), replicaId).get.highWatermark.messageOffset == 2 },
                            "High watermark should equal to log end offset")
    TestUtils.waitUntilTrue(() => { servers.head.replicaManager.getReplica(new TopicPartition("test3", 0), replicaId).get.highWatermark.messageOffset == 2 },
                            "High watermark should equal to log end offset")
    TestUtils.waitUntilTrue(() => { servers.head.replicaManager.getReplica(new TopicPartition("test4", 0), replicaId).get.highWatermark.messageOffset == 2 },
                            "High watermark should equal to log end offset")

    // test if the consumer received the messages in the correct order when producer has enabled request pipelining
    val request = builder.build()
    val response = consumer.fetch(request)
    for( (topic, partition) <- topics) {
      val fetched = response.messageSet(topic, partition)
      assertEquals(messages(topic), fetched.map(messageAndOffset => TestUtils.readString(messageAndOffset.message.payload)))
    }
  }
}
