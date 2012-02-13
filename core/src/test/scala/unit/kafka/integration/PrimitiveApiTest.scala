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

import scala.collection._
import java.io.File
import java.util.Properties
import junit.framework.Assert._
import kafka.common.{ErrorMapping, OffsetOutOfRangeException, InvalidPartitionException}
import kafka.message.{DefaultCompressionCodec, NoCompressionCodec, Message, ByteBufferMessageSet}
import kafka.producer.{ProducerData, Producer, ProducerConfig}
import kafka.serializer.StringDecoder
import kafka.server.{KafkaRequestHandler, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.log4j.{Level, Logger}
import org.scalatest.junit.JUnit3Suite
import java.nio.ByteBuffer
import kafka.api.{FetchRequest, FetchRequestBuilder, ProducerRequest}

/**
 * End to end tests of the primitive apis against a local server
 */
class PrimitiveApiTest extends JUnit3Suite with ProducerConsumerTestHarness with KafkaServerTestHarness {
  
  val port = TestUtils.choosePort
  val props = TestUtils.createBrokerConfig(0, port)
  val config = new KafkaConfig(props) {
    override val flushInterval = 1
  }
  val configs = List(config)
  val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])

  def testFetchRequestCanProperlySerialize() {
    val request = new FetchRequestBuilder()
      .correlationId(100)
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
  
  def testDefaultEncoderProducerAndFetch() {
    val topic = "test-topic"
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("broker.list", "0:localhost:" + port)
    val config = new ProducerConfig(props)

    val stringProducer1 = new Producer[String, String](config)
    stringProducer1.send(new ProducerData[String, String](topic, Array("test-message")))
    Thread.sleep(200)

    val request = new FetchRequestBuilder()
      .correlationId(100)
      .clientId("test-client")
      .addFetch(topic, 0, 0, 10000)
      .build()
    val fetched = consumer.fetch(request)
    assertEquals("Returned correlationId doesn't match that in request.", 100, fetched.correlationId)

    val messageSet = fetched.messageSet(topic, 0)
    assertTrue(messageSet.iterator.hasNext)

    val fetchedMessageAndOffset = messageSet.head
    val stringDecoder = new StringDecoder
    val fetchedStringMessage = stringDecoder.toEvent(fetchedMessageAndOffset.message)
    assertEquals("test-message", fetchedStringMessage)
  }

  def testDefaultEncoderProducerAndFetchWithCompression() {
    val topic = "test-topic"
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("broker.list", "0:localhost:" + port)
    props.put("compression", "true")
    val config = new ProducerConfig(props)

    val stringProducer1 = new Producer[String, String](config)
    stringProducer1.send(new ProducerData[String, String](topic, Array("test-message")))
    Thread.sleep(200)

    var fetched = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
    val messageSet = fetched.messageSet(topic, 0)
    assertTrue(messageSet.iterator.hasNext)

    val fetchedMessageAndOffset = messageSet.head
    val stringDecoder = new StringDecoder
    val fetchedStringMessage = stringDecoder.toEvent(fetchedMessageAndOffset.message)
    assertEquals("test-message", fetchedStringMessage)
  }

  def testProduceAndMultiFetch() {
    // send some messages
    val topics = List(("test4", 0), ("test1", 0), ("test2", 0), ("test3", 0));
    {
      val messages = new mutable.HashMap[String, ByteBufferMessageSet]
      val builder = new FetchRequestBuilder()
      for( (topic, partition) <- topics) {
        val set = new ByteBufferMessageSet(NoCompressionCodec,
                                           new Message(("a_" + topic).getBytes), new Message(("b_" + topic).getBytes))
        messages += topic -> set
        producer.send(topic, set)
        set.getBuffer.rewind
        builder.addFetch(topic, partition, 0, 10000)
      }

      // wait a bit for produced message to be available
      Thread.sleep(700)
      val request = builder.build()
      val response = consumer.fetch(request)
      for( (topic, partition) <- topics) {
        val fetched = response.messageSet(topic, partition)
        TestUtils.checkEquals(messages(topic).iterator, fetched.iterator)
      }
    }

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    {
      // send some invalid offsets
      val builder = new FetchRequestBuilder()
      for( (topic, partition) <- topics)
        builder.addFetch(topic, partition, -1, 10000)

      try {
        val request = builder.build()
        val response = consumer.fetch(request)
        for( (topic, partition) <- topics)
          response.messageSet(topic, partition).iterator
        fail("Expected exception when fetching message with invalid offset")
      } catch {
        case e: OffsetOutOfRangeException => "this is good"
      }
    }    

    {
      // send some invalid partitions
      val builder = new FetchRequestBuilder()
      for( (topic, partition) <- topics)
        builder.addFetch(topic, -1, 0, 10000)

      try {
        val request = builder.build()
        val response = consumer.fetch(request)
        for( (topic, partition) <- topics)
          response.messageSet(topic, -1).iterator
        fail("Expected exception when fetching message with invalid partition")
      } catch {
        case e: InvalidPartitionException => "this is good"
      }
    }

    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def testProduceAndMultiFetchWithCompression() {
    // send some messages
    val topics = List(("test4", 0), ("test1", 0), ("test2", 0), ("test3", 0));
    {
      val messages = new mutable.HashMap[String, ByteBufferMessageSet]
      val builder = new FetchRequestBuilder()
      for( (topic, partition) <- topics) {
        val set = new ByteBufferMessageSet(DefaultCompressionCodec,
                                           new Message(("a_" + topic).getBytes), new Message(("b_" + topic).getBytes))
        messages += topic -> set
        producer.send(topic, set)
        set.getBuffer.rewind
        builder.addFetch(topic, partition, 0, 10000)
      }

      // wait a bit for produced message to be available
      Thread.sleep(200)
      val request = builder.build()
      val response = consumer.fetch(request)
      for( (topic, partition) <- topics) {
        val fetched = response.messageSet(topic, partition)
        TestUtils.checkEquals(messages(topic).iterator, fetched.iterator)
      }
    }

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    {
      // send some invalid offsets
      val builder = new FetchRequestBuilder()
      for( (topic, partition) <- topics)
        builder.addFetch(topic, partition, -1, 10000)

      try {
        val request = builder.build()
        val response = consumer.fetch(request)
        for( (topic, partition) <- topics)
          response.messageSet(topic, partition).iterator
        fail("Expected exception when fetching message with invalid offset")
      } catch {
        case e: OffsetOutOfRangeException => "this is good"
      }
    }

    {
      // send some invalid partitions
      val builder = new FetchRequestBuilder()
      for( (topic, _) <- topics)
        builder.addFetch(topic, -1, 0, 10000)

      try {
        val request = builder.build()
        val response = consumer.fetch(request)
        for( (topic, _) <- topics)
          response.messageSet(topic, -1).iterator
        fail("Expected exception when fetching message with invalid partition")
      } catch {
        case e: InvalidPartitionException => "this is good"
      }
    }

    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def testMultiProduce() {
    // send some messages
    val topics = List(("test4", 0), ("test1", 0), ("test2", 0), ("test3", 0));
    val messages = new mutable.HashMap[String, ByteBufferMessageSet]
    val builder = new FetchRequestBuilder()
    var produceList: List[ProducerRequest] = Nil
    for( (topic, partition) <- topics) {
      val set = new ByteBufferMessageSet(NoCompressionCodec,
                                         new Message(("a_" + topic).getBytes), new Message(("b_" + topic).getBytes))
      messages += topic -> set
      produceList ::= new ProducerRequest(topic, 0, set)
      builder.addFetch(topic, partition, 0, 10000)
    }
    producer.multiSend(produceList.toArray)

    for (messageSet <- messages.values)
      messageSet.getBuffer.rewind
      
    // wait a bit for produced message to be available
    Thread.sleep(200)
    val request = builder.build()
    val response = consumer.fetch(request)
    for( (topic, partition) <- topics) {
      val fetched = response.messageSet(topic, partition)
      TestUtils.checkEquals(messages(topic).iterator, fetched.iterator)
    }
  }

  def testMultiProduceWithCompression() {
    // send some messages
    val topics = List(("test4", 0), ("test1", 0), ("test2", 0), ("test3", 0));
    val messages = new mutable.HashMap[String, ByteBufferMessageSet]
    val builder = new FetchRequestBuilder()
    var produceList: List[ProducerRequest] = Nil
    for( (topic, partition) <- topics) {
      val set = new ByteBufferMessageSet(DefaultCompressionCodec,
                                         new Message(("a_" + topic).getBytes), new Message(("b_" + topic).getBytes))
      messages += topic -> set
      produceList ::= new ProducerRequest(topic, 0, set)
      builder.addFetch(topic, partition, 0, 10000)
    }
    producer.multiSend(produceList.toArray)

    for (messageSet <- messages.values)
      messageSet.getBuffer.rewind

    // wait a bit for produced message to be available
    Thread.sleep(200)
    val request = builder.build()
    val response = consumer.fetch(request)
    for( (topic, partition) <- topics) {
      val fetched = response.messageSet(topic, 0)
      TestUtils.checkEquals(messages(topic).iterator, fetched.iterator)
    }
  }

  def testConsumerNotExistTopic() {
    val newTopic = "new-topic"
    val fetchResponse = consumer.fetch(new FetchRequestBuilder().addFetch(newTopic, 0, 0, 10000).build())
    assertFalse(fetchResponse.messageSet(newTopic, 0).iterator.hasNext)
    val logFile = new File(config.logDir, newTopic + "-0")
    assertTrue(!logFile.exists)
  }
}
