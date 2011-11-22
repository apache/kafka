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
import junit.framework.Assert._
import kafka.api.{ProducerRequest, FetchRequest}
import kafka.common.{OffsetOutOfRangeException, InvalidPartitionException}
import kafka.server.{KafkaRequestHandlers, KafkaConfig}
import org.apache.log4j.{Level, Logger}
import org.scalatest.junit.JUnit3Suite
import java.util.Properties
import kafka.producer.{ProducerData, Producer, ProducerConfig}
import kafka.serializer.StringDecoder
import kafka.utils.TestUtils
import kafka.message.{DefaultCompressionCodec, NoCompressionCodec, Message, ByteBufferMessageSet}
import java.io.File

/**
 * End to end tests of the primitive apis against a local server
 */
class PrimitiveApiTest extends JUnit3Suite with ProducerConsumerTestHarness with KafkaServerTestHarness {
  
  val port = TestUtils.choosePort
  val props = TestUtils.createBrokerConfig(0, port)
  val config = new KafkaConfig(props) {
                 override val enableZookeeper = false
               }
  val configs = List(config)
  val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandlers])

  def testDefaultEncoderProducerAndFetch() {
    val topic = "test-topic"
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("broker.list", "0:localhost:" + port)
    val config = new ProducerConfig(props)

    val stringProducer1 = new Producer[String, String](config)
    stringProducer1.send(new ProducerData[String, String](topic, "test", Array("test-message")))
    Thread.sleep(200)

    var fetched = consumer.fetch(new FetchRequest(topic, 0, 0, 10000))
    assertTrue(fetched.iterator.hasNext)

    val fetchedMessageAndOffset = fetched.iterator.next
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
    stringProducer1.send(new ProducerData[String, String](topic, "test", Array("test-message")))
    Thread.sleep(200)

    var fetched = consumer.fetch(new FetchRequest(topic, 0, 0, 10000))
    assertTrue(fetched.iterator.hasNext)

    val fetchedMessageAndOffset = fetched.iterator.next
    val stringDecoder = new StringDecoder
    val fetchedStringMessage = stringDecoder.toEvent(fetchedMessageAndOffset.message)
    assertEquals("test-message", fetchedStringMessage)
  }

  def testProduceAndMultiFetch() {
    // send some messages
    val topics = List("test1", "test2", "test3");
    {
      val messages = new mutable.HashMap[String, ByteBufferMessageSet]
      val fetches = new mutable.ArrayBuffer[FetchRequest]
      for(topic <- topics) {
        val set = new ByteBufferMessageSet(NoCompressionCodec,
                                           new Message(("a_" + topic).getBytes), new Message(("b_" + topic).getBytes))
        messages += topic -> set
        producer.send(topic, set)
        set.getBuffer.rewind
        fetches += new FetchRequest(topic, 0, 0, 10000)
      }

      // wait a bit for produced message to be available
      Thread.sleep(700)
      val response = consumer.multifetch(fetches: _*)
      for((topic, resp) <- topics.zip(response.toList))
        TestUtils.checkEquals(messages(topic).iterator, resp.iterator)
    }

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    {
      // send some invalid offsets
      val fetches = new mutable.ArrayBuffer[FetchRequest]
      for(topic <- topics)
        fetches += new FetchRequest(topic, 0, -1, 10000)

      try {
        val responses = consumer.multifetch(fetches: _*)
        for(resp <- responses)
          resp.iterator
        fail("expect exception")
      }
      catch {
        case e: OffsetOutOfRangeException => "this is good"
      }
    }    

    {
      // send some invalid partitions
      val fetches = new mutable.ArrayBuffer[FetchRequest]
      for(topic <- topics)
        fetches += new FetchRequest(topic, -1, 0, 10000)

      try {
        val responses = consumer.multifetch(fetches: _*)
        for(resp <- responses)
          resp.iterator
        fail("expect exception")
      }
      catch {
        case e: InvalidPartitionException => "this is good"
      }
    }

    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def testProduceAndMultiFetchWithCompression() {
    // send some messages
    val topics = List("test1", "test2", "test3");
    {
      val messages = new mutable.HashMap[String, ByteBufferMessageSet]
      val fetches = new mutable.ArrayBuffer[FetchRequest]
      for(topic <- topics) {
        val set = new ByteBufferMessageSet(DefaultCompressionCodec,
                                           new Message(("a_" + topic).getBytes), new Message(("b_" + topic).getBytes))
        messages += topic -> set
        producer.send(topic, set)
        set.getBuffer.rewind
        fetches += new FetchRequest(topic, 0, 0, 10000)
      }

      // wait a bit for produced message to be available
      Thread.sleep(200)
      val response = consumer.multifetch(fetches: _*)
      for((topic, resp) <- topics.zip(response.toList))
        TestUtils.checkEquals(messages(topic).iterator, resp.iterator)
    }

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    {
      // send some invalid offsets
      val fetches = new mutable.ArrayBuffer[FetchRequest]
      for(topic <- topics)
        fetches += new FetchRequest(topic, 0, -1, 10000)

      try {
        val responses = consumer.multifetch(fetches: _*)
        for(resp <- responses)
          resp.iterator
        fail("expect exception")
      }
      catch {
        case e: OffsetOutOfRangeException => "this is good"
      }
    }

    {
      // send some invalid partitions
      val fetches = new mutable.ArrayBuffer[FetchRequest]
      for(topic <- topics)
        fetches += new FetchRequest(topic, -1, 0, 10000)

      try {
        val responses = consumer.multifetch(fetches: _*)
        for(resp <- responses)
          resp.iterator
        fail("expect exception")
      }
      catch {
        case e: InvalidPartitionException => "this is good"
      }
    }

    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def testMultiProduce() {
    // send some messages
    val topics = List("test1", "test2", "test3");
    val messages = new mutable.HashMap[String, ByteBufferMessageSet]
    val fetches = new mutable.ArrayBuffer[FetchRequest]
    var produceList: List[ProducerRequest] = Nil
    for(topic <- topics) {
      val set = new ByteBufferMessageSet(NoCompressionCodec,
                                         new Message(("a_" + topic).getBytes), new Message(("b_" + topic).getBytes))
      messages += topic -> set
      produceList ::= new ProducerRequest(topic, 0, set)
      fetches += new FetchRequest(topic, 0, 0, 10000)
    }
    producer.multiSend(produceList.toArray)

    for (messageSet <- messages.values)
      messageSet.getBuffer.rewind
      
    // wait a bit for produced message to be available
    Thread.sleep(200)
    val response = consumer.multifetch(fetches: _*)
    for((topic, resp) <- topics.zip(response.toList))
      TestUtils.checkEquals(messages(topic).iterator, resp.iterator)
  }

  def testMultiProduceWithCompression() {
    // send some messages
    val topics = List("test1", "test2", "test3");
    val messages = new mutable.HashMap[String, ByteBufferMessageSet]
    val fetches = new mutable.ArrayBuffer[FetchRequest]
    var produceList: List[ProducerRequest] = Nil
    for(topic <- topics) {
      val set = new ByteBufferMessageSet(DefaultCompressionCodec,
                                         new Message(("a_" + topic).getBytes), new Message(("b_" + topic).getBytes))
      messages += topic -> set
      produceList ::= new ProducerRequest(topic, 0, set)
      fetches += new FetchRequest(topic, 0, 0, 10000)
    }
    producer.multiSend(produceList.toArray)

    for (messageSet <- messages.values)
      messageSet.getBuffer.rewind

    // wait a bit for produced message to be available
    Thread.sleep(200)
    val response = consumer.multifetch(fetches: _*)
    for((topic, resp) <- topics.zip(response.toList))
      TestUtils.checkEquals(messages(topic).iterator, resp.iterator)
  }

  def testConsumerNotExistTopic() {
    val newTopic = "new-topic"
    val messageSetIter = consumer.fetch(new FetchRequest(newTopic, 0, 0, 10000)).iterator
    assertTrue(messageSetIter.hasNext == false)
    val logFile = new File(config.logDir, newTopic + "-0")
    assertTrue(!logFile.exists)
  }
}
