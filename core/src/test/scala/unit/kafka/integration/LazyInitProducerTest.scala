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

import kafka.api.FetchRequestBuilder
import kafka.message.ByteBufferMessageSet
import kafka.server.{KafkaRequestHandler, KafkaConfig}
import org.apache.log4j.{Level, Logger}
import org.junit.Assert._
import org.scalatest.junit.JUnit3Suite
import scala.collection._
import kafka.producer.KeyedMessage
import kafka.utils._
import kafka.common.{ErrorMapping, KafkaException, OffsetOutOfRangeException}

/**
 * End to end tests of the primitive apis against a local server
 */
class LazyInitProducerTest extends JUnit3Suite with ProducerConsumerTestHarness {

  val port = TestUtils.choosePort
  val props = TestUtils.createBrokerConfig(0, port)
  val config = new KafkaConfig(props)
  val configs = List(config)
  val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])

  override def setUp() {
    super.setUp
    if(configs.size <= 0)
      throw new KafkaException("Must suply at least one server config.")

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)    
  }

  override def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)

    super.tearDown    
  }
  
  def testProduceAndFetch() {
    // send some messages
    val topic = "test"
    val sentMessages = List("hello", "there")
    val producerData = sentMessages.map(m => new KeyedMessage[String, String](topic, topic, m))

    producer.send(producerData:_*)

    var fetchedMessage: ByteBufferMessageSet = null
    while(fetchedMessage == null || fetchedMessage.validBytes == 0) {
      val fetched = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
      fetchedMessage = fetched.messageSet(topic, 0)
    }
    assertEquals(sentMessages, fetchedMessage.map(m => Utils.readString(m.message.payload)).toList)

    // send an invalid offset
    try {
      val fetchedWithError = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, -1, 10000).build())
      fetchedWithError.data.values.foreach(pdata => ErrorMapping.maybeThrowException(pdata.error))
      fail("Expected an OffsetOutOfRangeException exception to be thrown")
    } catch {
      case e: OffsetOutOfRangeException => 
    }
  }

  def testProduceAndMultiFetch() {
    // send some messages, with non-ordered topics
    val topicOffsets = List(("test4", 0), ("test1", 0), ("test2", 0), ("test3", 0));
    {
      val messages = new mutable.HashMap[String, Seq[String]]
      val builder = new FetchRequestBuilder()
      for( (topic, offset) <- topicOffsets) {
        val producedData = List("a_" + topic, "b_" + topic)
        messages += topic -> producedData
        producer.send(producedData.map(m => new KeyedMessage[String, String](topic, topic, m)):_*)
        builder.addFetch(topic, offset, 0, 10000)
      }

      // wait a bit for produced message to be available
      val request = builder.build()
      val response = consumer.fetch(request)
      for( (topic, offset) <- topicOffsets) {
        val fetched = response.messageSet(topic, offset)
        assertEquals(messages(topic), fetched.map(m => Utils.readString(m.message.payload)))
      }
    }

    // send some invalid offsets
    val builder = new FetchRequestBuilder()
    for((topic, offset) <- topicOffsets)
      builder.addFetch(topic, offset, -1, 10000)

    val request = builder.build()
    val responses = consumer.fetch(request)
    responses.data.values.foreach(pd => {
      try {
        ErrorMapping.maybeThrowException(pd.error)
        fail("Expected an OffsetOutOfRangeException exception to be thrown")
      } catch {
        case e: OffsetOutOfRangeException => // this is good
      }
    })
  }

  def testMultiProduce() {
    // send some messages
    val topics = List("test1", "test2", "test3");
    val messages = new mutable.HashMap[String, Seq[String]]
    val builder = new FetchRequestBuilder()
    var produceList: List[KeyedMessage[String, String]] = Nil
    for(topic <- topics) {
      val set = List("a_" + topic, "b_" + topic)
      messages += topic -> set
      produceList ++= set.map(new KeyedMessage[String, String](topic, topic, _))
      builder.addFetch(topic, 0, 0, 10000)
    }
    producer.send(produceList: _*)

    // wait a bit for produced message to be available
    val request = builder.build()
    val response = consumer.fetch(request)
    for(topic <- topics) {
      val fetched = response.messageSet(topic, 0)
      assertEquals(messages(topic), fetched.map(m => Utils.readString(m.message.payload)))
    }
  }

  def testMultiProduceResend() {
    // send some messages
    val topics = List("test1", "test2", "test3");
    val messages = new mutable.HashMap[String, Seq[String]]
    val builder = new FetchRequestBuilder()
    var produceList: List[KeyedMessage[String, String]] = Nil
    for(topic <- topics) {
      val set = List("a_" + topic, "b_" + topic)
      messages += topic -> set
      produceList ++= set.map(new KeyedMessage[String, String](topic, topic, _))
      builder.addFetch(topic, 0, 0, 10000)
    }
    producer.send(produceList: _*)

    producer.send(produceList: _*)
    // wait a bit for produced message to be available
    val request = builder.build()
    val response = consumer.fetch(request)
    for(topic <- topics) {
      val topicMessages = response.messageSet(topic, 0)
      assertEquals(messages(topic) ++ messages(topic), topicMessages.map(m => Utils.readString(m.message.payload)))
    }
  }
}
