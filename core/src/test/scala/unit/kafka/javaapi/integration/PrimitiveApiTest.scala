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

package kafka.javaapi.integration

import scala.collection._
import kafka.api.FetchRequestBuilder
import kafka.common.{InvalidPartitionException, OffsetOutOfRangeException}
import kafka.javaapi.ProducerRequest
import kafka.javaapi.message.ByteBufferMessageSet
import kafka.message.{DefaultCompressionCodec, NoCompressionCodec, Message}
import kafka.server.{KafkaRequestHandler, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.log4j.{Level, Logger}
import org.scalatest.junit.JUnit3Suite

/**
 * End to end tests of the primitive apis against a local server
 */
class PrimitiveApiTest extends JUnit3Suite with ProducerConsumerTestHarness with kafka.integration.KafkaServerTestHarness {
  
  val port = 9999
  val props = TestUtils.createBrokerConfig(0, port)
  val config = new KafkaConfig(props)
  val configs = List(config)
  val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])

  def testProduceAndFetch() {
    // send some messages
    val topic = "test"

    // send an empty messageset first
    val sent2 = new ByteBufferMessageSet(NoCompressionCodec, getMessageList(Seq.empty[Message]: _*))
    producer.send(topic, sent2)

    Thread.sleep(200)
    sent2.getBuffer.rewind
    val fetched2 = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
    val fetchedMessage2 = fetched2.messageSet(topic, 0)
    TestUtils.checkEquals(sent2.iterator, fetchedMessage2.iterator)


    // send some messages
    val sent3 = new ByteBufferMessageSet(NoCompressionCodec,
                                         getMessageList(
                                           new Message("hello".getBytes()),new Message("there".getBytes())))
    producer.send(topic, sent3)

    Thread.sleep(200)
    sent3.getBuffer.rewind
    var messageSet: ByteBufferMessageSet = null
    while(messageSet == null || messageSet.validBytes == 0) {
      val fetched3 = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
      messageSet = fetched3.messageSet(topic, 0).asInstanceOf[ByteBufferMessageSet]
    }
    TestUtils.checkEquals(sent3.iterator, messageSet.iterator)

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    // send an invalid offset
    try {
      val fetchedWithError = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, -1, 10000).build())
      val messageWithError = fetchedWithError.messageSet(topic, 0)
      messageWithError.iterator
      fail("Fetch with invalid offset should throw an exception when iterating over response")
    } catch {
      case e: OffsetOutOfRangeException => "this is good"
    }

    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def testProduceAndFetchWithCompression() {
    // send some messages
    val topic = "test"

    // send an empty messageset first
    val sent2 = new ByteBufferMessageSet(DefaultCompressionCodec, getMessageList(Seq.empty[Message]: _*))
    producer.send(topic, sent2)

    Thread.sleep(200)
    sent2.getBuffer.rewind
    val fetched2 = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
    val message2 = fetched2.messageSet(topic, 0)
    TestUtils.checkEquals(sent2.iterator, message2.iterator)


    // send some messages
    val sent3 = new ByteBufferMessageSet( DefaultCompressionCodec,
                                          getMessageList(
                                            new Message("hello".getBytes()),new Message("there".getBytes())))
    producer.send(topic, sent3)

    Thread.sleep(200)
    sent3.getBuffer.rewind
    var fetchedMessage: ByteBufferMessageSet = null
    while(fetchedMessage == null || fetchedMessage.validBytes == 0) {
      val fetched3 = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
      fetchedMessage = fetched3.messageSet(topic, 0).asInstanceOf[ByteBufferMessageSet]
    }
    TestUtils.checkEquals(sent3.iterator, fetchedMessage.iterator)

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    // send an invalid offset
    try {
      val fetchedWithError = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, -1, 10000).build())
      val messageWithError = fetchedWithError.messageSet(topic, 0)
      messageWithError.iterator
      fail("Fetch with invalid offset should throw an exception when iterating over response")
    } catch {
      case e: OffsetOutOfRangeException => "this is good"
    }

    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def testProduceAndMultiFetch() {
    // send some messages
    val topics = List(("test4", 0), ("test1", 0), ("test2", 0), ("test3", 0));
    {
      val messages = new mutable.HashMap[String, ByteBufferMessageSet]
      val builder = new FetchRequestBuilder()
      for( (topic, partition) <- topics) {
        val set = new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                           messages = getMessageList(new Message(("a_" + topic).getBytes),
                                                                     new Message(("b_" + topic).getBytes)))
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
        val messageSet = response.messageSet(topic, partition)
        TestUtils.checkEquals(messages(topic).iterator, messageSet.iterator)
      }
    }

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    {
      // send some invalid offsets
      val builder = new FetchRequestBuilder()
      for( (topic, partition) <- topics)
         builder.addFetch(topic, partition, -1, 10000)

      val request = builder.build()
      val response = consumer.fetch(request)
      for( (topic, partition) <- topics) {
        try {
            val iter = response.messageSet(topic, partition).iterator
            while (iter.hasNext)
              iter.next
            fail("MessageSet for invalid offset should throw exception")
        } catch {
          case e: OffsetOutOfRangeException => "this is good"
        }
      }
    }    

    {
      // send some invalid partitions
      val builder = new FetchRequestBuilder()
      for( (topic, _) <- topics)
        builder.addFetch(topic, -1, 0, 10000)

      val request = builder.build()
      val response = consumer.fetch(request)
      for( (topic, _) <- topics) {
        try {
          val iter = response.messageSet(topic, -1).iterator
          while (iter.hasNext)
            iter.next
          fail("MessageSet for invalid partition should throw exception")
        } catch {
          case e: InvalidPartitionException => "this is good"
        }
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
        val set = new ByteBufferMessageSet(compressionCodec = DefaultCompressionCodec,
                                           messages = getMessageList(new Message(("a_" + topic).getBytes),
                                                                     new Message(("b_" + topic).getBytes)))
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
        val iter = response.messageSet(topic, partition).iterator
        if (iter.hasNext) {
          TestUtils.checkEquals(messages(topic).iterator, iter)
        } else {
          fail("fewer responses than expected")
        }
      }
    }

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    {
      // send some invalid offsets
      val builder = new FetchRequestBuilder()
      for( (topic, partition) <- topics)
        builder.addFetch(topic, partition, -1, 10000)

      val request = builder.build()
      val response = consumer.fetch(request)
      for( (topic, partition) <- topics) {
        try {
          val iter = response.messageSet(topic, partition).iterator
          while (iter.hasNext)
            iter.next
          fail("Expected exception when fetching invalid offset")
        } catch {
          case e: OffsetOutOfRangeException => "this is good"
        }
      }
    }

    {
      // send some invalid partitions
      val builder = new FetchRequestBuilder()
      for( (topic, _) <- topics)
        builder.addFetch(topic, -1, 0, 10000)

      val request = builder.build()
      val response = consumer.fetch(request)
      for( (topic, _) <- topics) {
        try {
          val iter = response.messageSet(topic, -1).iterator
          while (iter.hasNext)
            iter.next
          fail("Expected exception when fetching invalid partition")
        } catch {
          case e: InvalidPartitionException => "this is good"
        }
      }
    }

    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def testProduceAndMultiFetchJava() {
    // send some messages
    val topics = List(("test4", 0), ("test1", 0), ("test2", 0), ("test3", 0));
    {
      val messages = new mutable.HashMap[String, ByteBufferMessageSet]
      val builder = new FetchRequestBuilder()
      for( (topic, partition) <- topics) {
        val set = new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                           messages = getMessageList(new Message(("a_" + topic).getBytes),
                                                                     new Message(("b_" + topic).getBytes)))
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
        val iter = response.messageSet(topic, partition).iterator
        if (iter.hasNext) {
          TestUtils.checkEquals(messages(topic).iterator, iter)
        } else {
          fail("fewer responses than expected")
        }
      }
    }
  }

  def testProduceAndMultiFetchJavaWithCompression() {
    // send some messages
    val topics = List(("test4", 0), ("test1", 0), ("test2", 0), ("test3", 0));
    {
      val messages = new mutable.HashMap[String, ByteBufferMessageSet]
      val builder = new FetchRequestBuilder()
      for( (topic, partition) <- topics) {
        val set = new ByteBufferMessageSet(compressionCodec = DefaultCompressionCodec,
                                           messages = getMessageList(new Message(("a_" + topic).getBytes),
                                                                     new Message(("b_" + topic).getBytes)))
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
        val iter = response.messageSet(topic, partition).iterator
        TestUtils.checkEquals(messages(topic).iterator, iter)
      }
    }
  }

  def testMultiProduce() {
    // send some messages
    val topics = List(("test4", 0), ("test1", 0), ("test2", 0), ("test3", 0));
    val messages = new mutable.HashMap[String, ByteBufferMessageSet]
    val builder = new FetchRequestBuilder()
    var produceList: List[ProducerRequest] = Nil
    for( (topic, partition) <- topics) {
      val set = new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                         messages = getMessageList(new Message(("a_" + topic).getBytes),
                                                                   new Message(("b_" + topic).getBytes)))
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
      val iter = response.messageSet(topic, partition).iterator
      if (iter.hasNext) {
        TestUtils.checkEquals(messages(topic).iterator, iter)
      } else {
        fail("fewer responses than expected")
      }
    }
  }

  def testMultiProduceWithCompression() {
    // send some messages
    val topics = List(("test4", 0), ("test1", 0), ("test2", 0), ("test3", 0));
    val messages = new mutable.HashMap[String, ByteBufferMessageSet]
    val builder = new FetchRequestBuilder()
    var produceList: List[ProducerRequest] = Nil
    for( (topic, partition) <- topics) {
      val set = new ByteBufferMessageSet(compressionCodec = DefaultCompressionCodec,
                                         messages = getMessageList(new Message(("a_" + topic).getBytes),
                                                                   new Message(("b_" + topic).getBytes)))
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
      val iter = response.messageSet(topic, partition).iterator
      if (iter.hasNext) {
        TestUtils.checkEquals(messages(topic).iterator, iter)
      } else {
        fail("fewer responses than expected")
      }
    }
  }

  private def getMessageList(messages: Message*): java.util.List[Message] = {
    val messageList = new java.util.ArrayList[Message]()
    messages.foreach(m => messageList.add(m))
    messageList
  }
}
