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
import kafka.common.OffsetOutOfRangeException
import kafka.api.{ProducerRequest, FetchRequest}
import kafka.server.{KafkaRequestHandlers, KafkaServer, KafkaConfig}
import org.apache.log4j.{Level, Logger}
import org.scalatest.junit.JUnit3Suite
import kafka.utils.{TestUtils, Utils}
import kafka.message.{NoCompressionCodec, Message, ByteBufferMessageSet}

/**
 * End to end tests of the primitive apis against a local server
 */
class LazyInitProducerTest extends JUnit3Suite with ProducerConsumerTestHarness   {

  val port = TestUtils.choosePort
  val props = TestUtils.createBrokerConfig(0, port)
  val config = new KafkaConfig(props) {
                 override val enableZookeeper = false
               }
  val configs = List(config)
  var servers: List[KafkaServer] = null
  val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandlers])

  override def setUp() {
    super.setUp
    if(configs.size <= 0)
      throw new IllegalArgumentException("Must suply at least one server config.")
    servers = configs.map(TestUtils.createServer(_))

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)    
  }

  override def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)

    super.tearDown    
    servers.map(server => server.shutdown())
    servers.map(server => Utils.rm(server.config.logDir))
  }
  
  def testProduceAndFetch() {
    // send some messages
    val topic = "test"
    val sent = new ByteBufferMessageSet(NoCompressionCodec,
                                        new Message("hello".getBytes()), new Message("there".getBytes()))
    producer.send(topic, sent)
    sent.getBuffer.rewind
    var fetched: ByteBufferMessageSet = null
    while(fetched == null || fetched.validBytes == 0)
      fetched = consumer.fetch(new FetchRequest(topic, 0, 0, 10000))
    TestUtils.checkEquals(sent.iterator, fetched.iterator)

    // send an invalid offset
    var exceptionThrown = false
    try {
      val fetchedWithError = consumer.fetch(new FetchRequest(topic, 0, -1, 10000))
      fetchedWithError.iterator
    }
    catch {
      case e: OffsetOutOfRangeException => exceptionThrown = true
      case e2 => throw e2
    }
    assertTrue(exceptionThrown)
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
      Thread.sleep(200)
      val response = consumer.multifetch(fetches: _*)
      for((topic, resp) <- topics.zip(response.toList))
        TestUtils.checkEquals(messages(topic).iterator, resp.iterator)
    }

    {
      // send some invalid offsets
      val fetches = new mutable.ArrayBuffer[FetchRequest]
      for(topic <- topics)
        fetches += new FetchRequest(topic, 0, -1, 10000)

      var exceptionThrown = false
      try {
        val responses = consumer.multifetch(fetches: _*)
        for(resp <- responses)
          resp.iterator
      }
      catch {
        case e: OffsetOutOfRangeException => exceptionThrown = true
        case e2 => throw e2
      }
      assertTrue(exceptionThrown)
    }
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

  def testMultiProduceResend() {
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

    // resend the same multisend
    producer.multiSend(produceList.toArray)

    for (messageSet <- messages.values)
      messageSet.getBuffer.rewind

    // wait a bit for produced message to be available
    Thread.sleep(750)
    val response = consumer.multifetch(fetches: _*)
    for((topic, resp) <- topics.zip(response.toList))
      TestUtils.checkEquals(TestUtils.stackedIterator(messages(topic).map(m => m.message).iterator,
                                                      messages(topic).map(m => m.message).iterator),
                            resp.map(m => m.message).iterator)
//      TestUtils.checkEquals(TestUtils.stackedIterator(messages(topic).iterator, messages(topic).iterator), resp.iterator)
  }
}
