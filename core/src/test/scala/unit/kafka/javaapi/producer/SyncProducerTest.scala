/*
 * Copyright 2010 LinkedIn
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.javaapi.producer

import junit.framework.{Assert, TestCase}
import kafka.utils.SystemTime
import kafka.utils.TestUtils
import kafka.server.{KafkaServer, KafkaConfig}
import org.apache.log4j.{Logger, Level}
import org.scalatest.junit.JUnitSuite
import org.junit.{After, Before, Test}
import java.util.Properties
import kafka.producer.SyncProducerConfig
import kafka.javaapi.message.ByteBufferMessageSet
import kafka.javaapi.ProducerRequest
import kafka.message.{NoCompressionCodec, Message}

class SyncProducerTest extends JUnitSuite {
  private var messageBytes =  new Array[Byte](2);
  private var server: KafkaServer = null
  val simpleProducerLogger = Logger.getLogger(classOf[kafka.producer.SyncProducer])

  @Before
  def setUp() {
    server = TestUtils.createServer(new KafkaConfig(TestUtils.createBrokerConfig(0, 9092))
    {
      override val enableZookeeper = false
    })
  }

  @After
  def tearDown() {
    server.shutdown
  }

  @Test
  def testUnreachableServer() {
    val props = new Properties()
    props.put("host", "NOT_USED")
    props.put("port", "9092")
    props.put("buffer.size", "102400")
    props.put("connect.timeout.ms", "300")
    props.put("reconnect.interval", "1000")
    val producer = new SyncProducer(new SyncProducerConfig(props))
    var failed = false
    val firstStart = SystemTime.milliseconds

    //temporarily increase log4j level to avoid error in output
    simpleProducerLogger.setLevel(Level.FATAL)
    try {
      producer.send("test", 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                                        messages = getMessageList(new Message(messageBytes))))
    }catch {
      case e: Exception => failed = true
    }
    Assert.assertTrue(failed)
    failed = false
    val firstEnd = SystemTime.milliseconds
    println("First message send retries took " + (firstEnd-firstStart) + " ms")
    Assert.assertTrue((firstEnd-firstStart) < 300)

    val secondStart = SystemTime.milliseconds
    try {
      producer.send("test", 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                                        messages = getMessageList(new Message(messageBytes))))
    }catch {
      case e: Exception => failed = true

    }
    val secondEnd = SystemTime.milliseconds
    println("Second message send retries took " + (secondEnd-secondStart) + " ms")
    Assert.assertTrue((secondEnd-secondEnd) < 300)
    simpleProducerLogger.setLevel(Level.ERROR)
  }

  @Test
  def testReachableServer() {
    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", "9092")
    props.put("buffer.size", "102400")
    props.put("connect.timeout.ms", "500")
    props.put("reconnect.interval", "1000")
    val producer = new SyncProducer(new SyncProducerConfig(props))
    var failed = false
    val firstStart = SystemTime.milliseconds
    try {
      producer.send("test", 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                                        messages = getMessageList(new Message(messageBytes))))
    }catch {
      case e: Exception => failed=true
    }
    Assert.assertFalse(failed)
    failed = false
    val firstEnd = SystemTime.milliseconds
    Assert.assertTrue((firstEnd-firstStart) < 500)
    val secondStart = SystemTime.milliseconds
    try {
      producer.send("test", 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                                        messages = getMessageList(new Message(messageBytes))))
    }catch {
      case e: Exception => failed = true
    }
    Assert.assertFalse(failed)
    val secondEnd = SystemTime.milliseconds
    Assert.assertTrue((secondEnd-secondEnd) < 500)

    try {
      producer.multiSend(Array(new ProducerRequest("test", 0,
        new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                 messages = getMessageList(new Message(messageBytes))))))
    }catch {
      case e: Exception => failed=true
    }
    Assert.assertFalse(failed)
  }

  @Test
  def testReachableServerWrongPort() {
    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", "9091")
    props.put("buffer.size", "102400")
    props.put("connect.timeout.ms", "300")
    props.put("reconnect.interval", "500")
    val producer = new SyncProducer(new SyncProducerConfig(props))
    var failed = false
    val firstStart = SystemTime.milliseconds
    //temporarily increase log4j level to avoid error in output
    simpleProducerLogger.setLevel(Level.FATAL)
    try {
      producer.send("test", 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                                        messages = getMessageList(new Message(messageBytes))))
    }catch {
      case e: Exception => failed = true
    }
    Assert.assertTrue(failed)
    failed = false
    val firstEnd = SystemTime.milliseconds
    Assert.assertTrue((firstEnd-firstStart) < 300)
    val secondStart = SystemTime.milliseconds
    try {
      producer.send("test", 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                                        messages = getMessageList(new Message(messageBytes))))
    }catch {
      case e: Exception => failed = true
    }
    Assert.assertTrue(failed)
    val secondEnd = SystemTime.milliseconds
    Assert.assertTrue((secondEnd-secondEnd) < 300)
    simpleProducerLogger.setLevel(Level.ERROR)
  }

  private def getMessageList(message: Message): java.util.List[Message] = {
    val messageList = new java.util.ArrayList[Message]()
    messageList.add(message)
    messageList
  }
}
