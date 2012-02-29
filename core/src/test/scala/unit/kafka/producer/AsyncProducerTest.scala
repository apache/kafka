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

import junit.framework.Assert
import java.util.Properties
import org.easymock.EasyMock
import kafka.api.ProducerRequest
import org.apache.log4j.{Logger, Level}
import org.junit.Test
import org.scalatest.junit.JUnitSuite
import kafka.producer.async._
import kafka.serializer.Encoder
import kafka.message.{NoCompressionCodec, ByteBufferMessageSet, Message}
import kafka.utils.TestZKUtils

class AsyncProducerTest extends JUnitSuite {

  private val messageContent1 = "test"
  private val topic1 = "test-topic"
  private val message1: Message = new Message(messageContent1.getBytes)

  private val messageContent2 = "test1"
  private val topic2 = "test1$topic"
  private val message2: Message = new Message(messageContent2.getBytes)
  val asyncProducerLogger = Logger.getLogger(classOf[AsyncProducer[String]])

  @Test
  def testProducerQueueSize() {
    val basicProducer = EasyMock.createMock(classOf[SyncProducer])
    basicProducer.multiSend(EasyMock.aryEq(Array(new ProducerRequest(topic1, ProducerRequest.RandomPartition,
      getMessageSetOfSize(List(message1), 10)))))
    EasyMock.expectLastCall
    basicProducer.close
    EasyMock.expectLastCall
    EasyMock.replay(basicProducer)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", "9092")
    props.put("queue.size", "10")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    val config = new AsyncProducerConfig(props)

    val producer = new AsyncProducer[String](config, basicProducer, new StringSerializer)

    //temporarily set log4j to a higher level to avoid error in the output
    producer.setLoggerLevel(Level.FATAL)

    try {
      for(i <- 0 until 11) {
        producer.send(messageContent1 + "-topic", messageContent1)
      }
      Assert.fail("Queue should be full")
    }
    catch {
      case e: QueueFullException => println("Queue is full..")
    }
    producer.start
    producer.close
    Thread.sleep(2000)
    EasyMock.verify(basicProducer)
    producer.setLoggerLevel(Level.ERROR)
  }

  @Test
  def testAddAfterQueueClosed() {
    val basicProducer = EasyMock.createMock(classOf[SyncProducer])
    basicProducer.multiSend(EasyMock.aryEq(Array(new ProducerRequest(topic1, ProducerRequest.RandomPartition,
      getMessageSetOfSize(List(message1), 10)))))
    EasyMock.expectLastCall
    basicProducer.close
    EasyMock.expectLastCall
    EasyMock.replay(basicProducer)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", "9092")
    props.put("queue.size", "10")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    val config = new AsyncProducerConfig(props)

    val producer = new AsyncProducer[String](config, basicProducer, new StringSerializer)

    producer.start
    for(i <- 0 until 10) {
      producer.send(messageContent1 + "-topic", messageContent1)
    }
    producer.close

    try {
      producer.send(messageContent1 + "-topic", messageContent1)
      Assert.fail("Queue should be closed")
    } catch {
      case e: QueueClosedException =>
    }
    EasyMock.verify(basicProducer)
  }

  @Test
  def testBatchSize() {
    val basicProducer = EasyMock.createStrictMock(classOf[SyncProducer])
    basicProducer.multiSend(EasyMock.aryEq(Array(new ProducerRequest(topic1, ProducerRequest.RandomPartition,
      getMessageSetOfSize(List(message1), 5)))))
    EasyMock.expectLastCall.times(2)
    basicProducer.multiSend(EasyMock.aryEq(Array(new ProducerRequest(topic1, ProducerRequest.RandomPartition,
      getMessageSetOfSize(List(message1), 1)))))
    EasyMock.expectLastCall
    basicProducer.close
    EasyMock.expectLastCall
    EasyMock.replay(basicProducer)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", "9092")
    props.put("queue.size", "10")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("batch.size", "5")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new AsyncProducerConfig(props)

    val producer = new AsyncProducer[String](config, basicProducer, new StringSerializer)

    producer.start
    for(i <- 0 until 10) {
      producer.send(messageContent1 + "-topic", messageContent1)
    }

    Thread.sleep(100)
    try {
      producer.send(messageContent1 + "-topic", messageContent1)
    } catch {
      case e: QueueFullException =>
        Assert.fail("Queue should not be full")
    }

    producer.close
    EasyMock.verify(basicProducer)
  }

  @Test
  def testQueueTimeExpired() {
    val basicProducer = EasyMock.createMock(classOf[SyncProducer])
    basicProducer.multiSend(EasyMock.aryEq(Array(new ProducerRequest(topic1, ProducerRequest.RandomPartition,
      getMessageSetOfSize(List(message1), 3)))))
    EasyMock.expectLastCall
    basicProducer.close
    EasyMock.expectLastCall
    EasyMock.replay(basicProducer)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", "9092")
    props.put("queue.size", "10")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("queue.time", "200")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new AsyncProducerConfig(props)

    val producer = new AsyncProducer[String](config, basicProducer, new StringSerializer)
    val serializer = new StringSerializer

    producer.start
    for(i <- 0 until 3) {
      producer.send(serializer.getTopic(messageContent1), messageContent1, ProducerRequest.RandomPartition)
    }

    Thread.sleep(300)
    producer.close
    EasyMock.verify(basicProducer)
  }

  @Test
  def testSenderThreadShutdown() {
    val syncProducerProps = new Properties()
    syncProducerProps.put("host", "localhost")
    syncProducerProps.put("port", "9092")
    syncProducerProps.put("buffer.size", "1000")
    syncProducerProps.put("connect.timeout.ms", "1000")
    syncProducerProps.put("reconnect.interval", "1000")
    val basicProducer = new MockProducer(new SyncProducerConfig(syncProducerProps))

    val asyncProducerProps = new Properties()
    asyncProducerProps.put("host", "localhost")
    asyncProducerProps.put("port", "9092")
    asyncProducerProps.put("queue.size", "10")
    asyncProducerProps.put("serializer.class", "kafka.producer.StringSerializer")
    asyncProducerProps.put("queue.time", "100")
    asyncProducerProps.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new AsyncProducerConfig(asyncProducerProps)
    val producer = new AsyncProducer[String](config, basicProducer, new StringSerializer)
    producer.start
    producer.send(messageContent1 + "-topic", messageContent1)
    producer.close
  }

  @Test
  def testCollateEvents() {
    val basicProducer = EasyMock.createMock(classOf[SyncProducer])
    basicProducer.multiSend(EasyMock.aryEq(Array(new ProducerRequest(topic2, ProducerRequest.RandomPartition,
                                                                     getMessageSetOfSize(List(message2), 5)),
                                                 new ProducerRequest(topic1, ProducerRequest.RandomPartition,
                                                                     getMessageSetOfSize(List(message1), 5)))))
    EasyMock.expectLastCall
    basicProducer.close
    EasyMock.expectLastCall
    EasyMock.replay(basicProducer)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", "9092")
    props.put("queue.size", "50")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("batch.size", "10")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new AsyncProducerConfig(props)

    val producer = new AsyncProducer[String](config, basicProducer, new StringSerializer)

    producer.start
    val serializer = new StringSerializer
    for(i <- 0 until 5) {
      producer.send(messageContent1 + "-topic", messageContent1)
      producer.send(messageContent2 + "$topic", messageContent2, ProducerRequest.RandomPartition)
    }

    producer.close
    EasyMock.verify(basicProducer)

  }

  @Test
  def testCollateAndSerializeEvents() {
    val basicProducer = EasyMock.createMock(classOf[SyncProducer])
    basicProducer.multiSend(EasyMock.aryEq(Array(new ProducerRequest(topic2, 1,
                                                                     getMessageSetOfSize(List(message2), 5)),
                                                 new ProducerRequest(topic1, 0,
                                                                     getMessageSetOfSize(List(message1), 5)),
                                                 new ProducerRequest(topic1, 1,
                                                                     getMessageSetOfSize(List(message1), 5)),
                                                 new ProducerRequest(topic2, 0,
                                                                     getMessageSetOfSize(List(message2), 5)))))

    EasyMock.expectLastCall
    basicProducer.close
    EasyMock.expectLastCall
    EasyMock.replay(basicProducer)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", "9092")
    props.put("queue.size", "50")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("batch.size", "20")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new AsyncProducerConfig(props)

    val producer = new AsyncProducer[String](config, basicProducer, new StringSerializer)

    producer.start
    val serializer = new StringSerializer
    for(i <- 0 until 5) {
      producer.send(topic2, messageContent2, 0)
      producer.send(topic2, messageContent2, 1)
      producer.send(topic1, messageContent1, 0)
      producer.send(topic1, messageContent1, 1)
    }

    producer.close
    EasyMock.verify(basicProducer)

  }

  private def getMessageSetOfSize(messages: List[Message], counts: Int): ByteBufferMessageSet = {
    var messageList = new Array[Message](counts)
    for(message <- messages) {
      for(i <- 0 until counts) {
        messageList(i) = message
      }
    }
    new ByteBufferMessageSet(NoCompressionCodec, messageList: _*)
  }

  class StringSerializer extends Encoder[String] {
    def toMessage(event: String):Message = new Message(event.getBytes)
    def getTopic(event: String): String = event.concat("-topic")
  }

  class MockProducer(override val config: SyncProducerConfig) extends SyncProducer(config) {
    override def send(topic: String, messages: ByteBufferMessageSet): Unit = {
      Thread.sleep(1000)
    }
    override def multiSend(produces: Array[ProducerRequest]) {
      Thread.sleep(1000)
    }
  }
}
