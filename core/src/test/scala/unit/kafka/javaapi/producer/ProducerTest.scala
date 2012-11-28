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

package kafka.javaapi.producer

import java.util.Properties
import org.apache.log4j.{Logger, Level}
import kafka.server.{KafkaRequestHandlers, KafkaServer, KafkaConfig}
import kafka.zk.EmbeddedZookeeper
import kafka.utils.{TestZKUtils, TestUtils}
import org.junit.{After, Before, Test}
import junit.framework.Assert
import org.easymock.EasyMock
import kafka.utils.Utils
import java.util.concurrent.ConcurrentHashMap
import kafka.cluster.Partition
import kafka.common.{UnavailableProducerException, InvalidPartitionException, InvalidConfigException}
import org.scalatest.junit.JUnitSuite
import kafka.producer.{SyncProducerConfig, Partitioner, ProducerConfig, DefaultPartitioner}
import kafka.producer.ProducerPool
import kafka.javaapi.message.ByteBufferMessageSet
import kafka.producer.async.AsyncProducer
import kafka.javaapi.Implicits._
import kafka.serializer.{StringEncoder, Encoder}
import kafka.javaapi.consumer.SimpleConsumer
import kafka.api.FetchRequest
import kafka.message.{NoCompressionCodec, Message}

class ProducerTest extends JUnitSuite {
  private val topic = "test-topic"
  private val brokerId1 = 0
  private val brokerId2 = 1  
  private val port1 = 9092
  private val port2 = 9093
  private var server1: KafkaServer = null
  private var server2: KafkaServer = null
  private var producer1: SyncProducer = null
  private var producer2: SyncProducer = null
  private var consumer1: SimpleConsumer = null
  private var consumer2: SimpleConsumer = null
  private var zkServer:EmbeddedZookeeper = null
  private val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandlers])

  @Before
  def setUp() {
    // set up 2 brokers with 4 partitions each
    zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect)

    val props1 = TestUtils.createBrokerConfig(brokerId1, port1)
    val config1 = new KafkaConfig(props1) {
      override val numPartitions = 4
    }
    server1 = TestUtils.createServer(config1)

    val props2 = TestUtils.createBrokerConfig(brokerId2, port2)
    val config2 = new KafkaConfig(props2) {
      override val numPartitions = 4
    }
    server2 = TestUtils.createServer(config2)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", port1.toString)

    producer1 = new SyncProducer(new SyncProducerConfig(props))
    val messages1 = new java.util.ArrayList[Message]
    messages1.add(new Message("test".getBytes()))
    producer1.send("test-topic", new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = messages1))

    producer2 = new SyncProducer(new SyncProducerConfig(props) {
      override val port = port2
    })
    val messages2 = new java.util.ArrayList[Message]
    messages2.add(new Message("test".getBytes()))

    producer2.send("test-topic", new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = messages2))

    consumer1 = new SimpleConsumer("localhost", port1, 1000000, 64*1024)
    consumer2 = new SimpleConsumer("localhost", port2, 1000000, 64*1024)

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    Thread.sleep(500)
  }

  @After
  def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
    server1.shutdown
    server2.shutdown
    Utils.rm(server1.config.logDir)
    Utils.rm(server2.config.logDir)    
    Thread.sleep(500)
    zkServer.shutdown
  }

  @Test
  def testSend() {
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.StaticPartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)


    val config = new ProducerConfig(props)
    val partitioner = new StaticPartitioner
    val serializer = new StringSerializer

    // 2 sync producers
    val syncProducers = new ConcurrentHashMap[Int, kafka.producer.SyncProducer]()
    val syncProducer1 = EasyMock.createMock(classOf[kafka.producer.SyncProducer])
    val syncProducer2 = EasyMock.createMock(classOf[kafka.producer.SyncProducer])
    // it should send to partition 0 (first partition) on second broker i.e broker2
    val messageList = new java.util.ArrayList[Message]
    messageList.add(new Message("test1".getBytes()))
    syncProducer2.send(topic, 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = messageList))
    EasyMock.expectLastCall
    syncProducer1.close
    EasyMock.expectLastCall
    syncProducer2.close
    EasyMock.expectLastCall
    EasyMock.replay(syncProducer1)
    EasyMock.replay(syncProducer2)

    syncProducers.put(brokerId1, syncProducer1)
    syncProducers.put(brokerId2, syncProducer2)

    val producerPool = new ProducerPool[String](config, serializer, syncProducers,
      new ConcurrentHashMap[Int, AsyncProducer[String]]())
    val producer = new Producer[String, String](config, partitioner, producerPool, false)

    val messagesContent = new java.util.ArrayList[String]
    messagesContent.add("test1")
    producer.send(new ProducerData[String, String](topic, "test", messagesContent))
    producer.close

    EasyMock.verify(syncProducer1)
    EasyMock.verify(syncProducer2)
  }

  @Test
  def testSendSingleMessage() {
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("broker.list", "0:localhost:9092")


    val config = new ProducerConfig(props)
    val partitioner = new StaticPartitioner
    val serializer = new StringSerializer

    // 2 sync producers
    val syncProducers = new ConcurrentHashMap[Int, kafka.producer.SyncProducer]()
    val syncProducer1 = EasyMock.createMock(classOf[kafka.producer.SyncProducer])
    // it should send to a random partition due to use of broker.list
    val messageList = new java.util.ArrayList[Message]
    messageList.add(new Message("t".getBytes()))
    syncProducer1.send(topic, -1, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = messageList))
    EasyMock.expectLastCall
    syncProducer1.close
    EasyMock.expectLastCall
    EasyMock.replay(syncProducer1)

    syncProducers.put(brokerId1, syncProducer1)

    val producerPool = new ProducerPool[String](config, serializer, syncProducers,
      new ConcurrentHashMap[Int, AsyncProducer[String]]())
    val producer = new Producer[String, String](config, partitioner, producerPool, false)

    producer.send(new ProducerData[String, String](topic, "t"))
    producer.close

    EasyMock.verify(syncProducer1)
  }

  @Test
  def testInvalidPartition() {
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.NegativePartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    val config = new ProducerConfig(props)

    val richProducer = new Producer[String, String](config)
    val messagesContent = new java.util.ArrayList[String]
    messagesContent.add("test")
    try {
      richProducer.send(new ProducerData[String, String](topic, "test", messagesContent))
      Assert.fail("Should fail with InvalidPartitionException")
    }catch {
      case e: InvalidPartitionException => // expected, do nothing
    }
  }

  @Test
  def testSyncProducerPool() {
    // 2 sync producers
    val syncProducers = new ConcurrentHashMap[Int, kafka.producer.SyncProducer]()
    val syncProducer1 = EasyMock.createMock(classOf[kafka.producer.SyncProducer])
    val syncProducer2 = EasyMock.createMock(classOf[kafka.producer.SyncProducer])
    val messageList = new java.util.ArrayList[Message]
    messageList.add(new Message("test1".getBytes()))
    syncProducer1.send("test-topic", 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = messageList))
    EasyMock.expectLastCall
    syncProducer1.close
    EasyMock.expectLastCall
    syncProducer2.close
    EasyMock.expectLastCall
    EasyMock.replay(syncProducer1)
    EasyMock.replay(syncProducer2)

    syncProducers.put(brokerId1, syncProducer1)
    syncProducers.put(brokerId2, syncProducer2)

    // default for producer.type is "sync"
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.NegativePartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    val producerPool = new ProducerPool[String](new ProducerConfig(props), new StringSerializer,
      syncProducers, new ConcurrentHashMap[Int, AsyncProducer[String]]())
    producerPool.send(producerPool.getProducerPoolData("test-topic", new Partition(brokerId1, 0), Array("test1")))

    producerPool.close
    EasyMock.verify(syncProducer1)
    EasyMock.verify(syncProducer2)
  }

  @Test
  def testAsyncProducerPool() {
    // 2 async producers
    val asyncProducers = new ConcurrentHashMap[Int, AsyncProducer[String]]()
    val asyncProducer1 = EasyMock.createMock(classOf[AsyncProducer[String]])
    val asyncProducer2 = EasyMock.createMock(classOf[AsyncProducer[String]])
    asyncProducer1.send(topic, "test1", 0)
    EasyMock.expectLastCall
    asyncProducer1.close
    EasyMock.expectLastCall
    asyncProducer2.close
    EasyMock.expectLastCall
    EasyMock.replay(asyncProducer1)
    EasyMock.replay(asyncProducer2)

    asyncProducers.put(brokerId1, asyncProducer1)
    asyncProducers.put(brokerId2, asyncProducer2)

    // change producer.type to "async"
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.NegativePartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("producer.type", "async")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    val producerPool = new ProducerPool[String](new ProducerConfig(props), new StringSerializer,
      new ConcurrentHashMap[Int, kafka.producer.SyncProducer](), asyncProducers)
    producerPool.send(producerPool.getProducerPoolData(topic, new Partition(brokerId1, 0), Array("test1")))

    producerPool.close
    EasyMock.verify(asyncProducer1)
    EasyMock.verify(asyncProducer2)
  }

  @Test
  def testSyncUnavailableProducerException() {
    val syncProducers = new ConcurrentHashMap[Int, kafka.producer.SyncProducer]()
    val syncProducer1 = EasyMock.createMock(classOf[kafka.producer.SyncProducer])
    val syncProducer2 = EasyMock.createMock(classOf[kafka.producer.SyncProducer])
    syncProducer2.close
    EasyMock.expectLastCall
    EasyMock.replay(syncProducer1)
    EasyMock.replay(syncProducer2)

    syncProducers.put(brokerId2, syncProducer2)

    // default for producer.type is "sync"
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.NegativePartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    val producerPool = new ProducerPool[String](new ProducerConfig(props), new StringSerializer,
      syncProducers, new ConcurrentHashMap[Int, AsyncProducer[String]]())
    try {
      producerPool.send(producerPool.getProducerPoolData("test-topic", new Partition(brokerId1, 0), Array("test1")))
      Assert.fail("Should fail with UnavailableProducerException")
    }catch {
      case e: UnavailableProducerException => // expected
    }

    producerPool.close
    EasyMock.verify(syncProducer1)
    EasyMock.verify(syncProducer2)
  }

  @Test
  def testAsyncUnavailableProducerException() {
    val asyncProducers = new ConcurrentHashMap[Int, AsyncProducer[String]]()
    val asyncProducer1 = EasyMock.createMock(classOf[AsyncProducer[String]])
    val asyncProducer2 = EasyMock.createMock(classOf[AsyncProducer[String]])
    asyncProducer2.close
    EasyMock.expectLastCall
    EasyMock.replay(asyncProducer1)
    EasyMock.replay(asyncProducer2)

    asyncProducers.put(brokerId2, asyncProducer2)

    // change producer.type to "async"
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.NegativePartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("producer.type", "async")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    val producerPool = new ProducerPool[String](new ProducerConfig(props), new StringSerializer,
      new ConcurrentHashMap[Int, kafka.producer.SyncProducer](), asyncProducers)
    try {
      producerPool.send(producerPool.getProducerPoolData(topic, new Partition(brokerId1, 0), Array("test1")))
      Assert.fail("Should fail with UnavailableProducerException")
    }catch {
      case e: UnavailableProducerException => // expected
    }

    producerPool.close
    EasyMock.verify(asyncProducer1)
    EasyMock.verify(asyncProducer2)
  }

  @Test
  def testConfigBrokerPartitionInfoWithPartitioner {
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.StaticPartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("producer.type", "async")
    props.put("broker.list", brokerId1 + ":" + "localhost" + ":" + port1 + ":" + 4 + "," +
                                       brokerId2 + ":" + "localhost" + ":" + port2 + ":" + 4)

    var config: ProducerConfig = null
    try {
      config = new ProducerConfig(props)
      fail("should fail with InvalidConfigException due to presence of partitioner.class and broker.list")
    }catch {
      case e: InvalidConfigException => // expected
    }
  }

  @Test
  def testConfigBrokerPartitionInfo() {
    val props = new Properties()
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("producer.type", "async")
    props.put("broker.list", brokerId1 + ":" + "localhost" + ":" + port1)

    val config = new ProducerConfig(props)
    val partitioner = new StaticPartitioner
    val serializer = new StringSerializer

    // 2 async producers
    val asyncProducers = new ConcurrentHashMap[Int, AsyncProducer[String]]()
    val asyncProducer1 = EasyMock.createMock(classOf[AsyncProducer[String]])
    // it should send to a random partition due to use of broker.list
    asyncProducer1.send(topic, "test1", -1)
    EasyMock.expectLastCall
    asyncProducer1.close
    EasyMock.expectLastCall
    EasyMock.replay(asyncProducer1)

    asyncProducers.put(brokerId1, asyncProducer1)

    val producerPool = new ProducerPool(config, serializer, new ConcurrentHashMap[Int, kafka.producer.SyncProducer](),
      asyncProducers)
    val producer = new Producer[String, String](config, partitioner, producerPool, false)

    val messagesContent = new java.util.ArrayList[String]
    messagesContent.add("test1")
    producer.send(new ProducerData[String, String](topic, "test1", messagesContent))
    producer.close

    EasyMock.verify(asyncProducer1)
  }

  @Test
  def testZKSendToNewTopic() {
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "kafka.producer.StaticPartitioner")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new ProducerConfig(props)
    val serializer = new StringEncoder

    val producer = new Producer[String, String](config)
    try {
      import scala.collection.JavaConversions._
      producer.send(new ProducerData[String, String]("new-topic", "test", asList(Array("test1"))))
      Thread.sleep(100)
      producer.send(new ProducerData[String, String]("new-topic", "test", asList(Array("test1"))))
      Thread.sleep(100)
      // cross check if brokers got the messages
      val messageSet1 = consumer1.fetch(new FetchRequest("new-topic", 0, 0, 10000)).iterator
      Assert.assertTrue("Message set should have 1 message", messageSet1.hasNext)
      Assert.assertEquals(new Message("test1".getBytes), messageSet1.next.message)
      val messageSet2 = consumer2.fetch(new FetchRequest("new-topic", 0, 0, 10000)).iterator
      Assert.assertTrue("Message set should have 1 message", messageSet2.hasNext)
      Assert.assertEquals(new Message("test1".getBytes), messageSet2.next.message)
    } catch {
      case e: Exception => fail("Not expected")
    }
    producer.close
  }

  @Test
  def testZKSendWithDeadBroker() {
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "kafka.producer.StaticPartitioner")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new ProducerConfig(props)
    val serializer = new StringEncoder

    val producer = new Producer[String, String](config)
    try {
      import scala.collection.JavaConversions._
      producer.send(new ProducerData[String, String]("new-topic", "test", asList(Array("test1"))))
      Thread.sleep(100)
      // kill 2nd broker
      server2.shutdown
      Thread.sleep(100)
      producer.send(new ProducerData[String, String]("new-topic", "test", asList(Array("test1"))))
      Thread.sleep(100)
      // cross check if brokers got the messages
      val messageSet1 = consumer1.fetch(new FetchRequest("new-topic", 0, 0, 10000)).iterator
      Assert.assertTrue("Message set should have 1 message", messageSet1.hasNext)
      Assert.assertEquals(new Message("test1".getBytes), messageSet1.next.message)
      Assert.assertTrue("Message set should have another message", messageSet1.hasNext)
      Assert.assertEquals(new Message("test1".getBytes), messageSet1.next.message)
    } catch {
      case e: Exception => fail("Not expected")
    }
    producer.close
  }

  @Test
  def testPartitionedSendToNewTopic() {
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.StaticPartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new ProducerConfig(props)
    val partitioner = new StaticPartitioner
    val serializer = new StringEncoder

    // 2 sync producers
    val syncProducers = new ConcurrentHashMap[Int, kafka.producer.SyncProducer]()
    val syncProducer1 = EasyMock.createMock(classOf[kafka.producer.SyncProducer])
    val syncProducer2 = EasyMock.createMock(classOf[kafka.producer.SyncProducer])
    import scala.collection.JavaConversions._
    syncProducer1.send("test-topic1", 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                                                  messages = asList(Array(new Message("test1".getBytes)))))
    EasyMock.expectLastCall
    syncProducer1.send("test-topic1", 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                                                  messages = asList(Array(new Message("test1".getBytes)))))
    EasyMock.expectLastCall
    syncProducer1.close
    EasyMock.expectLastCall
    syncProducer2.close
    EasyMock.expectLastCall
    EasyMock.replay(syncProducer1)
    EasyMock.replay(syncProducer2)

    syncProducers.put(brokerId1, syncProducer1)
    syncProducers.put(brokerId2, syncProducer2)

    val producerPool = new ProducerPool(config, serializer, syncProducers, new ConcurrentHashMap[Int, AsyncProducer[String]]())
    val producer = new Producer[String, String](config, partitioner, producerPool, false)

    producer.send(new ProducerData[String, String]("test-topic1", "test", asList(Array("test1"))))
    Thread.sleep(100)

    // now send again to this topic using a real producer, this time all brokers would have registered
    // their partitions in zookeeper
    producer1.send("test-topic1", new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                                           messages = asList(Array(new Message("test".getBytes())))))
    Thread.sleep(100)

    // wait for zookeeper to register the new topic
    producer.send(new ProducerData[String, String]("test-topic1", "test1", asList(Array("test1"))))
    Thread.sleep(100)
    producer.close

    EasyMock.verify(syncProducer1)
    EasyMock.verify(syncProducer2)
  }

  @Test
  def testPartitionedSendToNewBrokerInExistingTopic() {
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.StaticPartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new ProducerConfig(props)
    val partitioner = new StaticPartitioner
    val serializer = new StringSerializer

    // 2 sync producers
    val syncProducers = new ConcurrentHashMap[Int, kafka.producer.SyncProducer]()
    val syncProducer1 = EasyMock.createMock(classOf[kafka.producer.SyncProducer])
    val syncProducer2 = EasyMock.createMock(classOf[kafka.producer.SyncProducer])
    val syncProducer3 = EasyMock.createMock(classOf[kafka.producer.SyncProducer])
    val messages1 = new java.util.ArrayList[Message]
    messages1.add(new Message("test1".getBytes()))
    syncProducer3.send("test-topic", 2, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = messages1))
    EasyMock.expectLastCall
    syncProducer1.close
    EasyMock.expectLastCall
    syncProducer2.close
    EasyMock.expectLastCall
    syncProducer3.close
    EasyMock.expectLastCall
    EasyMock.replay(syncProducer1)
    EasyMock.replay(syncProducer2)
    EasyMock.replay(syncProducer3)

    syncProducers.put(brokerId1, syncProducer1)
    syncProducers.put(brokerId2, syncProducer2)
    syncProducers.put(2, syncProducer3)

    val producerPool = new ProducerPool(config, serializer, syncProducers, new ConcurrentHashMap[Int, AsyncProducer[String]]())
    val producer = new Producer[String, String](config, partitioner, producerPool, false)

    val serverProps = TestUtils.createBrokerConfig(2, 9094)
    val serverConfig = new KafkaConfig(serverProps) {
      override val numPartitions = 4
    }
    val server3 = TestUtils.createServer(serverConfig)

    // send a message to the new broker to register it under topic "test-topic"
    val tempProps = new Properties()
    tempProps.put("host", "localhost")
    tempProps.put("port", "9094")
    val tempProducer = new kafka.producer.SyncProducer(new SyncProducerConfig(tempProps))
    val messageList = new java.util.ArrayList[Message]
    messageList.add(new Message("test".getBytes()))
    tempProducer.send("test-topic", new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = messageList))
    Thread.sleep(500)

    val messagesContent = new java.util.ArrayList[String]
    messagesContent.add("test1")
    producer.send(new ProducerData[String, String]("test-topic", "test-topic", messagesContent))
    producer.close

    EasyMock.verify(syncProducer1)
    EasyMock.verify(syncProducer2)
    EasyMock.verify(syncProducer3)

    server3.shutdown
    Utils.rm(server3.config.logDir)
  }

  @Test
  def testDefaultPartitioner() {
    val props = new Properties()
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("producer.type", "async")
    props.put("broker.list", brokerId1 + ":" + "localhost" + ":" + port1)
    val config = new ProducerConfig(props)
    val partitioner = new DefaultPartitioner[String]
    val serializer = new StringSerializer

    // 2 async producers
    val asyncProducers = new ConcurrentHashMap[Int, AsyncProducer[String]]()
    val asyncProducer1 = EasyMock.createMock(classOf[AsyncProducer[String]])
    val asyncProducer2 = EasyMock.createMock(classOf[AsyncProducer[String]])
    // it should send to a random partition due to use of broker.list
    asyncProducer1.send(topic, "test1", -1)
    EasyMock.expectLastCall
    asyncProducer1.close
    EasyMock.expectLastCall
    EasyMock.replay(asyncProducer1)

    asyncProducers.put(brokerId1, asyncProducer1)

    val producerPool = new ProducerPool(config, serializer, new ConcurrentHashMap[Int, kafka.producer.SyncProducer](),
      asyncProducers)
    val producer = new Producer[String, String](config, partitioner, producerPool, false)

    val messagesContent = new java.util.ArrayList[String]
    messagesContent.add("test1")
    producer.send(new ProducerData[String, String](topic, "test", messagesContent))
    producer.close

    EasyMock.verify(asyncProducer1)
  }
}

class StringSerializer extends Encoder[String] {
  def toEvent(message: Message):String = message.toString
  def toMessage(event: String):Message = new Message(event.getBytes)
  def getTopic(event: String): String = event.concat("-topic")
}

class NegativePartitioner extends Partitioner[String] {
  def partition(data: String, numPartitions: Int): Int = {
    -1
  }
}

class StaticPartitioner extends Partitioner[String] {
  def partition(data: String, numPartitions: Int): Int = {
    (data.length % numPartitions)
  }
}

class HashPartitioner extends Partitioner[String] {
  def partition(data: String, numPartitions: Int): Int = {
    (data.hashCode % numPartitions)
  }
}
