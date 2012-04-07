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

import java.util.{LinkedList, Properties}
import java.util.concurrent.LinkedBlockingQueue
import junit.framework.Assert._
import org.easymock.EasyMock
import org.junit.Test
import kafka.api._
import kafka.cluster.Broker
import kafka.common.{InvalidConfigException, NoBrokersForPartitionException, InvalidPartitionException}
import kafka.message.{NoCompressionCodec, ByteBufferMessageSet, Message}
import kafka.producer.async._
import kafka.serializer.{StringEncoder, StringDecoder, Encoder}
import kafka.server.KafkaConfig
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import collection.Map
import collection.mutable.ListBuffer
import org.scalatest.junit.JUnit3Suite
import kafka.utils.{NegativePartitioner, TestZKUtils, TestUtils}

class AsyncProducerTest extends JUnit3Suite with ZooKeeperTestHarness {
  val props = createBrokerConfigs(1)
  val configs = props.map(p => new KafkaConfig(p) { override val flushInterval = 1})
  var brokers: Seq[Broker] = null

  override def setUp() {
    super.setUp()
    brokers = TestUtils.createBrokersInZk(zkClient, configs.map(config => config.brokerId))
  }

  override def tearDown() {
    super.tearDown()
  }

  @Test
  def testProducerQueueSize() {
    // a mock event handler that blocks
    val mockEventHandler = new EventHandler[String,String] {

      def handle(events: Seq[ProducerData[String,String]]) {
        Thread.sleep(1000000)
      }

      def close {}
    }

    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    props.put("producer.type", "async")
    props.put("queue.size", "10")
    props.put("batch.size", "1")

    val config = new ProducerConfig(props)
    val produceData = getProduceData(12)
    val producer = new Producer[String, String](config, mockEventHandler)
    try {
      // send all 10 messages, should hit the batch size and then reach broker
      producer.send(produceData: _*)
      fail("Queue should be full")
    }
    catch {
      case e: QueueFullException => //expected
    }
  }

  @Test
  def testProduceAfterClosed() {
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    props.put("producer.type", "async")
    props.put("batch.size", "1")

    val config = new ProducerConfig(props)
    val produceData = getProduceData(10)
    val producer = new Producer[String, String](config, zkClient)
    producer.close

    try {
      producer.send(produceData: _*)
      fail("should complain that producer is already closed")
    }
    catch {
      case e: ProducerClosedException => //expected
    }
  }

  def getProduceData(nEvents: Int): Seq[ProducerData[String,String]] = {
    val producerDataList = new ListBuffer[ProducerData[String,String]]
    for (i <- 0 until nEvents)
      producerDataList.append(new ProducerData[String,String]("topic1", null, List("msg" + i)))
    producerDataList
  }

  @Test
  def testBatchSize() {
    /**
     *  Send a total of 10 messages with batch size of 5. Expect 2 calls to the handler, one for each batch.
     */
    val producerDataList = getProduceData(10)
    val mockHandler = EasyMock.createStrictMock(classOf[DefaultEventHandler[String,String]])
    mockHandler.handle(producerDataList.take(5))
    EasyMock.expectLastCall
    mockHandler.handle(producerDataList.takeRight(5))
    EasyMock.expectLastCall
    EasyMock.replay(mockHandler)

    val queue = new LinkedBlockingQueue[ProducerData[String,String]](10)
    val producerSendThread =
      new ProducerSendThread[String,String]("thread1", queue, mockHandler, Integer.MAX_VALUE, 5)
    producerSendThread.start()

    for (producerData <- producerDataList)
      queue.put(producerData)

    producerSendThread.shutdown
    EasyMock.verify(mockHandler)
  }

  @Test
  def testQueueTimeExpired() {
    /**
     *  Send a total of 2 messages with batch size of 5 and queue time of 200ms.
     *  Expect 1 calls to the handler after 200ms.
     */
    val producerDataList = getProduceData(2)
    val mockHandler = EasyMock.createStrictMock(classOf[DefaultEventHandler[String,String]])
    mockHandler.handle(producerDataList)
    EasyMock.expectLastCall
    EasyMock.replay(mockHandler)

    val queue = new LinkedBlockingQueue[ProducerData[String,String]](10)
    val producerSendThread =
      new ProducerSendThread[String,String]("thread1", queue, mockHandler, 200, 5)
    producerSendThread.start()

    for (producerData <- producerDataList)
      queue.put(producerData)

    Thread.sleep(300)
    producerSendThread.shutdown
    EasyMock.verify(mockHandler)
  }

  @Test
  def testPartitionAndCollateEvents() {
    val producerDataList = new ListBuffer[ProducerData[Int,Message]]
    producerDataList.append(new ProducerData[Int,Message]("topic1", 0, new Message("msg1".getBytes)))
    producerDataList.append(new ProducerData[Int,Message]("topic2", 1, new Message("msg2".getBytes)))
    producerDataList.append(new ProducerData[Int,Message]("topic1", 2, new Message("msg3".getBytes)))
    producerDataList.append(new ProducerData[Int,Message]("topic1", 3, new Message("msg4".getBytes)))
    producerDataList.append(new ProducerData[Int,Message]("topic2", 4, new Message("msg5".getBytes)))

    val props = new Properties()
    props.put("zk.connect", zkConnect)
    val broker1 = new Broker(0, "localhost", "localhost", 9092)
    val broker2 = new Broker(1, "localhost", "localhost", 9093)
    // form expected partitions metadata
    val partition1Metadata = new PartitionMetadata(0, Some(broker1), List(broker1, broker2))
    val partition2Metadata = new PartitionMetadata(1, Some(broker2), List(broker1, broker2))
    val topic1Metadata = new TopicMetadata("topic1", List(partition1Metadata, partition2Metadata))
    val topic2Metadata = new TopicMetadata("topic2", List(partition1Metadata, partition2Metadata))

    val intPartitioner = new Partitioner[Int] {
      def partition(key: Int, numPartitions: Int): Int = key % numPartitions
    }
    val config = new ProducerConfig(props)

    val syncProducer = getSyncProducer(List("topic1", "topic2"), List(topic1Metadata, topic2Metadata))

    val producerPool = EasyMock.createMock(classOf[ProducerPool])
    producerPool.getZkClient
    EasyMock.expectLastCall().andReturn(zkClient)
    producerPool.addProducers(config)
    EasyMock.expectLastCall()
    producerPool.getAnyProducer
    EasyMock.expectLastCall().andReturn(syncProducer).times(2)
    EasyMock.replay(producerPool)
    val handler = new DefaultEventHandler[Int,String](config,
                                                      partitioner = intPartitioner,
                                                      encoder = null.asInstanceOf[Encoder[String]],
                                                      producerPool)


    val topic1Broker1Data = new ListBuffer[ProducerData[Int,Message]]
    topic1Broker1Data.appendAll(List(new ProducerData[Int,Message]("topic1", 0, new Message("msg1".getBytes)),
                                     new ProducerData[Int,Message]("topic1", 2, new Message("msg3".getBytes))))
    val topic1Broker2Data = new ListBuffer[ProducerData[Int,Message]]
    topic1Broker2Data.appendAll(List(new ProducerData[Int,Message]("topic1", 3, new Message("msg4".getBytes))))
    val topic2Broker1Data = new ListBuffer[ProducerData[Int,Message]]
    topic2Broker1Data.appendAll(List(new ProducerData[Int,Message]("topic2", 4, new Message("msg5".getBytes))))
    val topic2Broker2Data = new ListBuffer[ProducerData[Int,Message]]
    topic2Broker2Data.appendAll(List(new ProducerData[Int,Message]("topic2", 1, new Message("msg2".getBytes))))
    val expectedResult = Map(
        0 -> Map(
              ("topic1", 0) -> topic1Broker1Data,
              ("topic2", 0) -> topic2Broker1Data),
        1 -> Map(
              ("topic1", 1) -> topic1Broker2Data,
              ("topic2", 1) -> topic2Broker2Data)
      )

    val actualResult = handler.partitionAndCollate(producerDataList)
    assertEquals(expectedResult, actualResult)
    EasyMock.verify(syncProducer)
    EasyMock.verify(producerPool)
  }

  @Test
  def testSerializeEvents() {
    val produceData = TestUtils.getMsgStrings(5).map(m => new ProducerData[String,String]("topic1",m))
    val props = new Properties()
    props.put("zk.connect", zkConnect)
    val config = new ProducerConfig(props)
    // form expected partitions metadata
    val topic1Metadata = getTopicMetadata("topic1", 0, "localhost", 9092)

    val syncProducer = getSyncProducer(List("topic1"), List(topic1Metadata))
    val producerPool = getMockProducerPool(config, syncProducer)
    val handler = new DefaultEventHandler[String,String](config,
                                                         partitioner = null.asInstanceOf[Partitioner[String]],
                                                         encoder = new StringEncoder,
                                                         producerPool)

    val serializedData = handler.serialize(produceData)
    val decoder = new StringDecoder
    val deserializedData = serializedData.map(d => new ProducerData[String,String](d.getTopic, d.getData.map(m => decoder.toEvent(m))))
    TestUtils.checkEquals(produceData.iterator, deserializedData.iterator)
  }

  @Test
  def testInvalidPartition() {
    val producerDataList = new ListBuffer[ProducerData[String,Message]]
    producerDataList.append(new ProducerData[String,Message]("topic1", "key1", new Message("msg1".getBytes)))
    val props = new Properties()
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    val config = new ProducerConfig(props)

    // form expected partitions metadata
    val topic1Metadata = getTopicMetadata("topic1", 0, "localhost", 9092)

    val syncProducer = getSyncProducer(List("topic1"), List(topic1Metadata))

    val producerPool = getMockProducerPool(config, syncProducer)

    val handler = new DefaultEventHandler[String,String](config,
                                                         partitioner = new NegativePartitioner,
                                                         encoder = null.asInstanceOf[Encoder[String]],
                                                         producerPool)
    try {
      handler.partitionAndCollate(producerDataList)
      fail("Should fail with InvalidPartitionException")
    }
    catch {
      case e: InvalidPartitionException => // expected, do nothing
    }
    EasyMock.verify(syncProducer)
    EasyMock.verify(producerPool)
  }

  @Test
  def testNoBroker() {
    val props = new Properties()
    props.put("zk.connect", zkConnect)

    val config = new ProducerConfig(props)
    // create topic metadata with 0 partitions
    val topic1Metadata = new TopicMetadata("topic1", Seq.empty)

    val syncProducer = getSyncProducer(List("topic1"), List(topic1Metadata))

    val producerPool = getMockProducerPool(config, syncProducer)

    val producerDataList = new ListBuffer[ProducerData[String,String]]
    producerDataList.append(new ProducerData[String,String]("topic1", "msg1"))
    val handler = new DefaultEventHandler[String,String](config,
                                                         partitioner = null.asInstanceOf[Partitioner[String]],
                                                         encoder = new StringEncoder,
                                                         producerPool)
    try {
      handler.handle(producerDataList)
      fail("Should fail with NoBrokersForPartitionException")
    }
    catch {
      case e: NoBrokersForPartitionException => // expected, do nothing
    }
    EasyMock.verify(syncProducer)
    EasyMock.verify(producerPool)
  }

  @Test
  def testIncompatibleEncoder() {
    val props = new Properties()
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    val config = new ProducerConfig(props)

    val producer=new Producer[String, String](config)
    try {
      producer.send(getProduceData(1): _*)
      fail("Should fail with ClassCastException due to incompatible Encoder")
    } catch {
      case e: ClassCastException =>
    }
  }

  @Test
  def testRandomPartitioner() {
    val props = new Properties()
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    val config = new ProducerConfig(props)

    // create topic metadata with 0 partitions
    val topic1Metadata = getTopicMetadata("topic1", 0, "localhost", 9092)
    val topic2Metadata = getTopicMetadata("topic2", 0, "localhost", 9092)

    val syncProducer = getSyncProducer(List("topic1", "topic2"), List(topic1Metadata, topic2Metadata))

    val producerPool = EasyMock.createMock(classOf[ProducerPool])
    producerPool.getZkClient
    EasyMock.expectLastCall().andReturn(zkClient)
    producerPool.addProducers(config)
    EasyMock.expectLastCall()
    producerPool.getAnyProducer
    EasyMock.expectLastCall().andReturn(syncProducer).times(2)
    EasyMock.replay(producerPool)

    val handler = new DefaultEventHandler[String,String](config,
                                                         partitioner = null.asInstanceOf[Partitioner[String]],
                                                         encoder = null.asInstanceOf[Encoder[String]],
                                                         producerPool)
    val producerDataList = new ListBuffer[ProducerData[String,Message]]
    producerDataList.append(new ProducerData[String,Message]("topic1", new Message("msg1".getBytes)))
    producerDataList.append(new ProducerData[String,Message]("topic2", new Message("msg2".getBytes)))
    producerDataList.append(new ProducerData[String,Message]("topic1", new Message("msg3".getBytes)))

    val partitionedData = handler.partitionAndCollate(producerDataList)
    for ((brokerId, dataPerBroker) <- partitionedData) {
      for ( ((topic, partitionId), dataPerTopic) <- dataPerBroker)
        assertTrue(partitionId == 0)
    }
    EasyMock.verify(producerPool)
  }

  @Test
  def testBrokerListAndAsync() {
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")
    props.put("batch.size", "5")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new ProducerConfig(props)

    val topic = "topic1"
    val topic1Metadata = getTopicMetadata(topic, 0, "localhost", 9092)

    val msgs = TestUtils.getMsgStrings(10)
    val mockSyncProducer = EasyMock.createMock(classOf[SyncProducer])
    mockSyncProducer.send(new TopicMetadataRequest(List(topic)))
    EasyMock.expectLastCall().andReturn(List(topic1Metadata))
    mockSyncProducer.send(TestUtils.produceRequest(topic, 0, messagesToSet(msgs.take(5))))
    EasyMock.expectLastCall().andReturn(null)
    mockSyncProducer.send(TestUtils.produceRequest(topic, 0, messagesToSet(msgs.takeRight(5))))
    EasyMock.expectLastCall().andReturn(null)
	  EasyMock.replay(mockSyncProducer)

    val producerPool = EasyMock.createMock(classOf[ProducerPool])
    producerPool.getZkClient
    EasyMock.expectLastCall().andReturn(zkClient)
    producerPool.addProducers(config)
    EasyMock.expectLastCall()
    producerPool.getAnyProducer
    EasyMock.expectLastCall().andReturn(mockSyncProducer)
    producerPool.getProducer(0)
    EasyMock.expectLastCall().andReturn(mockSyncProducer).times(2)
    producerPool.close()
    EasyMock.expectLastCall()
    EasyMock.replay(producerPool)

    val handler = new DefaultEventHandler[String,String]( config,
                                                          partitioner = null.asInstanceOf[Partitioner[String]],
                                                          encoder = new StringEncoder,
                                                          producerPool = producerPool)

    val producer = new Producer[String, String](config, handler)
    try {
      // send all 10 messages, should create 2 batches and 2 syncproducer calls
      producer.send(msgs.map(m => new ProducerData[String,String](topic, List(m))): _*)
      producer.close

    } catch {
      case e: Exception => fail("Not expected", e)
    }

    EasyMock.verify(mockSyncProducer)
    EasyMock.verify(producerPool)
  }

  @Test
  def testJavaProducer() {
    val topic = "topic1"
    val msgs = TestUtils.getMsgStrings(5)
    val scalaProducerData = msgs.map(m => new ProducerData[String, String](topic, List(m)))
    val javaProducerData = scala.collection.JavaConversions.asList(msgs.map(m => {
        val javaList = new LinkedList[String]()
        javaList.add(m)
        new kafka.javaapi.producer.ProducerData[String, String](topic, javaList)
      }))

    val mockScalaProducer = EasyMock.createMock(classOf[kafka.producer.Producer[String, String]])
    mockScalaProducer.send(scalaProducerData.head)
    EasyMock.expectLastCall()
    mockScalaProducer.send(scalaProducerData: _*)
    EasyMock.expectLastCall()
    EasyMock.replay(mockScalaProducer)

    val javaProducer = new kafka.javaapi.producer.Producer[String, String](mockScalaProducer)
    javaProducer.send(javaProducerData.get(0))
    javaProducer.send(javaProducerData)

    EasyMock.verify(mockScalaProducer)
  }

  @Test
  def testInvalidConfiguration() {
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("broker.list", TestZKUtils.zookeeperConnect)
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    props.put("producer.type", "async")

    try {
      new ProducerConfig(props)
      fail("should complain about wrong config")
    }
    catch {
      case e: InvalidConfigException => //expected
    }
  }

  private def messagesToSet(messages: Seq[String]): ByteBufferMessageSet = {
    val encoder = new StringEncoder
    new ByteBufferMessageSet(NoCompressionCodec, messages.map(m => encoder.toMessage(m)): _*)
  }

  private def getSyncProducer(topic: Seq[String], topicMetadata: Seq[TopicMetadata]): SyncProducer = {
    val syncProducer = EasyMock.createMock(classOf[SyncProducer])
    topic.zip(topicMetadata).foreach { topicAndMetadata =>
      syncProducer.send(new TopicMetadataRequest(List(topicAndMetadata._1)))
      EasyMock.expectLastCall().andReturn(List(topicAndMetadata._2))
    }
    EasyMock.replay(syncProducer)
    syncProducer
  }

  private def getMockProducerPool(config: ProducerConfig, syncProducer: SyncProducer): ProducerPool = {
    val producerPool = EasyMock.createMock(classOf[ProducerPool])
    producerPool.getZkClient
    EasyMock.expectLastCall().andReturn(zkClient)
    producerPool.addProducers(config)
    EasyMock.expectLastCall()
    producerPool.getAnyProducer
    EasyMock.expectLastCall().andReturn(syncProducer)
    EasyMock.replay(producerPool)
    producerPool
  }

  private def getTopicMetadata(topic: String, brokerId: Int, brokerHost: String, brokerPort: Int): TopicMetadata = {
    val broker1 = new Broker(brokerId, brokerHost, brokerHost, brokerPort)
    val partition1Metadata = new PartitionMetadata(brokerId, Some(broker1), List(broker1))
    new TopicMetadata(topic, List(partition1Metadata))
  }

  class MockProducer(override val config: SyncProducerConfig) extends SyncProducer(config) {
    override def send(produceRequest: ProducerRequest): ProducerResponse = {
      Thread.sleep(1000)
      null
    }
  }
}
