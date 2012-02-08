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

import org.easymock.EasyMock
import kafka.api.ProducerRequest
import org.junit.Test
import org.scalatest.junit.JUnitSuite
import kafka.producer.async._
import java.util.concurrent.LinkedBlockingQueue
import junit.framework.Assert._
import collection.SortedSet
import kafka.cluster.{Broker, Partition}
import collection.mutable.{HashMap, ListBuffer}
import collection.Map
import kafka.message.{NoCompressionCodec, ByteBufferMessageSet, Message}
import kafka.serializer.{StringEncoder, StringDecoder, Encoder}
import java.util.{LinkedList, Properties}
import kafka.utils.{TestZKUtils, TestUtils}
import kafka.common.{InvalidConfigException, NoBrokersForPartitionException, InvalidPartitionException}

class AsyncProducerTest extends JUnitSuite {

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
    props.put("broker.list", "0:localhost:9092")
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
    props.put("broker.list", "0:localhost:9092")
    props.put("producer.type", "async")
    props.put("batch.size", "1")

    val config = new ProducerConfig(props)
    val produceData = getProduceData(10)
    val producer = new Producer[String, String](config)
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
    props.put("broker.list", "0:localhost:9092,1:localhost:9092")

    val intPartitioner = new Partitioner[Int] {
      def partition(key: Int, numPartitions: Int): Int = key % numPartitions
    }
    val config = new ProducerConfig(props)
    val handler = new DefaultEventHandler[Int,String](config,
                                                      partitioner = intPartitioner,
                                                      encoder = null.asInstanceOf[Encoder[String]],
                                                      producerPool = null,
                                                      populateProducerPool = false,
                                                      brokerPartitionInfo = null)

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
              ("topic1", -1) -> topic1Broker1Data,
              ("topic2", -1) -> topic2Broker1Data),
        1 -> Map(
              ("topic1", -1) -> topic1Broker2Data,
              ("topic2", -1) -> topic2Broker2Data)
      )

    val actualResult = handler.partitionAndCollate(producerDataList)
    assertEquals(expectedResult, actualResult)
  }

  @Test
  def testSerializeEvents() {
    val produceData = TestUtils.getMsgStrings(5).map(m => new ProducerData[String,String]("topic1",m))
    val props = new Properties()
    props.put("broker.list", "0:localhost:9092,1:localhost:9092")
    val config = new ProducerConfig(props)
    val handler = new DefaultEventHandler[String,String](config,
                                                         partitioner = null.asInstanceOf[Partitioner[String]],
                                                         encoder = new StringEncoder,
                                                         producerPool = null,
                                                         populateProducerPool = false,
                                                         brokerPartitionInfo = null)

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
    props.put("broker.list", "0:localhost:9092,1:localhost:9092")
    val config = new ProducerConfig(props)
    val handler = new DefaultEventHandler[String,String](config,
                                                         partitioner = new NegativePartitioner,
                                                         encoder = null.asInstanceOf[Encoder[String]],
                                                         producerPool = null,
                                                         populateProducerPool = false,
                                                         brokerPartitionInfo = null)
    try {
      handler.partitionAndCollate(producerDataList)
      fail("Should fail with InvalidPartitionException")
    }
    catch {
      case e: InvalidPartitionException => // expected, do nothing
    }
  }

  private def getMockBrokerPartitionInfo(): BrokerPartitionInfo ={
    new BrokerPartitionInfo {
      def getBrokerPartitionInfo(topic: String = null): SortedSet[Partition] = SortedSet.empty[Partition]

      def getBrokerInfo(brokerId: Int): Option[Broker] = None

      def getAllBrokerInfo: Map[Int, Broker] = new HashMap[Int, Broker]

      def updateInfo = {}

      def close = {}
    }
  }

  @Test
  def testNoBroker() {
    val producerDataList = new ListBuffer[ProducerData[String,String]]
    producerDataList.append(new ProducerData[String,String]("topic1", "msg1"))
    val props = new Properties()
    val config = new ProducerConfig(props)
    val handler = new DefaultEventHandler[String,String](config,
                                                         partitioner = null.asInstanceOf[Partitioner[String]],
                                                         encoder = new StringEncoder,
                                                         producerPool = null,
                                                         populateProducerPool = false,
                                                         brokerPartitionInfo = getMockBrokerPartitionInfo)
    try {
      handler.handle(producerDataList)
      fail("Should fail with NoBrokersForPartitionException")
    }
    catch {
      case e: NoBrokersForPartitionException => // expected, do nothing
    }
  }

  @Test
  def testIncompatibleEncoder() {
    val props = new Properties()
    props.put("broker.list", "0:localhost:9092,1:localhost:9092")
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
    props.put("broker.list", "0:localhost:9092,1:localhost:9092")
    val config = new ProducerConfig(props)
    val handler = new DefaultEventHandler[String,String](config,
                                                         partitioner = null.asInstanceOf[Partitioner[String]],
                                                         encoder = null.asInstanceOf[Encoder[String]],
                                                         producerPool = null,
                                                         populateProducerPool = false,
                                                         brokerPartitionInfo = null)
    val producerDataList = new ListBuffer[ProducerData[String,Message]]
    producerDataList.append(new ProducerData[String,Message]("topic1", new Message("msg1".getBytes)))
    producerDataList.append(new ProducerData[String,Message]("topic2", new Message("msg2".getBytes)))
    producerDataList.append(new ProducerData[String,Message]("topic1", new Message("msg3".getBytes)))

    val partitionedData = handler.partitionAndCollate(producerDataList)
    for ((brokerId, dataPerBroker) <- partitionedData) {
      for ( ((topic, partitionId), dataPerTopic) <- dataPerBroker)
        assertTrue(partitionId == ProducerRequest.RandomPartition)
    }
  }

  @Test
  def testBrokerListAndAsync() {
    val topic = "topic1"
    val msgs = TestUtils.getMsgStrings(10)
    val mockSyncProducer = EasyMock.createMock(classOf[SyncProducer])
    mockSyncProducer.multiSend(EasyMock.aryEq(Array(new ProducerRequest(topic, ProducerRequest.RandomPartition,
      messagesToSet(msgs.take(5))))))
    EasyMock.expectLastCall
    mockSyncProducer.multiSend(EasyMock.aryEq(Array(new ProducerRequest(topic, ProducerRequest.RandomPartition,
      messagesToSet(msgs.takeRight(5))))))
    EasyMock.expectLastCall
    mockSyncProducer.close
    EasyMock.expectLastCall
    EasyMock.replay(mockSyncProducer)

    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")
    props.put("batch.size", "5")
    props.put("broker.list", "0:localhost:9092")

    val config = new ProducerConfig(props)
    val producerPool = new ProducerPool(config)
    producerPool.addProducer(0, mockSyncProducer)

    val handler = new DefaultEventHandler[String,String](config,
                                                      partitioner = null.asInstanceOf[Partitioner[String]],
                                                      encoder = new StringEncoder,
                                                      producerPool = producerPool,
                                                      populateProducerPool = false,
                                                      brokerPartitionInfo = null)

    val producer = new Producer[String, String](config, handler)
    try {
      // send all 10 messages, should create 2 batches and 2 syncproducer calls
      producer.send(msgs.map(m => new ProducerData[String,String](topic, List(m))): _*)
      producer.close

    } catch {
      case e: Exception => fail("Not expected", e)
    }

    EasyMock.verify(mockSyncProducer)
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
    props.put("broker.list", "0:localhost:9092")
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

  class MockProducer(override val config: SyncProducerConfig) extends SyncProducer(config) {
    override def send(topic: String, messages: ByteBufferMessageSet): Unit = {
      Thread.sleep(1000)
    }
    override def multiSend(produces: Array[ProducerRequest]) {
      Thread.sleep(1000)
    }
  }
}
