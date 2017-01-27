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

import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue

import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.junit.Assert.{assertEquals, assertTrue}
import org.easymock.EasyMock
import org.junit.Test
import kafka.api._
import kafka.cluster.BrokerEndPoint
import kafka.common._
import kafka.message._
import kafka.producer.async._
import kafka.serializer._
import kafka.server.KafkaConfig
import kafka.utils.TestUtils._

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import kafka.utils._
import org.apache.kafka.common.utils.Time

@deprecated("This test has been deprecated and it will be removed in a future release.", "0.10.0.0")
class AsyncProducerTest {

  class NegativePartitioner(props: VerifiableProperties = null) extends Partitioner {
    def partition(data: Any, numPartitions: Int): Int = -1
  }

  // One of the few cases we can just set a fixed port because the producer is mocked out here since this uses mocks
  val props = Seq(createBrokerConfig(1, "127.0.0.1:1", port = 65534))
  val configs = props.map(KafkaConfig.fromProps)
  val brokerList = configs.map { config =>
    val endPoint = config.advertisedListeners.find(_.securityProtocol == SecurityProtocol.PLAINTEXT).get
    org.apache.kafka.common.utils.Utils.formatAddress(endPoint.host, endPoint.port)
  }.mkString(",")

  @Test
  def testProducerQueueSize() {
    // a mock event handler that blocks
    val mockEventHandler = new EventHandler[String,String] {

      def handle(events: Seq[KeyedMessage[String,String]]) {
        Thread.sleep(500)
      }

      def close {}
    }

    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("metadata.broker.list", brokerList)
    props.put("producer.type", "async")
    props.put("queue.buffering.max.messages", "10")
    props.put("batch.num.messages", "1")
    props.put("queue.enqueue.timeout.ms", "0")

    val config = new ProducerConfig(props)
    val produceData = getProduceData(12)
    val producer = new Producer[String, String](config, mockEventHandler)
    try {
      // send all 10 messages, should hit the batch size and then reach broker
      producer.send(produceData: _*)
      fail("Queue should be full")
    }
    catch {
      case _: QueueFullException => //expected
    }finally {
      producer.close()
    }
  }

  @Test
  def testProduceAfterClosed() {
    val produceData = getProduceData(10)
    val producer = createProducer[String, String](
      brokerList,
      encoder = classOf[StringEncoder].getName)

    producer.close

    try {
      producer.send(produceData: _*)
      fail("should complain that producer is already closed")
    }
    catch {
      case _: ProducerClosedException => //expected
    }
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

    val queue = new LinkedBlockingQueue[KeyedMessage[String,String]](10)
    val producerSendThread =
      new ProducerSendThread[String,String]("thread1", queue, mockHandler, Integer.MAX_VALUE, 5, "")
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

    val queueExpirationTime = 200
    val queue = new LinkedBlockingQueue[KeyedMessage[String,String]](10)
    val producerSendThread =
      new ProducerSendThread[String,String]("thread1", queue, mockHandler, queueExpirationTime, 5, "")
    producerSendThread.start()

    for (producerData <- producerDataList)
      queue.put(producerData)

    Thread.sleep(queueExpirationTime + 100)
    EasyMock.verify(mockHandler)
    producerSendThread.shutdown
  }

  @Test
  def testPartitionAndCollateEvents() {
    val producerDataList = new ArrayBuffer[KeyedMessage[Int,Message]]
    // use bogus key and partition key override for some messages
    producerDataList.append(new KeyedMessage[Int,Message]("topic1", key = 0, message = new Message("msg1".getBytes)))
    producerDataList.append(new KeyedMessage[Int,Message]("topic2", key = -99, partKey = 1, message = new Message("msg2".getBytes)))
    producerDataList.append(new KeyedMessage[Int,Message]("topic1", key = 2, message = new Message("msg3".getBytes)))
    producerDataList.append(new KeyedMessage[Int,Message]("topic1", key = -101, partKey = 3, message = new Message("msg4".getBytes)))
    producerDataList.append(new KeyedMessage[Int,Message]("topic2", key = 4, message = new Message("msg5".getBytes)))

    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    val broker1 = new BrokerEndPoint(0, "localhost", 9092)
    val broker2 = new BrokerEndPoint(1, "localhost", 9093)

    // form expected partitions metadata
    val partition1Metadata = new PartitionMetadata(0, Some(broker1), List(broker1, broker2))
    val partition2Metadata = new PartitionMetadata(1, Some(broker2), List(broker1, broker2))
    val topic1Metadata = new TopicMetadata("topic1", List(partition1Metadata, partition2Metadata))
    val topic2Metadata = new TopicMetadata("topic2", List(partition1Metadata, partition2Metadata))

    val topicPartitionInfos = new collection.mutable.HashMap[String, TopicMetadata]
    topicPartitionInfos.put("topic1", topic1Metadata)
    topicPartitionInfos.put("topic2", topic2Metadata)

    val intPartitioner = new Partitioner {
      def partition(key: Any, numPartitions: Int): Int = key.asInstanceOf[Int] % numPartitions
    }
    val config = new ProducerConfig(props)

    val producerPool = new ProducerPool(config)
    val handler = new DefaultEventHandler[Int,String](config,
                                                      partitioner = intPartitioner,
                                                      encoder = null.asInstanceOf[Encoder[String]],
                                                      keyEncoder = new IntEncoder(),
                                                      producerPool = producerPool,
                                                      topicPartitionInfos = topicPartitionInfos)

    val topic1Broker1Data =
      ArrayBuffer[KeyedMessage[Int,Message]](new KeyedMessage[Int,Message]("topic1", 0, new Message("msg1".getBytes)),
                                             new KeyedMessage[Int,Message]("topic1", 2, new Message("msg3".getBytes)))
    val topic1Broker2Data = ArrayBuffer[KeyedMessage[Int,Message]](new KeyedMessage[Int,Message]("topic1", -101, 3, new Message("msg4".getBytes)))
    val topic2Broker1Data = ArrayBuffer[KeyedMessage[Int,Message]](new KeyedMessage[Int,Message]("topic2", 4, new Message("msg5".getBytes)))
    val topic2Broker2Data = ArrayBuffer[KeyedMessage[Int,Message]](new KeyedMessage[Int,Message]("topic2", -99, 1, new Message("msg2".getBytes)))
    val expectedResult = Some(Map(
        0 -> Map(
              TopicAndPartition("topic1", 0) -> topic1Broker1Data,
              TopicAndPartition("topic2", 0) -> topic2Broker1Data),
        1 -> Map(
              TopicAndPartition("topic1", 1) -> topic1Broker2Data,
              TopicAndPartition("topic2", 1) -> topic2Broker2Data)
      ))

    val actualResult = handler.partitionAndCollate(producerDataList)
    assertEquals(expectedResult, actualResult)
  }

  @Test
  def testSerializeEvents() {
    val produceData = TestUtils.getMsgStrings(5).map(m => new KeyedMessage[String,String]("topic1",m))
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    val config = new ProducerConfig(props)
    // form expected partitions metadata
    val topic1Metadata = getTopicMetadata("topic1", 0, 0, "localhost", 9092)
    val topicPartitionInfos = new collection.mutable.HashMap[String, TopicMetadata]
    topicPartitionInfos.put("topic1", topic1Metadata)

    val producerPool = new ProducerPool(config)

    val handler = new DefaultEventHandler[String,String](config,
                                                         partitioner = null.asInstanceOf[Partitioner],
                                                         encoder = new StringEncoder,
                                                         keyEncoder = new StringEncoder,
                                                         producerPool = producerPool,
                                                         topicPartitionInfos = topicPartitionInfos)

    val serializedData = handler.serialize(produceData)
    val deserializedData = serializedData.map(d => new KeyedMessage[String,String](d.topic, TestUtils.readString(d.message.payload)))

    // Test that the serialize handles seq from a Stream
    val streamedSerializedData = handler.serialize(Stream(produceData:_*))
    val deserializedStreamData = streamedSerializedData.map(d => new KeyedMessage[String,String](d.topic, TestUtils.readString(d.message.payload)))

    TestUtils.checkEquals(produceData.iterator, deserializedData.iterator)
    TestUtils.checkEquals(produceData.iterator, deserializedStreamData.iterator)
  }

  @Test
  def testInvalidPartition() {
    val producerDataList = new ArrayBuffer[KeyedMessage[String,Message]]
    producerDataList.append(new KeyedMessage[String,Message]("topic1", "key1", new Message("msg1".getBytes)))
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    val config = new ProducerConfig(props)

    // form expected partitions metadata
    val topic1Metadata = getTopicMetadata("topic1", 0, 0, "localhost", 9092)

    val topicPartitionInfos = new collection.mutable.HashMap[String, TopicMetadata]
    topicPartitionInfos.put("topic1", topic1Metadata)

    val producerPool = new ProducerPool(config)

    val handler = new DefaultEventHandler[String,String](config,
                                                         partitioner = new NegativePartitioner,
                                                         encoder = null.asInstanceOf[Encoder[String]],
                                                         keyEncoder = null.asInstanceOf[Encoder[String]],
                                                         producerPool = producerPool,
                                                         topicPartitionInfos = topicPartitionInfos)
    try {
      handler.partitionAndCollate(producerDataList)
    }
    catch {
      // should not throw any exception
      case _: Throwable => fail("Should not throw any exception")

    }
  }

  @Test
  def testNoBroker() {
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)

    val config = new ProducerConfig(props)
    // create topic metadata with 0 partitions
    val topic1Metadata = new TopicMetadata("topic1", Seq.empty)

    val topicPartitionInfos = new collection.mutable.HashMap[String, TopicMetadata]
    topicPartitionInfos.put("topic1", topic1Metadata)

    val producerPool = new ProducerPool(config)

    val producerDataList = new ArrayBuffer[KeyedMessage[String,String]]
    producerDataList.append(new KeyedMessage[String,String]("topic1", "msg1"))
    val handler = new DefaultEventHandler[String,String](config,
                                                         partitioner = null.asInstanceOf[Partitioner],
                                                         encoder = new StringEncoder,
                                                         keyEncoder = new StringEncoder,
                                                         producerPool = producerPool,
                                                         topicPartitionInfos = topicPartitionInfos)
    try {
      handler.handle(producerDataList)
      fail("Should fail with FailedToSendMessageException")
    }
    catch {
      case _: FailedToSendMessageException => // we retry on any exception now
    }
  }

  @Test
  def testIncompatibleEncoder() {
    val props = new Properties()
    // no need to retry since the send will always fail
    props.put("message.send.max.retries", "0")
    val producer= createProducer[String, String](
      brokerList = brokerList,
      encoder = classOf[DefaultEncoder].getName,
      keyEncoder = classOf[DefaultEncoder].getName,
      producerProps = props)

    try {
      producer.send(getProduceData(1): _*)
      fail("Should fail with ClassCastException due to incompatible Encoder")
    } catch {
      case _: ClassCastException =>
    } finally {
      producer.close()
    }
  }

  @Test
  def testRandomPartitioner() {
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    val config = new ProducerConfig(props)

    // create topic metadata with 0 partitions
    val topic1Metadata = getTopicMetadata("topic1", 0, 0, "localhost", 9092)
    val topic2Metadata = getTopicMetadata("topic2", 0, 0, "localhost", 9092)

    val topicPartitionInfos = new collection.mutable.HashMap[String, TopicMetadata]
    topicPartitionInfos.put("topic1", topic1Metadata)
    topicPartitionInfos.put("topic2", topic2Metadata)

    val producerPool = new ProducerPool(config)
    val handler = new DefaultEventHandler[String,String](config,
                                                         partitioner = null.asInstanceOf[Partitioner],
                                                         encoder = null.asInstanceOf[Encoder[String]],
                                                         keyEncoder = null.asInstanceOf[Encoder[String]],
                                                         producerPool = producerPool,
                                                         topicPartitionInfos = topicPartitionInfos)
    val producerDataList = new ArrayBuffer[KeyedMessage[String,Message]]
    producerDataList.append(new KeyedMessage[String,Message]("topic1", new Message("msg1".getBytes)))
    producerDataList.append(new KeyedMessage[String,Message]("topic2", new Message("msg2".getBytes)))
    producerDataList.append(new KeyedMessage[String,Message]("topic1", new Message("msg3".getBytes)))

    val partitionedDataOpt = handler.partitionAndCollate(producerDataList)
    partitionedDataOpt match {
      case Some(partitionedData) =>
        for (dataPerBroker <- partitionedData.values) {
          for (tp <- dataPerBroker.keys)
            assertTrue(tp.partition == 0)
        }
      case None =>
        fail("Failed to collate requests by topic, partition")
    }
  }

  @Test
  def testFailedSendRetryLogic() {
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("request.required.acks", "1")
    props.put("serializer.class", classOf[StringEncoder].getName.toString)
    props.put("key.serializer.class", classOf[NullEncoder[Int]].getName.toString)
    props.put("producer.num.retries", 3.toString)

    val config = new ProducerConfig(props)

    val topic1 = "topic1"
    val topic1Metadata = getTopicMetadata(topic1, Array(0, 1), 0, "localhost", 9092)
    val topicPartitionInfos = new collection.mutable.HashMap[String, TopicMetadata]
    topicPartitionInfos.put("topic1", topic1Metadata)

    val msgs = TestUtils.getMsgStrings(2)

    import SyncProducerConfig.{DefaultAckTimeoutMs, DefaultClientId}

    // produce request for topic1 and partitions 0 and 1.  Let the first request fail
    // entirely.  The second request will succeed for partition 1 but fail for partition 0.
    // On the third try for partition 0, let it succeed.
    val request1 = TestUtils.produceRequestWithAcks(List(topic1), List(0, 1), messagesToSet(msgs), acks = 1,
      correlationId = 11, timeout = DefaultAckTimeoutMs, clientId = DefaultClientId)
    val request2 = TestUtils.produceRequestWithAcks(List(topic1), List(0, 1), messagesToSet(msgs), acks = 1,
      correlationId = 17, timeout = DefaultAckTimeoutMs, clientId = DefaultClientId)
    val response1 = ProducerResponse(0,
      Map((TopicAndPartition("topic1", 0), ProducerResponseStatus(Errors.NOT_LEADER_FOR_PARTITION.code, 0L)),
          (TopicAndPartition("topic1", 1), ProducerResponseStatus(Errors.NONE.code, 0L))))
    val request3 = TestUtils.produceRequest(topic1, 0, messagesToSet(msgs), acks = 1, correlationId = 21,
      timeout = DefaultAckTimeoutMs, clientId = DefaultClientId)
    val response2 = ProducerResponse(0,
      Map((TopicAndPartition("topic1", 0), ProducerResponseStatus(Errors.NONE.code, 0L))))
    val mockSyncProducer = EasyMock.createMock(classOf[SyncProducer])
    // don't care about config mock
    EasyMock.expect(mockSyncProducer.config).andReturn(EasyMock.anyObject()).anyTimes()
    EasyMock.expect(mockSyncProducer.send(request1)).andThrow(new RuntimeException) // simulate SocketTimeoutException
    EasyMock.expect(mockSyncProducer.send(request2)).andReturn(response1)
    EasyMock.expect(mockSyncProducer.send(request3)).andReturn(response2)
    EasyMock.replay(mockSyncProducer)

    val producerPool = EasyMock.createMock(classOf[ProducerPool])
    EasyMock.expect(producerPool.getProducer(0)).andReturn(mockSyncProducer).times(4)
    EasyMock.expect(producerPool.close())
    EasyMock.replay(producerPool)
    val time = new Time {
      override def nanoseconds: Long = 0L
      override def milliseconds: Long = 0L
      override def sleep(ms: Long): Unit = {}
      override def hiResClockMs: Long = 0L
    }
    val handler = new DefaultEventHandler[Int,String](config,
                                                      partitioner = new FixedValuePartitioner(),
                                                      encoder = new StringEncoder(),
                                                      keyEncoder = new NullEncoder[Int](),
                                                      producerPool = producerPool,
                                                      topicPartitionInfos = topicPartitionInfos,
                                                      time = time)
    val data = msgs.map(m => new KeyedMessage[Int,String](topic1, 0, m)) ++ msgs.map(m => new KeyedMessage[Int,String](topic1, 1, m))
    handler.handle(data)
    handler.close()

    EasyMock.verify(mockSyncProducer)
    EasyMock.verify(producerPool)
  }

  @Test
  def testJavaProducer() {
    val topic = "topic1"
    val msgs = TestUtils.getMsgStrings(5)
    val scalaProducerData = msgs.map(m => new KeyedMessage[String, String](topic, m))
    val javaProducerData: java.util.List[KeyedMessage[String, String]] = {
      import scala.collection.JavaConversions._
      scalaProducerData
    }

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
    props.put("producer.type", "async")
    try {
      new ProducerConfig(props)
      fail("should complain about wrong config")
    }
    catch {
      case _: IllegalArgumentException => //expected
    }
  }

  def getProduceData(nEvents: Int): Seq[KeyedMessage[String,String]] = {
    val producerDataList = new ArrayBuffer[KeyedMessage[String,String]]
    for (i <- 0 until nEvents)
      producerDataList.append(new KeyedMessage[String,String]("topic1", null, "msg" + i))
    producerDataList
  }

  private def getTopicMetadata(topic: String, partition: Int, brokerId: Int, brokerHost: String, brokerPort: Int): TopicMetadata = {
    getTopicMetadata(topic, List(partition), brokerId, brokerHost, brokerPort)
  }

  private def getTopicMetadata(topic: String, partition: Seq[Int], brokerId: Int, brokerHost: String, brokerPort: Int): TopicMetadata = {
    val broker1 = new BrokerEndPoint(brokerId, brokerHost, brokerPort)
    new TopicMetadata(topic, partition.map(new PartitionMetadata(_, Some(broker1), List(broker1))))
  }

  def messagesToSet(messages: Seq[String]): ByteBufferMessageSet = {
    new ByteBufferMessageSet(NoCompressionCodec, messages.map(m => new Message(m.getBytes, 0L, Message.MagicValue_V1)): _*)
  }

  def messagesToSet(key: Array[Byte], messages: Seq[Array[Byte]]): ByteBufferMessageSet = {
    new ByteBufferMessageSet(
      NoCompressionCodec,
      messages.map(m => new Message(key = key, bytes = m, timestamp = 0L, magicValue = Message.MagicValue_V1)): _*)
  }
}
