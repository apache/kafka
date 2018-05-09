/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import java.io.DataInputStream
import java.util
import java.util.Properties

import kafka.api.KAFKA_0_11_0_IV2
import kafka.log.LogConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{Record, RecordBatch}
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.apache.kafka.common.requests.{FetchMetadata => JFetchMetadata}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.util.Random

/**
  * Subclasses of `BaseConsumerTest` exercise the consumer and fetch request/response. This class
  * complements those classes with tests that require lower-level access to the protocol.
  */
class FetchRequestTest extends BaseRequestTest {

  private var producer: KafkaProducer[String, String] = null

  override def tearDown() {
    if (producer != null)
      producer.close()
    super.tearDown()
  }

  private def createFetchRequest(maxResponseBytes: Int, maxPartitionBytes: Int, topicPartitions: Seq[TopicPartition],
                                 offsetMap: Map[TopicPartition, Long]): FetchRequest =
    FetchRequest.Builder.forConsumer(Int.MaxValue, 0, createPartitionMap(maxPartitionBytes, topicPartitions, offsetMap))
      .setMaxBytes(maxResponseBytes).build()

  private def createPartitionMap(maxPartitionBytes: Int, topicPartitions: Seq[TopicPartition],
                                 offsetMap: Map[TopicPartition, Long] = Map.empty): util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData] = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    topicPartitions.foreach { tp =>
      partitionMap.put(tp, new FetchRequest.PartitionData(offsetMap.getOrElse(tp, 0), 0L, maxPartitionBytes))
    }
    partitionMap
  }

  private def sendFetchRequest(leaderId: Int, request: FetchRequest): FetchResponse = {
    val response = connectAndSend(request, ApiKeys.FETCH, destination = brokerSocketServer(leaderId))
    FetchResponse.parse(response, request.version)
  }

  private def initProducer(): Unit = {
    producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers),
      retries = 5, keySerializer = new StringSerializer, valueSerializer = new StringSerializer)
  }

  @Test
  def testBrokerRespectsPartitionsOrderAndSizeLimits(): Unit = {
    initProducer()

    val messagesPerPartition = 9
    val maxResponseBytes = 800
    val maxPartitionBytes = 190

    def createFetchRequest(topicPartitions: Seq[TopicPartition], offsetMap: Map[TopicPartition, Long] = Map.empty): FetchRequest =
      this.createFetchRequest(maxResponseBytes, maxPartitionBytes, topicPartitions, offsetMap)

    val topicPartitionToLeader = createTopics(numTopics = 5, numPartitions = 6)
    val random = new Random(0)
    val topicPartitions = topicPartitionToLeader.keySet
    produceData(topicPartitions, messagesPerPartition)

    val leaderId = servers.head.config.brokerId
    val partitionsForLeader = topicPartitionToLeader.toVector.collect {
      case (tp, partitionLeaderId) if partitionLeaderId == leaderId => tp
    }

    val partitionsWithLargeMessages = partitionsForLeader.takeRight(2)
    val partitionWithLargeMessage1 = partitionsWithLargeMessages.head
    val partitionWithLargeMessage2 = partitionsWithLargeMessages(1)
    producer.send(new ProducerRecord(partitionWithLargeMessage1.topic, partitionWithLargeMessage1.partition,
      "larger than partition limit", new String(new Array[Byte](maxPartitionBytes + 1)))).get
    producer.send(new ProducerRecord(partitionWithLargeMessage2.topic, partitionWithLargeMessage2.partition,
      "larger than response limit", new String(new Array[Byte](maxResponseBytes + 1)))).get

    val partitionsWithoutLargeMessages = partitionsForLeader.filterNot(partitionsWithLargeMessages.contains)

    // 1. Partitions with large messages at the end
    val shuffledTopicPartitions1 = random.shuffle(partitionsWithoutLargeMessages) ++ partitionsWithLargeMessages
    val fetchRequest1 = createFetchRequest(shuffledTopicPartitions1)
    val fetchResponse1 = sendFetchRequest(leaderId, fetchRequest1)
    checkFetchResponse(shuffledTopicPartitions1, fetchResponse1, maxPartitionBytes, maxResponseBytes, messagesPerPartition)

    // 2. Same as 1, but shuffled again
    val shuffledTopicPartitions2 = random.shuffle(partitionsWithoutLargeMessages) ++ partitionsWithLargeMessages
    val fetchRequest2 = createFetchRequest(shuffledTopicPartitions2)
    val fetchResponse2 = sendFetchRequest(leaderId, fetchRequest2)
    checkFetchResponse(shuffledTopicPartitions2, fetchResponse2, maxPartitionBytes, maxResponseBytes, messagesPerPartition)

    // 3. Partition with message larger than the partition limit at the start of the list
    val shuffledTopicPartitions3 = Seq(partitionWithLargeMessage1, partitionWithLargeMessage2) ++
      random.shuffle(partitionsWithoutLargeMessages)
    val fetchRequest3 = createFetchRequest(shuffledTopicPartitions3, Map(partitionWithLargeMessage1 -> messagesPerPartition))
    val fetchResponse3 = sendFetchRequest(leaderId, fetchRequest3)
    assertEquals(shuffledTopicPartitions3, fetchResponse3.responseData.keySet.asScala.toSeq)
    val responseSize3 = fetchResponse3.responseData.asScala.values.map { partitionData =>
      records(partitionData).map(_.sizeInBytes).sum
    }.sum
    assertTrue(responseSize3 <= maxResponseBytes)
    val partitionData3 = fetchResponse3.responseData.get(partitionWithLargeMessage1)
    assertEquals(Errors.NONE, partitionData3.error)
    assertTrue(partitionData3.highWatermark > 0)
    val size3 = records(partitionData3).map(_.sizeInBytes).sum
    assertTrue(s"Expected $size3 to be smaller than $maxResponseBytes", size3 <= maxResponseBytes)
    assertTrue(s"Expected $size3 to be larger than $maxPartitionBytes", size3 > maxPartitionBytes)
    assertTrue(maxPartitionBytes < partitionData3.records.sizeInBytes)

    // 4. Partition with message larger than the response limit at the start of the list
    val shuffledTopicPartitions4 = Seq(partitionWithLargeMessage2, partitionWithLargeMessage1) ++
      random.shuffle(partitionsWithoutLargeMessages)
    val fetchRequest4 = createFetchRequest(shuffledTopicPartitions4, Map(partitionWithLargeMessage2 -> messagesPerPartition))
    val fetchResponse4 = sendFetchRequest(leaderId, fetchRequest4)
    assertEquals(shuffledTopicPartitions4, fetchResponse4.responseData.keySet.asScala.toSeq)
    val nonEmptyPartitions4 = fetchResponse4.responseData.asScala.toSeq.collect {
      case (tp, partitionData) if records(partitionData).map(_.sizeInBytes).sum > 0 => tp
    }
    assertEquals(Seq(partitionWithLargeMessage2), nonEmptyPartitions4)
    val partitionData4 = fetchResponse4.responseData.get(partitionWithLargeMessage2)
    assertEquals(Errors.NONE, partitionData4.error)
    assertTrue(partitionData4.highWatermark > 0)
    val size4 = records(partitionData4).map(_.sizeInBytes).sum
    assertTrue(s"Expected $size4 to be larger than $maxResponseBytes", size4 > maxResponseBytes)
    assertTrue(maxResponseBytes < partitionData4.records.sizeInBytes)
  }

  @Test
  def testFetchRequestV2WithOversizedMessage(): Unit = {
    initProducer()
    val maxPartitionBytes = 200
    val (topicPartition, leaderId) = createTopics(numTopics = 1, numPartitions = 1).head
    producer.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key", new String(new Array[Byte](maxPartitionBytes + 1)))).get
    val fetchRequest = FetchRequest.Builder.forConsumer(Int.MaxValue, 0, createPartitionMap(maxPartitionBytes,
      Seq(topicPartition))).build(2)
    val fetchResponse = sendFetchRequest(leaderId, fetchRequest)
    val partitionData = fetchResponse.responseData.get(topicPartition)
    assertEquals(Errors.NONE, partitionData.error)
    assertTrue(partitionData.highWatermark > 0)
    assertEquals(maxPartitionBytes, partitionData.records.sizeInBytes)
    assertEquals(0, records(partitionData).map(_.sizeInBytes).sum)
  }

  @Test
  def testFetchRequestToNonReplica(): Unit = {
    val topic = "topic"
    val partition = 0
    val topicPartition = new TopicPartition(topic, partition)

    // Create a single-partition topic and find a broker which is not the leader
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, 1, servers)
    val leader = partitionToLeader(partition)
    val nonReplicaOpt = servers.find(_.config.brokerId != leader)
    assertTrue(nonReplicaOpt.isDefined)
    val nonReplicaId =  nonReplicaOpt.get.config.brokerId

    // Send the fetch request to the non-replica and verify the error code
    val fetchRequest = FetchRequest.Builder.forConsumer(Int.MaxValue, 0, createPartitionMap(1024,
      Seq(topicPartition))).build()
    val fetchResponse = sendFetchRequest(nonReplicaId, fetchRequest)
    val partitionData = fetchResponse.responseData.get(topicPartition)
    assertEquals(Errors.NOT_LEADER_FOR_PARTITION, partitionData.error)
  }

  /**
   * Tests that down-conversions dont leak memory. Large down conversions are triggered
   * in the server. The client closes its connection after reading partial data when the
   * channel is muted in the server. If buffers are not released this will result in OOM.
   */
  @Test
  def testDownConversionWithConnectionFailure(): Unit = {
    val (topicPartition, leaderId) = createTopics(numTopics = 1, numPartitions = 1).head

    val msgValueLen = 100 * 1000
    val batchSize = 4 * msgValueLen
    val propsOverride = new Properties
    propsOverride.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize.toString)
    val producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers),
      retries = 5, lingerMs = Long.MaxValue,
      keySerializer = new StringSerializer, valueSerializer = new ByteArraySerializer, props = Some(propsOverride))
    val bytes = new Array[Byte](msgValueLen)
    val futures = try {
      (0 to 1000).map { _ =>
        producer.send(new ProducerRecord(topicPartition.topic, topicPartition.partition, "key", bytes))
      }
    } finally {
      producer.close()
    }
    // Check futures to ensure sends succeeded, but do this after close since the last
    // batch is not complete, but sent when the producer is closed
    futures.foreach(_.get)

    def fetch(version: Short, maxPartitionBytes: Int, closeAfterPartialResponse: Boolean): Option[FetchResponse] = {
      val fetchRequest = FetchRequest.Builder.forConsumer(Int.MaxValue, 0, createPartitionMap(maxPartitionBytes,
        Seq(topicPartition))).build(version)

      val socket = connect(brokerSocketServer(leaderId))
      try {
        send(fetchRequest, ApiKeys.FETCH, socket)
        if (closeAfterPartialResponse) {
          // read some data to ensure broker has muted this channel and then close socket
          val size = new DataInputStream(socket.getInputStream).readInt()
          // Check that we have received almost `maxPartitionBytes` (minus a tolerance) since in
          // the case of OOM, the size will be significantly smaller. We can't check for exactly
          // maxPartitionBytes since we use approx message sizes that include only the message value.
          assertTrue(s"Fetch size too small $size, broker may have run out of memory",
              size > maxPartitionBytes - batchSize)
          None
        } else {
          Some(FetchResponse.parse(receive(socket), version))
        }
      } finally {
        socket.close()
      }
    }

    val version = 1.toShort
    (0 to 15).foreach(_ => fetch(version, maxPartitionBytes = msgValueLen * 1000, closeAfterPartialResponse = true))

    val response = fetch(version, maxPartitionBytes = batchSize, closeAfterPartialResponse = false)
    val fetchResponse = response.getOrElse(throw new IllegalStateException("No fetch response"))
    val partitionData = fetchResponse.responseData.get(topicPartition)
    assertEquals(Errors.NONE, partitionData.error)
    val batches = partitionData.records.batches.asScala.toBuffer
    assertEquals(3, batches.size) // size is 3 (not 4) since maxPartitionBytes=msgValueSize*4, excluding key and headers
  }

  /**
    * Ensure that we respect the fetch offset when returning records that were converted from an uncompressed v2
    * record batch to multiple v0/v1 record batches with size 1. If the fetch offset points to inside the record batch,
    * some records have to be dropped during the conversion.
    */
  @Test
  def testDownConversionFromBatchedToUnbatchedRespectsOffset(): Unit = {
    // Increase linger so that we have control over the batches created
    producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers),
      retries = 5, keySerializer = new StringSerializer, valueSerializer = new StringSerializer,
      lingerMs = 300 * 1000)

    val topicConfig = Map(LogConfig.MessageFormatVersionProp -> KAFKA_0_11_0_IV2.version)
    val (topicPartition, leaderId) = createTopics(numTopics = 1, numPartitions = 1, topicConfig).head
    val topic = topicPartition.topic

    val firstBatchFutures = (0 until 10).map(i => producer.send(new ProducerRecord(topic, s"key-$i", s"value-$i")))
    producer.flush()
    val secondBatchFutures = (10 until 25).map(i => producer.send(new ProducerRecord(topic, s"key-$i", s"value-$i")))
    producer.flush()

    firstBatchFutures.foreach(_.get)
    secondBatchFutures.foreach(_.get)

    def check(fetchOffset: Long, requestVersion: Short, expectedOffset: Long, expectedNumBatches: Int, expectedMagic: Byte): Unit = {
      val fetchRequest = FetchRequest.Builder.forConsumer(Int.MaxValue, 0, createPartitionMap(Int.MaxValue,
        Seq(topicPartition), Map(topicPartition -> fetchOffset))).build(requestVersion)
      val fetchResponse = sendFetchRequest(leaderId, fetchRequest)
      val partitionData = fetchResponse.responseData.get(topicPartition)
      assertEquals(Errors.NONE, partitionData.error)
      assertTrue(partitionData.highWatermark > 0)
      val batches = partitionData.records.batches.asScala.toBuffer
      assertEquals(expectedNumBatches, batches.size)
      val batch = batches.head
      assertEquals(expectedMagic, batch.magic)
      assertEquals(expectedOffset, batch.baseOffset)
    }

    // down conversion to message format 0, batches of 1 message are returned so we receive the exact offset we requested
    check(fetchOffset = 3, expectedOffset = 3, requestVersion = 1, expectedNumBatches = 22,
      expectedMagic = RecordBatch.MAGIC_VALUE_V0)
    check(fetchOffset = 15, expectedOffset = 15, requestVersion = 1, expectedNumBatches = 10,
      expectedMagic = RecordBatch.MAGIC_VALUE_V0)

    // down conversion to message format 1, batches of 1 message are returned so we receive the exact offset we requested
    check(fetchOffset = 3, expectedOffset = 3, requestVersion = 3, expectedNumBatches = 22,
      expectedMagic = RecordBatch.MAGIC_VALUE_V1)
    check(fetchOffset = 15, expectedOffset = 15, requestVersion = 3, expectedNumBatches = 10,
      expectedMagic = RecordBatch.MAGIC_VALUE_V1)

    // no down conversion, we receive a single batch so the received offset won't necessarily be the same
    check(fetchOffset = 3, expectedOffset = 0, requestVersion = 4, expectedNumBatches = 2,
      expectedMagic = RecordBatch.MAGIC_VALUE_V2)
    check(fetchOffset = 15, expectedOffset = 10, requestVersion = 4, expectedNumBatches = 1,
      expectedMagic = RecordBatch.MAGIC_VALUE_V2)

    // no down conversion, we receive a single batch and the exact offset we requested because it happens to be the
    // offset of the first record in the batch
    check(fetchOffset = 10, expectedOffset = 10, requestVersion = 4, expectedNumBatches = 1,
      expectedMagic = RecordBatch.MAGIC_VALUE_V2)
  }

  /**
    * Test that when an incremental fetch session contains partitions with an error,
    * those partitions are returned in all incremental fetch requests.
    */
  @Test
  def testCreateIncrementalFetchWithPartitionsInError(): Unit = {
    def createFetchRequest(topicPartitions: Seq[TopicPartition],
                           metadata: JFetchMetadata,
                           toForget: Seq[TopicPartition]): FetchRequest =
      FetchRequest.Builder.forConsumer(Int.MaxValue, 0,
        createPartitionMap(Integer.MAX_VALUE, topicPartitions, Map.empty))
          .toForget(toForget.asJava)
          .metadata(metadata)
          .build()
    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)
    createTopic("foo", Map(0 -> List(0, 1), 1 -> List(0, 2)))
    val bar0 = new TopicPartition("bar", 0)
    val req1 = createFetchRequest(List(foo0, foo1, bar0), JFetchMetadata.INITIAL, Nil)
    val resp1 = sendFetchRequest(0, req1)
    assertEquals(Errors.NONE, resp1.error())
    assertTrue("Expected the broker to create a new incremental fetch session", resp1.sessionId() > 0)
    debug(s"Test created an incremental fetch session ${resp1.sessionId}")
    assertTrue(resp1.responseData().containsKey(foo0))
    assertTrue(resp1.responseData().containsKey(foo1))
    assertTrue(resp1.responseData().containsKey(bar0))
    assertEquals(Errors.NONE, resp1.responseData().get(foo0).error)
    assertEquals(Errors.NONE, resp1.responseData().get(foo1).error)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, resp1.responseData().get(bar0).error)
    val req2 = createFetchRequest(Nil, new JFetchMetadata(resp1.sessionId(), 1), Nil)
    val resp2 = sendFetchRequest(0, req2)
    assertEquals(Errors.NONE, resp2.error())
    assertEquals("Expected the broker to continue the incremental fetch session",
      resp1.sessionId(), resp2.sessionId())
    assertFalse(resp2.responseData().containsKey(foo0))
    assertFalse(resp2.responseData().containsKey(foo1))
    assertTrue(resp2.responseData().containsKey(bar0))
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, resp2.responseData().get(bar0).error)
    createTopic("bar", Map(0 -> List(0, 1)))
    val req3 = createFetchRequest(Nil, new JFetchMetadata(resp1.sessionId(), 2), Nil)
    val resp3 = sendFetchRequest(0, req3)
    assertEquals(Errors.NONE, resp3.error())
    assertFalse(resp3.responseData().containsKey(foo0))
    assertFalse(resp3.responseData().containsKey(foo1))
    assertTrue(resp3.responseData().containsKey(bar0))
    assertEquals(Errors.NONE, resp3.responseData().get(bar0).error)
    val req4 = createFetchRequest(Nil, new JFetchMetadata(resp1.sessionId(), 3), Nil)
    val resp4 = sendFetchRequest(0, req4)
    assertEquals(Errors.NONE, resp4.error())
    assertFalse(resp4.responseData().containsKey(foo0))
    assertFalse(resp4.responseData().containsKey(foo1))
    assertFalse(resp4.responseData().containsKey(bar0))
  }

  private def records(partitionData: FetchResponse.PartitionData): Seq[Record] = {
    partitionData.records.records.asScala.toIndexedSeq
  }

  private def checkFetchResponse(expectedPartitions: Seq[TopicPartition], fetchResponse: FetchResponse,
                                 maxPartitionBytes: Int, maxResponseBytes: Int, numMessagesPerPartition: Int): Unit = {
    assertEquals(expectedPartitions, fetchResponse.responseData.keySet.asScala.toSeq)
    var emptyResponseSeen = false
    var responseSize = 0
    var responseBufferSize = 0

    expectedPartitions.foreach { tp =>
      val partitionData = fetchResponse.responseData.get(tp)
      assertEquals(Errors.NONE, partitionData.error)
      assertTrue(partitionData.highWatermark > 0)

      val records = partitionData.records
      responseBufferSize += records.sizeInBytes

      val batches = records.batches.asScala.toIndexedSeq
      assertTrue(batches.size < numMessagesPerPartition)
      val batchesSize = batches.map(_.sizeInBytes).sum
      responseSize += batchesSize
      if (batchesSize == 0 && !emptyResponseSeen) {
        assertEquals(0, records.sizeInBytes)
        emptyResponseSeen = true
      }
      else if (batchesSize != 0 && !emptyResponseSeen) {
        assertTrue(batchesSize <= maxPartitionBytes)
        assertEquals(maxPartitionBytes, records.sizeInBytes)
      }
      else if (batchesSize != 0 && emptyResponseSeen)
        fail(s"Expected partition with size 0, but found $tp with size $batchesSize")
      else if (records.sizeInBytes != 0 && emptyResponseSeen)
        fail(s"Expected partition buffer with size 0, but found $tp with size ${records.sizeInBytes}")
    }

    assertEquals(maxResponseBytes - maxResponseBytes % maxPartitionBytes, responseBufferSize)
    assertTrue(responseSize <= maxResponseBytes)
  }

  private def createTopics(numTopics: Int, numPartitions: Int, configs: Map[String, String] = Map.empty): Map[TopicPartition, Int] = {
    val topics = (0 until numPartitions).map(t => s"topic$t")
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.MinInSyncReplicasProp, 2.toString)
    configs.foreach { case (k, v) => topicConfig.setProperty(k, v) }
    topics.flatMap { topic =>
      val partitionToLeader = createTopic(topic, numPartitions = numPartitions, replicationFactor = 2,
        topicConfig = topicConfig)
      partitionToLeader.map { case (partition, leader) => new TopicPartition(topic, partition) -> leader }
    }.toMap
  }

  private def produceData(topicPartitions: Iterable[TopicPartition], numMessagesPerPartition: Int): Seq[ProducerRecord[String, String]] = {
    val records = for {
      tp <- topicPartitions.toSeq
      messageIndex <- 0 until numMessagesPerPartition
    } yield {
      val suffix = s"$tp-$messageIndex"
      new ProducerRecord(tp.topic, tp.partition, s"key $suffix", s"value $suffix")
    }
    records.map(producer.send(_).get)
    records
  }

}
