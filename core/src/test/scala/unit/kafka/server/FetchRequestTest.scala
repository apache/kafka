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

import java.util
import java.util.Properties

import kafka.log.LogConfig
import kafka.utils.TestUtils
import kafka.utils.TestUtils._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.LogEntry
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.apache.kafka.common.serialization.StringSerializer
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

  override def setUp() {
    super.setUp()
    producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers),
      retries = 5, keySerializer = new StringSerializer, valueSerializer = new StringSerializer)
  }

  override def tearDown() {
    producer.close()
    super.tearDown()
  }


  private def createFetchRequest(maxResponseBytes: Int, maxPartitionBytes: Int, topicPartitions: Seq[TopicPartition],
                                 offsetMap: Map[TopicPartition, Long] = Map.empty): FetchRequest =
    new FetchRequest.Builder(Int.MaxValue, 0, createPartitionMap(maxPartitionBytes, topicPartitions, offsetMap))
      .setMaxBytes(maxResponseBytes).build()

  private def createPartitionMap(maxPartitionBytes: Int, topicPartitions: Seq[TopicPartition],
                                 offsetMap: Map[TopicPartition, Long] = Map.empty): util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData] = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    topicPartitions.foreach { tp =>
      partitionMap.put(tp, new FetchRequest.PartitionData(offsetMap.getOrElse(tp, 0), maxPartitionBytes))
    }
    partitionMap
  }

  private def sendFetchRequest(leaderId: Int, request: FetchRequest): FetchResponse = {
    val response = send(request, ApiKeys.FETCH, destination = brokerSocketServer(leaderId))
    FetchResponse.parse(response)
  }

  @Test
  def testBrokerRespectsPartitionsOrderAndSizeLimits(): Unit = {
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
      logEntries(partitionData).map(_.sizeInBytes).sum
    }.sum
    assertTrue(responseSize3 <= maxResponseBytes)
    val partitionData3 = fetchResponse3.responseData.get(partitionWithLargeMessage1)
    assertEquals(Errors.NONE.code, partitionData3.errorCode)
    assertTrue(partitionData3.highWatermark > 0)
    val size3 = logEntries(partitionData3).map(_.sizeInBytes).sum
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
      case (tp, partitionData) if logEntries(partitionData).map(_.sizeInBytes).sum > 0 => tp
    }
    assertEquals(Seq(partitionWithLargeMessage2), nonEmptyPartitions4)
    val partitionData4 = fetchResponse4.responseData.get(partitionWithLargeMessage2)
    assertEquals(Errors.NONE.code, partitionData4.errorCode)
    assertTrue(partitionData4.highWatermark > 0)
    val size4 = logEntries(partitionData4).map(_.sizeInBytes).sum
    assertTrue(s"Expected $size4 to be larger than $maxResponseBytes", size4 > maxResponseBytes)
    assertTrue(maxResponseBytes < partitionData4.records.sizeInBytes)
  }

  @Test
  def testFetchRequestV2WithOversizedMessage(): Unit = {
    val maxPartitionBytes = 200
    val (topicPartition, leaderId) = createTopics(numTopics = 1, numPartitions = 1).head
    producer.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key", new String(new Array[Byte](maxPartitionBytes + 1)))).get
    val fetchRequestBuilder = new FetchRequest.Builder(
      Int.MaxValue, 0, createPartitionMap(maxPartitionBytes, Seq(topicPartition))).
      setVersion(2)
    val fetchResponse = sendFetchRequest(leaderId, fetchRequestBuilder.build())
    val partitionData = fetchResponse.responseData.get(topicPartition)
    assertEquals(Errors.NONE.code, partitionData.errorCode)
    assertTrue(partitionData.highWatermark > 0)
    assertEquals(maxPartitionBytes, partitionData.records.sizeInBytes)
    assertEquals(0, logEntries(partitionData).map(_.sizeInBytes).sum)
  }

  private def logEntries(partitionData: FetchResponse.PartitionData): Seq[LogEntry] = {
    partitionData.records.deepEntries.asScala.toIndexedSeq
  }

  private def checkFetchResponse(expectedPartitions: Seq[TopicPartition], fetchResponse: FetchResponse,
                                 maxPartitionBytes: Int, maxResponseBytes: Int, numMessagesPerPartition: Int): Unit = {
    assertEquals(expectedPartitions, fetchResponse.responseData.keySet.asScala.toSeq)
    var emptyResponseSeen = false
    var responseSize = 0
    var responseBufferSize = 0

    expectedPartitions.foreach { tp =>
      val partitionData = fetchResponse.responseData.get(tp)
      assertEquals(Errors.NONE.code, partitionData.errorCode)
      assertTrue(partitionData.highWatermark > 0)

      val records = partitionData.records
      responseBufferSize += records.sizeInBytes

      val entries = records.shallowEntries.asScala.toIndexedSeq
      assertTrue(entries.size < numMessagesPerPartition)
      val entriesSize = entries.map(_.sizeInBytes).sum
      responseSize += entriesSize
      if (entriesSize == 0 && !emptyResponseSeen) {
        assertEquals(0, records.sizeInBytes)
        emptyResponseSeen = true
      }
      else if (entriesSize != 0 && !emptyResponseSeen) {
        assertTrue(entriesSize <= maxPartitionBytes)
        assertEquals(maxPartitionBytes, records.sizeInBytes)
      }
      else if (entriesSize != 0 && emptyResponseSeen)
        fail(s"Expected partition with size 0, but found $tp with size $entriesSize")
      else if (records.sizeInBytes != 0 && emptyResponseSeen)
        fail(s"Expected partition buffer with size 0, but found $tp with size ${records.sizeInBytes}")

    }

    assertEquals(maxResponseBytes - maxResponseBytes % maxPartitionBytes, responseBufferSize)
    assertTrue(responseSize <= maxResponseBytes)
  }

  private def createTopics(numTopics: Int, numPartitions: Int): Map[TopicPartition, Int] = {
    val topics = (0 until numPartitions).map(t => s"topic$t")
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.MinInSyncReplicasProp, 2.toString)
    topics.flatMap { topic =>
      val partitionToLeader = createTopic(zkUtils, topic, numPartitions = numPartitions, replicationFactor = 2,
        servers = servers, topicConfig = topicConfig)
      partitionToLeader.map { case (partition, leader) => new TopicPartition(topic, partition) -> leader.get }
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
    records.map(producer.send).foreach(_.get)
    records
  }

}
