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

package kafka.server

import java.util
import java.util.{Optional, Properties}

import kafka.api.KAFKA_2_7_IV0
import kafka.network.SocketServer
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, MetadataRequest, MetadataResponse}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.jdk.CollectionConverters._
import scala.collection.Seq

class TopicIdWithOldInterBrokerProtocolTest extends BaseRequestTest {

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.setProperty(KafkaConfig.InterBrokerProtocolVersionProp, KAFKA_2_7_IV0.toString)
    properties.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.setProperty(KafkaConfig.DefaultReplicationFactorProp, "2")
    properties.setProperty(KafkaConfig.RackProp, s"rack/${properties.getProperty(KafkaConfig.BrokerIdProp)}")
  }

  @BeforeEach
  override def setUp(): Unit = {
    doSetup(createOffsetsTopic = false)
  }

  @Test
  def testMetadataTopicIdsWithOldIBP(): Unit = {
    val replicaAssignment = Map(0 -> Seq(1, 2, 0), 1 -> Seq(2, 0, 1))
    val topic1 = "topic1"
    createTopic(topic1, replicaAssignment)

    val resp = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1, topic1).asJava, true, 10, 10).build(), Some(notControllerSocketServer))
    assertEquals(1, resp.topicMetadata.size)
    resp.topicMetadata.forEach { topicMetadata =>
      assertEquals(Errors.NONE, topicMetadata.error)
      assertEquals(Uuid.ZERO_UUID, topicMetadata.topicId())
    }
  }

  // This also simulates sending a fetch to a broker that is still in the process of updating.
  @Test
  def testFetchTopicIdsWithOldIBPWrongFetchVersion(): Unit = {
    val replicaAssignment = Map(0 -> Seq(1, 2, 0), 1 -> Seq(2, 0, 1))
    val topic1 = "topic1"
    val tp0 = new TopicPartition("topic1", 0)
    val maxResponseBytes = 800
    val maxPartitionBytes = 190
    val topicIds = Map("topic1" -> Uuid.randomUuid())
    val topicNames = topicIds.map(_.swap)

    val leadersMap = createTopic(topic1, replicaAssignment)
    val req = createFetchRequest(maxResponseBytes, maxPartitionBytes, Seq(tp0),  Map.empty, topicIds, ApiKeys.FETCH.latestVersion())
    val resp = sendFetchRequest(leadersMap(0), req)

    val responseData = resp.responseData(topicNames.asJava)
    assertEquals(Errors.UNSUPPORTED_VERSION, responseData.get(tp0).error());
  }

  @Test
  def testFetchTopicIdsWithOldIBPCorrectFetchVersion(): Unit = {
    val replicaAssignment = Map(0 -> Seq(1, 2, 0), 1 -> Seq(2, 0, 1))
    val topic1 = "topic1"
    val tp0 = new TopicPartition("topic1", 0)
    val maxResponseBytes = 800
    val maxPartitionBytes = 190
    val topicIds = Map("topic1" -> Uuid.randomUuid())
    val topicNames = topicIds.map(_.swap)

    val leadersMap = createTopic(topic1, replicaAssignment)
    val req = createFetchRequest(maxResponseBytes, maxPartitionBytes, Seq(tp0),  Map.empty, topicIds, 12)
    val resp = sendFetchRequest(leadersMap(0), req)

    assertEquals(Errors.NONE, resp.error())

    val responseData = resp.responseData(topicNames.asJava)
    assertEquals(Errors.NONE, responseData.get(tp0).error());
  }

  private def sendMetadataRequest(request: MetadataRequest, destination: Option[SocketServer]): MetadataResponse = {
    connectAndReceive[MetadataResponse](request, destination = destination.getOrElse(anySocketServer))
  }

  private def createFetchRequest(maxResponseBytes: Int, maxPartitionBytes: Int, topicPartitions: Seq[TopicPartition],
                                 offsetMap: Map[TopicPartition, Long],
                                 topicIds: Map[String, Uuid],
                                 version: Short): FetchRequest = {
    FetchRequest.Builder.forConsumer(version, Int.MaxValue, 0, createPartitionMap(maxPartitionBytes, topicPartitions, offsetMap), topicIds.asJava)
      .setMaxBytes(maxResponseBytes).build()
  }

  private def createPartitionMap(maxPartitionBytes: Int, topicPartitions: Seq[TopicPartition],
                                 offsetMap: Map[TopicPartition, Long]): util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData] = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    topicPartitions.foreach { tp =>
      partitionMap.put(tp, new FetchRequest.PartitionData(offsetMap.getOrElse(tp, 0), 0L, maxPartitionBytes,
        Optional.empty()))
    }
    partitionMap
  }

  private def sendFetchRequest(leaderId: Int, request: FetchRequest): FetchResponse[MemoryRecords] = {
    connectAndReceive[FetchResponse[MemoryRecords]](request, destination = brokerSocketServer(leaderId))
  }

}
