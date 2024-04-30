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
import kafka.network.SocketServer
import kafka.utils.TestUtils
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.DeleteTopicsRequestData
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{DeleteTopicsRequest, DeleteTopicsResponse, FetchRequest, FetchResponse, MetadataRequest, MetadataResponse}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.common.MetadataVersion.IBP_2_7_IV0
import org.apache.kafka.server.config.ReplicationConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test, TestInfo}

import scala.collection.Seq
import scala.jdk.CollectionConverters._

class TopicIdWithOldInterBrokerProtocolTest extends BaseRequestTest {

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.setProperty(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, IBP_2_7_IV0.toString)
    properties.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "1")
    properties.setProperty(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "2")
    properties.setProperty(KafkaConfig.RackProp, s"rack/${properties.getProperty(KafkaConfig.BrokerIdProp)}")
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    doSetup(testInfo, createOffsetsTopic = false)
  }

  @Test
  def testMetadataTopicIdsWithOldIBP(): Unit = {
    val replicaAssignment = Map(0 -> Seq(1, 2, 0), 1 -> Seq(2, 0, 1))
    val topic1 = "topic1"
    createTopicWithAssignment(topic1, replicaAssignment)

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
    val tidp0 = new TopicIdPartition(topicIds(topic1), tp0)

    val leadersMap = createTopicWithAssignment(topic1, replicaAssignment)
    val req = createFetchRequest(maxResponseBytes, maxPartitionBytes, Seq(tidp0), Map.empty, ApiKeys.FETCH.latestVersion())
    val resp = sendFetchRequest(leadersMap(0), req)

    val responseData = resp.responseData(topicNames.asJava, ApiKeys.FETCH.latestVersion())
    assertEquals(Errors.NONE.code, resp.error().code())
    assertEquals(1, responseData.size())
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code, responseData.get(tp0).errorCode)
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
    val tidp0 = new TopicIdPartition(topicIds(topic1), tp0)

    val leadersMap = createTopicWithAssignment(topic1, replicaAssignment)
    val req = createFetchRequest(maxResponseBytes, maxPartitionBytes, Seq(tidp0), Map.empty, 12)
    val resp = sendFetchRequest(leadersMap(0), req)

    assertEquals(Errors.NONE, resp.error())

    val responseData = resp.responseData(topicNames.asJava, 12)
    assertEquals(Errors.NONE.code, responseData.get(tp0).errorCode)
  }

  @Test
  def testDeleteTopicsWithOldIBP(): Unit = {
    val timeout = 10000
    createTopic("topic-3", 5, 2)
    createTopic("topic-4", 1, 2)
    val request = new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData()
        .setTopicNames(util.Arrays.asList("topic-3", "topic-4"))
        .setTimeoutMs(timeout)).build()
    val resp = sendDeleteTopicsRequest(request)
    val error = resp.errorCounts.asScala.find(_._1 != Errors.NONE)
    assertTrue(error.isEmpty, s"There should be no errors, found ${resp.data.responses.asScala}")
    request.data.topicNames.forEach { topic =>
      validateTopicIsDeleted(topic)
    }
    resp.data.responses.forEach { response =>
      assertEquals(Uuid.ZERO_UUID, response.topicId())
    }
  }

  @Test
  def testDeleteTopicsWithOldIBPUsingIDs(): Unit = {
    val timeout = 10000
    createTopic("topic-7", 3, 2)
    createTopic("topic-6", 1, 2)
    val ids = Map("topic-7" -> Uuid.randomUuid(), "topic-6" -> Uuid.randomUuid())
    val request = new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData()
        .setTopics(util.Arrays.asList(new DeleteTopicState().setTopicId(ids("topic-7")),
          new DeleteTopicState().setTopicId(ids("topic-6"))
        )).setTimeoutMs(timeout)).build()
    val response = sendDeleteTopicsRequest(request)
    val error = response.errorCounts.asScala
    assertEquals(2, error(Errors.UNKNOWN_TOPIC_ID))
  }

  private def sendMetadataRequest(request: MetadataRequest, destination: Option[SocketServer]): MetadataResponse = {
    connectAndReceive[MetadataResponse](request, destination = destination.getOrElse(anySocketServer))
  }

  private def createFetchRequest(maxResponseBytes: Int, maxPartitionBytes: Int, topicPartitions: Seq[TopicIdPartition],
                                 offsetMap: Map[TopicPartition, Long],
                                 version: Short): FetchRequest = {
    FetchRequest.Builder.forConsumer(version, Int.MaxValue, 0, createPartitionMap(maxPartitionBytes, topicPartitions, offsetMap))
      .setMaxBytes(maxResponseBytes).build()
  }

  private def createPartitionMap(maxPartitionBytes: Int, topicPartitions: Seq[TopicIdPartition],
                                 offsetMap: Map[TopicPartition, Long]): util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData] = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    topicPartitions.foreach { tp =>
      partitionMap.put(tp.topicPartition, new FetchRequest.PartitionData(tp.topicId, offsetMap.getOrElse(tp.topicPartition, 0), 0L, maxPartitionBytes,
        Optional.empty()))
    }
    partitionMap
  }

  private def sendFetchRequest(leaderId: Int, request: FetchRequest): FetchResponse = {
    connectAndReceive[FetchResponse](request, destination = brokerSocketServer(leaderId))
  }

  private def sendDeleteTopicsRequest(request: DeleteTopicsRequest, socketServer: SocketServer = controllerSocketServer): DeleteTopicsResponse = {
    connectAndReceive[DeleteTopicsResponse](request, destination = socketServer)
  }

  private def validateTopicIsDeleted(topic: String): Unit = {
    val metadata = connectAndReceive[MetadataResponse](new MetadataRequest.Builder(
      List(topic).asJava, true).build).topicMetadata.asScala
    TestUtils.waitUntilTrue(() => !metadata.exists(p => p.topic.equals(topic) && p.error == Errors.NONE),
      s"The topic $topic should not exist")
  }

}
