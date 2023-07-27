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
import java.util.{Optional, Properties}
import kafka.network.RequestMetrics.{MessageConversionsTimeMs, TemporaryMemoryBytes}
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.jdk.CollectionConverters._

class FetchRequestDownConversionConfigTest extends BaseRequestTest {
  private var producer: KafkaProducer[String, String] = _
  override def brokerCount: Int = 2

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    initProducer()
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (producer != null)
      producer.close()
    super.tearDown()
  }

  override protected def brokerPropertyOverrides(properties: Properties): Unit = {
    super.brokerPropertyOverrides(properties)
    properties.put(KafkaConfig.LogMessageDownConversionEnableProp, "false")
  }

  private def initProducer(): Unit = {
    producer = TestUtils.createProducer(bootstrapServers(),
      keySerializer = new StringSerializer, valueSerializer = new StringSerializer)
  }

  private def createTopics(numTopics: Int, numPartitions: Int,
                           configs: Map[String, String] = Map.empty, topicSuffixStart: Int = 0): Map[TopicPartition, Int] = {
    val topics = (0 until numTopics).map(t => s"topic${t + topicSuffixStart}")
    val topicConfig = new Properties
    topicConfig.setProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, 1.toString)
    configs.foreach { case (k, v) => topicConfig.setProperty(k, v) }
    topics.flatMap { topic =>
      val partitionToLeader = createTopic(
        topic,
        numPartitions = numPartitions,
        replicationFactor = 2,
        topicConfig = topicConfig
      )
      partitionToLeader.map { case (partition, leader) => new TopicPartition(topic, partition) -> leader }
    }.toMap
  }

  private def createPartitionMap(maxPartitionBytes: Int, topicPartitions: Seq[TopicPartition],
                                 topicIds: Map[String, Uuid],
                                 offsetMap: Map[TopicPartition, Long] = Map.empty): util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData] = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    topicPartitions.foreach { tp =>
      partitionMap.put(tp, new FetchRequest.PartitionData(topicIds.getOrElse(tp.topic, Uuid.ZERO_UUID), offsetMap.getOrElse(tp, 0), 0L,
        maxPartitionBytes, Optional.empty()))
    }
    partitionMap
  }

  private def sendFetchRequest(leaderId: Int, request: FetchRequest): FetchResponse = {
    connectAndReceive[FetchResponse](request, destination = brokerSocketServer(leaderId))
  }

  /**
   * Tests that fetch request that require down-conversion returns with an error response when down-conversion is disabled on broker.
   */
  @Test
  def testV1FetchWithDownConversionDisabled(): Unit = {
    val topicMap = createTopics(numTopics = 5, numPartitions = 1)
    val topicPartitions = topicMap.keySet.toSeq
    val topicIds = servers.head.kafkaController.controllerContext.topicIds
    val topicNames = topicIds.map(_.swap)
    topicPartitions.foreach(tp => producer.send(new ProducerRecord(tp.topic(), "key", "value")).get())
    val fetchRequest = FetchRequest.Builder.forConsumer(1, Int.MaxValue, 0, createPartitionMap(1024,
      topicPartitions, topicIds.toMap)).build(1)
    val fetchResponse = sendFetchRequest(topicMap.head._2, fetchRequest)
    val fetchResponseData = fetchResponse.responseData(topicNames.asJava, 1)
    topicPartitions.foreach(tp => assertEquals(Errors.UNSUPPORTED_VERSION, Errors.forCode(fetchResponseData.get(tp).errorCode)))
  }

  /**
   * Tests that "message.downconversion.enable" has no effect when down-conversion is not required.
   */
  @Test
  def testLatestFetchWithDownConversionDisabled(): Unit = {
    val topicMap = createTopics(numTopics = 5, numPartitions = 1)
    val topicPartitions = topicMap.keySet.toSeq
    val topicIds = servers.head.kafkaController.controllerContext.topicIds
    val topicNames = topicIds.map(_.swap)
    topicPartitions.foreach(tp => producer.send(new ProducerRecord(tp.topic(), "key", "value")).get())
    val fetchRequest = FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion, Int.MaxValue, 0, createPartitionMap(1024,
      topicPartitions, topicIds.toMap)).build()
    val fetchResponse = sendFetchRequest(topicMap.head._2, fetchRequest)
    val fetchResponseData = fetchResponse.responseData(topicNames.asJava, ApiKeys.FETCH.latestVersion)
    topicPartitions.foreach(tp => assertEquals(Errors.NONE, Errors.forCode(fetchResponseData.get(tp).errorCode)))
  }

  /**
   * Tests that "message.downconversion.enable" has no effect when down-conversion is not required on last version before topic IDs.
   */
  @Test
  def testV12WithDownConversionDisabled(): Unit = {
    val topicMap = createTopics(numTopics = 5, numPartitions = 1)
    val topicPartitions = topicMap.keySet.toSeq
    val topicIds = servers.head.kafkaController.controllerContext.topicIds
    val topicNames = topicIds.map(_.swap)
    topicPartitions.foreach(tp => producer.send(new ProducerRecord(tp.topic(), "key", "value")).get())
    val fetchRequest = FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion, Int.MaxValue, 0, createPartitionMap(1024,
      topicPartitions, topicIds.toMap)).build(12)
    val fetchResponse = sendFetchRequest(topicMap.head._2, fetchRequest)
    val fetchResponseData = fetchResponse.responseData(topicNames.asJava, 12)
    topicPartitions.foreach(tp => assertEquals(Errors.NONE, Errors.forCode(fetchResponseData.get(tp).errorCode)))
  }

  /**
   * Tests that "message.downconversion.enable" can be set at topic level, and its configuration is obeyed for client
   * fetch requests.
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testV1FetchFromConsumer(quorum: String): Unit = {
    testV1Fetch(isFollowerFetch = false)
  }

  /**
   * Tests that "message.downconversion.enable" has no effect on fetch requests from replicas.
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testV1FetchFromReplica(quorum: String): Unit = {
    testV1Fetch(isFollowerFetch = true)
  }

  def testV1Fetch(isFollowerFetch: Boolean): Unit = {
    val fetchRequest = "request=Fetch"
    val fetchTemporaryMemoryBytesMetricName = s"$TemporaryMemoryBytes,$fetchRequest"
    val fetchMessageConversionsTimeMsMetricName = s"$MessageConversionsTimeMs,$fetchRequest"
    val initialFetchMessageConversionsPerSec = TestUtils.metersCount(BrokerTopicStats.FetchMessageConversionsPerSec)
    val initialFetchMessageConversionsTimeMs = TestUtils.metersCount(fetchMessageConversionsTimeMsMetricName)
    val initialFetchTemporaryMemoryBytes = TestUtils.metersCount(fetchTemporaryMemoryBytesMetricName)
    val topicWithDownConversionEnabled = "foo"
    val topicWithDownConversionDisabled = "bar"
    val replicaIds = brokers.map(_.config.brokerId)
    val leaderId = replicaIds.head
    val followerId = replicaIds.last

    val admin = createAdminClient()

    val topicWithDownConversionDisabledId = TestUtils.createTopicWithAdminRaw(
      admin,
      topicWithDownConversionDisabled,
      replicaAssignment = Map(0 -> replicaIds)
    )

    val topicConfig = new Properties
    topicConfig.put(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, "true")
    val topicWithDownConversionEnabledId = TestUtils.createTopicWithAdminRaw(
      admin,
      topicWithDownConversionEnabled,
      replicaAssignment = Map(0 -> replicaIds),
      topicConfig = topicConfig
    )

    val partitionWithDownConversionEnabled = new TopicPartition(topicWithDownConversionEnabled, 0)
    val partitionWithDownConversionDisabled = new TopicPartition(topicWithDownConversionDisabled, 0)

    val allTopicPartitions = Seq(
      partitionWithDownConversionEnabled,
      partitionWithDownConversionDisabled
    )

    allTopicPartitions.foreach { tp =>
      producer.send(new ProducerRecord(tp.topic, "key", "value")).get()
    }

    val topicIdMap = Map(
      topicWithDownConversionEnabled -> topicWithDownConversionEnabledId,
      topicWithDownConversionDisabled -> topicWithDownConversionDisabledId
    )

    val fetchResponseData = sendFetch(
      leaderId,
      allTopicPartitions,
      topicIdMap,
      fetchVersion = 1,
      replicaIdOpt = if (isFollowerFetch) Some(followerId) else None
    )

    def error(tp: TopicPartition): Errors = {
      Errors.forCode(fetchResponseData.get(tp).errorCode)
    }

    def verifyMetrics(): Unit = {
      TestUtils.waitUntilTrue(() => TestUtils.metersCount(BrokerTopicStats.FetchMessageConversionsPerSec) > initialFetchMessageConversionsPerSec,
        s"The `FetchMessageConversionsPerSec` metric count is not incremented after 5 seconds. " +
          s"init: $initialFetchMessageConversionsPerSec final: ${TestUtils.metersCount(BrokerTopicStats.FetchMessageConversionsPerSec)}", 5000)

      TestUtils.waitUntilTrue(() => TestUtils.metersCount(fetchMessageConversionsTimeMsMetricName) > initialFetchMessageConversionsTimeMs,
        s"The `MessageConversionsTimeMs` in fetch request metric count is not incremented after 5 seconds. " +
          s"init: $initialFetchMessageConversionsTimeMs final: ${TestUtils.metersCount(fetchMessageConversionsTimeMsMetricName)}", 5000)

      TestUtils.waitUntilTrue(() => TestUtils.metersCount(fetchTemporaryMemoryBytesMetricName) > initialFetchTemporaryMemoryBytes,
        s"The `TemporaryMemoryBytes` in fetch request metric count is not incremented after 5 seconds. " +
          s"init: $initialFetchTemporaryMemoryBytes final: ${TestUtils.metersCount(fetchTemporaryMemoryBytesMetricName)}", 5000)
    }

    assertEquals(Errors.NONE, error(partitionWithDownConversionEnabled))
    if (isFollowerFetch) {
      assertEquals(Errors.NONE, error(partitionWithDownConversionDisabled))
    } else {
      assertEquals(Errors.UNSUPPORTED_VERSION, error(partitionWithDownConversionDisabled))
    }

    verifyMetrics()
  }

  private def sendFetch(
    leaderId: Int,
    partitions: Seq[TopicPartition],
    topicIdMap: Map[String, Uuid],
    fetchVersion: Short,
    replicaIdOpt: Option[Int]
  ): util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData] = {
    val topicNameMap = topicIdMap.map(_.swap)
    val partitionMap = createPartitionMap(1024, partitions, topicIdMap)

    val fetchRequest = replicaIdOpt.map { replicaId =>
      FetchRequest.Builder.forReplica(fetchVersion, replicaId, -1, Int.MaxValue, 0, partitionMap)
        .build(fetchVersion)
    }.getOrElse {
      FetchRequest.Builder.forConsumer(fetchVersion, Int.MaxValue, 0, partitionMap)
        .build(fetchVersion)
    }

    val fetchResponse = sendFetchRequest(leaderId, fetchRequest)
    fetchResponse.responseData(topicNameMap.asJava, fetchVersion)
  }
}
