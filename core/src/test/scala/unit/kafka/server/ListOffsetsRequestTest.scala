/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import kafka.utils.TestUtils
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ListOffsetsRequest, ListOffsetsResponse}
import org.apache.kafka.common.{IsolationLevel, TopicPartition}
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.{Optional, Properties}
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class ListOffsetsRequestTest extends BaseRequestTest {

  val topic = "topic"
  val partition = new TopicPartition(topic, 0)

  override def modifyConfigs(props: Seq[Properties]): Unit = {
    super.modifyConfigs(props)
    props.foreach { p =>
      p.put(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true")
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testListOffsetsErrorCodes(quorum: String): Unit = {
    val targetTimes = List(new ListOffsetsTopic()
      .setName(topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(partition.partition)
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)
        .setCurrentLeaderEpoch(0)).asJava)).asJava

    val consumerRequest = ListOffsetsRequest.Builder
      .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes)
      .build()

    val replicaRequest = ListOffsetsRequest.Builder
      .forReplica(ApiKeys.LIST_OFFSETS.latestVersion, brokers.head.config.brokerId)
      .setTargetTimes(targetTimes)
      .build()

    val debugReplicaRequest = ListOffsetsRequest.Builder
      .forReplica(ApiKeys.LIST_OFFSETS.latestVersion, ListOffsetsRequest.DEBUGGING_REPLICA_ID)
      .setTargetTimes(targetTimes)
      .build()

    // Unknown topic
    val randomBrokerId = brokers.head.config.brokerId
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, consumerRequest)
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, replicaRequest)
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, debugReplicaRequest)

    val partitionToLeader = createTopic(numPartitions = 1, replicationFactor = 2)
    val topicDescription = createAdminClient().describeTopics(Seq(partition.topic).asJava).allTopicNames.get
    val replicas = topicDescription.get(partition.topic).partitions.get(partition.partition).replicas.asScala.map(_.id).toSet
    val leader = partitionToLeader(partition.partition)
    val follower = replicas.find(_ != leader).get
    val nonReplica = brokers.map(_.config.brokerId).find(!replicas.contains(_)).get

    // Follower
    assertResponseError(Errors.NOT_LEADER_OR_FOLLOWER, follower, consumerRequest)
    assertResponseError(Errors.NOT_LEADER_OR_FOLLOWER, follower, replicaRequest)
    assertResponseError(Errors.NONE, follower, debugReplicaRequest)

    // Non-replica
    assertResponseError(Errors.NOT_LEADER_OR_FOLLOWER, nonReplica, consumerRequest)
    assertResponseError(Errors.NOT_LEADER_OR_FOLLOWER, nonReplica, replicaRequest)
    assertResponseError(Errors.NOT_LEADER_OR_FOLLOWER, nonReplica, debugReplicaRequest)
  }

  def assertResponseErrorForEpoch(error: Errors, brokerId: Int, currentLeaderEpoch: Optional[Integer]): Unit = {
    val listOffsetPartition = new ListOffsetsPartition()
      .setPartitionIndex(partition.partition)
      .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)
    if (currentLeaderEpoch.isPresent)
      listOffsetPartition.setCurrentLeaderEpoch(currentLeaderEpoch.get)
    val targetTimes = List(new ListOffsetsTopic()
      .setName(topic)
      .setPartitions(List(listOffsetPartition).asJava)).asJava
    val request = ListOffsetsRequest.Builder
      .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes)
      .build()
    assertResponseError(error, brokerId, request)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCurrentEpochValidation(quorum: String): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val partitionToLeader = createTopic(numPartitions = 1, replicationFactor = 3)
    val firstLeaderId = partitionToLeader(topicPartition.partition)

    // We need a leader change in order to check epoch fencing since the first epoch is 0 and
    // -1 is treated as having no epoch at all
    killBroker(firstLeaderId)

    // Check leader error codes
    val secondLeaderId = TestUtils.awaitLeaderChange(brokers, topicPartition, oldLeaderOpt = Some(firstLeaderId))
    val secondLeaderEpoch = TestUtils.findLeaderEpoch(secondLeaderId, topicPartition, brokers)
    assertResponseErrorForEpoch(Errors.NONE, secondLeaderId, Optional.empty())
    assertResponseErrorForEpoch(Errors.NONE, secondLeaderId, Optional.of(secondLeaderEpoch))
    assertResponseErrorForEpoch(Errors.FENCED_LEADER_EPOCH, secondLeaderId, Optional.of(secondLeaderEpoch - 1))
    assertResponseErrorForEpoch(Errors.UNKNOWN_LEADER_EPOCH, secondLeaderId, Optional.of(secondLeaderEpoch + 1))

    // Check follower error codes
    val followerId = TestUtils.findFollowerId(topicPartition, brokers)
    assertResponseErrorForEpoch(Errors.NOT_LEADER_OR_FOLLOWER, followerId, Optional.empty())
    assertResponseErrorForEpoch(Errors.NOT_LEADER_OR_FOLLOWER, followerId, Optional.of(secondLeaderEpoch))
    assertResponseErrorForEpoch(Errors.UNKNOWN_LEADER_EPOCH, followerId, Optional.of(secondLeaderEpoch + 1))
    assertResponseErrorForEpoch(Errors.FENCED_LEADER_EPOCH, followerId, Optional.of(secondLeaderEpoch - 1))
  }

  private[this] def sendRequest(serverId: Int,
                                timestamp: Long,
                                version: Short): ListOffsetsPartitionResponse = {
    val targetTimes = List(new ListOffsetsTopic()
      .setName(topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(partition.partition)
        .setTimestamp(timestamp)).asJava)).asJava

    val builder = ListOffsetsRequest.Builder
      .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes)

    val request = if (version == -1) builder.build() else builder.build(version)

    sendRequest(serverId, request).topics.asScala.find(_.name == topic).get
      .partitions.asScala.find(_.partitionIndex == partition.partition).get
  }

  // -1 indicate "latest"
  private[this] def fetchOffsetAndEpoch(serverId: Int,
                                        timestamp: Long,
                                        version: Short): (Long, Int) = {
    val (offset, leaderEpoch, _) = fetchOffsetAndEpochWithError(serverId, timestamp, version)
    (offset, leaderEpoch)
  }

  private[this] def fetchOffsetAndEpochWithError(serverId: Int, timestamp: Long, version: Short): (Long, Int, Short) = {
    val partitionData = sendRequest(serverId, timestamp, version)
    (partitionData.offset, partitionData.leaderEpoch, partitionData.errorCode())
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testResponseIncludesLeaderEpoch(quorum: String): Unit = {
    val partitionToLeader = createTopic(numPartitions = 1, replicationFactor = 3)
    val firstLeaderId = partitionToLeader(partition.partition)

    TestUtils.generateAndProduceMessages(brokers, topic, 9)
    TestUtils.produceMessage(brokers, topic, "test-10", System.currentTimeMillis() + 10L)

    val firstLeaderEpoch = TestUtils.findLeaderEpoch(firstLeaderId, partition, brokers)

    assertEquals((0L, firstLeaderEpoch), fetchOffsetAndEpoch(firstLeaderId, 0L, -1))
    assertEquals((0L, firstLeaderEpoch), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_TIMESTAMP, -1))
    assertEquals((0L, firstLeaderEpoch), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, version = -1))
    assertEquals((10L, firstLeaderEpoch), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.LATEST_TIMESTAMP, -1))
    assertEquals((9L, firstLeaderEpoch), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.MAX_TIMESTAMP, -1))

    // Kill the first leader so that we can verify the epoch change when fetching the latest offset
    killBroker(firstLeaderId)
    val secondLeaderId = TestUtils.awaitLeaderChange(brokers, partition, oldLeaderOpt = Some(firstLeaderId))
    // make sure high watermark of new leader has caught up
    TestUtils.waitUntilTrue(() => sendRequest(secondLeaderId, ListOffsetsRequest.LATEST_TIMESTAMP, -1).errorCode != Errors.OFFSET_NOT_AVAILABLE.code,
      "the second leader does not sync to follower")
    val secondLeaderEpoch = TestUtils.findLeaderEpoch(secondLeaderId, partition, brokers)

    // No changes to written data
    assertEquals((0L, firstLeaderEpoch), fetchOffsetAndEpoch(secondLeaderId, 0L, -1))
    assertEquals((0L, firstLeaderEpoch), fetchOffsetAndEpoch(secondLeaderId, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, -1))

    assertEquals((0L, firstLeaderEpoch), fetchOffsetAndEpoch(secondLeaderId, 0L, -1))
    assertEquals((0L, firstLeaderEpoch), fetchOffsetAndEpoch(secondLeaderId, ListOffsetsRequest.EARLIEST_TIMESTAMP, -1))

    assertEquals((0L, firstLeaderEpoch), fetchOffsetAndEpoch(secondLeaderId, 0L, -1))
    assertEquals((0L, firstLeaderEpoch), fetchOffsetAndEpoch(secondLeaderId, ListOffsetsRequest.EARLIEST_TIMESTAMP, -1))

    // The latest offset reflects the updated epoch
    assertEquals((10L, secondLeaderEpoch, Errors.NONE.code), fetchOffsetAndEpochWithError(secondLeaderId, ListOffsetsRequest.LATEST_TIMESTAMP, -1))
    // No changes of epoch since the offset of max timestamp reflects the epoch of batch
    assertEquals((9L, firstLeaderEpoch, Errors.NONE.code), fetchOffsetAndEpochWithError(secondLeaderId, ListOffsetsRequest.MAX_TIMESTAMP, -1))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testResponseDefaultOffsetAndLeaderEpochForAllVersions(quorum: String): Unit = {
    val partitionToLeader = createTopic(numPartitions = 1, replicationFactor = 3)
    val firstLeaderId = partitionToLeader(partition.partition)

    TestUtils.generateAndProduceMessages(brokers, topic, 9)
    TestUtils.produceMessage(brokers, topic, "test-10", System.currentTimeMillis() + 10L)

    for (version <- ApiKeys.LIST_OFFSETS.oldestVersion to ApiKeys.LIST_OFFSETS.latestVersion) {
      if (version == 0) {
        assertEquals((-1L, -1), fetchOffsetAndEpoch(firstLeaderId, 0L, version.toShort))
        assertEquals((0L, -1), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_TIMESTAMP, version.toShort))
        assertEquals((10L, -1), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.LATEST_TIMESTAMP, version.toShort))
        assertEquals((-1L, -1, Errors.UNSUPPORTED_VERSION.code()), fetchOffsetAndEpochWithError(firstLeaderId, ListOffsetsRequest.MAX_TIMESTAMP, version.toShort))
        assertEquals((-1L, -1, Errors.UNSUPPORTED_VERSION.code()), fetchOffsetAndEpochWithError(firstLeaderId, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, version.toShort))
      } else if (version >= 1 && version <= 3) {
        assertEquals((0L, -1), fetchOffsetAndEpoch(firstLeaderId, 0L, version.toShort))
        assertEquals((0L, -1), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_TIMESTAMP, version.toShort))
        assertEquals((10L, -1), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.LATEST_TIMESTAMP, version.toShort))
        assertEquals((-1L, -1, Errors.UNSUPPORTED_VERSION.code()), fetchOffsetAndEpochWithError(firstLeaderId, ListOffsetsRequest.MAX_TIMESTAMP, version.toShort))
        assertEquals((-1L, -1, Errors.UNSUPPORTED_VERSION.code()), fetchOffsetAndEpochWithError(firstLeaderId, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, version.toShort))
      } else if (version >= 4 && version <= 6) {
        assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, 0L, version.toShort))
        assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_TIMESTAMP, version.toShort))
        assertEquals((10L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.LATEST_TIMESTAMP, version.toShort))
        assertEquals((-1L, -1, Errors.UNSUPPORTED_VERSION.code()), fetchOffsetAndEpochWithError(firstLeaderId, ListOffsetsRequest.MAX_TIMESTAMP, version.toShort))
        assertEquals((-1L, -1, Errors.UNSUPPORTED_VERSION.code()), fetchOffsetAndEpochWithError(firstLeaderId, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, version.toShort))
      } else if (version == 7) {
        assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, 0L, version.toShort))
        assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_TIMESTAMP, version.toShort))
        assertEquals((10L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.LATEST_TIMESTAMP, version.toShort))
        assertEquals((9L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.MAX_TIMESTAMP, version.toShort))
        assertEquals((-1L, -1, Errors.UNSUPPORTED_VERSION.code()), fetchOffsetAndEpochWithError(firstLeaderId, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, version.toShort))
      } else if (version >= 8) {
        assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, 0L, version.toShort))
        assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_TIMESTAMP, version.toShort))
        assertEquals((10L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.LATEST_TIMESTAMP, version.toShort))
        assertEquals((9L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.MAX_TIMESTAMP, version.toShort))
        assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, version.toShort))
      }
    }
  }

  private def assertResponseError(error: Errors, brokerId: Int, request: ListOffsetsRequest): Unit = {
    val response = sendRequest(brokerId, request)
    assertEquals(request.topics.size, response.topics.size)
    response.topics.asScala.foreach { topic =>
      topic.partitions.asScala.foreach { partition =>
        assertEquals(error.code, partition.errorCode)
      }
    }
  }

  private def sendRequest(leaderId: Int, request: ListOffsetsRequest): ListOffsetsResponse = {
    connectAndReceive[ListOffsetsResponse](request, destination = brokerSocketServer(leaderId))
  }

  def createTopic(numPartitions: Int, replicationFactor: Int): Map[Int, Int] = {
    super.createTopic(topic, numPartitions, replicationFactor)
  }
}
