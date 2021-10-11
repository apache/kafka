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
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.Optional
import scala.jdk.CollectionConverters._

class ListOffsetsRequestTest extends BaseRequestTest {

  val topic = "topic"
  val partition = new TopicPartition(topic, 0)

  @Test
  def testListOffsetsErrorCodes(): Unit = {
    val targetTimes = List(new ListOffsetsTopic()
      .setName(topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(partition.partition)
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)
        .setCurrentLeaderEpoch(0)).asJava)).asJava

    val consumerRequest = ListOffsetsRequest.Builder
      .forConsumer(false, IsolationLevel.READ_UNCOMMITTED, false)
      .setTargetTimes(targetTimes)
      .build()

    val replicaRequest = ListOffsetsRequest.Builder
      .forReplica(ApiKeys.LIST_OFFSETS.latestVersion, servers.head.config.brokerId)
      .setTargetTimes(targetTimes)
      .build()

    val debugReplicaRequest = ListOffsetsRequest.Builder
      .forReplica(ApiKeys.LIST_OFFSETS.latestVersion, ListOffsetsRequest.DEBUGGING_REPLICA_ID)
      .setTargetTimes(targetTimes)
      .build()

    // Unknown topic
    val randomBrokerId = servers.head.config.brokerId
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, consumerRequest)
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, replicaRequest)
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, debugReplicaRequest)

    val partitionToLeader = createTopic(numPartitions = 1, replicationFactor = 2)
    val replicas = zkClient.getReplicasForPartition(partition).toSet
    val leader = partitionToLeader(partition.partition)
    val follower = replicas.find(_ != leader).get
    val nonReplica = servers.map(_.config.brokerId).find(!replicas.contains(_)).get

    // Follower
    assertResponseError(Errors.NOT_LEADER_OR_FOLLOWER, follower, consumerRequest)
    assertResponseError(Errors.NOT_LEADER_OR_FOLLOWER, follower, replicaRequest)
    assertResponseError(Errors.NONE, follower, debugReplicaRequest)

    // Non-replica
    assertResponseError(Errors.NOT_LEADER_OR_FOLLOWER, nonReplica, consumerRequest)
    assertResponseError(Errors.NOT_LEADER_OR_FOLLOWER, nonReplica, replicaRequest)
    assertResponseError(Errors.NOT_LEADER_OR_FOLLOWER, nonReplica, debugReplicaRequest)
  }

  @Test
  def testListOffsetsMaxTimeStampOldestVersion(): Unit = {
    val consumerRequestBuilder = ListOffsetsRequest.Builder
      .forConsumer(false, IsolationLevel.READ_UNCOMMITTED, false)

    val maxTimestampRequestBuilder = ListOffsetsRequest.Builder
      .forConsumer(false, IsolationLevel.READ_UNCOMMITTED, true)

    assertEquals(0.toShort, consumerRequestBuilder.oldestAllowedVersion())
    assertEquals(7.toShort, maxTimestampRequestBuilder.oldestAllowedVersion())
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
      .forConsumer(false, IsolationLevel.READ_UNCOMMITTED, false)
      .setTargetTimes(targetTimes)
      .build()
    assertResponseError(error, brokerId, request)
  }

  @Test
  def testCurrentEpochValidation(): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val partitionToLeader = createTopic(numPartitions = 1, replicationFactor = 3)
    val firstLeaderId = partitionToLeader(topicPartition.partition)

    // We need a leader change in order to check epoch fencing since the first epoch is 0 and
    // -1 is treated as having no epoch at all
    killBroker(firstLeaderId)

    // Check leader error codes
    val secondLeaderId = TestUtils.awaitLeaderChange(servers, topicPartition, firstLeaderId)
    val secondLeaderEpoch = TestUtils.findLeaderEpoch(secondLeaderId, topicPartition, servers)
    assertResponseErrorForEpoch(Errors.NONE, secondLeaderId, Optional.empty())
    assertResponseErrorForEpoch(Errors.NONE, secondLeaderId, Optional.of(secondLeaderEpoch))
    assertResponseErrorForEpoch(Errors.FENCED_LEADER_EPOCH, secondLeaderId, Optional.of(secondLeaderEpoch - 1))
    assertResponseErrorForEpoch(Errors.UNKNOWN_LEADER_EPOCH, secondLeaderId, Optional.of(secondLeaderEpoch + 1))

    // Check follower error codes
    val followerId = TestUtils.findFollowerId(topicPartition, servers)
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
      .forConsumer(false, IsolationLevel.READ_UNCOMMITTED, false)
      .setTargetTimes(targetTimes)

    val request = if (version == -1) builder.build() else builder.build(version)

    sendRequest(serverId, request).topics.asScala.find(_.name == topic).get
      .partitions.asScala.find(_.partitionIndex == partition.partition).get
  }

  // -1 indicate "latest"
  private[this] def fetchOffsetAndEpoch(serverId: Int,
                                        timestamp: Long,
                                        version: Short): (Long, Int) = {
    val partitionData = sendRequest(serverId, timestamp, version)

    if (version == 0) {
      if (partitionData.oldStyleOffsets().isEmpty)
        (-1, partitionData.leaderEpoch)
      else
        (partitionData.oldStyleOffsets().asScala.head, partitionData.leaderEpoch)
    } else
      (partitionData.offset, partitionData.leaderEpoch)
  }

  @Test
  def testResponseIncludesLeaderEpoch(): Unit = {
    val partitionToLeader = createTopic(numPartitions = 1, replicationFactor = 3)
    val firstLeaderId = partitionToLeader(partition.partition)

    TestUtils.generateAndProduceMessages(servers, topic, 9)
    TestUtils.produceMessage(servers, topic, "test-10", System.currentTimeMillis() + 10L)

    assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, 0L, -1))
    assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_TIMESTAMP, -1))
    assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, version = -1))
    assertEquals((10L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.LATEST_TIMESTAMP, -1))
    assertEquals((9L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.MAX_TIMESTAMP, -1))

    // Kill the first leader so that we can verify the epoch change when fetching the latest offset
    killBroker(firstLeaderId)
    val secondLeaderId = TestUtils.awaitLeaderChange(servers, partition, firstLeaderId)
    // make sure high watermark of new leader has caught up
    TestUtils.waitUntilTrue(() => sendRequest(secondLeaderId, 0L, -1).errorCode() != Errors.OFFSET_NOT_AVAILABLE.code(),
      "the second leader does not sync to follower")
    val secondLeaderEpoch = TestUtils.findLeaderEpoch(secondLeaderId, partition, servers)

    // No changes to written data
    assertEquals((0L, 0), fetchOffsetAndEpoch(secondLeaderId, 0L, -1))
    assertEquals((0L, 0), fetchOffsetAndEpoch(secondLeaderId, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, -1))

    assertEquals((0L, 0), fetchOffsetAndEpoch(secondLeaderId, 0L, -1))
    assertEquals((0L, 0), fetchOffsetAndEpoch(secondLeaderId, ListOffsetsRequest.EARLIEST_TIMESTAMP, -1))

    assertEquals((0L, 0), fetchOffsetAndEpoch(secondLeaderId, 0L, -1))
    assertEquals((0L, 0), fetchOffsetAndEpoch(secondLeaderId, ListOffsetsRequest.EARLIEST_TIMESTAMP, -1))

    // The latest offset reflects the updated epoch
    assertEquals((10L, secondLeaderEpoch), fetchOffsetAndEpoch(secondLeaderId, ListOffsetsRequest.LATEST_TIMESTAMP, -1))
    assertEquals((9L, secondLeaderEpoch), fetchOffsetAndEpoch(secondLeaderId, ListOffsetsRequest.MAX_TIMESTAMP, -1))
  }

  @Test
  def testResponseDefaultOffsetAndLeaderEpochForAllVersions(): Unit = {
    val partitionToLeader = createTopic(numPartitions = 1, replicationFactor = 3)
    val firstLeaderId = partitionToLeader(partition.partition)

    TestUtils.generateAndProduceMessages(servers, topic, 9)
    TestUtils.produceMessage(servers, topic, "test-10", System.currentTimeMillis() + 10L)

    for (version <- ApiKeys.LIST_OFFSETS.oldestVersion to ApiKeys.LIST_OFFSETS.latestVersion) {
      if (version == 0) {
        assertEquals((-1L, -1), fetchOffsetAndEpoch(firstLeaderId, 0L, version.toShort))
        assertEquals((0L, -1), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_TIMESTAMP, version.toShort))
        assertEquals((0L, -1), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, version.toShort))
        assertEquals((10L, -1), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.LATEST_TIMESTAMP, version.toShort))
      } else if (version >= 1 && version <= 3) {
        assertEquals((0L, -1), fetchOffsetAndEpoch(firstLeaderId, 0L, version.toShort))
        assertEquals((0L, -1), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_TIMESTAMP, version.toShort))
        assertEquals((0L, -1), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, version.toShort))
        assertEquals((10L, -1), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.LATEST_TIMESTAMP, version.toShort))
      } else if (version >= 4  && version <= 6) {
        assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, 0L, version.toShort))
        assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_TIMESTAMP, version.toShort))
        assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, version.toShort))
        assertEquals((10L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.LATEST_TIMESTAMP, version.toShort))
      } else if (version >= 7) {
        assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, 0L, version.toShort))
        assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_TIMESTAMP, version.toShort))
        assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, version.toShort))
        assertEquals((10L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.LATEST_TIMESTAMP, version.toShort))
        assertEquals((9L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetsRequest.MAX_TIMESTAMP, version.toShort))
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
    TestUtils.createTopic(zkClient, topic, numPartitions, replicationFactor, servers)
  }
}
