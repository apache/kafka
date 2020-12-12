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

import java.util.Optional

import kafka.utils.TestUtils
import org.apache.kafka.common.message.ListOffsetRequestData.{ListOffsetPartition, ListOffsetTopic}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ListOffsetRequest, ListOffsetResponse}
import org.apache.kafka.common.{IsolationLevel, TopicPartition}
import org.junit.Assert._
import org.junit.Test

import scala.jdk.CollectionConverters._

class ListOffsetsRequestTest extends BaseRequestTest {

  val topic = "topic"
  val partition = new TopicPartition(topic, 0)

  @Test
  def testListOffsetsErrorCodes(): Unit = {
    val targetTimes = List(new ListOffsetTopic()
      .setName(topic)
      .setPartitions(List(new ListOffsetPartition()
        .setPartitionIndex(partition.partition)
        .setTimestamp(ListOffsetRequest.EARLIEST_TIMESTAMP)
        .setCurrentLeaderEpoch(0)).asJava)).asJava

    val consumerRequest = ListOffsetRequest.Builder
      .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes)
      .build()

    val replicaRequest = ListOffsetRequest.Builder
      .forReplica(ApiKeys.LIST_OFFSETS.latestVersion, servers.head.config.brokerId)
      .setTargetTimes(targetTimes)
      .build()

    val debugReplicaRequest = ListOffsetRequest.Builder
      .forReplica(ApiKeys.LIST_OFFSETS.latestVersion, ListOffsetRequest.DEBUGGING_REPLICA_ID)
      .setTargetTimes(targetTimes)
      .build()

    // Unknown topic
    val randomBrokerId = servers.head.config.brokerId
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, consumerRequest)
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, replicaRequest)
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, debugReplicaRequest)

    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 2, servers)
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

  def assertResponseErrorForEpoch(error: Errors, brokerId: Int, currentLeaderEpoch: Optional[Integer]): Unit = {
    val listOffsetPartition = new ListOffsetPartition()
      .setPartitionIndex(partition.partition)
      .setTimestamp(ListOffsetRequest.EARLIEST_TIMESTAMP)
    if (currentLeaderEpoch.isPresent)
      listOffsetPartition.setCurrentLeaderEpoch(currentLeaderEpoch.get)
    val targetTimes = List(new ListOffsetTopic()
      .setName(topic)
      .setPartitions(List(listOffsetPartition).asJava)).asJava
    val request = ListOffsetRequest.Builder
      .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes)
      .build()
    assertResponseError(error, brokerId, request)
  }

  @Test
  def testCurrentEpochValidation(): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 3, servers)
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

  // -1 indicate "latest"
  def fetchOffsetAndEpoch(serverId: Int,
                          timestamp: Long,
                          version: Short): (Long, Int) = {
    val targetTimes = List(new ListOffsetTopic()
      .setName(topic)
      .setPartitions(List(new ListOffsetPartition()
        .setPartitionIndex(partition.partition)
        .setTimestamp(timestamp)).asJava)).asJava

    val builder = ListOffsetRequest.Builder
      .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes)

    val request = if (version == -1) builder.build() else builder.build(version)

    val response = sendRequest(serverId, request)
    val partitionData = response.topics.asScala.find(_.name == topic).get
      .partitions.asScala.find(_.partitionIndex == partition.partition).get

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
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 3, servers)
    val firstLeaderId = partitionToLeader(partition.partition)

    TestUtils.generateAndProduceMessages(servers, topic, 10)

    assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, 0L, -1))
    assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetRequest.EARLIEST_TIMESTAMP, -1))
    assertEquals((10L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetRequest.LATEST_TIMESTAMP, -1))

    // Kill the first leader so that we can verify the epoch change when fetching the latest offset
    killBroker(firstLeaderId)
    val secondLeaderId = TestUtils.awaitLeaderChange(servers, partition, firstLeaderId)
    val secondLeaderEpoch = TestUtils.findLeaderEpoch(secondLeaderId, partition, servers)

    // No changes to written data
    assertEquals((0L, 0), fetchOffsetAndEpoch(secondLeaderId, 0L, -1))
    assertEquals((0L, 0), fetchOffsetAndEpoch(secondLeaderId, ListOffsetRequest.EARLIEST_TIMESTAMP, -1))

    assertEquals((0L, 0), fetchOffsetAndEpoch(secondLeaderId, 0L, -1))
    assertEquals((0L, 0), fetchOffsetAndEpoch(secondLeaderId, ListOffsetRequest.EARLIEST_TIMESTAMP, -1))

    // The latest offset reflects the updated epoch
    assertEquals((10L, secondLeaderEpoch), fetchOffsetAndEpoch(secondLeaderId, ListOffsetRequest.LATEST_TIMESTAMP, -1))
  }

  @Test
  def testResponseDefaultOffsetAndLeaderEpochForAllVersions(): Unit = {
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 3, servers)
    val firstLeaderId = partitionToLeader(partition.partition)

    TestUtils.generateAndProduceMessages(servers, topic, 10)

    for (version <- ApiKeys.LIST_OFFSETS.oldestVersion to ApiKeys.LIST_OFFSETS.latestVersion) {
      if (version == 0) {
        assertEquals((-1L, -1), fetchOffsetAndEpoch(firstLeaderId, 0L, version.toShort))
        assertEquals((0L, -1), fetchOffsetAndEpoch(firstLeaderId, ListOffsetRequest.EARLIEST_TIMESTAMP, version.toShort))
        assertEquals((10L, -1), fetchOffsetAndEpoch(firstLeaderId, ListOffsetRequest.LATEST_TIMESTAMP, version.toShort))
      } else if (version >= 1 && version <= 3) {
        assertEquals((0L, -1), fetchOffsetAndEpoch(firstLeaderId, 0L, version.toShort))
        assertEquals((0L, -1), fetchOffsetAndEpoch(firstLeaderId, ListOffsetRequest.EARLIEST_TIMESTAMP, version.toShort))
        assertEquals((10L, -1), fetchOffsetAndEpoch(firstLeaderId, ListOffsetRequest.LATEST_TIMESTAMP, version.toShort))
      } else if (version >= 4) {
        assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, 0L, version.toShort))
        assertEquals((0L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetRequest.EARLIEST_TIMESTAMP, version.toShort))
        assertEquals((10L, 0), fetchOffsetAndEpoch(firstLeaderId, ListOffsetRequest.LATEST_TIMESTAMP, version.toShort))
      }
    }
  }

  private def assertResponseError(error: Errors, brokerId: Int, request: ListOffsetRequest): Unit = {
    val response = sendRequest(brokerId, request)
    assertEquals(request.topics.size, response.topics.size)
    response.topics.asScala.foreach { topic =>
      topic.partitions.asScala.foreach { partition =>
        assertEquals(error.code, partition.errorCode)
      }
    }
  }

  private def sendRequest(leaderId: Int, request: ListOffsetRequest): ListOffsetResponse = {
    connectAndReceive[ListOffsetResponse](request, destination = brokerSocketServer(leaderId))
  }
}
