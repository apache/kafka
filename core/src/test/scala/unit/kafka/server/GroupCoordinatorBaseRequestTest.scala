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

import kafka.test.ClusterInstance
import kafka.test.junit.RaftClusterInvocationContext.RaftClusterInstance
import kafka.test.junit.ZkClusterInvocationContext.ZkClusterInstance
import kafka.utils.{NotNothing, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.{ConsumerGroupHeartbeatRequestData, JoinGroupRequestData, OffsetCommitRequestData, OffsetCommitResponseData, OffsetFetchResponseData, SyncGroupRequestData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse, JoinGroupRequest, JoinGroupResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, SyncGroupRequest, SyncGroupResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, fail}

import java.util.Comparator
import java.util.stream.Collectors
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

class GroupCoordinatorBaseRequestTest(cluster: ClusterInstance) {
  private def brokers(): Seq[KafkaBroker] = {
    if (cluster.isKRaftTest) {
      cluster.asInstanceOf[RaftClusterInstance].brokers.collect(Collectors.toList[KafkaBroker]).asScala.toSeq
    } else {
      cluster.asInstanceOf[ZkClusterInstance].servers.collect(Collectors.toList[KafkaBroker]).asScala.toSeq
    }
  }

  protected def createOffsetsTopic(): Unit = {
    TestUtils.createOffsetsTopicWithAdmin(
      admin = cluster.createAdminClient(),
      brokers = brokers()
    )
  }

  protected def createTopic(
    topic: String,
    numPartitions: Int
  ): Unit = {
    TestUtils.createTopicWithAdmin(
      admin = cluster.createAdminClient(),
      brokers = brokers(),
      topic = topic,
      numPartitions = numPartitions
    )
  }

  protected def isUnstableApiEnabled: Boolean = {
    cluster.config.serverProperties.getProperty("unstable.api.versions.enable") == "true"
  }

  protected def isNewGroupCoordinatorEnabled: Boolean = {
    cluster.config.serverProperties.getProperty("group.coordinator.new.enable") == "true"
  }

  protected def commitOffset(
    groupId: String,
    memberId: String,
    memberEpoch: Int,
    topic: String,
    partition: Int,
    offset: Long,
    expectedError: Errors,
    version: Short
  ): Unit = {
    val request = new OffsetCommitRequest.Builder(
      new OffsetCommitRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId)
        .setGenerationIdOrMemberEpoch(memberEpoch)
        .setTopics(List(
          new OffsetCommitRequestData.OffsetCommitRequestTopic()
            .setName(topic)
            .setPartitions(List(
              new OffsetCommitRequestData.OffsetCommitRequestPartition()
                .setPartitionIndex(partition)
                .setCommittedOffset(offset)
            ).asJava)
        ).asJava),
      isUnstableApiEnabled
    ).build(version)

    val expectedResponse = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName(topic)
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(partition)
              .setErrorCode(expectedError.code)
          ).asJava)
      ).asJava)

    val response = connectAndReceive[OffsetCommitResponse](request)
    assertEquals(expectedResponse, response.data)
  }

  protected def fetchOffsets(
    groupId: String,
    memberId: String,
    memberEpoch: Int,
    partitions: List[TopicPartition],
    requireStable: Boolean,
    version: Short
  ): OffsetFetchResponseData.OffsetFetchResponseGroup = {
    val request = new OffsetFetchRequest.Builder(
      groupId,
      memberId,
      memberEpoch,
      requireStable,
      if (partitions == null) null else partitions.asJava,
      false
    ).build(version)

    val response = connectAndReceive[OffsetFetchResponse](request)

    // Normalize the response based on the version to present the
    // same format to the caller.
    val groupResponse = if (version >= 8) {
      assertEquals(1, response.data.groups.size)
      assertEquals(groupId, response.data.groups.get(0).groupId)
      response.data.groups.asScala.head
    } else {
      new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId(groupId)
        .setErrorCode(response.data.errorCode)
        .setTopics(response.data.topics.asScala.map { topic =>
          new OffsetFetchResponseData.OffsetFetchResponseTopics()
            .setName(topic.name)
            .setPartitions(topic.partitions.asScala.map { partition =>
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(partition.partitionIndex)
                .setErrorCode(partition.errorCode)
                .setCommittedOffset(partition.committedOffset)
                .setCommittedLeaderEpoch(partition.committedLeaderEpoch)
                .setMetadata(partition.metadata)
            }.asJava)
        }.asJava)
    }

    // Sort topics and partitions within the response as their order is not guaranteed.
    sortTopicPartitions(groupResponse)

    groupResponse
  }

  protected def fetchOffsets(
    groups: Map[String, List[TopicPartition]],
    requireStable: Boolean,
    version: Short
  ): List[OffsetFetchResponseData.OffsetFetchResponseGroup] = {
    if (version < 8) {
      fail(s"OffsetFetch API version $version cannot fetch multiple groups.")
    }

    val request = new OffsetFetchRequest.Builder(
      groups.map { case (k, v) => (k, v.asJava) }.asJava,
      requireStable,
      false
    ).build(version)

    val response = connectAndReceive[OffsetFetchResponse](request)

    // Sort topics and partitions within the response as their order is not guaranteed.
    response.data.groups.asScala.foreach(sortTopicPartitions)

    response.data.groups.asScala.toList
  }

  private def sortTopicPartitions(
    group: OffsetFetchResponseData.OffsetFetchResponseGroup
  ): Unit = {
    group.topics.sort((t1, t2) => t1.name.compareTo(t2.name))
    group.topics.asScala.foreach { topic =>
      topic.partitions.sort(Comparator.comparingInt[OffsetFetchResponseData.OffsetFetchResponsePartitions](_.partitionIndex))
    }
  }

  protected def joinConsumerGroupWithOldProtocol(groupId: String): (String, Int) = {
    val joinGroupRequestData = new JoinGroupRequestData()
      .setGroupId(groupId)
      .setRebalanceTimeoutMs(5 * 50 * 1000)
      .setSessionTimeoutMs(600000)
      .setProtocolType("consumer")
      .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
        List(
          new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("consumer-range")
            .setMetadata(Array.empty)
        ).asJava.iterator
      ))

    // Join the group as a dynamic member.
    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var joinGroupRequest = new JoinGroupRequest.Builder(joinGroupRequestData).build()
    var joinGroupResponse: JoinGroupResponse = null
    TestUtils.waitUntilTrue(() => {
      joinGroupResponse = connectAndReceive[JoinGroupResponse](joinGroupRequest)
      joinGroupResponse.data.errorCode == Errors.MEMBER_ID_REQUIRED.code
    }, msg = s"Could not join the group successfully. Last response $joinGroupResponse.")

    // Rejoin the group with the member id.
    joinGroupRequestData.setMemberId(joinGroupResponse.data.memberId)
    joinGroupRequest = new JoinGroupRequest.Builder(joinGroupRequestData).build()
    joinGroupResponse = connectAndReceive[JoinGroupResponse](joinGroupRequest)
    assertEquals(Errors.NONE.code, joinGroupResponse.data.errorCode)

    val syncGroupRequestData = new SyncGroupRequestData()
      .setGroupId(groupId)
      .setMemberId(joinGroupResponse.data.memberId)
      .setGenerationId(joinGroupResponse.data.generationId)
      .setProtocolType("consumer")
      .setProtocolName("consumer-range")
      .setAssignments(List.empty.asJava)

    // Send the sync group request to complete the rebalance.
    val syncGroupRequest = new SyncGroupRequest.Builder(syncGroupRequestData).build()
    val syncGroupResponse = connectAndReceive[SyncGroupResponse](syncGroupRequest)
    assertEquals(Errors.NONE.code, syncGroupResponse.data.errorCode)

    (joinGroupResponse.data.memberId, joinGroupResponse.data.generationId)
  }

  protected def joinConsumerGroupWithNewProtocol(groupId: String): (String, Int) = {
    // Heartbeat request to join the group.
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava),
      true
    ).build()

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var consumerGroupHeartbeatResponse: ConsumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $consumerGroupHeartbeatResponse.")

    (consumerGroupHeartbeatResponse.data.memberId, consumerGroupHeartbeatResponse.data.memberEpoch)
  }

  protected def joinConsumerGroup(groupId: String, useNewProtocol: Boolean): (String, Int) = {
    if (useNewProtocol) {
      // Note that we heartbeat only once to join the group and assume
      // that the test will complete within the session timeout.
      joinConsumerGroupWithNewProtocol(groupId)
    } else {
      // Note that we don't heartbeat and assume  that the test will
      // complete within the session timeout.
      joinConsumerGroupWithOldProtocol(groupId)
    }
  }

  protected def connectAndReceive[T <: AbstractResponse](
    request: AbstractRequest
  )(implicit classTag: ClassTag[T], nn: NotNothing[T]): T = {
    IntegrationTestUtils.connectAndReceive[T](
      request,
      cluster.anyBrokerSocketServer(),
      cluster.clientListener()
    )
  }
}
