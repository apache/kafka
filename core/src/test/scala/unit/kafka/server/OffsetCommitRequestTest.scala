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
import kafka.test.annotation.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.junit.RaftClusterInvocationContext.RaftClusterInstance
import kafka.test.junit.ZkClusterInvocationContext.ZkClusterInstance
import kafka.utils.{NotNothing, TestUtils}
import org.apache.kafka.common.message.{ConsumerGroupHeartbeatRequestData, JoinGroupRequestData, OffsetCommitRequestData, OffsetCommitResponseData, SyncGroupRequestData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse, JoinGroupRequest, JoinGroupResponse, OffsetCommitRequest, OffsetCommitResponse, SyncGroupRequest, SyncGroupResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, fail}
import org.junit.jupiter.api.{Tag, Timeout}
import org.junit.jupiter.api.extension.ExtendWith

import java.util.stream.Collectors
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.KRAFT, brokers = 1)
@Tag("integration")
class OffsetCommitRequestTest(cluster: ClusterInstance) {

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "true"),
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "group.consumer.max.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "group.consumer.session.timeout.ms", value = "600000"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testOffsetCommitWithNewConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testOffsetCommit(true)
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "true"),
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testOffsetCommitWithOldConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testOffsetCommit(false)
  }

  @ClusterTest(clusterType = Type.ALL, serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "false"),
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "false"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testOffsetCommitWithOldConsumerGroupProtocolAndOldGroupCoordinator(): Unit = {
    testOffsetCommit(false)
  }

  private def testOffsetCommit(useNewProtocol: Boolean): Unit = {
    if (useNewProtocol && !isNewGroupCoordinatorEnabled) {
      fail("Cannot use the new protocol with the old group coordinator.")
    }

    val admin = cluster.createAdminClient()

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    TestUtils.createOffsetsTopicWithAdmin(
      admin = admin,
      brokers = if (cluster.isKRaftTest) {
        cluster.asInstanceOf[RaftClusterInstance].brokers.collect(Collectors.toList[KafkaBroker]).asScala
      } else {
        cluster.asInstanceOf[ZkClusterInstance].servers.collect(Collectors.toList[KafkaBroker]).asScala
      }
    )

    // Create the topic.
    TestUtils.createTopicWithAdminRaw(
      admin = admin,
      topic = "foo",
      numPartitions = 3
    )

    // Join the consumer group.
    val (memberId, memberEpoch) = if (useNewProtocol) {
      // Note that we heartbeat only once to join the group and assume
      // that the test will complete within the session timeout.
      joinConsumerGroupWithNewProtocol("grp")
    } else {
      // Note that we don't heartbeat and assume  that the test will
      // complete within the session timeout.
      joinConsumerGroupWithOldProtocol("grp")
    }

    // Start from version 1 because version 0 goes to ZK.
    for (version <- 1 to ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)) {
      // Commit offset.
      commitOffset(
        groupId = "grp",
        memberId = memberId,
        memberEpoch = memberEpoch,
        topic = "foo",
        partition = 0,
        offset = 100L,
        expectedError = Errors.NONE,
        version = version.toShort
      )

      // Commit offset with unknown group should fail.
      commitOffset(
        groupId = "unknown",
        memberId = memberId,
        memberEpoch = memberEpoch,
        topic = "foo",
        partition = 0,
        offset = 100L,
        expectedError =
          if (isNewGroupCoordinatorEnabled && version >= 9) Errors.GROUP_ID_NOT_FOUND
          else Errors.ILLEGAL_GENERATION,
        version = version.toShort
      )

      // Commit offset with unknown member id should fail.
      commitOffset(
        groupId = "grp",
        memberId = "",
        memberEpoch = memberEpoch,
        topic = "foo",
        partition = 0,
        offset = 100L,
        expectedError = Errors.UNKNOWN_MEMBER_ID,
        version = version.toShort
      )

      // Commit offset with stale member epoch should fail.
      commitOffset(
        groupId = "grp",
        memberId = memberId,
        memberEpoch = memberEpoch + 1,
        topic = "foo",
        partition = 0,
        offset = 100L,
        expectedError =
          if (useNewProtocol && version >= 9) Errors.STALE_MEMBER_EPOCH
          else if (useNewProtocol) Errors.UNSUPPORTED_VERSION
          else Errors.ILLEGAL_GENERATION,
        version = version.toShort
      )
    }
  }

  private def isUnstableApiEnabled: Boolean = {
    cluster.config.serverProperties.getProperty("unstable.api.versions.enable") == "true"
  }

  private def isNewGroupCoordinatorEnabled: Boolean = {
    cluster.config.serverProperties.getProperty("group.coordinator.new.enable") == "true"
  }

  private def commitOffset(
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

  private def joinConsumerGroupWithOldProtocol(groupId: String): (String, Int) = {
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

  private def joinConsumerGroupWithNewProtocol(groupId: String): (String, Int) = {
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

  private def connectAndReceive[T <: AbstractResponse](
    request: AbstractRequest
  )(implicit classTag: ClassTag[T], nn: NotNothing[T]): T = {
    IntegrationTestUtils.connectAndReceive[T](
      request,
      cluster.anyBrokerSocketServer(),
      cluster.clientListener()
    )
  }
}
