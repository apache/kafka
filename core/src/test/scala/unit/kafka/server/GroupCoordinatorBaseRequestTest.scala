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
import kafka.utils.{NotNothing, TestUtils}
import org.apache.kafka.common.message.{ConsumerGroupHeartbeatRequestData, JoinGroupRequestData, OffsetCommitRequestData, OffsetCommitResponseData, SyncGroupRequestData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse, JoinGroupRequest, JoinGroupResponse, OffsetCommitRequest, OffsetCommitResponse, SyncGroupRequest, SyncGroupResponse}
import org.junit.jupiter.api.Assertions.assertEquals

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

class GroupCoordinatorBaseRequestTest(cluster: ClusterInstance) {
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
