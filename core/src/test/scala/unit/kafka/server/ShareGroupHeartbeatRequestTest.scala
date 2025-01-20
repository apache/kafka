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

import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterInstance, ClusterTest, ClusterTestDefaults, ClusterTestExtensions, Type}
import kafka.utils.TestUtils
import kafka.utils.TestUtils.waitForAllPartitionsMetadata
import org.apache.kafka.clients.admin.{Admin, NewPartitions}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.message.{ShareGroupHeartbeatRequestData, ShareGroupHeartbeatResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertNotNull, assertNull, assertTrue}
import org.junit.jupiter.api.{Tag, Timeout}
import org.junit.jupiter.api.extension.ExtendWith

import scala.jdk.CollectionConverters._

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(types = Array(Type.KRAFT), brokers = 1, serverProperties = Array(
  new ClusterConfigProperty(key = "group.share.persister.class.name", value = "")
))
@Tag("integration")
class ShareGroupHeartbeatRequestTest(cluster: ClusterInstance) {

  @ClusterTest(
    types = Array(Type.KRAFT)
  )
  def testShareGroupHeartbeatIsInAccessibleWhenConfigsDisabled(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData(), true
    ).build()

    val shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
    val expectedResponse = new ShareGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    assertEquals(expectedResponse, shareGroupHeartbeatResponse.data)
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
      new ClusterConfigProperty(key = "group.share.enable", value = "true"),
      new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
      new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
    ))
  def testShareGroupHeartbeatIsAccessibleWhenShareGroupIsEnabled(): Unit = {
    val admin = cluster.admin()

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )

      // Heartbeat request to join the group. Note that the member subscribes
      // to an nonexistent topic.
      var shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(Uuid.randomUuid.toString)
          .setMemberEpoch(0)
          .setSubscribedTopicNames(List("foo").asJava),
        true
      ).build()

      // Send the request until receiving a successful response. There is a delay
      // here because the group coordinator is loaded in the background.
      var shareGroupHeartbeatResponse: ShareGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
      }, msg = s"Could not join the group successfully. Last response $shareGroupHeartbeatResponse.")

      // Verify the response.
      assertNotNull(shareGroupHeartbeatResponse.data.memberId)
      assertEquals(1, shareGroupHeartbeatResponse.data.memberEpoch)
      assertEquals(new ShareGroupHeartbeatResponseData.Assignment(), shareGroupHeartbeatResponse.data.assignment)

      // Create the topic.
      val topicId = TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 3
      )

      // Prepare the next heartbeat.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(shareGroupHeartbeatResponse.data.memberId)
          .setMemberEpoch(shareGroupHeartbeatResponse.data.memberEpoch),
        true
      ).build()

      // This is the expected assignment. here
      val expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
        .setTopicPartitions(List(new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(topicId)
          .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)

      // Heartbeats until the partitions are assigned.
      shareGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          shareGroupHeartbeatResponse.data.assignment == expectedAssignment
      }, msg = s"Could not get partitions assigned. Last response $shareGroupHeartbeatResponse.")

      // Verify the response.
      assertEquals(2, shareGroupHeartbeatResponse.data.memberEpoch)
      assertEquals(expectedAssignment, shareGroupHeartbeatResponse.data.assignment)

      // Leave the group.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(shareGroupHeartbeatResponse.data.memberId)
          .setMemberEpoch(-1),
        true
      ).build()

      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)

      // Verify the response.
      assertEquals(-1, shareGroupHeartbeatResponse.data.memberEpoch)
    } finally {
      admin.close()
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
      new ClusterConfigProperty(key = "group.share.enable", value = "true"),
      new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
      new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
    ))
  def testShareGroupHeartbeatWithMultipleMembers(): Unit = {
    val admin = cluster.admin()

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )

      // Heartbeat request to join the group. Note that the member subscribes
      // to an nonexistent topic.
      var shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(Uuid.randomUuid.toString)
          .setMemberEpoch(0)
          .setSubscribedTopicNames(List("foo").asJava),
        true
      ).build()

      // Send the request until receiving a successful response. There is a delay
      // here because the group coordinator is loaded in the background.
      var shareGroupHeartbeatResponse: ShareGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
      }, msg = s"Could not join the group successfully. Last response $shareGroupHeartbeatResponse.")

      // Verify the response for member 1.
      val memberId1 = shareGroupHeartbeatResponse.data.memberId
      assertNotNull(memberId1)
      assertEquals(1, shareGroupHeartbeatResponse.data.memberEpoch)
      assertEquals(new ShareGroupHeartbeatResponseData.Assignment(), shareGroupHeartbeatResponse.data.assignment)

      // The second member request to join the group.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(Uuid.randomUuid.toString)
          .setMemberEpoch(0)
          .setSubscribedTopicNames(List("foo").asJava),
        true
      ).build()

      // Send the second member request until receiving a successful response.
      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
      }, msg = s"Could not join the group successfully. Last response $shareGroupHeartbeatResponse.")

      // Verify the response for member 2.
      val memberId2 = shareGroupHeartbeatResponse.data.memberId
      assertNotNull(memberId2)
      assertEquals(2, shareGroupHeartbeatResponse.data.memberEpoch)
      assertEquals(new ShareGroupHeartbeatResponseData.Assignment(), shareGroupHeartbeatResponse.data.assignment)
      // Verify the member id is different.
      assertNotEquals(memberId1, memberId2)

      // Create the topic.
      val topicId = TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 3
      )

      // This is the expected assignment.
      val expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
        .setTopicPartitions(List(new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(topicId)
          .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)

      // Prepare the next heartbeat for member 1.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(memberId1)
          .setMemberEpoch(1),
        true
      ).build()

      // Heartbeats until the partitions are assigned for member 1.
      shareGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          shareGroupHeartbeatResponse.data.assignment == expectedAssignment
      }, msg = s"Could not get partitions assigned. Last response $shareGroupHeartbeatResponse.")

      // Verify the response.
      assertEquals(3, shareGroupHeartbeatResponse.data.memberEpoch)

      // Prepare the next heartbeat for member 2.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(memberId2)
          .setMemberEpoch(2),
        true
      ).build()

      // Heartbeats until the partitions are assigned for member 2.
      shareGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          shareGroupHeartbeatResponse.data.assignment == expectedAssignment
      }, msg = s"Could not get partitions assigned. Last response $shareGroupHeartbeatResponse.")

      // Verify the response.
      assertEquals(3, shareGroupHeartbeatResponse.data.memberEpoch)

      // Verify the assignments are not changed for member 1.
      // Prepare another heartbeat for member 1 with latest received epoch 3 for member 1.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(memberId1)
          .setMemberEpoch(3),
        true
      ).build()

      // Heartbeats until the response for no change of assignment occurs for member 1 with same epoch.
      shareGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          shareGroupHeartbeatResponse.data.assignment == null
      }, msg = s"Could not get partitions assigned. Last response $shareGroupHeartbeatResponse.")

      // Verify the response.
      assertEquals(3, shareGroupHeartbeatResponse.data.memberEpoch)
    } finally {
      admin.close()
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
      new ClusterConfigProperty(key = "group.share.enable", value = "true"),
      new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
      new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
    ))
  def testMemberLeavingAndRejoining(): Unit = {
    val admin = cluster.admin()

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )

      // Heartbeat request to join the group. Note that the member subscribes
      // to an nonexistent topic.
      var shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(Uuid.randomUuid.toString)
          .setMemberEpoch(0)
          .setSubscribedTopicNames(List("foo").asJava),
        true
      ).build()

      // Send the request until receiving a successful response. There is a delay
      // here because the group coordinator is loaded in the background.
      var shareGroupHeartbeatResponse: ShareGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
      }, msg = s"Could not join the group successfully. Last response $shareGroupHeartbeatResponse.")

      // Verify the response for member.
      val memberId = shareGroupHeartbeatResponse.data.memberId
      assertNotNull(memberId)
      assertEquals(1, shareGroupHeartbeatResponse.data.memberEpoch)
      assertEquals(new ShareGroupHeartbeatResponseData.Assignment(), shareGroupHeartbeatResponse.data.assignment)

      // Create the topic.
      val topicId = TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 2
      )

      // This is the expected assignment.
      val expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
        .setTopicPartitions(List(new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(topicId)
          .setPartitions(List[Integer](0, 1).asJava)).asJava)

      // Prepare the next heartbeat for member.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(memberId)
          .setMemberEpoch(1),
        true
      ).build()

      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          shareGroupHeartbeatResponse.data.assignment == expectedAssignment
      }, msg = s"Could not get partitions assigned. Last response $shareGroupHeartbeatResponse.")

      // Verify the response.
      assertEquals(2, shareGroupHeartbeatResponse.data.memberEpoch)

      // Member leaves the group.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberEpoch(-1)
          .setMemberId(memberId),
        true
      ).build()

      // Send the member request until receiving a successful response.
      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
      }, msg = s"Could not leave the group successfully. Last response $shareGroupHeartbeatResponse.")

      // Verify the response for member.
      assertEquals(-1, shareGroupHeartbeatResponse.data.memberEpoch)

      // Member sends request to rejoin the group.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberEpoch(0)
          .setMemberId(memberId)
          .setSubscribedTopicNames(List("foo").asJava),
        true
      ).build()

      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)

      // Verify the response for member 1.
      assertEquals(4, shareGroupHeartbeatResponse.data.memberEpoch)
      assertEquals(memberId, shareGroupHeartbeatResponse.data.memberId)
      // Partition assignment remains intact on rejoining.
      assertEquals(expectedAssignment, shareGroupHeartbeatResponse.data.assignment)
    } finally {
      admin.close()
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
      new ClusterConfigProperty(key = "group.share.enable", value = "true"),
      new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
      new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
    ))
  def testPartitionAssignmentWithChangingTopics(): Unit = {
    val admin = cluster.admin()
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )
      // Heartbeat request to join the group. Note that the member subscribes
      // to a nonexistent topic.
      var shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(Uuid.randomUuid.toString)
          .setMemberEpoch(0)
          .setSubscribedTopicNames(List("foo", "bar", "baz").asJava),
        true
      ).build()
      // Send the request until receiving a successful response. There is a delay
      // here because the group coordinator is loaded in the background.
      var shareGroupHeartbeatResponse: ShareGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
      }, msg = s"Could not join the group successfully. Last response $shareGroupHeartbeatResponse.")
      // Verify the response for member.
      val memberId = shareGroupHeartbeatResponse.data.memberId
      assertNotNull(memberId)
      assertEquals(1, shareGroupHeartbeatResponse.data.memberEpoch)
      assertEquals(new ShareGroupHeartbeatResponseData.Assignment(), shareGroupHeartbeatResponse.data.assignment)
      // Create the topic foo.
      val fooTopicId = TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 2
      )
      // Create the topic bar.
      val barTopicId = TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "bar",
        numPartitions = 3
      )

      var expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
        .setTopicPartitions(List(
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(fooTopicId)
            .setPartitions(List[Integer](0, 1).asJava),
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(barTopicId)
            .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)
      // Prepare the next heartbeat for member.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(memberId)
          .setMemberEpoch(1),
        true
      ).build()

      cluster.waitForTopic("foo", 2)
      cluster.waitForTopic("bar", 3)

      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          shareGroupHeartbeatResponse.data.assignment != null &&
          expectedAssignment.topicPartitions.containsAll(shareGroupHeartbeatResponse.data.assignment.topicPartitions) &&
          shareGroupHeartbeatResponse.data.assignment.topicPartitions.containsAll(expectedAssignment.topicPartitions)
      }, msg = s"Could not get partitions for topic foo and bar assigned. Last response $shareGroupHeartbeatResponse.")
      // Verify the response.
      assertEquals(2, shareGroupHeartbeatResponse.data.memberEpoch)
      // Create the topic baz.
      val bazTopicId = TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "baz",
        numPartitions = 4
      )

      expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
        .setTopicPartitions(List(
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(fooTopicId)
            .setPartitions(List[Integer](0, 1).asJava),
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(barTopicId)
            .setPartitions(List[Integer](0, 1, 2).asJava),
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(bazTopicId)
            .setPartitions(List[Integer](0, 1, 2, 3).asJava)).asJava)
      // Prepare the next heartbeat for member.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(memberId)
          .setMemberEpoch(2),
        true
      ).build()

      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          shareGroupHeartbeatResponse.data.assignment != null &&
          expectedAssignment.topicPartitions.containsAll(shareGroupHeartbeatResponse.data.assignment.topicPartitions) &&
          shareGroupHeartbeatResponse.data.assignment.topicPartitions.containsAll(expectedAssignment.topicPartitions)
      }, msg = s"Could not get partitions for topic baz assigned. Last response $shareGroupHeartbeatResponse.")
      // Verify the response.
      assertEquals(3, shareGroupHeartbeatResponse.data.memberEpoch)
      // Increasing the partitions of topic bar which is already being consumed in the share group.
      increasePartitions(admin, "bar", 6, Seq.empty)

      expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
        .setTopicPartitions(List(
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(fooTopicId)
            .setPartitions(List[Integer](0, 1).asJava),
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(barTopicId)
            .setPartitions(List[Integer](0, 1, 2, 3, 4, 5).asJava),
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(bazTopicId)
            .setPartitions(List[Integer](0, 1, 2, 3).asJava)).asJava)
      // Prepare the next heartbeat for member.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(memberId)
          .setMemberEpoch(3),
        true
      ).build()

      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          shareGroupHeartbeatResponse.data.assignment != null &&
          expectedAssignment.topicPartitions.containsAll(shareGroupHeartbeatResponse.data.assignment.topicPartitions) &&
          shareGroupHeartbeatResponse.data.assignment.topicPartitions.containsAll(expectedAssignment.topicPartitions)
      }, msg = s"Could not update partitions assignment for topic bar. Last response $shareGroupHeartbeatResponse.")
      // Verify the response.
      assertEquals(4, shareGroupHeartbeatResponse.data.memberEpoch)
      // Delete the topic foo.
      TestUtils.deleteTopicWithAdmin(
        admin = admin,
        topic = "foo",
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )

      expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
        .setTopicPartitions(List(
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(barTopicId)
            .setPartitions(List[Integer](0, 1, 2, 3, 4, 5).asJava),
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(bazTopicId)
            .setPartitions(List[Integer](0, 1, 2, 3).asJava)).asJava)
      // Prepare the next heartbeat for member.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(memberId)
          .setMemberEpoch(4),
        true
      ).build()

      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          shareGroupHeartbeatResponse.data.assignment != null &&
          expectedAssignment.topicPartitions.containsAll(shareGroupHeartbeatResponse.data.assignment.topicPartitions) &&
          shareGroupHeartbeatResponse.data.assignment.topicPartitions.containsAll(expectedAssignment.topicPartitions)
      }, msg = s"Could not update partitions assignment for topic foo. Last response $shareGroupHeartbeatResponse.")
      // Verify the response.
      assertEquals(5, shareGroupHeartbeatResponse.data.memberEpoch)
    } finally {
      admin.close()
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
      new ClusterConfigProperty(key = "group.share.enable", value = "true"),
      new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
      new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
      new ClusterConfigProperty(key = "group.share.heartbeat.interval.ms", value = "500"),
      new ClusterConfigProperty(key = "group.share.min.heartbeat.interval.ms", value = "500"),
      new ClusterConfigProperty(key = "group.share.session.timeout.ms", value = "501"),
      new ClusterConfigProperty(key = "group.share.min.session.timeout.ms", value = "501")
    ))
  def testMemberJoiningAndExpiring(): Unit = {
    val admin = cluster.admin()

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )

      // Heartbeat request to join the group. Note that the member subscribes
      // to an nonexistent topic.
      var shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(Uuid.randomUuid.toString)
          .setMemberEpoch(0)
          .setSubscribedTopicNames(List("foo").asJava),
        true
      ).build()

      // Send the request until receiving a successful response. There is a delay
      // here because the group coordinator is loaded in the background.
      var shareGroupHeartbeatResponse: ShareGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
      }, msg = s"Could not join the group successfully. Last response $shareGroupHeartbeatResponse.")

      // Verify the response for member.
      val memberId = shareGroupHeartbeatResponse.data.memberId
      var memberEpoch = shareGroupHeartbeatResponse.data.memberEpoch
      assertNotNull(memberId)
      assertEquals(1, memberEpoch)
      assertEquals(new ShareGroupHeartbeatResponseData.Assignment(), shareGroupHeartbeatResponse.data.assignment)

      // Create the topic.
      val fooId = TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 2
      )

      // This is the expected assignment.
      var expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
        .setTopicPartitions(List(new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(fooId)
          .setPartitions(List[Integer](0, 1).asJava)).asJava)

      // Prepare the next heartbeat for member.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(memberId)
          .setMemberEpoch(memberEpoch),
        true
      ).build()

      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          shareGroupHeartbeatResponse.data.assignment == expectedAssignment
      }, msg = s"Could not get foo partitions assigned. Last response $shareGroupHeartbeatResponse.")

      // Verify the response, the epoch should have been bumped.
      assertTrue(shareGroupHeartbeatResponse.data.memberEpoch > memberEpoch)
      memberEpoch = shareGroupHeartbeatResponse.data.memberEpoch

      // Prepare the next heartbeat with a new subscribed topic.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(memberId)
          .setMemberEpoch(memberEpoch)
          .setSubscribedTopicNames(List("foo", "bar").asJava),
        true
      ).build()

      val barId = TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "bar"
      )

      expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
        .setTopicPartitions(List(
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(fooId)
            .setPartitions(List[Integer](0, 1).asJava),
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(barId)
            .setPartitions(List[Integer](0).asJava)).asJava)

      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          shareGroupHeartbeatResponse.data.assignment != null &&
          expectedAssignment.topicPartitions.containsAll(shareGroupHeartbeatResponse.data.assignment.topicPartitions) &&
          shareGroupHeartbeatResponse.data.assignment.topicPartitions.containsAll(expectedAssignment.topicPartitions)
      }, msg = s"Could not get bar partitions assigned. Last response $shareGroupHeartbeatResponse.")

      // Verify the response, the epoch should have been bumped.
      assertTrue(shareGroupHeartbeatResponse.data.memberEpoch > memberEpoch)
      memberEpoch = shareGroupHeartbeatResponse.data.memberEpoch

      // Prepare the next heartbeat which is empty to verify no assignment changes.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(memberId)
          .setMemberEpoch(memberEpoch),
        true
      ).build()

      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
      }, msg = s"Could not get empty heartbeat response. Last response $shareGroupHeartbeatResponse.")

      // Verify the response, the epoch should be same.
      assertEquals(memberEpoch, shareGroupHeartbeatResponse.data.memberEpoch)

      // Blocking the thread for 1 sec so that the session times out and the member needs to rejoin.
      Thread.sleep(1000)

      // Prepare the next heartbeat which is empty to verify no assignment changes.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(memberId)
          .setMemberEpoch(memberEpoch),
        true
      ).build()

      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.UNKNOWN_MEMBER_ID.code
      }, msg = s"Member should have been expired because of the timeout . Last response $shareGroupHeartbeatResponse.")

      // Member sends a request again to join the share group
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(memberId)
          .setMemberEpoch(0)
          .setSubscribedTopicNames(List("foo", "bar").asJava),
        true
      ).build()

      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          shareGroupHeartbeatResponse.data.assignment != null &&
          expectedAssignment.topicPartitions.containsAll(shareGroupHeartbeatResponse.data.assignment.topicPartitions) &&
          shareGroupHeartbeatResponse.data.assignment.topicPartitions.containsAll(expectedAssignment.topicPartitions)
      }, msg = s"Could not get bar partitions assigned upon rejoining. Last response $shareGroupHeartbeatResponse.")

      // Epoch should have been bumped when a member is removed and again when it joins back.
      assertTrue(shareGroupHeartbeatResponse.data.memberEpoch > memberEpoch)
    } finally {
      admin.close()
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
      new ClusterConfigProperty(key = "group.share.enable", value = "true"),
      new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
      new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
    ))
  def testGroupCoordinatorChange(): Unit = {
    val admin = cluster.admin()
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )
      // Heartbeat request to join the group. Note that the member subscribes
      // to an nonexistent topic.
      var shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(Uuid.randomUuid.toString)
          .setMemberEpoch(0)
          .setSubscribedTopicNames(List("foo").asJava),
        true
      ).build()
      // Send the request until receiving a successful response. There is a delay
      // here because the group coordinator is loaded in the background.
      var shareGroupHeartbeatResponse: ShareGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
      }, msg = s"Could not join the group successfully. Last response $shareGroupHeartbeatResponse.")
      // Verify the response for member.
      val memberId = shareGroupHeartbeatResponse.data.memberId
      assertNotNull(memberId)
      assertEquals(1, shareGroupHeartbeatResponse.data.memberEpoch)
      assertEquals(new ShareGroupHeartbeatResponseData.Assignment(), shareGroupHeartbeatResponse.data.assignment)
      // Create the topic.
      val fooId = TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 2
      )
      // This is the expected assignment.
      val expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
        .setTopicPartitions(List(new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(fooId)
          .setPartitions(List[Integer](0, 1).asJava)).asJava)
      // Prepare the next heartbeat for member.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(memberId)
          .setMemberEpoch(1),
        true
      ).build()

      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          shareGroupHeartbeatResponse.data.assignment == expectedAssignment
      }, msg = s"Could not get partitions assigned. Last response $shareGroupHeartbeatResponse.")
      // Verify the response.
      assertEquals(2, shareGroupHeartbeatResponse.data.memberEpoch)

      // Restart the only running broker.
      val broker = cluster.brokers().values().iterator().next()
      cluster.shutdownBroker(broker.config.brokerId)
      cluster.startBroker(broker.config.brokerId)

      // Prepare the next heartbeat for member with no updates.
      shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
        new ShareGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(memberId)
          .setMemberEpoch(2),
        true
      ).build()

      // Should receive no error and no assignment changes.
      TestUtils.waitUntilTrue(() => {
        shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
        shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
      }, msg = s"Could not get partitions assigned. Last response $shareGroupHeartbeatResponse.")

      // Verify the response. Epoch should not have changed and null assignments determines that no
      // change in old assignment.
      assertEquals(2, shareGroupHeartbeatResponse.data.memberEpoch)
      assertNull(shareGroupHeartbeatResponse.data.assignment)
    } finally {
      admin.close()
    }
  }

  private def connectAndReceive(request: ShareGroupHeartbeatRequest): ShareGroupHeartbeatResponse = {
    IntegrationTestUtils.connectAndReceive[ShareGroupHeartbeatResponse](
      request,
      cluster.anyBrokerSocketServer(),
      cluster.clientListener()
    )
  }

  private def increasePartitions[B <: KafkaBroker](admin: Admin,
                                                   topic: String,
                                                   totalPartitionCount: Int,
                                                   brokersToValidate: Seq[B]
                                                  ): Unit = {
    val newPartitionSet: Map[String, NewPartitions] = Map.apply(topic -> NewPartitions.increaseTo(totalPartitionCount))
    admin.createPartitions(newPartitionSet.asJava)

    if (brokersToValidate.nonEmpty) {
      // wait until we've propagated all partitions metadata to all brokers
      val allPartitionsMetadata = waitForAllPartitionsMetadata(brokersToValidate, topic, totalPartitionCount)
      (0 until totalPartitionCount - 1).foreach(i => {
        allPartitionsMetadata.get(new TopicPartition(topic, i)).foreach { partitionMetadata =>
          assertEquals(totalPartitionCount, partitionMetadata.replicas.size)
        }
      })
    }
  }
}
