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

import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterFeature, ClusterTest, ClusterTestDefaults, Type}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.{TopicCollection, Uuid}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.message.{ConsumerGroupHeartbeatRequestData, ConsumerGroupHeartbeatResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.coordinator.group.{GroupConfig, GroupCoordinatorConfig}
import org.apache.kafka.server.common.Feature
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertNotNull}

import scala.collection.Map
import scala.jdk.CollectionConverters._

@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
  )
)
class ConsumerGroupHeartbeatRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic")
    )
  )
  def testConsumerGroupHeartbeatIsInaccessibleWhenDisabledByStaticConfig(): Unit = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
    ).build()

    val consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
    val expectedResponse = new ConsumerGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    assertEquals(expectedResponse, consumerGroupHeartbeatResponse.data)
  }

  @ClusterTest(
    features = Array(
      new ClusterFeature(feature = Feature.GROUP_VERSION, version = 0)
    )
  )
  def testConsumerGroupHeartbeatIsInaccessibleWhenFeatureFlagNotEnabled(): Unit = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
    ).build()

    val consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
    val expectedResponse = new ConsumerGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    assertEquals(expectedResponse, consumerGroupHeartbeatResponse.data)
  }

  @ClusterTest
  def testConsumerGroupHeartbeatIsAccessibleWhenNewGroupCoordinatorIsEnabled(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Heartbeat request to join the group. Note that the member subscribes
    // to an nonexistent topic.
    var consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(Uuid.randomUuid.toString)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var consumerGroupHeartbeatResponse: ConsumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $consumerGroupHeartbeatResponse.")

    // Verify the response.
    assertNotNull(consumerGroupHeartbeatResponse.data.memberId)
    assertEquals(1, consumerGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(new ConsumerGroupHeartbeatResponseData.Assignment(), consumerGroupHeartbeatResponse.data.assignment)

    // Create the topic.
    val topicId = createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Prepare the next heartbeat.
    consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(consumerGroupHeartbeatResponse.data.memberId)
        .setMemberEpoch(consumerGroupHeartbeatResponse.data.memberEpoch)
    ).build()

    // This is the expected assignment.
    val expectedAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
      .setTopicPartitions(List(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)

    // Heartbeats until the partitions are assigned.
    consumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        consumerGroupHeartbeatResponse.data.assignment == expectedAssignment
    }, msg = s"Could not get partitions assigned. Last response $consumerGroupHeartbeatResponse.")

    // Verify the response.
    assertEquals(2, consumerGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(expectedAssignment, consumerGroupHeartbeatResponse.data.assignment)

    // Leave the group.
    consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(consumerGroupHeartbeatResponse.data.memberId)
        .setMemberEpoch(-1)
    ).build()

    consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)

    // Verify the response.
    assertEquals(-1, consumerGroupHeartbeatResponse.data.memberEpoch)
  }

  @ClusterTest
  def testConsumerGroupHeartbeatWithRegularExpression(): Unit = {
    val admin = cluster.admin()

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    try {
      // Heartbeat request to join the group. Note that the member subscribes
      // to a nonexistent topic.
      var consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
        new ConsumerGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(Uuid.randomUuid().toString)
          .setMemberEpoch(0)
          .setRebalanceTimeoutMs(5 * 60 * 1000)
          .setSubscribedTopicRegex("foo*")
          .setTopicPartitions(List.empty.asJava)
      ).build()

      // Send the request until receiving a successful response. There is a delay
      // here because the group coordinator is loaded in the background.
      var consumerGroupHeartbeatResponse: ConsumerGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
        consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
      }, msg = s"Could not join the group successfully. Last response $consumerGroupHeartbeatResponse.")

      // Verify the response.
      assertNotNull(consumerGroupHeartbeatResponse.data.memberId)
      assertEquals(1, consumerGroupHeartbeatResponse.data.memberEpoch)
      assertEquals(new ConsumerGroupHeartbeatResponseData.Assignment(), consumerGroupHeartbeatResponse.data.assignment)

      // Create the topic.
      val topicId = createTopic(
        topic = "foo",
        numPartitions = 3
      )

      // Prepare the next heartbeat.
      consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
        new ConsumerGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(consumerGroupHeartbeatResponse.data.memberId)
          .setMemberEpoch(consumerGroupHeartbeatResponse.data.memberEpoch)
      ).build()

      // This is the expected assignment.
      var expectedAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
        .setTopicPartitions(List(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(topicId)
          .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)

      // Heartbeats until the partitions are assigned.
      consumerGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
        consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          consumerGroupHeartbeatResponse.data.assignment == expectedAssignment
      }, msg = s"Could not get partitions assigned. Last response $consumerGroupHeartbeatResponse.")

      // Verify the response.
      assertEquals(2, consumerGroupHeartbeatResponse.data.memberEpoch)
      assertEquals(expectedAssignment, consumerGroupHeartbeatResponse.data.assignment)

      // Delete the topic.
      admin.deleteTopics(TopicCollection.ofTopicIds(List(topicId).asJava)).all.get

      // Prepare the next heartbeat.
      consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
        new ConsumerGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberId(consumerGroupHeartbeatResponse.data.memberId)
          .setMemberEpoch(consumerGroupHeartbeatResponse.data.memberEpoch)
      ).build()

      // This is the expected assignment.
      expectedAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()

      // Heartbeats until the partitions are revoked.
      consumerGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
        consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          consumerGroupHeartbeatResponse.data.assignment == expectedAssignment
      }, msg = s"Could not get partitions revoked. Last response $consumerGroupHeartbeatResponse.")

      // Verify the response.
      assertEquals(2, consumerGroupHeartbeatResponse.data.memberEpoch)
      assertEquals(expectedAssignment, consumerGroupHeartbeatResponse.data.assignment)
    } finally {
      admin.close()
    }
  }

  @ClusterTest
  def testConsumerGroupHeartbeatWithInvalidRegularExpression(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Heartbeat request to join the group. Note that the member subscribes
    // to an nonexistent topic.
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(Uuid.randomUuid().toString)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicRegex("[")
        .setTopicPartitions(List.empty.asJava)
    ).build()

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var consumerGroupHeartbeatResponse: ConsumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.INVALID_REGULAR_EXPRESSION.code
    }, msg = s"Did not receive the expected error. Last response $consumerGroupHeartbeatResponse.")

    // Verify the response.
    assertEquals(Errors.INVALID_REGULAR_EXPRESSION.code, consumerGroupHeartbeatResponse.data.errorCode)
  }

  @ClusterTest
  def testEmptyConsumerGroupId(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Heartbeat request to join the group. Note that the member subscribes
    // to an nonexistent topic.
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("")
        .setMemberId(Uuid.randomUuid().toString)
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
      consumerGroupHeartbeatResponse.data.errorCode == Errors.INVALID_REQUEST.code
    }, msg = s"Did not receive the expected error. Last response $consumerGroupHeartbeatResponse.")

    // Verify the response.
    assertEquals(Errors.INVALID_REQUEST.code, consumerGroupHeartbeatResponse.data.errorCode)
    assertEquals("GroupId can't be empty.", consumerGroupHeartbeatResponse.data.errorMessage)
  }

  @ClusterTest
  def testConsumerGroupHeartbeatWithEmptySubscription(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Heartbeat request to join the group.
    var consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(Uuid.randomUuid().toString)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicRegex("")
        .setTopicPartitions(List.empty.asJava)
    ).build()

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var consumerGroupHeartbeatResponse: ConsumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Did not receive the expected successful response. Last response $consumerGroupHeartbeatResponse.")

    // Heartbeat request to join the group.
    consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(Uuid.randomUuid().toString)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List.empty.asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    consumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Did not receive the expected successful response. Last response $consumerGroupHeartbeatResponse.")
  }

  @ClusterTest
  def testRejoiningStaticMemberGetsAssignmentsBackWhenNewGroupCoordinatorIsEnabled(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    val instanceId = "instanceId"

    // Heartbeat request so that a static member joins the group
    var consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(Uuid.randomUuid.toString)
        .setInstanceId(instanceId)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var consumerGroupHeartbeatResponse: ConsumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Static member could not join the group successfully. Last response $consumerGroupHeartbeatResponse.")

    // Verify the response.
    assertNotNull(consumerGroupHeartbeatResponse.data.memberId)
    assertEquals(1, consumerGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(new ConsumerGroupHeartbeatResponseData.Assignment(), consumerGroupHeartbeatResponse.data.assignment)

    // Create the topic.
    val topicId = createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Prepare the next heartbeat.
    consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setInstanceId(instanceId)
        .setMemberId(consumerGroupHeartbeatResponse.data.memberId)
        .setMemberEpoch(consumerGroupHeartbeatResponse.data.memberEpoch)
    ).build()

    // This is the expected assignment.
    val expectedAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
      .setTopicPartitions(List(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)

    // Heartbeats until the partitions are assigned.
    consumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        consumerGroupHeartbeatResponse.data.assignment == expectedAssignment
    }, msg = s"Static member could not get partitions assigned. Last response $consumerGroupHeartbeatResponse.")

    // Verify the response.
    assertNotNull(consumerGroupHeartbeatResponse.data.memberId)
    assertEquals(2, consumerGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(expectedAssignment, consumerGroupHeartbeatResponse.data.assignment)

    val oldMemberId = consumerGroupHeartbeatResponse.data.memberId

    // Leave the group temporarily
    consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setInstanceId(instanceId)
        .setMemberId(consumerGroupHeartbeatResponse.data.memberId)
        .setMemberEpoch(-2)
    ).build()

    consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)

    // Verify the response.
    assertEquals(-2, consumerGroupHeartbeatResponse.data.memberEpoch)

    // Another static member replaces the above member. It gets the same assignments back
    consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(Uuid.randomUuid.toString)
        .setInstanceId(instanceId)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)

    // Verify the response.
    assertNotNull(consumerGroupHeartbeatResponse.data.memberId)
    assertEquals(2, consumerGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(expectedAssignment, consumerGroupHeartbeatResponse.data.assignment)
    // The 2 member IDs should be different
    assertNotEquals(oldMemberId, consumerGroupHeartbeatResponse.data.memberId)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, value = "5001"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "5001")
    )
  )
  def testStaticMemberRemovedAfterSessionTimeoutExpiryWhenNewGroupCoordinatorIsEnabled(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    val instanceId = "instanceId"

    // Heartbeat request to join the group. Note that the member subscribes
    // to an nonexistent topic.
    var consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(Uuid.randomUuid.toString)
        .setInstanceId(instanceId)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var consumerGroupHeartbeatResponse: ConsumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $consumerGroupHeartbeatResponse.")

    // Verify the response.
    assertNotNull(consumerGroupHeartbeatResponse.data.memberId)
    assertEquals(1, consumerGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(new ConsumerGroupHeartbeatResponseData.Assignment(), consumerGroupHeartbeatResponse.data.assignment)

    // Create the topic.
    val topicId = createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Prepare the next heartbeat.
    consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setInstanceId(instanceId)
        .setMemberId(consumerGroupHeartbeatResponse.data.memberId)
        .setMemberEpoch(consumerGroupHeartbeatResponse.data.memberEpoch)
    ).build()

    // This is the expected assignment.
    val expectedAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
      .setTopicPartitions(List(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)

    // Heartbeats until the partitions are assigned.
    consumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        consumerGroupHeartbeatResponse.data.assignment == expectedAssignment
    }, msg = s"Could not get partitions assigned. Last response $consumerGroupHeartbeatResponse.")

    // Verify the response.
    assertEquals(2, consumerGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(expectedAssignment, consumerGroupHeartbeatResponse.data.assignment)

    // A new static member tries to join the group with an inuse instanceid.
    consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(Uuid.randomUuid.toString)
        .setInstanceId(instanceId)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    // Validating that trying to join with an in-use instanceId would throw an UnreleasedInstanceIdException.
    consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
    assertEquals(Errors.UNRELEASED_INSTANCE_ID.code, consumerGroupHeartbeatResponse.data.errorCode)

    // The new static member join group will keep failing with an UnreleasedInstanceIdException
    // until eventually it gets through because the existing member will be kicked out
    // because of not sending a heartbeat till session timeout expiry.
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        consumerGroupHeartbeatResponse.data.assignment == expectedAssignment
    }, msg = s"Could not re-join the group successfully. Last response $consumerGroupHeartbeatResponse.")

    // Verify the response. The group epoch bumps upto 4 which eventually reflects in the new member epoch.
    assertEquals(4, consumerGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(expectedAssignment, consumerGroupHeartbeatResponse.data.assignment)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, value = "5000")
    )
  )
  def testUpdateConsumerGroupHeartbeatConfigSuccessful(): Unit = {
    val admin = cluster.admin()

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()
    try {
      val newHeartbeatIntervalMs = 10000
      val instanceId = "instanceId"
      val consumerGroupId = "grp"

      // Heartbeat request to join the group. Note that the member subscribes
      // to an nonexistent topic.
      var consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
        new ConsumerGroupHeartbeatRequestData()
          .setGroupId(consumerGroupId)
          .setMemberId(Uuid.randomUuid.toString)
          .setInstanceId(instanceId)
          .setMemberEpoch(0)
          .setRebalanceTimeoutMs(5 * 60 * 1000)
          .setSubscribedTopicNames(List("foo").asJava)
          .setTopicPartitions(List.empty.asJava)
      ).build()

      // Send the request until receiving a successful response. There is a delay
      // here because the group coordinator is loaded in the background.
      var consumerGroupHeartbeatResponse: ConsumerGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
        consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
      }, msg = s"Could not join the group successfully. Last response $consumerGroupHeartbeatResponse.")

      // Verify the response.
      assertNotNull(consumerGroupHeartbeatResponse.data.memberId)
      assertEquals(1, consumerGroupHeartbeatResponse.data.memberEpoch)
      assertEquals(5000, consumerGroupHeartbeatResponse.data.heartbeatIntervalMs)

      // Alter consumer heartbeat interval config
      val resource = new ConfigResource(ConfigResource.Type.GROUP, consumerGroupId)
      val op = new AlterConfigOp(
        new ConfigEntry(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, newHeartbeatIntervalMs.toString),
        OpType.SET
      )
      admin.incrementalAlterConfigs(Map(resource -> List(op).asJavaCollection).asJava).all.get

      // Prepare the next heartbeat.
      consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
        new ConsumerGroupHeartbeatRequestData()
          .setGroupId(consumerGroupId)
          .setInstanceId(instanceId)
          .setMemberId(consumerGroupHeartbeatResponse.data.memberId)
          .setMemberEpoch(consumerGroupHeartbeatResponse.data.memberEpoch)
      ).build()

      // Verify the response. The heartbeat interval was updated.
      TestUtils.waitUntilTrue(() => {
        consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
        consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
          newHeartbeatIntervalMs == consumerGroupHeartbeatResponse.data.heartbeatIntervalMs
      }, msg = s"Dynamic update consumer group config failed. Last response $consumerGroupHeartbeatResponse.")
    } finally {
      admin.close()
    }
  }

  @ClusterTest
  def testConsumerGroupHeartbeatFailureIfMemberIdMissingForVersionsAbove0(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    var consumerGroupHeartbeatResponse: ConsumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.INVALID_REQUEST.code
    }, msg = "Should fail due to invalid member id.")
  }

  @ClusterTest
  def testMemberIdGeneratedOnServerWhenApiVersionIs0(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build(0)

    var consumerGroupHeartbeatResponse: ConsumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $consumerGroupHeartbeatResponse.")

    val memberId = consumerGroupHeartbeatResponse.data().memberId()
    assertNotNull(memberId)
    assertFalse(memberId.isEmpty)
  }

  @ClusterTest
  def testFencedMemberCanRejoinWithEpochZero(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    val memberId = Uuid.randomUuid().toString
    val groupId = "test-fenced-rejoin-grp"

    // Heartbeat request to join the group.
    var consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    // Wait for successful join.
    var consumerGroupHeartbeatResponse: ConsumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $consumerGroupHeartbeatResponse.")

    // Verify initial join success.
    assertNotNull(consumerGroupHeartbeatResponse.data.memberId)
    assertEquals(1, consumerGroupHeartbeatResponse.data.memberEpoch)

    // Create the topic to trigger partition assignment.
    val topicId = createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Heartbeat to get partitions assigned.
    consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId)
        .setMemberEpoch(consumerGroupHeartbeatResponse.data.memberEpoch)
    ).build()

    // Expected assignment.
    val expectedAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
      .setTopicPartitions(List(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)

    // Wait until partitions are assigned and member epoch advances.
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        consumerGroupHeartbeatResponse.data.assignment == expectedAssignment
    }, msg = s"Could not get partitions assigned. Last response $consumerGroupHeartbeatResponse.")

    // Verify member has epoch > 0 (should be 2).
    assertEquals(2, consumerGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(expectedAssignment, consumerGroupHeartbeatResponse.data.assignment)

    // Simulate a fenced member attempting to rejoin with epoch=0.
    val rejoinRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    val rejoinResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](rejoinRequest)

    // Verify the full response.
    // Since the subscription/metadata hasn't changed, the member should get
    // their current state back with the same epoch (2) and assignment.
    val expectedRejoinResponse = new ConsumerGroupHeartbeatResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMemberId(memberId)
      .setMemberEpoch(2)
      .setHeartbeatIntervalMs(rejoinResponse.data.heartbeatIntervalMs)
      .setAssignment(expectedAssignment)

    assertEquals(expectedRejoinResponse, rejoinResponse.data)
  }

  @ClusterTest
  def testDuplicateFullHeartbeatInStableState(): Unit = {
    createOffsetsTopic()

    val memberId = Uuid.randomUuid().toString
    val groupId = "test-duplicate-stable-grp"

    // Create topic first so member gets assignment immediately.
    val topicId = createTopic(topic = "foo", numPartitions = 3)

    // Join the group.
    val request = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    var response: ConsumerGroupHeartbeatResponse = null
    val expectedAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
      .setTopicPartitions(List(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)

    TestUtils.waitUntilTrue(() => {
      response = connectAndReceive[ConsumerGroupHeartbeatResponse](request)
      response.data.errorCode == Errors.NONE.code &&
        response.data.assignment == expectedAssignment
    }, msg = s"Could not get assignment. Last response $response.")

    val stableEpoch = response.data.memberEpoch

    // Send full heartbeat request.
    val fullRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId)
        .setMemberEpoch(stableEpoch)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
          .setTopicId(topicId)
          .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)
    ).build()

    val firstResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](fullRequest)

    val expectedFirstResponse = new ConsumerGroupHeartbeatResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMemberId(memberId)
      .setMemberEpoch(stableEpoch)
      .setHeartbeatIntervalMs(firstResponse.data.heartbeatIntervalMs)
      .setAssignment(expectedAssignment)

    assertEquals(expectedFirstResponse, firstResponse.data)

    // Send duplicate heartbeat request.
    val duplicateResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](fullRequest)

    // Verify duplicate produces same response.
    assertEquals(expectedFirstResponse, duplicateResponse.data)
  }

  @ClusterTest
  def testDuplicateFullHeartbeatWhileWaitingForPartitions(): Unit = {
    createOffsetsTopic()

    val memberId1 = Uuid.randomUuid().toString
    val memberId2 = Uuid.randomUuid().toString
    val groupId = "test-duplicate-waiting-grp"

    // Create topic.
    val topicId = createTopic(topic = "foo", numPartitions = 2)

    // Member 1 joins and gets all partitions.
    val request1 = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId1)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    var response1: ConsumerGroupHeartbeatResponse = null
    val allPartitions = new ConsumerGroupHeartbeatResponseData.Assignment()
      .setTopicPartitions(List(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1).asJava)).asJava)

    TestUtils.waitUntilTrue(() => {
      response1 = connectAndReceive[ConsumerGroupHeartbeatResponse](request1)
      response1.data.errorCode == Errors.NONE.code &&
        response1.data.assignment == allPartitions
    }, msg = s"Member 1 could not get assignment. Last response $response1.")

    // Member 2 joins, triggering rebalance. Member 2 will wait for Member 1 to release partitions.
    val request2 = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId2)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    var response2: ConsumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      response2 = connectAndReceive[ConsumerGroupHeartbeatResponse](request2)
      response2.data.errorCode == Errors.NONE.code
    }, msg = s"Member 2 could not join. Last response $response2.")

    val member2Epoch = response2.data.memberEpoch

    // Member 2 sends full heartbeat while waiting for partitions from Member 1.
    val fullRequest2 = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId2)
        .setMemberEpoch(member2Epoch)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    val firstResponse2 = connectAndReceive[ConsumerGroupHeartbeatResponse](fullRequest2)

    val expectedFirstResponse2 = new ConsumerGroupHeartbeatResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMemberId(memberId2)
      .setMemberEpoch(member2Epoch)
      .setHeartbeatIntervalMs(firstResponse2.data.heartbeatIntervalMs)
      .setAssignment(firstResponse2.data.assignment)

    assertEquals(expectedFirstResponse2, firstResponse2.data)

    // Send duplicate heartbeat request.
    val duplicateResponse2 = connectAndReceive[ConsumerGroupHeartbeatResponse](fullRequest2)

    // Verify duplicate produces same response.
    assertEquals(expectedFirstResponse2, duplicateResponse2.data)
  }

  @ClusterTest
  def testDuplicateFullHeartbeatDuringRevocation(): Unit = {
    createOffsetsTopic()

    val memberId1 = Uuid.randomUuid().toString
    val memberId2 = Uuid.randomUuid().toString
    val groupId = "test-duplicate-revocation-grp"

    // Create topic.
    val topicId = createTopic(topic = "foo", numPartitions = 2)

    // Member 1 joins and gets all partitions.
    val request1 = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId1)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    var response1: ConsumerGroupHeartbeatResponse = null
    val allPartitions = new ConsumerGroupHeartbeatResponseData.Assignment()
      .setTopicPartitions(List(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1).asJava)).asJava)

    TestUtils.waitUntilTrue(() => {
      response1 = connectAndReceive[ConsumerGroupHeartbeatResponse](request1)
      response1.data.errorCode == Errors.NONE.code &&
        response1.data.assignment == allPartitions
    }, msg = s"Member 1 could not get assignment. Last response $response1.")

    val member1Epoch = response1.data.memberEpoch

    // Member 2 joins, triggering rebalance.
    val request2 = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId2)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    var response2: ConsumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      response2 = connectAndReceive[ConsumerGroupHeartbeatResponse](request2)
      response2.data.errorCode == Errors.NONE.code
    }, msg = s"Member 2 could not join. Last response $response2.")

    // Member 1 sends full heartbeat (still reporting all partitions).
    val fullRequest1 = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId1)
        .setMemberEpoch(member1Epoch)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
          .setTopicId(topicId)
          .setPartitions(List[Integer](0, 1).asJava)).asJava)
    ).build()

    val firstResponse1 = connectAndReceive[ConsumerGroupHeartbeatResponse](fullRequest1)

    val expectedFirstResponse1 = new ConsumerGroupHeartbeatResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMemberId(memberId1)
      .setMemberEpoch(firstResponse1.data.memberEpoch)
      .setHeartbeatIntervalMs(firstResponse1.data.heartbeatIntervalMs)
      .setAssignment(firstResponse1.data.assignment)

    assertEquals(expectedFirstResponse1, firstResponse1.data)

    // Send duplicate heartbeat request.
    val duplicateResponse1 = connectAndReceive[ConsumerGroupHeartbeatResponse](fullRequest1)

    // Verify duplicate produces same response.
    assertEquals(expectedFirstResponse1, duplicateResponse1.data)
  }

  @ClusterTest
  def testDuplicateFullHeartbeatWithRevocationAck(): Unit = {
    createOffsetsTopic()

    val memberId1 = Uuid.randomUuid().toString
    val memberId2 = Uuid.randomUuid().toString
    val groupId = "test-duplicate-revocation-ack-grp"

    // Create topic.
    val topicId = createTopic(topic = "foo", numPartitions = 2)

    // Member 1 joins and gets all partitions.
    val request1 = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId1)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    var response1: ConsumerGroupHeartbeatResponse = null
    val allPartitions = new ConsumerGroupHeartbeatResponseData.Assignment()
      .setTopicPartitions(List(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1).asJava)).asJava)

    TestUtils.waitUntilTrue(() => {
      response1 = connectAndReceive[ConsumerGroupHeartbeatResponse](request1)
      response1.data.errorCode == Errors.NONE.code &&
        response1.data.assignment == allPartitions
    }, msg = s"Member 1 could not get assignment. Last response $response1.")

    val member1InitialEpoch = response1.data.memberEpoch

    // Member 2 joins, triggering rebalance.
    val request2 = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId2)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List.empty.asJava)
    ).build()

    var response2: ConsumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      response2 = connectAndReceive[ConsumerGroupHeartbeatResponse](request2)
      response2.data.errorCode == Errors.NONE.code
    }, msg = s"Member 2 could not join. Last response $response2.")

    // Member 1 sends heartbeat acknowledging revocation (only reporting partition 0).
    val ackRequest1 = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId1)
        .setMemberEpoch(member1InitialEpoch)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
          .setTopicId(topicId)
          .setPartitions(List[Integer](0).asJava)).asJava)
    ).build()

    val ackResponse1 = connectAndReceive[ConsumerGroupHeartbeatResponse](ackRequest1)
    assertEquals(Errors.NONE.code, ackResponse1.data.errorCode)

    val member1NewEpoch = ackResponse1.data.memberEpoch

    // Member 1 sends full heartbeat with new epoch.
    val fullRequest1 = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId1)
        .setMemberEpoch(member1NewEpoch)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava)
        .setTopicPartitions(List(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
          .setTopicId(topicId)
          .setPartitions(List[Integer](0).asJava)).asJava)
    ).build()

    val firstResponse1 = connectAndReceive[ConsumerGroupHeartbeatResponse](fullRequest1)

    val expectedFirstResponse1 = new ConsumerGroupHeartbeatResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMemberId(memberId1)
      .setMemberEpoch(firstResponse1.data.memberEpoch)
      .setHeartbeatIntervalMs(firstResponse1.data.heartbeatIntervalMs)
      .setAssignment(firstResponse1.data.assignment)

    assertEquals(expectedFirstResponse1, firstResponse1.data)

    // Send duplicate heartbeat request.
    val duplicateResponse1 = connectAndReceive[ConsumerGroupHeartbeatResponse](fullRequest1)

    // Verify duplicate produces same response.
    assertEquals(expectedFirstResponse1, duplicateResponse1.data)
  }
}
