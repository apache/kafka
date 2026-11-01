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
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertNotNull, assertTrue}

import scala.collection.Map
import scala.jdk.CollectionConverters._

object ConsumerGroupHeartbeatRequestTest {
  @ClusterTestDefaults(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "0")
    )
  )
  class WithAssignmentBatchingDisabledTest(cluster: ClusterInstance) extends ConsumerGroupHeartbeatRequestTest(cluster) {
  }
}

@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
  )
)
class ConsumerGroupHeartbeatRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  protected def isConsumerAssignmentBatchingEnabled: Boolean = {
    cluster.brokers.values.stream.allMatch(b => b.config.groupCoordinatorConfig.consumerGroupAssignmentIntervalMs > 0)
  }

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
    assertEquals(2, consumerGroupHeartbeatResponse.data.memberEpoch)
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
    assertEquals(3, consumerGroupHeartbeatResponse.data.memberEpoch)
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
    assertEquals(2, consumerGroupHeartbeatResponse.data.memberEpoch)
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
    assertEquals(3, consumerGroupHeartbeatResponse.data.memberEpoch)
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
    assertEquals(3, consumerGroupHeartbeatResponse.data.memberEpoch)
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
    assertEquals(2, consumerGroupHeartbeatResponse.data.memberEpoch)
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
    assertEquals(3, consumerGroupHeartbeatResponse.data.memberEpoch)
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

    // Verify the response. The group epoch bumps upto 5 which eventually reflects in the new member epoch.
    assertEquals(5, consumerGroupHeartbeatResponse.data.memberEpoch)
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
      assertEquals(2, consumerGroupHeartbeatResponse.data.memberEpoch)
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
    assertEquals(2, consumerGroupHeartbeatResponse.data.memberEpoch)

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

    // Verify member has epoch > 0 (should be 3).
    assertEquals(3, consumerGroupHeartbeatResponse.data.memberEpoch)
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
    // their current state back with the same epoch (3) and assignment.
    val expectedRejoinResponse = new ConsumerGroupHeartbeatResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMemberId(memberId)
      .setMemberEpoch(3)
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

  @ClusterTest
  def testConsumerGroupHeartbeatRebalance(): Unit = {
    createOffsetsTopic()

    val memberId1 = Uuid.randomUuid().toString
    val memberId2 = Uuid.randomUuid().toString
    val groupId = "test-rebalance-grp"

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

    // Expected assignment.
    val expectedAssignment1 = new ConsumerGroupHeartbeatResponseData.Assignment()
      .setTopicPartitions(List(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1).asJava)).asJava)

    val expectedResponse1 = new ConsumerGroupHeartbeatResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMemberId(memberId1)
      .setMemberEpoch(2)
      .setHeartbeatIntervalMs(response1.data.heartbeatIntervalMs)
      .setAssignment(expectedAssignment1)
    assertEquals(expectedResponse1, response1.data)

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

    // Expected assignment.
    val expectedAssignment2 = new ConsumerGroupHeartbeatResponseData.Assignment()
      .setTopicPartitions(List.empty.asJava)

    val expectedResponse2 = new ConsumerGroupHeartbeatResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMemberId(memberId2)
      .setMemberEpoch(if (isConsumerAssignmentBatchingEnabled) member1InitialEpoch else member1InitialEpoch + 1)
      .setHeartbeatIntervalMs(response2.data.heartbeatIntervalMs)
      .setAssignment(expectedAssignment2)
    assertEquals(expectedResponse2, response2.data)

    val member2InitialEpoch = response2.data.memberEpoch

    // Member 1 heartbeats and sees revoked partition.
    val request3 = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId1)
        .setMemberEpoch(member1InitialEpoch)
    ).build()

    var response3: ConsumerGroupHeartbeatResponse = null
    if (isConsumerAssignmentBatchingEnabled) {
      TestUtils.waitUntilTrue(() => {
        response3 = connectAndReceive[ConsumerGroupHeartbeatResponse](request3)
        response3.data.errorCode == Errors.NONE.code &&
          response3.data.assignment != null
      }, msg = s"Member 1 did not get new assignment before timeout.")
    } else {
      response3 = connectAndReceive[ConsumerGroupHeartbeatResponse](request3)
      assertEquals(Errors.NONE.code, response3.data.errorCode)
    }

    // Expected assignment.
    val expectedAssignment3 = new ConsumerGroupHeartbeatResponseData.Assignment()
      .setTopicPartitions(List(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0).asJava)).asJava)

    val expectedResponse3 = new ConsumerGroupHeartbeatResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMemberId(memberId1)
      .setMemberEpoch(member1InitialEpoch)
      .setHeartbeatIntervalMs(response3.data.heartbeatIntervalMs)
      .setAssignment(expectedAssignment3)
    assertEquals(expectedResponse3, response3.data)

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

    // Member 2 heartbeats and is assigned new partition.
    val request4 = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId2)
        .setMemberEpoch(member2InitialEpoch)
    ).build()

    val response4 = connectAndReceive[ConsumerGroupHeartbeatResponse](request4)
    assertEquals(Errors.NONE.code, response4.data.errorCode)

    val expectedAssignment4 = new ConsumerGroupHeartbeatResponseData.Assignment()
      .setTopicPartitions(List(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](1).asJava)).asJava)

    val expectedResponse4 = new ConsumerGroupHeartbeatResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMemberId(memberId2)
      .setMemberEpoch(member1NewEpoch)
      .setHeartbeatIntervalMs(response4.data.heartbeatIntervalMs)
      .setAssignment(expectedAssignment4)
    assertEquals(expectedResponse4, response4.data)
  }

  private def ownedTopicPartitions(
    partitions: (Uuid, Int)*
  ): List[ConsumerGroupHeartbeatRequestData.TopicPartitions] = {
    partitions.groupBy(_._1).map { case (topicId, topicPartitions) =>
      new ConsumerGroupHeartbeatRequestData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(topicPartitions.map(partition => Int.box(partition._2)).asJava)
    }.toList
  }

  private def assignedTopicPartitions(
    response: ConsumerGroupHeartbeatResponseData
  ): Option[Set[(Uuid, Int)]] = {
    Option(response.assignment).map(_.topicPartitions.asScala.flatMap { topicPartitions =>
      topicPartitions.partitions.asScala.map(partition => (topicPartitions.topicId, partition.intValue))
    }.toSet)
  }

  @ClusterTest
  def testUnreportedPartitionIsFreedOnFullHeartbeat(): Unit = {
    // This test reproduces the following sequence, in which two clients own the same partition:
    // 1. M joins. The coordinator sends {foo-0, foo-1} at epoch 11.
    // 2. M starts the reconciliation and calls onPartitionsAssigned([foo-0, foo-1]).
    // 3. N joins and the target assignment moves to epoch 12, giving foo-1 to N.
    // 4. A heartbeat of M fails (a request timeout, a coordinator move, any error). M's next
    //    heartbeat resends all the fields, including the owned set {}. In that heartbeat, the
    //    coordinator applies the new target, which takes foo-1 away from M, and checks the
    //    revocation against the owned set M has just sent. That set doesn't contain foo-1, so
    //    foo-1 counts as already revoked: M moves straight to epoch 12 with {foo-0}, and foo-1
    //    is freed.
    // 5. N gets foo-1 and starts consuming it.
    // 6. M's callback returns, and M starts fetching foo-1 too. Two clients now own foo-1.
    // 7. M then applies {foo-0}, revokes foo-1, and commits foo-1's offset at epoch 12. The
    //    commit is accepted because the epoch matches, so it can overwrite N's progress.
    //
    // The epochs in the test differ from the ones above, and the assignor decides which of
    // the two partitions moves to N.
    createOffsetsTopic()

    val groupId = "test-unreported-partition-grp"
    val memberIdM = Uuid.randomUuid().toString
    val memberIdN = Uuid.randomUuid().toString
    val rebalanceTimeoutMs = 5 * 60 * 1000

    val topicId = createTopic(topic = "foo", numPartitions = 2)
    val foo0 = (topicId, 0)
    val foo1 = (topicId, 1)

    // M joins and is assigned {foo-0, foo-1}. M starts taking them, so its owned set
    // is still {}.
    var responseM: ConsumerGroupHeartbeatResponseData = null
    TestUtils.waitUntilTrue(() => {
      responseM = consumerGroupHeartbeat(
        groupId = groupId,
        memberId = memberIdM,
        memberEpoch = 0,
        rebalanceTimeoutMs = rebalanceTimeoutMs,
        subscribedTopicNames = List("foo"),
        topicPartitions = ownedTopicPartitions()
      )
      assignedTopicPartitions(responseM).contains(Set(foo0, foo1))
    }, msg = s"M could not get {foo-0, foo-1}. Last response $responseM.")
    val epochWithBoth = responseM.memberEpoch

    // N joins. The new target assignment gives one of the partitions to N, so N waits
    // until M no longer holds it.
    var responseN = consumerGroupHeartbeat(
      groupId = groupId,
      memberId = memberIdN,
      memberEpoch = 0,
      rebalanceTimeoutMs = rebalanceTimeoutMs,
      subscribedTopicNames = List("foo"),
      topicPartitions = ownedTopicPartitions()
    )
    assertEquals(Some(Set.empty), assignedTopicPartitions(responseN))

    // A heartbeat of M fails, so M's next heartbeat resends all the fields, with owned
    // set {}. The partition given to N is missing from it, so the coordinator treats it
    // as released: M moves to the new epoch with the other partition, without being
    // asked to revoke anything, and the partition given to N is freed.
    TestUtils.waitUntilTrue(() => {
      responseM = consumerGroupHeartbeat(
        groupId = groupId,
        memberId = memberIdM,
        memberEpoch = epochWithBoth,
        rebalanceTimeoutMs = rebalanceTimeoutMs,
        subscribedTopicNames = List("foo"),
        topicPartitions = ownedTopicPartitions()
      )
      !assignedTopicPartitions(responseM).contains(Set(foo0, foo1))
    }, msg = s"M's assignment did not change. Last response $responseM.")
    assertTrue(responseM.memberEpoch > epochWithBoth,
      s"Expected M to move past epoch $epochWithBoth. Last response $responseM.")
    val newEpoch = responseM.memberEpoch
    val partitionsOfM = assignedTopicPartitions(responseM).get
    assertEquals(1, partitionsOfM.size)
    val (_, movedPartition) = (Set(foo0, foo1) -- partitionsOfM).head

    // N is assigned the freed partition.
    TestUtils.waitUntilTrue(() => {
      responseN = consumerGroupHeartbeat(
        groupId = groupId,
        memberId = memberIdN,
        memberEpoch = responseN.memberEpoch
      )
      assignedTopicPartitions(responseN).contains(Set((topicId, movedPartition)))
    }, msg = s"N could not get foo-$movedPartition. Last response $responseN.")
    assertEquals(newEpoch, responseN.memberEpoch)

    // M's callback returns and M reports that it owns {foo-0, foo-1}. The coordinator
    // accepts the report although the moved partition is assigned to N.
    responseM = consumerGroupHeartbeat(
      groupId = groupId,
      memberId = memberIdM,
      memberEpoch = newEpoch,
      topicPartitions = ownedTopicPartitions(foo0, foo1)
    )
    assertEquals(newEpoch, responseM.memberEpoch)

    // N commits the moved partition's offset. M then revokes the moved partition and
    // commits its offset at its current epoch. M's commit is accepted because the epoch
    // matches, and it overwrites N's offset.
    commitOffset(
      groupId = groupId,
      memberId = memberIdN,
      memberEpoch = newEpoch,
      topic = "foo",
      topicId = topicId,
      partition = movedPartition,
      offset = 100L,
      expectedError = Errors.NONE
    )
    commitOffset(
      groupId = groupId,
      memberId = memberIdM,
      memberEpoch = newEpoch,
      topic = "foo",
      topicId = topicId,
      partition = movedPartition,
      offset = 50L,
      expectedError = Errors.NONE
    )
    assertEquals(50L, fetchOffset(groupId, "foo", movedPartition))
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "5000")
    )
  )
  def testCommitAcceptedForPartitionRevokedOnUnsubscribe(): Unit = {
    // This test reproduces the following sequence, in which a member commits the offset of a
    // partition that it no longer holds and the commit is accepted:
    // 1. M is STABLE at epoch 10 with {foo-0, bar-0}. The group's last target assignment was
    //    computed less than the assignment interval ago, so a new one can't be computed yet.
    // 2. The app unsubscribes from bar. The heartbeat bumps the group epoch to 11, but the
    //    target stays at epoch 10. The coordinator moves bar-0 to the revocation set. M stays
    //    at epoch 10 in UNREVOKED_PARTITIONS, and the response carries {foo-0}.
    // 3. M revokes bar-0, committing its offset at epoch 10 as part of the revocation, which is
    //    correct. M reports {foo-0}. The target is still at epoch 10, so M stays at epoch 10,
    //    STABLE with {foo-0}. bar-0 is freed.
    // 4. N joins, subscribed to bar, which bumps the group epoch to 12. A later heartbeat
    //    computes the target at epoch 12, which gives bar-0 to N. N gets bar-0 at epoch 12.
    // 5. M is still at epoch 10 until its next heartbeat. If M commits bar-0's offset now, for
    //    example with a late commitAsync or an explicit offset map, it sends epoch 10. That
    //    matches M's epoch, so the commit validation accepts it without checking bar-0. The
    //    commit is accepted and can overwrite N's progress. M isn't fenced.
    //
    // The epochs in the test differ from the ones above. The assignment interval is set to
    // 5 seconds so that steps 2 and 3 happen before the next target assignment is computed.
    createOffsetsTopic()

    val groupId = "test-unsubscribe-commit-grp"
    val memberIdM = Uuid.randomUuid().toString
    val memberIdN = Uuid.randomUuid().toString
    val rebalanceTimeoutMs = 5 * 60 * 1000

    val fooTopicId = createTopic(topic = "foo", numPartitions = 1)
    val barTopicId = createTopic(topic = "bar", numPartitions = 1)
    val foo0 = (fooTopicId, 0)
    val bar0 = (barTopicId, 0)

    // M joins, subscribed to foo and bar, and is assigned {foo-0, bar-0}.
    var responseM: ConsumerGroupHeartbeatResponseData = null
    TestUtils.waitUntilTrue(() => {
      responseM = consumerGroupHeartbeat(
        groupId = groupId,
        memberId = memberIdM,
        memberEpoch = 0,
        rebalanceTimeoutMs = rebalanceTimeoutMs,
        subscribedTopicNames = List("foo", "bar"),
        topicPartitions = ownedTopicPartitions()
      )
      assignedTopicPartitions(responseM).contains(Set(foo0, bar0))
    }, msg = s"M could not get {foo-0, bar-0}. Last response $responseM.")
    val epochM = responseM.memberEpoch

    // M reports that it owns {foo-0, bar-0}.
    responseM = consumerGroupHeartbeat(
      groupId = groupId,
      memberId = memberIdM,
      memberEpoch = epochM,
      topicPartitions = ownedTopicPartitions(foo0, bar0)
    )
    assertEquals(epochM, responseM.memberEpoch)

    // M unsubscribes from bar. The target assignment can't be computed yet, so M stays at
    // its epoch and is asked to revoke bar-0.
    responseM = consumerGroupHeartbeat(
      groupId = groupId,
      memberId = memberIdM,
      memberEpoch = epochM,
      subscribedTopicNames = List("foo")
    )
    assertEquals(epochM, responseM.memberEpoch)
    assertEquals(Some(Set(foo0)), assignedTopicPartitions(responseM))

    // M revokes bar-0, committing its offset as part of the revocation, and reports {foo-0}.
    // The target assignment hasn't changed, so M stays at its epoch, and bar-0 is freed.
    commitOffset(
      groupId = groupId,
      memberId = memberIdM,
      memberEpoch = epochM,
      topic = "bar",
      topicId = barTopicId,
      partition = 0,
      offset = 10L,
      expectedError = Errors.NONE
    )
    responseM = consumerGroupHeartbeat(
      groupId = groupId,
      memberId = memberIdM,
      memberEpoch = epochM,
      topicPartitions = ownedTopicPartitions(foo0)
    )
    assertEquals(epochM, responseM.memberEpoch)

    // N joins, subscribed to bar. Once the assignment interval has elapsed, a heartbeat of N
    // computes the new target assignment, which gives bar-0 to N.
    var responseN: ConsumerGroupHeartbeatResponseData = null
    var epochN = 0
    TestUtils.waitUntilTrue(() => {
      responseN = consumerGroupHeartbeat(
        groupId = groupId,
        memberId = memberIdN,
        memberEpoch = epochN,
        rebalanceTimeoutMs = rebalanceTimeoutMs,
        subscribedTopicNames = List("bar"),
        topicPartitions = ownedTopicPartitions()
      )
      epochN = responseN.memberEpoch
      assignedTopicPartitions(responseN).contains(Set(bar0))
    }, msg = s"N could not get bar-0. Last response $responseN.")
    assertTrue(epochN > epochM, s"Expected N to be past epoch $epochM. Last response $responseN.")

    // M hasn't sent a heartbeat since, so it is still at its epoch, with {foo-0}.
    val memberM = consumerGroupDescribe(List(groupId)).head.members.asScala.find(_.memberId == memberIdM).get
    assertEquals(epochM, memberM.memberEpoch)
    assertEquals(
      Set(foo0),
      memberM.assignment.topicPartitions.asScala.flatMap { topicPartitions =>
        topicPartitions.partitions.asScala.map(partition => (topicPartitions.topicId, partition.intValue))
      }.toSet
    )

    // N commits bar-0's offset. M then commits bar-0's offset at its epoch. The epoch matches
    // M's epoch, so the commit is accepted although M no longer holds bar-0, and it overwrites
    // N's offset.
    commitOffset(
      groupId = groupId,
      memberId = memberIdN,
      memberEpoch = epochN,
      topic = "bar",
      topicId = barTopicId,
      partition = 0,
      offset = 100L,
      expectedError = Errors.NONE
    )
    commitOffset(
      groupId = groupId,
      memberId = memberIdM,
      memberEpoch = epochM,
      topic = "bar",
      topicId = barTopicId,
      partition = 0,
      offset = 50L,
      expectedError = Errors.NONE
    )
    assertEquals(50L, fetchOffset(groupId, "bar", 0))
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "0")
    )
  )
  def testStaticMembersRejoinWithDifferentServerAssignor(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    val groupId = "grp"
    val instanceIds = List("instance-1", "instance-2", "instance-3")

    // A helper that joins a static member with the given server assignor and waits until
    // the heartbeat returns a successful response. Returns the member epoch from the response.
    def joinStaticMember(memberId: String, instanceId: String, serverAssignor: String): Int = {
      val joinRequest = new ConsumerGroupHeartbeatRequest.Builder(
        new ConsumerGroupHeartbeatRequestData()
          .setGroupId(groupId)
          .setMemberId(memberId)
          .setInstanceId(instanceId)
          .setMemberEpoch(0)
          .setRebalanceTimeoutMs(5 * 60 * 1000)
          .setServerAssignor(serverAssignor)
          .setSubscribedTopicNames(List("foo").asJava)
          .setTopicPartitions(List.empty.asJava)
      ).build()

      var response: ConsumerGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        response = connectAndReceive[ConsumerGroupHeartbeatResponse](joinRequest)
        response.data.errorCode == Errors.NONE.code
      }, msg = s"Static member $instanceId could not join the group. Last response $response.")
      response.data.memberEpoch
    }

    // Three static members join the group with the "uniform" assignor.
    val initialMemberIds = instanceIds.map(_ => Uuid.randomUuid.toString)
    val initialEpochs = initialMemberIds.zip(instanceIds).map { case (memberId, instanceId) =>
      joinStaticMember(memberId, instanceId, "uniform")
    }
    val epochBeforeRejoin = initialEpochs.last

    // All three members leave the group.
    initialMemberIds.zip(instanceIds).foreach { case (memberId, instanceId) =>
      val leaveRequest = new ConsumerGroupHeartbeatRequest.Builder(
        new ConsumerGroupHeartbeatRequestData()
          .setGroupId(groupId)
          .setMemberId(memberId)
          .setInstanceId(instanceId)
          .setMemberEpoch(-2)
      ).build()
      val leaveResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](leaveRequest)
      assertEquals(Errors.NONE.code, leaveResponse.data.errorCode)
      assertEquals(-2, leaveResponse.data.memberEpoch)
    }

    // All three members rejoin with the "range" assignor and new member ids. The group
    // epoch must be bumped once a majority of members has switched assignor so that the
    // target assignment is recomputed with the new assignor.
    val rejoinEpochs = instanceIds.map { instanceId =>
      joinStaticMember(Uuid.randomUuid.toString, instanceId, "range")
    }

    // The last rejoin epoch must be greater than the epoch before the leave/rejoin cycle,
    // confirming that the group epoch was bumped and the assignment was recomputed.
    val lastRejoinEpoch = rejoinEpochs.last
    assertTrue(lastRejoinEpoch > epochBeforeRejoin,
      s"Expected the last rejoin epoch ($lastRejoinEpoch) to be greater than " +
      s"the epoch before the rejoin ($epochBeforeRejoin). " +
      s"Rejoin epochs: ${rejoinEpochs.mkString(", ")}.")
  }
}
