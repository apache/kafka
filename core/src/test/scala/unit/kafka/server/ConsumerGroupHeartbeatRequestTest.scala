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

import org.apache.kafka.common.test.api.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterFeature, ClusterTest, Type}
import org.apache.kafka.common.test.api.ClusterTestExtensions
import org.apache.kafka.common.test.api.RaftClusterInvocationContext.RaftClusterInstance
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.message.{ConsumerGroupHeartbeatRequestData, ConsumerGroupHeartbeatResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse}
import org.apache.kafka.coordinator.group.{GroupConfig, GroupCoordinatorConfig}
import org.apache.kafka.server.common.Features
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertNotNull}
import org.junit.jupiter.api.extension.ExtendWith

import scala.collection.Map
import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class ConsumerGroupHeartbeatRequestTest(cluster: ClusterInstance) {

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testConsumerGroupHeartbeatIsInaccessibleWhenDisabledByStaticConfig(): Unit = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData(),
      true
    ).build()

    val consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)
    val expectedResponse = new ConsumerGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    assertEquals(expectedResponse, consumerGroupHeartbeatResponse.data)
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    ),
    features = Array(
      new ClusterFeature(feature = Features.GROUP_VERSION, version = 0)
    )
  )
  def testConsumerGroupHeartbeatIsInaccessibleWhenFeatureFlagNotEnabled(): Unit = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData(),
      true
    ).build()

    val consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)
    val expectedResponse = new ConsumerGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    assertEquals(expectedResponse, consumerGroupHeartbeatResponse.data)
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testConsumerGroupHeartbeatIsAccessibleWhenNewGroupCoordinatorIsEnabled(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    val admin = cluster.createAdminClient()

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    TestUtils.createOffsetsTopicWithAdmin(
      admin = admin,
      brokers = raftCluster.brokers.values().asScala.toSeq,
      controllers = raftCluster.controllers().values().asScala.toSeq
    )

    // Heartbeat request to join the group. Note that the member subscribes
    // to an nonexistent topic.
    var consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(Uuid.randomUuid.toString)
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
      consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $consumerGroupHeartbeatResponse.")

    // Verify the response.
    assertNotNull(consumerGroupHeartbeatResponse.data.memberId)
    assertEquals(1, consumerGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(new ConsumerGroupHeartbeatResponseData.Assignment(), consumerGroupHeartbeatResponse.data.assignment)

    // Create the topic.
    val topicId = TestUtils.createTopicWithAdminRaw(
      admin = admin,
      topic = "foo",
      numPartitions = 3
    )

    // Prepare the next heartbeat.
    consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(consumerGroupHeartbeatResponse.data.memberId)
        .setMemberEpoch(consumerGroupHeartbeatResponse.data.memberEpoch),
      true
    ).build()

    // This is the expected assignment.
    val expectedAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
      .setTopicPartitions(List(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)

    // Heartbeats until the partitions are assigned.
    consumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)
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
        .setMemberEpoch(-1),
      true
    ).build()

    consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)

    // Verify the response.
    assertEquals(-1, consumerGroupHeartbeatResponse.data.memberEpoch)
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testRejoiningStaticMemberGetsAssignmentsBackWhenNewGroupCoordinatorIsEnabled(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    val admin = cluster.createAdminClient()
    val instanceId = "instanceId"

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    TestUtils.createOffsetsTopicWithAdmin(
      admin = admin,
      brokers = raftCluster.brokers.values().asScala.toSeq,
      controllers = raftCluster.controllers().values().asScala.toSeq
    )

    // Heartbeat request so that a static member joins the group
    var consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(Uuid.randomUuid.toString)
        .setInstanceId(instanceId)
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
      consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Static member could not join the group successfully. Last response $consumerGroupHeartbeatResponse.")

    // Verify the response.
    assertNotNull(consumerGroupHeartbeatResponse.data.memberId)
    assertEquals(1, consumerGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(new ConsumerGroupHeartbeatResponseData.Assignment(), consumerGroupHeartbeatResponse.data.assignment)

    // Create the topic.
    val topicId = TestUtils.createTopicWithAdminRaw(
      admin = admin,
      topic = "foo",
      numPartitions = 3
    )

    // Prepare the next heartbeat.
    consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setInstanceId(instanceId)
        .setMemberId(consumerGroupHeartbeatResponse.data.memberId)
        .setMemberEpoch(consumerGroupHeartbeatResponse.data.memberEpoch),
      true
    ).build()

    // This is the expected assignment.
    val expectedAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
      .setTopicPartitions(List(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)

    // Heartbeats until the partitions are assigned.
    consumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)
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
        .setMemberEpoch(-2),
      true
    ).build()

    consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)

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
        .setTopicPartitions(List.empty.asJava),
      true
    ).build()

    consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)

    // Verify the response.
    assertNotNull(consumerGroupHeartbeatResponse.data.memberId)
    assertEquals(2, consumerGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(expectedAssignment, consumerGroupHeartbeatResponse.data.assignment)
    // The 2 member IDs should be different
    assertNotEquals(oldMemberId, consumerGroupHeartbeatResponse.data.memberId)
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, value = "5001"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "5001")
    )
  )
  def testStaticMemberRemovedAfterSessionTimeoutExpiryWhenNewGroupCoordinatorIsEnabled(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    val admin = cluster.createAdminClient()
    val instanceId = "instanceId"

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    TestUtils.createOffsetsTopicWithAdmin(
      admin = admin,
      brokers = raftCluster.brokers.values().asScala.toSeq,
      controllers = raftCluster.controllers().values().asScala.toSeq
    )

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
        .setTopicPartitions(List.empty.asJava),
      true
    ).build()

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var consumerGroupHeartbeatResponse: ConsumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $consumerGroupHeartbeatResponse.")

    // Verify the response.
    assertNotNull(consumerGroupHeartbeatResponse.data.memberId)
    assertEquals(1, consumerGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(new ConsumerGroupHeartbeatResponseData.Assignment(), consumerGroupHeartbeatResponse.data.assignment)

    // Create the topic.
    val topicId = TestUtils.createTopicWithAdminRaw(
      admin = admin,
      topic = "foo",
      numPartitions = 3
    )

    // Prepare the next heartbeat.
    consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setInstanceId(instanceId)
        .setMemberId(consumerGroupHeartbeatResponse.data.memberId)
        .setMemberEpoch(consumerGroupHeartbeatResponse.data.memberEpoch),
      true
    ).build()

    // This is the expected assignment.
    val expectedAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
      .setTopicPartitions(List(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)

    // Heartbeats until the partitions are assigned.
    consumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)
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
        .setTopicPartitions(List.empty.asJava),
      true
    ).build()

    // Validating that trying to join with an in-use instanceId would throw an UnreleasedInstanceIdException.
    consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)
    assertEquals(Errors.UNRELEASED_INSTANCE_ID.code, consumerGroupHeartbeatResponse.data.errorCode)

    // The new static member join group will keep failing with an UnreleasedInstanceIdException
    // until eventually it gets through because the existing member will be kicked out
    // because of not sending a heartbeat till session timeout expiry.
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        consumerGroupHeartbeatResponse.data.assignment == expectedAssignment
    }, msg = s"Could not re-join the group successfully. Last response $consumerGroupHeartbeatResponse.")

    // Verify the response. The group epoch bumps upto 4 which eventually reflects in the new member epoch.
    assertEquals(4, consumerGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(expectedAssignment, consumerGroupHeartbeatResponse.data.assignment)
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, value = "5000")
    )
  )
  def testUpdateConsumerGroupHeartbeatConfigSuccessful(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    val admin = cluster.createAdminClient()
    val newHeartbeatIntervalMs = 10000
    val instanceId = "instanceId"
    val consumerGroupId = "grp"

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    TestUtils.createOffsetsTopicWithAdmin(
      admin = admin,
      brokers = raftCluster.brokers.values().asScala.toSeq,
      controllers = raftCluster.controllers().values().asScala.toSeq
    )

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
        .setTopicPartitions(List.empty.asJava),
      true
    ).build()

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var consumerGroupHeartbeatResponse: ConsumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)
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
        .setMemberEpoch(consumerGroupHeartbeatResponse.data.memberEpoch),
      true
    ).build()

    // Verify the response. The heartbeat interval was updated.
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        newHeartbeatIntervalMs == consumerGroupHeartbeatResponse.data.heartbeatIntervalMs
    }, msg = s"Dynamic update consumer group config failed. Last response $consumerGroupHeartbeatResponse.")
  }

  private def connectAndReceive(request: ConsumerGroupHeartbeatRequest): ConsumerGroupHeartbeatResponse = {
    IntegrationTestUtils.connectAndReceive[ConsumerGroupHeartbeatResponse](
      request,
      cluster.anyBrokerSocketServer(),
      cluster.clientListener()
    )
  }
}
