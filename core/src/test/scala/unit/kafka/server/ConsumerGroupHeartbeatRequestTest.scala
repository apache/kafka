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

import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.junit.RaftClusterInvocationContext.RaftClusterInstance
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.common.message.{ConsumerGroupHeartbeatRequestData, ConsumerGroupHeartbeatResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertNotNull}
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.ExtendWith

import java.util.Collections
import java.util.stream.Collectors
import scala.jdk.CollectionConverters._
import scala.util.Using

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.ALL, brokers = 1)
@Tag("integration")
class ConsumerGroupHeartbeatRequestTest(cluster: ClusterInstance) {

  @ClusterTest()
  def testConsumerGroupHeartbeatIsAccessibleWhenEnabled(): Unit = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
    ).build()

    val consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)
    val expectedResponse = new ConsumerGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    assertEquals(expectedResponse, consumerGroupHeartbeatResponse.data)
  }

  @ClusterTest(clusterType = Type.KRAFT, serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testConsumerGroupHeartbeatIsAccessibleWhenNewGroupCoordinatorIsEnabled(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    Using(Admin.create(cluster.adminConfigs(Collections.emptyMap()))) { admin =>

      // Creates the __consumer_offsets topics because it won't be created automatically
      // in this test because it does not use FindCoordinator API.
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = raftCluster.brokers.collect(Collectors.toList[BrokerServer]).asScala,
        controllers = raftCluster.controllerServers().asScala.toSeq
      )


      // Heartbeat request to join the group. Note that the member subscribes
      // to an nonexistent topic.
      var consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
        new ConsumerGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setMemberEpoch(0)
          .setRebalanceTimeoutMs(5 * 60 * 1000)
          .setSubscribedTopicNames(List("foo").asJava)
          .setTopicPartitions(List.empty.asJava)
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
          .setMemberEpoch(-1)
      ).build()

      consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)

      // Verify the response.
      assertEquals(-1, consumerGroupHeartbeatResponse.data.memberEpoch)
    }
  }

  @ClusterTest(clusterType = Type.KRAFT, serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testRejoiningStaticMemberGetsAssignmentsBackWhenNewGroupCoordinatorIsEnabled(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    Using(Admin.create(cluster.adminConfigs(Collections.emptyMap()))) { admin =>
      val instanceId = "instanceId"

      // Creates the __consumer_offsets topics because it won't be created automatically
      // in this test because it does not use FindCoordinator API.
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = raftCluster.brokers.collect(Collectors.toList[BrokerServer]).asScala,
        controllers = raftCluster.controllerServers().asScala.toSeq
      )

      // Heartbeat request so that a static member joins the group
      var consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
        new ConsumerGroupHeartbeatRequestData()
          .setGroupId("grp")
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
          .setMemberEpoch(-2)
      ).build()

      consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)

      // Verify the response.
      assertEquals(-2, consumerGroupHeartbeatResponse.data.memberEpoch)

      // Another static member replaces the above member. It gets the same assignments back
      consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
        new ConsumerGroupHeartbeatRequestData()
          .setGroupId("grp")
          .setInstanceId(instanceId)
          .setMemberEpoch(0)
          .setRebalanceTimeoutMs(5 * 60 * 1000)
          .setSubscribedTopicNames(List("foo").asJava)
          .setTopicPartitions(List.empty.asJava)
      ).build()

      consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)

      // Verify the response.
      assertNotNull(consumerGroupHeartbeatResponse.data.memberId)
      assertEquals(2, consumerGroupHeartbeatResponse.data.memberEpoch)
      assertEquals(expectedAssignment, consumerGroupHeartbeatResponse.data.assignment)
      // The 2 member IDs should be different
      assertNotEquals(oldMemberId, consumerGroupHeartbeatResponse.data.memberId)
    }
  }

  @ClusterTest(clusterType = Type.KRAFT, serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    new ClusterConfigProperty(key = "group.consumer.session.timeout.ms", value = "5000"),
    new ClusterConfigProperty(key = "group.consumer.min.session.timeout.ms", value = "5000")
  ))
  def testStaticMemberRemovedAfterSessionTimeoutExpiryWhenNewGroupCoordinatorIsEnabled(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    Using(Admin.create(cluster.adminConfigs(Collections.emptyMap()))) { admin =>
      val instanceId = "instanceId"

      // Creates the __consumer_offsets topics because it won't be created automatically
      // in this test because it does not use FindCoordinator API.
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = raftCluster.brokers.collect(Collectors.toList[BrokerServer]).asScala,
        controllers = raftCluster.controllerServers().asScala.toSeq
      )

      // Heartbeat request to join the group. Note that the member subscribes
      // to an nonexistent topic.
      var consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
        new ConsumerGroupHeartbeatRequestData()
          .setGroupId("grp")
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
          .setInstanceId(instanceId)
          .setMemberEpoch(0)
          .setRebalanceTimeoutMs(5 * 60 * 1000)
          .setSubscribedTopicNames(List("foo").asJava)
          .setTopicPartitions(List.empty.asJava)
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
  }

  private def connectAndReceive(request: ConsumerGroupHeartbeatRequest): ConsumerGroupHeartbeatResponse = {
    IntegrationTestUtils.connectAndReceive[ConsumerGroupHeartbeatResponse](
      request,
      cluster.anyBrokerSocketServer(),
      cluster.clientListener()
    )
  }
}
