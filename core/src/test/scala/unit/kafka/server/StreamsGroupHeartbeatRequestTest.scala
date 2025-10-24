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
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.message.{StreamsGroupHeartbeatRequestData, StreamsGroupHeartbeatResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{StreamsGroupHeartbeatRequest, StreamsGroupHeartbeatResponse}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.common.config.ConfigResource
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertNull, assertTrue}

import java.util.Collections
import scala.jdk.CollectionConverters._

@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "true"),
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer,streams")
  )
)
class StreamsGroupHeartbeatRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest
  def testInternalTopicsCreation(): Unit = {
    val admin = cluster.admin()
    val memberId = "test-member-1"
    val groupId = "test-group"
    val inputTopicName = "input-topic"

    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )

      // Create input topic
      TestUtils.createTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq,
        topic = inputTopicName,
        numPartitions = 2
      )

      // Wait for topics to be available
      TestUtils.waitUntilTrue(() => {
        val topicNames = admin.listTopics().names().get()
        topicNames.contains(inputTopicName)
      }, msg = s"Input topic $inputTopicName is not available")

      // Create topology with internal topics (changelog and repartition topics)
      val topology = createTopologyWithInternalTopics(inputTopicName, groupId)

      // Send heartbeat with topology containing internal topics
      var streamsGroupHeartbeatResponse: StreamsGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequest.Builder(
          new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(1000)
            .setActiveTasks(Option(streamsGroupHeartbeatResponse)
              .map(r => convertTaskIds(r.data().activeTasks()))
              .getOrElse(Collections.emptyList()))
            .setStandbyTasks(Option(streamsGroupHeartbeatResponse)
              .map(r => convertTaskIds(r.data().standbyTasks()))
              .getOrElse(Collections.emptyList()))
            .setWarmupTasks(Option(streamsGroupHeartbeatResponse)
              .map(r => convertTaskIds(r.data().warmupTasks()))
              .getOrElse(Collections.emptyList()))
            .setTopology(topology)
        ).build(0)

        streamsGroupHeartbeatResponse = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest)
        streamsGroupHeartbeatResponse.data.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Verify the heartbeat was successful
      assert(streamsGroupHeartbeatResponse != null, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId, streamsGroupHeartbeatResponse.data.memberId())
      assertEquals(1, streamsGroupHeartbeatResponse.data.memberEpoch())

      // Wait for internal topics to be created
      val expectedChangelogTopic = s"$groupId-subtopology-1-changelog"
      val expectedRepartitionTopic = s"$groupId-subtopology-1-repartition"

      TestUtils.waitUntilTrue(() => {
        val topicNames = admin.listTopics().names().get()
        topicNames.contains(expectedChangelogTopic) && topicNames.contains(expectedRepartitionTopic)
      }, msg = s"Internal topics $expectedChangelogTopic and $expectedRepartitionTopic were not created")

      // Verify the internal topics exist and have correct properties
      val changelogTopicDescription = admin.describeTopics(java.util.Collections.singletonList(expectedChangelogTopic)).allTopicNames().get()
      val repartitionTopicDescription = admin.describeTopics(java.util.Collections.singletonList(expectedRepartitionTopic)).allTopicNames().get()

      assertTrue(changelogTopicDescription.containsKey(expectedChangelogTopic),
        s"Changelog topic $expectedChangelogTopic should exist")
      assertTrue(repartitionTopicDescription.containsKey(expectedRepartitionTopic),
        s"Repartition topic $expectedRepartitionTopic should exist")

      // Verify topic configurations
      val changelogTopic = changelogTopicDescription.get(expectedChangelogTopic)
      val repartitionTopic = repartitionTopicDescription.get(expectedRepartitionTopic)

      // Both topics should have 2 partitions (matching the input topic)
      assertEquals(2, changelogTopic.partitions().size(),
        s"Changelog topic should have 2 partitions, but has ${changelogTopic.partitions().size()}")
      assertEquals(2, repartitionTopic.partitions().size(),
        s"Repartition topic should have 2 partitions, but has ${repartitionTopic.partitions().size()}")

      // Verify replication factor
      assertEquals(1, changelogTopic.partitions().get(0).replicas().size(),
        s"Changelog topic should have replication factor 1")
      assertEquals(1, repartitionTopic.partitions().get(0).replicas().size(),
        s"Repartition topic should have replication factor 1")
    } finally {
      admin.close()
    }
  }

  @ClusterTest
  def testDynamicGroupConfig(): Unit = {
    val admin = cluster.admin()
    val memberId1 = "test-member-1"
    val memberId2 = "test-member-2"
    val groupId = "test-group"
    val topicName = "test-topic"

    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )

      // Create topic
      TestUtils.createTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq,
        topic = topicName,
        numPartitions = 2
      )
      // Wait for topic to be available
      TestUtils.waitUntilTrue(() => {
        admin.listTopics().names().get().contains(topicName)
      }, msg = s"Topic $topicName is not available to the group coordinator")

      val topology = createTopologyWithInternalTopics(topicName, groupId)

      // First member joins the group
      var streamsGroupHeartbeatResponse1: StreamsGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        val streamsGroupHeartbeatRequest1 = new StreamsGroupHeartbeatRequest.Builder(
          new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(1000)
            .setProcessId("process-1")
            .setActiveTasks(Option(streamsGroupHeartbeatResponse1)
              .map(r => convertTaskIds(r.data().activeTasks()))
              .getOrElse(Collections.emptyList()))
            .setStandbyTasks(Option(streamsGroupHeartbeatResponse1)
              .map(r => convertTaskIds(r.data().standbyTasks()))
              .getOrElse(Collections.emptyList()))
            .setWarmupTasks(Option(streamsGroupHeartbeatResponse1)
              .map(r => convertTaskIds(r.data().warmupTasks()))
              .getOrElse(Collections.emptyList()))
            .setTopology(topology)
        ).build(0)

        streamsGroupHeartbeatResponse1 = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest1)
        streamsGroupHeartbeatResponse1.data.errorCode == Errors.NONE.code()
      }, "First StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      val expectedChangelogTopic = s"$groupId-subtopology-1-changelog"
      val expectedRepartitionTopic = s"$groupId-subtopology-1-repartition"
      TestUtils.waitUntilTrue(() => {
        val topicNames = admin.listTopics().names().get()
        topicNames.contains(expectedChangelogTopic) && topicNames.contains(expectedRepartitionTopic)
      }, msg = s"Internal topics $expectedChangelogTopic or $expectedRepartitionTopic were not created")

    // Second member joins the group
    var streamsGroupHeartbeatResponse2: StreamsGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      val streamsGroupHeartbeatRequest2 = new StreamsGroupHeartbeatRequest.Builder(
        new StreamsGroupHeartbeatRequestData()
          .setGroupId(groupId)
          .setMemberId(memberId2)
          .setMemberEpoch(0)
          .setRebalanceTimeoutMs(1000)
          .setProcessId("process-2")
          .setActiveTasks(Option(streamsGroupHeartbeatResponse2)
            .map(r => convertTaskIds(r.data().activeTasks()))
            .getOrElse(Collections.emptyList()))
          .setStandbyTasks(Option(streamsGroupHeartbeatResponse2)
            .map(r => convertTaskIds(r.data().standbyTasks()))
            .getOrElse(Collections.emptyList()))
          .setWarmupTasks(Option(streamsGroupHeartbeatResponse2)
            .map(r => convertTaskIds(r.data().warmupTasks()))
            .getOrElse(Collections.emptyList()))
          .setTopology(topology)
      ).build(0)

      streamsGroupHeartbeatResponse2 = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest2)
      streamsGroupHeartbeatResponse2.data.errorCode == Errors.NONE.code()
    }, "Second StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Both members continue to send heartbeats with their assigned tasks
      TestUtils.waitUntilTrue(() => {
        val streamsGroupHeartbeatRequest1 = new StreamsGroupHeartbeatRequest.Builder(
          new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(streamsGroupHeartbeatResponse1.data.memberEpoch())
            .setRebalanceTimeoutMs(1000)
            .setActiveTasks(convertTaskIds(streamsGroupHeartbeatResponse1.data.activeTasks()))
            .setStandbyTasks(convertTaskIds(streamsGroupHeartbeatResponse1.data.standbyTasks()))
            .setWarmupTasks(convertTaskIds(streamsGroupHeartbeatResponse1.data.warmupTasks()))
            .setTopology(topology)
        ).build(0)

        streamsGroupHeartbeatResponse1 = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest1)
        streamsGroupHeartbeatResponse1.data.errorCode == Errors.NONE.code()
      }, "First member rebalance heartbeat did not succeed within the timeout period.")

      TestUtils.waitUntilTrue(() => {
        val streamsGroupHeartbeatRequest2 = new StreamsGroupHeartbeatRequest.Builder(
          new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(streamsGroupHeartbeatResponse2.data.memberEpoch())
            .setRebalanceTimeoutMs(1000)
            .setActiveTasks(convertTaskIds(streamsGroupHeartbeatResponse2.data.activeTasks()))
            .setStandbyTasks(convertTaskIds(streamsGroupHeartbeatResponse2.data.standbyTasks()))
            .setWarmupTasks(convertTaskIds(streamsGroupHeartbeatResponse2.data.warmupTasks()))
            .setTopology(topology)
        ).build(0)

        streamsGroupHeartbeatResponse2 = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest2)
        streamsGroupHeartbeatResponse2.data.errorCode == Errors.NONE.code()
      }, "Second member rebalance heartbeat did not succeed within the timeout period.")

    // Verify initial state with no standby tasks
    assertEquals(0, streamsGroupHeartbeatResponse1.data.standbyTasks().size(), "Member 1 should have no standby tasks initially")
    assertEquals(0, streamsGroupHeartbeatResponse2.data.standbyTasks().size(), "Member 2 should have no standby tasks initially")

      // Change streams.num.standby.replicas = 1
      val groupConfigResource = new ConfigResource(ConfigResource.Type.GROUP, groupId)
      val alterConfigOp = new AlterConfigOp(
        new ConfigEntry("streams.num.standby.replicas", "1"),
        AlterConfigOp.OpType.SET
      )
      val configChanges = Map(groupConfigResource -> List(alterConfigOp).asJavaCollection).asJava
      val options = new org.apache.kafka.clients.admin.AlterConfigsOptions()
      admin.incrementalAlterConfigs(configChanges, options).all().get()

      // Send heartbeats to trigger rebalance after config change
      TestUtils.waitUntilTrue(() => {
        val streamsGroupHeartbeatRequest1 = new StreamsGroupHeartbeatRequest.Builder(
          new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(streamsGroupHeartbeatResponse1.data.memberEpoch())
            .setRebalanceTimeoutMs(1000)
            .setActiveTasks(Collections.emptyList())
            .setStandbyTasks(Collections.emptyList())
            .setWarmupTasks(Collections.emptyList())
        ).build(0)

        streamsGroupHeartbeatResponse1 = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest1)
        streamsGroupHeartbeatResponse1.data.errorCode == Errors.NONE.code() &&
          streamsGroupHeartbeatResponse1.data.standbyTasks()!= null
      }, "First member heartbeat after config change did not succeed within the timeout period.")

      TestUtils.waitUntilTrue(() => {
        val streamsGroupHeartbeatRequest2 = new StreamsGroupHeartbeatRequest.Builder(
          new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(streamsGroupHeartbeatResponse2.data.memberEpoch())
            .setRebalanceTimeoutMs(1000)
            .setActiveTasks(Collections.emptyList())
            .setStandbyTasks(Collections.emptyList())
            .setWarmupTasks(Collections.emptyList())
        ).build(0)

        streamsGroupHeartbeatResponse2 = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest2)
        streamsGroupHeartbeatResponse2.data.errorCode == Errors.NONE.code() &&
          streamsGroupHeartbeatResponse2.data.standbyTasks() != null
      }, "Second member heartbeat after config change did not succeed within the timeout period.")

      // Verify that at least one member has active tasks
      val member1HasActive = streamsGroupHeartbeatResponse1.data.activeTasks().size() > 0
      val member2HasActive = streamsGroupHeartbeatResponse2.data.activeTasks().size() > 0
      assertTrue(member1HasActive || member2HasActive, "At least one member should have active tasks after config change")

      // Verify that at least one member has standby tasks
      val member1HasStandby = streamsGroupHeartbeatResponse1.data.standbyTasks().size() > 0
      val member2HasStandby = streamsGroupHeartbeatResponse2.data.standbyTasks().size() > 0
      assertTrue(member1HasStandby || member2HasStandby, "At least one member should have standby tasks after config change")

    } finally {
      admin.close()
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = "group.streams.heartbeat.interval.ms", value = "500"),
      new ClusterConfigProperty(key = "group.streams.min.heartbeat.interval.ms", value = "500"),
      new ClusterConfigProperty(key = "group.streams.session.timeout.ms", value = "501"),
      new ClusterConfigProperty(key = "group.streams.min.session.timeout.ms", value = "501")
    )
  )
  def testMemberJoiningAndExpiring(): Unit = {
    val admin = cluster.admin()
    val memberId = "test-member-1"
    val groupId = "test-group"
    val topicName = "test-topic"

    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )

      // Create topic
      TestUtils.createTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq,
        topic = topicName,
        numPartitions = 2
      )
      TestUtils.waitUntilTrue(() => {
        admin.listTopics().names().get().contains(topicName)
      }, msg = s"Topic $topicName is not available to the group coordinator")

      val topology = createMockTopology(topicName)

      // First member joins the group
      val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequest.Builder(
        new StreamsGroupHeartbeatRequestData()
          .setGroupId(groupId)
          .setMemberId(memberId)
          .setMemberEpoch(0)
          .setRebalanceTimeoutMs(1000)
          .setActiveTasks(java.util.Collections.emptyList())
          .setStandbyTasks(java.util.Collections.emptyList())
          .setWarmupTasks(java.util.Collections.emptyList())
          .setTopology(topology)
      ).build(0)

      var streamsGroupHeartbeatResponse: StreamsGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest)
        streamsGroupHeartbeatResponse.data.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      val memberEpoch = streamsGroupHeartbeatResponse.data.memberEpoch()
      assertEquals(1, memberEpoch)

      // Blocking the thread for 1 sec so that the session times out and the member needs to rejoin
      Thread.sleep(1000)

      // Prepare the next heartbeat which should fail due to member expiration
      val expiredHeartbeatRequest = new StreamsGroupHeartbeatRequest.Builder(
        new StreamsGroupHeartbeatRequestData()
          .setGroupId(groupId)
          .setMemberId(memberId)
          .setMemberEpoch(memberEpoch)
          .setRebalanceTimeoutMs(1000)
          .setActiveTasks(convertTaskIds(streamsGroupHeartbeatResponse.data.activeTasks()))
          .setStandbyTasks(convertTaskIds(streamsGroupHeartbeatResponse.data.standbyTasks()))
          .setWarmupTasks(convertTaskIds(streamsGroupHeartbeatResponse.data.warmupTasks()))
      ).build(0)

      TestUtils.waitUntilTrue(() => {
        val expiredResponse = connectAndReceive[StreamsGroupHeartbeatResponse](expiredHeartbeatRequest)
        expiredResponse.data.errorCode == Errors.UNKNOWN_MEMBER_ID.code() &&
          expiredResponse.data.memberEpoch() == 0
          expiredResponse.data.errorMessage().equals(s"Member $memberId is not a member of group $groupId.")
      }, "Member should have been expired because of the timeout.")

      // Member sends heartbeat again to join the streams group
      var rejoinHeartbeatResponse: StreamsGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        val rejoinRequest = new StreamsGroupHeartbeatRequest.Builder(
          new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(1000)
            .setActiveTasks(Option(rejoinHeartbeatResponse)
              .map(r => convertTaskIds(r.data().activeTasks()))
              .getOrElse(Collections.emptyList()))
            .setStandbyTasks(Option(rejoinHeartbeatResponse)
              .map(r => convertTaskIds(r.data().standbyTasks()))
              .getOrElse(Collections.emptyList()))
            .setWarmupTasks(Option(rejoinHeartbeatResponse)
              .map(r => convertTaskIds(r.data().warmupTasks()))
              .getOrElse(Collections.emptyList()))
            .setTopology(topology)
        ).build(0)

        rejoinHeartbeatResponse = connectAndReceive[StreamsGroupHeartbeatResponse](rejoinRequest)
        rejoinHeartbeatResponse.data.errorCode == Errors.NONE.code()
      }, "Member rejoin did not succeed within the timeout period.")

      // Verify the response for rejoined member
      assert(rejoinHeartbeatResponse != null, "Rejoin StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId, rejoinHeartbeatResponse.data.memberId())
      assertTrue(rejoinHeartbeatResponse.data.memberEpoch() > memberEpoch, "Epoch should have been bumped when member rejoined")
      val expectedActiveTasks = List(
        new StreamsGroupHeartbeatResponseData.TaskIds()
          .setSubtopologyId("subtopology-1")
          .setPartitions(List(0, 1).map(_.asInstanceOf[Integer]).asJava)
      ).asJava
      assertEquals(expectedActiveTasks, rejoinHeartbeatResponse.data().activeTasks())
      assertEquals(0, rejoinHeartbeatResponse.data.standbyTasks().size())
      assertEquals(0, rejoinHeartbeatResponse.data.warmupTasks().size())

    } finally {
      admin.close()
    }
  }

  @ClusterTest
  def testGroupCoordinatorChange(): Unit = {
    val admin = cluster.admin()
    val memberId = "test-member-1"
    val groupId = "test-group"
    val topicName = "test-topic"

    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )

      // Create topic
      TestUtils.createTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq,
        topic = topicName,
        numPartitions = 2
      )
      TestUtils.waitUntilTrue(() => {
        admin.listTopics().names().get().contains(topicName)
      }, msg = s"Topic $topicName is not available to the group coordinator")

      val topology = createMockTopology(topicName)

      // First member joins the group
      var streamsGroupHeartbeatResponse: StreamsGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequest.Builder(
          new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(1000)
            .setActiveTasks(Option(streamsGroupHeartbeatResponse)
              .map(r => convertTaskIds(r.data().activeTasks()))
              .getOrElse(Collections.emptyList()))
            .setStandbyTasks(Option(streamsGroupHeartbeatResponse)
              .map(r => convertTaskIds(r.data().standbyTasks()))
              .getOrElse(Collections.emptyList()))
            .setWarmupTasks(Option(streamsGroupHeartbeatResponse)
              .map(r => convertTaskIds(r.data().warmupTasks()))
              .getOrElse(Collections.emptyList()))
            .setTopology(topology)
        ).build(0)

        streamsGroupHeartbeatResponse = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest)
        streamsGroupHeartbeatResponse.data.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Verify the response for member
      assert(streamsGroupHeartbeatResponse != null, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId, streamsGroupHeartbeatResponse.data.memberId())
      assertEquals(1, streamsGroupHeartbeatResponse.data.memberEpoch())
      assertNotNull(streamsGroupHeartbeatResponse.data().activeTasks())

      // Restart the only running broker
      val broker = cluster.brokers().values().iterator().next()
      cluster.shutdownBroker(broker.config.brokerId)
      cluster.startBroker(broker.config.brokerId)

      // Prepare the next heartbeat for member
      val streamsGroupHeartbeatRequestAfterRestart = new StreamsGroupHeartbeatRequest.Builder(
        new StreamsGroupHeartbeatRequestData()
          .setGroupId(groupId)
          .setMemberId(memberId)
          .setMemberEpoch(streamsGroupHeartbeatResponse.data.memberEpoch())
          .setRebalanceTimeoutMs(1000)
          .setActiveTasks(convertTaskIds(streamsGroupHeartbeatResponse.data.activeTasks()))
          .setStandbyTasks(convertTaskIds(streamsGroupHeartbeatResponse.data.standbyTasks()))
          .setWarmupTasks(convertTaskIds(streamsGroupHeartbeatResponse.data.warmupTasks()))
      ).build(0)

      // Should receive no error and no assignment changes
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequestAfterRestart)
        streamsGroupHeartbeatResponse.data.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest after broker restart did not succeed within the timeout period.")

      // Verify the response. Epoch should not have changed and null assignments determine that no
      // change in old assignment
      assertEquals(memberId, streamsGroupHeartbeatResponse.data.memberId())
      assertEquals(1, streamsGroupHeartbeatResponse.data.memberEpoch())
      assertNull(streamsGroupHeartbeatResponse.data.activeTasks())
      assertNull(streamsGroupHeartbeatResponse.data.standbyTasks())
      assertNull(streamsGroupHeartbeatResponse.data.warmupTasks())

    } finally {
      admin.close()
    }
  }

  private def convertTaskIds(responseTasks: java.util.List[StreamsGroupHeartbeatResponseData.TaskIds]): java.util.List[StreamsGroupHeartbeatRequestData.TaskIds] = {
    if (responseTasks == null) {
      java.util.Collections.emptyList()
    } else {
      responseTasks.asScala.map { responseTask =>
        new StreamsGroupHeartbeatRequestData.TaskIds()
          .setSubtopologyId(responseTask.subtopologyId)
          .setPartitions(responseTask.partitions)
      }.asJava
    }
  }

  private def createMockTopology(topicName: String): StreamsGroupHeartbeatRequestData.Topology = {
    new StreamsGroupHeartbeatRequestData.Topology()
      .setEpoch(1)
      .setSubtopologies(List(
        new StreamsGroupHeartbeatRequestData.Subtopology()
          .setSubtopologyId("subtopology-1")
          .setSourceTopics(List(topicName).asJava)
          .setRepartitionSinkTopics(List.empty.asJava)
          .setRepartitionSourceTopics(List.empty.asJava)
      ).asJava)
  }

  private def createTopologyWithInternalTopics(inputTopicName: String, groupId: String): StreamsGroupHeartbeatRequestData.Topology = {
    new StreamsGroupHeartbeatRequestData.Topology()
      .setEpoch(1)
      .setSubtopologies(List(
        new StreamsGroupHeartbeatRequestData.Subtopology()
          .setSubtopologyId("subtopology-1")
          .setSourceTopics(List(inputTopicName).asJava)
          .setRepartitionSinkTopics(List(
            s"$groupId-subtopology-1-repartition"
          ).asJava)
          .setRepartitionSourceTopics(List(
            new StreamsGroupHeartbeatRequestData.TopicInfo()
              .setName(s"$groupId-subtopology-1-repartition")
          ).asJava)
          .setStateChangelogTopics(List(
            new StreamsGroupHeartbeatRequestData.TopicInfo()
              .setName(s"$groupId-subtopology-1-changelog")
          ).asJava)
      ).asJava)
  }
}