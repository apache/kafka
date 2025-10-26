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
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.common.config.ConfigResource
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertNull, assertTrue}

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
      var streamsGroupHeartbeatResponse: StreamsGroupHeartbeatResponseData = null
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId,
          memberEpoch = 0,
          rebalanceTimeoutMs = 1000,
          activeTasks = Option(streamsGroupHeartbeatResponse)
            .map(r => convertTaskIds(r.activeTasks()).asScala.toList)
            .getOrElse(List.empty),
          standbyTasks = Option(streamsGroupHeartbeatResponse)
            .map(r => convertTaskIds(r.standbyTasks()).asScala.toList)
            .getOrElse(List.empty),
          warmupTasks = Option(streamsGroupHeartbeatResponse)
            .map(r => convertTaskIds(r.warmupTasks()).asScala.toList)
            .getOrElse(List.empty),
          topology = topology
        )
        streamsGroupHeartbeatResponse.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Verify the heartbeat was successful
      assert(streamsGroupHeartbeatResponse != null, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId, streamsGroupHeartbeatResponse.memberId())
      assertEquals(1, streamsGroupHeartbeatResponse.memberEpoch())

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
      var streamsGroupHeartbeatResponse1: StreamsGroupHeartbeatResponseData = null
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse1 = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId1,
          memberEpoch = 0,
          rebalanceTimeoutMs = 1000,
          activeTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.activeTasks()).asScala.toList)
            .getOrElse(List.empty),
          standbyTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.standbyTasks()).asScala.toList)
            .getOrElse(List.empty),
          warmupTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.warmupTasks()).asScala.toList)
            .getOrElse(List.empty),
          topology = topology,
          processId = "process-1"
        )
        streamsGroupHeartbeatResponse1.errorCode == Errors.NONE.code()
      }, "First StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      val expectedChangelogTopic = s"$groupId-subtopology-1-changelog"
      val expectedRepartitionTopic = s"$groupId-subtopology-1-repartition"
      TestUtils.waitUntilTrue(() => {
        val topicNames = admin.listTopics().names().get()
        topicNames.contains(expectedChangelogTopic) && topicNames.contains(expectedRepartitionTopic)
      }, msg = s"Internal topics $expectedChangelogTopic or $expectedRepartitionTopic were not created")

    // Second member joins the group
    var streamsGroupHeartbeatResponse2: StreamsGroupHeartbeatResponseData = null
    TestUtils.waitUntilTrue(() => {
      streamsGroupHeartbeatResponse2 = streamsGroupHeartbeat(
        groupId = groupId,
        memberId = memberId2,
        memberEpoch = 0,
        rebalanceTimeoutMs = 1000,
        activeTasks = Option(streamsGroupHeartbeatResponse2)
          .map(r => convertTaskIds(r.activeTasks()).asScala.toList)
          .getOrElse(List.empty),
        standbyTasks = Option(streamsGroupHeartbeatResponse2)
          .map(r => convertTaskIds(r.standbyTasks()).asScala.toList)
          .getOrElse(List.empty),
        warmupTasks = Option(streamsGroupHeartbeatResponse2)
          .map(r => convertTaskIds(r.warmupTasks()).asScala.toList)
          .getOrElse(List.empty),
        topology = topology,
        processId = "process-2"
      )
      streamsGroupHeartbeatResponse2.errorCode == Errors.NONE.code()
    }, "Second StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Both members continue to send heartbeats with their assigned tasks
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse1 = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId1,
          memberEpoch = streamsGroupHeartbeatResponse1.memberEpoch(),
          rebalanceTimeoutMs = 1000,
          activeTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.activeTasks()).asScala.toList)
            .getOrElse(List.empty),
          standbyTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.standbyTasks()).asScala.toList)
            .getOrElse(List.empty),
          warmupTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.warmupTasks()).asScala.toList)
            .getOrElse(List.empty),
        )
        streamsGroupHeartbeatResponse1.errorCode == Errors.NONE.code()
      }, "First member rebalance heartbeat did not succeed within the timeout period.")

      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse2 = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId2,
          memberEpoch = streamsGroupHeartbeatResponse2.memberEpoch(),
          rebalanceTimeoutMs = 1000,
          activeTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.activeTasks()).asScala.toList)
            .getOrElse(List.empty),
          standbyTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.standbyTasks()).asScala.toList)
            .getOrElse(List.empty),
          warmupTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.warmupTasks()).asScala.toList)
            .getOrElse(List.empty)
        )
        streamsGroupHeartbeatResponse2.errorCode == Errors.NONE.code()
      }, "Second member rebalance heartbeat did not succeed within the timeout period.")

      // Verify initial state with no standby tasks
      assertEquals(0, streamsGroupHeartbeatResponse1.standbyTasks().size(), "Member 1 should have no standby tasks initially")
      assertEquals(0, streamsGroupHeartbeatResponse1.standbyTasks().size(), "Member 2 should have no standby tasks initially")

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
        streamsGroupHeartbeatResponse1 = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId1,
          memberEpoch = streamsGroupHeartbeatResponse1.memberEpoch(),
          rebalanceTimeoutMs = 1000,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty
        )
        streamsGroupHeartbeatResponse1.errorCode == Errors.NONE.code() &&
          streamsGroupHeartbeatResponse1.standbyTasks()!= null
      }, "First member heartbeat after config change did not succeed within the timeout period.")

      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse2 = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId2,
          memberEpoch = streamsGroupHeartbeatResponse2.memberEpoch(),
          rebalanceTimeoutMs = 1000,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty
        )
        streamsGroupHeartbeatResponse2.errorCode == Errors.NONE.code() &&
          streamsGroupHeartbeatResponse2.standbyTasks() != null
      }, "Second member heartbeat after config change did not succeed within the timeout period.")

      // Verify that at least one member has active tasks
      val member1HasActiveTasks = streamsGroupHeartbeatResponse1.activeTasks().size()
      val member2HasActiveTasks = streamsGroupHeartbeatResponse2.activeTasks().size()
      assertTrue(member1HasActiveTasks + member2HasActiveTasks > 0, "At least one member should have active tasks after config change")

      // Verify that at least one member has standby tasks
      val member1HasStandbyTasks = streamsGroupHeartbeatResponse1.standbyTasks().size()
      val member2HasStandbyTasks = streamsGroupHeartbeatResponse2.standbyTasks().size()
      assertTrue(member1HasStandbyTasks + member2HasStandbyTasks > 0, "At least one member should have standby tasks after config change")

      // With 2 members and streams.num.standby.replicas=1, each active task should have 1 standby
      val totalActiveTasks = member1HasActiveTasks + member2HasActiveTasks
      val totalStandbyTasks = member1HasStandbyTasks + member2HasStandbyTasks
      assertEquals(totalActiveTasks, totalStandbyTasks, "Each active task should have one standby")

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
      var streamsGroupHeartbeatResponse: StreamsGroupHeartbeatResponseData = null
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId,
          memberEpoch = 0,
          rebalanceTimeoutMs = 1000,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty,
          topology = topology
        )
        streamsGroupHeartbeatResponse.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      val memberEpoch = streamsGroupHeartbeatResponse.memberEpoch()
      assertEquals(1, memberEpoch)

      // Blocking the thread for 1 sec so that the session times out and the member needs to rejoin
      Thread.sleep(1000)

      // Prepare the next heartbeat which should fail due to member expiration
      TestUtils.waitUntilTrue(() => {
        val expiredResponse = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId,
          memberEpoch = memberEpoch,
          rebalanceTimeoutMs = 1000,
          activeTasks = convertTaskIds(streamsGroupHeartbeatResponse.activeTasks()).asScala.toList,
          standbyTasks = convertTaskIds(streamsGroupHeartbeatResponse.standbyTasks()).asScala.toList,
          warmupTasks = convertTaskIds(streamsGroupHeartbeatResponse.warmupTasks()).asScala.toList,
          expectedError = Errors.UNKNOWN_MEMBER_ID
        )
        expiredResponse.errorCode == Errors.UNKNOWN_MEMBER_ID.code() &&
          expiredResponse.memberEpoch() == 0 &&
          expiredResponse.errorMessage().equals(s"Member $memberId is not a member of group $groupId.")
      }, "Member should have been expired because of the timeout.")

      // Member sends heartbeat again to join the streams group
      var rejoinHeartbeatResponse: StreamsGroupHeartbeatResponseData = null
      TestUtils.waitUntilTrue(() => {
        rejoinHeartbeatResponse = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId,
          memberEpoch = 0,
          rebalanceTimeoutMs = 1000,
          activeTasks = Option(rejoinHeartbeatResponse)
            .map(r => convertTaskIds(r.activeTasks()).asScala.toList)
            .getOrElse(List.empty),
          standbyTasks = Option(rejoinHeartbeatResponse)
            .map(r => convertTaskIds(r.standbyTasks()).asScala.toList)
            .getOrElse(List.empty),
          warmupTasks = Option(rejoinHeartbeatResponse)
            .map(r => convertTaskIds(r.warmupTasks()).asScala.toList)
            .getOrElse(List.empty),
          topology = topology
        )
        rejoinHeartbeatResponse.errorCode == Errors.NONE.code()
      }, "Member rejoin did not succeed within the timeout period.")

      // Verify the response for rejoined member
      assert(rejoinHeartbeatResponse != null, "Rejoin StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId, rejoinHeartbeatResponse.memberId())
      assertTrue(rejoinHeartbeatResponse.memberEpoch() > memberEpoch, "Epoch should have been bumped when member rejoined")
      val expectedActiveTasks = List(
        new StreamsGroupHeartbeatResponseData.TaskIds()
          .setSubtopologyId("subtopology-1")
          .setPartitions(List(0, 1).map(_.asInstanceOf[Integer]).asJava)
      ).asJava
      assertEquals(expectedActiveTasks, rejoinHeartbeatResponse.activeTasks())
      assertEquals(0, Option(rejoinHeartbeatResponse.standbyTasks()).map(_.size()).getOrElse(0))
      assertEquals(0, Option(rejoinHeartbeatResponse.warmupTasks()).map(_.size()).getOrElse(0))

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
      var streamsGroupHeartbeatResponse: StreamsGroupHeartbeatResponseData = null
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId,
          memberEpoch = 0,
          rebalanceTimeoutMs = 1000,
          activeTasks = Option(streamsGroupHeartbeatResponse)
            .map(r => convertTaskIds(r.activeTasks()).asScala.toList)
            .getOrElse(List.empty),
          standbyTasks = Option(streamsGroupHeartbeatResponse)
            .map(r => convertTaskIds(r.standbyTasks()).asScala.toList)
            .getOrElse(List.empty),
          warmupTasks = Option(streamsGroupHeartbeatResponse)
            .map(r => convertTaskIds(r.warmupTasks()).asScala.toList)
            .getOrElse(List.empty),
          topology = topology
        )
        streamsGroupHeartbeatResponse.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Verify the response for member
      assert(streamsGroupHeartbeatResponse != null, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId, streamsGroupHeartbeatResponse.memberId())
      assertEquals(1, streamsGroupHeartbeatResponse.memberEpoch())
      assertNotNull(streamsGroupHeartbeatResponse.activeTasks())

      // Restart the only running broker
      val broker = cluster.brokers().values().iterator().next()
      cluster.shutdownBroker(broker.config.brokerId)
      cluster.startBroker(broker.config.brokerId)

      // Should receive no error and no assignment changes
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId,
          memberEpoch = streamsGroupHeartbeatResponse.memberEpoch(),
          rebalanceTimeoutMs = 1000,
          activeTasks = convertTaskIds(streamsGroupHeartbeatResponse.activeTasks()).asScala.toList,
          standbyTasks = convertTaskIds(streamsGroupHeartbeatResponse.standbyTasks()).asScala.toList,
          warmupTasks = convertTaskIds(streamsGroupHeartbeatResponse.warmupTasks()).asScala.toList
        )
        streamsGroupHeartbeatResponse.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest after broker restart did not succeed within the timeout period.")

      // Verify the response. Epoch should not have changed and null assignments determine that no
      // change in old assignment
      assertEquals(memberId, streamsGroupHeartbeatResponse.memberId())
      assertEquals(1, streamsGroupHeartbeatResponse.memberEpoch())
      assertNull(streamsGroupHeartbeatResponse.activeTasks())
      assertNull(streamsGroupHeartbeatResponse.standbyTasks())
      assertNull(streamsGroupHeartbeatResponse.warmupTasks())

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