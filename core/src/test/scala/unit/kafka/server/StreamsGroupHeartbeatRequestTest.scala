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
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.message.{StreamsGroupHeartbeatRequestData, StreamsGroupHeartbeatResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterFeature, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.server.common.Feature
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertNull, assertThrows, assertTrue}

import scala.jdk.CollectionConverters._

@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    new ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer,streams"),
    new ClusterConfigProperty(key = "group.streams.initial.rebalance.delay.ms", value = "0")
  )
)
class StreamsGroupHeartbeatRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic,consumer,streams"),
    )
  )
  def testStreamsGroupHeartbeatWithInvalidAPIVersion(): Unit = {
    // Test that invalid API version throws UnsupportedVersionException
    assertThrows(classOf[UnsupportedVersionException], () =>
      streamsGroupHeartbeat(
        groupId = "test-group",
        expectedError = Errors.UNSUPPORTED_VERSION,
        version = -1)
    )
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic,consumer,streams"),
    ),
    features = Array(
      new ClusterFeature(feature = Feature.STREAMS_VERSION, version = 0)
    )
  )
  def testStreamsGroupHeartbeatIsInaccessibleWhenDisabledByFeatureConfig(): Unit = {
    // Test with streams.version = 0, the API is disabled at server level
    val topology = new StreamsGroupHeartbeatRequestData.Topology()
      .setEpoch(1)
      .setSubtopologies(List().asJava)
    
    val streamsGroupHeartbeatResponse = streamsGroupHeartbeat(
      groupId = "test-group",
      memberId = "test-member",
      rebalanceTimeoutMs = 1000,
      activeTasks = List.empty,
      standbyTasks = List.empty,
      warmupTasks = List.empty,
      topology = topology,
      expectedError = Errors.UNSUPPORTED_VERSION,
    )
    
    val expectedResponse = new StreamsGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code())
    assertEquals(expectedResponse, streamsGroupHeartbeatResponse)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic,consumer"),
    )
  )
  def testStreamsGroupHeartbeatIsInaccessibleWhenDisabledByStaticGroupCoordinatorProtocolConfig(): Unit = {
    val topology = new StreamsGroupHeartbeatRequestData.Topology()
      .setEpoch(1)
      .setSubtopologies(List().asJava)
    
    val streamsGroupHeartbeatResponse = streamsGroupHeartbeat(
      groupId = "test-group",
      memberId = "test-member",
      rebalanceTimeoutMs = 1000,
      activeTasks = List.empty,
      standbyTasks = List.empty,
      warmupTasks = List.empty,
      topology = topology,
      expectedError = Errors.UNSUPPORTED_VERSION
    )

    val expectedResponse = new StreamsGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code())
    assertEquals(expectedResponse, streamsGroupHeartbeatResponse)
  }

  @ClusterTest
  def testStreamsGroupHeartbeatIsInaccessibleWhenOffsetTopicNotExist(): Unit = {
    val topology = new StreamsGroupHeartbeatRequestData.Topology()
      .setEpoch(1)
      .setSubtopologies(List().asJava)
    
    val streamsGroupHeartbeatResponse = streamsGroupHeartbeat(
      groupId = "test-group",
      memberId = "test-member",
      rebalanceTimeoutMs = 1000,
      activeTasks = List.empty,
      standbyTasks = List.empty,
      warmupTasks = List.empty,
      topology = topology,
      expectedError = Errors.NOT_COORDINATOR
    )

    val expectedResponse = new StreamsGroupHeartbeatResponseData().setErrorCode(Errors.NOT_COORDINATOR.code())
    assertEquals(expectedResponse, streamsGroupHeartbeatResponse)
  }

  @ClusterTest
  def testStreamsGroupHeartbeatIsAccessibleWhenNewGroupCoordinatorIsEnabledTopicNotExistFirst(): Unit = {
    val admin = cluster.admin()
    val memberId = "test-member"
    val groupId = "test-group"
    val topicName = "test-topic"

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )

      val topology = createMockTopology(topicName)

      // Heartbeat when topic does not exist
      var streamsGroupHeartbeatResponse: StreamsGroupHeartbeatResponseData = null
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId,
          rebalanceTimeoutMs = 1000,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty,
          topology = topology
        )
        streamsGroupHeartbeatResponse.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Verify the response
      assertNotNull(streamsGroupHeartbeatResponse, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId, streamsGroupHeartbeatResponse.memberId())
      assertEquals(2, streamsGroupHeartbeatResponse.memberEpoch())
      val expectedStatus = new StreamsGroupHeartbeatResponseData.Status()
        .setStatusCode(1)
        .setStatusDetail(s"Source topics $topicName are missing.")
      assertEquals(expectedStatus, streamsGroupHeartbeatResponse.status().get(0))

      // Create topic
      TestUtils.createTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq,
        topic = topicName,
        numPartitions = 3
      )
      // Wait for topic to be available
      TestUtils.waitUntilTrue(() => {
        admin.listTopics().names().get().contains(topicName)
      }, msg = s"Topic $topicName is not available to the group coordinator")

      // Heartbeat after topic is created
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId,
          rebalanceTimeoutMs = 1000,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty,
          topology = topology
        )
        streamsGroupHeartbeatResponse.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Active task assignment should be available
      assertNotNull(streamsGroupHeartbeatResponse, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId, streamsGroupHeartbeatResponse.memberId())
      assertEquals(3, streamsGroupHeartbeatResponse.memberEpoch())
      assertEquals(List.empty.asJava, streamsGroupHeartbeatResponse.status())
      val expectedActiveTasks = List(
        new StreamsGroupHeartbeatResponseData.TaskIds()
          .setSubtopologyId("subtopology-1")
          .setPartitions(List(0, 1, 2).map(_.asInstanceOf[Integer]).asJava)
      ).asJava
      assertEquals(expectedActiveTasks, streamsGroupHeartbeatResponse.activeTasks())
    } finally {
      admin.close()
    }
  }

  @ClusterTest
  def testStreamsGroupHeartbeatForMultipleMembers(): Unit = {
    val admin = cluster.admin()
    val memberId1 = "test-member-1"
    val memberId2 = "test-member-2"
    val groupId = "test-group"
    val topicName = "test-topic"

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
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
        numPartitions = 3
      )
      TestUtils.waitUntilTrue(() => {
        admin.listTopics().names().get().contains(topicName)
      }, msg = s"Topic $topicName is not available to the group coordinator")

      val topology = createMockTopology(topicName)

      // First member joins the group
      var streamsGroupHeartbeatResponse1: StreamsGroupHeartbeatResponseData = null
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse1 = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId1,
          rebalanceTimeoutMs = 1000,
          activeTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.activeTasks()))
            .getOrElse(List.empty),
          standbyTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.standbyTasks()))
            .getOrElse(List.empty),
          warmupTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.warmupTasks()))
            .getOrElse(List.empty),
          topology = topology
        )
        streamsGroupHeartbeatResponse1.errorCode == Errors.NONE.code()
      }, "First StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Verify first member gets all tasks initially
      assertNotNull(streamsGroupHeartbeatResponse1, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId1, streamsGroupHeartbeatResponse1.memberId())
      assertEquals(2, streamsGroupHeartbeatResponse1.memberEpoch())
      assertEquals(1, streamsGroupHeartbeatResponse1.activeTasks().size())
      assertEquals(3, streamsGroupHeartbeatResponse1.activeTasks().get(0).partitions().size())

      // Second member joins the group (should trigger a rebalance)
      var streamsGroupHeartbeatResponse2: StreamsGroupHeartbeatResponseData = null
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse2 = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId2,
          rebalanceTimeoutMs = 1000,
          activeTasks = Option(streamsGroupHeartbeatResponse2)
            .map(r => convertTaskIds(r.activeTasks()))
            .getOrElse(List.empty),
          standbyTasks = Option(streamsGroupHeartbeatResponse2)
            .map(r => convertTaskIds(r.standbyTasks()))
            .getOrElse(List.empty),
          warmupTasks = Option(streamsGroupHeartbeatResponse2)
            .map(r => convertTaskIds(r.warmupTasks()))
            .getOrElse(List.empty),
          topology = topology
        )
        streamsGroupHeartbeatResponse2.errorCode == Errors.NONE.code()
      }, "Second StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Verify second member gets assigned
      assertNotNull(streamsGroupHeartbeatResponse2, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId2, streamsGroupHeartbeatResponse2.memberId())
      assertEquals(3, streamsGroupHeartbeatResponse2.memberEpoch())

      // Wait for both members to get their task assignments by sending heartbeats
      // until they both have non-null activeTasks
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse1 = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId1,
          memberEpoch = streamsGroupHeartbeatResponse1.memberEpoch(),
          rebalanceTimeoutMs = 1000,
          activeTasks = convertTaskIds(streamsGroupHeartbeatResponse1.activeTasks()),
          standbyTasks = convertTaskIds(streamsGroupHeartbeatResponse1.standbyTasks()),
          warmupTasks = convertTaskIds(streamsGroupHeartbeatResponse1.warmupTasks())
        )
        streamsGroupHeartbeatResponse1.errorCode == Errors.NONE.code() && 
        streamsGroupHeartbeatResponse1.activeTasks() != null
      }, "First member did not get task assignment within the timeout period.")

      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse2 = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId2,
          memberEpoch = streamsGroupHeartbeatResponse2.memberEpoch(),
          rebalanceTimeoutMs = 1000,
          activeTasks = convertTaskIds(streamsGroupHeartbeatResponse2.activeTasks()),
          standbyTasks = convertTaskIds(streamsGroupHeartbeatResponse2.standbyTasks()),
          warmupTasks = convertTaskIds(streamsGroupHeartbeatResponse2.warmupTasks())
        )
        streamsGroupHeartbeatResponse2.errorCode == Errors.NONE.code() && 
        streamsGroupHeartbeatResponse2.activeTasks() != null
      }, "Second member did not get task assignment within the timeout period.")


      // Verify both members should have tasks assigned
      assertNotNull(streamsGroupHeartbeatResponse1, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId1, streamsGroupHeartbeatResponse1.memberId())
      
      assertNotNull(streamsGroupHeartbeatResponse2, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId2, streamsGroupHeartbeatResponse2.memberId())

      // At least one member should have active tasks
      val totalActiveTasks = streamsGroupHeartbeatResponse1.activeTasks().size() + streamsGroupHeartbeatResponse2.activeTasks().size()
      assertTrue(totalActiveTasks > 0, "At least one member should have active tasks")

    } finally {
      admin.close()
    }
  }

  @ClusterTest
  def testEmptyStreamsGroupId(): Unit = {
    val admin = cluster.admin()

    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )

      val topology = new StreamsGroupHeartbeatRequestData.Topology()
        .setEpoch(1)
        .setSubtopologies(List().asJava)

      val streamsGroupHeartbeatResponse = streamsGroupHeartbeat(
        groupId = "",  // Empty group ID
        memberId = "test-member",
        rebalanceTimeoutMs = 1000,
        activeTasks = List.empty,
        standbyTasks = List.empty,
        warmupTasks = List.empty,
        topology = topology,
        expectedError = Errors.INVALID_REQUEST
      )
      
      val expectedResponse = new StreamsGroupHeartbeatResponseData()
        .setErrorCode(Errors.INVALID_REQUEST.code())
        .setErrorMessage("GroupId can't be empty.")
      assertEquals(expectedResponse, streamsGroupHeartbeatResponse)
    } finally {
      admin.close()
    }
  }

  @ClusterTest
  def testMemberLeaveHeartbeat(): Unit = {
    val admin = cluster.admin()
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
        numPartitions = 3
      )
      TestUtils.waitUntilTrue(() => {
        admin.listTopics().names().get().contains(topicName)
      }, msg = s"Topic $topicName is not available to the group coordinator")

      val topology = createMockTopology(topicName)

      // Join group
      var streamsGroupHeartbeatResponse: StreamsGroupHeartbeatResponseData = null
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse = streamsGroupHeartbeat(
          groupId = "test-group",
          memberId = "test-member",
          rebalanceTimeoutMs = 1000,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty,
          topology = topology
        )
        streamsGroupHeartbeatResponse.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Verify the member joined successfully
      assertNotNull(streamsGroupHeartbeatResponse, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals("test-member", streamsGroupHeartbeatResponse.memberId())
      assertEquals(2, streamsGroupHeartbeatResponse.memberEpoch())

      // Send a leave request
      streamsGroupHeartbeatResponse = streamsGroupHeartbeat(
        groupId = "test-group",
        memberId = streamsGroupHeartbeatResponse.memberId(),
        memberEpoch = -1,  // LEAVE_GROUP_MEMBER_EPOCH
        rebalanceTimeoutMs = 1000,
        activeTasks = List.empty,
        standbyTasks = List.empty,
        warmupTasks = List.empty
      )
      
      // Verify the leave request was successful
      assertEquals(Errors.NONE.code(), streamsGroupHeartbeatResponse.errorCode())
      assertEquals("test-member", streamsGroupHeartbeatResponse.memberId())
      assertEquals(-1, streamsGroupHeartbeatResponse.memberEpoch())

    } finally {
      admin.close()
    }
  }

  @ClusterTest
  def testInvalidMemberEpoch(): Unit = {
    val admin = cluster.admin()
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
        numPartitions = 3
      )
      TestUtils.waitUntilTrue(() => {
        admin.listTopics().names().get().contains(topicName)
      }, msg = s"Topic $topicName is not available to the group coordinator")

      val topology = createMockTopology(topicName)

      var streamsGroupHeartbeatResponse: StreamsGroupHeartbeatResponseData = null
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse = streamsGroupHeartbeat(
          groupId = "test-group",
          memberId = "test-member",
          rebalanceTimeoutMs = 1000,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty,
          topology = topology
        )
        streamsGroupHeartbeatResponse.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      streamsGroupHeartbeatResponse = streamsGroupHeartbeat(
        groupId = "test-group",
        memberId = streamsGroupHeartbeatResponse.memberId(),
        memberEpoch = 999,  // Too high member epoch
        rebalanceTimeoutMs = 1000,
        activeTasks = List.empty,
        standbyTasks = List.empty,
        warmupTasks = List.empty,
        topology = null,
        expectedError = Errors.FENCED_MEMBER_EPOCH
      )
      
      val expectedResponse = new StreamsGroupHeartbeatResponseData()
        .setErrorCode(Errors.FENCED_MEMBER_EPOCH.code())
        .setErrorMessage("The streams group member has a greater member epoch (999) than the one known by the group coordinator (2). The member must abandon all its partitions and rejoin.")
      assertEquals(expectedResponse, streamsGroupHeartbeatResponse)
    } finally {
      admin.close()
    }
  }

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
          rebalanceTimeoutMs = 1000,
          activeTasks = Option(streamsGroupHeartbeatResponse)
            .map(r => convertTaskIds(r.activeTasks()))
            .getOrElse(List.empty),
          standbyTasks = Option(streamsGroupHeartbeatResponse)
            .map(r => convertTaskIds(r.standbyTasks()))
            .getOrElse(List.empty),
          warmupTasks = Option(streamsGroupHeartbeatResponse)
            .map(r => convertTaskIds(r.warmupTasks()))
            .getOrElse(List.empty),
          topology = topology
        )
        streamsGroupHeartbeatResponse.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Verify the heartbeat was successful
      assert(streamsGroupHeartbeatResponse != null, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId, streamsGroupHeartbeatResponse.memberId())
      assertEquals(2, streamsGroupHeartbeatResponse.memberEpoch())

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
          rebalanceTimeoutMs = 1000,
          activeTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.activeTasks()))
            .getOrElse(List.empty),
          standbyTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.standbyTasks()))
            .getOrElse(List.empty),
          warmupTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.warmupTasks()))
            .getOrElse(List.empty),
          topology = topology,
          processId = "process-1"
        )
        streamsGroupHeartbeatResponse1.errorCode == Errors.NONE.code() &&
          streamsGroupHeartbeatResponse1.activeTasks() != null
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
          rebalanceTimeoutMs = 1000,
          activeTasks = Option(streamsGroupHeartbeatResponse2)
            .map(r => convertTaskIds(r.activeTasks()))
            .getOrElse(List.empty),
          standbyTasks = Option(streamsGroupHeartbeatResponse2)
            .map(r => convertTaskIds(r.standbyTasks()))
            .getOrElse(List.empty),
          warmupTasks = Option(streamsGroupHeartbeatResponse2)
            .map(r => convertTaskIds(r.warmupTasks()))
            .getOrElse(List.empty),
          topology = topology,
          processId = "process-2"
        )
        streamsGroupHeartbeatResponse2.errorCode == Errors.NONE.code() &&
          streamsGroupHeartbeatResponse2.activeTasks() != null
      }, "Second StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Verify both members do not have standby tasks initially
      assertEquals(0, streamsGroupHeartbeatResponse1.standbyTasks().size(), "Member 1 should have no standby tasks initially")
      assertEquals(0, streamsGroupHeartbeatResponse2.standbyTasks().size(), "Member 2 should have no standby tasks initially")

      // Both members continue to send heartbeats with their assigned tasks
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse1 = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId1,
          memberEpoch = streamsGroupHeartbeatResponse1.memberEpoch(),
          rebalanceTimeoutMs = 1000,
          activeTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.activeTasks()))
            .getOrElse(List.empty),
          standbyTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.standbyTasks()))
            .getOrElse(List.empty),
          warmupTasks = Option(streamsGroupHeartbeatResponse1)
            .map(r => convertTaskIds(r.warmupTasks()))
            .getOrElse(List.empty),
        )
        streamsGroupHeartbeatResponse1.errorCode == Errors.NONE.code() &&
          streamsGroupHeartbeatResponse1.activeTasks() != null
      }, "First member rebalance heartbeat did not succeed within the timeout period.")

      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse2 = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId2,
          memberEpoch = streamsGroupHeartbeatResponse2.memberEpoch(),
          rebalanceTimeoutMs = 1000,
          activeTasks = Option(streamsGroupHeartbeatResponse2)
            .map(r => convertTaskIds(r.activeTasks()))
            .getOrElse(List.empty),
          standbyTasks = Option(streamsGroupHeartbeatResponse2)
            .map(r => convertTaskIds(r.standbyTasks()))
            .getOrElse(List.empty),
          warmupTasks = Option(streamsGroupHeartbeatResponse2)
            .map(r => convertTaskIds(r.warmupTasks()))
            .getOrElse(List.empty)
        )
        streamsGroupHeartbeatResponse2.errorCode == Errors.NONE.code()
      }, "Second member rebalance heartbeat did not succeed within the timeout period.")

      // Verify no standby tasks assigned in this configuration
      assertEquals(Errors.NONE.code(), streamsGroupHeartbeatResponse2.errorCode())
      assertEquals(0, streamsGroupHeartbeatResponse1.standbyTasks().size(), "Member 1 should have no standby tasks in this configuration")
      val member2StandbyTasksSize = if (streamsGroupHeartbeatResponse2.standbyTasks() != null) {
        streamsGroupHeartbeatResponse2.standbyTasks().size()
      } else {
        0
      }
      assertEquals(0, member2StandbyTasksSize, "Member 2 should have no standby tasks in this configuration")

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
          streamsGroupHeartbeatResponse1.standbyTasks() != null
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
      val member1ActiveTasksNum = streamsGroupHeartbeatResponse1.activeTasks().size()
      val member2ActiveTasksNum = streamsGroupHeartbeatResponse2.activeTasks().size()
      assertTrue(member1ActiveTasksNum + member2ActiveTasksNum > 0, "At least one member should have active tasks after config change")

      // Verify that at least one member has standby tasks
      val member1StandbyTasksNum = streamsGroupHeartbeatResponse1.standbyTasks().size()
      val member2StandbyTasksNum = streamsGroupHeartbeatResponse2.standbyTasks().size()
      assertTrue(member1StandbyTasksNum + member2StandbyTasksNum > 0, "At least one member should have standby tasks after config change")

      // With 2 members and streams.num.standby.replicas=1, each active task should have 1 standby task
      val totalActiveTasks = member1ActiveTasksNum + member2ActiveTasksNum
      val totalStandbyTasks = member1StandbyTasksNum + member2StandbyTasksNum
      assertEquals(totalActiveTasks, totalStandbyTasks, "Each active task should have one standby task")

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
          rebalanceTimeoutMs = 1000,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty,
          topology = topology
        )
        streamsGroupHeartbeatResponse.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      val memberEpoch = streamsGroupHeartbeatResponse.memberEpoch()
      assertEquals(2, memberEpoch)

      // Blocking the thread for 1 sec so that the session times out and the member needs to rejoin
      Thread.sleep(1000)

      // Prepare the next heartbeat which should fail due to member expiration
      TestUtils.waitUntilTrue(() => {
        val expiredResponse = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId,
          memberEpoch = memberEpoch,
          rebalanceTimeoutMs = 1000,
          activeTasks = convertTaskIds(streamsGroupHeartbeatResponse.activeTasks()),
          standbyTasks = convertTaskIds(streamsGroupHeartbeatResponse.standbyTasks()),
          warmupTasks = convertTaskIds(streamsGroupHeartbeatResponse.warmupTasks()),
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
          rebalanceTimeoutMs = 1000,
          activeTasks = Option(rejoinHeartbeatResponse)
            .map(r => convertTaskIds(r.activeTasks()))
            .getOrElse(List.empty),
          standbyTasks = Option(rejoinHeartbeatResponse)
            .map(r => convertTaskIds(r.standbyTasks()))
            .getOrElse(List.empty),
          warmupTasks = Option(rejoinHeartbeatResponse)
            .map(r => convertTaskIds(r.warmupTasks()))
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
      assertEquals(0, rejoinHeartbeatResponse.standbyTasks().size(), "There should be no standby tasks assigned")
      assertEquals(0, rejoinHeartbeatResponse.warmupTasks().size(), "There should be no warmup tasks assigned")

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
          rebalanceTimeoutMs = 1000,
          activeTasks = Option(streamsGroupHeartbeatResponse)
            .map(r => convertTaskIds(r.activeTasks()))
            .getOrElse(List.empty),
          standbyTasks = Option(streamsGroupHeartbeatResponse)
            .map(r => convertTaskIds(r.standbyTasks()))
            .getOrElse(List.empty),
          warmupTasks = Option(streamsGroupHeartbeatResponse)
            .map(r => convertTaskIds(r.warmupTasks()))
            .getOrElse(List.empty),
          topology = topology
        )
        streamsGroupHeartbeatResponse.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Verify the response for member
      assert(streamsGroupHeartbeatResponse != null, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId, streamsGroupHeartbeatResponse.memberId())
      assertEquals(2, streamsGroupHeartbeatResponse.memberEpoch())
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
          activeTasks = convertTaskIds(streamsGroupHeartbeatResponse.activeTasks()),
          standbyTasks = convertTaskIds(streamsGroupHeartbeatResponse.standbyTasks()),
          warmupTasks = convertTaskIds(streamsGroupHeartbeatResponse.warmupTasks())
        )
        streamsGroupHeartbeatResponse.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest after broker restart did not succeed within the timeout period.")

      // Verify the response. Epoch should not have changed and null assignments determine that no
      // change in old assignment
      assertEquals(memberId, streamsGroupHeartbeatResponse.memberId())
      assertEquals(2, streamsGroupHeartbeatResponse.memberEpoch())
      assertNull(streamsGroupHeartbeatResponse.activeTasks())
      assertNull(streamsGroupHeartbeatResponse.standbyTasks())
      assertNull(streamsGroupHeartbeatResponse.warmupTasks())

    } finally {
      admin.close()
    }
  }

  private def convertTaskIds(responseTasks: java.util.List[StreamsGroupHeartbeatResponseData.TaskIds]): List[StreamsGroupHeartbeatRequestData.TaskIds] = {
    if (responseTasks == null) {
      List()
    } else {
      responseTasks.asScala.map { responseTask =>
        new StreamsGroupHeartbeatRequestData.TaskIds()
          .setSubtopologyId(responseTask.subtopologyId)
          .setPartitions(responseTask.partitions)
      }.toList
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