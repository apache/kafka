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
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.{StreamsGroupHeartbeatRequestData, StreamsGroupTopologyDescriptionUpdateRequestData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.StreamsGroupDescribeResponse
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertNull, assertThrows}

import scala.jdk.CollectionConverters._

/**
 * Broker-level tests of the StreamsGroupTopologyDescriptionUpdate RPC with a topology
 * description plugin configured ([[org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin]]).
 * See [[StreamsGroupTopologyDescriptionNoPluginRequestTest]] for the plugin-less
 * UNSUPPORTED_VERSION behavior.
 */
@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "0"),
    new ClusterConfigProperty(
      key = GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG,
      value = "org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin")
  )
)
class StreamsGroupTopologyDescriptionRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  private val topologyEpoch = 1

  @ClusterTest
  def testStreamsGroupTopologyDescriptionUpdateWithInvalidApiVersion(): Unit = {
    assertThrows(classOf[UnsupportedVersionException], () =>
      streamsGroupTopologyDescriptionUpdate(
        groupId = "test-group",
        memberId = "test-member",
        topologyEpoch = topologyEpoch,
        topologyDescription = createTopologyDescription("test-topic"),
        version = -1)
    )
  }

  @ClusterTest
  def testHeartbeatSolicitsPushAndDescribeReturnsStoredTopologyDescription(): Unit = {
    val admin = cluster.admin()
    val groupId = "test-group"
    val memberId = "test-member"
    val topicName = "test-topic"

    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )
      TestUtils.createTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq,
        topic = topicName,
        numPartitions = 3
      )

      // Join the group and wait until the broker solicits a topology description push.
      // The flag is only set on the first heartbeat after the group's topology epoch is
      // resolved (the per-group back-off is armed at the same time), so it must be
      // accumulated across heartbeats rather than asserted on the last response.
      var solicited = false
      var memberEpoch = 0
      TestUtils.waitUntilTrue(() => {
        val response = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId,
          rebalanceTimeoutMs = 1000,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty,
          topology = createMockTopology(topicName)
        )
        solicited ||= response.topologyDescriptionRequired()
        memberEpoch = response.memberEpoch()
        response.errorCode == Errors.NONE.code() && solicited
      }, "Broker did not solicit a topology description push within the timeout period.")

      // Push the topology description for the current topology epoch.
      val updateResponse = streamsGroupTopologyDescriptionUpdate(
        groupId = groupId,
        memberId = memberId,
        topologyEpoch = topologyEpoch,
        topologyDescription = createTopologyDescription(topicName)
      )
      assertEquals(Errors.NONE.code(), updateResponse.errorCode(), s"Unexpected error: ${updateResponse.errorMessage()}")

      // Describe with IncludeTopologyDescription: the stored description must be returned.
      val describedGroup = streamsGroupDescribe(
        groupIds = List(groupId),
        includeTopologyDescription = true
      ).head
      assertEquals(Errors.NONE.code(), describedGroup.errorCode())
      assertEquals(StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_AVAILABLE, describedGroup.topologyDescriptionStatus())
      assertNotNull(describedGroup.topologyDescription())
      assertEquals(1, describedGroup.topologyDescription().subtopologies().size())
      val subtopology = describedGroup.topologyDescription().subtopologies().get(0)
      assertEquals("subtopology-1", subtopology.subtopologyId())
      assertEquals(1, subtopology.nodes().size())
      assertEquals("KSTREAM-SOURCE-0000000000", subtopology.nodes().get(0).name())
      assertEquals(List(topicName).asJava, subtopology.nodes().get(0).sourceTopics())

      // Describe without IncludeTopologyDescription: the status stays at its
      // NOT_REQUESTED default and no description is attached.
      val describedGroupWithoutTopology = streamsGroupDescribe(
        groupIds = List(groupId)
      ).head
      assertEquals(Errors.NONE.code(), describedGroupWithoutTopology.errorCode())
      assertEquals(StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_NOT_REQUESTED, describedGroupWithoutTopology.topologyDescriptionStatus())
      assertNull(describedGroupWithoutTopology.topologyDescription())

      // Once the description is stored at the current topology epoch, heartbeats must not
      // solicit another push.
      val heartbeatAfterPush = streamsGroupHeartbeat(
        groupId = groupId,
        memberId = memberId,
        memberEpoch = memberEpoch,
        rebalanceTimeoutMs = 1000,
        activeTasks = List.empty,
        standbyTasks = List.empty,
        warmupTasks = List.empty
      )
      assertFalse(heartbeatAfterPush.topologyDescriptionRequired(),
        "Broker must not re-solicit a topology description push once it is stored at the current epoch.")
    } finally {
      admin.close()
    }
  }

  @ClusterTest
  def testStreamsGroupTopologyDescriptionUpdateValidationErrors(): Unit = {
    val admin = cluster.admin()
    val groupId = "test-group"
    val memberId = "test-member"
    val topicName = "test-topic"

    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )
      TestUtils.createTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq,
        topic = topicName,
        numPartitions = 3
      )

      // Join the group so the coordinator is loaded and the group exists.
      TestUtils.waitUntilTrue(() => {
        val response = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId,
          rebalanceTimeoutMs = 1000,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty,
          topology = createMockTopology(topicName)
        )
        response.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Empty group id.
      var response = streamsGroupTopologyDescriptionUpdate(
        groupId = "",
        memberId = memberId,
        topologyEpoch = topologyEpoch,
        topologyDescription = createTopologyDescription(topicName)
      )
      assertEquals(Errors.INVALID_REQUEST.code(), response.errorCode())
      assertEquals("GroupId can't be empty.", response.errorMessage())

      // Empty member id.
      response = streamsGroupTopologyDescriptionUpdate(
        groupId = groupId,
        memberId = "",
        topologyEpoch = topologyEpoch,
        topologyDescription = createTopologyDescription(topicName)
      )
      assertEquals(Errors.INVALID_REQUEST.code(), response.errorCode())
      assertEquals("MemberId can't be empty.", response.errorMessage())

      // Unknown group.
      response = streamsGroupTopologyDescriptionUpdate(
        groupId = "unknown-group",
        memberId = memberId,
        topologyEpoch = topologyEpoch,
        topologyDescription = createTopologyDescription(topicName)
      )
      assertEquals(Errors.GROUP_ID_NOT_FOUND.code(), response.errorCode())

      // Unknown member.
      response = streamsGroupTopologyDescriptionUpdate(
        groupId = groupId,
        memberId = "unknown-member",
        topologyEpoch = topologyEpoch,
        topologyDescription = createTopologyDescription(topicName)
      )
      assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), response.errorCode())

      // Stale topology epoch.
      response = streamsGroupTopologyDescriptionUpdate(
        groupId = groupId,
        memberId = memberId,
        topologyEpoch = 42,
        topologyDescription = createTopologyDescription(topicName)
      )
      assertEquals(Errors.INVALID_REQUEST.code(), response.errorCode())
      assertEquals(s"Topology epoch 42 does not match the group's current topology epoch $topologyEpoch.", response.errorMessage())
    } finally {
      admin.close()
    }
  }

  @ClusterTest
  def testDeleteGroupWithStoredTopologyDescription(): Unit = {
    val admin = cluster.admin()
    val groupId = "test-group"
    val memberId = "test-member"
    val topicName = "test-topic"

    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq
      )
      TestUtils.createTopicWithAdmin(
        admin = admin,
        brokers = cluster.brokers.values().asScala.toSeq,
        controllers = cluster.controllers().values().asScala.toSeq,
        topic = topicName,
        numPartitions = 3
      )

      // Join and push the topology description.
      TestUtils.waitUntilTrue(() => {
        val response = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId,
          rebalanceTimeoutMs = 1000,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty,
          topology = createMockTopology(topicName)
        )
        response.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      val updateResponse = streamsGroupTopologyDescriptionUpdate(
        groupId = groupId,
        memberId = memberId,
        topologyEpoch = topologyEpoch,
        topologyDescription = createTopologyDescription(topicName)
      )
      assertEquals(Errors.NONE.code(), updateResponse.errorCode(), s"Unexpected error: ${updateResponse.errorMessage()}")

      val describedGroup = streamsGroupDescribe(
        groupIds = List(groupId),
        includeTopologyDescription = true
      ).head
      assertEquals(StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_AVAILABLE, describedGroup.topologyDescriptionStatus())

      // The member leaves so the group becomes empty and can be deleted.
      val leaveResponse = streamsGroupHeartbeat(
        groupId = groupId,
        memberId = memberId,
        memberEpoch = -1,
        rebalanceTimeoutMs = 1000,
        activeTasks = List.empty,
        standbyTasks = List.empty,
        warmupTasks = List.empty
      )
      assertEquals(Errors.NONE.code(), leaveResponse.errorCode())

      // Deleting the group drives plugin.deleteTopology before the tombstone; a NONE
      // result (rather than GROUP_DELETION_FAILED) shows the plugin delete succeeded.
      deleteGroups(
        groupIds = List(groupId),
        expectedErrors = List(Errors.NONE),
        version = ApiKeys.DELETE_GROUPS.latestVersion(isUnstableApiEnabled)
      )

      // The group is gone.
      val describedAfterDelete = streamsGroupDescribe(
        groupIds = List(groupId),
        includeTopologyDescription = true
      ).head
      assertEquals(Errors.GROUP_ID_NOT_FOUND.code(), describedAfterDelete.errorCode())
    } finally {
      admin.close()
    }
  }

  private def createMockTopology(topicName: String): StreamsGroupHeartbeatRequestData.Topology = {
    new StreamsGroupHeartbeatRequestData.Topology()
      .setEpoch(topologyEpoch)
      .setSubtopologies(List(
        new StreamsGroupHeartbeatRequestData.Subtopology()
          .setSubtopologyId("subtopology-1")
          .setSourceTopics(List(topicName).asJava)
          .setRepartitionSinkTopics(List.empty.asJava)
          .setRepartitionSourceTopics(List.empty.asJava)
      ).asJava)
  }

  private def createTopologyDescription(topicName: String): StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription = {
    new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription()
      .setSubtopologies(List(
        new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology()
          .setSubtopologyId("subtopology-1")
          .setNodes(List(
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode()
              .setName("KSTREAM-SOURCE-0000000000")
              .setNodeType(1.toByte) // SOURCE
              .setSourceTopics(List(topicName).asJava)
              .setStores(List.empty.asJava)
              .setSuccessors(List.empty.asJava)
          ).asJava)
      ).asJava)
      .setGlobalStores(List.empty.asJava)
  }
}
