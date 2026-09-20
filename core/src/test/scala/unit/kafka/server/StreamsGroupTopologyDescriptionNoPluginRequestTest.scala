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
import org.apache.kafka.common.message.{StreamsGroupHeartbeatRequestData, StreamsGroupTopologyDescriptionUpdateRequestData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.StreamsGroupDescribeResponse
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNull}

import scala.jdk.CollectionConverters._

/**
 * Broker-level tests of the StreamsGroupTopologyDescriptionUpdate RPC when no topology
 * description plugin is configured (`group.streams.topology.description.plugin.class`
 * unset): the RPC is rejected with UNSUPPORTED_VERSION, heartbeats never solicit a push,
 * and describe reports NOT_STORED. See [[StreamsGroupTopologyDescriptionRequestTest]]
 * for the behavior with a plugin configured.
 */
@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "0")
  )
)
class StreamsGroupTopologyDescriptionNoPluginRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  private val topologyEpoch = 1

  @ClusterTest
  def testStreamsGroupTopologyDescriptionUpdateReturnsUnsupportedVersionWhenNoPluginConfigured(): Unit = {
    // The plugin gate is checked before any group lookup, so no group setup is needed.
    val response = streamsGroupTopologyDescriptionUpdate(
      groupId = "test-group",
      memberId = "test-member",
      topologyEpoch = topologyEpoch,
      topologyDescription = createTopologyDescription("test-topic")
    )
    assertEquals(Errors.UNSUPPORTED_VERSION.code(), response.errorCode())
    assertEquals("The broker has no streams group topology description plugin configured.", response.errorMessage())
  }

  @ClusterTest
  def testHeartbeatDoesNotSolicitPushAndDescribeReturnsNotStoredWhenNoPluginConfigured(): Unit = {
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

      // Join the group and wait until the topology epoch is resolved (tasks assigned).
      // No heartbeat along the way may solicit a topology description push.
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
        assertFalse(response.topologyDescriptionRequired(),
          "Broker must not solicit a topology description push when no plugin is configured.")
        response.errorCode == Errors.NONE.code() && response.activeTasks() != null
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Describe with IncludeTopologyDescription: without a plugin the broker has no
      // description to serve, so the status is NOT_STORED.
      val describedGroup = streamsGroupDescribe(
        groupIds = List(groupId),
        includeTopologyDescription = true
      ).head
      assertEquals(Errors.NONE.code(), describedGroup.errorCode())
      assertEquals(StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED, describedGroup.topologyDescriptionStatus())
      assertNull(describedGroup.topologyDescription())
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
