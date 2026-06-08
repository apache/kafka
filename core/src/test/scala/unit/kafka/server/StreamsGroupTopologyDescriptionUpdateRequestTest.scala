/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{StreamsGroupTopologyDescriptionUpdateRequest, StreamsGroupTopologyDescriptionUpdateResponse}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertTrue}

import java.util
import scala.jdk.CollectionConverters._

@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG,
      value = "org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "0")
  )
)
class StreamsGroupTopologyDescriptionUpdateRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  private def createStreamsGroup(groupId: String, topicName: String): Unit = {
    val topology = new StreamsGroupHeartbeatRequestData.Topology()
      .setEpoch(1)
      .setSubtopologies(List(
        new StreamsGroupHeartbeatRequestData.Subtopology()
          .setSubtopologyId("subtopology-0")
          .setSourceTopics(List(topicName).asJava)
          .setRepartitionSinkTopics(List.empty.asJava)
          .setRepartitionSourceTopics(List.empty.asJava)
      ).asJava)

    streamsGroupHeartbeat(
      groupId = groupId,
      memberId = "setup-member",
      rebalanceTimeoutMs = 1000,
      activeTasks = List.empty,
      standbyTasks = List.empty,
      warmupTasks = List.empty,
      topology = topology,
      expectedError = Errors.NONE
    )
  }

  private def buildTopologyDescription(
    subtopologies: List[StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology] = List.empty,
    globalStores: List[StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionGlobalStore] = List.empty
  ): StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription = {
    new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription()
      .setSubtopologies(subtopologies.asJava)
      .setGlobalStores(globalStores.asJava)
  }

  private def sendUpdateTopologyDescription(
    groupId: String,
    topologyEpoch: Int,
    topoDesc: StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription
  ): StreamsGroupTopologyDescriptionUpdateResponse = {
    val requestData = new StreamsGroupTopologyDescriptionUpdateRequestData()
      .setGroupId(groupId)
      .setTopologyEpoch(1).setTopologyDescription(new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription())
      .setTopologyDescription(topoDesc)

    val request = new StreamsGroupTopologyDescriptionUpdateRequest.Builder(requestData)
      .build(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE.latestVersion(isUnstableApiEnabled))

    connectAndReceive[StreamsGroupTopologyDescriptionUpdateResponse](request)
  }

  private def simpleSubtopology(): List[StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology] = {
    val node = new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode()
      .setName("source-node")
      .setNodeType(1.toByte)
      .setSourceTopics(util.Arrays.asList("input-topic"))
      .setStores(util.Collections.emptyList())
      .setSuccessors(util.Arrays.asList("processor-node"))

    val processorNode = new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode()
      .setName("processor-node")
      .setNodeType(2.toByte)
      .setSourceTopics(util.Collections.emptyList())
      .setStores(util.Collections.emptyList())
      .setSuccessors(util.Collections.emptyList())

    List(new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology()
      .setSubtopologyId("subtopology-0")
      .setNodes(util.Arrays.asList(node, processorNode)))
  }

  @ClusterTest
  def testUpdateTopologyDescriptionWithPlugin(): Unit = {
    val groupId = "test-streams-group"
    val topicName = "input-topic"

    createOffsetsTopic()
    createTopic(topicName, 1)
    createStreamsGroup(groupId, topicName)

    val response = sendUpdateTopologyDescription(groupId, 1, buildTopologyDescription(simpleSubtopology()))
    assertEquals(Errors.NONE.code, response.data.errorCode,
      s"Expected NONE but got ${Errors.forCode(response.data.errorCode)}: ${response.data.errorMessage}")

    // Verify the plugin actually stored the topology
    val plugin = InMemoryTopologyDescriptionPlugin.instances().asScala
      .find(_.storedTopology(groupId).isPresent)
    assertTrue(plugin.isDefined, "Expected at least one plugin instance to have the topology")
    assertNotEquals(-1, plugin.get.storedDescriptionTopologyEpoch(groupId))
    val storedTopo = plugin.get.storedTopology(groupId).get()
    assertEquals(1, storedTopo.subtopologies.size)
    assertEquals("subtopology-0", storedTopo.subtopologies.iterator().next().id())
  }

  @ClusterTest
  def testUpdateTopologyDescriptionDeleteTopology(): Unit = {
    val groupId = "test-streams-group-delete"
    val topicName = "input-topic"

    createOffsetsTopic()
    createTopic(topicName, 1)
    createStreamsGroup(groupId, topicName)

    // First, set a topology
    val setResponse = sendUpdateTopologyDescription(groupId, 1, buildTopologyDescription(simpleSubtopology()))
    assertEquals(Errors.NONE.code, setResponse.data.errorCode)

    // Verify the plugin has the topology
    val plugin = InMemoryTopologyDescriptionPlugin.instances().asScala
      .find(_.storedTopology(groupId).isPresent)
    assertTrue(plugin.isDefined, "Expected plugin to have the topology after set")

    // Then, delete the topology by sending null
    val deleteResponse = sendUpdateTopologyDescription(groupId, 0, null)
    assertEquals(Errors.NONE.code, deleteResponse.data.errorCode)

    // Verify the plugin no longer has the topology
    assertFalse(plugin.get.storedTopology(groupId).isPresent,
      "Expected plugin to not have the topology after delete")
  }

  @ClusterTest
  def testUpdateTopologyDescriptionWithGlobalStore(): Unit = {
    val groupId = "test-streams-group-global"
    val topicName = "input-topic"

    createOffsetsTopic()
    createTopic(topicName, 1)
    createStreamsGroup(groupId, topicName)

    val sourceNode = new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode()
      .setName("global-source")
      .setNodeType(1.toByte)
      .setSourceTopics(util.Arrays.asList("global-topic"))
      .setStores(util.Collections.emptyList())
      .setSuccessors(util.Arrays.asList("global-processor"))

    val processorNode = new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode()
      .setName("global-processor")
      .setNodeType(2.toByte)
      .setSourceTopics(util.Collections.emptyList())
      .setStores(util.Arrays.asList("global-store"))
      .setSuccessors(util.Collections.emptyList())

    val globalStore = new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionGlobalStore()
      .setSource(sourceNode)
      .setProcessor(processorNode)

    val response = sendUpdateTopologyDescription(groupId, 2, buildTopologyDescription(globalStores = List(globalStore)))
    assertEquals(Errors.NONE.code, response.data.errorCode,
      s"Expected NONE but got ${Errors.forCode(response.data.errorCode)}: ${response.data.errorMessage}")
  }

  @ClusterTest
  def testUpdateTopologyDescriptionGroupNotFound(): Unit = {
    createOffsetsTopic()

    val response = sendUpdateTopologyDescription("nonexistent-group", 1, buildTopologyDescription(simpleSubtopology()))
    assertEquals(Errors.GROUP_ID_NOT_FOUND.code, response.data.errorCode,
      s"Expected GROUP_ID_NOT_FOUND but got ${Errors.forCode(response.data.errorCode)}: ${response.data.errorMessage}")
  }

  @ClusterTest
  def testHeartbeatSetsTopologyDescriptionRequiredWhenPluginMissingTopology(): Unit = {
    val groupId = "test-topo-push-group"
    val topicName = "topo-push-topic"

    createOffsetsTopic()
    createTopic(topicName, 1)

    val topology = new StreamsGroupHeartbeatRequestData.Topology()
      .setEpoch(1)
      .setSubtopologies(List(
        new StreamsGroupHeartbeatRequestData.Subtopology()
          .setSubtopologyId("subtopology-0")
          .setSourceTopics(List(topicName).asJava)
          .setRepartitionSinkTopics(List.empty.asJava)
          .setRepartitionSourceTopics(List.empty.asJava)
      ).asJava)

    // First heartbeat joins the group. The plugin doesn't have the topology yet,
    // so the response should set topologyDescriptionRequired=true.
    var response = streamsGroupHeartbeat(
      groupId = groupId,
      memberId = "test-member",
      rebalanceTimeoutMs = 1000,
      activeTasks = List.empty,
      standbyTasks = List.empty,
      warmupTasks = List.empty,
      topology = topology,
      expectedError = Errors.NONE
    )
    assertTrue(response.topologyDescriptionRequired, "Expected topologyDescriptionRequired=true when plugin doesn't have topology")

    // Push the topology description to the plugin
    val updateResponse = sendUpdateTopologyDescription(groupId, 1, buildTopologyDescription(simpleSubtopology()))
    assertEquals(Errors.NONE.code, updateResponse.data.errorCode)

    // Next heartbeat should no longer require topology description
    TestUtils.waitUntilTrue(() => {
      response = streamsGroupHeartbeat(
        groupId = groupId,
        memberId = "test-member",
        memberEpoch = response.memberEpoch,
        rebalanceTimeoutMs = 1000,
        activeTasks = List.empty,
        standbyTasks = List.empty,
        warmupTasks = List.empty,
        expectedError = Errors.NONE
      )
      response.topologyDescriptionRequired == false
    }, msg = "Expected topologyDescriptionRequired=false after pushing topology, " +
      s"but last response had topologyDescriptionRequired=${response.topologyDescriptionRequired}")
  }

  @ClusterTest
  def testUpdateTopologyDescriptionPluginError(): Unit = {
    val groupId = "test-streams-group-error"
    val topicName = "input-topic"

    createOffsetsTopic()
    createTopic(topicName, 1)
    createStreamsGroup(groupId, topicName)

    // Enable failure mode on all plugin instances
    InMemoryTopologyDescriptionPlugin.instances().forEach(_.setFailOnSet(true))
    try {
      val response = sendUpdateTopologyDescription(groupId, 1, buildTopologyDescription(simpleSubtopology()))
      assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, response.data.errorCode,
        s"Expected UNKNOWN_SERVER_ERROR but got ${Errors.forCode(response.data.errorCode)}: ${response.data.errorMessage}")
    } finally {
      InMemoryTopologyDescriptionPlugin.instances().forEach(_.setFailOnSet(false))
    }
  }

  @ClusterTest
  def testStreamsGroupDescribeWithTopology(): Unit = {
    val groupId = "test-streams-group-describe"
    val topicName = "input-topic"

    createOffsetsTopic()
    createTopic(topicName, 1)
    createStreamsGroup(groupId, topicName)

    // Push a topology description so the plugin has something to serve.
    val setResponse = sendUpdateTopologyDescription(groupId, 1, buildTopologyDescription(simpleSubtopology()))
    assertEquals(Errors.NONE.code, setResponse.data.errorCode)

    // Without includeTopologyDescription, the describe response should not carry the topology.
    val describedNoTopology = streamsGroupDescribe(List(groupId), includeTopologyDescription = false)
    assertEquals(1, describedNoTopology.size)
    assertEquals(Errors.NONE.code, describedNoTopology.head.errorCode)
    assertEquals(null, describedNoTopology.head.topologyDescription)

    // With includeTopologyDescription=true, the full topology comes back.
    val describedWithTopology = streamsGroupDescribe(List(groupId), includeTopologyDescription = true)
    assertEquals(1, describedWithTopology.size)
    assertEquals(Errors.NONE.code, describedWithTopology.head.errorCode)
    val topology = describedWithTopology.head.topologyDescription
    assertTrue(topology != null, "Expected topology description in describe response")
    assertEquals(1, topology.subtopologies.size)
    assertEquals("subtopology-0", topology.subtopologies.get(0).subtopologyId)
    assertEquals(2, topology.subtopologies.get(0).nodes.size)
  }

  @ClusterTest
  def testStreamsGroupDescribeWithTopologyPluginFailure(): Unit = {
    val groupId = "test-streams-group-describe-fail"
    val topicName = "input-topic"

    createOffsetsTopic()
    createTopic(topicName, 1)
    createStreamsGroup(groupId, topicName)

    val setResponse = sendUpdateTopologyDescription(groupId, 1, buildTopologyDescription(simpleSubtopology()))
    assertEquals(Errors.NONE.code, setResponse.data.errorCode)

    // Force plugin.getTopology to throw — describe should still succeed with a null topology.
    InMemoryTopologyDescriptionPlugin.instances().forEach(_.setFailOnGet(true))
    try {
      val described = streamsGroupDescribe(List(groupId), includeTopologyDescription = true)
      assertEquals(1, described.size)
      assertEquals(Errors.NONE.code, described.head.errorCode,
        "Describe must not fail when plugin.getTopology throws")
      assertEquals(null, described.head.topologyDescription)
    } finally {
      InMemoryTopologyDescriptionPlugin.instances().forEach(_.setFailOnGet(false))
    }
  }

}
