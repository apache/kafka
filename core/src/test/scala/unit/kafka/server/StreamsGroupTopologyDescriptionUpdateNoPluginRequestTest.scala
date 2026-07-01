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

import org.apache.kafka.common.message.{StreamsGroupHeartbeatRequestData, StreamsGroupTopologyDescriptionUpdateRequestData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{StreamsGroupTopologyDescriptionUpdateRequest, StreamsGroupTopologyDescriptionUpdateResponse}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.junit.jupiter.api.Assertions.assertEquals

import java.util
import scala.jdk.CollectionConverters._

/**
 * Integration tests for StreamsGroupTopologyDescriptionUpdate when no plugin is configured.
 * This is a separate class because @ClusterTestDefaults serverProperties merge with
 * per-test @ClusterTest serverProperties, so we cannot remove the plugin config per-test.
 */
@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "0")
  )
)
class StreamsGroupTopologyDescriptionUpdateNoPluginRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest
  def testUpdateTopologyDescriptionWithoutPlugin(): Unit = {
    createOffsetsTopic()

    val requestData = new StreamsGroupTopologyDescriptionUpdateRequestData()
      .setGroupId("test-streams-group")
      .setTopologyEpoch(1).setTopologyDescription(new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription())
      .setTopologyDescription(
        new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription()
          .setSubtopologies(util.Collections.emptyList())
          .setGlobalStores(util.Collections.emptyList()))

    val request = new StreamsGroupTopologyDescriptionUpdateRequest.Builder(requestData)
      .build(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE.latestVersion(isUnstableApiEnabled))

    val response = connectAndReceive[StreamsGroupTopologyDescriptionUpdateResponse](request)
    assertEquals(Errors.UNSUPPORTED_VERSION.code, response.data.errorCode,
      s"Expected UNSUPPORTED_VERSION but got ${Errors.forCode(response.data.errorCode)}: ${response.data.errorMessage}")
  }

  @ClusterTest
  def testHeartbeatDoesNotSetTopologyDescriptionRequiredWhenNoPlugin(): Unit = {
    val groupId = "test-no-plugin-group"
    val topicName = "no-plugin-topic"

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

    val response = streamsGroupHeartbeat(
      groupId = groupId,
      memberId = "test-member",
      rebalanceTimeoutMs = 1000,
      activeTasks = List.empty,
      standbyTasks = List.empty,
      warmupTasks = List.empty,
      topology = topology,
      expectedError = Errors.NONE
    )
    assertEquals(false, response.topologyDescriptionRequired, "Expected topologyDescriptionRequired=false when no plugin is configured")
  }

}
