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
import org.apache.kafka.common.message.{StreamsGroupDescribeRequestData, StreamsGroupDescribeResponseData, StreamsGroupHeartbeatRequestData, StreamsGroupHeartbeatResponseData}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{StreamsGroupDescribeRequest, StreamsGroupDescribeResponse}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.api._

import scala.jdk.CollectionConverters._
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.security.authorizer.AclEntry
import org.apache.kafka.server.common.Feature
import org.junit.Assert.{assertEquals, assertTrue}

import java.lang.{Byte => JByte}

@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  brokers = 1,
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
  )
)
class StreamsGroupDescribeRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest(
    features = Array(
      new ClusterFeature(feature = Feature.STREAMS_VERSION, version = 0)
    )
  )
  def testStreamsGroupDescribeWhenFeatureFlagNotEnabled(): Unit = {
    val streamsGroupDescribeRequest = new StreamsGroupDescribeRequest.Builder(
      new StreamsGroupDescribeRequestData().setGroupIds(List("grp-mock-1", "grp-mock-2").asJava)
    ).build(ApiKeys.STREAMS_GROUP_DESCRIBE.latestVersion(isUnstableApiEnabled))

    val streamsGroupDescribeResponse = connectAndReceive[StreamsGroupDescribeResponse](streamsGroupDescribeRequest)
    val expectedResponse = new StreamsGroupDescribeResponseData()
    expectedResponse.groups().add(
      new StreamsGroupDescribeResponseData.DescribedGroup()
        .setGroupId("grp-mock-1")
        .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    )
    expectedResponse.groups().add(
      new StreamsGroupDescribeResponseData.DescribedGroup()
        .setGroupId("grp-mock-2")
        .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    )
    assertEquals(expectedResponse, streamsGroupDescribeResponse.data)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic,consumer,streams"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testStreamsGroupDescribeGroupsWithNewGroupCoordinator(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    val admin = cluster.admin()
    val topicName = "foo"

    try {
      TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = topicName,
        numPartitions = 3
      )

      TestUtils.waitUntilTrue(() => {
        admin.listTopics().names().get().contains(topicName)
      }, msg = s"Topic $topicName is not available to the group coordinator")

      val timeoutMs = 5 * 60 * 1000
      val clientId = "client-id"
      val clientHost = "/127.0.0.1"
      val authorizedOperationsInt = Utils.to32BitField(
        AclEntry.supportedOperations(ResourceType.GROUP).asScala
          .map(_.code.asInstanceOf[JByte]).asJava)

      var grp1Member1Response: StreamsGroupHeartbeatResponseData = null
      var grp1Member2Response: StreamsGroupHeartbeatResponseData = null
      var grp2Member1Response: StreamsGroupHeartbeatResponseData = null
      var grp2Member2Response: StreamsGroupHeartbeatResponseData = null

      // grp-1 with 2 members
      TestUtils.waitUntilTrue(() => {
        grp1Member1Response = streamsGroupHeartbeat(
          groupId = "grp-1",
          memberId = "member-1",
          rebalanceTimeoutMs = timeoutMs,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty,
          topology = new StreamsGroupHeartbeatRequestData.Topology()
            .setEpoch(1)
            .setSubtopologies(List(
              new StreamsGroupHeartbeatRequestData.Subtopology()
                .setSubtopologyId("subtopology-1")
                .setSourceTopics(List(topicName).asJava)
                .setRepartitionSinkTopics(List.empty.asJava)
                .setRepartitionSourceTopics(List.empty.asJava)
                .setStateChangelogTopics(List.empty.asJava)
            ).asJava)
        )
        grp1Member2Response = streamsGroupHeartbeat(
          groupId = "grp-1",
          memberId = "member-2",
          rebalanceTimeoutMs = timeoutMs,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty,
          topology = new StreamsGroupHeartbeatRequestData.Topology()
            .setEpoch(1)
            .setSubtopologies(List(
              new StreamsGroupHeartbeatRequestData.Subtopology()
                .setSubtopologyId("subtopology-1")
                .setSourceTopics(List(topicName).asJava)
                .setRepartitionSinkTopics(List.empty.asJava)
                .setRepartitionSourceTopics(List.empty.asJava)
                .setStateChangelogTopics(List.empty.asJava)
            ).asJava)
        )

        val groupsDescription1 = streamsGroupDescribe(
          groupIds = List("grp-1"),
          includeAuthorizedOperations = true
        )
        grp1Member1Response.errorCode == Errors.NONE.code && grp1Member2Response.errorCode == Errors.NONE.code &&
          groupsDescription1.size == 1 && groupsDescription1.head.members.size == 2
      }, msg = s"Could not create grp-1 with 2 members successfully")

      // grp-2 with 2 members
      TestUtils.waitUntilTrue(() => {
        grp2Member1Response = streamsGroupHeartbeat(
          groupId = "grp-2",
          memberId = "member-3",
          rebalanceTimeoutMs = timeoutMs,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty,
          topology = new StreamsGroupHeartbeatRequestData.Topology()
            .setEpoch(1)
            .setSubtopologies(List(
              new StreamsGroupHeartbeatRequestData.Subtopology()
                .setSubtopologyId("subtopology-1")
                .setSourceTopics(List(topicName).asJava)
                .setRepartitionSinkTopics(List.empty.asJava)
                .setRepartitionSourceTopics(List.empty.asJava)
                .setStateChangelogTopics(List.empty.asJava)
            ).asJava)
        )
        grp2Member2Response = streamsGroupHeartbeat(
          groupId = "grp-2",
          memberId = "member-4",
          rebalanceTimeoutMs = timeoutMs,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty,
          topology = new StreamsGroupHeartbeatRequestData.Topology()
            .setEpoch(1)
            .setSubtopologies(List(
              new StreamsGroupHeartbeatRequestData.Subtopology()
                .setSubtopologyId("subtopology-1")
                .setSourceTopics(List(topicName).asJava)
                .setRepartitionSinkTopics(List.empty.asJava)
                .setRepartitionSourceTopics(List.empty.asJava)
                .setStateChangelogTopics(List.empty.asJava)
            ).asJava)
        )
        val groupsDescription2 = streamsGroupDescribe(
          groupIds = List("grp-2"),
          includeAuthorizedOperations = true,
          version = ApiKeys.STREAMS_GROUP_DESCRIBE.latestVersion(isUnstableApiEnabled).toShort
        )

        grp2Member1Response.errorCode == Errors.NONE.code && grp2Member2Response.errorCode == Errors.NONE.code &&
          groupsDescription2.size == 1 && groupsDescription2.head.members.size == 2
      }, msg = s"Could not create grp-2 with 2 members successfully")

      // Send follow-up heartbeats until both groups are stable
      TestUtils.waitUntilTrue(() => {
        grp1Member1Response = streamsGroupHeartbeat(
          groupId = "grp-1",
          memberId = grp1Member1Response.memberId,
          memberEpoch = grp1Member1Response.memberEpoch,
          rebalanceTimeoutMs = timeoutMs,
          activeTasks = convertTaskIds(grp1Member1Response.activeTasks),
          standbyTasks = convertTaskIds(grp1Member1Response.standbyTasks),
          warmupTasks = convertTaskIds(grp1Member1Response.warmupTasks),
          topology = null
        )
        grp1Member2Response = streamsGroupHeartbeat(
          groupId = "grp-1",
          memberId = grp1Member2Response.memberId,
          memberEpoch = grp1Member2Response.memberEpoch,
          rebalanceTimeoutMs = timeoutMs,
          activeTasks = convertTaskIds(grp1Member2Response.activeTasks),
          standbyTasks = convertTaskIds(grp1Member2Response.standbyTasks),
          warmupTasks = convertTaskIds(grp1Member2Response.warmupTasks),
          topology = null
        )
        grp2Member1Response = streamsGroupHeartbeat(
          groupId = "grp-2",
          memberId = grp2Member1Response.memberId,
          memberEpoch = grp2Member1Response.memberEpoch,
          rebalanceTimeoutMs = timeoutMs,
          activeTasks = convertTaskIds(grp2Member1Response.activeTasks),
          standbyTasks = convertTaskIds(grp2Member1Response.standbyTasks),
          warmupTasks = convertTaskIds(grp2Member1Response.warmupTasks),
          topology = null
        )
        grp2Member2Response = streamsGroupHeartbeat(
          groupId = "grp-2",
          memberId = grp2Member2Response.memberId,
          memberEpoch = grp2Member2Response.memberEpoch,
          rebalanceTimeoutMs = timeoutMs,
          activeTasks = convertTaskIds(grp2Member2Response.activeTasks),
          standbyTasks = convertTaskIds(grp2Member2Response.standbyTasks),
          warmupTasks = convertTaskIds(grp2Member2Response.warmupTasks),
          topology = null
        )
        val actual = streamsGroupDescribe(
          groupIds = List("grp-1","grp-2"),
          includeAuthorizedOperations = true,
          version = ApiKeys.STREAMS_GROUP_DESCRIBE.latestVersion(isUnstableApiEnabled).toShort
        )
          actual.head.groupState() == "Stable" && actual(1).groupState() == "Stable" &&
          actual.head.members.size == 2 && actual(1).members.size == 2
      }, "Two groups did not stabilize with 2 members each in time")

      // Test the describe request for both groups in stable state
      for (version <- ApiKeys.STREAMS_GROUP_DESCRIBE.oldestVersion() to ApiKeys.STREAMS_GROUP_DESCRIBE.latestVersion(isUnstableApiEnabled)) {
        val actual = streamsGroupDescribe(
          groupIds = List("grp-1","grp-2"),
          includeAuthorizedOperations = true,
          version = version.toShort
        )

        assertEquals(2, actual.size)
        assertEquals(actual.map(_.groupId).toSet, Set("grp-1", "grp-2"))
        for (describedGroup <- actual) {
          assertEquals("Stable", describedGroup.groupState)
          assertTrue("Group epoch is not equal to the assignment epoch", describedGroup.groupEpoch == describedGroup.assignmentEpoch)
          // Verify topology
          assertEquals(1, describedGroup.topology.epoch)
          assertEquals(1, describedGroup.topology.subtopologies.size)
          assertEquals("subtopology-1", describedGroup.topology.subtopologies.get(0).subtopologyId)
          assertEquals(List(topicName).asJava, describedGroup.topology.subtopologies.get(0).sourceTopics)

          // Verify members
          assertEquals(2, describedGroup.members.size)
          val expectedMemberIds = describedGroup.groupId match {
            case "grp-1" => Set(grp1Member1Response.memberId, grp1Member2Response.memberId)
            case "grp-2" => Set(grp2Member1Response.memberId, grp2Member2Response.memberId)
            case unexpected => throw new AssertionError(s"Unexpected group ID: $unexpected")
          }

          val actualMemberIds = describedGroup.members.asScala.map(_.memberId).toSet
          assertEquals(expectedMemberIds, actualMemberIds)
          assertEquals(authorizedOperationsInt, describedGroup.authorizedOperations)

          describedGroup.members.asScala.foreach { member =>
            assertTrue("Group epoch is not equal to the member epoch", member.memberEpoch == describedGroup.assignmentEpoch)
            assertEquals(1, member.topologyEpoch)
            assertEquals(member.targetAssignment, member.assignment)
            assertEquals(clientId, member.clientId())
            assertEquals(clientHost, member.clientHost())
          }
          // Verify all partitions 0, 1, 2 are assigned exactly once
          val allAssignedPartitions = describedGroup.members.asScala.flatMap { member =>
            member.assignment.activeTasks.asScala.flatMap(_.partitions.asScala)
          }.toList
          assertEquals(List(0, 1, 2).sorted, allAssignedPartitions.sorted)
        }
      }
    } finally{
      admin.close()
    }
  }

  private def convertTaskIds(responseTasks: java.util.List[StreamsGroupHeartbeatResponseData.TaskIds]): List[StreamsGroupHeartbeatRequestData.TaskIds] = {
    if (responseTasks == null) {
      List.empty
    } else {
      responseTasks.asScala.map { responseTask =>
        new StreamsGroupHeartbeatRequestData.TaskIds()
          .setSubtopologyId(responseTask.subtopologyId)
          .setPartitions(responseTask.partitions)
      }.toList
    }
  }
}