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
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.{DeleteGroupsRequestData, JoinGroupResponseData, StreamsGroupHeartbeatRequestData, StreamsGroupTopologyDescriptionUpdateRequestData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{DeleteGroupsRequest, DeleteGroupsResponse, StreamsGroupDescribeResponse}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertNull, assertThrows}
import org.junit.jupiter.api.Timeout

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
        version = ApiKeys.DELETE_GROUPS.latestVersion()
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

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(
      key = GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG,
      value = "kafka.server.FailingTopologyDescriptionPlugin")
  ))
  def testStreamsGroupTopologyDescriptionUpdatePermanentFailureRatchetsFailedEpoch(): Unit = {
    val admin = cluster.admin()
    val groupId = "test-group"
    val memberId = "test-member"
    val topicName = "test-topic"

    try {
      FailingTopologyDescriptionPlugin.reset()
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

      val memberEpoch = joinAndAwaitTopologyDescriptionSolicited(groupId, memberId, topicName)

      FailingTopologyDescriptionPlugin.failNextSetTopology(FailingTopologyDescriptionPlugin.SetTopologyFailureMode.PERMANENT)

      val updateResponse = streamsGroupTopologyDescriptionUpdate(
        groupId = groupId,
        memberId = memberId,
        topologyEpoch = topologyEpoch,
        topologyDescription = createTopologyDescription(topicName)
      )
      assertEquals(Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED.code(), updateResponse.errorCode())
      assertEquals("topology rejected by test plugin", updateResponse.errorMessage())

      // A permanent failure ratchets the group's failed topology epoch: the broker must not
      // re-solicit another push at the same epoch.
      val heartbeatAfterFailure = streamsGroupHeartbeat(
        groupId = groupId,
        memberId = memberId,
        memberEpoch = memberEpoch,
        rebalanceTimeoutMs = 1000,
        activeTasks = List.empty,
        standbyTasks = List.empty,
        warmupTasks = List.empty
      )
      assertFalse(heartbeatAfterFailure.topologyDescriptionRequired(),
        "Broker must not re-solicit a topology description push once the epoch is marked permanently failed.")

      // Restarting the broker replaces its StreamsGroupTopologyDescriptionBackoff with a fresh,
      // empty one, while the group's failed-topology-epoch record survives in the persisted
      // coordinator log. If the heartbeat below is still suppressed, that can only be the
      // persisted ratchet: neither a leftover back-off window from the original solicitation
      // nor a transient-failure back-off window would survive the restart.
      val brokerId = cluster.brokerIds().iterator().next()
      cluster.restartBroker(brokerId, java.util.Collections.emptyMap())
      cluster.waitForReadyBrokers()

      val heartbeatAfterRestart = streamsGroupHeartbeat(
        groupId = groupId,
        memberId = memberId,
        memberEpoch = memberEpoch,
        rebalanceTimeoutMs = 1000,
        activeTasks = List.empty,
        standbyTasks = List.empty,
        warmupTasks = List.empty
      )
      assertFalse(heartbeatAfterRestart.topologyDescriptionRequired(),
        "Broker must still ratchet the failed topology epoch after a restart clears in-memory back-off state.")

      // Nothing was ever stored for this epoch: the permanent failure leaves the group's
      // stored-topology-epoch at its UNCERTAIN sentinel, which Describe reports as NOT_STORED.
      val describedGroup = streamsGroupDescribe(
        groupIds = List(groupId),
        includeTopologyDescription = true
      ).head
      assertEquals(StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED, describedGroup.topologyDescriptionStatus())
    } finally {
      FailingTopologyDescriptionPlugin.reset()
      admin.close()
    }
  }

  @Timeout(180)
  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(
      key = GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG,
      value = "kafka.server.FailingTopologyDescriptionPlugin"),
    // The test below goes 40s without heartbeating; the default 45s session timeout would
    // leave only ~5s of margin before the member is reaped for inactivity.
    new ClusterConfigProperty(key = GroupCoordinatorConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG, value = "120000"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.STREAMS_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, value = "120000")
  ))
  def testStreamsGroupTopologyDescriptionUpdateTransientFailureArmsBackOff(): Unit = {
    val admin = cluster.admin()
    val groupId = "test-group"
    val memberId = "test-member"
    val topicName = "test-topic"

    try {
      FailingTopologyDescriptionPlugin.reset()
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

      // Join the group and wait until the broker solicits a topology description push. This
      // arms the first back-off window: 30s * 2^0, +/-20% jitter (StreamsGroupTopologyDescriptionBackoff),
      // i.e. 24-36s.
      var memberEpoch = joinAndAwaitTopologyDescriptionSolicited(groupId, memberId, topicName)

      // Let that window fully expire without heartbeating, so nothing but the upcoming
      // transient failure can be responsible for the suppression asserted below. The back-off
      // uses the broker's real clock, with no test seam to inject a mock one here, so this
      // waits out real time rather than simulating it.
      Thread.sleep(40000)

      FailingTopologyDescriptionPlugin.failNextSetTopology(FailingTopologyDescriptionPlugin.SetTopologyFailureMode.TRANSIENT)

      val updateResponse = streamsGroupTopologyDescriptionUpdate(
        groupId = groupId,
        memberId = memberId,
        topologyEpoch = topologyEpoch,
        topologyDescription = createTopologyDescription(topicName)
      )
      assertEquals(Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED.code(), updateResponse.errorCode())
      assertEquals("backend offline", updateResponse.errorMessage())

      // The first window already lapsed, so this suppression can only come from the back-off
      // the transient failure itself just armed (30s * 2^1, +/-20% jitter, i.e. 48-72s).
      val heartbeatAfterFailure = streamsGroupHeartbeat(
        groupId = groupId,
        memberId = memberId,
        memberEpoch = memberEpoch,
        rebalanceTimeoutMs = 1000,
        activeTasks = List.empty,
        standbyTasks = List.empty,
        warmupTasks = List.empty
      )
      assertFalse(heartbeatAfterFailure.topologyDescriptionRequired(),
        "Broker must not re-solicit a topology description push immediately after a transient failure; back-off must be armed.")

      // Unlike a permanent failure, this back-off lapses on its own: the broker eventually
      // re-solicits at the same topology epoch, which is what distinguishes transient handling
      // from ratcheting the epoch as permanently failed.
      TestUtils.waitUntilTrue(() => {
        val response = streamsGroupHeartbeat(
          groupId = groupId,
          memberId = memberId,
          memberEpoch = memberEpoch,
          rebalanceTimeoutMs = 1000,
          activeTasks = List.empty,
          standbyTasks = List.empty,
          warmupTasks = List.empty
        )
        memberEpoch = response.memberEpoch()
        response.errorCode == Errors.NONE.code() && response.topologyDescriptionRequired()
      }, "Broker did not re-solicit a topology description push after the transient back-off lapsed.", waitTimeMs = 90000, pause = 2000L)
    } finally {
      FailingTopologyDescriptionPlugin.reset()
      admin.close()
    }
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(
      key = GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG,
      value = "kafka.server.FailingTopologyDescriptionPlugin")
  ))
  def testDeleteGroupsPluginFailureBlocksTombstoneUntilRecovered(): Unit = {
    val admin = cluster.admin()
    val groupId = "test-group"
    val memberId = "test-member"
    val topicName = "test-topic"

    try {
      FailingTopologyDescriptionPlugin.reset()
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

      // Join and push the topology description successfully.
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

      FailingTopologyDescriptionPlugin.failDeleteTopologyWith(new RuntimeException("plugin offline"))

      val deleteVersion = ApiKeys.DELETE_GROUPS.latestVersion()
      val failedDeleteResponse = deleteGroupsRaw(List(groupId), deleteVersion)
      val failedResult = failedDeleteResponse.data.results.find(groupId)
      assertNotNull(failedResult)
      assertEquals(Errors.GROUP_DELETION_FAILED.code(), failedResult.errorCode())

      // The group survives a failed delete: Describe must still find it.
      val describedAfterFailedDelete = streamsGroupDescribe(
        groupIds = List(groupId),
        includeTopologyDescription = true
      ).head
      assertEquals(Errors.NONE.code(), describedAfterFailedDelete.errorCode())

      // Restore the plugin: a retried DeleteGroups must succeed and tombstone the group.
      FailingTopologyDescriptionPlugin.reset()
      deleteGroups(
        groupIds = List(groupId),
        expectedErrors = List(Errors.NONE),
        version = deleteVersion
      )

      val describedAfterDelete = streamsGroupDescribe(
        groupIds = List(groupId),
        includeTopologyDescription = true
      ).head
      assertEquals(Errors.GROUP_ID_NOT_FOUND.code(), describedAfterDelete.errorCode())
    } finally {
      FailingTopologyDescriptionPlugin.reset()
      admin.close()
    }
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(
      key = GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG,
      value = "kafka.server.FailingTopologyDescriptionPlugin"),
    // Keeps the periodic cleanup cycle from also deleting the topology while the test runs.
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG, value = "3600000")
  ))
  def testClassicJoinDeletesStoredTopologyAndConvertsEmptyStreamsGroup(): Unit = {
    val admin = cluster.admin()
    val groupId = "test-group"
    val topicName = "test-topic"

    try {
      FailingTopologyDescriptionPlugin.reset()
      createEmptyStreamsGroupWithStoredTopology(admin, groupId, topicName)

      assertEquals(Errors.NONE.code(), classicJoin(groupId).errorCode())
      assertEquals(1, FailingTopologyDescriptionPlugin.deleteTopologyAttempts(groupId))
      assertEquals(Errors.GROUP_ID_NOT_FOUND.code(), streamsGroupDescribe(List(groupId)).head.errorCode())
      val describedClassicGroup = describeGroups(List(groupId)).head
      assertEquals(Errors.NONE.code(), describedClassicGroup.errorCode())
      assertEquals("consumer", describedClassicGroup.protocolType())
    } finally {
      FailingTopologyDescriptionPlugin.reset()
      admin.close()
    }
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(
      key = GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG,
      value = "kafka.server.FailingTopologyDescriptionPlugin")
  ))
  def testClassicJoinIsRejectedRetriablyWhenPluginDeleteFails(): Unit = {
    val admin = cluster.admin()
    val groupId = "test-group"
    val topicName = "test-topic"

    try {
      FailingTopologyDescriptionPlugin.reset()
      createEmptyStreamsGroupWithStoredTopology(admin, groupId, topicName)

      FailingTopologyDescriptionPlugin.failDeleteTopologyWith(new RuntimeException("plugin offline"))
      assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), classicJoin(groupId).errorCode())
      assertEquals(1, FailingTopologyDescriptionPlugin.deleteTopologyAttempts(groupId))
      assertStillStreamsGroup(groupId)
    } finally {
      FailingTopologyDescriptionPlugin.reset()
      admin.close()
    }
  }

  @Timeout(120)
  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(
      key = GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG,
      value = "kafka.server.FailingTopologyDescriptionPlugin"),
    // Drives the periodic topology cleanup cycle, which runs on the offsets retention check interval.
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG, value = "500")
  ))
  def testPeriodicCleanupClearsTopologyAfterFailedClassicJoin(): Unit = {
    val admin = cluster.admin()
    val groupId = "test-group"
    val topicName = "test-topic"

    try {
      FailingTopologyDescriptionPlugin.reset()
      // Fail deletes before the group becomes empty: once it is, the cleanup cycle (500 ms) may
      // run at any moment and would otherwise delete the topology before the join is attempted.
      FailingTopologyDescriptionPlugin.failDeleteTopologyWith(new RuntimeException("plugin offline"))
      createEmptyStreamsGroupWithStoredTopology(admin, groupId, topicName)

      // The plugin cannot delete: the classic join is rejected and the group stays a streams group.
      assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), classicJoin(groupId).errorCode())
      assertStillStreamsGroup(groupId)

      // The plugin recovers. The failed join armed a back-off (at least 24s) that stops further
      // joins from calling the plugin, and only a successful cleanup cycle clears it. So a join
      // that gets past REBALANCE_IN_PROGRESS shows the periodic cycle emptied the plugin.
      FailingTopologyDescriptionPlugin.failDeleteTopologyWith(null)
      var joinResponse: JoinGroupResponseData = null
      TestUtils.waitUntilTrue(() => {
        joinResponse = sendJoinRequest(groupId = groupId)
        joinResponse.errorCode() != Errors.REBALANCE_IN_PROGRESS.code()
      }, "Classic join was still rejected after the plugin recovered.")

      // The group converts once the member re-joins with the member id the broker assigned.
      assertEquals(Errors.MEMBER_ID_REQUIRED.code(), joinResponse.errorCode())
      assertEquals(Errors.NONE.code(), sendJoinRequest(groupId = groupId, memberId = joinResponse.memberId()).errorCode())
      assertEquals(Errors.GROUP_ID_NOT_FOUND.code(), streamsGroupDescribe(List(groupId)).head.errorCode())
      val describedClassicGroup = describeGroups(List(groupId)).head
      assertEquals(Errors.NONE.code(), describedClassicGroup.errorCode())
      assertEquals("consumer", describedClassicGroup.protocolType())
    } finally {
      FailingTopologyDescriptionPlugin.reset()
      admin.close()
    }
  }

  /**
   * Joins as a new classic member. The topology cleanup runs on the first join; the group only
   * converts to classic once the member re-joins with the member id the broker assigns
   * (MEMBER_ID_REQUIRED), so this follows that second step.
   */
  private def classicJoin(groupId: String): JoinGroupResponseData = {
    val firstResponse = sendJoinRequest(groupId = groupId)
    if (firstResponse.errorCode() != Errors.MEMBER_ID_REQUIRED.code()) {
      firstResponse
    } else {
      sendJoinRequest(groupId = groupId, memberId = firstResponse.memberId())
    }
  }

  /**
   * Creates a streams group whose only member has left, leaving its topology description
   * stored in the plugin with no members remaining — the state in which a classic join
   * must delete the topology before converting the group.
   */
  private def createEmptyStreamsGroupWithStoredTopology(admin: Admin, groupId: String, topicName: String): Unit = {
    val memberId = "test-member"
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

    joinAndAwaitTopologyDescriptionSolicited(groupId, memberId, topicName)
    val updateResponse = streamsGroupTopologyDescriptionUpdate(
      groupId = groupId,
      memberId = memberId,
      topologyEpoch = topologyEpoch,
      topologyDescription = createTopologyDescription(topicName)
    )
    assertEquals(Errors.NONE.code(), updateResponse.errorCode(), s"Unexpected error: ${updateResponse.errorMessage()}")

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
  }

  private def assertStillStreamsGroup(groupId: String): Unit = {
    val describedGroup = streamsGroupDescribe(List(groupId)).head
    assertEquals(Errors.NONE.code(), describedGroup.errorCode())
  }

  private def joinAndAwaitTopologyDescriptionSolicited(groupId: String, memberId: String, topicName: String): Int = {
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
      memberEpoch = response.memberEpoch()
      response.errorCode == Errors.NONE.code() && response.topologyDescriptionRequired()
    }, "Broker did not solicit a topology description push within the timeout period.")
    memberEpoch
  }

  private def deleteGroupsRaw(groupIds: List[String], version: Short): DeleteGroupsResponse = {
    val deleteGroupsRequest = new DeleteGroupsRequest.Builder(
      new DeleteGroupsRequestData().setGroupsNames(groupIds.asJava)
    ).build(version)
    connectAndReceive[DeleteGroupsResponse](deleteGroupsRequest)
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
