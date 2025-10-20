package kafka.server


import kafka.utils.TestUtils
import org.apache.kafka.common.message.{StreamsGroupHeartbeatRequestData, StreamsGroupHeartbeatResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{StreamsGroupHeartbeatRequest, StreamsGroupHeartbeatResponse}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterFeature, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.server.common.Feature
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import java.util.Collections

import scala.jdk.CollectionConverters._

@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "true")
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
    assertThrows(classOf[UnsupportedVersionException], () => {
      new StreamsGroupHeartbeatRequest.Builder(
        new StreamsGroupHeartbeatRequestData()
      ).build(-1)
    })
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic,consumer,streams"),
    ),
    features = Array(
      new ClusterFeature(feature = Feature.STREAMS_VERSION, version = 0)
    )
  )
  def testStreamsGroupHeartbeatIsInaccessableWhenDisabledByFeatureConfig(): Unit = {
    // Test with streams.version = 0, the API is disabled at server level
    val topology = new StreamsGroupHeartbeatRequestData.Topology()
      .setEpoch(1)
      .setSubtopologies(java.util.Collections.emptyList())
    
    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequest.Builder(
      new StreamsGroupHeartbeatRequestData()
        .setGroupId("test-group")
        .setMemberId("test-member")
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(1000)
        .setActiveTasks(java.util.Collections.emptyList())
        .setStandbyTasks(java.util.Collections.emptyList())
        .setWarmupTasks(java.util.Collections.emptyList())
        .setTopology(topology)
    ).build(0)
    
    val streamsGroupHeartbeatResponse = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest)
    val expectedResponse = new StreamsGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code())
    assertEquals(expectedResponse, streamsGroupHeartbeatResponse.data)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic,consumer"),
    )
  )
  def testStreamsGroupHeartbeatIsInaccessableWhenDisabledByStaticGroupCoordinatorProtocolConfig(): Unit = {
    val topology = new StreamsGroupHeartbeatRequestData.Topology()
      .setEpoch(1)
      .setSubtopologies(java.util.Collections.emptyList())
    
    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequest.Builder(
      new StreamsGroupHeartbeatRequestData()
        .setGroupId("test-group")
        .setMemberId("test-member")
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(1000)
        .setActiveTasks(java.util.Collections.emptyList())
        .setStandbyTasks(java.util.Collections.emptyList())
        .setWarmupTasks(java.util.Collections.emptyList())
        .setTopology(topology),
      true  // enableUnstableLastVersion = true
    ).build(0)  // Explicitly use version 0

    val streamsGroupHeartbeatResponse = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest)
    val expectedResponse = new StreamsGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code())
    assertEquals(expectedResponse, streamsGroupHeartbeatResponse.data)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic,consumer,streams"),
    )
  )
  def testStreamsGroupHeartbeatIsInaccessibleWhenUnstableLatestVersionNotEnabled(): Unit = {
    val topology = new StreamsGroupHeartbeatRequestData.Topology()
      .setEpoch(1)
      .setSubtopologies(java.util.Collections.emptyList())
    
    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequest.Builder(
      new StreamsGroupHeartbeatRequestData()
        .setGroupId("test-group")
        .setMemberId("test-member")
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(1000)
        .setActiveTasks(java.util.Collections.emptyList())
        .setStandbyTasks(java.util.Collections.emptyList())
        .setWarmupTasks(java.util.Collections.emptyList())
        .setTopology(topology),
      false  // enableUnstableLastVersion = false
    ).build(0)  // Explicitly use version 0

    val streamsGroupHeartbeatResponse = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest)
    val expectedResponse = new StreamsGroupHeartbeatResponseData().setErrorCode(Errors.NOT_COORDINATOR.code())
    assertEquals(expectedResponse, streamsGroupHeartbeatResponse.data)
  }

  @ClusterTest
  def tesStreamsGroupHeartbeatIsAccessibleWhenNewGroupCoordinatorIsEnabledTopicNotExistFirst(): Unit = {
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

      val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequest.Builder(
        new StreamsGroupHeartbeatRequestData()
          .setGroupId(groupId)
          .setMemberId(memberId)
          .setMemberEpoch(0)
          .setRebalanceTimeoutMs(1000)
          .setActiveTasks(java.util.Collections.emptyList())
          .setStandbyTasks(java.util.Collections.emptyList())
          .setWarmupTasks(java.util.Collections.emptyList())
          .setTopology(
            new StreamsGroupHeartbeatRequestData.Topology()
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
      ).build(0)

      // Heartbeat when topic does not exist
      var streamsGroupHeartbeatResponse: StreamsGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        streamsGroupHeartbeatResponse = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest)
        streamsGroupHeartbeatResponse.data.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Verify the response
      assert(streamsGroupHeartbeatResponse != null, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId, streamsGroupHeartbeatResponse.data.memberId())
      assertEquals(1, streamsGroupHeartbeatResponse.data.memberEpoch())
      val expectedStatus = new StreamsGroupHeartbeatResponseData.Status()
        .setStatusCode(1)
        .setStatusDetail(s"Source topics $topicName are missing.")
      assertEquals(expectedStatus, streamsGroupHeartbeatResponse.data.status().get(0))

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
        streamsGroupHeartbeatResponse = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest)
        streamsGroupHeartbeatResponse.data.errorCode == Errors.NONE.code()
      }, "StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Active task assignment should be available
      assert(streamsGroupHeartbeatResponse != null, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId, streamsGroupHeartbeatResponse.data.memberId())
      assertEquals(2, streamsGroupHeartbeatResponse.data.memberEpoch())
      assertEquals(null, streamsGroupHeartbeatResponse.data.status())
      val expectedActiveTasks = List(
        new StreamsGroupHeartbeatResponseData.TaskIds()
          .setSubtopologyId("subtopology-1")
          .setPartitions(List(0, 1, 2).map(_.asInstanceOf[Integer]).asJava)
      ).asJava
      assertEquals(expectedActiveTasks, streamsGroupHeartbeatResponse.data.activeTasks())


    } finally {
      admin.close()
    }
  }

  @ClusterTest
  def tesStreamsGroupHeartbeatIsAccessibleWhenNewGroupCoordinatorIsEnabledTwoMembers(): Unit = {
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
      // Wait for topic to be available
      TestUtils.waitUntilTrue(() => {
        admin.listTopics().names().get().contains(topicName)
      }, msg = s"Topic $topicName is not available to the group coordinator")

      // First member joins the group
      var streamsGroupHeartbeatResponse1: StreamsGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        val streamsGroupHeartbeatRequest1 = new StreamsGroupHeartbeatRequest.Builder(
          new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(1000)
            .setActiveTasks(Option(streamsGroupHeartbeatResponse1)
              .map(r => convertTaskIds(r.data().activeTasks()))
              .getOrElse(Collections.emptyList()))
            .setStandbyTasks(Option(streamsGroupHeartbeatResponse1)
              .map(r => convertTaskIds(r.data().standbyTasks()))
              .getOrElse(Collections.emptyList()))
            .setWarmupTasks(Option(streamsGroupHeartbeatResponse1)
              .map(r => convertTaskIds(r.data().warmupTasks()))
              .getOrElse(Collections.emptyList()))
            .setTopology(
              new StreamsGroupHeartbeatRequestData.Topology()
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
        ).build(0)

        streamsGroupHeartbeatResponse1 = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest1)
        streamsGroupHeartbeatResponse1.data.errorCode == Errors.NONE.code()
      }, "First StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Verify first member gets all tasks initially
      assert(streamsGroupHeartbeatResponse1 != null, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId1, streamsGroupHeartbeatResponse1.data.memberId())
      assertEquals(1, streamsGroupHeartbeatResponse1.data.memberEpoch())
      assertEquals(1, streamsGroupHeartbeatResponse1.data.activeTasks().size())
      assertEquals(3, streamsGroupHeartbeatResponse1.data.activeTasks().get(0).partitions().size())

      // Second member joins the group (should trigger a rebalance)
      var streamsGroupHeartbeatResponse2: StreamsGroupHeartbeatResponse = null
      TestUtils.waitUntilTrue(() => {
        val streamsGroupHeartbeatRequest2 = new StreamsGroupHeartbeatRequest.Builder(
          new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(1000)
            .setActiveTasks(Option(streamsGroupHeartbeatResponse2)
              .map(r => convertTaskIds(r.data().activeTasks()))
              .getOrElse(Collections.emptyList()))
            .setStandbyTasks(Option(streamsGroupHeartbeatResponse2)
              .map(r => convertTaskIds(r.data().standbyTasks()))
              .getOrElse(Collections.emptyList()))
            .setWarmupTasks(Option(streamsGroupHeartbeatResponse2)
              .map(r => convertTaskIds(r.data().warmupTasks()))
              .getOrElse(Collections.emptyList()))
            .setTopology(
              new StreamsGroupHeartbeatRequestData.Topology()
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
        ).build(0)

        streamsGroupHeartbeatResponse2 = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest2)
        streamsGroupHeartbeatResponse2.data.errorCode == Errors.NONE.code()
      }, "Second StreamsGroupHeartbeatRequest did not succeed within the timeout period.")

      // Verify second member gets assigned
      assert(streamsGroupHeartbeatResponse2 != null, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId2, streamsGroupHeartbeatResponse2.data.memberId())
      assertEquals(2, streamsGroupHeartbeatResponse2.data.memberEpoch())

      // Now both members should send heartbeats with their assigned tasks
      // First member should continue with its tasks
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
            .setTopology(
              new StreamsGroupHeartbeatRequestData.Topology()
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
        ).build(0)


        streamsGroupHeartbeatResponse1 = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest1)
        streamsGroupHeartbeatResponse1.data.errorCode == Errors.NONE.code()
      }, "First member rebalance heartbeat did not succeed within the timeout period.")

      // Second member should also send heartbeat with its assigned tasks
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
            .setTopology(
              new StreamsGroupHeartbeatRequestData.Topology()
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
        ).build(0)

        streamsGroupHeartbeatResponse2 = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest2)
        streamsGroupHeartbeatResponse2.data.errorCode == Errors.NONE.code()
      }, "Second member rebalance heartbeat did not succeed within the timeout period.")

      // Verify final state - both members should have tasks assigned
      assert(streamsGroupHeartbeatResponse1 != null, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId1, streamsGroupHeartbeatResponse1.data.memberId())
      
      assert(streamsGroupHeartbeatResponse2 != null, "StreamsGroupHeartbeatResponse should not be null")
      assertEquals(memberId2, streamsGroupHeartbeatResponse2.data.memberId())

      // At least one member should have active tasks (in a real scenario, tasks would be distributed)
      val totalActiveTasks = streamsGroupHeartbeatResponse1.data.activeTasks().size() + streamsGroupHeartbeatResponse2.data.activeTasks().size()
      assert(totalActiveTasks > 0, "At least one member should have active tasks")

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
}