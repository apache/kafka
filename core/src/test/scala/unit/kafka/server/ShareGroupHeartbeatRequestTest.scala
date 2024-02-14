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

import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.junit.RaftClusterInvocationContext.RaftClusterInstance
import kafka.utils.TestUtils
import org.apache.kafka.common.message.{ShareGroupHeartbeatRequestData, ShareGroupHeartbeatResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.{Tag, Timeout}
import org.junit.jupiter.api.extension.ExtendWith

import java.util.stream.Collectors
import scala.jdk.CollectionConverters._

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.KRAFT, brokers = 1)
@Tag("integration")
class ShareGroupHeartbeatRequestTest(cluster: ClusterInstance) {

  @ClusterTest()
  def testShareGroupHeartbeatIsAccessibleWhenEnabled(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData(), true
    ).build()

    val shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
    val expectedResponse = new ShareGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    assertEquals(expectedResponse, shareGroupHeartbeatResponse.data)
  }

  // TODO: The below test runs fine for consumers joining and leaving the group but fails when checking for expected partition assignment.
  //  It should be enabled once the partition assignment in share groups is fixed
  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "group.share.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testShareGroupHeartbeatIsAccessibleWhenShareGroupIsEnabled(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    val admin = cluster.createAdminClient()

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    TestUtils.createOffsetsTopicWithAdmin(
      admin = admin,
      brokers = raftCluster.brokers.collect(Collectors.toList[BrokerServer]).asScala,
      controllers = raftCluster.controllerServers().asScala.toSeq
    )

    // Heartbeat request to join the group. Note that the member subscribes
    // to an nonexistent topic.
    var shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava), true
    ).build()

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var shareGroupHeartbeatResponse: ShareGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $shareGroupHeartbeatResponse.")

    // Verify the response.
    assertNotNull(shareGroupHeartbeatResponse.data.memberId)
    assertEquals(1, shareGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(new ShareGroupHeartbeatResponseData.Assignment(), shareGroupHeartbeatResponse.data.assignment)

    // Create the topic.
    val topicId = TestUtils.createTopicWithAdminRaw(
      admin = admin,
      topic = "foo",
      numPartitions = 3
    )

    // Prepare the next heartbeat.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(shareGroupHeartbeatResponse.data.memberId)
        .setMemberEpoch(shareGroupHeartbeatResponse.data.memberEpoch), true
    ).build()

    // This is the expected assignment.
    val expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
      .setAssignedTopicPartitions(List(new ShareGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)

    // Heartbeats until the partitions are assigned.
    shareGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        shareGroupHeartbeatResponse.data.assignment == expectedAssignment
    }, msg = s"Could not get partitions assigned. Last response $shareGroupHeartbeatResponse.")

    // Verify the response.
    assertEquals(2, shareGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(expectedAssignment, shareGroupHeartbeatResponse.data.assignment)

    // Leave the group.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(shareGroupHeartbeatResponse.data.memberId)
        .setMemberEpoch(-1), true
    ).build()

    shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)

    // Verify the response.
    assertEquals(-1, shareGroupHeartbeatResponse.data.memberEpoch)
  }

  private def connectAndReceive(request: ShareGroupHeartbeatRequest): ShareGroupHeartbeatResponse = {
    IntegrationTestUtils.connectAndReceive[ShareGroupHeartbeatResponse](
      request,
      cluster.anyBrokerSocketServer(),
      cluster.clientListener()
    )
  }
}
