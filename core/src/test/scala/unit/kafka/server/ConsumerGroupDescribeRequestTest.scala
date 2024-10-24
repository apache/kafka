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

import org.apache.kafka.common.test.api.ClusterInstance
import org.apache.kafka.common.test.api._
import org.apache.kafka.common.test.api.ClusterTestExtensions
import kafka.utils.TestUtils
import org.apache.kafka.common.{ConsumerGroupState, Uuid}
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData.{Assignment, DescribedGroup, TopicPartitions}
import org.apache.kafka.common.message.{ConsumerGroupDescribeRequestData, ConsumerGroupDescribeResponseData, ConsumerGroupHeartbeatResponseData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.security.authorizer.AclEntry
import org.apache.kafka.server.common.Features
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.extension.ExtendWith

import java.lang.{Byte => JByte}
import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(types = Array(Type.KRAFT), brokers = 1)
class ConsumerGroupDescribeRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    ),
    features = Array(
      new ClusterFeature(feature = Features.GROUP_VERSION, version = 0)
    )
  )
  def testConsumerGroupDescribeWhenFeatureFlagNotEnabled(): Unit = {
    val consumerGroupDescribeRequest = new ConsumerGroupDescribeRequest.Builder(
      new ConsumerGroupDescribeRequestData().setGroupIds(List("grp-1", "grp-2").asJava)
    ).build(ApiKeys.CONSUMER_GROUP_DESCRIBE.latestVersion(isUnstableApiEnabled))

    val consumerGroupDescribeResponse = connectAndReceive[ConsumerGroupDescribeResponse](consumerGroupDescribeRequest)
    val expectedResponse = new ConsumerGroupDescribeResponseData()
    expectedResponse.groups().add(
      new ConsumerGroupDescribeResponseData.DescribedGroup()
        .setGroupId("grp-1")
        .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    )
    expectedResponse.groups.add(
      new ConsumerGroupDescribeResponseData.DescribedGroup()
        .setGroupId("grp-2")
        .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    )

    assertEquals(expectedResponse, consumerGroupDescribeResponse.data)
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testConsumerGroupDescribeWithNewGroupCoordinator(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    val admin = cluster.createAdminClient()
    val topicId = TestUtils.createTopicWithAdminRaw(
      admin = admin,
      topic = "foo",
      numPartitions = 3
    )

    val timeoutMs = 5 * 60 * 1000
    val clientId = "client-id"
    val clientHost = "/127.0.0.1"
    val authorizedOperationsInt = Utils.to32BitField(
      AclEntry.supportedOperations(ResourceType.GROUP).asScala
        .map(_.code.asInstanceOf[JByte]).asJava)

    // Add first group with one member.
    var grp1Member1Response: ConsumerGroupHeartbeatResponseData = null
    TestUtils.waitUntilTrue(() => {
      grp1Member1Response = consumerGroupHeartbeat(
        groupId = "grp-1",
        memberId = Uuid.randomUuid().toString,
        rebalanceTimeoutMs = timeoutMs,
        subscribedTopicNames = List("bar"),
        topicPartitions = List.empty
      )
      grp1Member1Response.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $grp1Member1Response.")

    // Add second group with two members. For the first member, we
    // wait until it receives an assignment. We use 'range` in this
    // case to validate the assignor selection logic.
    var grp2Member1Response: ConsumerGroupHeartbeatResponseData = null
    TestUtils.waitUntilTrue(() => {
      grp2Member1Response = consumerGroupHeartbeat(
        memberId = "member-1",
        groupId = "grp-2",
        serverAssignor = "range",
        rebalanceTimeoutMs = timeoutMs,
        subscribedTopicNames = List("foo"),
        topicPartitions = List.empty
      )
      grp2Member1Response.assignment != null && !grp2Member1Response.assignment.topicPartitions.isEmpty
    }, msg = s"Could not join the group successfully. Last response $grp2Member1Response.")

    val grp2Member2Response = consumerGroupHeartbeat(
      memberId = "member-2",
      groupId = "grp-2",
      serverAssignor = "range",
      rebalanceTimeoutMs = timeoutMs,
      subscribedTopicNames = List("foo"),
      topicPartitions = List.empty
    )

    for (version <- ApiKeys.CONSUMER_GROUP_DESCRIBE.oldestVersion() to ApiKeys.CONSUMER_GROUP_DESCRIBE.latestVersion(isUnstableApiEnabled)) {
      val expected = List(
        new DescribedGroup()
          .setGroupId("grp-1")
          .setGroupState(ConsumerGroupState.STABLE.toString)
          .setGroupEpoch(1)
          .setAssignmentEpoch(1)
          .setAssignorName("uniform")
          .setAuthorizedOperations(authorizedOperationsInt)
          .setMembers(List(
            new ConsumerGroupDescribeResponseData.Member()
              .setMemberId(grp1Member1Response.memberId)
              .setMemberEpoch(grp1Member1Response.memberEpoch)
              .setClientId(clientId)
              .setClientHost(clientHost)
              .setSubscribedTopicRegex("")
              .setSubscribedTopicNames(List("bar").asJava)
          ).asJava),
        new DescribedGroup()
          .setGroupId("grp-2")
          .setGroupState(ConsumerGroupState.RECONCILING.toString)
          .setGroupEpoch(grp2Member2Response.memberEpoch)
          .setAssignmentEpoch(grp2Member2Response.memberEpoch)
          .setAssignorName("range")
          .setAuthorizedOperations(authorizedOperationsInt)
          .setMembers(List(
            new ConsumerGroupDescribeResponseData.Member()
              .setMemberId(grp2Member2Response.memberId)
              .setMemberEpoch(grp2Member2Response.memberEpoch)
              .setClientId(clientId)
              .setClientHost(clientHost)
              .setSubscribedTopicRegex("")
              .setSubscribedTopicNames(List("foo").asJava)
              .setAssignment(new Assignment())
              .setTargetAssignment(new Assignment()
                .setTopicPartitions(List(
                  new TopicPartitions()
                    .setTopicId(topicId)
                    .setTopicName("foo")
                    .setPartitions(List[Integer](2).asJava)
                ).asJava)),
            new ConsumerGroupDescribeResponseData.Member()
              .setMemberId(grp2Member1Response.memberId)
              .setMemberEpoch(grp2Member1Response.memberEpoch)
              .setClientId(clientId)
              .setClientHost(clientHost)
              .setSubscribedTopicRegex("")
              .setSubscribedTopicNames(List("foo").asJava)
              .setAssignment(new Assignment()
                .setTopicPartitions(List(
                  new TopicPartitions()
                    .setTopicId(topicId)
                    .setTopicName("foo")
                    .setPartitions(List[Integer](0, 1, 2).asJava)
                ).asJava))
              .setTargetAssignment(new Assignment()
                .setTopicPartitions(List(
                  new TopicPartitions()
                    .setTopicId(topicId)
                    .setTopicName("foo")
                    .setPartitions(List[Integer](0, 1).asJava)
                ).asJava)),
          ).asJava),
      )

      val actual = consumerGroupDescribe(
        groupIds = List("grp-1", "grp-2"),
        includeAuthorizedOperations = true,
        version = version.toShort,
      )

      assertEquals(expected, actual)
    }
  }
}
