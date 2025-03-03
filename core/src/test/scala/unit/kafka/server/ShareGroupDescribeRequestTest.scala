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
import org.apache.kafka.common.GroupState
import org.apache.kafka.common.message.ShareGroupDescribeResponseData.DescribedGroup
import org.apache.kafka.common.message.{ShareGroupDescribeRequestData, ShareGroupDescribeResponseData, ShareGroupHeartbeatResponseData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ShareGroupDescribeRequest, ShareGroupDescribeResponse}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig
import org.apache.kafka.security.authorizer.AclEntry
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{Tag, Timeout}
import org.junit.jupiter.api.extension.ExtendWith

import java.lang.{Byte => JByte}
import scala.jdk.CollectionConverters._

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(types = Array(Type.KRAFT), brokers = 1, serverProperties = Array(
  new ClusterConfigProperty(key = ShareGroupConfig.SHARE_GROUP_PERSISTER_CLASS_NAME_CONFIG, value = "")
))
@Tag("integration")
class ShareGroupDescribeRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, value = "true")
    )
  )
  def testShareGroupDescribeIsInAccessibleWhenConfigsDisabled(): Unit = {
    val shareGroupDescribeRequest = new ShareGroupDescribeRequest.Builder(
      new ShareGroupDescribeRequestData().setGroupIds(List("grp-1", "grp-2").asJava),
      true
    ).build(ApiKeys.SHARE_GROUP_DESCRIBE.latestVersion(isUnstableApiEnabled))

    val shareGroupDescribeResponse = connectAndReceive[ShareGroupDescribeResponse](shareGroupDescribeRequest)
    val expectedResponse = new ShareGroupDescribeResponseData()
    expectedResponse.groups().add(
      new ShareGroupDescribeResponseData.DescribedGroup()
        .setGroupId("grp-1")
        .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    )
    expectedResponse.groups.add(
      new ShareGroupDescribeResponseData.DescribedGroup()
        .setGroupId("grp-2")
        .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    )

    assertEquals(expectedResponse, shareGroupDescribeResponse.data)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic,consumer,share"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG, value = "true"),
    )
  )
  def testShareGroupDescribe(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    val admin = cluster.admin()
    try {
      TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 3
      )

      val clientId = "client-id"
      val clientHost = "/127.0.0.1"
      val authorizedOperationsInt = Utils.to32BitField(
        AclEntry.supportedOperations(ResourceType.GROUP).asScala
          .map(_.code.asInstanceOf[JByte]).asJava)

      // Add first group with one member.
      var grp1Member1Response: ShareGroupHeartbeatResponseData = null
      TestUtils.waitUntilTrue(() => {
        grp1Member1Response = shareGroupHeartbeat(
          groupId = "grp-1",
          subscribedTopicNames = List("bar"),
        )
        grp1Member1Response.errorCode == Errors.NONE.code
      }, msg = s"Could not join the group successfully. Last response $grp1Member1Response.")

      for (version <- ApiKeys.SHARE_GROUP_DESCRIBE.oldestVersion() to ApiKeys.SHARE_GROUP_DESCRIBE.latestVersion(isUnstableApiEnabled)) {
        val expected = List(
          new DescribedGroup()
            .setGroupId("grp-1")
            .setGroupState(GroupState.STABLE.toString)
            .setGroupEpoch(1)
            .setAssignmentEpoch(1)
            .setAssignorName("simple")
            .setAuthorizedOperations(authorizedOperationsInt)
            .setMembers(List(
              new ShareGroupDescribeResponseData.Member()
                .setMemberId(grp1Member1Response.memberId)
                .setMemberEpoch(grp1Member1Response.memberEpoch)
                .setClientId(clientId)
                .setClientHost(clientHost)
                .setSubscribedTopicNames(List("bar").asJava)
            ).asJava),
        )

        val actual = shareGroupDescribe(
          groupIds = List("grp-1"),
          includeAuthorizedOperations = true,
          version = version.toShort,
        )

        assertEquals(expected, actual)
      }
    } finally {
      admin.close()
    }
  }
}
