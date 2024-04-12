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
package unit.kafka.server

import kafka.server.GroupCoordinatorBaseRequestTest
import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import org.apache.kafka.common.ConsumerGroupState
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData.DescribedGroup
import org.apache.kafka.common.protocol.ApiKeys
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{Tag, Timeout}

import scala.jdk.CollectionConverters._

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.KRAFT, brokers = 1)
@Tag("integration")
class ConsumerGroupDescribeRequestsTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testConsumerGroupDescribeWith(): Unit = {
    testConsumerGroupDescribe()
  }

  private def testConsumerGroupDescribe(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    val timeoutMs = 5 * 60 * 1000
    val clientId = "client-id"
    val clientHost = "/127.0.0.1"

    // Add first group
    val consumerGroupHeartbeatResponseData = consumerGroupHeartbeat(
      groupId = "grp-1",
      rebalanceTimeoutMs = timeoutMs,
      subscribedTopicNames = List(""),
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
          .setMembers(List(
            new ConsumerGroupDescribeResponseData.Member()
              .setMemberId(consumerGroupHeartbeatResponseData.memberId)
              .setMemberEpoch(1)
              .setClientId(clientId)
              .setClientHost(clientHost)
              .setSubscribedTopicRegex("")
              .setSubscribedTopicNames(List("").asJava)
          ).asJava)
      )

      val actual = consumerGroupDescribe(
        groupIds = List("grp-1"),
        version = version.toShort
      )

      assertEquals(expected, actual)
    }
  }

}
