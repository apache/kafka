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

import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.message.SyncGroupRequestData
import org.junit.jupiter.api.{Tag, Timeout}
import org.junit.jupiter.api.extension.ExtendWith

import scala.concurrent.ExecutionContext.Implicits.global
import java.util.Collections
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.KRAFT, brokers = 1)
@Tag("integration")
class JoinGroupRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "true"),
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testJoinGroupWithOldConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testJoinGroup()
  }

  @ClusterTest(clusterType = Type.ALL, serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "false"),
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "false"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testJoinGroupWithOldConsumerGroupProtocolAndOldGroupCoordinator(): Unit = {
    testJoinGroup()
  }

  private def testJoinGroup(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    //    for (version <- ApiKeys.JOIN_GROUP.oldestVersion() to ApiKeys.JOIN_GROUP.latestVersion(isUnstableApiEnabled)) {

    // Invalid group id.
    //      val joinGroupResponseData = sendJoinRequest(groupId = "")
    //      assertEquals(Errors.INVALID_GROUP_ID.code, joinGroupResponseData.errorCode)

    val metadata = ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(Collections.singletonList("foo"))).array

    val joinGroupResponseData1 = sendJoinRequest(
      groupId = "grp",
      metadata = metadata
    )

    // Rejoin the group with the member id.
    val rejoinGroupResponseData1 = sendJoinRequest(
      groupId = "grp",
      memberId = joinGroupResponseData1.memberId,
      metadata = metadata
    )

    var assignments = List(
      new SyncGroupRequestData.SyncGroupRequestAssignment().setMemberId(rejoinGroupResponseData1.memberId).setAssignment(Array[Byte](1))
    )
    syncGroupWithOldProtocol(
      groupId = "grp",
      memberId = rejoinGroupResponseData1.memberId,
      generationId = rejoinGroupResponseData1.generationId,
      assignments = assignments
    )

    // join the second member
    val joinGroupResponseData2 = sendJoinRequest(
      groupId = "grp",
      metadata = metadata
    )

    val future2 = Future {
      sendJoinRequest("grp", joinGroupResponseData2.memberId, metadata = metadata)
    }
    val future3 = Future {
      sendJoinRequest("grp", rejoinGroupResponseData1.memberId, metadata = metadata)
    }
    val rejoinGroupResponseData2 = Await.result(future2, Duration.Inf)
    val rejoinGroupResponseData3 = Await.result(future3, Duration.Inf)


    assignments = List(
      new SyncGroupRequestData.SyncGroupRequestAssignment().setMemberId(rejoinGroupResponseData1.memberId).setAssignment(Array[Byte](1)),
      new SyncGroupRequestData.SyncGroupRequestAssignment().setMemberId(rejoinGroupResponseData2.memberId).setAssignment(Array[Byte](2))
    )
    syncGroupWithOldProtocol(
      groupId = "grp",
      memberId = rejoinGroupResponseData1.memberId,
      generationId = rejoinGroupResponseData3.generationId,
      assignments = assignments
    )
    syncGroupWithOldProtocol(
      groupId = "grp",
      memberId = rejoinGroupResponseData2.memberId,
      generationId = rejoinGroupResponseData2.generationId
    )
//    assertEquals(0, describeGroups(List("grp"), version = ApiKeys.DESCRIBE_GROUPS.latestVersion(isUnstableApiEnabled)))


    //    }
  }
}