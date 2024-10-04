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

import org.apache.kafka.common.test.api.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.common.test.api.ClusterTestExtensions
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.DescribeQuorumRequest.singletonRequest
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, DescribeQuorumRequest, DescribeQuorumResponse}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(types = Array(Type.KRAFT))
class DescribeQuorumRequestTest(cluster: ClusterInstance) {

  @ClusterTest
  def testDescribeQuorum(): Unit = {
    for (version <- ApiKeys.DESCRIBE_QUORUM.allVersions.asScala) {
      val request = new DescribeQuorumRequest.Builder(
        singletonRequest(KafkaRaftServer.MetadataPartition)
      ).build(version.toShort)
      val response = connectAndReceive[DescribeQuorumResponse](request)

      assertEquals(Errors.NONE, Errors.forCode(response.data.errorCode))
      assertEquals("", response.data.errorMessage)
      assertEquals(1, response.data.topics.size)

      val topicData = response.data.topics.get(0)
      assertEquals(KafkaRaftServer.MetadataTopic, topicData.topicName)
      assertEquals(1, topicData.partitions.size)

      val partitionData = topicData.partitions.get(0)
      assertEquals(KafkaRaftServer.MetadataPartition.partition, partitionData.partitionIndex)
      assertEquals(Errors.NONE, Errors.forCode(partitionData.errorCode))
      assertEquals("", partitionData.errorMessage())
      assertTrue(partitionData.leaderEpoch > 0)

      val leaderId = partitionData.leaderId
      assertTrue(leaderId > 0)
      assertTrue(partitionData.leaderEpoch() > 0)
      assertTrue(partitionData.highWatermark() > 0)

      val leaderState = partitionData.currentVoters.asScala.find(_.replicaId == leaderId)
        .getOrElse(throw new AssertionError("Failed to find leader among current voter states"))
      assertTrue(leaderState.logEndOffset > 0)

      val voterData = partitionData.currentVoters.asScala
      assertEquals(cluster.controllerIds().asScala, voterData.map(_.replicaId).toSet)

      val observerData = partitionData.observers.asScala
      assertEquals(cluster.brokerIds().asScala, observerData.map(_.replicaId).toSet)

      (voterData ++ observerData).foreach { state =>
        assertTrue(0 < state.logEndOffset)
        if (version == 0) {
          assertEquals(-1, state.lastFetchTimestamp)
          assertEquals(-1, state.lastCaughtUpTimestamp)
        } else {
          assertNotEquals(-1, state.lastFetchTimestamp)
          assertNotEquals(-1, state.lastCaughtUpTimestamp)
        }
      }

      if (version >= 2) {
        val nodes = response.data.nodes().asScala
        assertEquals(cluster.controllerIds().asScala, nodes.map(_.nodeId()).toSet)
        val node = nodes.find(_.nodeId() == cluster.controllers().keySet().asScala.head)
        assertEquals(cluster.controllerListenerName().get().value(), node.get.listeners().asScala.head.name())
      }
    }
  }

  private def connectAndReceive[T <: AbstractResponse](
    request: AbstractRequest
  )(
    implicit classTag: ClassTag[T]
  ): T = {
    IntegrationTestUtils.connectAndReceive(
      request,
      cluster.brokerSocketServers().asScala.head,
      cluster.clientListener()
    )
  }

}
