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

import java.io.IOException

import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.utils.NotNothing
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.DescribeQuorumRequest.singletonRequest
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, ApiVersionsRequest, ApiVersionsResponse, DescribeQuorumRequest, DescribeQuorumResponse}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.extension.ExtendWith

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.KRAFT)
@Tag("integration")
class DescribeQuorumRequestTest(cluster: ClusterInstance) {

  @ClusterTest(clusterType = Type.ZK)
  def testDescribeQuorumNotSupportedByZkBrokers(): Unit = {
    val apiRequest = new ApiVersionsRequest.Builder().build()
    val apiResponse =  connectAndReceive[ApiVersionsResponse](apiRequest)
    assertNull(apiResponse.apiVersion(ApiKeys.DESCRIBE_QUORUM.id))

    val describeQuorumRequest = new DescribeQuorumRequest.Builder(
      singletonRequest(KafkaRaftServer.MetadataPartition)
    ).build()

    assertThrows(classOf[IOException], () => {
      connectAndReceive[DescribeQuorumResponse](describeQuorumRequest)
    })
  }

  @ClusterTest
  def testDescribeQuorum(): Unit = {
    val request = new DescribeQuorumRequest.Builder(
      singletonRequest(KafkaRaftServer.MetadataPartition)
    ).build()

    val response = connectAndReceive[DescribeQuorumResponse](request)

    assertEquals(Errors.NONE, Errors.forCode(response.data.errorCode))
    assertEquals(1, response.data.topics.size)

    val topicData = response.data.topics.get(0)
    assertEquals(KafkaRaftServer.MetadataTopic, topicData.topicName)
    assertEquals(1, topicData.partitions.size)

    val partitionData = topicData.partitions.get(0)
    assertEquals(KafkaRaftServer.MetadataPartition.partition, partitionData.partitionIndex)
    assertEquals(Errors.NONE, Errors.forCode(partitionData.errorCode))
    assertTrue(partitionData.leaderEpoch > 0)

    val leaderId = partitionData.leaderId
    assertTrue(leaderId > 0)

    val leaderState = partitionData.currentVoters.asScala.find(_.replicaId == leaderId)
      .getOrElse(throw new AssertionError("Failed to find leader among current voter states"))
    assertTrue(leaderState.logEndOffset > 0)
  }

  private def connectAndReceive[T <: AbstractResponse](
    request: AbstractRequest
  )(
    implicit classTag: ClassTag[T], nn: NotNothing[T]
  ): T = {
    IntegrationTestUtils.connectAndReceive(
      request,
      cluster.brokerSocketServers().asScala.head,
      cluster.clientListener()
    )
  }

}
