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

import java.util.Optional

import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{OffsetsForLeaderEpochRequest, OffsetsForLeaderEpochResponse}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class OffsetsForLeaderEpochRequestTest extends BaseRequestTest {

  @Test
  def testOffsetsForLeaderEpochErrorCodes(): Unit = {
    val topic = "topic"
    val partition = new TopicPartition(topic, 0)

    val epochs = Map(partition -> new OffsetsForLeaderEpochRequest.PartitionData(Optional.empty[Integer], 0)).asJava
    val request = new OffsetsForLeaderEpochRequest.Builder(ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion, epochs).build()

    // Unknown topic
    val randomBrokerId = servers.head.config.brokerId
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, request)

    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 2, servers)
    val replicas = zkClient.getReplicasForPartition(partition).toSet
    val leader = partitionToLeader(partition.partition)
    val follower = replicas.find(_ != leader).get
    val nonReplica = servers.map(_.config.brokerId).find(!replicas.contains(_)).get

    assertResponseError(Errors.NOT_LEADER_FOR_PARTITION, follower, request)
    assertResponseError(Errors.NOT_LEADER_FOR_PARTITION, nonReplica, request)
  }

  @Test
  def testCurrentEpochValidation(): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 3, servers)
    val firstLeaderId = partitionToLeader(topicPartition.partition)

    def assertResponseErrorForEpoch(error: Errors, brokerId: Int, currentLeaderEpoch: Optional[Integer]): Unit = {
      val epochs = Map(topicPartition -> new OffsetsForLeaderEpochRequest.PartitionData(
        currentLeaderEpoch, 0)).asJava
      val request = new OffsetsForLeaderEpochRequest.Builder(ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion, epochs)
        .build()
      assertResponseError(error, brokerId, request)
    }

    // We need a leader change in order to check epoch fencing since the first epoch is 0 and
    // -1 is treated as having no epoch at all
    killBroker(firstLeaderId)

    // Check leader error codes
    val secondLeaderId = TestUtils.awaitLeaderChange(servers, topicPartition, firstLeaderId)
    val secondLeaderEpoch = TestUtils.findLeaderEpoch(secondLeaderId, topicPartition, servers)
    assertResponseErrorForEpoch(Errors.NONE, secondLeaderId, Optional.empty())
    assertResponseErrorForEpoch(Errors.NONE, secondLeaderId, Optional.of(secondLeaderEpoch))
    assertResponseErrorForEpoch(Errors.FENCED_LEADER_EPOCH, secondLeaderId, Optional.of(secondLeaderEpoch - 1))
    assertResponseErrorForEpoch(Errors.UNKNOWN_LEADER_EPOCH, secondLeaderId, Optional.of(secondLeaderEpoch + 1))

    // Check follower error codes
    val followerId = TestUtils.findFollowerId(topicPartition, servers)
    assertResponseErrorForEpoch(Errors.NOT_LEADER_FOR_PARTITION, followerId, Optional.empty())
    assertResponseErrorForEpoch(Errors.NOT_LEADER_FOR_PARTITION, followerId, Optional.of(secondLeaderEpoch))
    assertResponseErrorForEpoch(Errors.UNKNOWN_LEADER_EPOCH, followerId, Optional.of(secondLeaderEpoch + 1))
    assertResponseErrorForEpoch(Errors.FENCED_LEADER_EPOCH, followerId, Optional.of(secondLeaderEpoch - 1))
  }

  private def assertResponseError(error: Errors, brokerId: Int, request: OffsetsForLeaderEpochRequest): Unit = {
    val response = sendRequest(brokerId, request)
    assertEquals(request.epochsByTopicPartition.size, response.responses.size)
    response.responses.asScala.values.foreach { partitionData =>
      assertEquals(error, partitionData.error)
    }
  }

  private def sendRequest(brokerId: Int, request: OffsetsForLeaderEpochRequest): OffsetsForLeaderEpochResponse = {
    val response = connectAndSend(request, ApiKeys.OFFSET_FOR_LEADER_EPOCH, destination = brokerSocketServer(brokerId))
    OffsetsForLeaderEpochResponse.parse(response, request.version)
  }
}
