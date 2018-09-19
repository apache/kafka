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

    val request = new OffsetsForLeaderEpochRequest.Builder(ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion)
      .add(partition, Optional.of(5), 0)
      .build()

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

  private def assertResponseError(error: Errors, brokerId: Int, request: OffsetsForLeaderEpochRequest): Unit = {
    val response = sendRequest(brokerId, request)
    assertEquals(request.epochsByTopicPartition.size, response.responses.size)
    response.responses.asScala.values.foreach { partitionData =>
      assertEquals(error, partitionData.error)
    }
  }

  private def sendRequest(leaderId: Int, request: OffsetsForLeaderEpochRequest): OffsetsForLeaderEpochResponse = {
    val response = connectAndSend(request, ApiKeys.OFFSET_FOR_LEADER_EPOCH, destination = brokerSocketServer(leaderId))
    OffsetsForLeaderEpochResponse.parse(response, request.version)
  }
}
