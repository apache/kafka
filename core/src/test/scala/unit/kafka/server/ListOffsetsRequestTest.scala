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

import java.lang.{Long => JLong}

import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{IsolationLevel, ListOffsetRequest, ListOffsetResponse}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class ListOffsetsRequestTest extends BaseRequestTest {

  @Test
  def testListOffsetsErrorCodes(): Unit = {
    val topic = "topic"
    val partition = new TopicPartition(topic, 0)

    val consumerRequest = ListOffsetRequest.Builder
      .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(Map(partition -> ListOffsetRequest.EARLIEST_TIMESTAMP.asInstanceOf[JLong]).asJava)
      .build()

    val replicaRequest = ListOffsetRequest.Builder
      .forReplica(ApiKeys.LIST_OFFSETS.latestVersion, servers.head.config.brokerId)
      .setTargetTimes(Map(partition -> ListOffsetRequest.EARLIEST_TIMESTAMP.asInstanceOf[JLong]).asJava)
      .build()

    val debugReplicaRequest = ListOffsetRequest.Builder
      .forReplica(ApiKeys.LIST_OFFSETS.latestVersion, ListOffsetRequest.DEBUGGING_REPLICA_ID)
      .setTargetTimes(Map(partition -> ListOffsetRequest.EARLIEST_TIMESTAMP.asInstanceOf[JLong]).asJava)
      .build()

    // Unknown topic
    val randomBrokerId = servers.head.config.brokerId
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, consumerRequest)
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, replicaRequest)
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, debugReplicaRequest)

    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 2, servers)
    val replicas = zkClient.getReplicasForPartition(partition).toSet
    val leader = partitionToLeader(partition.partition)
    val follower = replicas.find(_ != leader).get
    val nonReplica = servers.map(_.config.brokerId).find(!replicas.contains(_)).get

    // Follower
    assertResponseError(Errors.NOT_LEADER_FOR_PARTITION, follower, consumerRequest)
    assertResponseError(Errors.NOT_LEADER_FOR_PARTITION, follower, replicaRequest)
    assertResponseError(Errors.NONE, follower, debugReplicaRequest)

    // Non-replica
    assertResponseError(Errors.NOT_LEADER_FOR_PARTITION, nonReplica, consumerRequest)
    assertResponseError(Errors.NOT_LEADER_FOR_PARTITION, nonReplica, replicaRequest)
    assertResponseError(Errors.REPLICA_NOT_AVAILABLE, nonReplica, debugReplicaRequest)
  }

  private def assertResponseError(error: Errors, brokerId: Int, request: ListOffsetRequest): Unit = {
    val response = sendRequest(brokerId, request)
    assertEquals(request.partitionTimestamps.size, response.responseData.size)
    response.responseData.asScala.values.foreach { partitionData =>
      assertEquals(error, partitionData.error)
    }
  }

  private def sendRequest(leaderId: Int, request: ListOffsetRequest): ListOffsetResponse = {
    val response = connectAndSend(request, ApiKeys.LIST_OFFSETS, destination = brokerSocketServer(leaderId))
    ListOffsetResponse.parse(response, request.version)
  }

}
