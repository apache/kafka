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
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.{OffsetsForLeaderEpochRequest, OffsetsForLeaderEpochResponse}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.jdk.CollectionConverters._

class OffsetsForLeaderEpochRequestTest extends BaseRequestTest {

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testOffsetsForLeaderEpochErrorCodes(quorum: String): Unit = {
    val topic = "topic"
    val partition = new TopicPartition(topic, 0)
    val epochs = offsetForLeaderTopicCollectionFor(partition, 0, RecordBatch.NO_PARTITION_LEADER_EPOCH)

    val request = OffsetsForLeaderEpochRequest.Builder.forFollower(
      ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion, epochs, 1).build()

    // Unknown topic
    val randomBrokerId = brokers.head.config.brokerId
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, request)

    val partitionToLeader = createTopic(topic, replicationFactor = 2)
    val topicDescription = createAdminClient().describeTopics(Seq(partition.topic()).asJava).allTopicNames().get()
    val replicas = topicDescription.get(partition.topic()).partitions().get(partition.partition()).replicas().asScala.map(_.id()).toSet
    val leader = partitionToLeader(partition.partition)
    val follower = replicas.find(_ != leader).get
    val nonReplica = brokers.map(_.config.brokerId).find(!replicas.contains(_)).get

    assertResponseError(Errors.NOT_LEADER_OR_FOLLOWER, follower, request)
    assertResponseError(Errors.NOT_LEADER_OR_FOLLOWER, nonReplica, request)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testCurrentEpochValidation(quorum: String): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val partitionToLeader = createTopic(topic, replicationFactor = 3)
    val firstLeaderId = partitionToLeader(topicPartition.partition)

    def assertResponseErrorForEpoch(error: Errors, brokerId: Int, currentLeaderEpoch: Optional[Integer]): Unit = {
      val epochs = offsetForLeaderTopicCollectionFor(topicPartition, 0,
        currentLeaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
      val request = OffsetsForLeaderEpochRequest.Builder.forFollower(
        ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion, epochs, 1).build()
      assertResponseError(error, brokerId, request)
    }

    // We need a leader change in order to check epoch fencing since the first epoch is 0 and
    // -1 is treated as having no epoch at all
    killBroker(firstLeaderId)

    // Check leader error codes
    val secondLeaderId = TestUtils.awaitLeaderChange(brokers, topicPartition, firstLeaderId)
    val secondLeaderEpoch = TestUtils.findLeaderEpoch(secondLeaderId, topicPartition, brokers)
    assertResponseErrorForEpoch(Errors.NONE, secondLeaderId, Optional.empty())
    assertResponseErrorForEpoch(Errors.NONE, secondLeaderId, Optional.of(secondLeaderEpoch))
    assertResponseErrorForEpoch(Errors.FENCED_LEADER_EPOCH, secondLeaderId, Optional.of(secondLeaderEpoch - 1))
    assertResponseErrorForEpoch(Errors.UNKNOWN_LEADER_EPOCH, secondLeaderId, Optional.of(secondLeaderEpoch + 1))

    // Check follower error codes
    val followerId = TestUtils.findFollowerId(topicPartition, brokers)
    assertResponseErrorForEpoch(Errors.NOT_LEADER_OR_FOLLOWER, followerId, Optional.empty())
    assertResponseErrorForEpoch(Errors.NOT_LEADER_OR_FOLLOWER, followerId, Optional.of(secondLeaderEpoch))
    assertResponseErrorForEpoch(Errors.UNKNOWN_LEADER_EPOCH, followerId, Optional.of(secondLeaderEpoch + 1))
    assertResponseErrorForEpoch(Errors.FENCED_LEADER_EPOCH, followerId, Optional.of(secondLeaderEpoch - 1))
  }

  private def offsetForLeaderTopicCollectionFor(
    topicPartition: TopicPartition,
    leaderEpoch: Int,
    currentLeaderEpoch: Int
  ): OffsetForLeaderTopicCollection = {
    new OffsetForLeaderTopicCollection(List(
      new OffsetForLeaderTopic()
        .setTopic(topicPartition.topic)
        .setPartitions(List(
          new OffsetForLeaderPartition()
            .setPartition(topicPartition.partition)
            .setLeaderEpoch(leaderEpoch)
            .setCurrentLeaderEpoch(currentLeaderEpoch)
        ).asJava)).iterator.asJava)
  }

  private def assertResponseError(error: Errors, brokerId: Int, request: OffsetsForLeaderEpochRequest): Unit = {
    val response = sendRequest(brokerId, request)
    assertEquals(request.data.topics.size, response.data.topics.size)
    response.data.topics.asScala.foreach { offsetForLeaderTopic =>
      assertEquals(request.data.topics.find(offsetForLeaderTopic.topic).partitions.size,
        offsetForLeaderTopic.partitions.size)
      offsetForLeaderTopic.partitions.asScala.foreach { offsetForLeaderPartition =>
        assertEquals(error.code(), offsetForLeaderPartition.errorCode())
      }
    }
  }

  private def sendRequest(brokerId: Int, request: OffsetsForLeaderEpochRequest): OffsetsForLeaderEpochResponse = {
    connectAndReceive[OffsetsForLeaderEpochResponse](request, destination = brokerSocketServer(brokerId))
  }

}
