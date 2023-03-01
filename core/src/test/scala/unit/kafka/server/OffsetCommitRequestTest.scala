/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unit.kafka.server

import kafka.server.{BaseRequestTest, KafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.message.OffsetCommitRequestData
import org.apache.kafka.common.message.OffsetCommitRequestData.{OffsetCommitRequestPartition, OffsetCommitRequestTopic}
import org.apache.kafka.common.requests.{OffsetCommitRequest, OffsetCommitResponse}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.util.Collections.singletonList
import java.util.Properties

class OffsetCommitRequestTest extends BaseRequestTest {
  override def brokerCount: Int = 1

  val brokerId: Integer = 0
  val offset = 15L
  val leaderEpoch = 3
  val metadata = "metadata"
  val topic = "topic"
  val groupId = "groupId"

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.BrokerIdProp, brokerId.toString)
    properties.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
  }

  @Test
  def testTopicIdsArePopulatedInOffsetCommitResponses(): Unit = {
    createTopic(topic)
    val topicId: Uuid = getTopicIds().get(topic) match {
      case Some(x) => x
      case _ => throw new AssertionError("Topic ID not found for " + topic)
    }

    val tpList = singletonList(new TopicPartition(topic, 0))
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    val consumer = createConsumer()
    consumer.assign(tpList)

    val requestData = new OffsetCommitRequestData().setGroupId(groupId)
    requestData.topics().add(new OffsetCommitRequestTopic()
      .setTopicId(topicId)
      .setName(topic)
      .setPartitions(singletonList(new OffsetCommitRequestPartition()
        .setPartitionIndex(0)
        .setCommittedOffset(offset)
        .setCommittedMetadata(metadata)
        .setCommittedLeaderEpoch(leaderEpoch)
      )))

    val request = new OffsetCommitRequest.Builder(requestData).build(OffsetCommitRequestData.HIGHEST_SUPPORTED_VERSION)
    val response = connectAndReceive[OffsetCommitResponse](request)

    assertEquals(topicId, response.data().topics().get(0).topicId())
  }
}
