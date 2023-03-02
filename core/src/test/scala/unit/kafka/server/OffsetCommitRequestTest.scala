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

import kafka.server.KafkaApisTest.{NameAndId, newOffsetCommitRequestData, newOffsetCommitResponseData}
import kafka.server.{BaseRequestTest, KafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.requests.{OffsetCommitRequest, OffsetCommitResponse}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.util.Properties
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.SeqHasAsJava

class OffsetCommitRequestTest extends BaseRequestTest {
  override def brokerCount: Int = 1

  val brokerId: Integer = 0
  val offset = 15L
  val groupId = "groupId"

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.BrokerIdProp, brokerId.toString)
    properties.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
  }

  def createTopics(topicNames: String*): Seq[NameAndId] = {
    topicNames.map(topic => {
      createTopic(topic)
      val topicId: Uuid = getTopicIds().get(topic) match {
        case Some(x) => x
        case _ => throw new AssertionError("Topic ID not found for " + topic)
      }
      NameAndId(topic, topicId)
    })
  }

  @Test
  def testTopicIdsArePopulatedInOffsetCommitResponses(): Unit = {
    val topics = createTopics("topic1", "topic2", "topic3")
    val topicPartitions = topics.map(topic => new TopicPartition(topic.name, 0))

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val consumer = createConsumer()
    consumer.assign(topicPartitions.asJava)

    val requestData = newOffsetCommitRequestData(
      groupId = "group",
      offsets = topics.map((_, ListMap(0 -> offset)))
    )

    ApiKeys.OFFSET_COMMIT.allVersions.forEach { version =>
      val expectedResponse = newOffsetCommitResponseData(
        version,
        topicPartitions = topics.map((_, Map(0 -> Errors.NONE))),
      )

      val response = connectAndReceive[OffsetCommitResponse](
        new OffsetCommitRequest.Builder(requestData).build(version)
      )

      assertEquals(expectedResponse, response.data(), s"OffsetCommit version = $version")
    }
  }

 /* @Test
  def testOffsetCommitWithInvalidId(): Unit = {
    val topics = createTopics("topic1", "topic2", "topic3")
    val topicPartitions = topics.map(topic => new TopicPartition(topic.name, 0))


  }*/
}
