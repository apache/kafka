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
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.OffsetCommitRequestTest.assertResponseEquals
import org.apache.kafka.common.message.{OffsetCommitRequestData, OffsetCommitResponseData}
import org.apache.kafka.common.message.OffsetCommitRequestData.{OffsetCommitRequestPartition, OffsetCommitRequestTopic}
import org.apache.kafka.common.message.OffsetCommitResponseData.{OffsetCommitResponsePartition, OffsetCommitResponseTopic}
import org.apache.kafka.common.requests.{OffsetCommitRequest, OffsetCommitResponse}
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource
import org.junit.jupiter.api.{BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest

import java.util.Properties
import scala.collection.Seq
import scala.jdk.CollectionConverters.SeqHasAsJava

class OffsetCommitRequestTest extends BaseRequestTest {
  override def brokerCount: Int = 1

  val brokerId: Integer = 0
  val offset = 15L
  val groupId = "groupId"

  var consumer: KafkaConsumer[_, _] = _

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.BrokerIdProp, brokerId.toString)
    properties.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    val configOverrides = new Properties()
    configOverrides.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    consumer = createConsumer(configOverrides = configOverrides)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
  def testTopicIdsArePopulatedInOffsetCommitResponses(version: Short): Unit = {
    if (version < 9) {
      // No support of topic id prior to version 9.
      return
    }

    val topicNames = Seq("topic1", "topic2", "topic3")
    topicNames.foreach(createTopic(_))
    consumer.subscribe(topicNames.asJava)

    val topicIds = getTopicIds(topicNames.toSeq)

    val requestData = new OffsetCommitRequestData()
      .setGroupId(groupId)
      .setTopics(List(
        new OffsetCommitRequestTopic()
          .setTopicId(topicIds("topic1"))
          .setPartitions(List(new OffsetCommitRequestPartition()
            .setPartitionIndex(0)
            .setCommittedOffset(offset)).asJava),
        new OffsetCommitRequestTopic()
          .setTopicId(topicIds("topic2"))
          .setPartitions(List(new OffsetCommitRequestPartition()
            .setPartitionIndex(0)
            .setCommittedOffset(offset)).asJava),
        new OffsetCommitRequestTopic()
          .setTopicId(topicIds("topic3"))
          .setPartitions(List(new OffsetCommitRequestPartition()
            .setPartitionIndex(0)
            .setCommittedOffset(offset)).asJava)
      ).asJava)

    val responseData = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseTopic()
          .setTopicId(topicIds("topic1"))
          .setPartitions(List(new OffsetCommitResponsePartition()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)).asJava),
        new OffsetCommitResponseTopic()
          .setTopicId(topicIds("topic2"))
          .setPartitions(List(new OffsetCommitResponsePartition()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)).asJava),
        new OffsetCommitResponseTopic()
          .setTopicId(topicIds("topic3"))
          .setPartitions(List(new OffsetCommitResponsePartition()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)).asJava),
      ).asJava)

    val response = connectAndReceive[OffsetCommitResponse](
      new OffsetCommitRequest.Builder(requestData, true).build(version)
    )

    assertResponseEquals(new OffsetCommitResponse(responseData), response)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
  def testOffsetCommitWithUnknownTopicId(version: Short): Unit = {
    if (version < 9) {
      // No support of topic id prior to version 9.
      return
    }

    val topicNames = Seq("topic1", "topic2")
    topicNames.foreach(createTopic(_))
    val topicIds = getTopicIds(topicNames)

    val requestData = new OffsetCommitRequestData()
      .setGroupId(groupId)
      .setTopics(List(
        new OffsetCommitRequestTopic()
          .setTopicId(topicIds("topic1"))
          .setPartitions(List(new OffsetCommitRequestPartition()
            .setPartitionIndex(0)
            .setCommittedOffset(offset)).asJava),
        new OffsetCommitRequestTopic()
          .setTopicId(topicIds("topic2"))
          .setPartitions(List(new OffsetCommitRequestPartition()
            .setPartitionIndex(0)
            .setCommittedOffset(offset)).asJava),
        new OffsetCommitRequestTopic()
          .setName("unresolvable")
          .setPartitions(List(new OffsetCommitRequestPartition()
            .setPartitionIndex(0)
            .setCommittedOffset(offset)).asJava)
      ).asJava)

    val responseData = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseTopic()
          .setTopicId(topicIds("topic1"))
          .setPartitions(List(new OffsetCommitResponsePartition()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)).asJava),
        new OffsetCommitResponseTopic()
          .setTopicId(topicIds("topic2"))
          .setPartitions(List(new OffsetCommitResponsePartition()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)).asJava),
        new OffsetCommitResponseTopic()
          .setName("")
          .setPartitions(List(new OffsetCommitResponsePartition()
            .setPartitionIndex(0)
            .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code)).asJava),
      ).asJava)

    val response = connectAndReceive[OffsetCommitResponse](
      new OffsetCommitRequest.Builder(requestData, true).build(version)
    )

    assertResponseEquals(new OffsetCommitResponse(responseData), response)
  }
}
