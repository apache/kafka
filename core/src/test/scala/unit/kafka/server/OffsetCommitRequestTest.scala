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
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.requests.{OffsetCommitRequest, OffsetCommitResponse}
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

import java.util.Optional.empty
import java.util.Properties
import scala.collection.{Map, Seq}
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.{ListHasAsScala, MapHasAsJava, SeqHasAsJava}

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
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    consumer = createConsumer()
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (consumer != null)
      Utils.closeQuietly(consumer, "KafkaConsumer")
    super.tearDown()
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
    consumer.subscribe(topics.map(_.name).asJava)

    sendOffsetCommitRequest(
      topics.map((_, ListMap(0 -> offset))),
      topics.map((_, Map(0 -> Errors.NONE))),
      ApiKeys.OFFSET_COMMIT.allVersions().asScala
    )
  }

  @Test
  def testCommitOffsetFromConsumer(): Unit = {
    val topics = createTopics("topic1", "topic2", "topic3")
    consumer.commitSync(offsetsToCommit(topics, offset))
  }

  @Test
  def testOffsetCommitWithUnknownTopicId(): Unit = {
    val topics = createTopics("topic1", "topic2", "topic3")

    sendOffsetCommitRequest(
      topics.map((_, ListMap(0 -> offset))) ++ Seq((NameAndId("unresolvable"), ListMap(0 -> offset))),
      Seq((NameAndId("unresolvable"), ListMap(0 -> Errors.UNKNOWN_TOPIC_ID))) ++ topics.map((_, Map(0 -> Errors.NONE))),
      ApiKeys.OFFSET_COMMIT.allVersions().asScala.filter(_ >= 9)
    )
  }

  @Test
  def alterConsumerGroupOffsetsDoNotUseTopicIds(): Unit = {
    val topics = createTopics("topic1", "topic2", "topic3")
    val admin = createAdminClient()

    try {
      // Would throw an UnknownTopicId exception if the OffsetCommitRequest was set to version 9 or higher.
      admin.alterConsumerGroupOffsets(groupId, offsetsToCommit(topics, offset)).all.get()

    } finally {
      Utils.closeQuietly(admin, "AdminClient")
    }
  }

  def sendOffsetCommitRequest(offsets: Seq[(NameAndId, Map[Int, Long])],
                              responses: Seq[(NameAndId, Map[Int, Errors])],
                              versions: Seq[java.lang.Short]): Unit = {

    val requestData = newOffsetCommitRequestData(
      groupId = "group",
      offsets = offsets
    )

    versions.foreach { version =>
      val expectedResponse = newOffsetCommitResponseData(
        version,
        topicPartitions = responses,
      )
      val response = connectAndReceive[OffsetCommitResponse](
        new OffsetCommitRequest.Builder(requestData).build(version)
      )
      assertEquals(expectedResponse, response.data(), s"OffsetCommit version = $version")
    }
  }

  def offsetsToCommit(topics: Seq[NameAndId], offset: Long): java.util.Map[TopicPartition, OffsetAndMetadata] = {
    topics
      .map(t => new TopicPartition(t.name, 0) -> new OffsetAndMetadata(offset, empty(), "metadata"))
      .toMap
      .asJava
  }
}
