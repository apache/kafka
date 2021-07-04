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

package kafka.server

import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractResponse, OffsetFetchRequest, OffsetFetchResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

import java.util
import java.util.Collections.singletonList
import scala.jdk.CollectionConverters._
import java.util.{Optional, Properties}

class OffsetFetchRequestTest extends BaseRequestTest{

  override def brokerCount: Int = 1

  val brokerId: Integer = 0
  val offset = 15L
  val leaderEpoch: Optional[Integer] = Optional.of(3)
  val metadata = "metadata"

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.BrokerIdProp, brokerId.toString)
    properties.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.TransactionsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.TransactionsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.TransactionsTopicMinISRProp, "1")
  }

  @BeforeEach
  override def setUp(): Unit = {
    doSetup(createOffsetsTopic = false)

    TestUtils.createOffsetsTopic(zkClient, servers)
  }

  @Test
  def testOffsetFetchRequestLessThanV8(): Unit = {
    val topic = "topic"
    createTopic(topic)

    val groupId = "groupId"
    val tpList = singletonList(new TopicPartition(topic, 0))
    val topicOffsets = tpList.asScala.map{
      tp => (tp, new OffsetAndMetadata(offset, leaderEpoch, metadata))
    }.toMap.asJava

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val consumer = createConsumer()
    consumer.assign(tpList)
    consumer.commitSync(topicOffsets)
    consumer.close()
    // testing from version 1 onward since version 0 read offsets from ZK
    for (version <- 1 to ApiKeys.OFFSET_FETCH.latestVersion()) {
      if (version < 8) {
        val request =
          if (version < 7) {
            new OffsetFetchRequest.Builder(
              groupId, false, tpList, false)
              .build(version.asInstanceOf[Short])
          } else {
            new OffsetFetchRequest.Builder(
              groupId, false, tpList, true)
              .build(version.asInstanceOf[Short])
          }
        val response = connectAndReceive[OffsetFetchResponse](request)
        val topicData = response.data().topics().get(0)
        val partitionData = topicData.partitions().get(0)
        if (version < 3) {
          assertEquals(AbstractResponse.DEFAULT_THROTTLE_TIME, response.throttleTimeMs())
        }
        assertEquals(Errors.NONE, response.error())
        assertEquals(topic, topicData.name())
        assertEquals(0, partitionData.partitionIndex())
        assertEquals(offset, partitionData.committedOffset())
        if (version >= 5) {
          // committed leader epoch introduced with V5
          assertEquals(leaderEpoch.get(), partitionData.committedLeaderEpoch())
        }
        assertEquals(metadata, partitionData.metadata())
        assertEquals(Errors.NONE.code(), partitionData.errorCode())
      }
    }
  }

  @Test
  def testOffsetFetchRequestV8AndAbove(): Unit = {
    val groupOne = "group1"
    val groupTwo = "group2"
    val groupThree = "group3"
    val groupFour = "group4"
    val groupFive = "group5"

    val topic1 = "topic1"
    val topic1List = singletonList(new TopicPartition(topic1, 0))
    val topic2 = "topic2"
    val topic1And2List = util.Arrays.asList(
      new TopicPartition(topic1, 0),
      new TopicPartition(topic2, 0),
      new TopicPartition(topic2, 1))
    val topic3 = "topic3"
    val allTopicsList = util.Arrays.asList(
      new TopicPartition(topic1, 0),
      new TopicPartition(topic2, 0),
      new TopicPartition(topic2, 1),
      new TopicPartition(topic3, 0),
      new TopicPartition(topic3, 1),
      new TopicPartition(topic3, 2))

    // create group to partition map to build batched offsetFetch request
    val groupToPartitionMap: util.Map[String, util.List[TopicPartition]] = new util.HashMap[String, util
    .List[TopicPartition]]()
    groupToPartitionMap.put(groupOne, topic1List)
    groupToPartitionMap.put(groupTwo, topic1And2List)
    groupToPartitionMap.put(groupThree, allTopicsList)
    groupToPartitionMap.put(groupFour, null)
    groupToPartitionMap.put(groupFive, null)

    createTopic(topic1)
    createTopic(topic2, numPartitions = 2)
    createTopic(topic3, numPartitions = 3)

    val topicOneOffsets = topic1List.asScala.map{
      tp => (tp, new OffsetAndMetadata(offset, leaderEpoch, metadata))
    }.toMap.asJava
    val topicOneAndTwoOffsets = topic1And2List.asScala.map{
      tp => (tp, new OffsetAndMetadata(offset, leaderEpoch, metadata))
    }.toMap.asJava
    val allTopicOffsets = allTopicsList.asScala.map{
      tp => (tp, new OffsetAndMetadata(offset, leaderEpoch, metadata))
    }.toMap.asJava

    // create 5 consumers to commit offsets so we can fetch them later

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupOne)
    var consumer = createConsumer()
    consumer.assign(topic1List)
    consumer.commitSync(topicOneOffsets)
    consumer.close()

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupTwo)
    consumer = createConsumer()
    consumer.assign(topic1And2List)
    consumer.commitSync(topicOneAndTwoOffsets)
    consumer.close()

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupThree)
    consumer = createConsumer()
    consumer.assign(allTopicsList)
    consumer.commitSync(allTopicOffsets)
    consumer.close()

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupFour)
    consumer = createConsumer()
    consumer.assign(allTopicsList)
    consumer.commitSync(allTopicOffsets)
    consumer.close()

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupFive)
    consumer = createConsumer()
    consumer.assign(allTopicsList)
    consumer.commitSync(allTopicOffsets)
    consumer.close()
    for (version <- 8 to ApiKeys.OFFSET_FETCH.latestVersion()) {
      val request =  new OffsetFetchRequest.Builder(groupToPartitionMap, false, false)
        .build(version.asInstanceOf[Short])
      val response = connectAndReceive[OffsetFetchResponse](request)
      response.data().groupIds().forEach(g =>
        g.groupId() match {
          case "group1" =>
            assertEquals(Errors.NONE, response.groupLevelError(groupOne))
            assertTrue(response.responseData(groupOne).size() == 1)
            assertTrue(response.responseData(groupOne).containsKey(topic1List.get(0)))
          case "group2" =>
            assertEquals(Errors.NONE, response.groupLevelError(groupTwo))
            val group2Response = response.responseData(groupTwo)
            assertTrue(group2Response.size() == 3)
            assertTrue(group2Response.keySet().containsAll(topic1And2List))
            topic1And2List.forEach(t => verifyPartitionData(group2Response.get(t)))
          case "group3" =>
            assertEquals(Errors.NONE, response.groupLevelError(groupThree))
            val group3Response = response.responseData(groupThree)
            assertTrue(group3Response.size() == 6)
            assertTrue(group3Response.keySet().containsAll(allTopicsList))
            allTopicsList.forEach(t => verifyPartitionData(group3Response.get(t)))
          case "group4" =>
            assertEquals(Errors.NONE, response.groupLevelError(groupFour))
            val group4Response = response.responseData(groupFour)
            assertTrue(group4Response.size() == 6)
            allTopicsList.forEach(t => verifyPartitionData(group4Response.get(t)))
          case "group5" =>
            assertEquals(Errors.NONE, response.groupLevelError(groupFive))
            val group5Response = response.responseData(groupFive)
            assertTrue(group5Response.size() == 6)
            allTopicsList.forEach(t => verifyPartitionData(group5Response.get(t)))
        })
    }
  }

  private def verifyPartitionData(partitionData: OffsetFetchResponse.PartitionData): Unit = {
    assertTrue(!partitionData.hasError)
    assertEquals(offset, partitionData.offset)
    assertEquals(metadata, partitionData.metadata)
    assertEquals(leaderEpoch.get(), partitionData.leaderEpoch.get())
  }
}
