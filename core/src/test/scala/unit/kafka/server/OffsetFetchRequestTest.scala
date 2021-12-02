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
import org.apache.kafka.common.message.OffsetFetchRequestData.{OffsetFetchRequestGroup, OffsetFetchRequestTopics}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData
import org.apache.kafka.common.requests.{AbstractResponse, OffsetFetchRequest, OffsetFetchResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test, TestInfo}
import java.util
import java.util.Collections.singletonList

import scala.jdk.CollectionConverters._
import java.util.{Optional, Properties}

class OffsetFetchRequestTest extends BaseRequestTest {

  override def brokerCount: Int = 1

  val brokerId: Integer = 0
  val offset = 15L
  val leaderEpoch: Optional[Integer] = Optional.of(3)
  val metadata = "metadata"
  val topic = "topic"
  val groupId = "groupId"
  val groups: Seq[String] = (1 to 5).map(i => s"group$i")
  val topics: Seq[String] = (1 to 3).map(i => s"topic$i")
  val topic1List = singletonList(new TopicPartition(topics(0), 0))
  val topic1And2List = util.Arrays.asList(
    new TopicPartition(topics(0), 0),
    new TopicPartition(topics(1), 0),
    new TopicPartition(topics(1), 1))
  val allTopicsList = util.Arrays.asList(
    new TopicPartition(topics(0), 0),
    new TopicPartition(topics(1), 0),
    new TopicPartition(topics(1), 1),
    new TopicPartition(topics(2), 0),
    new TopicPartition(topics(2), 1),
    new TopicPartition(topics(2), 2))
  val groupToPartitionMap: util.Map[String, util.List[TopicPartition]] =
    new util.HashMap[String, util.List[TopicPartition]]()
  groupToPartitionMap.put(groups(0), topic1List)
  groupToPartitionMap.put(groups(1), topic1And2List)
  groupToPartitionMap.put(groups(2), allTopicsList)
  groupToPartitionMap.put(groups(3), null)
  groupToPartitionMap.put(groups(4), null)

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.BrokerIdProp, brokerId.toString)
    properties.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.TransactionsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.TransactionsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.TransactionsTopicMinISRProp, "1")
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    doSetup(testInfo, createOffsetsTopic = false)

    TestUtils.createOffsetsTopic(zkClient, servers)
  }

  @Test
  def testOffsetFetchRequestSingleGroup(): Unit = {
    createTopic(topic)

    val tpList = singletonList(new TopicPartition(topic, 0))
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    commitOffsets(tpList)

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
        verifySingleGroupResponse(version.asInstanceOf[Short],
          response.error().code(), partitionData.errorCode(), topicData.name(),
          partitionData.partitionIndex(), partitionData.committedOffset(),
          partitionData.committedLeaderEpoch(), partitionData.metadata())
      } else {
        val request = new OffsetFetchRequest.Builder(
          Map(groupId -> tpList).asJava, false, false)
          .build(version.asInstanceOf[Short])
        val response = connectAndReceive[OffsetFetchResponse](request)
        val groupData = response.data().groups().get(0)
        val topicData = groupData.topics().get(0)
        val partitionData = topicData.partitions().get(0)
        verifySingleGroupResponse(version.asInstanceOf[Short],
          groupData.errorCode(), partitionData.errorCode(), topicData.name(),
          partitionData.partitionIndex(), partitionData.committedOffset(),
          partitionData.committedLeaderEpoch(), partitionData.metadata())
      }
    }
  }

  @Test
  def testOffsetFetchRequestWithMultipleGroups(): Unit = {
    createTopic(topics(0))
    createTopic(topics(1), numPartitions = 2)
    createTopic(topics(2), numPartitions = 3)

    // create 5 consumers to commit offsets so we can fetch them later
    val partitionMap = groupToPartitionMap.asScala.map(e => (e._1, Option(e._2).getOrElse(allTopicsList)))
    groups.foreach { groupId =>
      consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
      commitOffsets(partitionMap(groupId))
    }

    for (version <- 8 to ApiKeys.OFFSET_FETCH.latestVersion()) {
      val request =  new OffsetFetchRequest.Builder(groupToPartitionMap, false, false)
        .build(version.asInstanceOf[Short])
      val response = connectAndReceive[OffsetFetchResponse](request)
      response.data.groups.asScala.map(_.groupId).foreach( groupId =>
        verifyResponse(response.groupLevelError(groupId), response.partitionDataMap(groupId), partitionMap(groupId))
      )
    }
  }

  @Test
  def testOffsetFetchRequestWithMultipleGroupsWithOneGroupRepeating(): Unit = {
    createTopic(topics(0))
    createTopic(topics(1), numPartitions = 2)
    createTopic(topics(2), numPartitions = 3)

    // create 5 consumers to commit offsets so we can fetch them later
    val partitionMap = groupToPartitionMap.asScala.map(e => (e._1, Option(e._2).getOrElse(allTopicsList)))
    groups.foreach { groupId =>
      consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
      commitOffsets(partitionMap(groupId))
    }

    for (version <- 8 to ApiKeys.OFFSET_FETCH.latestVersion()) {
      val request = new OffsetFetchRequest.Builder(groupToPartitionMap, false, false)
        .build(version.asInstanceOf[Short])
      val requestGroups = request.data().groups()
      requestGroups.add(
        // add the same group as before with different topic partitions
        new OffsetFetchRequestGroup()
          .setGroupId(groups(2))
          .setTopics(singletonList(
            new OffsetFetchRequestTopics()
              .setName(topics(0))
              .setPartitionIndexes(singletonList(0)))))
      request.data().setGroups(requestGroups)
      val response = connectAndReceive[OffsetFetchResponse](request)
      response.data.groups.asScala.map(_.groupId).foreach( groupId =>
        if (groupId == "group3") // verify that the response gives back the latest changed topic partition list
          verifyResponse(response.groupLevelError(groupId), response.partitionDataMap(groupId), topic1List)
        else
          verifyResponse(response.groupLevelError(groupId), response.partitionDataMap(groupId), partitionMap(groupId))
      )
    }
  }

  private def verifySingleGroupResponse(version: Short,
                                        responseError: Short,
                                        partitionError: Short,
                                        topicName: String,
                                        partitionIndex: Integer,
                                        committedOffset: Long,
                                        committedLeaderEpoch: Integer,
                                        partitionMetadata: String): Unit = {
    assertEquals(Errors.NONE.code(), responseError)
    assertEquals(topic, topicName)
    assertEquals(0, partitionIndex)
    assertEquals(offset, committedOffset)
    if (version >= 5) {
      assertEquals(leaderEpoch.get(), committedLeaderEpoch)
    }
    assertEquals(metadata, partitionMetadata)
    assertEquals(Errors.NONE.code(), partitionError)
  }

  private def verifyPartitionData(partitionData: OffsetFetchResponse.PartitionData): Unit = {
    assertTrue(!partitionData.hasError)
    assertEquals(offset, partitionData.offset)
    assertEquals(metadata, partitionData.metadata)
    assertEquals(leaderEpoch.get(), partitionData.leaderEpoch.get())
  }

  private def verifyResponse(groupLevelResponse: Errors,
                             partitionData: util.Map[TopicPartition, PartitionData],
                             topicList: util.List[TopicPartition]): Unit = {
    assertEquals(Errors.NONE, groupLevelResponse)
    assertTrue(partitionData.size() == topicList.size())
    topicList.forEach(t => verifyPartitionData(partitionData.get(t)))
  }

  private def commitOffsets(tpList: util.List[TopicPartition]): Unit = {
    val consumer = createConsumer()
    consumer.assign(tpList)
    val offsets = tpList.asScala.map{
      tp => (tp, new OffsetAndMetadata(offset, leaderEpoch, metadata))
    }.toMap.asJava
    consumer.commitSync(offsets)
    consumer.close()
  }
}
