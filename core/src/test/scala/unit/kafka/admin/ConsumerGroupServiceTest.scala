/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import java.util
import java.util.{Collections, Optional}

import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, ConsumerGroupService}
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, RangeAssignor}
import org.apache.kafka.common.{ConsumerGroupState, KafkaFuture, Node, TopicPartition, TopicPartitionInfo}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import scala.jdk.CollectionConverters._

class ConsumerGroupServiceTest {

  private val group = "testGroup"
  private val topics = (0 until 5).map(i => s"testTopic$i")
  private val numPartitions = 10
  private val topicPartitions = topics.flatMap(topic => (0 until numPartitions).map(i => new TopicPartition(topic, i)))
  private val admin = mock(classOf[Admin])

  @Test
  def testAdminRequestsForDescribeOffsets(): Unit = {
    val args = Array("--bootstrap-server", "localhost:9092", "--group", group, "--describe", "--offsets")
    val groupService = consumerGroupService(args)

    when(admin.describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(group)), any()))
      .thenReturn(describeGroupsResult(ConsumerGroupState.STABLE))
    when(admin.listConsumerGroupOffsets(ArgumentMatchers.eq(group), any()))
      .thenReturn(listGroupOffsetsResult)
    when(admin.listOffsets(offsetsArgMatcher, any()))
      .thenReturn(listOffsetsResult)

    val (state, assignments) = groupService.collectGroupOffsets(group)
    assertEquals(Some("Stable"), state)
    assertTrue(assignments.nonEmpty)
    assertEquals(topicPartitions.size, assignments.get.size)

    verify(admin, times(1)).describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(group)), any())
    verify(admin, times(1)).listConsumerGroupOffsets(ArgumentMatchers.eq(group), any())
    verify(admin, times(1)).listOffsets(offsetsArgMatcher, any())
  }

  @Test
  def testAdminRequestsForResetOffsets(): Unit = {
    val args = Seq("--bootstrap-server", "localhost:9092", "--group", group, "--reset-offsets", "--to-latest")
    val topicsWithoutPartitionsSpecified = topics.tail
    val topicArgs = Seq("--topic", s"${topics.head}:${(0 until numPartitions).mkString(",")}") ++
      topicsWithoutPartitionsSpecified.flatMap(topic => Seq("--topic", topic))
    val groupService = consumerGroupService((args ++ topicArgs).toArray)

    when(admin.describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(group)), any()))
      .thenReturn(describeGroupsResult(ConsumerGroupState.DEAD))
    when(admin.describeTopics(ArgumentMatchers.eq(topicsWithoutPartitionsSpecified.asJava), any()))
      .thenReturn(describeTopicsResult(topicsWithoutPartitionsSpecified))
    when(admin.listOffsets(offsetsArgMatcher, any()))
      .thenReturn(listOffsetsResult)

    val resetResult = groupService.resetOffsets()
    assertEquals(Set(group), resetResult.keySet)
    assertEquals(topicPartitions.toSet, resetResult(group).keySet)

    verify(admin, times(1)).describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(group)), any())
    verify(admin, times(1)).describeTopics(ArgumentMatchers.eq(topicsWithoutPartitionsSpecified.asJava), any())
    verify(admin, times(1)).listOffsets(offsetsArgMatcher, any())
  }

  private def consumerGroupService(args: Array[String]): ConsumerGroupService = {
    new ConsumerGroupService(new ConsumerGroupCommandOptions(args)) {
      override protected def createAdminClient(configOverrides: collection.Map[String, String]): Admin = {
        admin
      }
    }
  }

  private def describeGroupsResult(groupState: ConsumerGroupState): DescribeConsumerGroupsResult = {
    val member1 = new MemberDescription("member1", Optional.of("instance1"), "client1", "host1", null)
    val description = new ConsumerGroupDescription(group,
      true,
      Collections.singleton(member1),
      classOf[RangeAssignor].getName,
      groupState,
      new Node(1, "localhost", 9092))
    new DescribeConsumerGroupsResult(Collections.singletonMap(group, KafkaFuture.completedFuture(description)))
  }

  private def listGroupOffsetsResult: ListConsumerGroupOffsetsResult = {
    val offsets = topicPartitions.map(_ -> new OffsetAndMetadata(100)).toMap.asJava
    AdminClientTestUtils.listConsumerGroupOffsetsResult(offsets)
  }

  private def offsetsArgMatcher: util.Map[TopicPartition, OffsetSpec] = {
    val expectedOffsets = topicPartitions.map(tp => tp -> OffsetSpec.latest).toMap
    ArgumentMatchers.argThat[util.Map[TopicPartition, OffsetSpec]] { map =>
      map.keySet.asScala == expectedOffsets.keySet && map.values.asScala.forall(_.isInstanceOf[OffsetSpec.LatestSpec])
    }
  }

  private def listOffsetsResult: ListOffsetsResult = {
    val resultInfo = new ListOffsetsResult.ListOffsetsResultInfo(100, System.currentTimeMillis, Optional.of(1))
    val futures = topicPartitions.map(_ -> KafkaFuture.completedFuture(resultInfo)).toMap
    new ListOffsetsResult(futures.asJava)
  }

  private def describeTopicsResult(topics: Seq[String]): DescribeTopicsResult = {
   val topicDescriptions = topics.map { topic =>
      val partitions = (0 until numPartitions).map(i => new TopicPartitionInfo(i, null, Collections.emptyList[Node], Collections.emptyList[Node]))
      topic -> new TopicDescription(topic, false, partitions.asJava)
    }.toMap
    AdminClientTestUtils.describeTopicsResult(topicDescriptions.asJava)
  }
}
