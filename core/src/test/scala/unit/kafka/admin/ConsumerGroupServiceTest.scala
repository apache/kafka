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
import org.mockito.ArgumentMatcher

import scala.jdk.CollectionConverters._
import org.apache.kafka.common.internals.KafkaFutureImpl

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
    when(admin.listConsumerGroupOffsets(ArgumentMatchers.eq(listConsumerGroupOffsetsSpec), any()))
      .thenReturn(listGroupOffsetsResult(group))
    when(admin.listOffsets(offsetsArgMatcher, any()))
      .thenReturn(listOffsetsResult)

    val (state, assignments) = groupService.collectGroupOffsets(group)
    assertEquals(Some("Stable"), state)
    assertTrue(assignments.nonEmpty)
    assertEquals(topicPartitions.size, assignments.get.size)

    verify(admin, times(1)).describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(group)), any())
    verify(admin, times(1)).listConsumerGroupOffsets(ArgumentMatchers.eq(listConsumerGroupOffsetsSpec), any())
    verify(admin, times(1)).listOffsets(offsetsArgMatcher, any())
  }

  @Test
  def testAdminRequestsForDescribeNegativeOffsets(): Unit = {
    val args = Array("--bootstrap-server", "localhost:9092", "--group", group, "--describe", "--offsets")
    val groupService = consumerGroupService(args)

    val testTopicPartition0 = new TopicPartition("testTopic1", 0);
    val testTopicPartition1 = new TopicPartition("testTopic1", 1);
    val testTopicPartition2 = new TopicPartition("testTopic1", 2);
    val testTopicPartition3 = new TopicPartition("testTopic2", 0);
    val testTopicPartition4 = new TopicPartition("testTopic2", 1);
    val testTopicPartition5 = new TopicPartition("testTopic2", 2);

    // Some topic's partitions gets valid OffsetAndMetadata values, other gets nulls values (negative integers) and others aren't defined
    val committedOffsets = Map(
      testTopicPartition1 -> new OffsetAndMetadata(100),
      testTopicPartition2 -> null,
      testTopicPartition3 -> new OffsetAndMetadata(100),
      testTopicPartition4 -> new OffsetAndMetadata(100),
      testTopicPartition5 -> null,
    ).asJava

    val resultInfo = new ListOffsetsResult.ListOffsetsResultInfo(100, System.currentTimeMillis, Optional.of(1))
    val endOffsets = Map(
      testTopicPartition0 -> KafkaFuture.completedFuture(resultInfo),
      testTopicPartition1 -> KafkaFuture.completedFuture(resultInfo),
      testTopicPartition2 -> KafkaFuture.completedFuture(resultInfo),
      testTopicPartition3 -> KafkaFuture.completedFuture(resultInfo),
      testTopicPartition4 -> KafkaFuture.completedFuture(resultInfo),
      testTopicPartition5 -> KafkaFuture.completedFuture(resultInfo),
    )
    val assignedTopicPartitions = Set(testTopicPartition0, testTopicPartition1, testTopicPartition2)
    val unassignedTopicPartitions = Set(testTopicPartition3, testTopicPartition4, testTopicPartition5)

    val consumerGroupDescription = new ConsumerGroupDescription(group,
      true,
      Collections.singleton(new MemberDescription("member1", Optional.of("instance1"), "client1", "host1", new MemberAssignment(assignedTopicPartitions.asJava))),
      classOf[RangeAssignor].getName,
      ConsumerGroupState.STABLE,
      new Node(1, "localhost", 9092))

    def offsetsArgMatcher(expectedPartitions: Set[TopicPartition]): ArgumentMatcher[util.Map[TopicPartition, OffsetSpec]] = {
      topicPartitionOffsets => topicPartitionOffsets != null && topicPartitionOffsets.keySet.asScala.equals(expectedPartitions)
    }

    val future = new KafkaFutureImpl[ConsumerGroupDescription]()
    future.complete(consumerGroupDescription)
    when(admin.describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(group)), any()))
      .thenReturn(new DescribeConsumerGroupsResult(Collections.singletonMap(group, future)))
    when(admin.listConsumerGroupOffsets(ArgumentMatchers.eq(listConsumerGroupOffsetsSpec), any()))
      .thenReturn(
        AdminClientTestUtils.listConsumerGroupOffsetsResult(
          Collections.singletonMap(group, committedOffsets)))
    when(admin.listOffsets(
      ArgumentMatchers.argThat(offsetsArgMatcher(assignedTopicPartitions)),
      any()
    )).thenReturn(new ListOffsetsResult(endOffsets.filter { case (tp, _) => assignedTopicPartitions.contains(tp) }.asJava))
    when(admin.listOffsets(
      ArgumentMatchers.argThat(offsetsArgMatcher(unassignedTopicPartitions)),
      any()
    )).thenReturn(new ListOffsetsResult(endOffsets.filter { case (tp, _) => unassignedTopicPartitions.contains(tp) }.asJava))

    val (state, assignments) = groupService.collectGroupOffsets(group)
    val returnedOffsets = assignments.map { results =>
      results.map { assignment =>
        new TopicPartition(assignment.topic.get, assignment.partition.get) -> assignment.offset
      }.toMap
    }.getOrElse(Map.empty)

    val expectedOffsets = Map(
      testTopicPartition0 -> None,
      testTopicPartition1 -> Some(100),
      testTopicPartition2 -> None,
      testTopicPartition3 -> Some(100),
      testTopicPartition4 -> Some(100),
      testTopicPartition5 -> None
    )
    assertEquals(Some("Stable"), state)
    assertEquals(expectedOffsets, returnedOffsets)

    verify(admin, times(1)).describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(group)), any())
    verify(admin, times(1)).listConsumerGroupOffsets(ArgumentMatchers.eq(listConsumerGroupOffsetsSpec), any())
    verify(admin, times(1)).listOffsets(ArgumentMatchers.argThat(offsetsArgMatcher(assignedTopicPartitions)), any())
    verify(admin, times(1)).listOffsets(ArgumentMatchers.argThat(offsetsArgMatcher(unassignedTopicPartitions)), any())
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
    val future = new KafkaFutureImpl[ConsumerGroupDescription]()
    future.complete(description)
    new DescribeConsumerGroupsResult(Collections.singletonMap(group, future))
  }

  private def listGroupOffsetsResult(groupId: String): ListConsumerGroupOffsetsResult = {
    val offsets = topicPartitions.map(_ -> new OffsetAndMetadata(100)).toMap.asJava
    AdminClientTestUtils.listConsumerGroupOffsetsResult(Map(groupId -> offsets).asJava)
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

  private def listConsumerGroupOffsetsSpec: util.Map[String, ListConsumerGroupOffsetsSpec] = {
    Collections.singletonMap(group, new ListConsumerGroupOffsetsSpec())
  }
}
