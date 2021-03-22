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
import org.easymock.{EasyMock, IArgumentMatcher}
import org.easymock.EasyMock._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class ConsumerGroupServiceTest {

  private val group = "testGroup"
  private val topics = (0 until 5).map(i => s"testTopic$i")
  private val numPartitions = 10
  private val topicPartitions = topics.flatMap(topic => (0 until numPartitions).map(i => new TopicPartition(topic, i)))
  private val admin: Admin = mock(classOf[Admin])

  @Test
  def testAdminRequestsForDescribeOffsets(): Unit = {
    val args = Array("--bootstrap-server", "localhost:9092", "--group", group, "--describe", "--offsets")
    val groupService = consumerGroupService(args)

    expect(admin.describeConsumerGroups(EasyMock.eq(Collections.singletonList(group)), anyObject()))
      .andReturn(describeGroupsResult(ConsumerGroupState.STABLE))
      .once()
    expect(admin.listConsumerGroupOffsets(EasyMock.eq(group), anyObject()))
      .andReturn(listGroupOffsetsResult)
      .once()
    expect(admin.listOffsets(offsetsArgMatcher, anyObject()))
      .andReturn(listOffsetsResult)
      .once()
    replay(admin)

    val (state, assignments) = groupService.collectGroupOffsets(group)
    assertEquals(Some("Stable"), state)
    assertTrue(assignments.nonEmpty)
    assertEquals(topicPartitions.size, assignments.get.size)
    verify(admin)
  }

  @Test
  def testAdminRequestsForResetOffsets(): Unit = {
    val args = Seq("--bootstrap-server", "localhost:9092", "--group", group, "--reset-offsets", "--to-latest")
    val topicsWithoutPartitionsSpecified = topics.tail
    val topicArgs = Seq("--topic", s"${topics.head}:${(0 until numPartitions).mkString(",")}") ++
      topicsWithoutPartitionsSpecified.flatMap(topic => Seq("--topic", topic))
    val groupService = consumerGroupService((args ++ topicArgs).toArray)

    expect(admin.describeConsumerGroups(EasyMock.eq(Collections.singletonList(group)), anyObject()))
      .andReturn(describeGroupsResult(ConsumerGroupState.DEAD))
      .once()
    expect(admin.describeTopics(EasyMock.eq(topicsWithoutPartitionsSpecified.asJava), anyObject()))
      .andReturn(describeTopicsResult(topicsWithoutPartitionsSpecified))
      .once()
    expect(admin.listOffsets(offsetsArgMatcher, anyObject()))
      .andReturn(listOffsetsResult)
      .once()
    replay(admin)

    val resetResult = groupService.resetOffsets()
    assertEquals(Set(group), resetResult.keySet)
    assertEquals(topicPartitions.toSet, resetResult(group).keySet)
    verify(admin)
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
    val result: ListConsumerGroupOffsetsResult = mock(classOf[ListConsumerGroupOffsetsResult]) // mocking since there is no public constructor
    expect(result.partitionsToOffsetAndMetadata()).andReturn(KafkaFuture.completedFuture(offsets)).anyTimes()
    replay(result)
    result
  }

  private def offsetsArgMatcher: util.Map[TopicPartition, OffsetSpec] = {
    val expectedOffsets = topicPartitions.map(tp => tp -> OffsetSpec.latest).toMap
    EasyMock.reportMatcher(new IArgumentMatcher {
      override def matches(argument: Any): Boolean = {
        argument match {
          case map: util.Map[_, _] =>
            map.keySet.asScala == expectedOffsets.keySet && map.values.asScala.forall(_.isInstanceOf[OffsetSpec.LatestSpec])
          case _ =>
            false
        }
      }

      override def appendTo(buffer: StringBuffer): Unit = buffer.append(s"partitionOffsets($expectedOffsets)")
    })
    expectedOffsets.asJava
  }

  private def listOffsetsResult: ListOffsetsResult = {
    val resultInfo = new ListOffsetsResult.ListOffsetsResultInfo(100, System.currentTimeMillis, Optional.of(1))
    val futures = topicPartitions.map(_ -> KafkaFuture.completedFuture(resultInfo)).toMap
    new ListOffsetsResult(futures.asJava)
  }

  private def describeTopicsResult(topics: Seq[String]): DescribeTopicsResult = {
    val result: DescribeTopicsResult = mock(classOf[DescribeTopicsResult])  // mocking since there is no public constructor
    val topicDescriptions = topics.map { topic =>
      val partitions = (0 until numPartitions).map(i => new TopicPartitionInfo(i, null, Collections.emptyList[Node], Collections.emptyList[Node]))
      topic -> new TopicDescription(topic, false, partitions.asJava)
    }.toMap
    expect(result.all()).andReturn(KafkaFuture.completedFuture(topicDescriptions.asJava)).anyTimes()
    replay(result)
    result
  }
}
