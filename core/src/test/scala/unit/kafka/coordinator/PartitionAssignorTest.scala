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

package kafka.coordinator

import kafka.common.TopicAndPartition

import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class PartitionAssignorTest extends JUnitSuite {

  @Test
  def testRangeAssignorOneConsumerNoTopic() {
    val consumer = "consumer"
    val assignor = new RangeAssignor()
    val topicsPerConsumer = Map(consumer -> Set.empty[String])
    val partitionsPerTopic = Map.empty[String, Int]
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(consumer -> Set.empty[TopicAndPartition])
    assertEquals(expected, actual)
  }

  @Test
  def testRangeAssignorOneConsumerNonexistentTopic() {
    val topic = "topic"
    val consumer = "consumer"
    val assignor = new RangeAssignor()
    val topicsPerConsumer = Map(consumer -> Set(topic))
    val partitionsPerTopic = Map(topic -> 0)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(consumer -> Set.empty[TopicAndPartition])
    assertEquals(expected, actual)
  }

  @Test
  def testRangeAssignorOneConsumerOneTopic() {
    val topic = "topic"
    val consumer = "consumer"
    val numPartitions = 3
    val assignor = new RangeAssignor()
    val topicsPerConsumer = Map(consumer -> Set(topic))
    val partitionsPerTopic = Map(topic -> numPartitions)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(consumer -> topicAndPartitions(Map(topic -> Set(0, 1, 2))))
    assertEquals(expected, actual)
  }

  @Test
  def testRangeAssignorOnlyAssignsPartitionsFromSubscribedTopics() {
    val subscribedTopic = "topic"
    val otherTopic = "other"
    val consumer = "consumer"
    val subscribedTopicNumPartitions = 3
    val otherTopicNumPartitions = 3
    val assignor = new RangeAssignor()
    val topicsPerConsumer = Map(consumer -> Set(subscribedTopic))
    val partitionsPerTopic = Map(subscribedTopic -> subscribedTopicNumPartitions, otherTopic -> otherTopicNumPartitions)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(consumer -> topicAndPartitions(Map(subscribedTopic -> Set(0, 1, 2))))
    assertEquals(expected, actual)
  }

  @Test
  def testRangeAssignorOneConsumerMultipleTopics() {
    val topic1 = "topic1"
    val topic2 = "topic2"
    val consumer = "consumer"
    val numTopic1Partitions = 1
    val numTopic2Partitions = 2
    val assignor = new RangeAssignor()
    val topicsPerConsumer = Map(consumer -> Set(topic1, topic2))
    val partitionsPerTopic = Map(topic1 -> numTopic1Partitions, topic2 -> numTopic2Partitions)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(consumer -> topicAndPartitions(Map(topic1 -> Set(0), topic2 -> Set(0, 1))))
    assertEquals(expected, actual)
  }

  @Test
  def testRangeAssignorTwoConsumersOneTopicOnePartition() {
    val topic = "topic"
    val consumer1 = "consumer1"
    val consumer2 = "consumer2"
    val numPartitions = 1
    val assignor = new RangeAssignor()
    val topicsPerConsumer = Map(consumer1 -> Set(topic), consumer2 -> Set(topic))
    val partitionsPerTopic = Map(topic -> numPartitions)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(
      consumer1 -> topicAndPartitions(Map(topic -> Set(0))),
      consumer2 -> Set.empty[TopicAndPartition])
    assertEquals(expected, actual)
  }

  @Test
  def testRangeAssignorTwoConsumersOneTopicTwoPartitions() {
    val topic = "topic"
    val consumer1 = "consumer1"
    val consumer2 = "consumer2"
    val numPartitions = 2
    val assignor = new RangeAssignor()
    val topicsPerConsumer = Map(consumer1 -> Set(topic), consumer2 -> Set(topic))
    val partitionsPerTopic = Map(topic -> numPartitions)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(
      consumer1 -> topicAndPartitions(Map(topic -> Set(0))),
      consumer2 -> topicAndPartitions(Map(topic -> Set(1))))
    assertEquals(expected, actual)
  }

  @Test
  def testRangeAssignorMultipleConsumersMixedTopics() {
    val topic1 = "topic1"
    val topic2 = "topic2"
    val consumer1 = "consumer1"
    val consumer2 = "consumer2"
    val consumer3 = "consumer3"
    val numTopic1Partitions = 3
    val numTopic2Partitions = 2
    val assignor = new RangeAssignor()
    val topicsPerConsumer = Map(consumer1 -> Set(topic1), consumer2 -> Set(topic1, topic2), consumer3 -> Set(topic1))
    val partitionsPerTopic = Map(topic1 -> numTopic1Partitions, topic2 -> numTopic2Partitions)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(
      consumer1 -> topicAndPartitions(Map(topic1 -> Set(0))),
      consumer2 -> topicAndPartitions(Map(topic1 -> Set(1), topic2 -> Set(0, 1))),
      consumer3 -> topicAndPartitions(Map(topic1 -> Set(2))))
    assertEquals(expected, actual)
  }

  @Test
  def testRangeAssignorTwoConsumersTwoTopicsSixPartitions() {
    val topic1 = "topic1"
    val topic2 = "topic2"
    val consumer1 = "consumer1"
    val consumer2 = "consumer2"
    val numTopic1Partitions = 3
    val numTopic2Partitions = 3
    val assignor = new RangeAssignor()
    val topicsPerConsumer = Map(consumer1 -> Set(topic1, topic2), consumer2 -> Set(topic1, topic2))
    val partitionsPerTopic = Map(topic1 -> numTopic1Partitions, topic2 -> numTopic2Partitions)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(
      consumer1 -> topicAndPartitions(Map(topic1 -> Set(0, 1), topic2 -> Set(0, 1))),
      consumer2 -> topicAndPartitions(Map(topic1 -> Set(2), topic2 -> Set(2))))
    assertEquals(expected, actual)
  }

  @Test
  def testRoundRobinAssignorOneConsumerNoTopic() {
    val consumer = "consumer"
    val assignor = new RoundRobinAssignor()
    val topicsPerConsumer = Map(consumer -> Set.empty[String])
    val partitionsPerTopic = Map.empty[String, Int]
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(consumer -> Set.empty[TopicAndPartition])
    assertEquals(expected, actual)
  }

  @Test
  def testRoundRobinAssignorOneConsumerNonexistentTopic() {
    val topic = "topic"
    val consumer = "consumer"
    val assignor = new RoundRobinAssignor()
    val topicsPerConsumer = Map(consumer -> Set(topic))
    val partitionsPerTopic = Map(topic -> 0)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(consumer -> Set.empty[TopicAndPartition])
    assertEquals(expected, actual)
  }

  @Test
  def testRoundRobinAssignorOneConsumerOneTopic() {
    val topic = "topic"
    val consumer = "consumer"
    val numPartitions = 3
    val assignor = new RoundRobinAssignor()
    val topicsPerConsumer = Map(consumer -> Set(topic))
    val partitionsPerTopic = Map(topic -> numPartitions)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(consumer -> topicAndPartitions(Map(topic -> Set(0, 1, 2))))
    assertEquals(expected, actual)
  }

  @Test
  def testRoundRobinAssignorOnlyAssignsPartitionsFromSubscribedTopics() {
    val subscribedTopic = "topic"
    val otherTopic = "other"
    val consumer = "consumer"
    val subscribedTopicNumPartitions = 3
    val otherTopicNumPartitions = 3
    val assignor = new RoundRobinAssignor()
    val topicsPerConsumer = Map(consumer -> Set(subscribedTopic))
    val partitionsPerTopic = Map(subscribedTopic -> subscribedTopicNumPartitions, otherTopic -> otherTopicNumPartitions)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(consumer -> topicAndPartitions(Map(subscribedTopic -> Set(0, 1, 2))))
    assertEquals(expected, actual)
  }

  @Test
  def testRoundRobinAssignorOneConsumerMultipleTopics() {
    val topic1 = "topic1"
    val topic2 = "topic2"
    val consumer = "consumer"
    val numTopic1Partitions = 1
    val numTopic2Partitions = 2
    val assignor = new RoundRobinAssignor()
    val topicsPerConsumer = Map(consumer -> Set(topic1, topic2))
    val partitionsPerTopic = Map(topic1 -> numTopic1Partitions, topic2 -> numTopic2Partitions)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(consumer -> topicAndPartitions(Map(topic1 -> Set(0), topic2 -> Set(0, 1))))
    assertEquals(expected, actual)
  }

  @Test
  def testRoundRobinAssignorTwoConsumersOneTopicOnePartition() {
    val topic = "topic"
    val consumer1 = "consumer1"
    val consumer2 = "consumer2"
    val numPartitions = 1
    val assignor = new RoundRobinAssignor()
    val topicsPerConsumer = Map(consumer1 -> Set(topic), consumer2 -> Set(topic))
    val partitionsPerTopic = Map(topic -> numPartitions)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(
      consumer1 -> topicAndPartitions(Map(topic -> Set(0))),
      consumer2 -> Set.empty[TopicAndPartition])
    assertEquals(expected, actual)
  }

  @Test
  def testRoundRobinAssignorTwoConsumersOneTopicTwoPartitions() {
    val topic = "topic"
    val consumer1 = "consumer1"
    val consumer2 = "consumer2"
    val numPartitions = 2
    val assignor = new RoundRobinAssignor()
    val topicsPerConsumer = Map(consumer1 -> Set(topic), consumer2 -> Set(topic))
    val partitionsPerTopic = Map(topic -> numPartitions)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(
      consumer1 -> topicAndPartitions(Map(topic -> Set(0))),
      consumer2 -> topicAndPartitions(Map(topic -> Set(1))))
    assertEquals(expected, actual)
  }

  @Test
  def testRoundRobinAssignorMultipleConsumersMixedTopics() {
    val topic1 = "topic1"
    val topic2 = "topic2"
    val consumer1 = "consumer1"
    val consumer2 = "consumer2"
    val consumer3 = "consumer3"
    val numTopic1Partitions = 3
    val numTopic2Partitions = 2
    val assignor = new RoundRobinAssignor()
    val topicsPerConsumer = Map(consumer1 -> Set(topic1), consumer2 -> Set(topic1, topic2), consumer3 -> Set(topic1))
    val partitionsPerTopic = Map(topic1 -> numTopic1Partitions, topic2 -> numTopic2Partitions)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(
      consumer1 -> topicAndPartitions(Map(topic1 -> Set(0))),
      consumer2 -> topicAndPartitions(Map(topic1 -> Set(1), topic2 -> Set(0, 1))),
      consumer3 -> topicAndPartitions(Map(topic1 -> Set(2))))
    assertEquals(expected, actual)
  }

  @Test
  def testRoundRobinAssignorTwoConsumersTwoTopicsSixPartitions() {
    val topic1 = "topic1"
    val topic2 = "topic2"
    val consumer1 = "consumer1"
    val consumer2 = "consumer2"
    val numTopic1Partitions = 3
    val numTopic2Partitions = 3
    val assignor = new RoundRobinAssignor()
    val topicsPerConsumer = Map(consumer1 -> Set(topic1, topic2), consumer2 -> Set(topic1, topic2))
    val partitionsPerTopic = Map(topic1 -> numTopic1Partitions, topic2 -> numTopic2Partitions)
    val actual = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    val expected = Map(
      consumer1 -> topicAndPartitions(Map(topic1 -> Set(0, 2), topic2 -> Set(1))),
      consumer2 -> topicAndPartitions(Map(topic1 -> Set(1), topic2 -> Set(0, 2))))
    assertEquals(expected, actual)
  }

  private def topicAndPartitions(topicPartitions: Map[String, Set[Int]]): Set[TopicAndPartition] = {
    topicPartitions.flatMap { case (topic, partitions) =>
      partitions.map(partition => TopicAndPartition(topic, partition))
    }.toSet
  }
}
