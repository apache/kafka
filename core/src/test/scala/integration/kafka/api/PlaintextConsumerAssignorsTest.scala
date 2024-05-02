/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedAssignorException
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, CsvSource, MethodSource}

import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.stream.Stream
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Integration tests for the consumer that covers assignors logic (client and server side assignors)
 */
@Timeout(600)
class PlaintextConsumerAssignorsTest extends AbstractConsumerTest {

  // Only the classic group protocol supports client-side assignors
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly"))
  def testRoundRobinAssignment(quorum: String, groupProtocol: String): Unit = {
    // 1 consumer using round-robin assignment
    this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "roundrobin-group")
    this.consumerConfig.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[RoundRobinAssignor].getName)
    val consumer = createConsumer()

    // create two new topics, each having 2 partitions
    val topic1 = "topic1"
    val topic2 = "topic2"
    val producer = createProducer()
    val expectedAssignment = createTopicAndSendRecords(producer, topic1, 2, 100) ++
      createTopicAndSendRecords(producer, topic2, 2, 100)

    assertEquals(0, consumer.assignment().size)

    // subscribe to two topics
    consumer.subscribe(List(topic1, topic2).asJava)
    awaitAssignment(consumer, expectedAssignment)

    // add one more topic with 2 partitions
    val topic3 = "topic3"
    createTopicAndSendRecords(producer, topic3, 2, 100)

    val newExpectedAssignment = expectedAssignment ++ Set(new TopicPartition(topic3, 0), new TopicPartition(topic3, 1))
    consumer.subscribe(List(topic1, topic2, topic3).asJava)
    awaitAssignment(consumer, newExpectedAssignment)

    // remove the topic we just added
    consumer.subscribe(List(topic1, topic2).asJava)
    awaitAssignment(consumer, expectedAssignment)

    consumer.unsubscribe()
    assertEquals(0, consumer.assignment().size)
  }

  // Only the classic group protocol supports client-side assignors
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly"))
  def testMultiConsumerRoundRobinAssignor(quorum: String, groupProtocol: String): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "roundrobin-group")
    this.consumerConfig.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[RoundRobinAssignor].getName)

    // create two new topics, total number of partitions must be greater than number of consumers
    val topic1 = "topic1"
    val topic2 = "topic2"
    val producer = createProducer()
    val subscriptions = createTopicAndSendRecords(producer, topic1, 5, 100) ++
      createTopicAndSendRecords(producer, topic2, 8, 100)

    // create a group of consumers, subscribe the consumers to all the topics and start polling
    // for the topic partition assignment
    val (consumerGroup, consumerPollers) = createConsumerGroupAndWaitForAssignment(10, List(topic1, topic2), subscriptions)
    try {
      validateGroupAssignment(consumerPollers, subscriptions)

      // add one more consumer and validate re-assignment
      addConsumersToGroupAndWaitForGroupAssignment(1, consumerGroup, consumerPollers,
        List(topic1, topic2), subscriptions, "roundrobin-group")
    } finally {
      consumerPollers.foreach(_.shutdown())
    }
  }

  /**
   * This test runs the following scenario to verify sticky assignor behavior.
   * Topics: single-topic, with random number of partitions, where #par is 10, 20, 30, 40, 50, 60, 70, 80, 90, or 100
   * Consumers: 9 consumers subscribed to the single topic
   * Expected initial assignment: partitions are assigned to consumers in a round robin fashion.
   *  - (#par mod 9) consumers will get (#par / 9 + 1) partitions, and the rest get (#par / 9) partitions
   *    Then consumer #10 is added to the list (subscribing to the same single topic)
   *    Expected new assignment:
   *  - (#par / 10) partition per consumer, where one partition from each of the early (#par mod 9) consumers
   *    will move to consumer #10, leading to a total of (#par mod 9) partition movement
   */
  // Only the classic group protocol supports client-side assignors
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly"))
  def testMultiConsumerStickyAssignor(quorum: String, groupProtocol: String): Unit = {

    def reverse(m: Map[Long, Set[TopicPartition]]) =
      m.values.toSet.flatten.map(v => (v, m.keys.filter(m(_).contains(v)).head)).toMap

    this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "sticky-group")
    this.consumerConfig.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[StickyAssignor].getName)

    // create one new topic
    val topic = "single-topic"
    val rand = 1 + scala.util.Random.nextInt(10)
    val producer = createProducer()
    val partitions = createTopicAndSendRecords(producer, topic, rand * 10, 100)

    // create a group of consumers, subscribe the consumers to the single topic and start polling
    // for the topic partition assignment
    val (consumerGroup, consumerPollers) = createConsumerGroupAndWaitForAssignment(9, List(topic), partitions)
    validateGroupAssignment(consumerPollers, partitions)
    val prePartition2PollerId = reverse(consumerPollers.map(poller => (poller.getId, poller.consumerAssignment())).toMap)

    // add one more consumer and validate re-assignment
    addConsumersToGroupAndWaitForGroupAssignment(1, consumerGroup, consumerPollers, List(topic), partitions, "sticky-group")

    val postPartition2PollerId = reverse(consumerPollers.map(poller => (poller.getId, poller.consumerAssignment())).toMap)
    val keys = prePartition2PollerId.keySet.union(postPartition2PollerId.keySet)
    var changes = 0
    keys.foreach { key =>
      val preVal = prePartition2PollerId.get(key)
      val postVal = postPartition2PollerId.get(key)
      if (preVal.nonEmpty && postVal.nonEmpty) {
        if (preVal.get != postVal.get)
          changes += 1
      } else
        changes += 1
    }

    consumerPollers.foreach(_.shutdown())

    assertEquals(rand, changes, "Expected only two topic partitions that have switched to other consumers.")
  }

  /**
   * This test re-uses BaseConsumerTest's consumers.
   * As a result, it is testing the default assignment strategy set by BaseConsumerTest
   * It tests the assignment results is expected using default assignor (i.e. Range assignor)
   */
  // Only the classic group protocol supports client-side assignors
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly"))
  def testMultiConsumerDefaultAssignorAndVerifyAssignment(quorum: String, groupProtocol: String): Unit = {
    // create two new topics, each having 3 partitions
    val topic1 = "topic1"
    val topic2 = "topic2"

    createTopic(topic1, 3)
    createTopic(topic2, 3)

    val consumersInGroup = mutable.Buffer[Consumer[Array[Byte], Array[Byte]]]()
    consumersInGroup += createConsumer()
    consumersInGroup += createConsumer()

    val tp1_0 = new TopicPartition(topic1, 0)
    val tp1_1 = new TopicPartition(topic1, 1)
    val tp1_2 = new TopicPartition(topic1, 2)
    val tp2_0 = new TopicPartition(topic2, 0)
    val tp2_1 = new TopicPartition(topic2, 1)
    val tp2_2 = new TopicPartition(topic2, 2)

    val subscriptions = Set(tp1_0, tp1_1, tp1_2, tp2_0, tp2_1, tp2_2)
    val consumerPollers = subscribeConsumers(consumersInGroup, List(topic1, topic2))

    val expectedAssignment = mutable.Buffer(Set(tp1_0, tp1_1, tp2_0, tp2_1), Set(tp1_2, tp2_2))

    try {
      validateGroupAssignment(consumerPollers, subscriptions, expectedAssignment = expectedAssignment)
    } finally {
      consumerPollers.foreach(_.shutdown())
    }
  }

  /**
   * This test re-uses BaseConsumerTest's consumers.
   * As a result, it is testing the default assignment strategy set by BaseConsumerTest
   */
  // Only the classic group protocol supports client-side assignors
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly"))
  def testMultiConsumerDefaultAssignor(quorum: String, groupProtocol: String): Unit = {
    // use consumers and topics defined in this class + one more topic
    val producer = createProducer()
    sendRecords(producer, numRecords = 100, tp)
    sendRecords(producer, numRecords = 100, tp2)
    val topic1 = "topic1"
    val subscriptions = Set(tp, tp2) ++ createTopicAndSendRecords(producer, topic1, 5, 100)

    // subscribe all consumers to all topics and validate the assignment

    val consumersInGroup = mutable.Buffer[Consumer[Array[Byte], Array[Byte]]]()
    consumersInGroup += createConsumer()
    consumersInGroup += createConsumer()

    val consumerPollers = subscribeConsumers(consumersInGroup, List(topic, topic1))
    try {
      validateGroupAssignment(consumerPollers, subscriptions)

      // add 2 more consumers and validate re-assignment
      addConsumersToGroupAndWaitForGroupAssignment(2, consumersInGroup, consumerPollers, List(topic, topic1), subscriptions)

      // add one more topic and validate partition re-assignment
      val topic2 = "topic2"
      val expandedSubscriptions = subscriptions ++ createTopicAndSendRecords(producer, topic2, 3, 100)
      changeConsumerGroupSubscriptionAndValidateAssignment(consumerPollers, List(topic, topic1, topic2), expandedSubscriptions)

      // remove the topic we just added and validate re-assignment
      changeConsumerGroupSubscriptionAndValidateAssignment(consumerPollers, List(topic, topic1), subscriptions)

    } finally {
      consumerPollers.foreach(_.shutdown())
    }
  }

  // Remote assignors only supported with consumer group protocol
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @CsvSource(Array(
    "kraft+kip848, consumer"
  ))
  def testRemoteAssignorInvalid(quorum: String, groupProtocol: String): Unit = {
    // 1 consumer using invalid remote assignor
    this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "invalid-assignor-group")
    this.consumerConfig.setProperty(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "invalid")
    val consumer = createConsumer()

    // create two new topics, each having 2 partitions
    val topic1 = "topic1"
    val producer = createProducer()
    val expectedAssignment = createTopicAndSendRecords(producer, topic1, 2, 100)

    assertEquals(0, consumer.assignment().size)

    // subscribe to two topics
    consumer.subscribe(List(topic1).asJava)

    val e: UnsupportedAssignorException = assertThrows(
      classOf[UnsupportedAssignorException],
      () => awaitAssignment(consumer, expectedAssignment)
    )

    assertTrue(e.getMessage.startsWith("ServerAssignor invalid is not supported. " +
      "Supported assignors: "))
  }

  // Remote assignors only supported with consumer group protocol
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @CsvSource(Array(
    "kraft+kip848, consumer"
  ))
  def testRemoteAssignorRange(quorum: String, groupProtocol: String): Unit = {
    // 1 consumer using range assignment
    this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "range-group")
    this.consumerConfig.setProperty(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "range")
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "30000")
    val consumer = createConsumer()

    // create two new topics, each having 2 partitions
    val topic1 = "topic1"
    val topic2 = "topic2"
    val producer = createProducer()
    val expectedAssignment = createTopicAndSendRecords(producer, topic1, 2, 100) ++
      createTopicAndSendRecords(producer, topic2, 2, 100)

    assertEquals(0, consumer.assignment().size)

    // subscribe to two topics
    consumer.subscribe(List(topic1, topic2).asJava)
    awaitAssignment(consumer, expectedAssignment)

    // add one more topic with 2 partitions
    val topic3 = "topic3"
    val additionalAssignment = createTopicAndSendRecords(producer, topic3, 2, 100)

    val newExpectedAssignment = expectedAssignment ++ additionalAssignment
    consumer.subscribe(List(topic1, topic2, topic3).asJava)
    awaitAssignment(consumer, newExpectedAssignment)

    // remove the topic we just added
    consumer.subscribe(List(topic1, topic2).asJava)
    awaitAssignment(consumer, expectedAssignment)

    consumer.unsubscribe()
    assertEquals(0, consumer.assignment().size)
  }

  // Only the classic group protocol supports client-side assignors
  @ParameterizedTest
  @CsvSource(Array(
    "org.apache.kafka.clients.consumer.CooperativeStickyAssignor,   zk",
    "org.apache.kafka.clients.consumer.RangeAssignor,               zk",
    "org.apache.kafka.clients.consumer.CooperativeStickyAssignor,   kraft",
    "org.apache.kafka.clients.consumer.RangeAssignor,               kraft"
  ))
  def testRebalanceAndRejoin(assignmentStrategy: String, quorum: String): Unit = {
    // create 2 consumers
    this.consumerConfig.setProperty(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic")
    this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "rebalance-and-rejoin-group")
    this.consumerConfig.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignmentStrategy)
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    val consumer1 = createConsumer()
    val consumer2 = createConsumer()

    // create a new topic, have 2 partitions
    val topic = "topic1"
    val producer = createProducer()
    val expectedAssignment = createTopicAndSendRecords(producer, topic, 2, 100)

    assertEquals(0, consumer1.assignment().size)
    assertEquals(0, consumer2.assignment().size)

    val lock = new ReentrantLock()
    var generationId1 = -1
    var memberId1 = ""
    val customRebalanceListener = new ConsumerRebalanceListener {
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
      }

      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        if (!lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
          fail(s"Time out while awaiting for lock.")
        }
        try {
          generationId1 = consumer1.groupMetadata().generationId()
          memberId1 = consumer1.groupMetadata().memberId()
        } finally {
          lock.unlock()
        }
      }
    }
    val consumerPoller1 = new ConsumerAssignmentPoller(consumer1, List(topic), Set.empty, customRebalanceListener)
    consumerPoller1.start()
    TestUtils.waitUntilTrue(() => consumerPoller1.consumerAssignment() == expectedAssignment,
      s"Timed out while awaiting expected assignment change to $expectedAssignment.")

    // Since the consumer1 already completed the rebalance,
    // the `onPartitionsAssigned` rebalance listener will be invoked to set the generationId and memberId
    var stableGeneration = -1
    var stableMemberId1 = ""
    if (!lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
      fail(s"Time out while awaiting for lock.")
    }
    try {
      stableGeneration = generationId1
      stableMemberId1 = memberId1
    } finally {
      lock.unlock()
    }

    val consumerPoller2 = subscribeConsumerAndStartPolling(consumer2, List(topic))
    TestUtils.waitUntilTrue(() => consumerPoller1.consumerAssignment().size == 1,
      s"Timed out while awaiting expected assignment size change to 1.")
    TestUtils.waitUntilTrue(() => consumerPoller2.consumerAssignment().size == 1,
      s"Timed out while awaiting expected assignment size change to 1.")

    if (!lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
      fail(s"Time out while awaiting for lock.")
    }
    try {
      if (assignmentStrategy.equals(classOf[CooperativeStickyAssignor].getName)) {
        // cooperative rebalance should rebalance twice before finally stable
        assertEquals(stableGeneration + 2, generationId1)
      } else {
        // eager rebalance should rebalance once before finally stable
        assertEquals(stableGeneration + 1, generationId1)
      }
      assertEquals(stableMemberId1, memberId1)
    } finally {
      lock.unlock()
    }

    consumerPoller1.shutdown()
    consumerPoller2.shutdown()
  }

}

object PlaintextConsumerAssignorsTest {
  def getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly: Stream[Arguments] =
    BaseConsumerTest.getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly()
}
