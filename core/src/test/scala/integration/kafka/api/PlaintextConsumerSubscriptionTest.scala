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
import org.apache.kafka.common.errors.{InvalidRegularExpression, InvalidTopicException}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.time.Duration
import java.util.regex.Pattern
import scala.jdk.CollectionConverters._

/**
 * Integration tests for the consumer that covers the subscribe and unsubscribe logic.
 */
@Timeout(600)
class PlaintextConsumerSubscriptionTest extends AbstractConsumerTest {

  /**
   * Verifies that pattern subscription performs as expected.
   * The pattern matches the topics 'topic' and 'tblablac', but not 'tblablak' or 'tblab1'.
   * It is expected that the consumer is subscribed to all partitions of 'topic' and
   * 'tblablac' after the subscription when metadata is refreshed.
   * When a new topic 'tsomec' is added afterwards, it is expected that upon the next
   * metadata refresh the consumer becomes subscribed to this new topic and all partitions
   * of that topic are assigned to it.
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPatternSubscription(quorum: String, groupProtocol: String): Unit = {
    val numRecords = 10000
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)

    val topic1 = "tblablac" // matches subscribed pattern
    createTopic(topic1, 2, brokerCount)
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic1, 0))
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic1, 1))

    val topic2 = "tblablak" // does not match subscribed pattern
    createTopic(topic2, 2, brokerCount)
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic2, 0))
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic2, 1))

    val topic3 = "tblab1" // does not match subscribed pattern
    createTopic(topic3, 2, brokerCount)
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic3, 0))
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic3, 1))

    val consumer = createConsumer()
    assertEquals(0, consumer.assignment().size)

    val pattern = Pattern.compile("t.*c")
    consumer.subscribe(pattern, new TestConsumerReassignmentListener)

    var assignment = Set(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1),
      new TopicPartition(topic1, 0),
      new TopicPartition(topic1, 1))
    awaitAssignment(consumer, assignment)

    val topic4 = "tsomec" // matches subscribed pattern
    createTopic(topic4, 2, brokerCount)
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic4, 0))
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic4, 1))

    assignment ++= Set(
      new TopicPartition(topic4, 0),
      new TopicPartition(topic4, 1))
    awaitAssignment(consumer, assignment)

    consumer.unsubscribe()
    assertEquals(0, consumer.assignment().size)
  }

  /**
   * Verifies that a second call to pattern subscription succeeds and performs as expected.
   * The initial subscription is to a pattern that matches two topics 'topic' and 'foo'.
   * The second subscription is to a pattern that matches 'foo' and a new topic 'bar'.
   * It is expected that the consumer is subscribed to all partitions of 'topic' and 'foo' after
   * the first subscription, and to all partitions of 'foo' and 'bar' after the second.
   * The metadata refresh interval is intentionally increased to a large enough value to guarantee
   * that it is the subscription call that triggers a metadata refresh, and not the timeout.
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testSubsequentPatternSubscription(quorum: String, groupProtocol: String): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "30000")
    val consumer = createConsumer()

    val numRecords = 10000
    val producer = createProducer()
    sendRecords(producer, numRecords = numRecords, tp)

    // the first topic ('topic')  matches first subscription pattern only

    val fooTopic = "foo" // matches both subscription patterns
    createTopic(fooTopic, 1, brokerCount)
    sendRecords(producer, numRecords = 1000, new TopicPartition(fooTopic, 0))

    assertEquals(0, consumer.assignment().size)

    val pattern1 = Pattern.compile(".*o.*") // only 'topic' and 'foo' match this
    consumer.subscribe(pattern1, new TestConsumerReassignmentListener)

    var assignment = Set(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1),
      new TopicPartition(fooTopic, 0))
    awaitAssignment(consumer, assignment)

    val barTopic = "bar" // matches the next subscription pattern
    createTopic(barTopic, 1, brokerCount)
    sendRecords(producer, numRecords = 1000, new TopicPartition(barTopic, 0))

    val pattern2 = Pattern.compile("...") // only 'foo' and 'bar' match this
    consumer.subscribe(pattern2, new TestConsumerReassignmentListener)
    assignment --= Set(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1))
    assignment ++= Set(
      new TopicPartition(barTopic, 0))
    awaitAssignment(consumer, assignment)

    consumer.unsubscribe()
    assertEquals(0, consumer.assignment().size)
  }

  /**
   * Verifies that pattern unsubscription performs as expected.
   * The pattern matches the topics 'topic' and 'tblablac'.
   * It is expected that the consumer is subscribed to all partitions of 'topic' and
   * 'tblablac' after the subscription when metadata is refreshed.
   * When consumer unsubscribes from all its subscriptions, it is expected that its
   * assignments are cleared right away.
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPatternUnsubscription(quorum: String, groupProtocol: String): Unit = {
    val numRecords = 10000
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)

    val topic1 = "tblablac" // matches the subscription pattern
    createTopic(topic1, 2, brokerCount)
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic1, 0))
    sendRecords(producer, numRecords = 1000, new TopicPartition(topic1, 1))

    val consumer = createConsumer()
    assertEquals(0, consumer.assignment().size)

    consumer.subscribe(Pattern.compile("t.*c"), new TestConsumerReassignmentListener)
    val assignment = Set(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1),
      new TopicPartition(topic1, 0),
      new TopicPartition(topic1, 1))
    awaitAssignment(consumer, assignment)

    consumer.unsubscribe()
    assertEquals(0, consumer.assignment().size)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersConsumerGroupProtocolOnly"))
  def testRe2JPatternSubscription(quorum: String, groupProtocol: String): Unit = {
    val topic1 = "tblablac" // matches subscribed pattern
    createTopic(topic1, 2, brokerCount)

    val topic2 = "tblablak" // does not match subscribed pattern
    createTopic(topic2, 2, brokerCount)

    val topic3 = "tblab1" // does not match subscribed pattern
    createTopic(topic3, 2, brokerCount)

    val consumer = createConsumer()
    assertEquals(0, consumer.assignment().size)

    val pattern = new SubscriptionPattern("t.*c")
    consumer.subscribe(pattern)

    val assignment = Set(
      new TopicPartition(topic, 0),
      new TopicPartition(topic, 1),
      new TopicPartition(topic1, 0),
      new TopicPartition(topic1, 1))
    awaitAssignment(consumer, assignment)
    consumer.unsubscribe()
    assertEquals(0, consumer.assignment().size)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersConsumerGroupProtocolOnly"))
  def testRe2JPatternSubscriptionInvalidRegex(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()
    assertEquals(0, consumer.assignment().size)

    val pattern = new SubscriptionPattern("(t.*c")
    consumer.subscribe(pattern)

    TestUtils.tryUntilNoAssertionError() {
      assertThrows(classOf[InvalidRegularExpression], () => consumer.poll(Duration.ZERO))
    }
    consumer.unsubscribe()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testExpandingTopicSubscriptions(quorum: String, groupProtocol: String): Unit = {
    val otherTopic = "other"
    val initialAssignment = Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1))
    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)
    awaitAssignment(consumer, initialAssignment)

    createTopic(otherTopic, 2, brokerCount)
    val expandedAssignment = initialAssignment ++ Set(new TopicPartition(otherTopic, 0), new TopicPartition(otherTopic, 1))
    consumer.subscribe(List(topic, otherTopic).asJava)
    awaitAssignment(consumer, expandedAssignment)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testShrinkingTopicSubscriptions(quorum: String, groupProtocol: String): Unit = {
    val otherTopic = "other"
    createTopic(otherTopic, 2, brokerCount)
    val initialAssignment = Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(otherTopic, 0), new TopicPartition(otherTopic, 1))
    val consumer = createConsumer()
    consumer.subscribe(List(topic, otherTopic).asJava)
    awaitAssignment(consumer, initialAssignment)

    val shrunkenAssignment = Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1))
    consumer.subscribe(List(topic).asJava)
    awaitAssignment(consumer, shrunkenAssignment)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testUnsubscribeTopic(quorum: String, groupProtocol: String): Unit = {
    if (groupProtocol.equals(GroupProtocol.CLASSIC.name)) {
      this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "100") // timeout quickly to avoid slow test
      this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "30")
    }
    val consumer = createConsumer()

    val listener = new TestConsumerReassignmentListener()
    consumer.subscribe(List(topic).asJava, listener)

    // the initial subscription should cause a callback execution
    awaitRebalance(consumer, listener)

    consumer.subscribe(List[String]().asJava)
    assertEquals(0, consumer.assignment.size())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testSubscribeInvalidTopicCanUnsubscribe(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()

    setupSubscribeInvalidTopic(consumer)
    if(groupProtocol == "consumer") {
      // Must ensure memberId is not empty before sending leave group heartbeat. This is a temporary solution before KIP-1082.
      TestUtils.waitUntilTrue(() => consumer.groupMetadata().memberId().nonEmpty,
        waitTimeMs = 30000, msg = "Timeout waiting for first consumer group heartbeat response")
    }
    assertDoesNotThrow(new Executable {
      override def execute(): Unit = consumer.unsubscribe()
    })
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testSubscribeInvalidTopicCanClose(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()

    setupSubscribeInvalidTopic(consumer)
    assertDoesNotThrow(new Executable {
      override def execute(): Unit = consumer.close()
    })
  }

  def setupSubscribeInvalidTopic(consumer: Consumer[Array[Byte], Array[Byte]]): Unit = {
    // Invalid topic name due to space
    val invalidTopicName = "topic abc"
    consumer.subscribe(List(invalidTopicName).asJava)

    var exception : InvalidTopicException = null
    TestUtils.waitUntilTrue(() => {
      try consumer.poll(Duration.ofMillis(500)) catch {
        case e : InvalidTopicException => exception = e
        case e : Throwable => fail(s"An InvalidTopicException should be thrown. But ${e.getClass} is thrown")
      }
      exception != null
    }, waitTimeMs = 5000, msg = "An InvalidTopicException should be thrown.")

    assertEquals(s"Invalid topics: [${invalidTopicName}]", exception.getMessage)
  }
}
