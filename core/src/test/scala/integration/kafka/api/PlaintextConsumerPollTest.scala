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

import kafka.utils.TestInfoUtils
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{MetricName, TopicPartition}
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.time.Duration
import java.util
import java.util.stream.Stream
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Integration tests for the consumer that covers the poll logic
 */
@Timeout(600)
class PlaintextConsumerPollTest extends AbstractConsumerTest {

  // Deprecated poll(timeout) not supported for consumer group protocol
  @deprecated("poll(Duration) is the replacement", since = "2.0")
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly"))
  def testDeprecatedPollBlocksForAssignment(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()
    consumer.subscribe(Set(topic).asJava)
    consumer.poll(0)
    assertEquals(Set(tp, tp2), consumer.assignment().asScala)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testMaxPollRecords(quorum: String, groupProtocol: String): Unit = {
    val maxPollRecords = 2
    val numRecords = 10000

    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer, numRecords = numRecords, startingOffset = 0, maxPollRecords = maxPollRecords,
      startingTimestamp = startingTimestamp)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testMaxPollIntervalMs(quorum: String, groupProtocol: String): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 500.toString)
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 2000.toString)

    val consumer = createConsumer()

    val listener = new TestConsumerReassignmentListener()
    consumer.subscribe(List(topic).asJava, listener)

    // rebalance to get the initial assignment
    awaitRebalance(consumer, listener)
    assertEquals(1, listener.callsToAssigned)
    assertEquals(0, listener.callsToRevoked)

    // after we extend longer than max.poll a rebalance should be triggered
    // NOTE we need to have a relatively much larger value than max.poll to let heartbeat expired for sure
    Thread.sleep(3000)

    awaitRebalance(consumer, listener)
    assertEquals(2, listener.callsToAssigned)
    assertEquals(1, listener.callsToRevoked)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testMaxPollIntervalMsDelayInRevocation(quorum: String, groupProtocol: String): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 500.toString)
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false.toString)

    val consumer = createConsumer()
    var commitCompleted = false
    var committedPosition: Long = -1

    val listener = new TestConsumerReassignmentListener {
      override def onPartitionsLost(partitions: util.Collection[TopicPartition]): Unit = {}

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
        if (!partitions.isEmpty && partitions.contains(tp)) {
          // on the second rebalance (after we have joined the group initially), sleep longer
          // than session timeout and then try a commit. We should still be in the group,
          // so the commit should succeed
          Utils.sleep(1500)
          committedPosition = consumer.position(tp)
          consumer.commitSync(Map(tp -> new OffsetAndMetadata(committedPosition)).asJava)
          commitCompleted = true
        }
        super.onPartitionsRevoked(partitions)
      }
    }

    consumer.subscribe(List(topic).asJava, listener)

    // rebalance to get the initial assignment
    awaitRebalance(consumer, listener)

    // force a rebalance to trigger an invocation of the revocation callback while in the group
    consumer.subscribe(List("otherTopic").asJava, listener)
    awaitRebalance(consumer, listener)

    assertEquals(0, committedPosition)
    assertTrue(commitCompleted)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testMaxPollIntervalMsDelayInAssignment(quorum: String, groupProtocol: String): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 500.toString)
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false.toString)

    val consumer = createConsumer()
    val listener = new TestConsumerReassignmentListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        // sleep longer than the session timeout, we should still be in the group after invocation
        Utils.sleep(1500)
        super.onPartitionsAssigned(partitions)
      }
    }
    consumer.subscribe(List(topic).asJava, listener)

    // rebalance to get the initial assignment
    awaitRebalance(consumer, listener)

    // We should still be in the group after this invocation
    ensureNoRebalance(consumer, listener)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testMaxPollIntervalMsShorterThanPollTimeout(quorum: String, groupProtocol: String): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000.toString)
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 500.toString)

    val consumer = createConsumer()
    val listener = new TestConsumerReassignmentListener
    consumer.subscribe(List(topic).asJava, listener)

    // rebalance to get the initial assignment
    awaitRebalance(consumer, listener)

    val callsToAssignedAfterFirstRebalance = listener.callsToAssigned

    consumer.poll(Duration.ofMillis(2000))

    // If the poll poll above times out, it would trigger a rebalance.
    // Leave some time for the rebalance to happen and check for the rebalance event.
    consumer.poll(Duration.ofMillis(500))
    consumer.poll(Duration.ofMillis(500))

    assertEquals(callsToAssignedAfterFirstRebalance, listener.callsToAssigned)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPerPartitionLeadWithMaxPollRecords(quorum: String, groupProtocol: String): Unit = {
    val numMessages = 1000
    val maxPollRecords = 10
    val producer = createProducer()
    sendRecords(producer, numMessages, tp)

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLeadWithMaxPollRecords")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLeadWithMaxPollRecords")
    consumerConfig.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    awaitNonEmptyRecords(consumer, tp)

    val tags = new util.HashMap[String, String]()
    tags.put("client-id", "testPerPartitionLeadWithMaxPollRecords")
    tags.put("topic", tp.topic())
    tags.put("partition", String.valueOf(tp.partition()))
    val lead = consumer.metrics.get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags))
    assertEquals(maxPollRecords, lead.metricValue().asInstanceOf[Double], s"The lead should be $maxPollRecords")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPerPartitionLagWithMaxPollRecords(quorum: String, groupProtocol: String): Unit = {
    val numMessages = 1000
    val maxPollRecords = 10
    val producer = createProducer()
    sendRecords(producer, numMessages, tp)

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLagWithMaxPollRecords")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLagWithMaxPollRecords")
    consumerConfig.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    val records = awaitNonEmptyRecords(consumer, tp)

    val tags = new util.HashMap[String, String]()
    tags.put("client-id", "testPerPartitionLagWithMaxPollRecords")
    tags.put("topic", tp.topic())
    tags.put("partition", String.valueOf(tp.partition()))
    val lag = consumer.metrics.get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags))

    assertEquals(numMessages - records.count, lag.metricValue.asInstanceOf[Double], epsilon, s"The lag should be ${numMessages - records.count}")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testMultiConsumerSessionTimeoutOnStopPolling(quorum: String, groupProtocol: String): Unit = {
    runMultiConsumerSessionTimeoutTest(false)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testMultiConsumerSessionTimeoutOnClose(quorum: String, groupProtocol: String): Unit = {
    runMultiConsumerSessionTimeoutTest(true)
  }

  def runMultiConsumerSessionTimeoutTest(closeConsumer: Boolean): Unit = {
    // use consumers defined in this class plus one additional consumer
    // Use topic defined in this class + one additional topic
    val producer = createProducer()
    sendRecords(producer, numRecords = 100, tp)
    sendRecords(producer, numRecords = 100, tp2)
    val topic1 = "topic1"
    val subscriptions = Set(tp, tp2) ++ createTopicAndSendRecords(producer, topic1, 6, 100)

    // first subscribe consumers that are defined in this class
    val consumerPollers = mutable.Buffer[ConsumerAssignmentPoller]()
    consumerPollers += subscribeConsumerAndStartPolling(createConsumer(), List(topic, topic1))
    consumerPollers += subscribeConsumerAndStartPolling(createConsumer(), List(topic, topic1))

    // create one more consumer and add it to the group; we will timeout this consumer
    val timeoutConsumer = createConsumer()
    val timeoutPoller = subscribeConsumerAndStartPolling(timeoutConsumer, List(topic, topic1))
    consumerPollers += timeoutPoller

    // validate the initial assignment
    validateGroupAssignment(consumerPollers, subscriptions)

    // stop polling and close one of the consumers, should trigger partition re-assignment among alive consumers
    timeoutPoller.shutdown()
    consumerPollers -= timeoutPoller
    if (closeConsumer)
      timeoutConsumer.close()

    validateGroupAssignment(consumerPollers, subscriptions,
      Some(s"Did not get valid assignment for partitions ${subscriptions.asJava} after one consumer left"), 3 * groupMaxSessionTimeoutMs)

    // done with pollers and consumers
    for (poller <- consumerPollers)
      poller.shutdown()
  }
}

object PlaintextConsumerPollTest {
  def getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly: Stream[Arguments] =
    BaseConsumerTest.getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly()

  def getTestQuorumAndGroupProtocolParametersAll: Stream[Arguments] =
    BaseConsumerTest.getTestQuorumAndGroupProtocolParametersAll()
}
