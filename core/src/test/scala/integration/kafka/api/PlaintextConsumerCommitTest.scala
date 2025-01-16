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
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.test.MockConsumerInterceptor
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.time.Duration
import java.util
import java.util.Optional
import scala.jdk.CollectionConverters._

/**
 * Integration tests for the consumer that covers the logic related to committing offsets.
 */
@Timeout(600)
class PlaintextConsumerCommitTest extends AbstractConsumerTest {

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testAutoCommitOnClose(quorum: String, groupProtocol: String): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    val consumer = createConsumer()

    val numRecords = 10000
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)

    consumer.subscribe(List(topic).asJava)
    awaitAssignment(consumer, Set(tp, tp2))

    // should auto-commit sought positions before closing
    consumer.seek(tp, 300)
    consumer.seek(tp2, 500)
    consumer.close()

    // now we should see the committed positions from another consumer
    val anotherConsumer = createConsumer()
    assertEquals(300, anotherConsumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(500, anotherConsumer.committed(Set(tp2).asJava).get(tp2).offset)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testAutoCommitOnCloseAfterWakeup(quorum: String, groupProtocol: String): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    val consumer = createConsumer()

    val numRecords = 10000
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)

    consumer.subscribe(List(topic).asJava)
    awaitAssignment(consumer, Set(tp, tp2))

    // should auto-commit sought positions before closing
    consumer.seek(tp, 300)
    consumer.seek(tp2, 500)

    // wakeup the consumer before closing to simulate trying to break a poll
    // loop from another thread
    consumer.wakeup()
    consumer.close()

    // now we should see the committed positions from another consumer
    val anotherConsumer = createConsumer()
    assertEquals(300, anotherConsumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(500, anotherConsumer.committed(Set(tp2).asJava).get(tp2).offset)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testCommitMetadata(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)

    // sync commit
    val syncMetadata = new OffsetAndMetadata(5, Optional.of(15), "foo")
    consumer.commitSync(Map((tp, syncMetadata)).asJava)
    assertEquals(syncMetadata, consumer.committed(Set(tp).asJava).get(tp))

    // async commit
    val asyncMetadata = new OffsetAndMetadata(10, "bar")
    sendAndAwaitAsyncCommit(consumer, Some(Map(tp -> asyncMetadata)))
    assertEquals(asyncMetadata, consumer.committed(Set(tp).asJava).get(tp))

    // handle null metadata
    val nullMetadata = new OffsetAndMetadata(5, null)
    consumer.commitSync(Map(tp -> nullMetadata).asJava)
    assertEquals(nullMetadata, consumer.committed(Set(tp).asJava).get(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testAsyncCommit(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)

    val callback = new CountConsumerCommitCallback
    val count = 5

    for (i <- 1 to count)
      consumer.commitAsync(Map(tp -> new OffsetAndMetadata(i)).asJava, callback)

    TestUtils.pollUntilTrue(consumer, () => callback.successCount >= count || callback.lastError.isDefined,
      "Failed to observe commit callback before timeout", waitTimeMs = 10000)

    assertEquals(None, callback.lastError)
    assertEquals(count, callback.successCount)
    assertEquals(new OffsetAndMetadata(count), consumer.committed(Set(tp).asJava).get(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testAutoCommitIntercept(quorum: String, groupProtocol: String): Unit = {
    val topic2 = "topic2"
    createTopic(topic2, 2, brokerCount)

    // produce records
    val numRecords = 100
    val testProducer = createProducer(keySerializer = new StringSerializer, valueSerializer = new StringSerializer)
    (0 until numRecords).map { i =>
      testProducer.send(new ProducerRecord(tp.topic(), tp.partition(), s"key $i", s"value $i"))
    }.foreach(_.get)

    // create consumer with interceptor
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    this.consumerConfig.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockConsumerInterceptor")
    val testConsumer = createConsumer(keyDeserializer = new StringDeserializer, valueDeserializer = new StringDeserializer)
    val rebalanceListener = new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        // keep partitions paused in this test so that we can verify the commits based on specific seeks
        testConsumer.pause(partitions)
      }

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {}
    }
    changeConsumerSubscriptionAndValidateAssignment(testConsumer, List(topic), Set(tp, tp2), rebalanceListener)
    testConsumer.seek(tp, 10)
    testConsumer.seek(tp2, 20)

    // change subscription to trigger rebalance
    val commitCountBeforeRebalance = MockConsumerInterceptor.ON_COMMIT_COUNT.intValue()
    changeConsumerSubscriptionAndValidateAssignment(testConsumer,
      List(topic, topic2),
      Set(tp, tp2, new TopicPartition(topic2, 0), new TopicPartition(topic2, 1)),
      rebalanceListener)

    // after rebalancing, we should have reset to the committed positions
    assertEquals(10, testConsumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(20, testConsumer.committed(Set(tp2).asJava).get(tp2).offset)

    // In both CLASSIC and CONSUMER protocols, interceptors are executed in poll and close.
    // However, in the CONSUMER protocol, the assignment may be changed outside of a poll, so
    // we need to poll once to ensure the interceptor is called.
    if (groupProtocol.toUpperCase == GroupProtocol.CONSUMER.name) {
      testConsumer.poll(Duration.ZERO)
    }

    assertTrue(MockConsumerInterceptor.ON_COMMIT_COUNT.intValue() > commitCountBeforeRebalance)

    // verify commits are intercepted on close
    val commitCountBeforeClose = MockConsumerInterceptor.ON_COMMIT_COUNT.intValue()
    testConsumer.close()
    assertTrue(MockConsumerInterceptor.ON_COMMIT_COUNT.intValue() > commitCountBeforeClose)
    testProducer.close()

    // cleanup
    MockConsumerInterceptor.resetCounters()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testCommitSpecifiedOffsets(quorum: String, groupProtocol: String): Unit = {
    val producer = createProducer()
    sendRecords(producer, numRecords = 5, tp)
    sendRecords(producer, numRecords = 7, tp2)

    val consumer = createConsumer()
    consumer.assign(List(tp, tp2).asJava)

    val pos1 = consumer.position(tp)
    val pos2 = consumer.position(tp2)
    consumer.commitSync(Map[TopicPartition, OffsetAndMetadata]((tp, new OffsetAndMetadata(3L))).asJava)
    assertEquals(3, consumer.committed(Set(tp).asJava).get(tp).offset)
    assertNull(consumer.committed(Set(tp2).asJava).get(tp2))

    // Positions should not change
    assertEquals(pos1, consumer.position(tp))
    assertEquals(pos2, consumer.position(tp2))
    consumer.commitSync(Map[TopicPartition, OffsetAndMetadata]((tp2, new OffsetAndMetadata(5L))).asJava)
    assertEquals(3, consumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(5, consumer.committed(Set(tp2).asJava).get(tp2).offset)

    // Using async should pick up the committed changes after commit completes
    sendAndAwaitAsyncCommit(consumer, Some(Map(tp2 -> new OffsetAndMetadata(7L))))
    assertEquals(7, consumer.committed(Set(tp2).asJava).get(tp2).offset)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testAutoCommitOnRebalance(quorum: String, groupProtocol: String): Unit = {
    val topic2 = "topic2"
    createTopic(topic2, 2, brokerCount)

    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    val consumer = createConsumer()

    val numRecords = 10000
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)

    val rebalanceListener = new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        // keep partitions paused in this test so that we can verify the commits based on specific seeks
        consumer.pause(partitions)
      }

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {}
    }

    consumer.subscribe(List(topic).asJava, rebalanceListener)

    awaitAssignment(consumer, Set(tp, tp2))

    consumer.seek(tp, 300)
    consumer.seek(tp2, 500)

    // change subscription to trigger rebalance
    consumer.subscribe(List(topic, topic2).asJava, rebalanceListener)

    val newAssignment = Set(tp, tp2, new TopicPartition(topic2, 0), new TopicPartition(topic2, 1))
    awaitAssignment(consumer, newAssignment)

    // after rebalancing, we should have reset to the committed positions
    assertEquals(300, consumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(500, consumer.committed(Set(tp2).asJava).get(tp2).offset)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testSubscribeAndCommitSync(quorum: String, groupProtocol: String): Unit = {
    // This test ensure that the member ID is propagated from the group coordinator when the
    // assignment is received into a subsequent offset commit
    val consumer = createConsumer()
    assertEquals(0, consumer.assignment.size)
    consumer.subscribe(List(topic).asJava)
    awaitAssignment(consumer, Set(tp, tp2))

    consumer.seek(tp, 0)

    consumer.commitSync()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPositionAndCommit(quorum: String, groupProtocol: String): Unit = {
    val producer = createProducer()
    var startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = 5, tp, startingTimestamp = startingTimestamp)

    val topicPartition = new TopicPartition(topic, 15)
    val consumer = createConsumer()
    assertNull(consumer.committed(Set(topicPartition).asJava).get(topicPartition))

    // position() on a partition that we aren't subscribed to throws an exception
    assertThrows(classOf[IllegalStateException], () => consumer.position(topicPartition))

    consumer.assign(List(tp).asJava)

    assertEquals(0L, consumer.position(tp), "position() on a partition that we are subscribed to should reset the offset")
    consumer.commitSync()
    assertEquals(0L, consumer.committed(Set(tp).asJava).get(tp).offset)
    consumeAndVerifyRecords(consumer = consumer, numRecords = 5, startingOffset = 0, startingTimestamp = startingTimestamp)
    assertEquals(5L, consumer.position(tp), "After consuming 5 records, position should be 5")
    consumer.commitSync()
    assertEquals(5L, consumer.committed(Set(tp).asJava).get(tp).offset, "Committed offset should be returned")

    startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = 1, tp, startingTimestamp = startingTimestamp)

    // another consumer in the same group should get the same position
    val otherConsumer = createConsumer()
    otherConsumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = otherConsumer, numRecords = 1, startingOffset = 5, startingTimestamp = startingTimestamp)
  }

  // TODO: This only works in the new consumer, but should be fixed for the old consumer as well
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersConsumerGroupProtocolOnly"))
  def testCommitAsyncCompletedBeforeConsumerCloses(quorum: String, groupProtocol: String): Unit = {
    // This is testing the contract that asynchronous offset commit are completed before the consumer
    // is closed, even when no commit sync is performed as part of the close (due to auto-commit
    // disabled, or simply because there are no consumed offsets).
    val producer = createProducer()
    sendRecords(producer, numRecords = 3, tp)
    sendRecords(producer, numRecords = 3, tp2)

    val consumer = createConsumer()
    consumer.assign(List(tp, tp2).asJava)

    // Try without looking up the coordinator first
    val cb = new CountConsumerCommitCallback
    consumer.commitAsync(Map[TopicPartition, OffsetAndMetadata]((tp, new OffsetAndMetadata(1L))).asJava, cb)
    consumer.commitAsync(Map[TopicPartition, OffsetAndMetadata]((tp2, new OffsetAndMetadata(1L))).asJava, cb)
    consumer.close()
    assertEquals(2, cb.successCount)
  }

  // TODO: This only works in the new consumer, but should be fixed for the old consumer as well
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersConsumerGroupProtocolOnly"))
  def testCommitAsyncCompletedBeforeCommitSyncReturns(quorum: String, groupProtocol: String): Unit = {
    // This is testing the contract that asynchronous offset commits sent previously with the
    // `commitAsync` are guaranteed to have their callbacks invoked prior to completion of
    // `commitSync` (given that it does not time out).
    val producer = createProducer()
    sendRecords(producer, numRecords = 3, tp)
    sendRecords(producer, numRecords = 3, tp2)

    val consumer = createConsumer()
    consumer.assign(List(tp, tp2).asJava)

    // Try without looking up the coordinator first
    val cb = new CountConsumerCommitCallback
    consumer.commitAsync(Map[TopicPartition, OffsetAndMetadata]((tp, new OffsetAndMetadata(1L))).asJava, cb)
    consumer.commitSync(Map.empty[TopicPartition, OffsetAndMetadata].asJava)
    assertEquals(1, consumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(1, cb.successCount)

    // Try with coordinator known
    consumer.commitAsync(Map[TopicPartition, OffsetAndMetadata]((tp, new OffsetAndMetadata(2L))).asJava, cb)
    consumer.commitSync(Map[TopicPartition, OffsetAndMetadata]((tp2, new OffsetAndMetadata(2L))).asJava)
    assertEquals(2, consumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(2, consumer.committed(Set(tp2).asJava).get(tp2).offset)
    assertEquals(2, cb.successCount)

    // Try with empty sync commit
    consumer.commitAsync(Map[TopicPartition, OffsetAndMetadata]((tp, new OffsetAndMetadata(3L))).asJava, cb)
    consumer.commitSync(Map.empty[TopicPartition, OffsetAndMetadata].asJava)
    assertEquals(3, consumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(2, consumer.committed(Set(tp2).asJava).get(tp2).offset)
    assertEquals(3, cb.successCount)
  }

  def changeConsumerSubscriptionAndValidateAssignment[K, V](consumer: Consumer[K, V],
                                                            topicsToSubscribe: List[String],
                                                            expectedAssignment: Set[TopicPartition],
                                                            rebalanceListener: ConsumerRebalanceListener): Unit = {
    consumer.subscribe(topicsToSubscribe.asJava, rebalanceListener)
    awaitAssignment(consumer, expectedAssignment)
  }
}
