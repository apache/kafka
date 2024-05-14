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
import java.util.Properties
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import org.apache.kafka.common.PartitionInfo
import java.util.stream.Stream
import scala.jdk.CollectionConverters._
import scala.collection.mutable
import org.junit.jupiter.params.provider.CsvSource

/**
 * Integration tests for the consumer that covers logic related to manual assignment.
 */
@Timeout(600)
class PlaintextConsumerAssignTest extends AbstractConsumerTest {

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testAssignAndCommitAsyncNotCommitted(quorum: String, groupProtocol: String): Unit = {
    val props = new Properties()
    val consumer = createConsumer(configOverrides = props)
    val producer = createProducer()
    val numRecords = 10000
    val startingTimestamp = System.currentTimeMillis()
    val cb = new CountConsumerCommitCallback
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)
    consumer.assign(List(tp).asJava)
    consumer.commitAsync(cb)
    TestUtils.pollUntilTrue(consumer, () => cb.successCount >= 1 || cb.lastError.isDefined,
      "Failed to observe commit callback before timeout", waitTimeMs = 10000)
    val committedOffset = consumer.committed(Set(tp).asJava)
    assertNotNull(committedOffset)
    // No valid fetch position due to the absence of consumer.poll; and therefore no offset was committed to
    // tp. The committed offset should be null. This is intentional.
    assertNull(committedOffset.get(tp))
    assertTrue(consumer.assignment.contains(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testAssignAndCommitSyncNotCommitted(quorum: String, groupProtocol: String): Unit = {
    val props = new Properties()
    val consumer = createConsumer(configOverrides = props)
    val producer = createProducer()
    val numRecords = 10000
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)
    consumer.assign(List(tp).asJava)
    consumer.commitSync()
    val committedOffset = consumer.committed(Set(tp).asJava)
    assertNotNull(committedOffset)
    // No valid fetch position due to the absence of consumer.poll; and therefore no offset was committed to
    // tp. The committed offset should be null. This is intentional.
    assertNull(committedOffset.get(tp))
    assertTrue(consumer.assignment.contains(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testAssignAndCommitSyncAllConsumed(quorum: String, groupProtocol: String): Unit = {
    val numRecords = 10000

    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val props = new Properties()
    val consumer = createConsumer(configOverrides = props)
    consumer.assign(List(tp).asJava)
    consumer.seek(tp, 0)
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 0, startingTimestamp = startingTimestamp)

    consumer.commitSync()
    val committedOffset = consumer.committed(Set(tp).asJava)
    assertNotNull(committedOffset)
    assertNotNull(committedOffset.get(tp))
    assertEquals(numRecords, committedOffset.get(tp).offset())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testAssignAndConsume(quorum: String, groupProtocol: String): Unit = {
    val numRecords = 10

    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val props = new Properties()
    val consumer = createConsumer(configOverrides = props,
      configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))
    consumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 0, startingTimestamp = startingTimestamp)

    assertEquals(numRecords, consumer.position(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testAssignAndConsumeSkippingPosition(quorum: String, groupProtocol: String): Unit = {
    val numRecords = 10

    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val props = new Properties()
    val consumer = createConsumer(configOverrides = props,
      configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))
    consumer.assign(List(tp).asJava)
    val offset = 1
    consumer.seek(tp, offset)
    consumeAndVerifyRecords(consumer = consumer, numRecords - offset, startingOffset = offset,
      startingKeyAndValueIndex = offset, startingTimestamp = startingTimestamp + offset)

    assertEquals(numRecords, consumer.position(tp))
  }

  // partitionsFor not implemented in consumer group protocol and this test requires ZK also
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @CsvSource(Array(
    "zk, classic"
  ))
  def testAssignAndConsumeWithLeaderChangeValidatingPositions(quorum: String, groupProtocol: String): Unit = {
    val numRecords = 10
    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)
    val props = new Properties()
    val consumer = createConsumer(configOverrides = props,
      configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))
    consumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 0, startingTimestamp = startingTimestamp)

    // Force leader epoch change to trigger position validation
    var parts: mutable.Buffer[PartitionInfo] = null
    while (parts == null)
      parts = consumer.partitionsFor(tp.topic()).asScala
    val leader = parts.head.leader().id()
    this.servers(leader).shutdown()
    this.servers(leader).startup()

    // Consume after leader change
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 10,
      startingTimestamp = startingTimestamp)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testAssignAndFetchCommittedOffsets(quorum: String, groupProtocol: String): Unit = {
    val numRecords = 100
    val startingTimestamp = System.currentTimeMillis()
    val producer = createProducer()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)
    val props = new Properties()
    val consumer = createConsumer(configOverrides = props)
    consumer.assign(List(tp).asJava)
    // First consumer consumes and commits offsets
    consumer.seek(tp, 0)
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 0,
      startingTimestamp = startingTimestamp)
    consumer.commitSync()
    assertEquals(numRecords, consumer.committed(Set(tp).asJava).get(tp).offset)
    // We should see the committed offsets from another consumer
    val anotherConsumer = createConsumer(configOverrides = props)
    anotherConsumer.assign(List(tp).asJava)
    assertEquals(numRecords, anotherConsumer.committed(Set(tp).asJava).get(tp).offset)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testAssignAndConsumeFromCommittedOffsets(quorum: String, groupProtocol: String): Unit = {
    val producer = createProducer()
    val numRecords = 100
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = numRecords, tp, startingTimestamp = startingTimestamp)

    // Commit offset with first consumer
    val props = new Properties()
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group1")
    val consumer = createConsumer(configOverrides = props)
    consumer.assign(List(tp).asJava)
    val offset = 10
    consumer.commitSync(Map[TopicPartition, OffsetAndMetadata]((tp, new OffsetAndMetadata(offset)))
      .asJava)
    assertEquals(offset, consumer.committed(Set(tp).asJava).get(tp).offset)
    consumer.close()

    // Consume from committed offsets with another consumer in same group
    val anotherConsumer = createConsumer(configOverrides = props)
    assertEquals(offset, anotherConsumer.committed(Set(tp).asJava).get(tp).offset)
    anotherConsumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = anotherConsumer, numRecords - offset,
      startingOffset = offset, startingKeyAndValueIndex = offset,
      startingTimestamp = startingTimestamp + offset)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testAssignAndRetrievingCommittedOffsetsMultipleTimes(quorum: String, groupProtocol: String): Unit = {
    val numRecords = 100
    val startingTimestamp = System.currentTimeMillis()
    val producer = createProducer()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val props = new Properties()
    val consumer = createConsumer(configOverrides = props)
    consumer.assign(List(tp).asJava)

    // Consume and commit offsets
    consumer.seek(tp, 0)
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 0,
      startingTimestamp = startingTimestamp)
    consumer.commitSync()

    // Check committed offsets twice with same consumer
    assertEquals(numRecords, consumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(numRecords, consumer.committed(Set(tp).asJava).get(tp).offset)
  }

}

object PlaintextConsumerAssignTest {
  def getTestQuorumAndGroupProtocolParametersAll: Stream[Arguments] =
    BaseConsumerTest.getTestQuorumAndGroupProtocolParametersAll()
}
