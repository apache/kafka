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
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

/**
 * Integration tests for the consumer that covers logic related to manual assignment.
 */
@Timeout(600)
class PlaintextConsumerAssignTest extends AbstractConsumerTest {

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testAssignAndCommitAsyncNotCommitted(groupProtocol: String): Unit = {
    val props = new Properties()
    val consumer = createConsumer(configOverrides = props)
    val producer = createProducer()
    val numRecords = 10000
    val startingTimestamp = System.currentTimeMillis()
    val cb = new CountConsumerCommitCallback
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)
    consumer.assign(java.util.List.of(tp))
    consumer.commitAsync(cb)
    TestUtils.pollUntilTrue(consumer, () => cb.successCount >= 1 || cb.lastError.isDefined,
      "Failed to observe commit callback before timeout", waitTimeMs = 10000)
    val committedOffset = consumer.committed(java.util.Set.of(tp))
    assertNotNull(committedOffset)
    // No valid fetch position due to the absence of consumer.poll; and therefore no offset was committed to
    // tp. The committed offset should be null. This is intentional.
    assertNull(committedOffset.get(tp))
    assertTrue(consumer.assignment.contains(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testAssignAndCommitSyncNotCommitted(groupProtocol: String): Unit = {
    val props = new Properties()
    val consumer = createConsumer(configOverrides = props)
    val producer = createProducer()
    val numRecords = 10000
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)
    consumer.assign(java.util.List.of(tp))
    consumer.commitSync()
    val committedOffset = consumer.committed(java.util.Set.of(tp))
    assertNotNull(committedOffset)
    // No valid fetch position due to the absence of consumer.poll; and therefore no offset was committed to
    // tp. The committed offset should be null. This is intentional.
    assertNull(committedOffset.get(tp))
    assertTrue(consumer.assignment.contains(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testAssignAndCommitSyncAllConsumed(groupProtocol: String): Unit = {
    val numRecords = 10000

    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val props = new Properties()
    val consumer = createConsumer(configOverrides = props)
    consumer.assign(java.util.List.of(tp))
    consumer.seek(tp, 0)
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 0, startingTimestamp = startingTimestamp)

    consumer.commitSync()
    val committedOffset = consumer.committed(java.util.Set.of(tp))
    assertNotNull(committedOffset)
    assertNotNull(committedOffset.get(tp))
    assertEquals(numRecords, committedOffset.get(tp).offset())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testAssignAndConsume(groupProtocol: String): Unit = {
    val numRecords = 10

    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val props = new Properties()
    val consumer = createConsumer(configOverrides = props,
      configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))
    consumer.assign(java.util.List.of(tp))
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 0, startingTimestamp = startingTimestamp)

    assertEquals(numRecords, consumer.position(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testAssignAndConsumeSkippingPosition(groupProtocol: String): Unit = {
    val numRecords = 10

    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val props = new Properties()
    val consumer = createConsumer(configOverrides = props,
      configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))
    consumer.assign(java.util.List.of(tp))
    val offset = 1
    consumer.seek(tp, offset)
    consumeAndVerifyRecords(consumer = consumer, numRecords - offset, startingOffset = offset,
      startingKeyAndValueIndex = offset, startingTimestamp = startingTimestamp + offset)

    assertEquals(numRecords, consumer.position(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testAssignAndFetchCommittedOffsets(groupProtocol: String): Unit = {
    val numRecords = 100
    val startingTimestamp = System.currentTimeMillis()
    val producer = createProducer()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)
    val props = new Properties()
    val consumer = createConsumer(configOverrides = props)
    consumer.assign(java.util.List.of(tp))
    // First consumer consumes and commits offsets
    consumer.seek(tp, 0)
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 0,
      startingTimestamp = startingTimestamp)
    consumer.commitSync()
    assertEquals(numRecords, consumer.committed(java.util.Set.of(tp)).get(tp).offset)
    // We should see the committed offsets from another consumer
    val anotherConsumer = createConsumer(configOverrides = props)
    anotherConsumer.assign(java.util.List.of(tp))
    assertEquals(numRecords, anotherConsumer.committed(java.util.Set.of(tp)).get(tp).offset)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testAssignAndConsumeFromCommittedOffsets(groupProtocol: String): Unit = {
    val producer = createProducer()
    val numRecords = 100
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = numRecords, tp, startingTimestamp = startingTimestamp)

    // Commit offset with first consumer
    val props = new Properties()
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group1")
    val consumer = createConsumer(configOverrides = props)
    consumer.assign(java.util.List.of(tp))
    val offset = 10
    consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(offset)))
    assertEquals(offset, consumer.committed(java.util.Set.of(tp)).get(tp).offset)
    consumer.close()

    // Consume from committed offsets with another consumer in same group
    val anotherConsumer = createConsumer(configOverrides = props)
    assertEquals(offset, anotherConsumer.committed(java.util.Set.of(tp)).get(tp).offset)
    anotherConsumer.assign(java.util.List.of(tp))
    consumeAndVerifyRecords(consumer = anotherConsumer, numRecords - offset,
      startingOffset = offset, startingKeyAndValueIndex = offset,
      startingTimestamp = startingTimestamp + offset)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testAssignAndRetrievingCommittedOffsetsMultipleTimes(groupProtocol: String): Unit = {
    val numRecords = 100
    val startingTimestamp = System.currentTimeMillis()
    val producer = createProducer()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val props = new Properties()
    val consumer = createConsumer(configOverrides = props)
    consumer.assign(java.util.List.of(tp))

    // Consume and commit offsets
    consumer.seek(tp, 0)
    consumeAndVerifyRecords(consumer = consumer, numRecords, startingOffset = 0,
      startingTimestamp = startingTimestamp)
    consumer.commitSync()

    // Check committed offsets twice with same consumer
    assertEquals(numRecords, consumer.committed(java.util.Set.of(tp)).get(tp).offset)
    assertEquals(numRecords, consumer.committed(java.util.Set.of(tp)).get(tp).offset)
  }

}
