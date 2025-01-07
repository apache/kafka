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
package integration.kafka.api

import kafka.api.AbstractConsumerTest
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRebalanceListener}
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.util
import java.util.Arrays.asList
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Integration tests for the consumer that cover interaction with the consumer from within callbacks
 * and listeners.
 */
class PlaintextConsumerCallbackTest extends AbstractConsumerTest {

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumerRebalanceListenerAssignOnPartitionsAssigned(quorum: String, groupProtocol: String): Unit = {
    val tp = new TopicPartition(topic, 0)
    triggerOnPartitionsAssigned { (consumer, _) =>
      val e: Exception = assertThrows(classOf[IllegalStateException], () => consumer.assign(Collections.singletonList(tp)))
      assertEquals(e.getMessage, "Subscription to topics, partitions and pattern are mutually exclusive")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumerRebalanceListenerAssignmentOnPartitionsAssigned(quorum: String, groupProtocol: String): Unit = {
    val tp = new TopicPartition(topic, 0)
    triggerOnPartitionsAssigned { (consumer, _) =>
      assertTrue(consumer.assignment().contains(tp));
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumerRebalanceListenerBeginningOffsetsOnPartitionsAssigned(quorum: String, groupProtocol: String): Unit = {
    val tp = new TopicPartition(topic, 0)
    triggerOnPartitionsAssigned { (consumer, _) =>
      val map = consumer.beginningOffsets(Collections.singletonList(tp))
      assertTrue(map.containsKey(tp))
      assertEquals(0, map.get(tp))
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumerRebalanceListenerAssignOnPartitionsRevoked(quorum: String, groupProtocol: String): Unit = {
    val tp = new TopicPartition(topic, 0)
    triggerOnPartitionsRevoked { (consumer, _) =>
      val e: Exception = assertThrows(classOf[IllegalStateException], () => consumer.assign(Collections.singletonList(tp)))
      assertEquals(e.getMessage, "Subscription to topics, partitions and pattern are mutually exclusive")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumerRebalanceListenerAssignmentOnPartitionsRevoked(quorum: String, groupProtocol: String): Unit = {
    val tp = new TopicPartition(topic, 0)
    triggerOnPartitionsRevoked { (consumer, _) =>
      assertTrue(consumer.assignment().contains(tp))
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumerRebalanceListenerBeginningOffsetsOnPartitionsRevoked(quorum: String, groupProtocol: String): Unit = {
    val tp = new TopicPartition(topic, 0)
    triggerOnPartitionsRevoked { (consumer, _) =>
      val map = consumer.beginningOffsets(Collections.singletonList(tp))
      assertTrue(map.containsKey(tp))
      assertEquals(0, map.get(tp))
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testGetPositionOfNewlyAssignedPartitionOnPartitionsAssignedCallback(quorum: String, groupProtocol: String): Unit = {
    val tp = new TopicPartition(topic, 0)
    triggerOnPartitionsAssigned { (consumer, _) => assertDoesNotThrow(() => consumer.position(tp)) }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testSeekPositionAndPauseNewlyAssignedPartitionOnPartitionsAssignedCallback(quorum: String,
                                                                                 groupProtocol: String): Unit = {
    val consumer = createConsumer()
    val startingOffset = 100L
    val totalRecords = 120L

    val producer = createProducer()
    val startingTimestamp = 0
    sendRecords(producer, totalRecords.toInt, tp, startingTimestamp)

    triggerOnPartitionsAssigned(consumer, { (consumer, _) =>
      consumer.seek(tp, startingOffset)
      consumer.pause(asList(tp))
    })

    assertTrue(consumer.paused().contains(tp))
    consumer.resume(asList(tp))
    consumeAndVerifyRecords(consumer, numRecords = (totalRecords - startingOffset).toInt,
      startingOffset = startingOffset.toInt, startingKeyAndValueIndex = startingOffset.toInt,
      startingTimestamp = startingOffset)
  }

  private def triggerOnPartitionsAssigned(execute: (Consumer[Array[Byte], Array[Byte]], util.Collection[TopicPartition]) => Unit): Unit = {
    val consumer = createConsumer()
    triggerOnPartitionsAssigned(consumer, execute)
  }
  private def triggerOnPartitionsAssigned(consumer: Consumer[Array[Byte], Array[Byte]], execute: (Consumer[Array[Byte], Array[Byte]], util.Collection[TopicPartition]) => Unit): Unit = {
    val partitionsAssigned = new AtomicBoolean(false)
    consumer.subscribe(asList(topic), new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        execute(consumer, partitions)
        partitionsAssigned.set(true)
      }

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
        // noop
      }
    })
    TestUtils.pollUntilTrue(consumer, () => partitionsAssigned.get(), "Timed out before expected rebalance completed")
  }

  private def triggerOnPartitionsRevoked(execute: (Consumer[Array[Byte], Array[Byte]], util.Collection[TopicPartition]) => Unit): Unit = {
    val consumer = createConsumer()
    val partitionsAssigned = new AtomicBoolean(false)
    val partitionsRevoked = new AtomicBoolean(false)
    consumer.subscribe(asList(topic), new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        partitionsAssigned.set(true)
      }

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
        execute(consumer, partitions)
        partitionsRevoked.set(true)
      }
    })
    TestUtils.pollUntilTrue(consumer, () => partitionsAssigned.get(), "Timed out before expected rebalance completed")
    consumer.close()
    assertTrue(partitionsRevoked.get())
  }
}
