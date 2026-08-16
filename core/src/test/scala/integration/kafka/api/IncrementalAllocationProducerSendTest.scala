/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.api

import java.util.concurrent.ThreadLocalRandom
import java.util.{Properties, List => JList}
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

/**
 * Runs the whole [[BaseProducerSendTest]] suite with the producer configured with the incremental
 * buffer.memory allocation strategy, plus a few scenarios specific to chunked allocation.
 */
class IncrementalAllocationProducerSendTest extends BaseProducerSendTest {

  override protected def producerOverrides: Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG,
      ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_INCREMENTAL)
    props
  }

  // The incremental strategy does not support compression yet; the base test would fail at
  // producer construction. Overriding without the test annotations removes it from this
  // subclass's run. TODO: remove this override when compression support lands.
  override def testSendCompressedMessageWithCreateTime(groupProtocol: String): Unit = {}

  // batch.size=0 is below the internal chunk size, so the incremental strategy falls back to the full
  // allocation path (a batch is smaller than one chunk). Verifies that fallback yields a working producer.
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersConsumerGroupProtocolOnly"))
  def testBatchSizeZero(groupProtocol: String): Unit = {
    sendAndVerify(createProducer(
      lingerMs = Int.MaxValue,
      deliveryTimeoutMs = Int.MaxValue,
      batchSize = 0))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersConsumerGroupProtocolOnly"))
  def testSendLargeRecordsSpanningMultipleChunks(groupProtocol: String): Unit = {
    TestUtils.createTopicWithAdmin(admin, topic, brokers, controllerServers, 1, 2)
    val partition = 0
    val tp = new TopicPartition(topic, partition)
    val valueSize = 600_000 // far larger than one chunk, so each record spans many chunks
    // TODO: also exercise the compressed codecs here once the incremental strategy supports compression.

    val producer = createProducer(batchSize = 1024 * 1024, bufferSize = 8L * 1024 * 1024)
    val value = randomBytes(valueSize)
    val metadata = producer.send(new ProducerRecord(topic, partition, "key".getBytes, value)).get
    assertEquals(valueSize, metadata.serializedValueSize)

    // Verify the exact bytes round-trip.
    val consumer = TestUtils.createConsumer(
      bootstrapServers(listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)),
      groupProtocolFromTestParameters())
    try {
      consumer.assign(JList.of(tp))
      consumer.seekToBeginning(JList.of(tp))
      val consumed = TestUtils.consumeRecords(consumer, 1)
      assertEquals(metadata.offset, consumed.head.offset)
      assertArrayEquals(value, consumed.head.value)
    } finally {
      consumer.close()
    }
  }

  private def randomBytes(size: Int): Array[Byte] = {
    val bytes = new Array[Byte](size)
    ThreadLocalRandom.current().nextBytes(bytes)
    bytes
  }
}
