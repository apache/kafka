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
package kafka.server

import kafka.api.KAFKA_0_11_0_IV2
import kafka.log.LogConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Properties
import scala.annotation.nowarn
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class FetchWithLegacyMessageFormatTest extends BaseFetchRequestTest {

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.FetchMaxBytes, Int.MaxValue.toString)
  }

  /**
   * Ensure that we respect the fetch offset when returning records that were converted from an uncompressed v2
   * record batch to multiple v0/v1 record batches with size 1. If the fetch offset points to inside the record batch,
   * some records have to be dropped during the conversion.
   */
  @nowarn("cat=deprecation")
  @Test
  def testDownConversionFromBatchedToUnbatchedRespectsOffset(): Unit = {
    // Increase linger so that we have control over the batches created
    producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(servers),
      retries = 5,
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer,
      lingerMs = 30 * 1000,
      deliveryTimeoutMs = 60 * 1000)

    val topicConfig = Map(LogConfig.MessageFormatVersionProp -> KAFKA_0_11_0_IV2.version)
    val (topicPartition, leaderId) = createTopics(numTopics = 1, numPartitions = 1, topicConfig).head
    val topic = topicPartition.topic
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava

    val firstBatchFutures = (0 until 10).map(i => producer.send(new ProducerRecord(topic, s"key-$i", s"value-$i")))
    producer.flush()
    val secondBatchFutures = (10 until 25).map(i => producer.send(new ProducerRecord(topic, s"key-$i", s"value-$i")))
    producer.flush()

    firstBatchFutures.foreach(_.get)
    secondBatchFutures.foreach(_.get)

    def check(fetchOffset: Long, requestVersion: Short, expectedOffset: Long, expectedNumBatches: Int, expectedMagic: Byte): Unit = {
      var batchesReceived = 0
      var currentFetchOffset = fetchOffset
      var currentExpectedOffset = expectedOffset

      // With KIP-283, we might not receive all batches in a single fetch request so loop through till we have consumed
      // all batches we are interested in.
      while (batchesReceived < expectedNumBatches) {
        val fetchRequest = FetchRequest.Builder.forConsumer(requestVersion, Int.MaxValue, 0, createPartitionMap(Int.MaxValue,
          Seq(topicPartition), Map(topicPartition -> currentFetchOffset)), topicIds).build(requestVersion)
        val fetchResponse = sendFetchRequest(leaderId, fetchRequest)

        // validate response
        val partitionData = fetchResponse.responseData(topicNames, requestVersion).get(topicPartition)
        assertEquals(Errors.NONE.code, partitionData.errorCode)
        assertTrue(partitionData.highWatermark > 0)
        val batches = FetchResponse.recordsOrFail(partitionData).batches.asScala.toBuffer
        val batch = batches.head
        assertEquals(expectedMagic, batch.magic)
        assertEquals(currentExpectedOffset, batch.baseOffset)

        currentFetchOffset = batches.last.lastOffset + 1
        currentExpectedOffset += (batches.last.lastOffset - batches.head.baseOffset + 1)
        batchesReceived += batches.size
      }

      assertEquals(expectedNumBatches, batchesReceived)
    }

    // down conversion to message format 0, batches of 1 message are returned so we receive the exact offset we requested
    check(fetchOffset = 3, expectedOffset = 3, requestVersion = 1, expectedNumBatches = 22,
      expectedMagic = RecordBatch.MAGIC_VALUE_V0)
    check(fetchOffset = 15, expectedOffset = 15, requestVersion = 1, expectedNumBatches = 10,
      expectedMagic = RecordBatch.MAGIC_VALUE_V0)

    // down conversion to message format 1, batches of 1 message are returned so we receive the exact offset we requested
    check(fetchOffset = 3, expectedOffset = 3, requestVersion = 3, expectedNumBatches = 22,
      expectedMagic = RecordBatch.MAGIC_VALUE_V1)
    check(fetchOffset = 15, expectedOffset = 15, requestVersion = 3, expectedNumBatches = 10,
      expectedMagic = RecordBatch.MAGIC_VALUE_V1)

    // no down conversion, we receive a single batch so the received offset won't necessarily be the same
    check(fetchOffset = 3, expectedOffset = 0, requestVersion = 4, expectedNumBatches = 2,
      expectedMagic = RecordBatch.MAGIC_VALUE_V2)
    check(fetchOffset = 15, expectedOffset = 10, requestVersion = 4, expectedNumBatches = 1,
      expectedMagic = RecordBatch.MAGIC_VALUE_V2)

    // no down conversion, we receive a single batch and the exact offset we requested because it happens to be the
    // offset of the first record in the batch
    check(fetchOffset = 10, expectedOffset = 10, requestVersion = 4, expectedNumBatches = 1,
      expectedMagic = RecordBatch.MAGIC_VALUE_V2)
  }
}
