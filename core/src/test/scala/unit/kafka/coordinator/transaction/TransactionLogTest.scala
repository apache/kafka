/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.coordinator.transaction


import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.protocol.{ByteBufferAccessor, MessageUtil}
import org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection
import org.apache.kafka.common.protocol.types.{CompactArrayOf, Field, Schema, Struct, Type}
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch, SimpleRecord}
import org.apache.kafka.coordinator.transaction.generated.{TransactionLogKey, TransactionLogValue}
import org.apache.kafka.server.common.TransactionVersion.{TV_0, TV_2}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.ByteBuffer
import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._

class TransactionLogTest {

  val producerEpoch: Short = 0
  val transactionTimeoutMs: Int = 1000

  val topicPartitions: Set[TopicPartition] = Set[TopicPartition](new TopicPartition("topic1", 0),
    new TopicPartition("topic1", 1),
    new TopicPartition("topic2", 0),
    new TopicPartition("topic2", 1),
    new TopicPartition("topic2", 2))

  @Test
  def shouldThrowExceptionWriteInvalidTxn(): Unit = {
    val transactionalId = "transactionalId"
    val producerId = 23423L

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_ID, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, transactionTimeoutMs, Empty, collection.mutable.Set.empty[TopicPartition], 0, 0, TV_0)
    txnMetadata.addPartitions(topicPartitions)

    assertThrows(classOf[IllegalStateException], () => TransactionLog.valueToBytes(txnMetadata.prepareNoTransit(), TV_2))
  }

  @Test
  def shouldReadWriteMessages(): Unit = {
    val pidMappings = Map[String, Long]("zero" -> 0L,
      "one" -> 1L,
      "two" -> 2L,
      "three" -> 3L,
      "four" -> 4L,
      "five" -> 5L)

    val transactionStates = Map[Long, TransactionState](0L -> Empty,
      1L -> Ongoing,
      2L -> PrepareCommit,
      3L -> CompleteCommit,
      4L -> PrepareAbort,
      5L -> CompleteAbort)

    // generate transaction log messages
    val txnRecords = pidMappings.map { case (transactionalId, producerId) =>
      val txnMetadata = new TransactionMetadata(transactionalId, producerId, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_ID, producerEpoch,
        RecordBatch.NO_PRODUCER_EPOCH, transactionTimeoutMs, transactionStates(producerId), collection.mutable.Set.empty[TopicPartition], 0, 0, TV_0)

      if (!txnMetadata.state.equals(Empty))
        txnMetadata.addPartitions(topicPartitions)

      val keyBytes = TransactionLog.keyToBytes(transactionalId)
      val valueBytes = TransactionLog.valueToBytes(txnMetadata.prepareNoTransit(), TV_2)

      new SimpleRecord(keyBytes, valueBytes)
    }.toSeq

    val records = MemoryRecords.withRecords(0, Compression.NONE, txnRecords: _*)

    var count = 0
    for (record <- records.records.asScala) {
      val txnKey = TransactionLog.readTxnRecordKey(record.key)
      val transactionalId = txnKey.transactionalId
      val txnMetadata = TransactionLog.readTxnRecordValue(transactionalId, record.value).get

      assertEquals(pidMappings(transactionalId), txnMetadata.producerId)
      assertEquals(producerEpoch, txnMetadata.producerEpoch)
      assertEquals(transactionTimeoutMs, txnMetadata.txnTimeoutMs)
      assertEquals(transactionStates(txnMetadata.producerId), txnMetadata.state)

      if (txnMetadata.state.equals(Empty))
        assertEquals(Set.empty[TopicPartition], txnMetadata.topicPartitions)
      else
        assertEquals(topicPartitions, txnMetadata.topicPartitions)

      count = count + 1
    }

    assertEquals(pidMappings.size, count)
  }

  @Test
  def testSerializeTransactionLogValueToHighestNonFlexibleVersion(): Unit = {
    val txnTransitMetadata = TxnTransitMetadata(1, 1, 1, 1, 1, 1000, CompleteCommit, mutable.Set.empty, 500, 500, TV_0)
    val txnLogValueBuffer = ByteBuffer.wrap(TransactionLog.valueToBytes(txnTransitMetadata, TV_0))
    assertEquals(0, txnLogValueBuffer.getShort)
  }

  @Test
  def testSerializeTransactionLogValueToFlexibleVersion(): Unit = {
    val txnTransitMetadata = TxnTransitMetadata(1, 1, 1, 1, 1, 1000, CompleteCommit, mutable.Set.empty, 500, 500, TV_2)
    val txnLogValueBuffer = ByteBuffer.wrap(TransactionLog.valueToBytes(txnTransitMetadata, TV_2))
    assertEquals(TransactionLogValue.HIGHEST_SUPPORTED_VERSION, txnLogValueBuffer.getShort)
  }

  @Test
  def testDeserializeHighestSupportedTransactionLogValue(): Unit = {
    val txnPartitions = new TransactionLogValue.PartitionsSchema()
      .setTopic("topic")
      .setPartitionIds(java.util.Collections.singletonList(0))

    val txnLogValue = new TransactionLogValue()
      .setProducerId(100)
      .setProducerEpoch(50.toShort)
      .setTransactionStatus(CompleteCommit.id)
      .setTransactionStartTimestampMs(750L)
      .setTransactionLastUpdateTimestampMs(1000L)
      .setTransactionTimeoutMs(500)
      .setTransactionPartitions(java.util.Collections.singletonList(txnPartitions))

    val serialized = MessageUtil.toVersionPrefixedByteBuffer(1, txnLogValue)
    val deserialized = TransactionLog.readTxnRecordValue("transactionId", serialized).get

    assertEquals(100, deserialized.producerId)
    assertEquals(50, deserialized.producerEpoch)
    assertEquals(CompleteCommit, deserialized.state)
    assertEquals(750L, deserialized.txnStartTimestamp)
    assertEquals(1000L, deserialized.txnLastUpdateTimestamp)
    assertEquals(500, deserialized.txnTimeoutMs)

    val actualTxnPartitions = deserialized.topicPartitions
    assertEquals(1, actualTxnPartitions.size)
    assertTrue(actualTxnPartitions.contains(new TopicPartition("topic", 0)))
  }

  @Test
  def testDeserializeFutureTransactionLogValue(): Unit = {
    // Copy of TransactionLogValue.PartitionsSchema.SCHEMA_1 with a few
    // additional tagged fields.
    val futurePartitionsSchema = new Schema(
      new Field("topic", Type.COMPACT_STRING, ""),
      new Field("partition_ids", new CompactArrayOf(Type.INT32), ""),
      TaggedFieldsSection.of(
        Int.box(100), new Field("partition_foo", Type.STRING, ""),
        Int.box(101), new Field("partition_foo", Type.INT32, "")
      )
    )

    // Create TransactionLogValue.PartitionsSchema with tagged fields
    val txnPartitions = new Struct(futurePartitionsSchema)
    txnPartitions.set("topic", "topic")
    txnPartitions.set("partition_ids", Array(Integer.valueOf(1)))
    val txnPartitionsTaggedFields = new java.util.TreeMap[Integer, Any]()
    txnPartitionsTaggedFields.put(100, "foo")
    txnPartitionsTaggedFields.put(101, 4000)
    txnPartitions.set("_tagged_fields", txnPartitionsTaggedFields)

    // Copy of TransactionLogValue.SCHEMA_1 with a few
    // additional tagged fields.
    val futureTransactionLogValueSchema = new Schema(
      new Field("producer_id", Type.INT64, ""),
      new Field("producer_epoch", Type.INT16, ""),
      new Field("transaction_timeout_ms", Type.INT32, ""),
      new Field("transaction_status", Type.INT8, ""),
      new Field("transaction_partitions", CompactArrayOf.nullable(futurePartitionsSchema), ""),
      new Field("transaction_last_update_timestamp_ms", Type.INT64, ""),
      new Field("transaction_start_timestamp_ms", Type.INT64, ""),
      TaggedFieldsSection.of(
        Int.box(100), new Field("txn_foo", Type.STRING, ""),
        Int.box(101), new Field("txn_bar", Type.INT32, "")
      )
    )

    // Create TransactionLogValue with tagged fields
    val transactionLogValue = new Struct(futureTransactionLogValueSchema)
    transactionLogValue.set("producer_id", 1000L)
    transactionLogValue.set("producer_epoch", 100.toShort)
    transactionLogValue.set("transaction_timeout_ms", 1000)
    transactionLogValue.set("transaction_status", CompleteCommit.id)
    transactionLogValue.set("transaction_partitions", Array(txnPartitions))
    transactionLogValue.set("transaction_last_update_timestamp_ms", 2000L)
    transactionLogValue.set("transaction_start_timestamp_ms", 3000L)
    val txnLogValueTaggedFields = new java.util.TreeMap[Integer, Any]()
    txnLogValueTaggedFields.put(100, "foo")
    txnLogValueTaggedFields.put(101, 4000)
    transactionLogValue.set("_tagged_fields", txnLogValueTaggedFields)

    // Prepare the buffer.
    val buffer = ByteBuffer.allocate(transactionLogValue.sizeOf() + 2)
    buffer.put(0.toByte)
    buffer.put(1.toByte) // Add 1 as version.
    transactionLogValue.writeTo(buffer)
    buffer.flip()

    // Read the buffer with the real schema and verify that tagged
    // fields were read but ignored.
    buffer.getShort() // Skip version.
    val value = new TransactionLogValue(new ByteBufferAccessor(buffer), 1.toShort)
    assertEquals(Seq(100, 101), value.unknownTaggedFields().asScala.map(_.tag))
    assertEquals(Seq(100, 101), value.transactionPartitions().get(0).unknownTaggedFields().asScala.map(_.tag))

    // Read the buffer with readTxnRecordValue.
    buffer.rewind()
    val txnMetadata = TransactionLog.readTxnRecordValue("transaction-id", buffer).get
    assertEquals(1000L, txnMetadata.producerId)
    assertEquals(100, txnMetadata.producerEpoch)
    assertEquals(1000L, txnMetadata.txnTimeoutMs)
    assertEquals(CompleteCommit, txnMetadata.state)
    assertEquals(Set(new TopicPartition("topic", 1)), txnMetadata.topicPartitions)
    assertEquals(2000L, txnMetadata.txnLastUpdateTimestamp)
    assertEquals(3000L, txnMetadata.txnStartTimestamp)
  }

  @Test
  def testReadTxnRecordKeyCanReadUnknownMessage(): Unit = {
    val record = new TransactionLogKey()
    val unknownRecord = MessageUtil.toVersionPrefixedBytes(Short.MaxValue, record)
    val key = TransactionLog.readTxnRecordKey(ByteBuffer.wrap(unknownRecord))
    assertEquals(UnknownKey(Short.MaxValue), key)
  }
}
