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
import org.apache.kafka.common.record.{CompressionType, SimpleRecord, MemoryRecords}

import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalatest.Assertions.intercept

import scala.collection.JavaConverters._

class TransactionLogTest {

  val producerEpoch: Short = 0
  val transactionTimeoutMs: Int = 1000

  val topicPartitions: Set[TopicPartition] = Set[TopicPartition](new TopicPartition("topic1", 0),
    new TopicPartition("topic1", 1),
    new TopicPartition("topic2", 0),
    new TopicPartition("topic2", 1),
    new TopicPartition("topic2", 2))

  @Test
  def shouldThrowExceptionWriteInvalidTxn() {
    val transactionalId = "transactionalId"
    val producerId = 23423L

    val txnMetadata = TransactionMetadata(transactionalId, producerId, producerEpoch, transactionTimeoutMs, 0)
    txnMetadata.addPartitions(topicPartitions)

    intercept[IllegalStateException] {
      TransactionLog.valueToBytes(txnMetadata.prepareNoTransit())
    }
  }

  @Test
  def shouldReadWriteMessages() {
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
      val txnMetadata = TransactionMetadata(transactionalId, producerId, producerEpoch, transactionTimeoutMs,
        transactionStates(producerId), 0)

      if (!txnMetadata.state.equals(Empty))
        txnMetadata.addPartitions(topicPartitions)

      val keyBytes = TransactionLog.keyToBytes(transactionalId)
      val valueBytes = TransactionLog.valueToBytes(txnMetadata.prepareNoTransit())

      new SimpleRecord(keyBytes, valueBytes)
    }.toSeq

    val records = MemoryRecords.withRecords(0, CompressionType.NONE, txnRecords: _*)

    var count = 0
    for (record <- records.records.asScala) {
      val txnKey = TransactionLog.readTxnRecordKey(record.key)
      val transactionalId = txnKey.transactionalId
      val txnMetadata = TransactionLog.readTxnRecordValue(transactionalId, record.value)

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

}
