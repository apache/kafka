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
package kafka.coordinator

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{CompressionType, KafkaRecord, MemoryRecords}

import org.junit.Test
import org.junit.Assert.assertEquals
import org.scalatest.junit.JUnitSuite

import scala.collection.JavaConverters._

class TransactionLogManagerTest extends JUnitSuite {

  val epoch: Short = 0
  val transactionTimeoutMs: Int = 1000

  val topicPartitions: Set[TopicPartition] = Set[TopicPartition](new TopicPartition("topic1", 0),
    new TopicPartition("topic1", 1),
    new TopicPartition("topic2", 0),
    new TopicPartition("topic2", 1),
    new TopicPartition("topic2", 2))

  @Test
  def shouldThrowExceptionWriteInvalidTxn() {
    val txnMetadata = new TransactionMetadata(NotExist)
    txnMetadata.addPartitions(topicPartitions)

    val pidMetadata = new PidMetadata(0L, epoch, transactionTimeoutMs, txnMetadata)

    intercept[IllegalStateException] {
      TransactionLogManager.valueToBytes(pidMetadata)
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

    val transactionStates = Map[Long, TransactionState](0L -> NotExist,
      1L -> Ongoing,
      2L -> PrepareCommit,
      3L -> CompleteCommit,
      4L -> PrepareAbort,
      5L -> CompleteAbort)

    // generate transaction log messages

    val txnRecords = pidMappings.map { case (transactionalId, pid) =>
      val txnMetadata = new TransactionMetadata(transactionStates(pid))

      if (!txnMetadata.state.equals(NotExist))
        txnMetadata.addPartitions(topicPartitions)

      val pidMetadata = new PidMetadata(pid, epoch, transactionTimeoutMs, txnMetadata)

      val keyBytes = TransactionLogManager.keyToBytes(transactionalId)
      val valueBytes = TransactionLogManager.valueToBytes(pidMetadata)

      new KafkaRecord(keyBytes, valueBytes)
    }.toSeq

    val records = MemoryRecords.withRecords(0, CompressionType.NONE, txnRecords: _*)

    var count = 0
    for (record <- records.records.asScala) {
      val key = TransactionLogManager.readMessageKey(record.key())

      key match {
        case pidKey: TxnKey =>
          val transactionalId = pidKey.key
          val pidMetadata = TransactionLogManager.readMessageValue(record.value())

          assertEquals(pidMappings(transactionalId), pidMetadata.pid)
          assertEquals(epoch, pidMetadata.epoch)
          assertEquals(transactionTimeoutMs, pidMetadata.txnTimeoutMs)

          val txnMetadata = pidMetadata.txnMetadata

          assertEquals(transactionStates(pidMetadata.pid), txnMetadata.state)

          if (txnMetadata.state.equals(NotExist))
            assertEquals(Set.empty[TopicPartition], txnMetadata.topicPartitions)
          else
            assertEquals(topicPartitions, txnMetadata.topicPartitions)

          count = count + 1

        case _ => fail(s"Unexpected transaction topic message key $key")
      }
    }

    assertEquals(pidMappings.size, count)
  }

}
