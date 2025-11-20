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
package kafka.coordinator.group

import kafka.server.{LogAppendResult, ReplicaManager}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.NotLeaderOrFollowerException
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, ControlRecordType, EndTransactionMarker, MemoryRecords, RecordBatch, RecordValidationStats, SimpleRecord}
import org.apache.kafka.coordinator.common.runtime.PartitionWriter
import org.apache.kafka.server.common.TransactionVersion
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogAppendInfo, LogConfig, VerificationGuard}
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{mock, verify, when}

import java.nio.charset.Charset
import java.util
import java.util.Optional
import scala.collection.Map

class CoordinatorPartitionWriterTest {
  @Test
  def testRegisterDeregisterListener(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager
    )

    val listener = new PartitionWriter.Listener {
      override def onHighWatermarkUpdated(tp: TopicPartition, offset: Long): Unit = {}
    }

    partitionRecordWriter.registerListener(tp, listener)
    verify(replicaManager).maybeAddListener(tp, new ListenerAdapter(listener))

    partitionRecordWriter.deregisterListener(tp, listener)
    verify(replicaManager).removeListener(tp, new ListenerAdapter(listener))

    assertEquals(
      new ListenerAdapter(listener),
      new ListenerAdapter(listener)
    )
    assertEquals(
      new ListenerAdapter(listener).hashCode(),
      new ListenerAdapter(listener).hashCode()
    )
  }

  @Test
  def testConfig(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager
    )

    when(replicaManager.getLogConfig(tp)).thenReturn(Some(new LogConfig(util.Map.of)))
    assertEquals(new LogConfig(util.Map.of), partitionRecordWriter.config(tp))

    when(replicaManager.getLogConfig(tp)).thenReturn(None)
    assertThrows(classOf[NotLeaderOrFollowerException], () => partitionRecordWriter.config(tp))
  }


  @Test
  def testWriteRecords(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val topicId = Uuid.fromString("TbEp6-A4s3VPT1TwiI5COw")
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.topicIdPartition(tp)).thenReturn(new TopicIdPartition(topicId, tp))

    val partitionRecordWriter = new CoordinatorPartitionWriter(
        replicaManager
    )

    val recordsCapture: ArgumentCaptor[Map[TopicIdPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, MemoryRecords]])

    when(replicaManager.appendRecordsToLeader(
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      recordsCapture.capture(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.eq(Map(tp -> VerificationGuard.SENTINEL)),
      ArgumentMatchers.eq(TransactionVersion.TV_UNKNOWN)
    )).thenReturn(Map(new TopicIdPartition(topicId, tp) -> LogAppendResult(
      new LogAppendInfo(
        5L,
        10L,
        Optional.empty,
        RecordBatch.NO_TIMESTAMP,
        0L,
        0L,
        RecordValidationStats.EMPTY,
        CompressionType.NONE,
        100,
        10L
      ),
      Option.empty,
      hasCustomErrorMessage = false
    )))

    // Test non-transactional records (regular coordinator records) - should use TV_UNKNOWN
    val batch = MemoryRecords.withRecords(
      Compression.NONE,
      new SimpleRecord(
        0L,
        "foo".getBytes(Charset.defaultCharset()),
        "bar".getBytes(Charset.defaultCharset())
      )
    )

    assertEquals(11, partitionRecordWriter.append(
      tp,
      VerificationGuard.SENTINEL,
      batch,
      TransactionVersion.TV_UNKNOWN
    ))
    assertEquals(
      batch,
      recordsCapture.getValue.getOrElse(new TopicIdPartition(topicId, tp), throw new AssertionError(s"No records for $tp"))
    )
  }

  @Test
  def testWriteTransactionMarker(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val topicId = Uuid.fromString("TbEp6-A4s3VPT1TwiI5COw")
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.topicIdPartition(tp)).thenReturn(new TopicIdPartition(topicId, tp))

    val partitionRecordWriter = new CoordinatorPartitionWriter(
        replicaManager
    )

    val recordsCapture: ArgumentCaptor[Map[TopicIdPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, MemoryRecords]])

    when(replicaManager.appendRecordsToLeader(
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      recordsCapture.capture(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.eq(Map(tp -> VerificationGuard.SENTINEL)),
      ArgumentMatchers.eq(TransactionVersion.TV_2.featureLevel())
    )).thenReturn(Map(new TopicIdPartition(topicId, tp) -> LogAppendResult(
      new LogAppendInfo(
        5L,
        10L,
        Optional.empty,
        RecordBatch.NO_TIMESTAMP,
        0L,
        0L,
        RecordValidationStats.EMPTY,
        CompressionType.NONE,
        100,
        10L
      ),
      Option.empty,
      hasCustomErrorMessage = false
    )))

    // Test transactional records (transaction marker) - should use explicit transaction version
    val producerId = 100L
    val producerEpoch = 5.toShort
    val markerBatch = MemoryRecords.withEndTransactionMarker(
      System.currentTimeMillis(),
      producerId,
      producerEpoch,
      new EndTransactionMarker(ControlRecordType.COMMIT, 1)
    )

    assertEquals(11, partitionRecordWriter.append(
      tp,
      VerificationGuard.SENTINEL,
      markerBatch,
      TransactionVersion.TV_2.featureLevel()
    ))
    assertEquals(
      markerBatch,
      recordsCapture.getValue.getOrElse(new TopicIdPartition(topicId, tp), throw new AssertionError(s"No records for $tp"))
    )
  }

  @Test
  def testWriteTransactionMarkerWithTVUnknownThrowsException(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val topicId = Uuid.fromString("TbEp6-A4s3VPT1TwiI5COw")
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.topicIdPartition(tp)).thenReturn(new TopicIdPartition(topicId, tp))

    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager
    )

    val recordsCapture: ArgumentCaptor[Map[TopicIdPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, MemoryRecords]])

    // Mock ReplicaManager to throw IllegalArgumentException when TV_UNKNOWN is passed for a transaction marker
    // This simulates the validation error from ProducerAppendInfo.appendEndTxnMarker()
    when(replicaManager.appendRecordsToLeader(
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      recordsCapture.capture(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.eq(Map(tp -> VerificationGuard.SENTINEL)),
      ArgumentMatchers.eq(TransactionVersion.TV_UNKNOWN)
    )).thenThrow(new IllegalArgumentException(
      "transactionVersion must be explicitly specified (TV_0, TV_1, or TV_2), " +
      "cannot use default value TV_UNKNOWN for origin COORDINATOR"
    ))

    // Test that passing TV_UNKNOWN for a transaction marker throws IllegalArgumentException
    val producerId = 100L
    val producerEpoch = 5.toShort
    val markerBatch = MemoryRecords.withEndTransactionMarker(
      System.currentTimeMillis(),
      producerId,
      producerEpoch,
      new EndTransactionMarker(ControlRecordType.COMMIT, 1)
    )

    val exception = assertThrows(classOf[IllegalArgumentException], () => partitionRecordWriter.append(
      tp,
      VerificationGuard.SENTINEL,
      markerBatch,
      TransactionVersion.TV_UNKNOWN
    ))
    assertTrue(exception.getMessage.contains("transactionVersion must be explicitly specified"))
    assertTrue(exception.getMessage.contains("TV_UNKNOWN"))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[Errors], names = Array("NONE", "NOT_ENOUGH_REPLICAS"))
  def testMaybeStartTransactionVerification(error: Errors): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager
    )

    val verificationGuard = if (error == Errors.NONE) {
      new VerificationGuard()
    } else {
      VerificationGuard.SENTINEL
    }

    val callbackCapture: ArgumentCaptor[((Errors, VerificationGuard)) => Unit] =
      ArgumentCaptor.forClass(classOf[((Errors, VerificationGuard)) => Unit])

    when(replicaManager.maybeSendPartitionToTransactionCoordinator(
      ArgumentMatchers.eq(tp),
      ArgumentMatchers.eq("transactional-id"),
      ArgumentMatchers.eq(10L),
      ArgumentMatchers.eq(5.toShort),
      ArgumentMatchers.eq(RecordBatch.NO_SEQUENCE),
      callbackCapture.capture(),
      ArgumentMatchers.any()
    )).thenAnswer(_ => {
      callbackCapture.getValue.apply((
        error,
        verificationGuard
      ))
    })

    val future = partitionRecordWriter.maybeStartTransactionVerification(
      tp,
      "transactional-id",
      10L,
      5.toShort,
      ApiKeys.TXN_OFFSET_COMMIT.latestVersion().toInt
    )

    if (error == Errors.NONE) {
      assertEquals(verificationGuard, future.get)
    } else {
      assertFutureThrows(error.exception.getClass, future)
    }
  }

  @Test
  def testWriteRecordsWithFailure(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val topicId = Uuid.fromString("TbEp6-A4s3VPT1TwiI5COw")
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.topicIdPartition(tp)).thenReturn(new TopicIdPartition(topicId, tp))

    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager
    )

    val recordsCapture: ArgumentCaptor[Map[TopicIdPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, MemoryRecords]])

    when(replicaManager.appendRecordsToLeader(
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      recordsCapture.capture(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.eq(Map(tp -> VerificationGuard.SENTINEL)),
      ArgumentMatchers.eq(TransactionVersion.TV_UNKNOWN)
    )).thenReturn(Map(new TopicIdPartition(topicId, tp) -> LogAppendResult(
      LogAppendInfo.UNKNOWN_LOG_APPEND_INFO,
      Some(Errors.NOT_LEADER_OR_FOLLOWER.exception),
      hasCustomErrorMessage = false
    )))

    val batch = MemoryRecords.withRecords(
      Compression.NONE,
      new SimpleRecord(
        0L,
        "foo".getBytes(Charset.defaultCharset()),
        "bar".getBytes(Charset.defaultCharset())
      )
    )

    assertThrows(classOf[NotLeaderOrFollowerException], () => partitionRecordWriter.append(
      tp,
      VerificationGuard.SENTINEL,
      batch,
      TransactionVersion.TV_UNKNOWN
    ))
  }

  @Test
  def testDeleteRecordsResponseContainsError(): Unit = {
    val replicaManager = mock(classOf[ReplicaManager])
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager
    )

    val callbackCapture: ArgumentCaptor[Map[TopicPartition, DeleteRecordsPartitionResult] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, DeleteRecordsPartitionResult] => Unit])

    // Response contains error.
    when(replicaManager.deleteRecords(
      ArgumentMatchers.anyLong(),
      ArgumentMatchers.any(),
      callbackCapture.capture(),
      ArgumentMatchers.eq(true)
    )).thenAnswer { _ =>
      callbackCapture.getValue.apply(Map(
        new TopicPartition("random-topic", 0) -> new DeleteRecordsPartitionResult()
          .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code
          )))
    }

    partitionRecordWriter.deleteRecords(
      new TopicPartition("random-topic", 0),
      10L
    ).whenComplete { (_, exp) =>
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.exception, exp)
    }

    // Empty response
    when(replicaManager.deleteRecords(
      ArgumentMatchers.anyLong(),
      ArgumentMatchers.any(),
      callbackCapture.capture(),
      ArgumentMatchers.eq(true)
    )).thenAnswer { _ =>
      callbackCapture.getValue.apply(Map[TopicPartition, DeleteRecordsPartitionResult]())
    }

    partitionRecordWriter.deleteRecords(
      new TopicPartition("random-topic", 0),
      10L
    ).whenComplete { (_, exp) =>
      assertTrue(exp.isInstanceOf[IllegalStateException])
    }
  }

  @Test
  def testDeleteRecordsSuccess(): Unit = {
    val replicaManager = mock(classOf[ReplicaManager])
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager
    )

    val callbackCapture: ArgumentCaptor[Map[TopicPartition, DeleteRecordsPartitionResult] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, DeleteRecordsPartitionResult] => Unit])

    // response contains error
    when(replicaManager.deleteRecords(
      ArgumentMatchers.anyLong(),
      ArgumentMatchers.any(),
      callbackCapture.capture(),
      ArgumentMatchers.eq(true)
    )).thenAnswer { _ =>
      callbackCapture.getValue.apply(Map(
        new TopicPartition("random-topic", 0) -> new DeleteRecordsPartitionResult()
          .setErrorCode(Errors.NONE.code)
      ))
    }

    partitionRecordWriter.deleteRecords(
      new TopicPartition("random-topic", 0),
      10L
    ).whenComplete { (_, exp) =>
      assertNull(exp)
    }
  }
}
