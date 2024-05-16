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

import kafka.server.ReplicaManager
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.{NotLeaderOrFollowerException, RecordTooLargeException}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, ControlRecordType, MemoryRecords, RecordBatch}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.{MockTime, Time}
import org.apache.kafka.coordinator.group.runtime.PartitionWriter
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogConfig, VerificationGuard}
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{mock, verify, when}

import java.nio.charset.Charset
import java.util.{Collections, Properties}
import scala.collection.Map
import scala.jdk.CollectionConverters._

class StringKeyValueSerializer extends PartitionWriter.Serializer[(String, String)] {
  override def serializeKey(record: (String, String)): Array[Byte] = {
    record._1.getBytes(Charset.defaultCharset())
  }

  override def serializeValue(record: (String, String)): Array[Byte] = {
    record._2.getBytes(Charset.defaultCharset())
  }
}

class CoordinatorPartitionWriterTest {
  @Test
  def testRegisterDeregisterListener(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager,
      new StringKeyValueSerializer(),
      CompressionType.NONE,
      Time.SYSTEM
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
  def testWriteRecords(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val time = new MockTime()
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager,
      new StringKeyValueSerializer(),
      CompressionType.NONE,
      time
    )

    when(replicaManager.getLogConfig(tp)).thenReturn(Some(LogConfig.fromProps(
      Collections.emptyMap(),
      new Properties()
    )))

    val recordsCapture: ArgumentCaptor[Map[TopicPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, MemoryRecords]])
    val callbackCapture: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.appendRecords(
      ArgumentMatchers.eq(0L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      recordsCapture.capture(),
      callbackCapture.capture(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.eq(Map(tp -> VerificationGuard.SENTINEL)),
    )).thenAnswer( _ => {
      callbackCapture.getValue.apply(Map(
        tp -> new PartitionResponse(
          Errors.NONE,
          5,
          10,
          RecordBatch.NO_TIMESTAMP,
          -1,
          Collections.emptyList(),
          ""
        )
      ))
    })

    val records = List(
      ("k0", "v0"),
      ("k1", "v1"),
      ("k2", "v2"),
    )

    assertEquals(11, partitionRecordWriter.append(
      tp,
      RecordBatch.NO_PRODUCER_ID,
      RecordBatch.NO_PRODUCER_EPOCH,
      VerificationGuard.SENTINEL,
      records.asJava
    ))

    val batch = recordsCapture.getValue.getOrElse(tp,
      throw new AssertionError(s"No records for $tp"))
    assertEquals(1, batch.batches().asScala.toList.size)

    val receivedRecords = batch.records.asScala.map { record =>
      (
        Charset.defaultCharset().decode(record.key).toString,
        Charset.defaultCharset().decode(record.value).toString,
      )
    }.toList

    assertEquals(records, receivedRecords)
  }

  @Test
  def testTransactionalWriteRecords(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val time = new MockTime()
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager,
      new StringKeyValueSerializer(),
      CompressionType.NONE,
      time
    )
    val verificationGuard = new VerificationGuard()

    when(replicaManager.getLogConfig(tp)).thenReturn(Some(LogConfig.fromProps(
      Collections.emptyMap(),
      new Properties()
    )))

    val recordsCapture: ArgumentCaptor[Map[TopicPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, MemoryRecords]])
    val callbackCapture: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.appendRecords(
      ArgumentMatchers.eq(0L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      recordsCapture.capture(),
      callbackCapture.capture(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.eq(Map(tp -> verificationGuard)),
    )).thenAnswer(_ => {
      callbackCapture.getValue.apply(Map(
        tp -> new PartitionResponse(
          Errors.NONE,
          5,
          10,
          RecordBatch.NO_TIMESTAMP,
          -1,
          Collections.emptyList(),
          ""
        )
      ))
    })

    val records = List(
      ("k0", "v0"),
      ("k1", "v1"),
      ("k2", "v2"),
    )

    assertEquals(11, partitionRecordWriter.append(
      tp,
      100L,
      50.toShort,
      verificationGuard,
      records.asJava
    ))

    val batch = recordsCapture.getValue.getOrElse(tp,
      throw new AssertionError(s"No records for $tp"))
    assertEquals(1, batch.batches().asScala.toList.size)

    val firstBatch = batch.batches.asScala.head
    assertEquals(100L, firstBatch.producerId)
    assertEquals(50.toShort, firstBatch.producerEpoch)
    assertTrue(firstBatch.isTransactional)

    val receivedRecords = batch.records.asScala.map { record =>
      (
        Charset.defaultCharset().decode(record.key).toString,
        Charset.defaultCharset().decode(record.value).toString,
      )
    }.toList

    assertEquals(records, receivedRecords)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[ControlRecordType], names = Array("COMMIT", "ABORT"))
  def testWriteEndTransactionMarker(controlRecordType: ControlRecordType): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val time = new MockTime()
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager,
      new StringKeyValueSerializer(),
      CompressionType.NONE,
      time
    )

    when(replicaManager.getLogConfig(tp)).thenReturn(Some(LogConfig.fromProps(
      Collections.emptyMap(),
      new Properties()
    )))

    val recordsCapture: ArgumentCaptor[Map[TopicPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, MemoryRecords]])
    val callbackCapture: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.appendRecords(
      ArgumentMatchers.eq(0L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      recordsCapture.capture(),
      callbackCapture.capture(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.eq(Map(tp -> VerificationGuard.SENTINEL)),
    )).thenAnswer(_ => {
      callbackCapture.getValue.apply(Map(
        tp -> new PartitionResponse(
          Errors.NONE,
          5,
          10,
          RecordBatch.NO_TIMESTAMP,
          -1,
          Collections.emptyList(),
          ""
        )
      ))
    })

    assertEquals(11, partitionRecordWriter.appendEndTransactionMarker(
      tp,
      100L,
      50.toShort,
      10,
      if (controlRecordType == ControlRecordType.COMMIT) TransactionResult.COMMIT else TransactionResult.ABORT
    ))

    val batch = recordsCapture.getValue.getOrElse(tp,
      throw new AssertionError(s"No records for $tp"))
    assertEquals(1, batch.batches.asScala.toList.size)

    val firstBatch = batch.batches.asScala.head
    assertEquals(100L, firstBatch.producerId)
    assertEquals(50.toShort, firstBatch.producerEpoch)
    assertTrue(firstBatch.isTransactional)
    assertTrue(firstBatch.isControlBatch)

    val receivedRecords = batch.records.asScala.map { record =>
      ControlRecordType.parse(record.key)
    }.toList

    assertEquals(List(controlRecordType), receivedRecords)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[Errors], names = Array("NONE", "NOT_ENOUGH_REPLICAS"))
  def testMaybeStartTransactionVerification(error: Errors): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val time = new MockTime()
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager,
      new StringKeyValueSerializer(),
      CompressionType.NONE,
      time
    )

    val verificationGuard = if (error == Errors.NONE) {
      new VerificationGuard()
    } else {
      VerificationGuard.SENTINEL
    }

    val callbackCapture: ArgumentCaptor[((Errors, VerificationGuard)) => Unit] =
      ArgumentCaptor.forClass(classOf[((Errors, VerificationGuard)) => Unit])

    when(replicaManager.maybeStartTransactionVerificationForPartition(
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
      ApiKeys.TXN_OFFSET_COMMIT.latestVersion()
    )

    if (error == Errors.NONE) {
      assertEquals(verificationGuard, future.get)
    } else {
      assertFutureThrows(future, error.exception.getClass)
    }
  }

  @Test
  def testWriteRecordsWithFailure(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val time = new MockTime()
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager,
      new StringKeyValueSerializer(),
      CompressionType.NONE,
      time
    )

    when(replicaManager.getLogConfig(tp)).thenReturn(Some(LogConfig.fromProps(
      Collections.emptyMap(),
      new Properties()
    )))

    val recordsCapture: ArgumentCaptor[Map[TopicPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, MemoryRecords]])
    val callbackCapture: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.appendRecords(
      ArgumentMatchers.eq(0L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      recordsCapture.capture(),
      callbackCapture.capture(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.eq(Map(tp -> VerificationGuard.SENTINEL)),
    )).thenAnswer(_ => {
      callbackCapture.getValue.apply(Map(
        tp -> new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER)
      ))
    })

    val records = List(
      ("k0", "v0"),
      ("k1", "v1"),
      ("k2", "v2"),
    )

    assertThrows(classOf[NotLeaderOrFollowerException], () => partitionRecordWriter.append(
      tp,
      RecordBatch.NO_PRODUCER_ID,
      RecordBatch.NO_PRODUCER_EPOCH,
      VerificationGuard.SENTINEL,
      records.asJava
    ))
  }

  @Test
  def testWriteRecordTooLarge(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager,
      new StringKeyValueSerializer(),
      CompressionType.NONE,
      Time.SYSTEM
    )

    val maxBatchSize = 16384
    when(replicaManager.getLogConfig(tp)).thenReturn(Some(LogConfig.fromProps(
      Map(TopicConfig.MAX_MESSAGE_BYTES_CONFIG -> maxBatchSize).asJava,
      new Properties()
    )))

    val randomBytes = TestUtils.randomBytes(maxBatchSize + 1)
    // We need more than one record here because the first record
    // is always allowed by the MemoryRecordsBuilder.
    val records = List(
      ("k0", new String(randomBytes)),
      ("k1", new String(randomBytes)),
    )

    assertThrows(classOf[RecordTooLargeException], () => partitionRecordWriter.append(
      tp,
      RecordBatch.NO_PRODUCER_ID,
      RecordBatch.NO_PRODUCER_EPOCH,
      VerificationGuard.SENTINEL,
      records.asJava
    ))
  }

  @Test
  def testWriteEmptyRecordList(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager,
      new StringKeyValueSerializer(),
      CompressionType.NONE,
      Time.SYSTEM
    )

    when(replicaManager.getLogConfig(tp)).thenReturn(Some(LogConfig.fromProps(
      Collections.emptyMap(),
      new Properties()
    )))

    assertThrows(classOf[IllegalStateException], () => partitionRecordWriter.append(
      tp,
      RecordBatch.NO_PRODUCER_ID,
      RecordBatch.NO_PRODUCER_EPOCH,
      VerificationGuard.SENTINEL,
      List.empty.asJava
    ))
  }

  @Test
  def testNonexistentPartition(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager,
      new StringKeyValueSerializer(),
      CompressionType.NONE,
      Time.SYSTEM
    )

    when(replicaManager.getLogConfig(tp)).thenReturn(None)

    val records = List(
      ("k0", "v0"),
      ("k1", "v1"),
      ("k2", "v2"),
    )

    assertThrows(classOf[NotLeaderOrFollowerException], () => partitionRecordWriter.append(
      tp,
      RecordBatch.NO_PRODUCER_ID,
      RecordBatch.NO_PRODUCER_EPOCH,
      VerificationGuard.SENTINEL,
      records.asJava
    ))
  }
}
