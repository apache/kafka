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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.NotLeaderOrFollowerException
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch, SimpleRecord}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.coordinator.common.runtime.PartitionWriter
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogConfig, VerificationGuard}
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{mock, verify, when}

import java.nio.charset.Charset
import java.util.Collections
import scala.collection.Map
import scala.jdk.CollectionConverters._

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

    when(replicaManager.getLogConfig(tp)).thenReturn(Some(new LogConfig(Map.empty.asJava)))
    assertEquals(new LogConfig(Map.empty.asJava), partitionRecordWriter.config(tp))

    when(replicaManager.getLogConfig(tp)).thenReturn(None)
    assertThrows(classOf[NotLeaderOrFollowerException], () => partitionRecordWriter.config(tp))
  }


  @Test
  def testWriteRecords(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager
    )

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
      batch
    ))

    assertEquals(
      batch,
      recordsCapture.getValue.getOrElse(tp,
        throw new AssertionError(s"No records for $tp"))
    )
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
    val partitionRecordWriter = new CoordinatorPartitionWriter(
      replicaManager
    )

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
      batch
    ))
  }
}
