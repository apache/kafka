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

import kafka.log.UnifiedLog
import kafka.server.ReplicaManager
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.NotLeaderOrFollowerException
import org.apache.kafka.common.record.{CompressionType, ControlRecordType, EndTransactionMarker, FileRecords, MemoryRecords, RecordBatch, SimpleRecord}
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.{MockTime, Time}
import org.apache.kafka.coordinator.group.runtime.CoordinatorLoader.UnknownRecordTypeException
import org.apache.kafka.coordinator.group.runtime.{CoordinatorLoader, CoordinatorPlayback}
import org.apache.kafka.storage.internals.log.{FetchDataInfo, FetchIsolation, LogOffsetMetadata}
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.{Test, Timeout}
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{mock, times, verify, when}
import org.mockito.invocation.InvocationOnMock

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.concurrent.{CountDownLatch, TimeUnit}

class StringKeyValueDeserializer extends CoordinatorLoader.Deserializer[(String, String)] {
  override def deserialize(key: ByteBuffer, value: ByteBuffer): (String, String) = {
    (
      Charset.defaultCharset().decode(key).toString,
      Charset.defaultCharset().decode(value).toString
    )
  }
}

@Timeout(60)
class CoordinatorLoaderImplTest {
  @Test
  def testNonexistentPartition(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val serde = mock(classOf[CoordinatorLoader.Deserializer[(String, String)]])
    val coordinator = mock(classOf[CoordinatorPlayback[(String, String)]])

    TestUtils.resource(new CoordinatorLoaderImpl[(String, String)](
      time = Time.SYSTEM,
      replicaManager = replicaManager,
      deserializer = serde,
      loadBufferSize = 1000
    )) { loader =>
      when(replicaManager.getLog(tp)).thenReturn(None)

      val result = loader.load(tp, coordinator)
      assertFutureThrows(result, classOf[NotLeaderOrFollowerException])
    }
  }

  @Test
  def testLoadingIsRejectedWhenClosed(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val serde = mock(classOf[CoordinatorLoader.Deserializer[(String, String)]])
    val coordinator = mock(classOf[CoordinatorPlayback[(String, String)]])

    TestUtils.resource(new CoordinatorLoaderImpl[(String, String)](
      time = Time.SYSTEM,
      replicaManager = replicaManager,
      deserializer = serde,
      loadBufferSize = 1000
    )) { loader =>
      loader.close()

      val result = loader.load(tp, coordinator)
      assertFutureThrows(result, classOf[RuntimeException])
    }
  }

  @Test
  def testLoading(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val serde = new StringKeyValueDeserializer
    val log = mock(classOf[UnifiedLog])
    val coordinator = mock(classOf[CoordinatorPlayback[(String, String)]])

    TestUtils.resource(new CoordinatorLoaderImpl[(String, String)](
      time = Time.SYSTEM,
      replicaManager = replicaManager,
      deserializer = serde,
      loadBufferSize = 1000
    )) { loader =>
      when(replicaManager.getLog(tp)).thenReturn(Some(log))
      when(log.logStartOffset).thenReturn(0L)
      when(replicaManager.getLogEndOffset(tp)).thenReturn(Some(9L))
      when(log.highWatermark).thenReturn(0L)

      val readResult1 = logReadResult(startOffset = 0, records = Seq(
        new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes)
      ))

      when(log.read(
        startOffset = 0L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult1)

      val readResult2 = logReadResult(startOffset = 2, records = Seq(
        new SimpleRecord("k3".getBytes, "v3".getBytes),
        new SimpleRecord("k4".getBytes, "v4".getBytes),
        new SimpleRecord("k5".getBytes, "v5".getBytes)
      ))

      when(log.read(
        startOffset = 2L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult2)

      val readResult3 = logReadResult(startOffset = 5, producerId = 100L, producerEpoch = 5, records = Seq(
        new SimpleRecord("k6".getBytes, "v6".getBytes),
        new SimpleRecord("k7".getBytes, "v7".getBytes)
      ))

      when(log.read(
        startOffset = 5L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult3)

      val readResult4 = logReadResult(
        startOffset = 7,
        producerId = 100L,
        producerEpoch = 5,
        controlRecordType = ControlRecordType.COMMIT
      )

      when(log.read(
        startOffset = 7L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult4)

      val readResult5 = logReadResult(
        startOffset = 8,
        producerId = 500L,
        producerEpoch = 10,
        controlRecordType = ControlRecordType.ABORT
      )

      when(log.read(
        startOffset = 8L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult5)

      assertNotNull(loader.load(tp, coordinator).get(10, TimeUnit.SECONDS))

      verify(coordinator).replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k1", "v1"))
      verify(coordinator).replay(1L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k2", "v2"))
      verify(coordinator).replay(2L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k3", "v3"))
      verify(coordinator).replay(3L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k4", "v4"))
      verify(coordinator).replay(4L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k5", "v5"))
      verify(coordinator).replay(5L, 100L, 5.toShort, ("k6", "v6"))
      verify(coordinator).replay(6L, 100L, 5.toShort, ("k7", "v7"))
      verify(coordinator).replayEndTransactionMarker(100L, 5, TransactionResult.COMMIT)
      verify(coordinator).replayEndTransactionMarker(500L, 10, TransactionResult.ABORT)
      verify(coordinator).updateLastWrittenOffset(2)
      verify(coordinator).updateLastWrittenOffset(5)
      verify(coordinator).updateLastWrittenOffset(7)
      verify(coordinator).updateLastWrittenOffset(8)
      verify(coordinator).updateLastCommittedOffset(0)
    }
  }

  @Test
  def testLoadingStoppedWhenClosed(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val serde = new StringKeyValueDeserializer
    val log = mock(classOf[UnifiedLog])
    val coordinator = mock(classOf[CoordinatorPlayback[(String, String)]])

    TestUtils.resource(new CoordinatorLoaderImpl[(String, String)](
      time = Time.SYSTEM,
      replicaManager = replicaManager,
      deserializer = serde,
      loadBufferSize = 1000
    )) { loader =>
      when(replicaManager.getLog(tp)).thenReturn(Some(log))
      when(log.logStartOffset).thenReturn(0L)
      when(replicaManager.getLogEndOffset(tp)).thenReturn(Some(100L))

      val readResult = logReadResult(startOffset = 0, records = Seq(
        new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes)
      ))

      val latch = new CountDownLatch(1)
      when(log.read(
        startOffset = ArgumentMatchers.anyLong(),
        maxLength = ArgumentMatchers.eq(1000),
        isolation = ArgumentMatchers.eq(FetchIsolation.LOG_END),
        minOneMessage = ArgumentMatchers.eq(true)
      )).thenAnswer { _ =>
        latch.countDown()
        readResult
      }

      val result = loader.load(tp, coordinator)
      latch.await(10, TimeUnit.SECONDS)
      loader.close()

      val ex = assertFutureThrows(result, classOf[RuntimeException])
      assertEquals("Coordinator loader is closed.", ex.getMessage)
    }
  }

  @Test
  def testUnknownRecordTypeAreIgnored(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val serde = mock(classOf[StringKeyValueDeserializer])
    val log = mock(classOf[UnifiedLog])
    val coordinator = mock(classOf[CoordinatorPlayback[(String, String)]])

    TestUtils.resource(new CoordinatorLoaderImpl[(String, String)](
      time = Time.SYSTEM,
      replicaManager = replicaManager,
      deserializer = serde,
      loadBufferSize = 1000
    )) { loader =>
      when(replicaManager.getLog(tp)).thenReturn(Some(log))
      when(log.logStartOffset).thenReturn(0L)
      when(replicaManager.getLogEndOffset(tp)).thenReturn(Some(2L))

      val readResult = logReadResult(startOffset = 0, records = Seq(
        new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes)
      ))

      when(log.read(
        startOffset = 0L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult)

      when(serde.deserialize(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenThrow(new UnknownRecordTypeException(1))
        .thenReturn(("k2", "v2"))

      loader.load(tp, coordinator).get(10, TimeUnit.SECONDS)

      verify(coordinator).replay(1L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k2", "v2"))
    }
  }

  @Test
  def testDeserializationErrorFailsTheLoading(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val serde = mock(classOf[StringKeyValueDeserializer])
    val log = mock(classOf[UnifiedLog])
    val coordinator = mock(classOf[CoordinatorPlayback[(String, String)]])

    TestUtils.resource(new CoordinatorLoaderImpl[(String, String)](
      time = Time.SYSTEM,
      replicaManager = replicaManager,
      deserializer = serde,
      loadBufferSize = 1000
    )) { loader =>
      when(replicaManager.getLog(tp)).thenReturn(Some(log))
      when(log.logStartOffset).thenReturn(0L)
      when(replicaManager.getLogEndOffset(tp)).thenReturn(Some(2L))

      val readResult = logReadResult(startOffset = 0, records = Seq(
        new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes)
      ))

      when(log.read(
        startOffset = 0L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult)

      when(serde.deserialize(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenThrow(new RuntimeException("Error!"))

      val ex = assertFutureThrows(loader.load(tp, coordinator), classOf[RuntimeException])
      assertEquals("Error!", ex.getMessage)
    }
  }

  @Test
  def testLoadGroupAndOffsetsWithCorruptedLog(): Unit = {
    // Simulate a case where startOffset < endOffset but log is empty. This could theoretically happen
    // when all the records are expired and the active segment is truncated or when the partition
    // is accidentally corrupted.
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val serde = mock(classOf[StringKeyValueDeserializer])
    val log = mock(classOf[UnifiedLog])
    val coordinator = mock(classOf[CoordinatorPlayback[(String, String)]])

    TestUtils.resource(new CoordinatorLoaderImpl[(String, String)](
      time = Time.SYSTEM,
      replicaManager = replicaManager,
      deserializer = serde,
      loadBufferSize = 1000
    )) { loader =>
      when(replicaManager.getLog(tp)).thenReturn(Some(log))
      when(log.logStartOffset).thenReturn(0L)
      when(replicaManager.getLogEndOffset(tp)).thenReturn(Some(10L))

      val readResult = logReadResult(startOffset = 0, records = Seq())

      when(log.read(
        startOffset = 0L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult)

      assertNotNull(loader.load(tp, coordinator).get(10, TimeUnit.SECONDS))
    }
  }

  @Test
  def testLoadSummary(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val serde = new StringKeyValueDeserializer
    val log = mock(classOf[UnifiedLog])
    val coordinator = mock(classOf[CoordinatorPlayback[(String, String)]])
    val time = new MockTime()

    TestUtils.resource(new CoordinatorLoaderImpl[(String, String)](
      time,
      replicaManager = replicaManager,
      deserializer = serde,
      loadBufferSize = 1000
    )) { loader =>
      val startTimeMs = time.milliseconds()
      when(replicaManager.getLog(tp)).thenReturn(Some(log))
      when(log.logStartOffset).thenReturn(0L)
      when(replicaManager.getLogEndOffset(tp)).thenReturn(Some(5L))

      val readResult1 = logReadResult(startOffset = 0, records = Seq(
        new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes)
      ))

      when(log.read(
        startOffset = 0L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenAnswer((_: InvocationOnMock) => {
        time.sleep(1000)
        readResult1
      })

      val readResult2 = logReadResult(startOffset = 2, records = Seq(
        new SimpleRecord("k3".getBytes, "v3".getBytes),
        new SimpleRecord("k4".getBytes, "v4".getBytes),
        new SimpleRecord("k5".getBytes, "v5".getBytes)
      ))

      when(log.read(
        startOffset = 2L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult2)

      val summary = loader.load(tp, coordinator).get(10, TimeUnit.SECONDS)
      assertEquals(startTimeMs, summary.startTimeMs())
      assertEquals(startTimeMs + 1000, summary.endTimeMs())
      assertEquals(5, summary.numRecords())
      assertEquals(readResult1.records.sizeInBytes() + readResult2.records.sizeInBytes(), summary.numBytes())
    }
  }

  @Test
  def testUpdateLastWrittenOffsetOnBatchLoaded(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val serde = new StringKeyValueDeserializer
    val log = mock(classOf[UnifiedLog])
    val coordinator = mock(classOf[CoordinatorPlayback[(String, String)]])

    TestUtils.resource(new CoordinatorLoaderImpl[(String, String)](
      time = Time.SYSTEM,
      replicaManager = replicaManager,
      deserializer = serde,
      loadBufferSize = 1000
    )) { loader =>
      when(replicaManager.getLog(tp)).thenReturn(Some(log))
      when(log.logStartOffset).thenReturn(0L)
      when(log.highWatermark).thenReturn(0L).thenReturn(0L).thenReturn(2L)
      when(replicaManager.getLogEndOffset(tp)).thenReturn(Some(7L))

      val readResult1 = logReadResult(startOffset = 0, records = Seq(
        new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes)
      ))

      when(log.read(
        startOffset = 0L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult1)

      val readResult2 = logReadResult(startOffset = 2, records = Seq(
        new SimpleRecord("k3".getBytes, "v3".getBytes),
        new SimpleRecord("k4".getBytes, "v4".getBytes),
        new SimpleRecord("k5".getBytes, "v5".getBytes)
      ))

      when(log.read(
        startOffset = 2L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult2)

      val readResult3 = logReadResult(startOffset = 5, records = Seq(
        new SimpleRecord("k6".getBytes, "v6".getBytes),
        new SimpleRecord("k7".getBytes, "v7".getBytes)
      ))

      when(log.read(
        startOffset = 5L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult3)

      assertNotNull(loader.load(tp, coordinator).get(10, TimeUnit.SECONDS))

      verify(coordinator).replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k1", "v1"))
      verify(coordinator).replay(1L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k2", "v2"))
      verify(coordinator).replay(2L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k3", "v3"))
      verify(coordinator).replay(3L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k4", "v4"))
      verify(coordinator).replay(4L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k5", "v5"))
      verify(coordinator).replay(5L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k6", "v6"))
      verify(coordinator).replay(6L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k7", "v7"))
      verify(coordinator, times(0)).updateLastWrittenOffset(0)
      verify(coordinator, times(1)).updateLastWrittenOffset(2)
      verify(coordinator, times(1)).updateLastWrittenOffset(5)
      verify(coordinator, times(1)).updateLastWrittenOffset(7)
      verify(coordinator, times(1)).updateLastCommittedOffset(0)
      verify(coordinator, times(1)).updateLastCommittedOffset(2)
      verify(coordinator, times(0)).updateLastCommittedOffset(5)
    }
  }

  @Test
  def testUpdateLastWrittenOffsetAndUpdateLastCommittedOffsetNoRecordsRead(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val serde = new StringKeyValueDeserializer
    val log = mock(classOf[UnifiedLog])
    val coordinator = mock(classOf[CoordinatorPlayback[(String, String)]])

    TestUtils.resource(new CoordinatorLoaderImpl[(String, String)](
      time = Time.SYSTEM,
      replicaManager = replicaManager,
      deserializer = serde,
      loadBufferSize = 1000
    )) { loader =>
      when(replicaManager.getLog(tp)).thenReturn(Some(log))
      when(log.logStartOffset).thenReturn(0L)
      when(log.highWatermark).thenReturn(0L)
      when(replicaManager.getLogEndOffset(tp)).thenReturn(Some(0L))

      assertNotNull(loader.load(tp, coordinator).get(10, TimeUnit.SECONDS))

      verify(coordinator, times(0)).updateLastWrittenOffset(anyLong())
      verify(coordinator, times(0)).updateLastCommittedOffset(anyLong())
    }
  }

  @Test
  def testUpdateLastWrittenOffsetOnBatchLoadedWhileHighWatermarkAhead(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val serde = new StringKeyValueDeserializer
    val log = mock(classOf[UnifiedLog])
    val coordinator = mock(classOf[CoordinatorPlayback[(String, String)]])

    TestUtils.resource(new CoordinatorLoaderImpl[(String, String)](
      time = Time.SYSTEM,
      replicaManager = replicaManager,
      deserializer = serde,
      loadBufferSize = 1000
    )) { loader =>
      when(replicaManager.getLog(tp)).thenReturn(Some(log))
      when(log.logStartOffset).thenReturn(0L)
      when(log.highWatermark).thenReturn(5L).thenReturn(7L).thenReturn(7L)
      when(replicaManager.getLogEndOffset(tp)).thenReturn(Some(7L))

      val readResult1 = logReadResult(startOffset = 0, records = Seq(
        new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes)
      ))

      when(log.read(
        startOffset = 0L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult1)

      val readResult2 = logReadResult(startOffset = 2, records = Seq(
        new SimpleRecord("k3".getBytes, "v3".getBytes),
        new SimpleRecord("k4".getBytes, "v4".getBytes),
        new SimpleRecord("k5".getBytes, "v5".getBytes)
      ))

      when(log.read(
        startOffset = 2L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult2)

      val readResult3 = logReadResult(startOffset = 5, records = Seq(
        new SimpleRecord("k6".getBytes, "v6".getBytes),
        new SimpleRecord("k7".getBytes, "v7".getBytes)
      ))

      when(log.read(
        startOffset = 5L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult3)

      assertNotNull(loader.load(tp, coordinator).get(10, TimeUnit.SECONDS))

      verify(coordinator).replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k1", "v1"))
      verify(coordinator).replay(1L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k2", "v2"))
      verify(coordinator).replay(2L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k3", "v3"))
      verify(coordinator).replay(3L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k4", "v4"))
      verify(coordinator).replay(4L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k5", "v5"))
      verify(coordinator).replay(5L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k6", "v6"))
      verify(coordinator).replay(6L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, ("k7", "v7"))
      verify(coordinator, times(0)).updateLastWrittenOffset(0)
      verify(coordinator, times(0)).updateLastWrittenOffset(2)
      verify(coordinator, times(0)).updateLastWrittenOffset(5)
      verify(coordinator, times(1)).updateLastWrittenOffset(7)
      verify(coordinator, times(0)).updateLastCommittedOffset(0)
      verify(coordinator, times(0)).updateLastCommittedOffset(2)
      verify(coordinator, times(0)).updateLastCommittedOffset(5)
      verify(coordinator, times(1)).updateLastCommittedOffset(7)
    }
  }

  @Test
  def testPartitionGoesOfflineDuringLoad(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val serde = new StringKeyValueDeserializer
    val log = mock(classOf[UnifiedLog])
    val coordinator = mock(classOf[CoordinatorPlayback[(String, String)]])

    TestUtils.resource(new CoordinatorLoaderImpl[(String, String)](
      time = Time.SYSTEM,
      replicaManager = replicaManager,
      deserializer = serde,
      loadBufferSize = 1000
    )) { loader =>
      when(replicaManager.getLog(tp)).thenReturn(Some(log))
      when(log.logStartOffset).thenReturn(0L)
      when(log.highWatermark).thenReturn(0L)
      when(replicaManager.getLogEndOffset(tp)).thenReturn(Some(5L)).thenReturn(Some(-1L))

      val readResult1 = logReadResult(startOffset = 0, records = Seq(
        new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes)
      ))

      when(log.read(
        startOffset = 0L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult1)

      val readResult2 = logReadResult(startOffset = 2, records = Seq(
        new SimpleRecord("k3".getBytes, "v3".getBytes),
        new SimpleRecord("k4".getBytes, "v4".getBytes),
        new SimpleRecord("k5".getBytes, "v5".getBytes)
      ))

      when(log.read(
        startOffset = 2L,
        maxLength = 1000,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true
      )).thenReturn(readResult2)

      assertFutureThrows(loader.load(tp, coordinator), classOf[NotLeaderOrFollowerException])
    }
  }

  private def logReadResult(
    startOffset: Long,
    producerId: Long = RecordBatch.NO_PRODUCER_ID,
    producerEpoch: Short = RecordBatch.NO_PRODUCER_EPOCH,
    records: Seq[SimpleRecord]
  ): FetchDataInfo = {
    val fileRecords = mock(classOf[FileRecords])
    val memoryRecords = if (producerId == RecordBatch.NO_PRODUCER_ID) {
      MemoryRecords.withRecords(
        startOffset,
        CompressionType.NONE,
        records: _*
      )
    } else {
      MemoryRecords.withTransactionalRecords(
        startOffset,
        CompressionType.NONE,
        producerId,
        producerEpoch,
        0,
        RecordBatch.NO_PARTITION_LEADER_EPOCH,
        records: _*
      )
    }

    when(fileRecords.sizeInBytes).thenReturn(memoryRecords.sizeInBytes)

    val bufferCapture: ArgumentCaptor[ByteBuffer] = ArgumentCaptor.forClass(classOf[ByteBuffer])
    when(fileRecords.readInto(
      bufferCapture.capture(),
      ArgumentMatchers.anyInt())
    ).thenAnswer { _ =>
      val buffer = bufferCapture.getValue
      buffer.put(memoryRecords.buffer.duplicate)
      buffer.flip()
    }

    new FetchDataInfo(new LogOffsetMetadata(startOffset), fileRecords)
  }

  private def logReadResult(
    startOffset: Long,
    producerId: Long,
    producerEpoch: Short,
    controlRecordType: ControlRecordType
  ): FetchDataInfo = {
    val fileRecords = mock(classOf[FileRecords])
    val memoryRecords = MemoryRecords.withEndTransactionMarker(
      startOffset,
      0L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      producerId,
      producerEpoch,
      new EndTransactionMarker(controlRecordType, 0)
    )

    when(fileRecords.sizeInBytes).thenReturn(memoryRecords.sizeInBytes)

    val bufferCapture: ArgumentCaptor[ByteBuffer] = ArgumentCaptor.forClass(classOf[ByteBuffer])
    when(fileRecords.readInto(
      bufferCapture.capture(),
      ArgumentMatchers.anyInt())
    ).thenAnswer { _ =>
      val buffer = bufferCapture.getValue
      buffer.put(memoryRecords.buffer.duplicate)
      buffer.flip()
    }

    new FetchDataInfo(new LogOffsetMetadata(startOffset), fileRecords)
  }
}
