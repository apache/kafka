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
import org.apache.kafka.common.record.{CompressionType, FileRecords, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.{MockTime, Time}
import org.apache.kafka.coordinator.group.runtime.CoordinatorLoader.UnknownRecordTypeException
import org.apache.kafka.coordinator.group.runtime.{CoordinatorLoader, CoordinatorPlayback}
import org.apache.kafka.storage.internals.log.{FetchDataInfo, FetchIsolation, LogOffsetMetadata}
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.{Test, Timeout}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{mock, verify, when}
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

      assertNotNull(loader.load(tp, coordinator).get(10, TimeUnit.SECONDS))

      verify(coordinator).replay(("k1", "v1"))
      verify(coordinator).replay(("k2", "v2"))
      verify(coordinator).replay(("k3", "v3"))
      verify(coordinator).replay(("k4", "v4"))
      verify(coordinator).replay(("k5", "v5"))
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

      verify(coordinator).replay(("k2", "v2"))
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

  private def logReadResult(
    startOffset: Long,
    records: Seq[SimpleRecord]
  ): FetchDataInfo = {
    val fileRecords = mock(classOf[FileRecords])
    val memoryRecords = MemoryRecords.withRecords(
      startOffset,
      CompressionType.NONE,
      records: _*
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
