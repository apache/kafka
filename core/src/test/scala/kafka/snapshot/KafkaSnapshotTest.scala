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
package kafka.snapshot

import java.nio.ByteBuffer
import java.nio.file.Files
import kafka.utils.TestUtils
import org.apache.kafka.common.record.BufferSupplier.GrowableBufferSupplier
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.raft.OffsetAndEpoch
import org.junit.Assert._
import org.junit.Test
import scala.util.Random

final class KafkaSnapshotTest {
  import KafkaSnapshotTest._

  @Test
  def testWritingSnapshot(): Unit = {
    val tempDir = TestUtils.tempDir().toPath
    val offsetAndEpoch = new OffsetAndEpoch(10L, 3)
    val bufferSize = 256
    val batches = 10

    TestUtils.resource(KafkaSnapshotWriter(tempDir, offsetAndEpoch)) { snapshot =>
      val records = buildRecords(Array(ByteBuffer.wrap(Random.nextBytes(bufferSize))))
      for (_ <- 0 until batches) {
        snapshot.append(records)
      }

      snapshot.freeze()
    }

    // File should exist and the size should be greater than the sum of all the buffers
    assertTrue(Files.exists(snapshotPath(tempDir, offsetAndEpoch)))
    assertTrue(Files.size(snapshotPath(tempDir, offsetAndEpoch)) > bufferSize * batches)
  }

  @Test
  def testWriteReadSnapshot(): Unit = {
    val tempDir = TestUtils.tempDir().toPath
    val offsetAndEpoch = new OffsetAndEpoch(10L, 3)
    val bufferSize = 256
    val batches = 10

    val expectedBuffer = ByteBuffer.wrap(Random.nextBytes(bufferSize))

    TestUtils.resource(KafkaSnapshotWriter(tempDir, offsetAndEpoch)) { snapshot =>
      val records = buildRecords(Array(expectedBuffer))
      for (_ <- 0 until batches) {
        snapshot.append(records)
      }

      snapshot.freeze()
    }

    TestUtils.resource(KafkaSnapshotReader(tempDir, offsetAndEpoch)) { snapshot =>
      var countBatches = 0
      var countRecords = 0
      snapshot.forEach { batch =>
        countBatches += 1

        batch.streamingIterator(new GrowableBufferSupplier()).forEachRemaining { record =>
          countRecords += 1
          assertFalse(record.hasKey)
          assertTrue(record.hasValue)
          assertEquals(bufferSize, record.value.remaining)
          assertEquals(expectedBuffer, record.value)
        }
      }

      assertEquals(batches, countBatches)
      assertEquals(batches, countRecords)
    }
  }

  @Test
  def testBatchWriteReadSnapshot(): Unit = {
    val tempDir = TestUtils.tempDir().toPath
    val offsetAndEpoch = new OffsetAndEpoch(10L, 3)
    val bufferSize = 256
    val batchSize = 3
    val batches = 10

    TestUtils.resource(KafkaSnapshotWriter(tempDir, offsetAndEpoch)) { snapshot =>
      for (_ <- 0 until batches) {
        val buffers = for {
          _ <- 0 until batchSize
        } yield ByteBuffer.wrap(Random.nextBytes(bufferSize))

        val records = buildRecords(buffers.toArray)
        snapshot.append(records)
      }

      snapshot.freeze()
    }

    TestUtils.resource(KafkaSnapshotReader(tempDir, offsetAndEpoch)) { snapshot =>
      var countBatches = 0
      var countRecords = 0
      snapshot.forEach { batch =>
        countBatches += 1

        batch.streamingIterator(new GrowableBufferSupplier()).forEachRemaining { record =>
          countRecords += 1
          assertFalse(record.hasKey)
          assertTrue(record.hasValue)
          assertEquals(bufferSize, record.value.remaining)
        }
      }

      assertEquals(batches, countBatches)
      assertEquals(batches * batchSize, countRecords)
    }
  }

  @Test
  def testAbortedSnapshot(): Unit = {
    val tempDir = TestUtils.tempDir().toPath
    val offsetAndEpoch = new OffsetAndEpoch(20L, 2)
    val bufferSize = 256
    val batches = 10

    TestUtils.resource(KafkaSnapshotWriter(tempDir, offsetAndEpoch)) { snapshot =>
      val records = buildRecords(Array(ByteBuffer.wrap(Random.nextBytes(bufferSize))))
      for (_ <- 0 until batches) {
        snapshot.append(records)
      }
    }

    // File should not exist since freeze was not called before
    assertFalse(Files.exists(snapshotPath(tempDir, offsetAndEpoch)))
    assertEquals(0, Files.list(snapshotDir(tempDir)).count())
  }

  @Test
  def testAppendToFrozenSnapshot(): Unit = {
    val tempDir = TestUtils.tempDir().toPath
    val offsetAndEpoch = new OffsetAndEpoch(10L, 3)
    val bufferSize = 256
    val batches = 10

    TestUtils.resource(KafkaSnapshotWriter(tempDir, offsetAndEpoch)) { snapshot =>
      val records = buildRecords(Array(ByteBuffer.wrap(Random.nextBytes(bufferSize))))
      for (_ <- 0 until batches) {
        snapshot.append(records)
      }

      snapshot.freeze()

      assertThrows(classOf[RuntimeException], () => snapshot.append(records))
    }

    // File should exist and the size should be greater than the sum of all the buffers
    assertTrue(Files.exists(snapshotPath(tempDir, offsetAndEpoch)))
    assertTrue(Files.size(snapshotPath(tempDir, offsetAndEpoch)) > bufferSize * batches)
  }

  @Test
  def testCreateSnapshotWithSameId(): Unit = {
    val tempDir = TestUtils.tempDir().toPath
    val offsetAndEpoch = new OffsetAndEpoch(20L, 2)
    val bufferSize = 256
    val batches = 1

    TestUtils.resource(KafkaSnapshotWriter(tempDir, offsetAndEpoch)) { snapshot =>
      val records = buildRecords(Array(ByteBuffer.wrap(Random.nextBytes(bufferSize))))
      for (_ <- 0 until batches) {
        snapshot.append(records)
      }

      snapshot.freeze()
    }

    // Create another snapshot with the same id
    TestUtils.resource(KafkaSnapshotWriter(tempDir, offsetAndEpoch)) { snapshot =>
      val records = buildRecords(Array(ByteBuffer.wrap(Random.nextBytes(bufferSize))))
      for (_ <- 0 until batches) {
        snapshot.append(records)
      }

      snapshot.freeze()
    }
  }
}

object KafkaSnapshotTest {
  def buildRecords(buffers: Array[ByteBuffer]): MemoryRecords = {
    MemoryRecords.withRecords(
      CompressionType.NONE,
      buffers.map(new SimpleRecord(_)): _*
    )
  }
}
