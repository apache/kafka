/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server.metadata

import java.nio.ByteBuffer
import java.util.Optional
import java.util.concurrent.{CompletableFuture, CountDownLatch}

import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.record.{CompressionType, MemoryRecords}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataImageTest}
import org.apache.kafka.metadata.MetadataRecordSerde
import org.apache.kafka.queue.EventQueue
import org.apache.kafka.raft.OffsetAndEpoch
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.snapshot.{MockRawSnapshotWriter, RecordsSnapshotWriter, SnapshotWriter}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test


class BrokerMetadataSnapshotterTest {
  @Test
  def testCreateAndClose(): Unit = {
    val snapshotter = new BrokerMetadataSnapshotter(0, Time.SYSTEM, None,
      (_, _, _) => throw new RuntimeException("unimplemented"))
    snapshotter.close()
  }

  class MockSnapshotWriterBuilder extends SnapshotWriterBuilder {
    var image = new CompletableFuture[MetadataImage]

    override def build(committedOffset: Long,
                       committedEpoch: Int,
                       lastContainedLogTime: Long): SnapshotWriter[ApiMessageAndVersion] = {
      val offsetAndEpoch = new OffsetAndEpoch(committedOffset, committedEpoch)
      RecordsSnapshotWriter.createWithHeader(
        () => {
          Optional.of(
            new MockRawSnapshotWriter(offsetAndEpoch, consumeSnapshotBuffer(committedOffset, committedEpoch))
          )
        },
        1024,
        MemoryPool.NONE,
        Time.SYSTEM,
        lastContainedLogTime,
        CompressionType.NONE,
        MetadataRecordSerde.INSTANCE
      ).get();
    }

    def consumeSnapshotBuffer(committedOffset: Long, committedEpoch: Int)(buffer: ByteBuffer): Unit = {
      val delta = new MetadataDelta(MetadataImage.EMPTY)
      val memoryRecords = MemoryRecords.readableRecords(buffer)
      val batchIterator = memoryRecords.batchIterator()
      while (batchIterator.hasNext) {
        val batch = batchIterator.next()
        if (!batch.isControlBatch()) {
          batch.forEach(record => {
            val recordBuffer = record.value().duplicate()
            val messageAndVersion = MetadataRecordSerde.INSTANCE.read(
              new ByteBufferAccessor(recordBuffer), recordBuffer.remaining())
            delta.replay(committedOffset, committedEpoch, messageAndVersion.message())
          })
        }
      }
      image.complete(delta.apply())
    }
  }

  class BlockingEvent extends EventQueue.Event {
    val latch = new CountDownLatch(1)
    override def run(): Unit = latch.await()
  }

  @Test
  def testCreateSnapshot(): Unit = {
    val writerBuilder = new MockSnapshotWriterBuilder()
    val snapshotter = new BrokerMetadataSnapshotter(0, Time.SYSTEM, None, writerBuilder)
    try {
      val blockingEvent = new BlockingEvent()
      snapshotter.eventQueue.append(blockingEvent)
      assertTrue(snapshotter.maybeStartSnapshot(10000L, MetadataImageTest.IMAGE1))
      assertFalse(snapshotter.maybeStartSnapshot(11000L, MetadataImageTest.IMAGE2))
      blockingEvent.latch.countDown()
      assertEquals(MetadataImageTest.IMAGE1, writerBuilder.image.get())
    } finally {
      snapshotter.close()
    }
  }
}
