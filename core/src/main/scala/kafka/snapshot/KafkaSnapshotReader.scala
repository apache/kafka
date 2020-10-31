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
import java.nio.file.Path
import java.util.{Iterator => JIterator}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.FileRecords
import org.apache.kafka.raft.OffsetAndEpoch
import org.apache.kafka.snapshot.SnapshotReader

final class KafkaSnapshotReader private (fileRecords: FileRecords, snapshotId: OffsetAndEpoch) extends SnapshotReader {
  def snapshotId(): OffsetAndEpoch = {
    snapshotId
  }

  def sizeInBytes(): Long = {
    fileRecords.sizeInBytes()
  }

  def iterator(): JIterator[RecordBatch] = {
    new JIterator[RecordBatch] {
      private[this] val iterator = fileRecords.batchIterator()

      override def hasNext(): Boolean = {
        iterator.hasNext()
      }

      override def next(): RecordBatch = {
        iterator.next()
      }
    }
  }

  def read(buffer: ByteBuffer, position: Long): Int =  {
    fileRecords.channel.read(buffer, position)
  }

  def close(): Unit = {
    fileRecords.close()
  }
}

object KafkaSnapshotReader {
  def apply(logDir: Path, snapshotId: OffsetAndEpoch): KafkaSnapshotReader = {
    val fileRecords = FileRecords.open(
      snapshotPath(logDir, snapshotId).toFile,
      false, // mutable
      true, // fileAlreadyExists
      0, // initFileSize
      false // preallocate
    )

    new KafkaSnapshotReader(fileRecords, snapshotId)
  }
}
