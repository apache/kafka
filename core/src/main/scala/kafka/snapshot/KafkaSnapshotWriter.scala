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
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import kafka.utils.Logging
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.raft.OffsetAndEpoch
import org.apache.kafka.snapshot.SnapshotWriter

final class KafkaSnapshotWriter(
  path: Path,
  channel: FileChannel,
  snapshotId: OffsetAndEpoch
) extends SnapshotWriter with Logging {
  private[this] var frozen = false

  override def snapshotId(): OffsetAndEpoch = {
    snapshotId
  }

  override def sizeInBytes(): Long = {
    channel.size()
  }

  override def append(records: MemoryRecords): Int = {
    if (frozen) {
      throw new IllegalStateException(s"Append not supported. Snapshot is already frozen: id = $snapshotId; path = $path")
    }

    records.writeFullyTo(channel)
  }

  override def append(buffer: ByteBuffer): Unit = {
    if (frozen) {
      throw new IllegalStateException(s"Append not supported. Snapshot is already frozen: id = $snapshotId; path = $path")
    }

    Utils.writeFully(channel, buffer)
  }

  override def isFrozen(): Boolean = {
    frozen
  }

  override def freeze(): Unit = {
    channel.close()
    frozen = true

    // Set readonly and ignore the result
    if (!path.toFile.setReadOnly()) {
      info(s"Unable to change permission to readonly for internal snapshot file '$path'")
    }

    val destination = moveRename(path, snapshotId)
    Files.move(path, destination, StandardCopyOption.ATOMIC_MOVE)
  }

  override def close(): Unit = {
    channel.close()
    Files.deleteIfExists(path)
  }
}

object KafkaSnapshotWriter {
  def apply(logDir: Path, snapshotId: OffsetAndEpoch): KafkaSnapshotWriter = {
    val path = createTempFile(logDir, snapshotId)

    new KafkaSnapshotWriter(
      path,
      FileChannel.open(path, Utils.mkSet(StandardOpenOption.WRITE, StandardOpenOption.APPEND)),
      snapshotId
    )
  }
}
