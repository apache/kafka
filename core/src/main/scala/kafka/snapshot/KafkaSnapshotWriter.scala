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

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import kafka.utils.Logging
import org.apache.kafka.common.record.FileRecords
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.raft.OffsetAndEpoch
import org.apache.kafka.snapshot.SnapshotWriter

final class KafkaSnapshotWriter private (fileRecords: FileRecords, snapshotId: OffsetAndEpoch) extends SnapshotWriter with Logging {
  private[this] var frozen = false

  def snapshotId(): OffsetAndEpoch = {
    snapshotId
  }

  def append(records: MemoryRecords): Int = {
    if (frozen) {
      // TODO: Do we need a new expection?
      throw new RuntimeException(s"Append not supported. Snapshot is already frozen: id = $snapshotId; path = ${fileRecords.file}")
    }

    fileRecords.append(records)
  }

  def freeze(): Unit = {
    fileRecords.close()
    frozen = true

    val source = fileRecords.file.toPath
    val destination = moveRename(source, snapshotId)

    // Set readonly and ignore the result
    if (!fileRecords.file.setReadOnly()) {
      info(s"Unable to change permission to readonly for snapshot file '${fileRecords.file}'")
    }

    Files.move(source, destination, StandardCopyOption.ATOMIC_MOVE)
  }

  def close(): Unit = {
    // If it exist then it means that freeze was not called. Otherwise, this is a noop.
    fileRecords.deleteIfExists()
  }
}

object KafkaSnapshotWriter {
  private[this] val PartialSuffix = ".part"

  def apply(logDir: Path, snapshotId: OffsetAndEpoch): KafkaSnapshotWriter = {
    val fileRecords = FileRecords.open(
      createTempFile(logDir, snapshotId).toFile,
      true, // mutable
      true, // fileAlreadyExists
      0, // initFileSize
      false // preallocate
    )

    new KafkaSnapshotWriter(fileRecords, snapshotId)
  }

  private def createTempFile(logDir: Path, snapshotId: OffsetAndEpoch): Path = {
    val dir = snapshotDir(logDir)

    // Create the snapshot directory if it doesn't exists
    Files.createDirectories(dir)

    Files.createTempFile(dir, filenameFromSnapshotId(snapshotId), PartialSuffix)
  }
}
