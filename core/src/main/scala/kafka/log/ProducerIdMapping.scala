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
package kafka.log

import java.io._
import java.nio.ByteBuffer
import java.nio.file.Files

import kafka.common.KafkaException
import kafka.utils.{Logging, nonthreadsafe}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{DuplicateSequenceNumberException, OutOfOrderSequenceException, ProducerFencedException}
import org.apache.kafka.common.protocol.types._
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.utils.{ByteUtils, Crc32C}

import scala.collection.{immutable, mutable}

private[log] object ProducerIdEntry {
  val Empty = ProducerIdEntry(RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
    -1, 0, RecordBatch.NO_TIMESTAMP)
}

private[log] case class ProducerIdEntry(epoch: Short, lastSeq: Int, lastOffset: Long, offsetDelta: Int, timestamp: Long) {
  def firstSeq: Int = lastSeq - offsetDelta
  def firstOffset: Long = lastOffset - offsetDelta

  def isDuplicate(batch: RecordBatch): Boolean = {
    batch.producerEpoch == epoch &&
      batch.baseSequence == firstSeq &&
      batch.lastSequence == lastSeq
  }
}

private[log] class ProducerAppendInfo(val pid: Long, initialEntry: ProducerIdEntry) {
  // the initialEntry here is the last successful appended batch. we validate incoming entries transitively, starting
  // with the last appended entry.
  private var epoch = initialEntry.epoch
  private var firstSeq = initialEntry.firstSeq
  private var lastSeq = initialEntry.lastSeq
  private var lastOffset = initialEntry.lastOffset
  private var maxTimestamp = initialEntry.timestamp

  private def validateAppend(epoch: Short, firstSeq: Int, lastSeq: Int) = {
    if (this.epoch > epoch) {
      throw new ProducerFencedException(s"Producer's epoch is no longer valid. There is probably another producer with a newer epoch. $epoch (request epoch), ${this.epoch} (server epoch)")
    } else if (this.epoch == RecordBatch.NO_PRODUCER_EPOCH || this.epoch < epoch) {
      if (firstSeq != 0)
        throw new OutOfOrderSequenceException(s"Invalid sequence number for new epoch: $epoch " +
          s"(request epoch), $firstSeq (seq. number)")
    } else if (firstSeq == this.firstSeq && lastSeq == this.lastSeq) {
      throw new DuplicateSequenceNumberException(s"Duplicate sequence number: $pid (pid), $firstSeq " +
        s"(seq. number), ${this.firstSeq} (expected seq. number)")
    } else if (firstSeq != this.lastSeq + 1L) {
      throw new OutOfOrderSequenceException(s"Invalid sequence number: $pid (pid), $firstSeq " +
        s"(seq. number), ${this.lastSeq} (expected seq. number)")
    }
  }

  def assignLastOffsetAndTimestamp(lastOffset: Long, lastTimestamp: Long): Unit = {
    this.lastOffset = lastOffset
    this.maxTimestamp = lastTimestamp
  }

  private def append(epoch: Short, firstSeq: Int, lastSeq: Int, lastTimestamp: Long, lastOffset: Long) {
    validateAppend(epoch, firstSeq, lastSeq)
    this.epoch = epoch
    this.firstSeq = firstSeq
    this.lastSeq = lastSeq
    this.maxTimestamp = lastTimestamp
    this.lastOffset = lastOffset
  }

  def append(batch: RecordBatch): Unit =
    append(batch.producerEpoch, batch.baseSequence, batch.lastSequence, batch.maxTimestamp, batch.lastOffset)

  def append(entry: ProducerIdEntry): Unit =
    append(entry.epoch, entry.firstSeq, entry.lastSeq, entry.timestamp, entry.lastOffset)

  def lastEntry: ProducerIdEntry =
    ProducerIdEntry(epoch, lastSeq, lastOffset, lastSeq - firstSeq, maxTimestamp)
}

private[log] class CorruptSnapshotException(msg: String) extends KafkaException(msg)

object ProducerIdMapping {
  private val PidSnapshotVersion: Short = 1
  private val VersionField = "version"
  private val CrcField = "crc"
  private val PidField = "pid"
  private val LastSequenceField = "last_sequence"
  private val EpochField = "epoch"
  private val LastOffsetField = "last_offset"
  private val OffsetDeltaField = "offset_delta"
  private val TimestampField = "timestamp"
  private val PidEntriesField = "pid_entries"

  private val VersionOffset = 0
  private val CrcOffset = VersionOffset + 2
  private val PidEntriesOffset = CrcOffset + 4

  private val maxPidSnapshotsToRetain = 2

  val PidSnapshotEntrySchema = new Schema(
    new Field(PidField, Type.INT64, "The producer ID"),
    new Field(EpochField, Type.INT16, "Current epoch of the producer"),
    new Field(LastSequenceField, Type.INT32, "Last written sequence of the producer"),
    new Field(LastOffsetField, Type.INT64, "Last written offset of the producer"),
    new Field(OffsetDeltaField, Type.INT32, "The difference of the last sequence and first sequence in the last written batch"),
    new Field(TimestampField, Type.INT64, "Max timestamp from the last written entry"))
  val PidSnapshotMapSchema = new Schema(
    new Field(VersionField, Type.INT16, "Version of the snapshot file"),
    new Field(CrcField, Type.UNSIGNED_INT32, "CRC of the snapshot data"),
    new Field(PidEntriesField, new ArrayOf(PidSnapshotEntrySchema), "The entries in the PID table"))

  private def readSnapshot(file: File): Iterable[(Long, ProducerIdEntry)] = {
    val buffer = Files.readAllBytes(file.toPath)
    val struct = PidSnapshotMapSchema.read(ByteBuffer.wrap(buffer))

    val version = struct.getShort(VersionField)
    if (version != PidSnapshotVersion)
      throw new IllegalArgumentException(s"Unhandled snapshot file version $version")

    val crc = struct.getUnsignedInt(CrcField)
    val computedCrc =  Crc32C.compute(buffer, PidEntriesOffset, buffer.length - PidEntriesOffset)
    if (crc != computedCrc)
      throw new CorruptSnapshotException(s"Snapshot file is corrupted (CRC is no longer valid). Stored crc: ${crc}. Computed crc: ${computedCrc}")

    struct.getArray(PidEntriesField).map { pidEntryObj =>
      val pidEntryStruct = pidEntryObj.asInstanceOf[Struct]
      val pid: Long = pidEntryStruct.getLong(PidField)
      val epoch = pidEntryStruct.getShort(EpochField)
      val seq = pidEntryStruct.getInt(LastSequenceField)
      val offset = pidEntryStruct.getLong(LastOffsetField)
      val timestamp = pidEntryStruct.getLong(TimestampField)
      val offsetDelta = pidEntryStruct.getInt(OffsetDeltaField)
      val newEntry = ProducerIdEntry(epoch, seq, offset, offsetDelta, timestamp)
      pid -> newEntry
    }
  }

  private def writeSnapshot(file: File, entries: mutable.Map[Long, ProducerIdEntry]) {
    val struct = new Struct(PidSnapshotMapSchema)
    struct.set(VersionField, PidSnapshotVersion)
    struct.set(CrcField, 0L) // we'll fill this after writing the entries
    val entriesArray = entries.map {
      case (pid, entry) =>
        val pidEntryStruct = struct.instance(PidEntriesField)
        pidEntryStruct.set(PidField, pid)
          .set(EpochField, entry.epoch)
          .set(LastSequenceField, entry.lastSeq)
          .set(LastOffsetField, entry.lastOffset)
          .set(OffsetDeltaField, entry.offsetDelta)
          .set(TimestampField, entry.timestamp)
        pidEntryStruct
    }.toArray
    struct.set(PidEntriesField, entriesArray)

    val buffer = ByteBuffer.allocate(struct.sizeOf)
    struct.writeTo(buffer)
    buffer.flip()

    // now fill in the CRC
    val crc = Crc32C.compute(buffer, PidEntriesOffset, buffer.limit - PidEntriesOffset)
    ByteUtils.writeUnsignedInt(buffer, CrcOffset, crc)

    val fos = new FileOutputStream(file)
    try {
      fos.write(buffer.array, buffer.arrayOffset, buffer.limit)
    } finally {
      fos.close()
    }
  }

  private def isSnapshotFile(name: String): Boolean = name.endsWith(Log.PidSnapshotFileSuffix)

}

/**
 * Maintains a mapping from ProducerIds (PIDs) to metadata about the last appended entries (e.g.
 * epoch, sequence number, last offset, etc.)
 *
 * The sequence number is the last number successfully appended to the partition for the given identifier.
 * The epoch is used for fencing against zombie writers. The offset is the one of the last successful message
 * appended to the partition.
 *
 * As long as a PID is contained in the map, the corresponding producer can continue to write data.
 * However, PIDs can be expired due to lack of recent use or if the last written entry has been deleted from
 * the log (e.g. if the retention policy is "delete"). For compacted topics, the log cleaner will ensure
 * that the most recent entry from a given PID is retained in the log provided it hasn't expired due to
 * age. This ensures that PIDs will not be expired until either the max expiration time has been reached,
 * or if the topic also is configured for deletion, the segment containing the last written offset has
 * been deleted.
 */
@nonthreadsafe
class ProducerIdMapping(val config: LogConfig,
                        val topicPartition: TopicPartition,
                        val logDir: File,
                        val maxPidExpirationMs: Int) extends Logging {
  import ProducerIdMapping._

  private val pidMap = mutable.Map[Long, ProducerIdEntry]()
  private var lastMapOffset = 0L
  private var lastSnapOffset = 0L

  /**
   * Returns the last offset of this map
   */
  def mapEndOffset = lastMapOffset

  /**
   * Get a copy of the active producers
   */
  def activePids: immutable.Map[Long, ProducerIdEntry] = pidMap.toMap

  private def loadFromSnapshot(logStartOffset: Long, currentTime: Long) {
    pidMap.clear()

    while (true) {
      latestSnapshotFile match {
        case Some(file) =>
          try {
            info(s"Loading PID mapping from snapshot file ${file.getName} for partition $topicPartition")
            readSnapshot(file).foreach { case (pid, entry) =>
              if (!isExpired(currentTime, entry))
                pidMap.put(pid, entry)
            }

            lastSnapOffset = Log.offsetFromFilename(file.getName)
            lastMapOffset = lastSnapOffset
            return
          } catch {
            case e: CorruptSnapshotException =>
              error(s"Snapshot file at ${file.getPath} is corrupt: ${e.getMessage}")
              Files.deleteIfExists(file.toPath)
          }
        case None =>
          lastSnapOffset = logStartOffset
          lastMapOffset = logStartOffset
          return
      }
    }
  }

  private def isExpired(currentTimeMs: Long, producerIdEntry: ProducerIdEntry) : Boolean =
    currentTimeMs - producerIdEntry.timestamp >= maxPidExpirationMs


  def removeExpiredPids(currentTimeMs: Long) {
    pidMap.retain { case (pid, lastEntry) =>
      !isExpired(currentTimeMs, lastEntry)
    }
  }

  /**
   * Truncate the PID mapping to the given offset range and reload the entries from the most recent
   * snapshot in range (if there is one).
   */
  def truncateAndReload(logStartOffset: Long, logEndOffset: Long, currentTimeMs: Long) {
    if (logEndOffset != mapEndOffset) {
      deleteSnapshotFiles { file =>
        val offset = Log.offsetFromFilename(file.getName)
        offset > logEndOffset || offset <= logStartOffset
      }
      loadFromSnapshot(logStartOffset, currentTimeMs)
    } else {
      expirePids(logStartOffset)
    }
  }

  /**
   * Update the mapping with the given append information
   */
  def update(appendInfo: ProducerAppendInfo): Unit = {
    if (appendInfo.pid == RecordBatch.NO_PRODUCER_ID)
      throw new IllegalArgumentException("Invalid PID passed to update")
    val entry = appendInfo.lastEntry
    pidMap.put(appendInfo.pid, entry)
  }

  def updateMapEndOffset(lastOffset: Long): Unit = {
    lastMapOffset = lastOffset
  }

  /**
   * Load a previously stored PID entry into the cache. Ignore the entry if the timestamp is older
   * than the current time minus the PID expiration time (i.e. if the PID has expired).
   */
  def load(pid: Long, entry: ProducerIdEntry, currentTimeMs: Long) {
    if (pid != RecordBatch.NO_PRODUCER_ID && !isExpired(currentTimeMs, entry))
      pidMap.put(pid, entry)
  }

  /**
   * Get the last written entry for the given PID.
   */
  def lastEntry(pid: Long): Option[ProducerIdEntry] = pidMap.get(pid)

  /**
   * Write a new snapshot if there have been updates since the last one.
   */
  def maybeTakeSnapshot() {
    // If not a new offset, then it is not worth taking another snapshot
    if (lastMapOffset > lastSnapOffset) {
      val snapshotFile = Log.pidSnapshotFilename(logDir, lastMapOffset)
      debug(s"Writing producer snapshot for partition $topicPartition at offset $lastMapOffset")
      writeSnapshot(snapshotFile, pidMap)

      // Update the last snap offset according to the serialized map
      lastSnapOffset = lastMapOffset

      maybeRemoveOldestSnapshot()
    }
  }

  /**
   * Get the last offset (exclusive) of the latest snapshot file.
   */
  def latestSnapshotOffset: Option[Long] = latestSnapshotFile.map(file => Log.offsetFromFilename(file.getName))

  /**
   * When we remove the head of the log due to retention, we need to clean up the id map. This method takes
   * the new start offset and expires all pids which have a smaller last written offset.
   */
  def expirePids(logStartOffset: Long) {
    pidMap.retain((pid, entry) => entry.lastOffset >= logStartOffset)
    deleteSnapshotFiles(file => Log.offsetFromFilename(file.getName) <= logStartOffset)
    if (lastMapOffset < logStartOffset)
      lastMapOffset = logStartOffset
    lastSnapOffset = latestSnapshotOffset.getOrElse(logStartOffset)
  }

  /**
   * Truncate the PID mapping and remove all snapshots. This resets the state of the mapping.
   */
  def truncate() {
    pidMap.clear()
    deleteSnapshotFiles()
    lastSnapOffset = 0L
    lastMapOffset = 0L
  }

  private def maybeRemoveOldestSnapshot() {
    val list = listSnapshotFiles
    if (list.size > maxPidSnapshotsToRetain) {
      val toDelete = list.minBy(file => Log.offsetFromFilename(file.getName))
      Files.deleteIfExists(toDelete.toPath)
    }
  }

  private def listSnapshotFiles: List[File] = {
    if (logDir.exists && logDir.isDirectory)
      logDir.listFiles.filter(f => f.isFile && isSnapshotFile(f.getName)).toList
    else
      List.empty[File]
  }

  private def latestSnapshotFile: Option[File] = {
    val files = listSnapshotFiles
    if (files.nonEmpty)
      Some(files.maxBy(file => Log.offsetFromFilename(file.getName)))
    else
      None
  }

  private def deleteSnapshotFiles(predicate: File => Boolean = _ => true) {
    listSnapshotFiles.filter(predicate).foreach(file => Files.deleteIfExists(file.toPath))
  }

}
