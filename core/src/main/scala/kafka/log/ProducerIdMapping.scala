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
import org.apache.kafka.common.record.{ControlRecordType, InvalidRecordException, RecordBatch}
import org.apache.kafka.common.utils.{ByteUtils, Crc32C}

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}

private[log] object ProducerIdEntry {
  val Empty = ProducerIdEntry(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
    -1, 0, RecordBatch.NO_TIMESTAMP, -1)
}

private[log] case class ProducerIdEntry(pid: Long, epoch: Short, lastSeq: Int, lastOffset: Long, offsetDelta: Int,
                                        timestamp: Long, currentTxnFirstOffset: Long) {
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
  private var currentTxnFirstOffset = initialEntry.currentTxnFirstOffset
  private val startedTxns = ListBuffer.empty[OngoingTxn]

  def this(pid: Long, initialEntry: Option[ProducerIdEntry]) =
    this(pid, initialEntry.getOrElse(ProducerIdEntry.Empty))

  def this(pid: Long, initialBatch: RecordBatch) =
    this(pid, ProducerIdEntry(pid, initialBatch.producerEpoch, initialBatch.lastSequence, initialBatch.lastOffset,
      initialBatch.lastSequence - initialBatch.baseSequence, initialBatch.maxTimestamp,
      if (initialBatch.isTransactional) initialBatch.baseOffset else -1))

  private def validateAppend(epoch: Short, firstSeq: Int, lastSeq: Int) = {
    if (this.epoch > epoch) {
      throw new ProducerFencedException(s"Producer's epoch is no longer valid. There is probably another producer " +
        s"with a newer epoch. $epoch (request epoch), ${this.epoch} (server epoch)")
    } else if (this.epoch == RecordBatch.NO_PRODUCER_EPOCH || this.epoch < epoch) {
      if (firstSeq != 0)
        throw new OutOfOrderSequenceException(s"Invalid sequence number for new epoch: $epoch " +
          s"(request epoch), $firstSeq (seq. number)")
    } else if (firstSeq == this.firstSeq && lastSeq == this.lastSeq) {
      throw new DuplicateSequenceNumberException(s"Duplicate sequence number: pid: $pid, (incomingBatch.firstSeq, " +
        s"incomingBatch.lastSeq): ($firstSeq, $lastSeq), (lastEntry.firstSeq, lastEntry.lastSeq): " +
        s"(${this.firstSeq}, ${this.lastSeq}).")
    } else if (firstSeq != this.lastSeq + 1L) {
      throw new OutOfOrderSequenceException(s"Out of order sequence number: $pid (pid), $firstSeq " +
        s"(incoming seq. number), ${this.lastSeq} (current end sequence number)")
    }
  }

  def assignLastOffsetAndTimestamp(lastOffset: Long, lastTimestamp: Long): Unit = {
    if (startedTxns.nonEmpty) {
      startedTxns.clear()
      val firstOffset = lastOffset - (lastSeq - firstSeq)
      startedTxns += OngoingTxn(pid, firstOffset)
      if (currentTxnFirstOffset == 0)
        currentTxnFirstOffset = firstOffset
    }

    this.lastOffset = lastOffset
    this.maxTimestamp = lastTimestamp
  }

  def append(epoch: Short, firstSeq: Int, lastSeq: Int, lastTimestamp: Long, lastOffset: Long, isTransactional: Boolean) {
    validateAppend(epoch, firstSeq, lastSeq)
    this.epoch = epoch
    this.firstSeq = firstSeq
    this.lastSeq = lastSeq
    this.maxTimestamp = lastTimestamp
    this.lastOffset = lastOffset

    if (currentTxnFirstOffset >= 0 && !isTransactional)
      // FIXME: Do we need a new error code here?
      throw new InvalidRecordException(s"Expected transactional write from producer $pid")

    if (isTransactional && currentTxnFirstOffset < 0) {
      val firstOffset = lastOffset - (lastSeq - firstSeq)
      currentTxnFirstOffset = firstOffset
      startedTxns += OngoingTxn(pid, firstOffset)
    }
  }

  def append(batch: RecordBatch): Unit =
    append(batch.producerEpoch, batch.baseSequence, batch.lastSequence, batch.maxTimestamp, batch.lastOffset,
      batch.isTransactional)

  def appendControl(controlRecord: ControlRecord): CompletedTxn = {
    if (this.epoch > controlRecord.epoch)
      throw new ProducerFencedException(s"Invalid epoch (zombie writer): ${controlRecord.epoch} (request epoch), ${this.epoch}")

    controlRecord.controlType match {
      case ControlRecordType.ABORT | ControlRecordType.COMMIT =>
        val firstOffset = if (currentTxnFirstOffset >= 0)
          currentTxnFirstOffset
        else
          controlRecord.offset

        currentTxnFirstOffset = -1
        maxTimestamp = controlRecord.timestamp
        CompletedTxn(pid, firstOffset, lastOffset, controlRecord.controlType == ControlRecordType.ABORT)

      case unhandled => throw new IllegalArgumentException(s"Unhandled control type $unhandled")
    }
  }

  def lastEntry: ProducerIdEntry =
    ProducerIdEntry(pid, epoch, lastSeq, lastOffset, lastSeq - firstSeq, maxTimestamp, currentTxnFirstOffset)

  def startedTransactions: List[OngoingTxn] = startedTxns.toList
}

class CorruptSnapshotException(msg: String) extends KafkaException(msg)
private[log] case class OngoingTxn(pid: Long, firstOffset: Long) extends Ordered[OngoingTxn] {
  override def compare(that: OngoingTxn): Int = {
    val res = this.firstOffset compare that.firstOffset
    if (res == 0)
      this.pid compare that.pid
    else
      res
  }
}

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
  private val CurrentTxnFirstOffsetField = "current_txn_first_offset"

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
    new Field(TimestampField, Type.INT64, "Max timestamp from the last written entry"),
    new Field(CurrentTxnFirstOffsetField, Type.INT64, "The first offset of the on-going transaction (-1 if there is none)"))
  val PidSnapshotMapSchema = new Schema(
    new Field(VersionField, Type.INT16, "Version of the snapshot file"),
    new Field(CrcField, Type.UNSIGNED_INT32, "CRC of the snapshot data"),
    new Field(PidEntriesField, new ArrayOf(PidSnapshotEntrySchema), "The entries in the PID table"))

  def readSnapshot(file: File): Iterable[(Long, ProducerIdEntry)] = {
    val buffer = Files.readAllBytes(file.toPath)
    val struct = PidSnapshotMapSchema.read(ByteBuffer.wrap(buffer))

    val version = struct.getShort(VersionField)
    if (version != PidSnapshotVersion)
      throw new IllegalArgumentException(s"Unhandled snapshot file version $version")

    val crc = struct.getUnsignedInt(CrcField)
    val computedCrc =  Crc32C.compute(buffer, PidEntriesOffset, buffer.length - PidEntriesOffset)
    if (crc != computedCrc)
      throw new CorruptSnapshotException(s"Snapshot file '$file' is corrupted (CRC is no longer valid). " +
        s"Stored crc: $crc. Computed crc: $computedCrc")

    struct.getArray(PidEntriesField).map { pidEntryObj =>
      val pidEntryStruct = pidEntryObj.asInstanceOf[Struct]
      val pid: Long = pidEntryStruct.getLong(PidField)
      val epoch = pidEntryStruct.getShort(EpochField)
      val seq = pidEntryStruct.getInt(LastSequenceField)
      val offset = pidEntryStruct.getLong(LastOffsetField)
      val timestamp = pidEntryStruct.getLong(TimestampField)
      val offsetDelta = pidEntryStruct.getInt(OffsetDeltaField)
      val currentTxnFirstOffset = pidEntryStruct.getLong(CurrentTxnFirstOffsetField)
      val newEntry = ProducerIdEntry(pid, epoch, seq, offset, offsetDelta, timestamp, currentTxnFirstOffset)
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
          .set(CurrentTxnFirstOffsetField, -1L)
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

  // ongoing transactions sorted by the first offset of the transaction
  private val ongoingTransactions = mutable.TreeSet.empty[OngoingTxn]

  def firstUnstableOffset: Option[Long] = ongoingTransactions.headOption.map(_.firstOffset)

  def firstUnstableOffset(withoutTxn: CompletedTxn): Option[Long] =
    ongoingTransactions.find(txn => txn.firstOffset != withoutTxn.firstOffset).map(_.firstOffset)

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
    producerIdEntry.currentTxnFirstOffset < 0 && currentTimeMs - producerIdEntry.timestamp >= maxPidExpirationMs

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
    ongoingTransactions ++= appendInfo.startedTransactions
  }

  def updateMapEndOffset(lastOffset: Long): Unit = {
    lastMapOffset = lastOffset
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

  def completeTxn(completedTxn: CompletedTxn): Unit =
    ongoingTransactions.remove(OngoingTxn(completedTxn.pid, completedTxn.firstOffset))

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
