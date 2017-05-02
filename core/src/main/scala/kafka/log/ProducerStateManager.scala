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
    -1, 0, RecordBatch.NO_TIMESTAMP, None)
}

private[log] case class ProducerIdEntry(producerId: Long, epoch: Short, lastSeq: Int, lastOffset: Long, offsetDelta: Int,
                                        timestamp: Long, currentTxnFirstOffset: Option[Long]) {
  def firstSeq: Int = lastSeq - offsetDelta
  def firstOffset: Long = lastOffset - offsetDelta

  def isDuplicate(batch: RecordBatch): Boolean = {
    batch.producerEpoch == epoch &&
      batch.baseSequence == firstSeq &&
      batch.lastSequence == lastSeq
  }
}

private[log] class ProducerAppendInfo(val producerId: Long, initialEntry: ProducerIdEntry, loadingFromLog: Boolean = false) {
  // the initialEntry here is the last successful appended batch. we validate incoming entries transitively, starting
  // with the last appended entry.
  private var epoch = initialEntry.epoch
  private var firstSeq = initialEntry.firstSeq
  private var lastSeq = initialEntry.lastSeq
  private var lastOffset = initialEntry.lastOffset
  private var maxTimestamp = initialEntry.timestamp
  private var currentTxnFirstOffset = initialEntry.currentTxnFirstOffset

  def this(pid: Long, initialEntry: Option[ProducerIdEntry], loadingFromLog: Boolean) =
    this(pid, initialEntry.getOrElse(ProducerIdEntry.Empty), loadingFromLog)

  private def validateAppend(epoch: Short, firstSeq: Int, lastSeq: Int) = {
    if (this.epoch > epoch) {
      throw new ProducerFencedException(s"Producer's epoch is no longer valid. There is probably another producer " +
        s"with a newer epoch. $epoch (request epoch), ${this.epoch} (server epoch)")
    } else if (this.epoch == RecordBatch.NO_PRODUCER_EPOCH || this.epoch < epoch) {
      if (firstSeq != 0)
        throw new OutOfOrderSequenceException(s"Invalid sequence number for new epoch: $epoch " +
          s"(request epoch), $firstSeq (seq. number)")
    } else if (this.firstSeq == RecordBatch.NO_SEQUENCE && firstSeq != 0) {
      // the epoch was bumped by a control record, so we expect the sequence number to be reset
      throw new OutOfOrderSequenceException(s"Out of order sequence number: $producerId (pid), found $firstSeq " +
        s"(incoming seq. number), but expected 0")
    } else if (firstSeq == this.firstSeq && lastSeq == this.lastSeq) {
      throw new DuplicateSequenceNumberException(s"Duplicate sequence number: pid: $producerId, (incomingBatch.firstSeq, " +
        s"incomingBatch.lastSeq): ($firstSeq, $lastSeq), (lastEntry.firstSeq, lastEntry.lastSeq): " +
        s"(${this.firstSeq}, ${this.lastSeq}).")
    } else if (firstSeq != this.lastSeq + 1L) {
      throw new OutOfOrderSequenceException(s"Out of order sequence number: $producerId (pid), $firstSeq " +
        s"(incoming seq. number), ${this.lastSeq} (current end sequence number)")
    }
  }

  def assignTimestamp(lastTimestamp: Long): Unit = {
    this.maxTimestamp = lastTimestamp
  }

  def append(epoch: Short,
             firstSeq: Int,
             lastSeq: Int,
             lastTimestamp: Long,
             lastOffset: Long,
             isTransactional: Boolean): Unit = {
    if (epoch != RecordBatch.NO_PRODUCER_EPOCH && !loadingFromLog)
      // skip validation if this is the first entry when loading from the log. Log retention
      // will generally have removed the beginning entries from each PID
      validateAppend(epoch, firstSeq, lastSeq)

    this.epoch = epoch
    this.firstSeq = firstSeq
    this.lastSeq = lastSeq
    this.maxTimestamp = lastTimestamp
    this.lastOffset = lastOffset

    if (currentTxnFirstOffset.isDefined && !isTransactional)
      // FIXME: Do we need a new error code here?
      throw new InvalidRecordException(s"Expected transactional write from producer $producerId")

    if (isTransactional && currentTxnFirstOffset.isEmpty) {
      val firstOffset = lastOffset - (lastSeq - firstSeq)
      currentTxnFirstOffset = Some(firstOffset)
    }
  }

  def appendControlRecord(controlType: ControlRecordType,
                          producerEpoch: Short,
                          offset: Long,
                          timestamp: Long): CompletedTxn = {
    if (this.epoch > producerEpoch)
      throw new ProducerFencedException(s"Invalid epoch (zombie writer): $producerEpoch (request epoch), ${this.epoch}")

    if (producerEpoch > this.epoch) {
      // it is possible that this control record is the first record seen from a new epoch (the producer
      // may fail before sending to the partition or the request itself could fail for some reason). In this
      // case, we bump the epoch and reset the sequence numbers
      this.epoch = producerEpoch
      this.firstSeq = RecordBatch.NO_SEQUENCE
      this.lastSeq = RecordBatch.NO_SEQUENCE
    } else {
      // the control record is the last append to the log, so the last offset will be updated to point to it.
      // However, the sequence numbers still point to the previous batch, so the duplicate check would no longer
      // be correct: it would return the wrong offset. To fix this, we treat the control record as a batch
      // of size 1 which uses the last appended sequence number.
      this.firstSeq = this.lastSeq
    }

    controlType match {
      case ControlRecordType.ABORT | ControlRecordType.COMMIT =>
        val firstOffset = currentTxnFirstOffset.getOrElse(offset)
        this.lastOffset = offset
        this.currentTxnFirstOffset = None
        this.maxTimestamp = timestamp
        CompletedTxn(producerId, firstOffset, offset, controlType == ControlRecordType.ABORT)

      case unhandled => throw new IllegalArgumentException(s"Unhandled control type $unhandled")
    }
  }

  def lastEntry: ProducerIdEntry =
    ProducerIdEntry(producerId, epoch, lastSeq, lastOffset, lastSeq - firstSeq, maxTimestamp, currentTxnFirstOffset)

  def startedTransaction: Option[OngoingTxn] = currentTxnFirstOffset.map { offset =>
    OngoingTxn(producerId, offset)
  }

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

object ProducerStateManager {
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
      val newEntry = ProducerIdEntry(pid, epoch, seq, offset, offsetDelta, timestamp,
        if (currentTxnFirstOffset >= 0) Some(currentTxnFirstOffset) else None)
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
          .set(CurrentTxnFirstOffsetField, entry.currentTxnFirstOffset.getOrElse(-1L))
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
class ProducerStateManager(val config: LogConfig,
                           val topicPartition: TopicPartition,
                           val logDir: File,
                           val maxPidExpirationMs: Int) extends Logging {
  import ProducerStateManager._

  private val producers = mutable.Map.empty[Long, ProducerIdEntry]
  private var lastMapOffset = 0L
  private var lastSnapOffset = 0L

  // ongoing transactions sorted by the first offset of the transaction
  private val ongoingTxns = mutable.TreeSet.empty[OngoingTxn]

  // completed transactions whose markers are at offsets above the high watermark
  private val unreplicatedTxns = mutable.TreeSet.empty[CompletedTxn]

  /**
   * An unstable offset is one which is either undecided (i.e. its ultimate outcome is not yet known),
   * or one that is decided, but may not have been replicated (i.e. any transaction which has a COMMIT/ABORT
   * marker written at a higher offset than the current high watermark).
   */
  def firstUnstableOffset: Option[Long] = {
    val firstUnackedOffset = unreplicatedTxns.headOption.map(_.firstOffset)
    firstUndecidedOffset.map { offset =>
      math.min(offset, firstUnackedOffset.getOrElse(Long.MaxValue))
    }.orElse(firstUnackedOffset)
  }

  /**
   * Acknowledge all transactions which have been completed before a given offset. This allows the LSO
   * to advance to the next unstable offset.
   */
  def onHighWatermarkUpdated(highWatermark: Long): Unit = unreplicatedTxns.retain(_.lastOffset >= highWatermark)

  /**
   * The first undecided offset is the earliest transactional message which has not yet been committed
   * or aborted.
   */
  def firstUndecidedOffset: Option[Long] = ongoingTxns.headOption.map(_.firstOffset)

  /**
   * Get the next first undecided offset once the given transaction is completed.
   */
  def firstUndecidedOffsetExcluding(txn: CompletedTxn): Option[Long] =
    ongoingTxns.find(ongoingTxn => txn.firstOffset != ongoingTxn.firstOffset).map(_.firstOffset)

  /**
   * Returns the last offset of this map
   */
  def mapEndOffset = lastMapOffset

  /**
   * Get a copy of the active producers
   */
  def activePids: immutable.Map[Long, ProducerIdEntry] = producers.toMap

  private def loadFromSnapshot(logStartOffset: Long, currentTime: Long) {
    while (true) {
      latestSnapshotFile match {
        case Some(file) =>
          try {
            info(s"Loading PID mapping from snapshot file ${file.getName} for partition $topicPartition")
            readSnapshot(file).foreach { case (pid, entry) =>
              if (!isExpired(currentTime, entry)) {
                producers.put(pid, entry)
                entry.currentTxnFirstOffset.foreach { offset =>
                  ongoingTxns += OngoingTxn(pid, offset)
                }
              }
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

  private def isExpired(currentTimeMs: Long, producerIdEntry: ProducerIdEntry): Boolean =
    producerIdEntry.currentTxnFirstOffset.isEmpty && currentTimeMs - producerIdEntry.timestamp >= maxPidExpirationMs

  /**
   * Expire any PIDs which have been idle longer than the configured maximum expiration timeout.
   */
  def removeExpiredProducers(currentTimeMs: Long) {
    producers.retain { case (pid, lastEntry) =>
      !isExpired(currentTimeMs, lastEntry)
    }
  }

  /**
   * Truncate the PID mapping to the given offset range and reload the entries from the most recent
   * snapshot in range (if there is one).
   */
  def truncateAndReload(logStartOffset: Long, logEndOffset: Long, currentTimeMs: Long) {
    if (logEndOffset != mapEndOffset) {
      producers.clear()
      ongoingTxns.clear()
      unreplicatedTxns.clear()
      deleteSnapshotFiles { file =>
        val offset = Log.offsetFromFilename(file.getName)
        offset > logEndOffset || offset <= logStartOffset
      }
      loadFromSnapshot(logStartOffset, currentTimeMs)
    } else {
      evictUnretainedPids(logStartOffset)
    }
  }

  /**
   * Update the mapping with the given append information
   */
  def update(appendInfo: ProducerAppendInfo): Unit = {
    if (appendInfo.producerId == RecordBatch.NO_PRODUCER_ID)
      throw new IllegalArgumentException("Invalid PID passed to update")
    val entry = appendInfo.lastEntry
    producers.put(appendInfo.producerId, entry)
    appendInfo.startedTransaction.foreach(ongoingTxns += _)
  }

  def updateMapEndOffset(lastOffset: Long): Unit = {
    lastMapOffset = lastOffset
  }

  /**
   * Get the last written entry for the given PID.
   */
  def lastEntry(producerId: Long): Option[ProducerIdEntry] = producers.get(producerId)

  /**
   * Write a new snapshot if there have been updates since the last one.
   */
  def maybeTakeSnapshot() {
    // If not a new offset, then it is not worth taking another snapshot
    if (lastMapOffset > lastSnapOffset) {
      val snapshotFile = Log.pidSnapshotFilename(logDir, lastMapOffset)
      debug(s"Writing producer snapshot for partition $topicPartition at offset $lastMapOffset")
      writeSnapshot(snapshotFile, producers)

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
   * the new start offset and removes all pids which have a smaller last written offset.
   */
  def evictUnretainedPids(logStartOffset: Long) {
    val evictedPidEntries = producers.filter(_._2.lastOffset < logStartOffset)
    val evictedPids = evictedPidEntries.keySet

    ongoingTxns.retain(txn => !evictedPids.contains(txn.pid))
    unreplicatedTxns.retain(txn => !evictedPids.contains(txn.producerId))
    producers --= evictedPids

    deleteSnapshotFiles(file => Log.offsetFromFilename(file.getName) <= logStartOffset)
    if (lastMapOffset < logStartOffset)
      lastMapOffset = logStartOffset
    lastSnapOffset = latestSnapshotOffset.getOrElse(logStartOffset)
  }

  /**
   * Truncate the PID mapping and remove all snapshots. This resets the state of the mapping.
   */
  def truncate() {
    producers.clear()
    ongoingTxns.clear()
    unreplicatedTxns.clear()
    deleteSnapshotFiles()
    lastSnapOffset = 0L
    lastMapOffset = 0L
  }

  def completeTxn(completedTxn: CompletedTxn): Unit = {
    ongoingTxns -= OngoingTxn(completedTxn.producerId, completedTxn.firstOffset)
    unreplicatedTxns += completedTxn
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
