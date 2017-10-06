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
import kafka.log.Log.offsetFromFile
import kafka.server.LogOffsetMetadata
import kafka.utils.{Logging, nonthreadsafe, threadsafe}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.types._
import org.apache.kafka.common.record.{ControlRecordType, EndTransactionMarker, RecordBatch}
import org.apache.kafka.common.utils.{ByteUtils, Crc32C}

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}

class CorruptSnapshotException(msg: String) extends KafkaException(msg)


// ValidationType and its subtypes define the extent of the validation to perform on a given ProducerAppendInfo instance
private[log] sealed trait ValidationType
private[log] object ValidationType {

  /**
    * This indicates no validation should be performed on the incoming append. This is the case for all appends on
    * a replica, as well as appends when the producer state is being built from the log.
    */
  case object None extends ValidationType

  /**
    * We only validate the epoch (and not the sequence numbers) for offset commit requests coming from the transactional
    * producer. These appends will not have sequence numbers, so we can't validate them.
    */
  case object EpochOnly extends ValidationType

  /**
    * Perform the full validation. This should be used fo regular produce requests coming to the leader.
    */
  case object Full extends ValidationType
}

private[log] case class TxnMetadata(producerId: Long, var firstOffset: LogOffsetMetadata, var lastOffset: Option[Long] = None) {
  def this(producerId: Long, firstOffset: Long) = this(producerId, LogOffsetMetadata(firstOffset))

  override def toString: String = {
    "TxnMetadata(" +
      s"producerId=$producerId, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset)"
  }
}

private[log] object ProducerIdEntry {
  private[log] val NumBatchesToRetain = 5
  def empty(producerId: Long) = new ProducerIdEntry(producerId, mutable.Queue[BatchMetadata](), RecordBatch.NO_PRODUCER_EPOCH, -1, None)
}

private[log] case class BatchMetadata(lastSeq: Int, lastOffset: Long, offsetDelta: Int, timestamp: Long) {
  def firstSeq = lastSeq - offsetDelta
  def firstOffset = lastOffset - offsetDelta

  override def toString: String = {
    "BatchMetadata(" +
      s"firstSeq=$firstSeq, " +
      s"lastSeq=$lastSeq, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, " +
      s"timestamp=$timestamp)"
  }
}

// the batchMetadata is ordered such that the batch with the lowest sequence is at the head of the queue while the
// batch with the highest sequence is at the tail of the queue. We will retain at most ProducerIdEntry.NumBatchesToRetain
// elements in the queue. When the queue is at capacity, we remove the first element to make space for the incoming batch.
private[log] class ProducerIdEntry(val producerId: Long, val batchMetadata: mutable.Queue[BatchMetadata],
                                   var producerEpoch: Short, var coordinatorEpoch: Int,
                                   var currentTxnFirstOffset: Option[Long]) {

  def firstSeq: Int = if (batchMetadata.isEmpty) RecordBatch.NO_SEQUENCE else batchMetadata.front.firstSeq
  def firstOffset: Long = if (batchMetadata.isEmpty) -1L else batchMetadata.front.firstOffset

  def lastSeq: Int = if (batchMetadata.isEmpty) RecordBatch.NO_SEQUENCE else batchMetadata.last.lastSeq
  def lastDataOffset: Long = if (batchMetadata.isEmpty) -1L else batchMetadata.last.lastOffset
  def lastTimestamp = if (batchMetadata.isEmpty) RecordBatch.NO_TIMESTAMP else batchMetadata.last.timestamp
  def lastOffsetDelta : Int = if (batchMetadata.isEmpty) 0 else batchMetadata.last.offsetDelta

  def addBatchMetadata(producerEpoch: Short, lastSeq: Int, lastOffset: Long, offsetDelta: Int, timestamp: Long) = {
    maybeUpdateEpoch(producerEpoch)

    if (batchMetadata.size == ProducerIdEntry.NumBatchesToRetain)
      batchMetadata.dequeue()

    batchMetadata.enqueue(BatchMetadata(lastSeq, lastOffset, offsetDelta, timestamp))
  }

  def maybeUpdateEpoch(producerEpoch: Short): Boolean = {
    if (this.producerEpoch != producerEpoch) {
      batchMetadata.clear()
      this.producerEpoch = producerEpoch
      true
    } else {
      false
    }
  }

  def removeBatchesOlderThan(offset: Long) = batchMetadata.dropWhile(_.lastOffset < offset)

  def duplicateOf(batch: RecordBatch): Option[BatchMetadata] = {
    if (batch.producerEpoch() != producerEpoch)
       None
    else
      batchWithSequenceRange(batch.baseSequence(), batch.lastSequence())
  }

  // Return the batch metadata of the cached batch having the exact sequence range, if any.
  def batchWithSequenceRange(firstSeq: Int, lastSeq: Int): Option[BatchMetadata] = {
    val duplicate = batchMetadata.filter { case(metadata) =>
      firstSeq == metadata.firstSeq && lastSeq == metadata.lastSeq
    }
    duplicate.headOption
  }

  override def toString: String = {
    "ProducerIdEntry(" +
      s"producerId=$producerId, " +
      s"producerEpoch=$producerEpoch, " +
      s"currentTxnFirstOffset=$currentTxnFirstOffset, " +
      s"coordinatorEpoch=$coordinatorEpoch, " +
      s"batchMetadata=$batchMetadata"
  }
}

/**
 * This class is used to validate the records appended by a given producer before they are written to the log.
 * It is initialized with the producer's state after the last successful append, and transitively validates the
 * sequence numbers and epochs of each new record. Additionally, this class accumulates transaction metadata
 * as the incoming records are validated.
 *
 * @param producerId The id of the producer appending to the log
 * @param currentEntry  The current entry associated with the producer id which contains metadata for a fixed number of
 *                      the most recent appends made by the producer. Validation of the first incoming append will
 *                      be made against the lastest append in the current entry. New appends will replace older appends
 *                      in the current entry so that the space overhead is constant.
 * @param validationType Indicates the extent of validation to perform on the appends on this instance. Offset commits
 *                       coming from the producer should have ValidationType.EpochOnly. Appends which aren't from a client
 *                       should have ValidationType.None. Appends coming from a client for produce requests should have
 *                       ValidationType.Full.
 */
private[log] class ProducerAppendInfo(val producerId: Long,
                                      currentEntry: ProducerIdEntry,
                                      validationType: ValidationType) {

  private val transactions = ListBuffer.empty[TxnMetadata]

  private def maybeValidateAppend(producerEpoch: Short, firstSeq: Int, lastSeq: Int) = {
    validationType match {
      case ValidationType.None =>

      case ValidationType.EpochOnly =>
        checkEpoch(producerEpoch)

      case ValidationType.Full =>
        checkEpoch(producerEpoch)
        checkSequence(producerEpoch, firstSeq, lastSeq)
    }
  }

  private def checkEpoch(producerEpoch: Short): Unit = {
    if (isFenced(producerEpoch)) {
      throw new ProducerFencedException(s"Producer's epoch is no longer valid. There is probably another producer " +
        s"with a newer epoch. $producerEpoch (request epoch), ${currentEntry.producerEpoch} (server epoch)")
    }
  }

  private def checkSequence(producerEpoch: Short, firstSeq: Int, lastSeq: Int): Unit = {
    if (producerEpoch != currentEntry.producerEpoch) {
      if (firstSeq != 0) {
        if (currentEntry.producerEpoch != RecordBatch.NO_PRODUCER_EPOCH) {
          throw new OutOfOrderSequenceException(s"Invalid sequence number for new epoch: $producerEpoch " +
            s"(request epoch), $firstSeq (seq. number)")
        } else {
          throw new UnknownProducerIdException(s"Found no record of producerId=$producerId on the broker. It is possible " +
            s"that the last message with the producerId=$producerId has been removed due to hitting the retention limit.")
        }
      }
    } else if (currentEntry.lastSeq == RecordBatch.NO_SEQUENCE && firstSeq != 0) {
      // the epoch was bumped by a control record, so we expect the sequence number to be reset
      throw new OutOfOrderSequenceException(s"Out of order sequence number for producerId $producerId: found $firstSeq " +
        s"(incoming seq. number), but expected 0")
    } else if (isDuplicate(firstSeq, lastSeq)) {
      throw new DuplicateSequenceException(s"Duplicate sequence number for producerId $producerId: (incomingBatch.firstSeq, " +
        s"incomingBatch.lastSeq): ($firstSeq, $lastSeq).")
    } else if (!inSequence(firstSeq, lastSeq)) {
      throw new OutOfOrderSequenceException(s"Out of order sequence number for producerId $producerId: $firstSeq " +
        s"(incoming seq. number), ${currentEntry.lastSeq} (current end sequence number)")
    }
  }

  private def isDuplicate(firstSeq: Int, lastSeq: Int): Boolean = {
    ((lastSeq != 0 && currentEntry.firstSeq != Int.MaxValue && lastSeq < currentEntry.firstSeq)
      || currentEntry.batchWithSequenceRange(firstSeq, lastSeq).isDefined)
  }

  private def inSequence(firstSeq: Int, lastSeq: Int): Boolean = {
    firstSeq == currentEntry.lastSeq + 1L || (firstSeq == 0 && currentEntry.lastSeq == Int.MaxValue)
  }

  private def isFenced(producerEpoch: Short): Boolean = {
    producerEpoch < currentEntry.producerEpoch
  }

  def append(batch: RecordBatch): Option[CompletedTxn] = {
    if (batch.isControlBatch) {
      val record = batch.iterator.next()
      val endTxnMarker = EndTransactionMarker.deserialize(record)
      val completedTxn = appendEndTxnMarker(endTxnMarker, batch.producerEpoch, batch.baseOffset, record.timestamp)
      Some(completedTxn)
    } else {
      append(batch.producerEpoch, batch.baseSequence, batch.lastSequence, batch.maxTimestamp, batch.lastOffset,
        batch.isTransactional)
      None
    }
  }

  def append(epoch: Short,
             firstSeq: Int,
             lastSeq: Int,
             lastTimestamp: Long,
             lastOffset: Long,
             isTransactional: Boolean): Unit = {
    maybeValidateAppend(epoch, firstSeq, lastSeq)

    currentEntry.addBatchMetadata(epoch, lastSeq, lastOffset, lastSeq - firstSeq, lastTimestamp)

    if (currentEntry.currentTxnFirstOffset.isDefined && !isTransactional)
      throw new InvalidTxnStateException(s"Expected transactional write from producer $producerId")

    if (isTransactional && currentEntry.currentTxnFirstOffset.isEmpty) {
      val firstOffset = lastOffset - (lastSeq - firstSeq)
      currentEntry.currentTxnFirstOffset = Some(firstOffset)
      transactions += new TxnMetadata(producerId, firstOffset)
    }
  }

  def appendEndTxnMarker(endTxnMarker: EndTransactionMarker,
                         producerEpoch: Short,
                         offset: Long,
                         timestamp: Long): CompletedTxn = {
    if (isFenced(producerEpoch))
      throw new ProducerFencedException(s"Invalid producer epoch: $producerEpoch (zombie): ${currentEntry.producerEpoch} (current)")

    if (currentEntry.coordinatorEpoch > endTxnMarker.coordinatorEpoch)
      throw new TransactionCoordinatorFencedException(s"Invalid coordinator epoch: ${endTxnMarker.coordinatorEpoch} " +
        s"(zombie), ${currentEntry.coordinatorEpoch} (current)")

    currentEntry.maybeUpdateEpoch(producerEpoch)

    val firstOffset = currentEntry.currentTxnFirstOffset match {
      case Some(txnFirstOffset) => txnFirstOffset
      case None =>
        transactions += new TxnMetadata(producerId, offset)
        offset
    }

    currentEntry.currentTxnFirstOffset = None
    currentEntry.coordinatorEpoch = endTxnMarker.coordinatorEpoch
    CompletedTxn(producerId, firstOffset, offset, endTxnMarker.controlType == ControlRecordType.ABORT)
  }

  def latestEntry: ProducerIdEntry = currentEntry

  def startedTransactions: List[TxnMetadata] = transactions.toList

  def maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata: LogOffsetMetadata): Unit = {
    // we will cache the log offset metadata if it corresponds to the starting offset of
    // the last transaction that was started. This is optimized for leader appends where it
    // is only possible to have one transaction started for each log append, and the log
    // offset metadata will always match in that case since no data from other producers
    // is mixed into the append
    transactions.headOption.foreach { txn =>
      if (txn.firstOffset.messageOffset == logOffsetMetadata.messageOffset)
        txn.firstOffset = logOffsetMetadata
    }
  }

  override def toString: String = {
    "ProducerAppendInfo(" +
      s"producerId=$producerId, " +
      s"producerEpoch=${currentEntry.producerEpoch}, " +
      s"firstSequence=${currentEntry.firstSeq}, " +
      s"lastSequence=${currentEntry.lastSeq}, " +
      s"currentTxnFirstOffset=${currentEntry.currentTxnFirstOffset}, " +
      s"coordinatorEpoch=${currentEntry.coordinatorEpoch}, " +
      s"startedTransactions=$transactions)"
  }
}

object ProducerStateManager {
  private val ProducerSnapshotVersion: Short = 1
  private val VersionField = "version"
  private val CrcField = "crc"
  private val ProducerIdField = "producer_id"
  private val LastSequenceField = "last_sequence"
  private val ProducerEpochField = "epoch"
  private val LastOffsetField = "last_offset"
  private val OffsetDeltaField = "offset_delta"
  private val TimestampField = "timestamp"
  private val ProducerEntriesField = "producer_entries"
  private val CoordinatorEpochField = "coordinator_epoch"
  private val CurrentTxnFirstOffsetField = "current_txn_first_offset"

  private val VersionOffset = 0
  private val CrcOffset = VersionOffset + 2
  private val ProducerEntriesOffset = CrcOffset + 4

  val ProducerSnapshotEntrySchema = new Schema(
    new Field(ProducerIdField, Type.INT64, "The producer ID"),
    new Field(ProducerEpochField, Type.INT16, "Current epoch of the producer"),
    new Field(LastSequenceField, Type.INT32, "Last written sequence of the producer"),
    new Field(LastOffsetField, Type.INT64, "Last written offset of the producer"),
    new Field(OffsetDeltaField, Type.INT32, "The difference of the last sequence and first sequence in the last written batch"),
    new Field(TimestampField, Type.INT64, "Max timestamp from the last written entry"),
    new Field(CoordinatorEpochField, Type.INT32, "The epoch of the last transaction coordinator to send an end transaction marker"),
    new Field(CurrentTxnFirstOffsetField, Type.INT64, "The first offset of the on-going transaction (-1 if there is none)"))
  val PidSnapshotMapSchema = new Schema(
    new Field(VersionField, Type.INT16, "Version of the snapshot file"),
    new Field(CrcField, Type.UNSIGNED_INT32, "CRC of the snapshot data"),
    new Field(ProducerEntriesField, new ArrayOf(ProducerSnapshotEntrySchema), "The entries in the producer table"))

  def readSnapshot(file: File): Iterable[ProducerIdEntry] = {
    try {
      val buffer = Files.readAllBytes(file.toPath)
      val struct = PidSnapshotMapSchema.read(ByteBuffer.wrap(buffer))

      val version = struct.getShort(VersionField)
      if (version != ProducerSnapshotVersion)
        throw new CorruptSnapshotException(s"Snapshot contained an unknown file version $version")

      val crc = struct.getUnsignedInt(CrcField)
      val computedCrc =  Crc32C.compute(buffer, ProducerEntriesOffset, buffer.length - ProducerEntriesOffset)
      if (crc != computedCrc)
        throw new CorruptSnapshotException(s"Snapshot is corrupt (CRC is no longer valid). " +
          s"Stored crc: $crc. Computed crc: $computedCrc")

      struct.getArray(ProducerEntriesField).map { producerEntryObj =>
        val producerEntryStruct = producerEntryObj.asInstanceOf[Struct]
        val producerId: Long = producerEntryStruct.getLong(ProducerIdField)
        val producerEpoch = producerEntryStruct.getShort(ProducerEpochField)
        val seq = producerEntryStruct.getInt(LastSequenceField)
        val offset = producerEntryStruct.getLong(LastOffsetField)
        val timestamp = producerEntryStruct.getLong(TimestampField)
        val offsetDelta = producerEntryStruct.getInt(OffsetDeltaField)
        val coordinatorEpoch = producerEntryStruct.getInt(CoordinatorEpochField)
        val currentTxnFirstOffset = producerEntryStruct.getLong(CurrentTxnFirstOffsetField)
        val newEntry = new ProducerIdEntry(producerId, mutable.Queue[BatchMetadata](BatchMetadata(seq, offset, offsetDelta, timestamp)), producerEpoch,
          coordinatorEpoch, if (currentTxnFirstOffset >= 0) Some(currentTxnFirstOffset) else None)
        newEntry
      }
    } catch {
      case e: SchemaException =>
        throw new CorruptSnapshotException(s"Snapshot failed schema validation: ${e.getMessage}")
    }
  }

  private def writeSnapshot(file: File, entries: mutable.Map[Long, ProducerIdEntry]) {
    val struct = new Struct(PidSnapshotMapSchema)
    struct.set(VersionField, ProducerSnapshotVersion)
    struct.set(CrcField, 0L) // we'll fill this after writing the entries
    val entriesArray = entries.map {
      case (producerId, entry) =>
        val producerEntryStruct = struct.instance(ProducerEntriesField)
        producerEntryStruct.set(ProducerIdField, producerId)
          .set(ProducerEpochField, entry.producerEpoch)
          .set(LastSequenceField, entry.lastSeq)
          .set(LastOffsetField, entry.lastDataOffset)
          .set(OffsetDeltaField, entry.lastOffsetDelta)
          .set(TimestampField, entry.lastTimestamp)
          .set(CoordinatorEpochField, entry.coordinatorEpoch)
          .set(CurrentTxnFirstOffsetField, entry.currentTxnFirstOffset.getOrElse(-1L))
        producerEntryStruct
    }.toArray
    struct.set(ProducerEntriesField, entriesArray)

    val buffer = ByteBuffer.allocate(struct.sizeOf)
    struct.writeTo(buffer)
    buffer.flip()

    // now fill in the CRC
    val crc = Crc32C.compute(buffer, ProducerEntriesOffset, buffer.limit() - ProducerEntriesOffset)
    ByteUtils.writeUnsignedInt(buffer, CrcOffset, crc)

    val fos = new FileOutputStream(file)
    try {
      fos.write(buffer.array, buffer.arrayOffset, buffer.limit())
    } finally {
      fos.close()
    }
  }

  private def isSnapshotFile(file: File): Boolean = file.getName.endsWith(Log.ProducerSnapshotFileSuffix)

  // visible for testing
  private[log] def listSnapshotFiles(dir: File): Seq[File] = {
    if (dir.exists && dir.isDirectory) {
      Option(dir.listFiles).map { files =>
        files.filter(f => f.isFile && isSnapshotFile(f)).toSeq
      }.getOrElse(Seq.empty)
    } else Seq.empty
  }

  // visible for testing
  private[log] def deleteSnapshotsBefore(dir: File, offset: Long): Unit = deleteSnapshotFiles(dir, _ < offset)

  private def deleteSnapshotFiles(dir: File, predicate: Long => Boolean = _ => true) {
    listSnapshotFiles(dir).filter(file => predicate(offsetFromFile(file))).foreach { file =>
      Files.deleteIfExists(file.toPath)
    }
  }

}

/**
 * Maintains a mapping from ProducerIds to metadata about the last appended entries (e.g.
 * epoch, sequence number, last offset, etc.)
 *
 * The sequence number is the last number successfully appended to the partition for the given identifier.
 * The epoch is used for fencing against zombie writers. The offset is the one of the last successful message
 * appended to the partition.
 *
 * As long as a producer id is contained in the map, the corresponding producer can continue to write data.
 * However, producer ids can be expired due to lack of recent use or if the last written entry has been deleted from
 * the log (e.g. if the retention policy is "delete"). For compacted topics, the log cleaner will ensure
 * that the most recent entry from a given producer id is retained in the log provided it hasn't expired due to
 * age. This ensures that producer ids will not be expired until either the max expiration time has been reached,
 * or if the topic also is configured for deletion, the segment containing the last written offset has
 * been deleted.
 */
@nonthreadsafe
class ProducerStateManager(val topicPartition: TopicPartition,
                           val logDir: File,
                           val maxProducerIdExpirationMs: Int = 60 * 60 * 1000) extends Logging {
  import ProducerStateManager._
  import java.util

  private val producers = mutable.Map.empty[Long, ProducerIdEntry]
  private var lastMapOffset = 0L
  private var lastSnapOffset = 0L

  // ongoing transactions sorted by the first offset of the transaction
  private val ongoingTxns = new util.TreeMap[Long, TxnMetadata]

  // completed transactions whose markers are at offsets above the high watermark
  private val unreplicatedTxns = new util.TreeMap[Long, TxnMetadata]

  /**
   * An unstable offset is one which is either undecided (i.e. its ultimate outcome is not yet known),
   * or one that is decided, but may not have been replicated (i.e. any transaction which has a COMMIT/ABORT
   * marker written at a higher offset than the current high watermark).
   */
  def firstUnstableOffset: Option[LogOffsetMetadata] = {
    val unreplicatedFirstOffset = Option(unreplicatedTxns.firstEntry).map(_.getValue.firstOffset)
    val undecidedFirstOffset = Option(ongoingTxns.firstEntry).map(_.getValue.firstOffset)
    if (unreplicatedFirstOffset.isEmpty)
      undecidedFirstOffset
    else if (undecidedFirstOffset.isEmpty)
      unreplicatedFirstOffset
    else if (undecidedFirstOffset.get.messageOffset < unreplicatedFirstOffset.get.messageOffset)
      undecidedFirstOffset
    else
      unreplicatedFirstOffset
  }

  /**
   * Acknowledge all transactions which have been completed before a given offset. This allows the LSO
   * to advance to the next unstable offset.
   */
  def onHighWatermarkUpdated(highWatermark: Long): Unit = {
    removeUnreplicatedTransactions(highWatermark)
  }

  /**
   * The first undecided offset is the earliest transactional message which has not yet been committed
   * or aborted.
   */
  def firstUndecidedOffset: Option[Long] = Option(ongoingTxns.firstEntry).map(_.getValue.firstOffset.messageOffset)

  /**
   * Returns the last offset of this map
   */
  def mapEndOffset = lastMapOffset

  /**
   * Get a copy of the active producers
   */
  def activeProducers: immutable.Map[Long, ProducerIdEntry] = producers.toMap

  def isEmpty: Boolean = producers.isEmpty && unreplicatedTxns.isEmpty

  private def loadFromSnapshot(logStartOffset: Long, currentTime: Long) {
    while (true) {
      latestSnapshotFile match {
        case Some(file) =>
          try {
            info(s"Loading producer state from snapshot file '$file' for partition $topicPartition")
            val loadedProducers = readSnapshot(file).filter { producerEntry =>
              isProducerRetained(producerEntry, logStartOffset) && !isProducerExpired(currentTime, producerEntry)
            }
            loadedProducers.foreach(loadProducerEntry)
            lastSnapOffset = offsetFromFile(file)
            lastMapOffset = lastSnapOffset
            return
          } catch {
            case e: CorruptSnapshotException =>
              warn(s"Failed to load producer snapshot from '$file': ${e.getMessage}")
              Files.deleteIfExists(file.toPath)
          }
        case None =>
          lastSnapOffset = logStartOffset
          lastMapOffset = logStartOffset
          return
      }
    }
  }

  // visible for testing
  private[log] def loadProducerEntry(entry: ProducerIdEntry): Unit = {
    val producerId = entry.producerId
    producers.put(producerId, entry)
    entry.currentTxnFirstOffset.foreach { offset =>
      ongoingTxns.put(offset, new TxnMetadata(producerId, offset))
    }
  }

  private def isProducerExpired(currentTimeMs: Long, producerIdEntry: ProducerIdEntry): Boolean =
    producerIdEntry.currentTxnFirstOffset.isEmpty && currentTimeMs - producerIdEntry.lastTimestamp >= maxProducerIdExpirationMs

  /**
   * Expire any producer ids which have been idle longer than the configured maximum expiration timeout.
   */
  def removeExpiredProducers(currentTimeMs: Long) {
    producers.retain { case (producerId, lastEntry) =>
      !isProducerExpired(currentTimeMs, lastEntry)
    }
  }

  /**
   * Truncate the producer id mapping to the given offset range and reload the entries from the most recent
   * snapshot in range (if there is one). Note that the log end offset is assumed to be less than
   * or equal to the high watermark.
   */
  def truncateAndReload(logStartOffset: Long, logEndOffset: Long, currentTimeMs: Long) {
    // remove all out of range snapshots
    deleteSnapshotFiles(logDir, { snapOffset =>
      snapOffset > logEndOffset || snapOffset <= logStartOffset
    })

    if (logEndOffset != mapEndOffset) {
      producers.clear()
      ongoingTxns.clear()

      // since we assume that the offset is less than or equal to the high watermark, it is
      // safe to clear the unreplicated transactions
      unreplicatedTxns.clear()
      loadFromSnapshot(logStartOffset, currentTimeMs)
    } else {
      truncateHead(logStartOffset)
    }
  }

  def prepareUpdate(producerId: Long, isFromClient: Boolean): ProducerAppendInfo = {
    val validationToPerform =
      if (!isFromClient)
        ValidationType.None
      else if (topicPartition.topic == Topic.GROUP_METADATA_TOPIC_NAME)
        ValidationType.EpochOnly
      else
        ValidationType.Full

    new ProducerAppendInfo(producerId, lastEntry(producerId).getOrElse(ProducerIdEntry.empty(producerId)), validationToPerform)
  }

  /**
   * Update the mapping with the given append information
   */
  def update(appendInfo: ProducerAppendInfo): Unit = {
    if (appendInfo.producerId == RecordBatch.NO_PRODUCER_ID)
      throw new IllegalArgumentException(s"Invalid producer id ${appendInfo.producerId} passed to update")

    trace(s"Updated producer ${appendInfo.producerId} state to $appendInfo")

    val entry = appendInfo.latestEntry
    producers.put(appendInfo.producerId, entry)
    appendInfo.startedTransactions.foreach { txn =>
      ongoingTxns.put(txn.firstOffset.messageOffset, txn)
    }
  }

  def updateMapEndOffset(lastOffset: Long): Unit = {
    lastMapOffset = lastOffset
  }

  /**
   * Get the last written entry for the given producer id.
   */
  def lastEntry(producerId: Long): Option[ProducerIdEntry] = producers.get(producerId)

  /**
   * Take a snapshot at the current end offset if one does not already exist.
   */
  def takeSnapshot(): Unit = {
    // If not a new offset, then it is not worth taking another snapshot
    if (lastMapOffset > lastSnapOffset) {
      val snapshotFile = Log.producerSnapshotFile(logDir, lastMapOffset)
      debug(s"Writing producer snapshot for partition $topicPartition at offset $lastMapOffset")
      writeSnapshot(snapshotFile, producers)

      // Update the last snap offset according to the serialized map
      lastSnapOffset = lastMapOffset
    }
  }

  /**
   * Get the last offset (exclusive) of the latest snapshot file.
   */
  def latestSnapshotOffset: Option[Long] = latestSnapshotFile.map(file => offsetFromFile(file))

  /**
   * Get the last offset (exclusive) of the oldest snapshot file.
   */
  def oldestSnapshotOffset: Option[Long] = oldestSnapshotFile.map(file => offsetFromFile(file))

  private def isProducerRetained(producerIdEntry: ProducerIdEntry, logStartOffset: Long): Boolean = {
    producerIdEntry.removeBatchesOlderThan(logStartOffset)
    producerIdEntry.lastDataOffset >= logStartOffset
  }

  /**
   * When we remove the head of the log due to retention, we need to clean up the id map. This method takes
   * the new start offset and removes all producerIds which have a smaller last written offset. Additionally,
   * we remove snapshots older than the new log start offset.
   *
   * Note that snapshots from offsets greater than the log start offset may have producers included which
   * should no longer be retained: these producers will be removed if and when we need to load state from
   * the snapshot.
   */
  def truncateHead(logStartOffset: Long) {
    val evictedProducerEntries = producers.filter { case (_, producerIdEntry) =>
      !isProducerRetained(producerIdEntry, logStartOffset)
    }
    val evictedProducerIds = evictedProducerEntries.keySet

    producers --= evictedProducerIds
    removeEvictedOngoingTransactions(evictedProducerIds)
    removeUnreplicatedTransactions(logStartOffset)

    if (lastMapOffset < logStartOffset)
      lastMapOffset = logStartOffset

    deleteSnapshotsBefore(logStartOffset)
    lastSnapOffset = latestSnapshotOffset.getOrElse(logStartOffset)
  }

  private def removeEvictedOngoingTransactions(expiredProducerIds: collection.Set[Long]): Unit = {
    val iterator = ongoingTxns.entrySet.iterator
    while (iterator.hasNext) {
      val txnEntry = iterator.next()
      if (expiredProducerIds.contains(txnEntry.getValue.producerId))
        iterator.remove()
    }
  }

  private def removeUnreplicatedTransactions(offset: Long): Unit = {
    val iterator = unreplicatedTxns.entrySet.iterator
    while (iterator.hasNext) {
      val txnEntry = iterator.next()
      val lastOffset = txnEntry.getValue.lastOffset
      if (lastOffset.exists(_ < offset))
        iterator.remove()
    }
  }

  /**
   * Truncate the producer id mapping and remove all snapshots. This resets the state of the mapping.
   */
  def truncate() {
    producers.clear()
    ongoingTxns.clear()
    unreplicatedTxns.clear()
    deleteSnapshotFiles(logDir)
    lastSnapOffset = 0L
    lastMapOffset = 0L
  }

  /**
   * Complete the transaction and return the last stable offset.
   */
  def completeTxn(completedTxn: CompletedTxn): Long = {
    val txnMetadata = ongoingTxns.remove(completedTxn.firstOffset)
    if (txnMetadata == null)
      throw new IllegalArgumentException("Attempted to complete a transaction which was not started")

    txnMetadata.lastOffset = Some(completedTxn.lastOffset)
    unreplicatedTxns.put(completedTxn.firstOffset, txnMetadata)

    val lastStableOffset = firstUndecidedOffset.getOrElse(completedTxn.lastOffset + 1)
    lastStableOffset
  }

  @threadsafe
  def deleteSnapshotsBefore(offset: Long): Unit = ProducerStateManager.deleteSnapshotsBefore(logDir, offset)

  private def oldestSnapshotFile: Option[File] = {
    val files = listSnapshotFiles
    if (files.nonEmpty)
      Some(files.minBy(offsetFromFile))
    else
      None
  }

  private def latestSnapshotFile: Option[File] = {
    val files = listSnapshotFiles
    if (files.nonEmpty)
      Some(files.maxBy(offsetFromFile))
    else
      None
  }

  private def listSnapshotFiles: Seq[File] = ProducerStateManager.listSnapshotFiles(logDir)

}
