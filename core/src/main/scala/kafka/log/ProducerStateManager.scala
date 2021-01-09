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

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, StandardOpenOption}
import java.util.concurrent.ConcurrentSkipListMap

import kafka.log.Log.offsetFromFile
import kafka.server.LogOffsetMetadata
import kafka.utils.{Logging, nonthreadsafe, threadsafe}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.protocol.types._
import org.apache.kafka.common.record.{ControlRecordType, DefaultRecordBatch, EndTransactionMarker, RecordBatch}
import org.apache.kafka.common.utils.{ByteUtils, Crc32C}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}

class CorruptSnapshotException(msg: String) extends KafkaException(msg)

/**
 * The last written record for a given producer. The last data offset may be undefined
 * if the only log entry for a producer is a transaction marker.
 */
case class LastRecord(lastDataOffset: Option[Long], producerEpoch: Short)


private[log] case class TxnMetadata(
  producerId: Long,
  firstOffset: LogOffsetMetadata,
  var lastOffset: Option[Long] = None
) {
  def this(producerId: Long, firstOffset: Long) = this(producerId, LogOffsetMetadata(firstOffset))

  override def toString: String = {
    "TxnMetadata(" +
      s"producerId=$producerId, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset)"
  }
}

private[log] object ProducerStateEntry {
  private[log] val NumBatchesToRetain = 5

  def empty(producerId: Long) = new ProducerStateEntry(producerId,
    batchMetadata = mutable.Queue[BatchMetadata](),
    producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
    coordinatorEpoch = -1,
    lastTimestamp = RecordBatch.NO_TIMESTAMP,
    currentTxnFirstOffset = None)
}

private[log] case class BatchMetadata(lastSeq: Int, lastOffset: Long, offsetDelta: Int, timestamp: Long) {
  def firstSeq: Int =  DefaultRecordBatch.decrementSequence(lastSeq, offsetDelta)
  def firstOffset: Long = lastOffset - offsetDelta

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
// batch with the highest sequence is at the tail of the queue. We will retain at most ProducerStateEntry.NumBatchesToRetain
// elements in the queue. When the queue is at capacity, we remove the first element to make space for the incoming batch.
private[log] class ProducerStateEntry(val producerId: Long,
                                      val batchMetadata: mutable.Queue[BatchMetadata],
                                      var producerEpoch: Short,
                                      var coordinatorEpoch: Int,
                                      var lastTimestamp: Long,
                                      var currentTxnFirstOffset: Option[Long]) {

  def firstSeq: Int = if (isEmpty) RecordBatch.NO_SEQUENCE else batchMetadata.front.firstSeq

  def firstDataOffset: Long = if (isEmpty) -1L else batchMetadata.front.firstOffset

  def lastSeq: Int = if (isEmpty) RecordBatch.NO_SEQUENCE else batchMetadata.last.lastSeq

  def lastDataOffset: Long = if (isEmpty) -1L else batchMetadata.last.lastOffset

  def lastOffsetDelta : Int = if (isEmpty) 0 else batchMetadata.last.offsetDelta

  def isEmpty: Boolean = batchMetadata.isEmpty

  def addBatch(producerEpoch: Short, lastSeq: Int, lastOffset: Long, offsetDelta: Int, timestamp: Long): Unit = {
    maybeUpdateProducerEpoch(producerEpoch)
    addBatchMetadata(BatchMetadata(lastSeq, lastOffset, offsetDelta, timestamp))
    this.lastTimestamp = timestamp
  }

  def maybeUpdateProducerEpoch(producerEpoch: Short): Boolean = {
    if (this.producerEpoch != producerEpoch) {
      batchMetadata.clear()
      this.producerEpoch = producerEpoch
      true
    } else {
      false
    }
  }

  private def addBatchMetadata(batch: BatchMetadata): Unit = {
    if (batchMetadata.size == ProducerStateEntry.NumBatchesToRetain)
      batchMetadata.dequeue()
    batchMetadata.enqueue(batch)
  }

  def update(nextEntry: ProducerStateEntry): Unit = {
    maybeUpdateProducerEpoch(nextEntry.producerEpoch)
    while (nextEntry.batchMetadata.nonEmpty)
      addBatchMetadata(nextEntry.batchMetadata.dequeue())
    this.coordinatorEpoch = nextEntry.coordinatorEpoch
    this.currentTxnFirstOffset = nextEntry.currentTxnFirstOffset
    this.lastTimestamp = nextEntry.lastTimestamp
  }

  def findDuplicateBatch(batch: RecordBatch): Option[BatchMetadata] = {
    if (batch.producerEpoch != producerEpoch)
       None
    else
      batchWithSequenceRange(batch.baseSequence, batch.lastSequence)
  }

  // Return the batch metadata of the cached batch having the exact sequence range, if any.
  def batchWithSequenceRange(firstSeq: Int, lastSeq: Int): Option[BatchMetadata] = {
    val duplicate = batchMetadata.filter { metadata =>
      firstSeq == metadata.firstSeq && lastSeq == metadata.lastSeq
    }
    duplicate.headOption
  }

  override def toString: String = {
    "ProducerStateEntry(" +
      s"producerId=$producerId, " +
      s"producerEpoch=$producerEpoch, " +
      s"currentTxnFirstOffset=$currentTxnFirstOffset, " +
      s"coordinatorEpoch=$coordinatorEpoch, " +
      s"lastTimestamp=$lastTimestamp, " +
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
 *                      be made against the latest append in the current entry. New appends will replace older appends
 *                      in the current entry so that the space overhead is constant.
 * @param origin Indicates the origin of the append which implies the extent of validation. For example, offset
 *               commits, which originate from the group coordinator, do not have sequence numbers and therefore
 *               only producer epoch validation is done. Appends which come through replication are not validated
 *               (we assume the validation has already been done) and appends from clients require full validation.
 */
private[log] class ProducerAppendInfo(val topicPartition: TopicPartition,
                                      val producerId: Long,
                                      val currentEntry: ProducerStateEntry,
                                      val origin: AppendOrigin) extends Logging {

  private val transactions = ListBuffer.empty[TxnMetadata]
  private val updatedEntry = ProducerStateEntry.empty(producerId)

  updatedEntry.producerEpoch = currentEntry.producerEpoch
  updatedEntry.coordinatorEpoch = currentEntry.coordinatorEpoch
  updatedEntry.lastTimestamp = currentEntry.lastTimestamp
  updatedEntry.currentTxnFirstOffset = currentEntry.currentTxnFirstOffset

  private def maybeValidateDataBatch(producerEpoch: Short, firstSeq: Int, offset: Long): Unit = {
    checkProducerEpoch(producerEpoch, offset)
    if (origin == AppendOrigin.Client) {
      checkSequence(producerEpoch, firstSeq, offset)
    }
  }

  private def checkProducerEpoch(producerEpoch: Short, offset: Long): Unit = {
    if (producerEpoch < updatedEntry.producerEpoch) {
      val message = s"Epoch of producer $producerId at offset $offset in $topicPartition is $producerEpoch, " +
        s"which is smaller than the last seen epoch ${updatedEntry.producerEpoch}"

      if (origin == AppendOrigin.Replication) {
        warn(message)
      } else {
        // Starting from 2.7, we replaced ProducerFenced error with InvalidProducerEpoch in the
        // producer send response callback to differentiate from the former fatal exception,
        // letting client abort the ongoing transaction and retry.
        throw new InvalidProducerEpochException(message)
      }
    }
  }

  private def checkSequence(producerEpoch: Short, appendFirstSeq: Int, offset: Long): Unit = {
    if (producerEpoch != updatedEntry.producerEpoch) {
      if (appendFirstSeq != 0) {
        if (updatedEntry.producerEpoch != RecordBatch.NO_PRODUCER_EPOCH) {
          throw new OutOfOrderSequenceException(s"Invalid sequence number for new epoch of producer $producerId " +
            s"at offset $offset in partition $topicPartition: $producerEpoch (request epoch), $appendFirstSeq (seq. number), " +
            s"${updatedEntry.producerEpoch} (current producer epoch)")
        }
      }
    } else {
      val currentLastSeq = if (!updatedEntry.isEmpty)
        updatedEntry.lastSeq
      else if (producerEpoch == currentEntry.producerEpoch)
        currentEntry.lastSeq
      else
        RecordBatch.NO_SEQUENCE

      // If there is no current producer epoch (possibly because all producer records have been deleted due to
      // retention or the DeleteRecords API) accept writes with any sequence number
      if (!(currentEntry.producerEpoch == RecordBatch.NO_PRODUCER_EPOCH || inSequence(currentLastSeq, appendFirstSeq))) {
        throw new OutOfOrderSequenceException(s"Out of order sequence number for producer $producerId at " +
          s"offset $offset in partition $topicPartition: $appendFirstSeq (incoming seq. number), " +
          s"$currentLastSeq (current end sequence number)")
      }
    }
  }

  private def inSequence(lastSeq: Int, nextSeq: Int): Boolean = {
    nextSeq == lastSeq + 1L || (nextSeq == 0 && lastSeq == Int.MaxValue)
  }

  def append(batch: RecordBatch, firstOffsetMetadataOpt: Option[LogOffsetMetadata]): Option[CompletedTxn] = {
    if (batch.isControlBatch) {
      val recordIterator = batch.iterator
      if (recordIterator.hasNext) {
        val record = recordIterator.next()
        val endTxnMarker = EndTransactionMarker.deserialize(record)
        appendEndTxnMarker(endTxnMarker, batch.producerEpoch, batch.baseOffset, record.timestamp)
      } else {
        // An empty control batch means the entire transaction has been cleaned from the log, so no need to append
        None
      }
    } else {
      val firstOffsetMetadata = firstOffsetMetadataOpt.getOrElse(LogOffsetMetadata(batch.baseOffset))
      appendDataBatch(batch.producerEpoch, batch.baseSequence, batch.lastSequence, batch.maxTimestamp,
        firstOffsetMetadata, batch.lastOffset, batch.isTransactional)
      None
    }
  }

  def appendDataBatch(epoch: Short,
                      firstSeq: Int,
                      lastSeq: Int,
                      lastTimestamp: Long,
                      firstOffsetMetadata: LogOffsetMetadata,
                      lastOffset: Long,
                      isTransactional: Boolean): Unit = {
    val firstOffset = firstOffsetMetadata.messageOffset
    maybeValidateDataBatch(epoch, firstSeq, firstOffset)
    updatedEntry.addBatch(epoch, lastSeq, lastOffset, (lastOffset - firstOffset).toInt, lastTimestamp)

    updatedEntry.currentTxnFirstOffset match {
      case Some(_) if !isTransactional =>
        // Received a non-transactional message while a transaction is active
        throw new InvalidTxnStateException(s"Expected transactional write from producer $producerId at " +
          s"offset $firstOffsetMetadata in partition $topicPartition")

      case None if isTransactional =>
        // Began a new transaction
        updatedEntry.currentTxnFirstOffset = Some(firstOffset)
        transactions += TxnMetadata(producerId, firstOffsetMetadata)

      case _ => // nothing to do
    }
  }

  private def checkCoordinatorEpoch(endTxnMarker: EndTransactionMarker, offset: Long): Unit = {
    if (updatedEntry.coordinatorEpoch > endTxnMarker.coordinatorEpoch) {
      if (origin == AppendOrigin.Replication) {
        info(s"Detected invalid coordinator epoch for producerId $producerId at " +
          s"offset $offset in partition $topicPartition: ${endTxnMarker.coordinatorEpoch} " +
          s"is older than previously known coordinator epoch ${updatedEntry.coordinatorEpoch}")
      } else {
        throw new TransactionCoordinatorFencedException(s"Invalid coordinator epoch for producerId $producerId at " +
          s"offset $offset in partition $topicPartition: ${endTxnMarker.coordinatorEpoch} " +
          s"(zombie), ${updatedEntry.coordinatorEpoch} (current)")
      }
    }
  }

  def appendEndTxnMarker(
    endTxnMarker: EndTransactionMarker,
    producerEpoch: Short,
    offset: Long,
    timestamp: Long
  ): Option[CompletedTxn] = {
    checkProducerEpoch(producerEpoch, offset)
    checkCoordinatorEpoch(endTxnMarker, offset)

    // Only emit the `CompletedTxn` for non-empty transactions. A transaction marker
    // without any associated data will not have any impact on the last stable offset
    // and would not need to be reflected in the transaction index.
    val completedTxn = updatedEntry.currentTxnFirstOffset.map { firstOffset =>
      CompletedTxn(producerId, firstOffset, offset, endTxnMarker.controlType == ControlRecordType.ABORT)
    }

    updatedEntry.maybeUpdateProducerEpoch(producerEpoch)
    updatedEntry.currentTxnFirstOffset = None
    updatedEntry.coordinatorEpoch = endTxnMarker.coordinatorEpoch
    updatedEntry.lastTimestamp = timestamp

    completedTxn
  }

  def toEntry: ProducerStateEntry = updatedEntry

  def startedTransactions: List[TxnMetadata] = transactions.toList

  override def toString: String = {
    "ProducerAppendInfo(" +
      s"producerId=$producerId, " +
      s"producerEpoch=${updatedEntry.producerEpoch}, " +
      s"firstSequence=${updatedEntry.firstSeq}, " +
      s"lastSequence=${updatedEntry.lastSeq}, " +
      s"currentTxnFirstOffset=${updatedEntry.currentTxnFirstOffset}, " +
      s"coordinatorEpoch=${updatedEntry.coordinatorEpoch}, " +
      s"lastTimestamp=${updatedEntry.lastTimestamp}, " +
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

  def readSnapshot(file: File): Iterable[ProducerStateEntry] = {
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
        val producerId = producerEntryStruct.getLong(ProducerIdField)
        val producerEpoch = producerEntryStruct.getShort(ProducerEpochField)
        val seq = producerEntryStruct.getInt(LastSequenceField)
        val offset = producerEntryStruct.getLong(LastOffsetField)
        val timestamp = producerEntryStruct.getLong(TimestampField)
        val offsetDelta = producerEntryStruct.getInt(OffsetDeltaField)
        val coordinatorEpoch = producerEntryStruct.getInt(CoordinatorEpochField)
        val currentTxnFirstOffset = producerEntryStruct.getLong(CurrentTxnFirstOffsetField)
        val lastAppendedDataBatches = mutable.Queue.empty[BatchMetadata]
        if (offset >= 0)
          lastAppendedDataBatches += BatchMetadata(seq, offset, offsetDelta, timestamp)

        val newEntry = new ProducerStateEntry(producerId, lastAppendedDataBatches, producerEpoch,
          coordinatorEpoch, timestamp, if (currentTxnFirstOffset >= 0) Some(currentTxnFirstOffset) else None)
        newEntry
      }
    } catch {
      case e: SchemaException =>
        throw new CorruptSnapshotException(s"Snapshot failed schema validation: ${e.getMessage}")
    }
  }

  private def writeSnapshot(file: File, entries: mutable.Map[Long, ProducerStateEntry]): Unit = {
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

    val fileChannel = FileChannel.open(file.toPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
    try {
      fileChannel.write(buffer)
      fileChannel.force(true)
    } finally {
      fileChannel.close()
    }
  }

  private def isSnapshotFile(file: File): Boolean = file.getName.endsWith(Log.ProducerSnapshotFileSuffix)

  // visible for testing
  private[log] def listSnapshotFiles(dir: File): Seq[SnapshotFile] = {
    if (dir.exists && dir.isDirectory) {
      Option(dir.listFiles).map { files =>
        files.filter(f => f.isFile && isSnapshotFile(f)).map(SnapshotFile(_)).toSeq
      }.getOrElse(Seq.empty)
    } else Seq.empty
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
                           @volatile var _logDir: File,
                           val maxProducerIdExpirationMs: Int = 60 * 60 * 1000) extends Logging {
  import ProducerStateManager._
  import java.util

  this.logIdent = s"[ProducerStateManager partition=$topicPartition] "

  private var snapshots: ConcurrentSkipListMap[java.lang.Long, SnapshotFile] = locally {
    loadSnapshots()
  }

  private val producers = mutable.Map.empty[Long, ProducerStateEntry]
  private var lastMapOffset = 0L
  private var lastSnapOffset = 0L

  // ongoing transactions sorted by the first offset of the transaction
  private val ongoingTxns = new util.TreeMap[Long, TxnMetadata]

  // completed transactions whose markers are at offsets above the high watermark
  private val unreplicatedTxns = new util.TreeMap[Long, TxnMetadata]

  /**
   * Load producer state snapshots by scanning the _logDir.
   */
  private def loadSnapshots(): ConcurrentSkipListMap[java.lang.Long, SnapshotFile] = {
    val tm = new ConcurrentSkipListMap[java.lang.Long, SnapshotFile]()
    for (f <- listSnapshotFiles(_logDir)) {
      tm.put(f.offset, f)
    }
    tm
  }

  /**
   * Scans the log directory, gathering all producer state snapshot files. Snapshot files which do not have an offset
   * corresponding to one of the provided offsets in segmentBaseOffsets will be removed, except in the case that there
   * is a snapshot file at a higher offset than any offset in segmentBaseOffsets.
   *
   * The goal here is to remove any snapshot files which do not have an associated segment file, but not to remove the
   * largest stray snapshot file which was emitted during clean shutdown.
   */
  private[log] def removeStraySnapshots(segmentBaseOffsets: Seq[Long]): Unit = {
    val maxSegmentBaseOffset = if (segmentBaseOffsets.isEmpty) None else Some(segmentBaseOffsets.max)
    val baseOffsets = segmentBaseOffsets.toSet
    var latestStraySnapshot: Option[SnapshotFile] = None

    val ss = loadSnapshots()
    for (snapshot <- ss.values().asScala) {
      val key = snapshot.offset
      latestStraySnapshot match {
        case Some(prev) =>
          if (!baseOffsets.contains(key)) {
            // this snapshot is now the largest stray snapshot.
            prev.deleteIfExists()
            ss.remove(prev.offset)
            latestStraySnapshot = Some(snapshot)
          }
        case None =>
          if (!baseOffsets.contains(key)) {
            latestStraySnapshot = Some(snapshot)
          }
      }
    }

    // Check to see if the latestStraySnapshot is larger than the largest segment base offset, if it is not,
    // delete the largestStraySnapshot.
    for (strayOffset <- latestStraySnapshot.map(_.offset); maxOffset <- maxSegmentBaseOffset) {
      if (strayOffset < maxOffset) {
        Option(ss.remove(strayOffset)).foreach(_.deleteIfExists())
      }
    }

    this.snapshots = ss
  }

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
   * or aborted. Unlike [[firstUnstableOffset]], this does not reflect the state of replication (i.e.
   * whether a completed transaction marker is beyond the high watermark).
   */
  private[log] def firstUndecidedOffset: Option[Long] = Option(ongoingTxns.firstEntry).map(_.getValue.firstOffset.messageOffset)

  /**
   * Returns the last offset of this map
   */
  def mapEndOffset: Long = lastMapOffset

  /**
   * Get a copy of the active producers
   */
  def activeProducers: immutable.Map[Long, ProducerStateEntry] = producers.toMap

  def isEmpty: Boolean = producers.isEmpty && unreplicatedTxns.isEmpty

  private def loadFromSnapshot(logStartOffset: Long, currentTime: Long): Unit = {
    while (true) {
      latestSnapshotFile match {
        case Some(snapshot) =>
          try {
            info(s"Loading producer state from snapshot file '$snapshot'")
            val loadedProducers = readSnapshot(snapshot.file).filter { producerEntry => !isProducerExpired(currentTime, producerEntry) }
            loadedProducers.foreach(loadProducerEntry)
            lastSnapOffset = snapshot.offset
            lastMapOffset = lastSnapOffset
            return
          } catch {
            case e: CorruptSnapshotException =>
              warn(s"Failed to load producer snapshot from '${snapshot.file}': ${e.getMessage}")
              removeAndDeleteSnapshot(snapshot.offset)
          }
        case None =>
          lastSnapOffset = logStartOffset
          lastMapOffset = logStartOffset
          return
      }
    }
  }

  // visible for testing
  private[log] def loadProducerEntry(entry: ProducerStateEntry): Unit = {
    val producerId = entry.producerId
    producers.put(producerId, entry)
    entry.currentTxnFirstOffset.foreach { offset =>
      ongoingTxns.put(offset, new TxnMetadata(producerId, offset))
    }
  }

  private def isProducerExpired(currentTimeMs: Long, producerState: ProducerStateEntry): Boolean =
    producerState.currentTxnFirstOffset.isEmpty && currentTimeMs - producerState.lastTimestamp >= maxProducerIdExpirationMs

  /**
   * Expire any producer ids which have been idle longer than the configured maximum expiration timeout.
   */
  def removeExpiredProducers(currentTimeMs: Long): Unit = {
    producers --= producers.filter { case (_, lastEntry) => isProducerExpired(currentTimeMs, lastEntry) }.keySet
  }

  /**
   * Truncate the producer id mapping to the given offset range and reload the entries from the most recent
   * snapshot in range (if there is one). We delete snapshot files prior to the logStartOffset but do not remove
   * producer state from the map. This means that in-memory and on-disk state can diverge, and in the case of
   * broker failover or unclean shutdown, any in-memory state not persisted in the snapshots will be lost, which
   * would lead to UNKNOWN_PRODUCER_ID errors. Note that the log end offset is assumed to be less than or equal
   * to the high watermark.
   */
  def truncateAndReload(logStartOffset: Long, logEndOffset: Long, currentTimeMs: Long): Unit = {
    // remove all out of range snapshots
    snapshots.values().asScala.foreach { snapshot =>
      if (snapshot.offset > logEndOffset || snapshot.offset <= logStartOffset) {
        removeAndDeleteSnapshot(snapshot.offset)
      }
    }

    if (logEndOffset != mapEndOffset) {
      producers.clear()
      ongoingTxns.clear()

      // since we assume that the offset is less than or equal to the high watermark, it is
      // safe to clear the unreplicated transactions
      unreplicatedTxns.clear()
      loadFromSnapshot(logStartOffset, currentTimeMs)
    } else {
      onLogStartOffsetIncremented(logStartOffset)
    }
  }

  def prepareUpdate(producerId: Long, origin: AppendOrigin): ProducerAppendInfo = {
    val currentEntry = lastEntry(producerId).getOrElse(ProducerStateEntry.empty(producerId))
    new ProducerAppendInfo(topicPartition, producerId, currentEntry, origin)
  }

  /**
   * Update the mapping with the given append information
   */
  def update(appendInfo: ProducerAppendInfo): Unit = {
    if (appendInfo.producerId == RecordBatch.NO_PRODUCER_ID)
      throw new IllegalArgumentException(s"Invalid producer id ${appendInfo.producerId} passed to update " +
        s"for partition $topicPartition")

    trace(s"Updated producer ${appendInfo.producerId} state to $appendInfo")
    val updatedEntry = appendInfo.toEntry
    producers.get(appendInfo.producerId) match {
      case Some(currentEntry) =>
        currentEntry.update(updatedEntry)

      case None =>
        producers.put(appendInfo.producerId, updatedEntry)
    }

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
  def lastEntry(producerId: Long): Option[ProducerStateEntry] = producers.get(producerId)

  /**
   * Take a snapshot at the current end offset if one does not already exist.
   */
  def takeSnapshot(): Unit = {
    // If not a new offset, then it is not worth taking another snapshot
    if (lastMapOffset > lastSnapOffset) {
      val snapshotFile = SnapshotFile(Log.producerSnapshotFile(_logDir, lastMapOffset))
      info(s"Writing producer snapshot at offset $lastMapOffset")
      writeSnapshot(snapshotFile.file, producers)
      snapshots.put(snapshotFile.offset, snapshotFile)

      // Update the last snap offset according to the serialized map
      lastSnapOffset = lastMapOffset
    }
  }

  /**
   * Update the parentDir for this ProducerStateManager and all of the snapshot files which it manages.
   */
  def updateParentDir(parentDir: File): Unit ={
    _logDir = parentDir
    snapshots.forEach((_, s) => s.updateParentDir(parentDir))
  }

  /**
   * Get the last offset (exclusive) of the latest snapshot file.
   */
  def latestSnapshotOffset: Option[Long] = latestSnapshotFile.map(_.offset)

  /**
   * Get the last offset (exclusive) of the oldest snapshot file.
   */
  def oldestSnapshotOffset: Option[Long] = oldestSnapshotFile.map(_.offset)

  /**
   * Remove any unreplicated transactions lower than the provided logStartOffset and bring the lastMapOffset forward
   * if necessary.
   */
  def onLogStartOffsetIncremented(logStartOffset: Long): Unit = {
    removeUnreplicatedTransactions(logStartOffset)

    if (lastMapOffset < logStartOffset)
      lastMapOffset = logStartOffset

    lastSnapOffset = latestSnapshotOffset.getOrElse(logStartOffset)
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
  def truncateFullyAndStartAt(offset: Long): Unit = {
    producers.clear()
    ongoingTxns.clear()
    unreplicatedTxns.clear()
    snapshots.values().asScala.foreach { snapshot =>
      removeAndDeleteSnapshot(snapshot.offset)
    }
    lastSnapOffset = 0L
    lastMapOffset = offset
  }

  /**
   * Compute the last stable offset of a completed transaction, but do not yet mark the transaction complete.
   * That will be done in `completeTxn` below. This is used to compute the LSO that will be appended to the
   * transaction index, but the completion must be done only after successfully appending to the index.
   */
  def lastStableOffset(completedTxn: CompletedTxn): Long = {
    val nextIncompleteTxn = ongoingTxns.values.asScala.find(_.producerId != completedTxn.producerId)
    nextIncompleteTxn.map(_.firstOffset.messageOffset).getOrElse(completedTxn.lastOffset  + 1)
  }

  /**
   * Mark a transaction as completed. We will still await advancement of the high watermark before
   * advancing the first unstable offset.
   */
  def completeTxn(completedTxn: CompletedTxn): Unit = {
    val txnMetadata = ongoingTxns.remove(completedTxn.firstOffset)
    if (txnMetadata == null)
      throw new IllegalArgumentException(s"Attempted to complete transaction $completedTxn on partition $topicPartition " +
        s"which was not started")

    txnMetadata.lastOffset = Some(completedTxn.lastOffset)
    unreplicatedTxns.put(completedTxn.firstOffset, txnMetadata)
  }

  @threadsafe
  def deleteSnapshotsBefore(offset: Long): Unit = {
    snapshots.subMap(0, offset).values().asScala.foreach { snapshot =>
      removeAndDeleteSnapshot(snapshot.offset)
    }
  }

  private def oldestSnapshotFile: Option[SnapshotFile] = {
    Option(snapshots.firstEntry()).map(_.getValue)
  }

  private def latestSnapshotFile: Option[SnapshotFile] = {
    Option(snapshots.lastEntry()).map(_.getValue)
  }

  /**
   * Removes the producer state snapshot file metadata corresponding to the provided offset if it exists from this
   * ProducerStateManager, and deletes the backing snapshot file.
   */
  private[log] def removeAndDeleteSnapshot(snapshotOffset: Long): Unit = {
    Option(snapshots.remove(snapshotOffset)).foreach(_.deleteIfExists())
  }
}

case class SnapshotFile private[log] (private var _file: File,
                                      offset: Long) {
  def deleteIfExists(): Boolean = {
    Files.deleteIfExists(file.toPath)
  }

  def updateParentDir(parentDir: File): Unit = {
    _file = new File(parentDir, _file.getName)
  }

  def file: File = {
    _file
  }
}

object SnapshotFile {
  def apply(file: File): SnapshotFile = {
    val offset = offsetFromFile(file)
    SnapshotFile(file, offset)
  }
}
