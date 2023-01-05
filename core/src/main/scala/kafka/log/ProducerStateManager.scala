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

import kafka.server.{BrokerReconfigurable, KafkaConfig}
import kafka.utils.{Logging, nonthreadsafe, threadsafe}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.protocol.types._
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.utils.{ByteUtils, Crc32C, Time}
import org.apache.kafka.server.log.internals._

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, NoSuchFileException, StandardOpenOption}
import java.util.OptionalLong
import java.util.concurrent.ConcurrentSkipListMap
import scala.collection.{immutable, mutable}
import scala.jdk.CollectionConverters._









object ProducerStateManager {
  val LateTransactionBufferMs = 5 * 60 * 1000

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
        val batchMetadata = new java.util.ArrayDeque[BatchMetadata]
        if (offset >= 0)
          batchMetadata.add(new BatchMetadata(seq, offset, offsetDelta, timestamp))

        val currentTxnFirstOffsetValue = if (currentTxnFirstOffset >= 0) OptionalLong.of(currentTxnFirstOffset) else OptionalLong.empty()
        new ProducerStateEntry(producerId, batchMetadata, producerEpoch,
          coordinatorEpoch, timestamp, currentTxnFirstOffsetValue)
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
          .set(CurrentTxnFirstOffsetField, entry.currentTxnFirstOffset.orElse(-1L))
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

  private def isSnapshotFile(file: File): Boolean = file.getName.endsWith(UnifiedLog.ProducerSnapshotFileSuffix)

  // visible for testing
  private[log] def listSnapshotFiles(dir: File): Seq[SnapshotFile] = {
    if (dir.exists && dir.isDirectory) {
      Option(dir.listFiles).map { files =>
        files.filter(f => f.isFile && isSnapshotFile(f)).map(new SnapshotFile(_)).toSeq
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
class ProducerStateManager(
  val topicPartition: TopicPartition,
  @volatile var _logDir: File,
  val maxTransactionTimeoutMs: Int,
  val producerStateManagerConfig: ProducerStateManagerConfig,
  val time: Time
) extends Logging {
  import ProducerStateManager._
  import java.util

  this.logIdent = s"[ProducerStateManager partition=$topicPartition] "

  private var snapshots: ConcurrentSkipListMap[java.lang.Long, SnapshotFile] = locally {
    loadSnapshots()
  }

  private val producers = mutable.Map.empty[Long, ProducerStateEntry]
  private var lastMapOffset = 0L
  private var lastSnapOffset = 0L

  // Keep track of the last timestamp from the oldest transaction. This is used
  // to detect (approximately) when a transaction has been left hanging on a partition.
  // We make the field volatile so that it can be safely accessed without a lock.
  @volatile private var oldestTxnLastTimestamp: Long = -1L

  // ongoing transactions sorted by the first offset of the transaction
  private val ongoingTxns = new util.TreeMap[Long, TxnMetadata]

  // completed transactions whose markers are at offsets above the high watermark
  private val unreplicatedTxns = new util.TreeMap[Long, TxnMetadata]

  @threadsafe
  def hasLateTransaction(currentTimeMs: Long): Boolean = {
    val lastTimestamp = oldestTxnLastTimestamp
    lastTimestamp > 0 && (currentTimeMs - lastTimestamp) > maxTransactionTimeoutMs + ProducerStateManager.LateTransactionBufferMs
  }

  def truncateFullyAndReloadSnapshots(): Unit = {
    info("Reloading the producer state snapshots")
    truncateFullyAndStartAt(0L)
    snapshots = loadSnapshots()
  }

  /**
   * Load producer state snapshots by scanning the _logDir.
   */
  private def loadSnapshots(): ConcurrentSkipListMap[java.lang.Long, SnapshotFile] = {
    val offsetToSnapshots = new ConcurrentSkipListMap[java.lang.Long, SnapshotFile]()
    for (snapshotFile <- listSnapshotFiles(_logDir)) {
      offsetToSnapshots.put(snapshotFile.offset, snapshotFile)
    }
    offsetToSnapshots
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
            updateOldestTxnTimestamp()
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
    entry.currentTxnFirstOffset.ifPresent((offset: Long) => ongoingTxns.put(offset, new TxnMetadata(producerId, offset)))
  }

  private def isProducerExpired(currentTimeMs: Long, producerState: ProducerStateEntry): Boolean =
    !producerState.currentTxnFirstOffset.isPresent && currentTimeMs - producerState.lastTimestamp >= producerStateManagerConfig.producerIdExpirationMs

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
      updateOldestTxnTimestamp()

      // since we assume that the offset is less than or equal to the high watermark, it is
      // safe to clear the unreplicated transactions
      unreplicatedTxns.clear()
      loadFromSnapshot(logStartOffset, currentTimeMs)
    } else {
      onLogStartOffsetIncremented(logStartOffset)
    }
  }

  def prepareUpdate(producerId: Long, origin: AppendOrigin): ProducerAppendInfo = {
    val currentEntry = lastEntry(producerId).getOrElse(new ProducerStateEntry(producerId))
    new ProducerAppendInfo(topicPartition, producerId, currentEntry, origin)
  }

  /**
   * Update the mapping with the given append information
   */
  def update(appendInfo: ProducerAppendInfo): Unit = {
    if (appendInfo.producerId() == RecordBatch.NO_PRODUCER_ID)
      throw new IllegalArgumentException(s"Invalid producer id ${appendInfo.producerId()} passed to update " +
        s"for partition $topicPartition")

    trace(s"Updated producer ${appendInfo.producerId} state to $appendInfo")
    val updatedEntry = appendInfo.toEntry
    producers.get(appendInfo.producerId) match {
      case Some(currentEntry) =>
        currentEntry.update(updatedEntry)

      case None =>
        producers.put(appendInfo.producerId, updatedEntry)
    }

    appendInfo.startedTransactions.asScala.foreach { txn =>
      ongoingTxns.put(txn.firstOffset.messageOffset, txn)
    }

    updateOldestTxnTimestamp()
  }

  private def updateOldestTxnTimestamp(): Unit = {
    val firstEntry = ongoingTxns.firstEntry()
    if (firstEntry == null) {
      oldestTxnLastTimestamp = -1
    } else {
      val oldestTxnMetadata = firstEntry.getValue
      oldestTxnLastTimestamp = producers.get(oldestTxnMetadata.producerId)
        .map(_.lastTimestamp)
        .getOrElse(-1L)
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
      val snapshotFile = new SnapshotFile(UnifiedLog.producerSnapshotFile(_logDir, lastMapOffset))
      val start = time.hiResClockMs()
      writeSnapshot(snapshotFile.file, producers)
      info(s"Wrote producer snapshot at offset $lastMapOffset with ${producers.size} producer ids in ${time.hiResClockMs() - start} ms.")

      snapshots.put(snapshotFile.offset, snapshotFile)

      // Update the last snap offset according to the serialized map
      lastSnapOffset = lastMapOffset
    }
  }

  /**
   * Update the parentDir for this ProducerStateManager and all of the snapshot files which it manages.
   */
  def updateParentDir(parentDir: File): Unit = {
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
   * Visible for testing
   */
  private[log] def snapshotFileForOffset(offset: Long): Option[SnapshotFile] = {
    Option(snapshots.get(offset))
  }

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
      if (lastOffset.isPresent && lastOffset.getAsLong < offset)
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
    updateOldestTxnTimestamp()
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

    txnMetadata.lastOffset = OptionalLong.of(completedTxn.lastOffset)
    unreplicatedTxns.put(completedTxn.firstOffset, txnMetadata)
    updateOldestTxnTimestamp()
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
  private def removeAndDeleteSnapshot(snapshotOffset: Long): Unit = {
    Option(snapshots.remove(snapshotOffset)).foreach(_.deleteIfExists())
  }

  /**
   * Removes the producer state snapshot file metadata corresponding to the provided offset if it exists from this
   * ProducerStateManager, and renames the backing snapshot file to have the Log.DeletionSuffix.
   *
   * Note: This method is safe to use with async deletes. If a race occurs and the snapshot file
   *       is deleted without this ProducerStateManager instance knowing, the resulting exception on
   *       SnapshotFile rename will be ignored and None will be returned.
   */
  private[log] def removeAndMarkSnapshotForDeletion(snapshotOffset: Long): Option[SnapshotFile] = {
    Option(snapshots.remove(snapshotOffset)).flatMap { snapshot => {
      // If the file cannot be renamed, it likely means that the file was deleted already.
      // This can happen due to the way we construct an intermediate producer state manager
      // during log recovery, and use it to issue deletions prior to creating the "real"
      // producer state manager.
      //
      // In any case, removeAndMarkSnapshotForDeletion is intended to be used for snapshot file
      // deletion, so ignoring the exception here just means that the intended operation was
      // already completed.
      try {
        snapshot.renameTo(UnifiedLog.DeletedFileSuffix)
        Some(snapshot)
      } catch {
        case _: NoSuchFileException =>
          info(s"Failed to rename producer state snapshot ${snapshot.file.getAbsoluteFile} with deletion suffix because it was already deleted")
          None
      }
    }
    }
  }
}





class ProducerStateManagerConfig(@volatile var producerIdExpirationMs: Int) extends Logging with BrokerReconfigurable {

  override def reconfigurableConfigs: Set[String] = ProducerStateManagerConfig.ReconfigurableConfigs

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    if (producerIdExpirationMs != newConfig.producerIdExpirationMs) {
      info(s"Reconfigure ${KafkaConfig.ProducerIdExpirationMsProp} from $producerIdExpirationMs to ${newConfig.producerIdExpirationMs}")
      producerIdExpirationMs = newConfig.producerIdExpirationMs
    }
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    if (newConfig.producerIdExpirationMs < 0)
      throw new ConfigException(s"${KafkaConfig.ProducerIdExpirationMsProp} cannot be less than 0, current value is $producerIdExpirationMs")
  }
}

object ProducerStateManagerConfig {
  val ReconfigurableConfigs = Set(KafkaConfig.ProducerIdExpirationMsProp)
}
