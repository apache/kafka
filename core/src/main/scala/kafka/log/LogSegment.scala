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
package kafka.log

import java.io.{File, IOException}
import java.nio.file.{Files, NoSuchFileException, Path}
import java.nio.file.attribute.FileTime
import java.util
import java.util.Comparator
import java.util.concurrent.TimeUnit

import kafka.common.{KafkaException, LogSegmentOffsetOverflowException}
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.{FetchDataInfo, LogOffsetMetadata}
import kafka.utils._
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.record.FileRecords.{LogOffsetPosition, TimestampAndOffset}
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.math._
import scala.util.Random

/**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileRecords containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 *
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 *
 * @param log The file records containing log entries
 * @param lazyOffsetIndex The offset index
 * @param lazyTimeIndex The timestamp index
 * @param txnIndex The transaction index
 * @param baseOffset A lower bound on the offsets in this segment
 * @param indexIntervalBytes The approximate number of bytes between entries in the index
 * @param rollJitterMs The maximum random jitter subtracted from the scheduled segment roll time
 * @param time The time instance
 */
@nonthreadsafe
class LogSegment private[log] (val log: FileRecords,
                               val lazyOffsetIndex: LazyIndex[OffsetIndex],
                               val lazyTimeIndex: LazyIndex[TimeIndex],
                               val txnIndex: TransactionIndex,
                               val baseOffset: Long,
                               val indexIntervalBytes: Int,
                               val rollJitterMs: Long,
                               val time: Time, dir: File) extends Logging {

  def offsetIndex: OffsetIndex = lazyOffsetIndex.get

  def timeIndex: TimeIndex = lazyTimeIndex.get

  def segDir: File = dir

  def shouldRoll(rollParams: RollParams): Boolean = {
    val reachedRollMs = timeWaitedForRoll(rollParams.now, rollParams.maxTimestampInMessages) > rollParams.maxSegmentMs - rollJitterMs
    size > rollParams.maxSegmentBytes - rollParams.messagesSize ||
      (size > 0 && reachedRollMs) ||
      offsetIndex.isFull || timeIndex.isFull || !canConvertToRelativeOffset(rollParams.maxOffsetInMessages)
  }

  def resizeIndexes(size: Int): Unit = {
    offsetIndex.resize(size)
    timeIndex.resize(size)
  }

  def sanityCheck(timeIndexFileNewlyCreated: Boolean): Unit = {
    if (lazyOffsetIndex.file.exists) {
      // Resize the time index file to 0 if it is newly created.
      if (timeIndexFileNewlyCreated)
        timeIndex.resize(0)
      // Sanity checks for time index and offset index are skipped because
      // we will recover the segments above the recovery point in recoverLog()
      // in any case so sanity checking them here is redundant.
      txnIndex.sanityCheck()
    }
    else throw new NoSuchFileException(s"Offset index file ${lazyOffsetIndex.file.getAbsolutePath} does not exist")
  }

  private var created = time.milliseconds

  /* the number of bytes since we last added an entry in the offset index */
  private var bytesSinceLastIndexEntry = 0

  // The timestamp we used for time based log rolling and for ensuring max compaction delay
  // volatile for LogCleaner to see the update
  @volatile private var rollingBasedTimestamp: Option[Long] = None

  /* The maximum timestamp we see so far */
  @volatile private var _maxTimestampSoFar: Option[Long] = None
  def maxTimestampSoFar_=(timestamp: Long): Unit = _maxTimestampSoFar = Some(timestamp)
  def maxTimestampSoFar: Long = {
    if (_maxTimestampSoFar.isEmpty)
      _maxTimestampSoFar = Some(timeIndex.lastEntry.timestamp)
    _maxTimestampSoFar.get
  }

  @volatile private var _offsetOfMaxTimestampSoFar: Option[Long] = None
  def offsetOfMaxTimestampSoFar_=(offset: Long): Unit = _offsetOfMaxTimestampSoFar = Some(offset)
  def offsetOfMaxTimestampSoFar: Long = {
    if (_offsetOfMaxTimestampSoFar.isEmpty)
      _offsetOfMaxTimestampSoFar = Some(timeIndex.lastEntry.offset)
    _offsetOfMaxTimestampSoFar.get
  }

  /* Return the size in bytes of this log segment */
  def size: Int = log.sizeInBytes()

  /**
   * checks that the argument offset can be represented as an integer offset relative to the baseOffset.
   */
  def canConvertToRelativeOffset(offset: Long): Boolean = {
    offsetIndex.canAppendOffset(offset)
  }

  /**
   * Append the given messages starting with the given offset. Add
   * an entry to the index if needed.
   *
   * It is assumed this method is being called from within a lock.
   *
   * @param largestOffset The last offset in the message set
   * @param largestTimestamp The largest timestamp in the message set.
   * @param shallowOffsetOfMaxTimestamp The offset of the message that has the largest timestamp in the messages to append.
   * @param records The log entries to append.
   * @return the physical position in the file of the appended records
   * @throws LogSegmentOffsetOverflowException if the largest offset causes index offset overflow
   */
  @nonthreadsafe
  def append(largestOffset: Long,
             largestTimestamp: Long,
             shallowOffsetOfMaxTimestamp: Long,
             records: MemoryRecords): Unit = {
    if (records.sizeInBytes > 0) {
      trace(s"Inserting ${records.sizeInBytes} bytes at end offset $largestOffset at position ${log.sizeInBytes} " +
            s"with largest timestamp $largestTimestamp at shallow offset $shallowOffsetOfMaxTimestamp")
      val physicalPosition = log.sizeInBytes()
      if (physicalPosition == 0)
        rollingBasedTimestamp = Some(largestTimestamp)

      ensureOffsetInRange(largestOffset)

      // append the messages
      val appendedBytes = log.append(records)
      trace(s"Appended $appendedBytes to ${log.file} at end offset $largestOffset")
      // Update the in memory max timestamp and corresponding offset.
      if (largestTimestamp > maxTimestampSoFar) {
        maxTimestampSoFar = largestTimestamp
        offsetOfMaxTimestampSoFar = shallowOffsetOfMaxTimestamp
      }
      // append an entry to the index (if needed)
      if (bytesSinceLastIndexEntry > indexIntervalBytes) {
        offsetIndex.append(largestOffset, physicalPosition)
        timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar)
        bytesSinceLastIndexEntry = 0
      }
      bytesSinceLastIndexEntry += records.sizeInBytes
    }
  }

  private def ensureOffsetInRange(offset: Long): Unit = {
    if (!canConvertToRelativeOffset(offset))
      throw new LogSegmentOffsetOverflowException(this, offset)
  }

  private def appendChunkFromFile(records: FileRecords, position: Int, bufferSupplier: BufferSupplier): Int = {
    var bytesToAppend = 0
    var maxTimestamp = Long.MinValue
    var offsetOfMaxTimestamp = Long.MinValue
    var maxOffset = Long.MinValue
    var readBuffer = bufferSupplier.get(1024 * 1024)

    def canAppend(batch: RecordBatch) =
      canConvertToRelativeOffset(batch.lastOffset) &&
        (bytesToAppend == 0 || bytesToAppend + batch.sizeInBytes < readBuffer.capacity)

    // find all batches that are valid to be appended to the current log segment and
    // determine the maximum offset and timestamp
    val nextBatches = records.batchesFrom(position).asScala.iterator
    for (batch <- nextBatches.takeWhile(canAppend)) {
      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp
        offsetOfMaxTimestamp = batch.lastOffset
      }
      maxOffset = batch.lastOffset
      bytesToAppend += batch.sizeInBytes
    }

    if (bytesToAppend > 0) {
      // Grow buffer if needed to ensure we copy at least one batch
      if (readBuffer.capacity < bytesToAppend)
        readBuffer = bufferSupplier.get(bytesToAppend)

      readBuffer.limit(bytesToAppend)
      records.readInto(readBuffer, position)

      append(maxOffset, maxTimestamp, offsetOfMaxTimestamp, MemoryRecords.readableRecords(readBuffer))
    }

    bufferSupplier.release(readBuffer)
    bytesToAppend
  }

  /**
   * Append records from a file beginning at the given position until either the end of the file
   * is reached or an offset is found which is too large to convert to a relative offset for the indexes.
   *
   * @return the number of bytes appended to the log (may be less than the size of the input if an
   *         offset is encountered which would overflow this segment)
   */
  def appendFromFile(records: FileRecords, start: Int): Int = {
    var position = start
    val bufferSupplier: BufferSupplier = new BufferSupplier.GrowableBufferSupplier
    while (position < start + records.sizeInBytes) {
      val bytesAppended = appendChunkFromFile(records, position, bufferSupplier)
      if (bytesAppended == 0)
        return position - start
      position += bytesAppended
    }
    position - start
  }

  @nonthreadsafe
  def updateTxnIndex(completedTxn: CompletedTxn, lastStableOffset: Long): Unit = {
    if (completedTxn.isAborted) {
      trace(s"Writing aborted transaction $completedTxn to transaction index, last stable offset is $lastStableOffset")
      txnIndex.append(new AbortedTxn(completedTxn, lastStableOffset))
    }
  }

  private def updateProducerState(producerStateManager: ProducerStateManager, batch: RecordBatch): Unit = {
    if (batch.hasProducerId) {
      val producerId = batch.producerId
      val appendInfo = producerStateManager.prepareUpdate(producerId, origin = AppendOrigin.Replication)
      val maybeCompletedTxn = appendInfo.append(batch, firstOffsetMetadataOpt = None)
      producerStateManager.update(appendInfo)
      maybeCompletedTxn.foreach { completedTxn =>
        val lastStableOffset = producerStateManager.lastStableOffset(completedTxn)
        updateTxnIndex(completedTxn, lastStableOffset)
        producerStateManager.completeTxn(completedTxn)
      }
    }
    producerStateManager.updateMapEndOffset(batch.lastOffset + 1)
  }

  /**
   * Find the physical file position for the first message with offset >= the requested offset.
   *
   * The startingFilePosition argument is an optimization that can be used if we already know a valid starting position
   * in the file higher than the greatest-lower-bound from the index.
   *
   * @param offset The offset we want to translate
   * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
   * when omitted, the search will begin at the position in the offset index.
   * @return The position in the log storing the message with the least offset >= the requested offset and the size of the
    *        message or null if no message meets this criteria.
   */
  @threadsafe
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): LogOffsetPosition = {
    val mapping = offsetIndex.lookup(offset)
    log.searchForOffsetWithSize(offset, max(mapping.position, startingFilePosition))
  }

  /**
   * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
   * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
   *
   * @param startOffset A lower bound on the first offset to include in the message set we read
   * @param maxSize The maximum number of bytes to include in the message set we read
   * @param maxPosition The maximum position in the log segment that should be exposed for read
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxSize` (if one exists)
   *
   * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
   *         or null if the startOffset is larger than the largest offset in this log
   */
  @threadsafe
  def read(startOffset: Long,
           maxSize: Int,
           maxPosition: Long = size,
           minOneMessage: Boolean = false): FetchDataInfo = {
    if (maxSize < 0)
      throw new IllegalArgumentException(s"Invalid max size $maxSize for log read from segment $log")

    val startOffsetAndSize = translateOffset(startOffset)

    // if the start position is already off the end of the log, return null
    if (startOffsetAndSize == null)
      return null

    val startPosition = startOffsetAndSize.position
    val offsetMetadata = LogOffsetMetadata(startOffset, this.baseOffset, startPosition)

    val adjustedMaxSize =
      if (minOneMessage) math.max(maxSize, startOffsetAndSize.size)
      else maxSize

    // return a log segment but with zero size in the case below
    if (adjustedMaxSize == 0)
      return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY)

    // calculate the length of the message set to read based on whether or not they gave us a maxOffset
    val fetchSize: Int = min((maxPosition - startPosition).toInt, adjustedMaxSize)

    FetchDataInfo(offsetMetadata, log.slice(startPosition, fetchSize),
      firstEntryIncomplete = adjustedMaxSize < startOffsetAndSize.size)
  }

   def fetchUpperBoundOffset(startOffsetPosition: OffsetPosition, fetchSize: Int): Option[Long] =
     offsetIndex.fetchUpperBoundOffset(startOffsetPosition, fetchSize).map(_.offset)

  /**
   * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes
   * from the end of the log and index.
   *
   * @param producerStateManager Producer state corresponding to the segment's base offset. This is needed to recover
   *                             the transaction index.
   * @param leaderEpochCache Optionally a cache for updating the leader epoch during recovery.
   * @return The number of bytes truncated from the log
   * @throws LogSegmentOffsetOverflowException if the log segment contains an offset that causes the index offset to overflow
   */
  @nonthreadsafe
  def recover(producerStateManager: ProducerStateManager, leaderEpochCache: Option[LeaderEpochFileCache] = None): Int = {
    offsetIndex.reset()
    timeIndex.reset()
    txnIndex.reset()
    var validBytes = 0
    var lastIndexEntry = 0
    maxTimestampSoFar = RecordBatch.NO_TIMESTAMP
    try {
      for (batch <- log.batches.asScala) {
        batch.ensureValid()
        ensureOffsetInRange(batch.lastOffset)

        // The max timestamp is exposed at the batch level, so no need to iterate the records
        if (batch.maxTimestamp > maxTimestampSoFar) {
          maxTimestampSoFar = batch.maxTimestamp
          offsetOfMaxTimestampSoFar = batch.lastOffset
        }

        // Build offset index
        if (validBytes - lastIndexEntry > indexIntervalBytes) {
          offsetIndex.append(batch.lastOffset, validBytes)
          timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar)
          lastIndexEntry = validBytes
        }
        validBytes += batch.sizeInBytes()

        if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
          leaderEpochCache.foreach { cache =>
            if (batch.partitionLeaderEpoch > 0 && cache.latestEpoch.forall(batch.partitionLeaderEpoch > _))
              cache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
          }
          updateProducerState(producerStateManager, batch)
        }
      }
    } catch {
      case e@ (_: CorruptRecordException | _: InvalidRecordException) =>
        warn("Found invalid messages in log segment %s at byte offset %d: %s. %s"
          .format(log.file.getAbsolutePath, validBytes, e.getMessage, e.getCause))
    }
    val truncated = log.sizeInBytes - validBytes
    if (truncated > 0)
      debug(s"Truncated $truncated invalid bytes at the end of segment ${log.file.getAbsoluteFile} during recovery")

    log.truncateTo(validBytes)
    offsetIndex.trimToValidSize()
    // A normally closed segment always appends the biggest timestamp ever seen into log segment, we do this as well.
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, skipFullCheck = true)
    timeIndex.trimToValidSize()
    truncated
  }

  private def loadLargestTimestamp(): Unit = {
    // Get the last time index entry. If the time index is empty, it will return (-1, baseOffset)
    val lastTimeIndexEntry = timeIndex.lastEntry
    maxTimestampSoFar = lastTimeIndexEntry.timestamp
    offsetOfMaxTimestampSoFar = lastTimeIndexEntry.offset

    val offsetPosition = offsetIndex.lookup(lastTimeIndexEntry.offset)
    // Scan the rest of the messages to see if there is a larger timestamp after the last time index entry.
    val maxTimestampOffsetAfterLastEntry = log.largestTimestampAfter(offsetPosition.position)
    if (maxTimestampOffsetAfterLastEntry.timestamp > lastTimeIndexEntry.timestamp) {
      maxTimestampSoFar = maxTimestampOffsetAfterLastEntry.timestamp
      offsetOfMaxTimestampSoFar = maxTimestampOffsetAfterLastEntry.offset
    }
  }

  /**
   * Check whether the last offset of the last batch in this segment overflows the indexes.
   */
  def hasOverflow: Boolean = {
    val nextOffset = readNextOffset
    nextOffset > baseOffset && !canConvertToRelativeOffset(nextOffset - 1)
  }

  def collectAbortedTxns(fetchOffset: Long, upperBoundOffset: Long): TxnIndexSearchResult =
    txnIndex.collectAbortedTxns(fetchOffset, upperBoundOffset)

  override def toString: String = "LogSegment(baseOffset=" + baseOffset +
    ", size=" + size +
    ", lastModifiedTime=" + lastModified +
    ", largestTime=" + largestTimestamp +
    ")"

  /**
   * Truncate off all index and log entries with offsets >= the given offset.
   * If the given offset is larger than the largest message in this segment, do nothing.
   *
   * @param offset The offset to truncate to
   * @return The number of log bytes truncated
   */
  @nonthreadsafe
  def truncateTo(offset: Long): Int = {
    // Do offset translation before truncating the index to avoid needless scanning
    // in case we truncate the full index
    val mapping = translateOffset(offset)
    offsetIndex.truncateTo(offset)
    timeIndex.truncateTo(offset)
    txnIndex.truncateTo(offset)

    // After truncation, reset and allocate more space for the (new currently active) index
    offsetIndex.resize(offsetIndex.maxIndexSize)
    timeIndex.resize(timeIndex.maxIndexSize)

    val bytesTruncated = if (mapping == null) 0 else log.truncateTo(mapping.position)
    if (log.sizeInBytes == 0) {
      created = time.milliseconds
      rollingBasedTimestamp = None
    }

    bytesSinceLastIndexEntry = 0
    if (maxTimestampSoFar >= 0)
      loadLargestTimestamp()
    bytesTruncated
  }

  /**
   * Calculate the offset that would be used for the next message to be append to this segment.
   * Note that this is expensive.
   */
  @threadsafe
  def readNextOffset: Long = {
    val fetchData = read(offsetIndex.lastOffset, log.sizeInBytes)
    if (fetchData == null)
      baseOffset
    else
      fetchData.records.batches.asScala.lastOption
        .map(_.nextOffset)
        .getOrElse(baseOffset)
  }

  /**
   * Flush this log segment to disk
   */
  @threadsafe
  def flush(): Unit = {
    LogFlushStats.logFlushTimer.time {
      log.flush()
      offsetIndex.flush()
      timeIndex.flush()
      txnIndex.flush()
    }
  }

  /**
   * Update the directory reference for the log and indices in this segment. This would typically be called after a
   * directory is renamed.
   */
  def updateDir(dir: File): Unit = {
    log.setFile(new File(dir, log.file.getName))
    lazyOffsetIndex.file = new File(dir, lazyOffsetIndex.file.getName)
    lazyTimeIndex.file = new File(dir, lazyTimeIndex.file.getName)
    txnIndex.file = new File(dir, txnIndex.file.getName)
  }

  /**
   * Get the status for the for this log segment
   * IOException from this method should be handled by the caller
   */
  def getSegmentStatus(): SegmentStatus = {
    SegmentStatusHandler.getStatus(new File(segDir, SegmentFile.STATUS.getName))
  }


  /**
   * Change the status for the for this log segment
   * IOException from this method should be handled by the caller
   */
  def changeSegmentStatus(segmentStatus: SegmentStatus): Unit = {
    changeSegmentStatus(SegmentStatus.HOT, segmentStatus)
  }

  /**
   * Change the status for the for this log segment
   * IOException from this method should be handled by the caller
   */
  def changeSegmentStatus(oldStatus: SegmentStatus, newStatus: SegmentStatus): Unit = {
    val file = new File(segDir, SegmentFile.STATUS.getName)
    val segStatus = SegmentStatusHandler.getStatus(file)
    if(segStatus == oldStatus){
      SegmentStatusHandler.setStatus(file, newStatus)
    }else{
      throw new KafkaException(s"Invalid Segment Status $segStatus, expected $oldStatus")
    }
  }

  /**
   * Change the suffix for the index and log file for this log segment
   * IOException from this method should be handled by the caller
   */
  def changeFileSuffixes(oldSuffix: String, newSuffix: String): Unit = {
    log.renameTo(new File(CoreUtils.replaceSuffix(log.file.getPath, oldSuffix, newSuffix)))
    offsetIndex.renameTo(new File(CoreUtils.replaceSuffix(lazyOffsetIndex.file.getPath, oldSuffix, newSuffix)))
    timeIndex.renameTo(new File(CoreUtils.replaceSuffix(lazyTimeIndex.file.getPath, oldSuffix, newSuffix)))
    txnIndex.renameTo(new File(CoreUtils.replaceSuffix(txnIndex.file.getPath, oldSuffix, newSuffix)))
  }

  /**
   * Append the largest time index entry to the time index and trim the log and indexes.
   *
   * The time index entry appended will be used to decide when to delete the segment.
   */
  def onBecomeInactiveSegment(): Unit = {
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, skipFullCheck = true)
    offsetIndex.trimToValidSize()
    timeIndex.trimToValidSize()
    log.trim()
  }

  /**
    * If not previously loaded,
    * load the timestamp of the first message into memory.
    */
  private def loadFirstBatchTimestamp(): Unit = {
    if (rollingBasedTimestamp.isEmpty) {
      val iter = log.batches.iterator()
      if (iter.hasNext)
        rollingBasedTimestamp = Some(iter.next().maxTimestamp)
    }
  }

  /**
   * The time this segment has waited to be rolled.
   * If the first message batch has a timestamp we use its timestamp to determine when to roll a segment. A segment
   * is rolled if the difference between the new batch's timestamp and the first batch's timestamp exceeds the
   * segment rolling time.
   * If the first batch does not have a timestamp, we use the wall clock time to determine when to roll a segment. A
   * segment is rolled if the difference between the current wall clock time and the segment create time exceeds the
   * segment rolling time.
   */
  def timeWaitedForRoll(now: Long, messageTimestamp: Long) : Long = {
    // Load the timestamp of the first message into memory
    loadFirstBatchTimestamp()
    rollingBasedTimestamp match {
      case Some(t) if t >= 0 => messageTimestamp - t
      case _ => now - created
    }
  }

  /**
    * @return the first batch timestamp if the timestamp is available. Otherwise return Long.MaxValue
    */
  def getFirstBatchTimestamp() : Long = {
    loadFirstBatchTimestamp()
    rollingBasedTimestamp match {
      case Some(t) if t >= 0 => t
      case _ => Long.MaxValue
    }
  }

  /**
   * Search the message offset based on timestamp and offset.
   *
   * This method returns an option of TimestampOffset. The returned value is determined using the following ordered list of rules:
   *
   * - If all the messages in the segment have smaller offsets, return None
   * - If all the messages in the segment have smaller timestamps, return None
   * - If all the messages in the segment have larger timestamps, or no message in the segment has a timestamp
   *   the returned the offset will be max(the base offset of the segment, startingOffset) and the timestamp will be Message.NoTimestamp.
   * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
   *   is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
   *
   * This methods only returns None when 1) all messages' offset < startOffing or 2) the log is not empty but we did not
   * see any message when scanning the log from the indexed position. The latter could happen if the log is truncated
   * after we get the indexed position but before we scan the log from there. In this case we simply return None and the
   * caller will need to check on the truncated log and maybe retry or even do the search on another log segment.
   *
   * @param timestamp The timestamp to search for.
   * @param startingOffset The starting offset to search.
   * @return the timestamp and offset of the first message that meets the requirements. None will be returned if there is no such message.
   */
  def findOffsetByTimestamp(timestamp: Long, startingOffset: Long = baseOffset): Option[TimestampAndOffset] = {
    // Get the index entry with a timestamp less than or equal to the target timestamp
    val timestampOffset = timeIndex.lookup(timestamp)
    val position = offsetIndex.lookup(math.max(timestampOffset.offset, startingOffset)).position

    // Search the timestamp
    Option(log.searchForTimestamp(timestamp, position, startingOffset))
  }

  /**
   * Close this log segment
   */
  def close(): Unit = {
    CoreUtils.swallow(timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, skipFullCheck = true), this)
    CoreUtils.swallow(offsetIndex.close(), this)
    CoreUtils.swallow(timeIndex.close(), this)
    CoreUtils.swallow(log.close(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  /**
    * Close file handlers used by the log segment but don't write to disk. This is used when the disk may have failed
    */
  def closeHandlers(): Unit = {
    CoreUtils.swallow(offsetIndex.closeHandler(), this)
    CoreUtils.swallow(timeIndex.closeHandler(), this)
    CoreUtils.swallow(log.closeHandlers(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  /**
   * Delete this log segment from the filesystem.
   */
  def deleteIfExists(): Unit = {
    def delete(delete: () => Boolean, fileType: String, file: File, logIfMissing: Boolean): Unit = {
      try {
        if (delete())
          info(s"Deleted $fileType ${file.getAbsolutePath}.")
        else if (logIfMissing)
          info(s"Failed to delete $fileType ${file.getAbsolutePath} because it does not exist.")
      }
      catch {
        case e: IOException => throw new IOException(s"Delete of $fileType ${file.getAbsolutePath} failed.", e)
      }
    }

    CoreUtils.tryAll(Seq(
      () => delete(log.deleteIfExists _, "log", log.file, logIfMissing = true),
      () => delete(offsetIndex.deleteIfExists _, "offset index", lazyOffsetIndex.file, logIfMissing = true),
      () => delete(timeIndex.deleteIfExists _, "time index", lazyTimeIndex.file, logIfMissing = true),
      () => delete(txnIndex.deleteIfExists _, "transaction index", txnIndex.file, logIfMissing = false)
    ))
  }

  /**
   * The last modified time of this log segment as a unix time stamp
   */
  def lastModified = log.file.lastModified

  /**
   * The largest timestamp this segment contains.
   */
  def largestTimestamp = if (maxTimestampSoFar >= 0) maxTimestampSoFar else lastModified

  /**
   * Change the last modified time for this log segment
   */
  def lastModified_=(ms: Long) = {
    val fileTime = FileTime.fromMillis(ms)
    Files.setLastModifiedTime(log.file.toPath, fileTime)
    Files.setLastModifiedTime(lazyOffsetIndex.file.toPath, fileTime)
    Files.setLastModifiedTime(lazyTimeIndex.file.toPath, fileTime)
  }

}

object LogSegment {

  private val random = scala.util.Random
  def open(segDir: File, baseOffset: Long, config: LogConfig, time: Time, fileAlreadyExists: Boolean = false,
           initFileSize: Int = 0, preallocate: Boolean = false, segmentStatus: SegmentStatus = SegmentStatus.HOT): LogSegment = {
    val maxIndexSize = config.maxIndexSize
    if(!fileAlreadyExists){
      segDir.mkdirs()
      SegmentStatusHandler.setStatus(new File(segDir, SegmentFile.STATUS.getName), segmentStatus)
    }
    new LogSegment(
      FileRecords.open(new File(segDir, SegmentFile.LOG.getName), fileAlreadyExists, initFileSize, preallocate),
      LazyIndex.forOffset(new File(segDir, SegmentFile.OFFSET_INDEX.getName), baseOffset = baseOffset, maxIndexSize = maxIndexSize),
      LazyIndex.forTime(new File(segDir, SegmentFile.TIME_INDEX.getName), baseOffset = baseOffset, maxIndexSize = maxIndexSize),
      new TransactionIndex(baseOffset, new File(segDir, SegmentFile.TXN_INDEX.getName)),
      baseOffset,
      indexIntervalBytes = config.indexInterval,
      rollJitterMs = config.randomSegmentJitter,
      time, segDir)
  }

  def isSegmentDir(file: File): Boolean = {
    file.isDirectory && !file.getName.equals("snapshot")
  }

  def getCleanedSegmentFiles(logDir: File, offset: Long): Array[File] = {
    val fNameDir = String.valueOf(offset)
    val fNameDyn = fNameDir+"-"
    logDir.listFiles( file => (file.getName.startsWith(fNameDyn) || file.getName.equals(fNameDir)) && getStatus(file) == SegmentStatus.CLEANED)
  }

  def getSegmentDir(logDir: File, baseOffset: Long, randomDigits: Boolean = false): File = {
    new File(logDir, if(randomDigits) baseOffset+"-"+random.nextInt(100000) else String.valueOf(baseOffset))
  }

  def getSegmentOffset(segDir: File): Long ={
    val name = segDir.getName
    val index = name.indexOf("-")
    if(index > -1){
      name.substring(0, index).toLong
    }else{
      name.toLong
    }
  }

  def deleteIfExists(segDir: File): Unit = {
    if(segDir.exists()){
      val files = segDir.listFiles()
      if(files != null){
        files.foreach( file => Files.deleteIfExists(file.toPath))
      }
      Files.deleteIfExists(segDir.toPath)
    }
  }

  def deleteIndicesIfExist(segDir: File): Unit = {
    Files.deleteIfExists(new File(segDir, SegmentFile.OFFSET_INDEX.getName).toPath)
    Files.deleteIfExists(new File(segDir, SegmentFile.TIME_INDEX.getName).toPath)
    Files.deleteIfExists(new File(segDir, SegmentFile.TXN_INDEX.getName).toPath)
  }

  def getSnapshotDir(logDir: File): File = {
    new File(logDir, "snapshot")
  }

  def getSnapshotFile(logDir: File, baseOffset: Long): File = {
    val segDir = new File(logDir, "snapshot")
    new File(segDir, String.valueOf(baseOffset))
  }

  def getSnapshotOffset(snapshot : File): Long = {
      snapshot.getName.toLong
  }

  def getStatus(segDir: File): SegmentStatus = {
    val statusFile = new File(segDir, SegmentFile.STATUS.getName)
    if(statusFile.exists()){
      SegmentStatusHandler.getStatus(statusFile)
    }else{
      SegmentStatus.UNKNOWN;
    }
  }

  def setStatus(segDir: File, status: SegmentStatus): Unit = {
    val statusFile = new File(segDir, SegmentFile.STATUS.getName)
    SegmentStatusHandler.setStatus(statusFile, status)
  }


  def isSegmentFileExists(dir: File, baseOffset: Long, segmentFile: SegmentFile): Boolean = {
    val segDir = new File(dir, String.valueOf(baseOffset))
    new File(segDir, segmentFile.getName).exists
  }
  def isSegmentFileExists(segDir: File, segmentFile: SegmentFile): Boolean = {
    new File(segDir, segmentFile.getName).exists
  }

  def canReadSegment(segDir: File): Boolean = {
    val files = segDir.listFiles
    var status = true
    if(files != null){
      for (file <- files if file.isFile){
        status = status && file.canRead
      }
    }
    status
  }


}

object LogFlushStats extends KafkaMetricsGroup {
  val logFlushTimer = new KafkaTimer(newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}
