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

import java.io.{IOException, File}
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import java.util.concurrent.atomic._
import kafka.utils._
import scala.collection.JavaConversions.asIterable;
import java.text.NumberFormat
import kafka.message._
import kafka.common._
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge


/**
 * An append-only log for storing messages.
 * 
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 * 
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 * 
 * @param dir The directory in which log segments are created.
 * @param maxSegmentSize The maximum segment size in bytes.
 * @param maxMessageSize The maximum message size in bytes (including headers) that will be allowed in this log.
 * @param flushInterval The number of messages that can be appended to this log before we force a flush of the log.
 * @param rollIntervalMs The time after which we will force the rolling of a new log segment
 * @param needsRecovery Should we run recovery on this log when opening it? This should be done if the log wasn't cleanly shut down.
 * @param maxIndexSize The maximum size of an offset index in this log. The index of the active log segment will be pre-allocated to this size.
 * @param indexIntervalBytes The (approximate) number of bytes between entries in the offset index for this log.
 * 
 */
@threadsafe
class Log(val dir: File,
          val scheduler: Scheduler,
          val maxSegmentSize: Int,
          val maxMessageSize: Int, 
          val flushInterval: Int = Int.MaxValue,
          val rollIntervalMs: Long = Long.MaxValue, 
          val needsRecovery: Boolean, 
          val maxIndexSize: Int = (10*1024*1024),
          val indexIntervalBytes: Int = 4096,
          val segmentDeleteDelayMs: Long = 60000,
          time: Time = SystemTime) extends Logging with KafkaMetricsGroup {

  import kafka.log.Log._
  
  /* A lock that guards all modifications to the log */
  private val lock = new Object

  /* The current number of unflushed messages appended to the write */
  private val unflushed = new AtomicInteger(0)

  /* last time it was flushed */
  private val lastflushedTime = new AtomicLong(time.milliseconds)

  /* the actual segments of the log */
  private val segments: ConcurrentNavigableMap[Long,LogSegment] = loadSegments()
    
  /* Calculate the offset of the next message */
  private val nextOffset: AtomicLong = new AtomicLong(activeSegment.nextOffset())

  newGauge(name + "-" + "NumLogSegments",
           new Gauge[Int] { def getValue = numberOfSegments })

  newGauge(name + "-" + "LogEndOffset",
           new Gauge[Long] { def getValue = logEndOffset })

  /** The name of this log */
  def name  = dir.getName()

  /* Load the log segments from the log files on disk */
  private def loadSegments(): ConcurrentNavigableMap[Long, LogSegment] = {
    // open all the segments read-only
    val logSegments = new ConcurrentSkipListMap[Long, LogSegment]
    val ls = dir.listFiles()
    if(ls != null) {
      for(file <- ls if file.isFile) {
        if(!file.canRead)
          throw new IOException("Could not read file " + file)
        val filename = file.getName
        if(filename.endsWith(DeletedFileSuffix)) {
          // if the file ends in .deleted, delete it
          val deleted = file.delete()
          if(!deleted)
            warn("Attempt to delete defunct segment file %s failed.".format(filename))
        }  else if(filename.endsWith(IndexFileSuffix)) {
          // if it is an index file, make sure it has a corresponding .log file
          val logFile = new File(file.getAbsolutePath.replace(IndexFileSuffix, LogFileSuffix))
          if(!logFile.exists) {
            warn("Found an orphaned index file, %s, with no corresponding log file.".format(file.getAbsolutePath))
            file.delete()
          }
        } else if(filename.endsWith(LogFileSuffix)) {
          // if its a log file, load the corresponding log segment
          val start = filename.substring(0, filename.length - LogFileSuffix.length).toLong
          val hasIndex = Log.indexFilename(dir, start).exists
          val segment = new LogSegment(dir = dir, 
                                       startOffset = start,
                                       indexIntervalBytes = indexIntervalBytes, 
                                       maxIndexSize = maxIndexSize)
          if(!hasIndex) {
            // this can only happen if someone manually deletes the index file
            error("Could not find index file corresponding to log file %s, rebuilding index...".format(segment.log.file.getAbsolutePath))
            segment.recover(maxMessageSize)
          }
          logSegments.put(start, segment)
        }
      }
    }

    if(logSegments.size == 0) {
      // no existing segments, create a new mutable segment beginning at offset 0
      logSegments.put(0,
                      new LogSegment(dir = dir, 
                                     startOffset = 0,
                                     indexIntervalBytes = indexIntervalBytes, 
                                     maxIndexSize = maxIndexSize))
    } else {
      // reset the index size of the currently active log segment to allow more entries
      val active = logSegments.lastEntry.getValue
      active.index.resize(maxIndexSize)

      // run recovery on the active segment if necessary
      if(needsRecovery) {
        info("Recovering active segment of %s.".format(name))
        active.recover(maxMessageSize)
      }
    }
    logSegments
  }

  /**
   * The number of segments in the log.
   * Take care! this is an O(n) operation.
   */
  def numberOfSegments: Int = segments.size

  /**
   * Close this log
   */
  def close() {
    debug("Closing log " + name)
    lock synchronized {
      for(seg <- logSegments)
        seg.close()
    }
  }

  /**
   * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
   * 
   * This method will generally be responsible for assigning offsets to the messages, 
   * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
   * 
   * @param messages The message set to append
   * @param assignOffsets Should the log assign offsets to this message set or blindly apply what it is given
   * 
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * 
   * @return Information about the appended messages including the first and last offset
   */
  def append(messages: ByteBufferMessageSet, assignOffsets: Boolean = true): LogAppendInfo = {
    val appendInfo = analyzeAndValidateMessageSet(messages)
    
    // if we have any valid messages, append them to the log
    if(appendInfo.count == 0)
      return appendInfo
      
    // trim any invalid bytes or partial messages before appending it to the on-disk log
    var validMessages = trimInvalidBytes(messages)

    try {
      // they are valid, insert them in the log
      lock synchronized {
        // maybe roll the log if this segment is full
        val segment = maybeRoll()
          
        if(assignOffsets) {
           // assign offsets to the messageset
          appendInfo.firstOffset = nextOffset.get
          validMessages = validMessages.assignOffsets(nextOffset, appendInfo.codec)
          appendInfo.lastOffset = nextOffset.get - 1
        } else {
          // we are taking the offsets we are given
          if(!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffset.get)
              throw new IllegalArgumentException("Out of order offsets found in " + messages)
          nextOffset.set(appendInfo.lastOffset + 1)
        }
          
        // now append to the log
        trace("Appending message set to %s with offsets %d to %d.".format(name, appendInfo.firstOffset, appendInfo.lastOffset))
        segment.append(appendInfo.firstOffset, validMessages)
          
        // maybe flush the log and index
        maybeFlush(appendInfo.count)
        
        appendInfo
      }
    } catch {
      case e: IOException => throw new KafkaStorageException("I/O exception in append to log '%s'".format(name), e)
    }
  }
  
  /** Struct to hold various quantities we compute about each message set before appending to the log
   * @param firstOffset The first offset in the message set
   * @param lastOffset The last offset in the message set
   * @param codec The codec used in the message set
   * @param count The number of messages
   * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
   */
  case class LogAppendInfo(var firstOffset: Long, var lastOffset: Long, codec: CompressionCodec, count: Int, offsetsMonotonic: Boolean)
  
  /**
   * Validate the following:
   * <ol>
   * <li> each message is not too large
   * <li> each message matches its CRC
   * </ol>
   * 
   * Also compute the following quantities:
   * <ol>
   * <li> First offset in the message set
   * <li> Last offset in the message set
   * <li> Number of messages
   * <li> Whether the offsets are monotonically increasing
   * <li> Whether any compression codec is used (if many are used, then the last one is given)
   * </ol>
   */
  private def analyzeAndValidateMessageSet(messages: ByteBufferMessageSet): LogAppendInfo = {
    var messageCount = 0
    var firstOffset, lastOffset = -1L
    var codec: CompressionCodec = NoCompressionCodec
    var monotonic = true
    for(messageAndOffset <- messages.shallowIterator) {
      // update the first offset if on the first message
      if(firstOffset < 0)
        firstOffset = messageAndOffset.offset
      // check that offsets are monotonically increasing
      if(lastOffset >= messageAndOffset.offset)
        monotonic = false
      // update the last offset seen
      lastOffset = messageAndOffset.offset

      // check the validity of the message by checking CRC and message size
      val m = messageAndOffset.message
      m.ensureValid()
      if(MessageSet.entrySize(m) > maxMessageSize)
        throw new MessageSizeTooLargeException("Message size is %d bytes which exceeds the maximum configured message size of %d.".format(MessageSet.entrySize(m), maxMessageSize))
      
      messageCount += 1;
      
      val messageCodec = m.compressionCodec
      if(messageCodec != NoCompressionCodec)
        codec = messageCodec
    }
    LogAppendInfo(firstOffset, lastOffset, codec, messageCount, monotonic)
  }
  
  /**
   * Trim any invalid bytes from the end of this message set (if there are any)
   * @param messages The message set to trim
   * @return A trimmed message set. This may be the same as what was passed in or it may not.
   */
  private def trimInvalidBytes(messages: ByteBufferMessageSet): ByteBufferMessageSet = {
    val messageSetValidBytes = messages.validBytes
    if(messageSetValidBytes < 0)
      throw new InvalidMessageSizeException("Illegal length of message set " + messageSetValidBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests")
    if(messageSetValidBytes == messages.sizeInBytes) {
      messages
    } else {
      // trim invalid bytes
      val validByteBuffer = messages.buffer.duplicate()
      validByteBuffer.limit(messageSetValidBytes)
      new ByteBufferMessageSet(validByteBuffer)
    }
  }

  /**
   * Read messages from the log
   * @param startOffset The offset to begin reading at
   * @param maxLength The maximum number of bytes to read
   * @param maxOffset -The offset to read up to, exclusive. (i.e. the first offset NOT included in the resulting message set).
   * 
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the base offset of the first segment.
   * @return The messages read
   */
  def read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None): MessageSet = {
    trace("Reading %d bytes from offset %d in log %s of length %d bytes".format(maxLength, startOffset, name, size))

    // check if the offset is valid and in range
    val next = nextOffset.get
    if(startOffset == next)
      return MessageSet.Empty
    
    var entry = segments.floorEntry(startOffset)
      
    // attempt to read beyond the log end offset is an error
    if(startOffset > next || entry == null)
      throw new OffsetOutOfRangeException("Request for offset %d but we only have log segments in the range %d to %d.".format(startOffset, segments.firstKey, next))
    
    // do the read on the segment with a base offset less than the target offset
    // but if that segment doesn't contain any messages with an offset greater than that
    // continue to read from successive segments until we get some messages or we reach the end of the log
    while(entry != null) {
      val messages = entry.getValue.read(startOffset, maxOffset, maxLength)
      if(messages == null)
        entry = segments.higherEntry(entry.getKey)
      else
        return messages
    }
    
    // okay we are beyond the end of the last segment but less than the log end offset
    MessageSet.Empty
  }

  /**
   * Delete any log segments matching the given predicate function,
   * starting with the oldest segment and moving forward until a segment doesn't match.
   * @param predicate A function that takes in a single log segment and returns true iff it is deletable
   * @return The number of segments deleted
   */
  def deleteOldSegments(predicate: LogSegment => Boolean): Int = {
    // find any segments that match the user-supplied predicate UNLESS it is the final segment 
    // and it is empty (since we would just end up re-creating it
    val lastSegment = activeSegment
    var deletable = logSegments.takeWhile(s => predicate(s) && (s.baseOffset != lastSegment.baseOffset || s.size > 0))
    val numToDelete = deletable.size
    if(numToDelete > 0) {
      lock synchronized {
        // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
        if(segments.size == numToDelete)
          roll()
        // remove the segments for lookups
        deletable.foreach(deleteSegment(_))
      }
    }
    numToDelete
  }

  /**
   * The size of the log in bytes
   */
  def size: Long = logSegments.map(_.size).sum

  /**
   *  The offset of the next message that will be appended to the log
   */
  def logEndOffset: Long = nextOffset.get

  /**
   * Roll the log over to a new empty log segment if necessary
   * @return The currently active segment after (perhaps) rolling to a new segment
   */
  private def maybeRoll(): LogSegment = {
    val segment = activeSegment
    if (segment.size > maxSegmentSize) {
      info("Rolling %s due to full data log".format(name))
      roll()
    } else if (segment.size > 0 && time.milliseconds - segment.created > rollIntervalMs) {
      info("Rolling %s due to time based rolling".format(name))
      roll()
    } else if (segment.index.isFull) {
      info("Rolling %s due to full index maxIndexSize = %d, entries = %d, maxEntries = %d"
        .format(name, segment.index.maxIndexSize, segment.index.entries(), segment.index.maxEntries))
      roll()
    } else
      segment
  }
  
  /**
   * Roll the log over to a new active segment starting with the current logEndOffset.
   * This will trim the index to the exact size of the number of entries it currently contains.
   * @return The newly rolled segment
   */
  def roll(): LogSegment = {
    lock synchronized {
      // flush the log to ensure that only the active segment needs to be recovered
      if(!segments.isEmpty())
        flush()
  
      val newOffset = logEndOffset
      val logFile = logFilename(dir, newOffset)
      val indexFile = indexFilename(dir, newOffset)
      for(file <- List(logFile, indexFile); if file.exists) {
        warn("Newly rolled segment file " + file.getName + " already exists; deleting it first")
        file.delete()
      }
    
      info("Rolling log '" + name + "' to " + logFile.getName + " and " + indexFile.getName)
      segments.lastEntry() match {
        case null => 
        case entry => entry.getValue.index.trimToValidSize()
      }
      val segment = new LogSegment(dir, 
                                   startOffset = newOffset,
                                   indexIntervalBytes = indexIntervalBytes, 
                                   maxIndexSize = maxIndexSize)
      val prev = segments.put(segment.baseOffset, segment)
      if(prev != null)
        throw new KafkaException("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists.".format(dir.getName, newOffset))
      segment
    }
  }

  /**
   * Flush the log if necessary
   * @param numberOfMessages The number of messages that are being appended
   */
  private def maybeFlush(numberOfMessages : Int) {
    if(unflushed.addAndGet(numberOfMessages) >= flushInterval)
      flush()
  }

  /**
   * Flush this log file and assoicated index to the physical disk
   */
  def flush() : Unit = {
    if (unflushed.get == 0)
      return

    debug("Flushing log '" + name + "' last flushed: " + lastFlushTime + " current time: " +
          time.milliseconds + " unflushed = " + unflushed.get)
    lock synchronized {
      activeSegment.flush()
      unflushed.set(0)
      lastflushedTime.set(time.milliseconds)
     }
  }

  /**
   * Completely delete this log directory and all contents from the file system with no delay
   */
  def delete(): Unit = {
    logSegments.foreach(_.delete())
    Utils.rm(dir)
  }

  /**
   * Truncate this log so that it ends with the greatest offset < targetOffset.
   * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
   */
  def truncateTo(targetOffset: Long) {
    info("Truncating log %s to offset %d.".format(name, targetOffset))
    if(targetOffset < 0)
      throw new IllegalArgumentException("Cannot truncate to a negative offset (%d).".format(targetOffset))
    if(targetOffset > logEndOffset) {
      info("Truncating %s to %d has no effect as the largest offset in the log is %d.".format(name, targetOffset, logEndOffset-1))
      return
    }
    lock synchronized {
      if(segments.firstEntry.getValue.baseOffset > targetOffset) {
        truncateFullyAndStartAt(targetOffset)
      } else {
        val deletable = logSegments.filter(segment => segment.baseOffset > targetOffset)
        deletable.foreach(deleteSegment(_))
        activeSegment.truncateTo(targetOffset)
        this.nextOffset.set(targetOffset)
      }
    }
  }
    
  /**
   *  Delete all data in the log and start at the new offset
   *  @param newOffset The new offset to start the log with
   */
  def truncateFullyAndStartAt(newOffset: Long) {
    debug("Truncate and start log '" + name + "' to " + newOffset)
    lock synchronized {
      val segmentsToDelete = logSegments.toList
      segmentsToDelete.foreach(deleteSegment(_))
      segments.put(newOffset, 
                   new LogSegment(dir, 
                                  newOffset,
                                  indexIntervalBytes = indexIntervalBytes, 
                                  maxIndexSize = maxIndexSize))
      this.nextOffset.set(newOffset)
    }
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   */
  def lastFlushTime(): Long = lastflushedTime.get
  
  /**
   * The active segment that is currently taking appends
   */
  def activeSegment = segments.lastEntry.getValue
  
  /**
   * All the log segments in this log ordered from oldest to newest
   */
  def logSegments: Iterable[LogSegment] = asIterable(segments.values)
  
  override def toString() = "Log(" + this.dir + ")"
  
  /**
   * This method performs an asynchronous log segment delete by doing the following:
   * <ol>
   *   <li>It removes the segment from the segment map so that it will no longer be used for reads.
   *   <li>It renames the index and log files by appending .deleted to the respective file name
   *   <li>It schedules an asynchronous delete operation to occur in the future
   * </ol>
   * This allows reads to happen concurrently without synchronization and without the possibility of physically
   * deleting a file while it is being read from.
   * 
   * @param segment The log segment to schedule for deletion
   */
  private def deleteSegment(segment: LogSegment) {
    info("Scheduling log segment %d for log %s for deletion.".format(segment.baseOffset, dir.getName))
    lock synchronized {
      segments.remove(segment.baseOffset)
      val deletedLog = new File(segment.log.file.getPath + Log.DeletedFileSuffix)
      val deletedIndex = new File(segment.index.file.getPath + Log.DeletedFileSuffix)
      val renamedLog = segment.log.file.renameTo(deletedLog)
      val renamedIndex = segment.index.file.renameTo(deletedIndex)
      if(!renamedLog && segment.log.file.exists)
        throw new KafkaStorageException("Failed to rename file %s to %s for log %s.".format(segment.log.file.getPath, deletedLog.getPath, name))
      if(!renamedIndex && segment.index.file.exists)
        throw new KafkaStorageException("Failed to rename file %s to %s for log %s.".format(segment.index.file.getPath, deletedIndex.getPath, name))
      def asyncDeleteFiles() {
        info("Deleting log segment %s for log %s.".format(segment.baseOffset, name))
        if(!deletedLog.delete())
          warn("Failed to delete log segment file %s for log %s.".format(deletedLog.getPath, name))
        if(!deletedIndex.delete())
          warn("Failed to delete index segment file %s for log %s.".format(deletedLog.getPath, name))
      }
      scheduler.schedule("delete-log-segment", asyncDeleteFiles, delay = segmentDeleteDelayMs)
    }
  }
  
}

/**
 * Helper functions for logs
 */
object Log {
  val LogFileSuffix = ".log"
  val IndexFileSuffix = ".index"
  val DeletedFileSuffix = ".deleted"

  /**
   * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
   * so that ls sorts the files numerically.
   * @param offset The offset to use in the file name
   * @return The filename
   */
  def filenamePrefixFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    nf.setMinimumIntegerDigits(20)
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset)
  }
  
  /**
   * Construct a log file name in the given dir with the given base offset
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def logFilename(dir: File, offset: Long) = 
    new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix)
  
  /**
   * Construct an index file name in the given dir using the given base offset
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def indexFilename(dir: File, offset: Long) = 
    new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix)
  
}
  
