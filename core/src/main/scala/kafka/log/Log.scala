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

import kafka.api.OffsetRequest
import java.io.{IOException, File}
import java.util.{Comparator, Collections, ArrayList}
import java.util.concurrent.atomic._
import kafka.utils._
import scala.math._
import java.text.NumberFormat
import kafka.server.BrokerTopicStats
import kafka.message._
import kafka.common._
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge

object Log {
  val LogFileSuffix = ".log"
  val IndexFileSuffix = ".index"

  /**
   * Search for the greatest range with start <= the target value.
   */
  def findRange[T <: Range](ranges: Array[T], value: Long, arraySize: Int): Option[T] = {
    if(ranges.size < 1)
      return None

    // check out of bounds
    if(value < ranges(0).start)
      return None

    var low = 0
    var high = arraySize - 1
    while(low < high) {
      val mid = ceil((high + low) / 2.0).toInt
      val found = ranges(mid)
      if(found.start == value)
        return Some(found)
      else if (value < found.start)
        high = mid - 1
      else
        low = mid
    }
    Some(ranges(low))
  }

  def findRange[T <: Range](ranges: Array[T], value: Long): Option[T] =
    findRange(ranges, value, ranges.length)

  /**
   * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
   * so that ls sorts the files numerically
   */
  def filenamePrefixFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    nf.setMinimumIntegerDigits(20)
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset)
  }
  
  def logFilename(dir: File, offset: Long) = 
    new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix)
  
  def indexFilename(dir: File, offset: Long) = 
    new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix)

  def getEmptyOffsets(timestamp: Long): Seq[Long] =
    if (timestamp == OffsetRequest.LatestTime || timestamp == OffsetRequest.EarliestTime)
      Seq(0L)
    else Nil
}


/**
 * An append-only log for storing messages.
 * 
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 * 
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 * 
 */
@threadsafe
private[kafka] class Log(val dir: File, 
                         val maxLogFileSize: Int,
                         val maxMessageSize: Int, 
                         val flushInterval: Int = Int.MaxValue,
                         val rollIntervalMs: Long = Long.MaxValue, 
                         val needsRecovery: Boolean, 
                         val maxIndexSize: Int = (10*1024*1024),
                         val indexIntervalBytes: Int = 4096,
                         time: Time = SystemTime,
                         brokerId: Int = 0) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[Kafka Log on Broker " + brokerId + "], "

  import kafka.log.Log._
  
  /* A lock that guards all modifications to the log */
  private val lock = new Object

  /* The current number of unflushed messages appended to the write */
  private val unflushed = new AtomicInteger(0)

  /* last time it was flushed */
  private val lastflushedTime = new AtomicLong(System.currentTimeMillis)

  /* the actual segments of the log */
  private[log] val segments: SegmentList[LogSegment] = loadSegments()
    
  /* Calculate the offset of the next message */
  private var nextOffset: AtomicLong = new AtomicLong(segments.view.last.nextOffset())
  
  info("Completed load of log %s with log end offset %d".format(name, logEndOffset))

  newGauge(name + "-" + "NumLogSegments",
           new Gauge[Int] { def value = numberOfSegments })

  newGauge(name + "-" + "LogEndOffset",
           new Gauge[Long] { def value = logEndOffset })

  /* The name of this log */
  def name  = dir.getName()

  /* Load the log segments from the log files on disk */
  private def loadSegments(): SegmentList[LogSegment] = {
    // open all the segments read-only
    val logSegments = new ArrayList[LogSegment]
    val ls = dir.listFiles()

    if(ls != null) {
      for(file <- ls if file.isFile) {
        val filename = file.getName()
        if(!file.canRead) {
          throw new IOException("Could not read file " + file)
        } else if(filename.endsWith(IndexFileSuffix)) {
          // ensure that we have a corresponding log file for this index file
          val log = new File(file.getAbsolutePath.replace(IndexFileSuffix, LogFileSuffix))
          if(!log.exists) {
            warn("Found an orphaned index file, %s, with no corresponding log file.".format(file.getAbsolutePath))
            file.delete()
          }
        } else if(filename.endsWith(LogFileSuffix)) {
          val offset = filename.substring(0, filename.length - LogFileSuffix.length).toLong
          // TODO: we should ideally rebuild any missing index files, instead of erroring out
          if(!Log.indexFilename(dir, offset).exists)
            throw new IllegalStateException("Found log file with no corresponding index file.")
          logSegments.add(new LogSegment(dir = dir, 
                                         startOffset = offset,
                                         indexIntervalBytes = indexIntervalBytes, 
                                         maxIndexSize = maxIndexSize))
        }
      }
    }

    if(logSegments.size == 0) {
      // no existing segments, create a new mutable segment
      logSegments.add(new LogSegment(dir = dir, 
                                     startOffset = 0,
                                     indexIntervalBytes = indexIntervalBytes, 
                                     maxIndexSize = maxIndexSize))
    } else {
      // there is at least one existing segment, validate and recover them/it
      // sort segments into ascending order for fast searching
      Collections.sort(logSegments, new Comparator[LogSegment] {
        def compare(s1: LogSegment, s2: LogSegment): Int = {
          if(s1.start == s2.start) 0
          else if(s1.start < s2.start) -1
          else 1
        }
      })

      // reset the index size of the last (current active) log segment to its maximum value
      logSegments.get(logSegments.size() - 1).index.resize(maxIndexSize)

      // run recovery on the last segment if necessary
      if(needsRecovery) {
        var activeSegment = logSegments.get(logSegments.size - 1)
        try {
          recoverSegment(activeSegment)
        } catch {
          case e: InvalidOffsetException =>
            val startOffset = activeSegment.start
            warn("Found invalid offset during recovery of the active segment for topic partition " + dir.getName +". Deleting the segment and " +
                 "creating an empty one with starting offset " + startOffset)
            // truncate the active segment to its starting offset
            activeSegment.truncateTo(startOffset)
        }
      }
    }

    val segmentList = logSegments.toArray(new Array[LogSegment](logSegments.size))
    // Check for the index file of every segment, if it's empty or its last offset is greater than its base offset.
    for (s <- segmentList) {
      require(s.index.entries == 0 || s.index.lastOffset > s.index.baseOffset,
              "Corrupt index found, index file (%s) has non-zero size but the last offset is %d and the base offset is %d"
                .format(s.index.file.getAbsolutePath, s.index.lastOffset, s.index.baseOffset))
    }

    new SegmentList(segmentList)
  }
  
  /**
   * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes from the end of the log.
   */
  private def recoverSegment(segment: LogSegment) {
    info("Recovering log segment %s".format(segment.messageSet.file.getAbsolutePath))
    segment.index.truncate()
    var validBytes = 0
    var lastIndexEntry = 0
    val iter = segment.messageSet.iterator
    try {
      while(iter.hasNext) {
        val entry = iter.next
        entry.message.ensureValid()
        if(validBytes - lastIndexEntry > indexIntervalBytes) {
          // we need to decompress the message, if required, to get the offset of the first uncompressed message
          val startOffset =
            entry.message.compressionCodec match {
              case NoCompressionCodec =>
                entry.offset
              case _ =>
                ByteBufferMessageSet.decompress(entry.message).head.offset
          }
          segment.index.append(startOffset, validBytes)
          lastIndexEntry = validBytes
        }
        validBytes += MessageSet.entrySize(entry.message)
      }
    } catch {
      case e: InvalidMessageException => 
        logger.warn("Found invalid messages in log " + name)
    }
    val truncated = segment.messageSet.sizeInBytes - validBytes
    if(truncated > 0)
      warn("Truncated " + truncated + " invalid bytes from the log " + name + ".")
    segment.messageSet.truncateTo(validBytes)
  }

  /**
   * The number of segments in the log
   */
  def numberOfSegments: Int = segments.view.length

  /**
   * Close this log
   */
  def close() {
    debug("Closing log " + name)
    lock synchronized {
      for(seg <- segments.view)
        seg.close()
    }
  }

  /**
   * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
   * 
   * This method will generally be responsible for assigning offsets to the messages, 
   * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
   * 
   * Returns a tuple containing (first_offset, last_offset) for the newly appended of the message set, 
   * or (-1,-1) if the message set is empty
   */
  def append(messages: ByteBufferMessageSet, assignOffsets: Boolean = true): (Long, Long) = {
    val messageSetInfo = analyzeAndValidateMessageSet(messages)

    // if we have any valid messages, append them to the log
    if(messageSetInfo.count == 0) {
      (-1L, -1L)
    } else {
      // trim any invalid bytes or partial messages before appending it to the on-disk log
      var validMessages = trimInvalidBytes(messages)

      try {
        // they are valid, insert them in the log
        val offsets = lock synchronized {
          val firstOffset = nextOffset.get

          // maybe roll the log if this segment is full
          val segment = maybeRoll(segments.view.last)
          
          // assign offsets to the messageset
          val lastOffset =
            if(assignOffsets) {
              val offsetCounter = new AtomicLong(nextOffset.get)
              try {
                validMessages = validMessages.assignOffsets(offsetCounter, messageSetInfo.codec)
              } catch {
                case e: IOException => throw new KafkaException("Error in validating messages while appending to log '%s'".format(name), e)
              }
              val assignedLastOffset = offsetCounter.get - 1
              val numMessages = assignedLastOffset - firstOffset + 1
              BrokerTopicStats.getBrokerTopicStats(topicName).messagesInRate.mark(numMessages)
              BrokerTopicStats.getBrokerAllTopicsStats.messagesInRate.mark(numMessages)
              assignedLastOffset
            } else {
              require(messageSetInfo.offsetsMonotonic, "Out of order offsets found in " + messages)
              require(messageSetInfo.firstOffset >= nextOffset.get, 
                      "Attempt to append a message set beginning with offset %d to a log with log end offset %d."
                      .format(messageSetInfo.firstOffset, nextOffset.get))
              messageSetInfo.lastOffset
            }

          // Check if the message sizes are valid. This check is done after assigning offsets to ensure the comparison
          // happens with the new message size (after re-compression, if any)
          for(messageAndOffset <- validMessages.shallowIterator) {
            if(MessageSet.entrySize(messageAndOffset.message) > maxMessageSize)
              throw new MessageSizeTooLargeException("Message size is %d bytes which exceeds the maximum configured message size of %d."
                .format(MessageSet.entrySize(messageAndOffset.message), maxMessageSize))
          }

          // now append to the log
          segment.append(firstOffset, validMessages)
          
          // advance the log end offset
          nextOffset.set(lastOffset + 1)

          trace("Appended message set to log %s with first offset: %d, next offset: %d, and messages: %s"
                  .format(this.name, firstOffset, nextOffset.get(), validMessages))

          // return the offset at which the messages were appended
          (firstOffset, lastOffset)
        }
        
        // maybe flush the log and index
        val numAppendedMessages = (offsets._2 - offsets._1 + 1).toInt
        maybeFlush(numAppendedMessages)
        
        // return the first and last offset
        offsets
      } catch {
        case e: IOException => throw new KafkaStorageException("I/O exception in append to log '%s'".format(name), e)
      }
    }
  }
  
  /* struct to hold various quantities we compute about each message set before appending to the log */
  case class MessageSetAppendInfo(firstOffset: Long, lastOffset: Long, codec: CompressionCodec, count: Int, offsetsMonotonic: Boolean)
  
  /**
   * Validate the following:
   * 1. each message matches its CRC
   * 
   * Also compute the following quantities:
   * 1. First offset in the message set
   * 2. Last offset in the message set
   * 3. Number of messages
   * 4. Whether the offsets are monotonically increasing
   * 5. Whether any compression codec is used (if many are used, then the last one is given)
   */
  private def analyzeAndValidateMessageSet(messages: ByteBufferMessageSet): MessageSetAppendInfo = {
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

      // check the validity of the message by checking CRC
      val m = messageAndOffset.message
      m.ensureValid()

      messageCount += 1;
      
      val messageCodec = m.compressionCodec
      if(messageCodec != NoCompressionCodec)
        codec = messageCodec
    }
    MessageSetAppendInfo(firstOffset, lastOffset, codec, messageCount, monotonic)
  }
  
  /**
   * Trim any invalid bytes from the end of this message set (if there are any)
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
   * Read a message set from the log. 
   * startOffset - The logical offset to begin reading at
   * maxLength - The maximum number of bytes to read
   * maxOffset - The first offset not included in the read
   */
  def read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None): MessageSet = {
    trace("Reading %d bytes from offset %d in log %s of length %d bytes".format(maxLength, startOffset, name, size))
    val view = segments.view
        
    // check if the offset is valid and in range
    val first = view.head.start
    val next = nextOffset.get
    if(startOffset == next)
      return MessageSet.Empty
    else if(startOffset > next || startOffset < first)
      throw new OffsetOutOfRangeException("Request for offset %d but we only have log segments in the range %d to %d.".format(startOffset, first, next))
    
    // Do the read on the segment with a base offset less than the target offset
    // TODO: to handle sparse offsets, we need to skip to the next segment if this read doesn't find anything
    Log.findRange(view, startOffset, view.length) match {
      case None => throw new OffsetOutOfRangeException("Offset is earlier than the earliest offset")
      case Some(segment) => segment.read(startOffset, maxLength, maxOffset)
    }
  }

  /**
   * Delete any log segments matching the given predicate function
   */
  def markDeletedWhile(predicate: LogSegment => Boolean): Seq[LogSegment] = {
    lock synchronized {
      debug("Garbage collecting log..")
      debug("Segments of log %s : %s ".format(this.name, segments.view.mkString(",")))
      debug("Index files for log %s: %s".format(this.name, segments.view.map(_.index.file.exists()).mkString(",")))
      debug("Data files for log %s: %s".format(this.name, segments.view.map(_.messageSet.file.exists()).mkString(",")))
      val view = segments.view
      val deletable = view.takeWhile(predicate)
      for(seg <- deletable)
        seg.deleted = true
      var numToDelete = deletable.size
      // if we are deleting everything, create a new empty segment
      if(numToDelete == view.size) {
        if (view(numToDelete - 1).size > 0)
          roll()
        else {
          // If the last segment to be deleted is empty and we roll the log, the new segment will have the same
          // file name. So simply reuse the last segment and reset the modified time.
          view(numToDelete - 1).messageSet.file.setLastModified(time.milliseconds)
          numToDelete -=1
        }
      }
      segments.trunc(numToDelete)
    }
  }

  /**
   * Get the size of the log in bytes
   */
  def size: Long = segments.view.foldLeft(0L)(_ + _.size)

  /**
   *  Get the offset of the next message that will be appended
   */
  def logEndOffset: Long = nextOffset.get

  /**
   * Roll the log over if necessary
   */
  private def maybeRoll(segment: LogSegment): LogSegment = {
    if(segment.messageSet.sizeInBytes > maxLogFileSize) {
      info("Rolling %s due to full data log".format(name))
      roll()
    } else if((segment.firstAppendTime.isDefined) && (time.milliseconds - segment.firstAppendTime.get > rollIntervalMs)) {
      info("Rolling %s due to time based rolling".format(name))
      roll()
    } else if(segment.index.isFull) {
      info("Rolling %s due to full index maxIndexSize = %d, entries = %d, maxEntries = %d"
        .format(name, segment.index.maxIndexSize, segment.index.entries(), segment.index.maxEntries))
      roll()
    } else
      segment
  }

  /**
   * Create a new segment and make it active, and return it
   */
  def roll(): LogSegment = {
    lock synchronized {
      flush()
      rollToOffset(logEndOffset)
    }
  }
  
  /**
   * Roll the log over to the given new offset value
   */
  private def rollToOffset(newOffset: Long): LogSegment = {
    val logFile = logFilename(dir, newOffset)
    val indexFile = indexFilename(dir, newOffset)
    for(file <- List(logFile, indexFile); if file.exists) {
      warn("Newly rolled segment file " + file.getAbsolutePath + " already exists; deleting it first")
      file.delete()
    }
    info("Rolling log '" + name + "' to " + logFile.getAbsolutePath + " and " + indexFile.getAbsolutePath)
    segments.view.lastOption match {
      case Some(segment) => segment.index.trimToValidSize()
      case None => 
    }

    val segmentsView = segments.view
    if(segmentsView.size > 0 && segmentsView.last.start == newOffset)
      throw new KafkaException("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists".format(dir.getName, newOffset))

    val segment = new LogSegment(dir, 
                                 startOffset = newOffset,
                                 indexIntervalBytes = indexIntervalBytes, 
                                 maxIndexSize = maxIndexSize)
    segments.append(segment)
    segment
  }

  /**
   * Flush the log if necessary
   */
  private def maybeFlush(numberOfMessages : Int) {
    if(unflushed.addAndGet(numberOfMessages) >= flushInterval)
      flush()
  }

  /**
   * Flush this log file to the physical disk
   */
  def flush() : Unit = {
    if (unflushed.get == 0)
      return

    lock synchronized {
      debug("Flushing log '" + name + "' last flushed: " + getLastFlushedTime + " current time: " +
          time.milliseconds)
      segments.view.last.flush()
      unflushed.set(0)
      lastflushedTime.set(time.milliseconds)
     }
  }

  def getOffsetsBefore(timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    val segsArray = segments.view
    var offsetTimeArray: Array[(Long, Long)] = null
    if(segsArray.last.size > 0)
      offsetTimeArray = new Array[(Long, Long)](segsArray.length + 1)
    else
      offsetTimeArray = new Array[(Long, Long)](segsArray.length)

    for(i <- 0 until segsArray.length)
      offsetTimeArray(i) = (segsArray(i).start, segsArray(i).messageSet.file.lastModified)
    if(segsArray.last.size > 0)
      offsetTimeArray(segsArray.length) = (logEndOffset, time.milliseconds)

    var startIndex = -1
    timestamp match {
      case OffsetRequest.LatestTime =>
        startIndex = offsetTimeArray.length - 1
      case OffsetRequest.EarliestTime =>
        startIndex = 0
      case _ =>
        var isFound = false
        debug("Offset time array = " + offsetTimeArray.foreach(o => "%d, %d".format(o._1, o._2)))
        startIndex = offsetTimeArray.length - 1
        while (startIndex >= 0 && !isFound) {
          if (offsetTimeArray(startIndex)._2 <= timestamp)
            isFound = true
          else
            startIndex -=1
        }
    }

    val retSize = maxNumOffsets.min(startIndex + 1)
    val ret = new Array[Long](retSize)
    for(j <- 0 until retSize) {
      ret(j) = offsetTimeArray(startIndex)._1
      startIndex -= 1
    }
    // ensure that the returned seq is in descending order of offsets
    ret.toSeq.sortBy(- _)
  }

  def delete(): Unit = {
    deleteSegments(segments.contents.get())
    Utils.rm(dir)
  }


  /* Attempts to delete all provided segments from a log and returns how many it was able to */
  def deleteSegments(segments: Seq[LogSegment]): Int = {
    var total = 0
    for(segment <- segments) {
      info("Deleting log segment " + segment.start + " from " + name)
      val deletedLog = segment.messageSet.delete()
      val deletedIndex = segment.index.delete()
      if(!deletedIndex || !deletedLog) {
        throw new KafkaStorageException("Deleting log segment " + segment.start + " failed.")
      } else {
        total += 1
      }
      if(segment.messageSet.file.exists())
        error("Data log file %s still exists".format(segment.messageSet.file.getAbsolutePath))
      if(segment.index.file.exists())
        error("Index file %s still exists".format(segment.index.file.getAbsolutePath))
    }
    total
  }
  
  def truncateTo(targetOffset: Long) {
    if(targetOffset < 0)
      throw new IllegalArgumentException("Cannot truncate to a negative offset (%d).".format(targetOffset))
    lock synchronized {
      val segmentsToBeDeleted = segments.view.filter(segment => segment.start > targetOffset)
      val viewSize = segments.view.size
      val numSegmentsDeleted = deleteSegments(segmentsToBeDeleted)
      /* We should not hit this error because segments.view is locked in markedDeletedWhile() */
      if(numSegmentsDeleted != segmentsToBeDeleted.size)
        error("Failed to delete some segments when attempting to truncate to offset " + targetOffset +")")
      if (numSegmentsDeleted == viewSize) {
        segments.trunc(segments.view.size)
        rollToOffset(targetOffset)
        this.nextOffset.set(targetOffset)
      } else {
        if(targetOffset > logEndOffset) {
          error("Target offset %d cannot be greater than the last message offset %d in the log %s".
                format(targetOffset, logEndOffset, segments.view.last.messageSet.file.getAbsolutePath))
        } else {
          // find the log segment that has this hw
          val segmentToBeTruncated = findRange(segments.view, targetOffset)
          segmentToBeTruncated match {
            case Some(segment) =>
              val truncatedSegmentIndex = segments.view.indexOf(segment)
              segments.truncLast(truncatedSegmentIndex)
              segment.truncateTo(targetOffset)
              this.nextOffset.set(targetOffset)
              info("Truncated log segment %s to target offset %d".format(segments.view.last.messageSet.file.getAbsolutePath, targetOffset))
            case None => // nothing to do
          }
        }
      }
    }
  }
    
    /**
   *  Truncate all segments in the log and start a new segment on a new offset
   */
  def truncateAndStartWithNewOffset(newOffset: Long) {
    lock synchronized {
      val deletedSegments = segments.trunc(segments.view.size)
      info("Truncate and start log '" + name + "' to " + newOffset)
      deleteSegments(deletedSegments)
      segments.append(new LogSegment(dir,
                                     newOffset,
                                     indexIntervalBytes = indexIntervalBytes, 
                                     maxIndexSize = maxIndexSize))
      this.nextOffset.set(newOffset)
    }
  }

  def topicName():String = {
    name.substring(0, name.lastIndexOf("-"))
  }

  def getLastFlushedTime():Long = {
    return lastflushedTime.get
  }
  
  override def toString() = "Log(" + this.dir + ")"
  
}
  
