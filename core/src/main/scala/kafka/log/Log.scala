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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicInteger}
import kafka.utils._
import java.text.NumberFormat
import kafka.server.BrokerTopicStat
import kafka.message.{ByteBufferMessageSet, MessageSet, InvalidMessageException, FileMessageSet}
import kafka.common.{KafkaException, InvalidMessageSizeException, OffsetOutOfRangeException}

object Log {
  val FileSuffix = ".kafka"

  /**
   * Find a given range object in a list of ranges by a value in that range. Does a binary search over the ranges
   * but instead of checking for equality looks within the range. Takes the array size as an option in case
   * the array grows while searching happens
   *
   * TODO: This should move into SegmentList.scala
   */
  def findRange[T <: Range](ranges: Array[T], value: Long, arraySize: Int): Option[T] = {
    if(ranges.size < 1)
      return None

    // check out of bounds
    if(value < ranges(0).start || value > ranges(arraySize - 1).start + ranges(arraySize - 1).size)
      throw new OffsetOutOfRangeException("offset " + value + " is out of range")

    // check at the end
    if (value == ranges(arraySize - 1).start + ranges(arraySize - 1).size)
      return None

    var low = 0
    var high = arraySize - 1
    while(low <= high) {
      val mid = (high + low) / 2
      val found = ranges(mid)
      if(found.contains(value))
        return Some(found)
      else if (value < found.start)
        high = mid - 1
      else
        low = mid + 1
    }
    None
  }

  def findRange[T <: Range](ranges: Array[T], value: Long): Option[T] =
    findRange(ranges, value, ranges.length)

  /**
   * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
   * so that ls sorts the files numerically
   */
  def nameFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    nf.setMinimumIntegerDigits(20)
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset) + FileSuffix
  }

  def getEmptyOffsets(request: OffsetRequest): Array[Long] = {
    if (request.time == OffsetRequest.LatestTime || request.time == OffsetRequest.EarliestTime)
      return Array(0L)
    else
      return Array()
  }
}


/**
 * A segment file in the log directory. Each log semgment consists of an open message set, a start offset and a size 
 */
class LogSegment(val file: File, val messageSet: FileMessageSet, val start: Long, time: Time) extends Range {
  var firstAppendTime: Option[Long] = None
  @volatile var deleted = false
  /* Return the size in bytes of this log segment */
  def size: Long = messageSet.sizeInBytes()
  /* Return the absolute end offset of this log segment */
  def absoluteEndOffset: Long = start + messageSet.sizeInBytes()

  def updateFirstAppendTime() {
    if (firstAppendTime.isEmpty)
      firstAppendTime = Some(time.milliseconds)
  }

  def append(messages: ByteBufferMessageSet) {
    if (messages.sizeInBytes > 0) {
      messageSet.append(messages)
      updateFirstAppendTime()
    }
  }

  override def toString() = "(file=" + file + ", start=" + start + ", size=" + size + ")"

  /**
   * Truncate this log segment upto absolute offset value. Since the offset specified is absolute, to compute the amount
   * of data to be deleted, we have to compute the offset relative to start of the log segment
   * @param offset Absolute offset for this partition
   */
  def truncateTo(offset: Long) = {
    messageSet.truncateTo(offset - start)
  }
}


/**
 * An append-only log for storing messages. 
 */
@threadsafe
private[kafka] class Log( val dir: File, val maxSize: Long,
                          val flushInterval: Int, val rollIntervalMs: Long, val needRecovery: Boolean,
                          time: Time, brokerId: Int = 0) extends Logging {
  this.logIdent = "[Kafka Log on Broker " + brokerId + "], "

  import kafka.log.Log._

  /* A lock that guards all modifications to the log */
  private val lock = new Object

  /* The current number of unflushed messages appended to the write */
  private val unflushed = new AtomicInteger(0)

  /* last time it was flushed */
  private val lastflushedTime = new AtomicLong(System.currentTimeMillis)

  /* The actual segments of the log */
  private[log] val segments: SegmentList[LogSegment] = loadSegments()

  private val logStats = new LogStats(this)

  Utils.registerMBean(logStats, "kafka:type=kafka.logs." + dir.getName)

  /* The name of this log */
  def name  = dir.getName()

  /* Load the log segments from the log files on disk */
  private def loadSegments(): SegmentList[LogSegment] = {
    // open all the segments read-only
    val logSegments = new ArrayList[LogSegment]
    val ls = dir.listFiles()
    if(ls != null) {
      for(file <- ls if file.isFile && file.toString.endsWith(FileSuffix)) {
        if(!file.canRead)
          throw new IOException("Could not read file " + file)
        val filename = file.getName()
        val start = filename.substring(0, filename.length - FileSuffix.length).toLong
        val messageSet = new FileMessageSet(file, false)
        logSegments.add(new LogSegment(file, messageSet, start, time))
      }
    }

    if(logSegments.size == 0) {
      // no existing segments, create a new mutable segment
      val newFile = new File(dir, nameFromOffset(0))
      val set = new FileMessageSet(newFile, true)
      logSegments.add(new LogSegment(newFile, set, 0, time))
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
      validateSegments(logSegments)

      //make the final section mutable and run recovery on it if necessary
      val last = logSegments.remove(logSegments.size - 1)
      last.messageSet.close()
      info("Loading the last segment " + last.file.getAbsolutePath() + " in mutable mode, recovery " + needRecovery)
      val mutable = new LogSegment(last.file, new FileMessageSet(last.file, true, new AtomicBoolean(needRecovery)), last.start, time)
      logSegments.add(mutable)
    }
    new SegmentList(logSegments.toArray(new Array[LogSegment](logSegments.size)))
  }

  /**
   * Check that the ranges and sizes add up, otherwise we have lost some data somewhere
   */
  private def validateSegments(segments: ArrayList[LogSegment]) {
    lock synchronized {
      for(i <- 0 until segments.size - 1) {
        val curr = segments.get(i)
        val next = segments.get(i+1)
        if(curr.start + curr.size != next.start)
          throw new KafkaException("The following segments don't validate: " + curr.file.getAbsolutePath() + ", " + next.file.getAbsolutePath())
      }
    }
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
      for(seg <- segments.view) {
        info("Closing log segment " + seg.file.getAbsolutePath)
        seg.messageSet.close()
      }
    }
  }

  /**
   * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
   * Returns the offset at which the messages are written.
   */
  def append(messages: ByteBufferMessageSet): Unit = {
    // validate the messages
    var numberOfMessages = 0
    for(messageAndOffset <- messages) {
      if(!messageAndOffset.message.isValid)
        throw new InvalidMessageException()
      numberOfMessages += 1;
    }

    BrokerTopicStat.getBrokerTopicStat(topicName).recordMessagesIn(numberOfMessages)
    BrokerTopicStat.getBrokerAllTopicStat.recordMessagesIn(numberOfMessages)
    logStats.recordAppendedMessages(numberOfMessages)

    // truncate the message set's buffer upto validbytes, before appending it to the on-disk log
    val validByteBuffer = messages.buffer.duplicate()
    val messageSetValidBytes = messages.validBytes
    if(messageSetValidBytes > Int.MaxValue || messageSetValidBytes < 0)
      throw new InvalidMessageSizeException("Illegal length of message set " + messageSetValidBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests")

    validByteBuffer.limit(messageSetValidBytes.asInstanceOf[Int])
    val validMessages = new ByteBufferMessageSet(validByteBuffer)

    // they are valid, insert them in the log
    lock synchronized {
      try {
        var segment = segments.view.last
        maybeRoll(segment)
        segment = segments.view.last
        segment.append(validMessages)
        maybeFlush(numberOfMessages)
      }
      catch {
        case e: IOException =>
          fatal("Halting due to unrecoverable I/O error while handling producer request", e)
          Runtime.getRuntime.halt(1)
        case e2 => throw e2
      }
    }
  }

  /**
   * Read from the log file at the given offset
   */
  def read(offset: Long, length: Int): MessageSet = {
    trace("Reading %d bytes from offset %d in log %s of length %s bytes".format(length, offset, name, size))
    val view = segments.view
    Log.findRange(view, offset, view.length) match {
      case Some(segment) =>
        if(length <= 0)
          MessageSet.Empty
        else
          segment.messageSet.read((offset - segment.start), length)
      case _ => MessageSet.Empty
    }
  }

  /**
   * Delete any log segments matching the given predicate function
   */
  def markDeletedWhile(predicate: LogSegment => Boolean): Seq[LogSegment] = {
    lock synchronized {
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
          view(numToDelete - 1).file.setLastModified(time.milliseconds)
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
   *  Get the absolute offset of the last message in the log
   */
  def logEndOffset: Long = segments.view.last.start + segments.view.last.size

  /**
   * Roll the log over if necessary
   */
  private def maybeRoll(segment: LogSegment) {
    if ((segment.messageSet.sizeInBytes > maxSize) ||
       ((segment.firstAppendTime.isDefined) && (time.milliseconds - segment.firstAppendTime.get > rollIntervalMs)))
      roll()
  }

  /**
   * Create a new segment and make it active
   */
  def roll() {
    lock synchronized {
      flush
      val newOffset = logEndOffset
      val newFile = new File(dir, nameFromOffset(newOffset))
      if (newFile.exists) {
        warn("newly rolled logsegment " + newFile.getName + " already exists; deleting it first")
        newFile.delete()
      }
      debug("Rolling log '" + name + "' to " + newFile.getName())
      segments.append(new LogSegment(newFile, new FileMessageSet(newFile, true), newOffset, time))
    }
  }

  /**
   * Flush the log if necessary
   */
  private def maybeFlush(numberOfMessages : Int) {
    if(unflushed.addAndGet(numberOfMessages) >= flushInterval) {
      flush()
    }
  }

  /**
   * Flush this log file to the physical disk
   */
  def flush() : Unit = {
    if (unflushed.get == 0) return

    lock synchronized {
      debug("Flushing log '" + name + "' last flushed: " + getLastFlushedTime + " current time: " +
          time.milliseconds)
      segments.view.last.messageSet.flush()
      unflushed.set(0)
      lastflushedTime.set(time.milliseconds)
     }
  }

  def getOffsetsBefore(request: OffsetRequest): Array[Long] = {
    val segsArray = segments.view
    var offsetTimeArray: Array[(Long, Long)] = null
    if (segsArray.last.size > 0)
      offsetTimeArray = new Array[(Long, Long)](segsArray.length + 1)
    else
      offsetTimeArray = new Array[(Long, Long)](segsArray.length)

    for (i <- 0 until segsArray.length)
      offsetTimeArray(i) = (segsArray(i).start, segsArray(i).file.lastModified)
    if (segsArray.last.size > 0)
      offsetTimeArray(segsArray.length) = (segsArray.last.start + segsArray.last.messageSet.sizeInBytes(), time.milliseconds)

    var startIndex = -1
    request.time match {
      case OffsetRequest.LatestTime =>
        startIndex = offsetTimeArray.length - 1
      case OffsetRequest.EarliestTime =>
        startIndex = 0
      case _ =>
        var isFound = false
        debug("Offset time array = " + offsetTimeArray.foreach(o => "%d, %d".format(o._1, o._2)))
        startIndex = offsetTimeArray.length - 1
        while (startIndex >= 0 && !isFound) {
          if (offsetTimeArray(startIndex)._2 <= request.time)
            isFound = true
          else
            startIndex -=1
        }
    }

    val retSize = request.maxNumOffsets.min(startIndex + 1)
    val ret = new Array[Long](retSize)
    for (j <- 0 until retSize) {
      ret(j) = offsetTimeArray(startIndex)._1
      startIndex -= 1
    }
    ret
  }

  /**
   *  Truncate all segments in the log and start a new segment on a new offset
   */
  def truncateAndStartWithNewOffset(newOffset: Long) {
    lock synchronized {
      val deletedSegments = segments.trunc(segments.view.size)
      val newFile = new File(dir, Log.nameFromOffset(newOffset))
      debug("Truncate and start log '" + name + "' to " + newFile.getName())
      segments.append(new LogSegment(newFile, new FileMessageSet(newFile, true), newOffset, time))
      deleteSegments(deletedSegments)
    }
  }


  def deleteWholeLog():Unit = {
    deleteSegments(segments.contents.get())
    Utils.rm(dir)
  }

  /* Attempts to delete all provided segments from a log and returns how many it was able to */
  def deleteSegments(segments: Seq[LogSegment]): Int = {
    var total = 0
    for(segment <- segments) {
      info("Deleting log segment " + segment.file.getName() + " from " + name)
      swallow(segment.messageSet.close())
      if(!segment.file.delete()) {
        warn("Delete failed.")
      } else {
        total += 1
      }
    }
    total
  }

  def truncateTo(targetOffset: Long) {
    // find the log segment that has this hw
    val segmentToBeTruncated = segments.view.find(
      segment => targetOffset >= segment.start && targetOffset < segment.absoluteEndOffset)

    segmentToBeTruncated match {
      case Some(segment) =>
        val truncatedSegmentIndex = segments.view.indexOf(segment)
        segments.truncLast(truncatedSegmentIndex)
        segment.truncateTo(targetOffset)
        info("Truncated log segment %s to highwatermark %d".format(segment.file.getAbsolutePath, targetOffset))
      case None =>
        if(targetOffset > segments.view.last.absoluteEndOffset)
         error("Last checkpointed hw %d cannot be greater than the latest message offset %d in the log %s".format(targetOffset, segments.view.last.absoluteEndOffset, segments.view.last.file.getAbsolutePath))
    }

    val segmentsToBeDeleted = segments.view.filter(segment => segment.start > targetOffset)
    if(segmentsToBeDeleted.size < segments.view.size) {
      val numSegmentsDeleted = deleteSegments(segmentsToBeDeleted)
      if(numSegmentsDeleted != segmentsToBeDeleted.size)
        error("Failed to delete some segments during log recovery")
    }
  }

  def topicName():String = {
    name.substring(0, name.lastIndexOf("-"))
  }

  def getLastFlushedTime():Long = {
    return lastflushedTime.get
  }
}
  
