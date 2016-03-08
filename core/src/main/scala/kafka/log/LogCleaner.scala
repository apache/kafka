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

import java.io.{DataOutputStream, File}
import java.nio._
import java.util.Date
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.yammer.metrics.core.Gauge
import kafka.common._
import kafka.message._
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._

import scala.collection._

/**
 * The cleaner is responsible for removing obsolete records from logs which have the dedupe retention strategy.
 * A message with key K and offset O is obsolete if there exists a message with key K and offset O' such that O < O'.
 * 
 * Each log can be thought of being split into two sections of segments: a "clean" section which has previously been cleaned followed by a
 * "dirty" section that has not yet been cleaned. The active log segment is always excluded from cleaning.
 *
 * The cleaning is carried out by a pool of background threads. Each thread chooses the dirtiest log that has the "dedupe" retention policy 
 * and cleans that. The dirtiness of the log is guessed by taking the ratio of bytes in the dirty section of the log to the total bytes in the log. 
 * 
 * To clean a log the cleaner first builds a mapping of key=>last_offset for the dirty section of the log. See kafka.log.OffsetMap for details of
 * the implementation of the mapping. 
 * 
 * Once the key=>offset map is built, the log is cleaned by recopying each log segment but omitting any key that appears in the offset map with a 
 * higher offset than what is found in the segment (i.e. messages with a key that appears in the dirty section of the log).
 * 
 * To avoid segments shrinking to very small sizes with repeated cleanings we implement a rule by which if we will merge successive segments when
 * doing a cleaning if their log and index size are less than the maximum log and index size prior to the clean beginning.
 * 
 * Cleaned segments are swapped into the log as they become available.
 * 
 * One nuance that the cleaner must handle is log truncation. If a log is truncated while it is being cleaned the cleaning of that log is aborted.
 * 
 * Messages with null payload are treated as deletes for the purpose of log compaction. This means that they receive special treatment by the cleaner. 
 * The cleaner will only retain delete records for a period of time to avoid accumulating space indefinitely. This period of time is configurable on a per-topic
 * basis and is measured from the time the segment enters the clean portion of the log (at which point any prior message with that key has been removed).
 * Delete markers in the clean section of the log that are older than this time will not be retained when log segments are being recopied as part of cleaning.
 * 
 * @param config Configuration parameters for the cleaner
 * @param logDirs The directories where offset checkpoints reside
 * @param logs The pool of logs
 * @param time A way to control the passage of time
 */
class LogCleaner(val config: CleanerConfig,
                 val logDirs: Array[File],
                 val logs: Pool[TopicAndPartition, Log], 
                 time: Time = SystemTime) extends Logging with KafkaMetricsGroup {
  
  /* for managing the state of partitions being cleaned. package-private to allow access in tests */
  private[log] val cleanerManager = new LogCleanerManager(logDirs, logs)

  /* a throttle used to limit the I/O of all the cleaner threads to a user-specified maximum rate */
  private val throttler = new Throttler(desiredRatePerSec = config.maxIoBytesPerSecond, 
                                        checkIntervalMs = 300, 
                                        throttleDown = true, 
                                        "cleaner-io",
                                        "bytes",
                                        time = time)
  
  /* the threads */
  private val cleaners = (0 until config.numThreads).map(new CleanerThread(_))
  
  /* a metric to track the maximum utilization of any thread's buffer in the last cleaning */
  newGauge("max-buffer-utilization-percent", 
           new Gauge[Int] {
             def value: Int = cleaners.map(_.lastStats).map(100 * _.bufferUtilization).max.toInt
           })
  /* a metric to track the recopy rate of each thread's last cleaning */
  newGauge("cleaner-recopy-percent", 
           new Gauge[Int] {
             def value: Int = {
               val stats = cleaners.map(_.lastStats)
               val recopyRate = stats.map(_.bytesWritten).sum.toDouble / math.max(stats.map(_.bytesRead).sum, 1)
               (100 * recopyRate).toInt
             }
           })
  /* a metric to track the maximum cleaning time for the last cleaning from each thread */
  newGauge("max-clean-time-secs",
           new Gauge[Int] {
             def value: Int = cleaners.map(_.lastStats).map(_.elapsedSecs).max.toInt
           })
  
  /**
   * Start the background cleaning
   */
  def startup() {
    info("Starting the log cleaner")
    cleaners.foreach(_.start())
  }
  
  /**
   * Stop the background cleaning
   */
  def shutdown() {
    info("Shutting down the log cleaner.")
    cleaners.foreach(_.shutdown())
  }
  
  /**
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   */
  def abortCleaning(topicAndPartition: TopicAndPartition) {
    cleanerManager.abortCleaning(topicAndPartition)
  }

  /**
   * Update checkpoint file, removing topics and partitions that no longer exist
   */
  def updateCheckpoints(dataDir: File) {
    cleanerManager.updateCheckpoints(dataDir, update=None)
  }

  /**
   * Truncate cleaner offset checkpoint for the given partition if its checkpointed offset is larger than the given offset
   */
  def maybeTruncateCheckpoint(dataDir: File, topicAndPartition: TopicAndPartition, offset: Long) {
    cleanerManager.maybeTruncateCheckpoint(dataDir, topicAndPartition, offset)
  }

  /**
   *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
   *  This call blocks until the cleaning of the partition is aborted and paused.
   */
  def abortAndPauseCleaning(topicAndPartition: TopicAndPartition) {
    cleanerManager.abortAndPauseCleaning(topicAndPartition)
  }

  /**
   *  Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
   */
  def resumeCleaning(topicAndPartition: TopicAndPartition) {
    cleanerManager.resumeCleaning(topicAndPartition)
  }

  /**
   * For testing, a way to know when work has completed. This method waits until the
   * cleaner has processed up to the given offset on the specified topic/partition
   *
   * @param topic The Topic to be cleaned
   * @param part The partition of the topic to be cleaned
   * @param offset The first dirty offset that the cleaner doesn't have to clean
   * @param maxWaitMs The maximum time in ms to wait for cleaner
   *
   * @return A boolean indicating whether the work has completed before timeout
   */
  def awaitCleaned(topic: String, part: Int, offset: Long, maxWaitMs: Long = 60000L): Boolean = {
    def isCleaned = cleanerManager.allCleanerCheckpoints.get(TopicAndPartition(topic, part)).fold(false)(_ >= offset)
    var remainingWaitMs = maxWaitMs
    while (!isCleaned && remainingWaitMs > 0) {
      val sleepTime = math.min(100, remainingWaitMs)
      Thread.sleep(sleepTime)
      remainingWaitMs -= sleepTime
    }
    isCleaned
  }
  
  /**
   * The cleaner threads do the actual log cleaning. Each thread processes does its cleaning repeatedly by
   * choosing the dirtiest log, cleaning it, and then swapping in the cleaned segments.
   */
  private class CleanerThread(threadId: Int)
    extends ShutdownableThread(name = "kafka-log-cleaner-thread-" + threadId, isInterruptible = false) {
    
    override val loggerName = classOf[LogCleaner].getName
    
    if(config.dedupeBufferSize / config.numThreads > Int.MaxValue)
      warn("Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...")

    val cleaner = new Cleaner(id = threadId,
                              offsetMap = new SkimpyOffsetMap(memory = math.min(config.dedupeBufferSize / config.numThreads, Int.MaxValue).toInt, 
                                                              hashAlgorithm = config.hashAlgorithm),
                              ioBufferSize = config.ioBufferSize / config.numThreads / 2,
                              maxIoBufferSize = config.maxMessageSize,
                              dupBufferLoadFactor = config.dedupeBufferLoadFactor,
                              throttler = throttler,
                              time = time,
                              checkDone = checkDone)
    
    @volatile var lastStats: CleanerStats = new CleanerStats()
    private val backOffWaitLatch = new CountDownLatch(1)

    private def checkDone(topicAndPartition: TopicAndPartition) {
      if (!isRunning.get())
        throw new ThreadShutdownException
      cleanerManager.checkCleaningAborted(topicAndPartition)
    }

    /**
     * The main loop for the cleaner thread
     */
    override def doWork() {
      cleanOrSleep()
    }
    
    
    override def shutdown() = {
    	 initiateShutdown()
    	 backOffWaitLatch.countDown()
    	 awaitShutdown()
     }
     
    /**
     * Clean a log if there is a dirty log available, otherwise sleep for a bit
     */
    private def cleanOrSleep() {
      cleanerManager.grabFilthiestLog() match {
        case None =>
          // there are no cleanable logs, sleep a while
          backOffWaitLatch.await(config.backOffMs, TimeUnit.MILLISECONDS)
        case Some(cleanable) =>
          // there's a log, clean it
          var endOffset = cleanable.firstDirtyOffset
          try {
            endOffset = cleaner.clean(cleanable)
            recordStats(cleaner.id, cleanable.log.name, cleanable.firstDirtyOffset, endOffset, cleaner.stats)
          } catch {
            case pe: LogCleaningAbortedException => // task can be aborted, let it go.
          } finally {
            cleanerManager.doneCleaning(cleanable.topicPartition, cleanable.log.dir.getParentFile, endOffset)
          }
      }
    }
    
    /**
     * Log out statistics on a single run of the cleaner.
     */
    def recordStats(id: Int, name: String, from: Long, to: Long, stats: CleanerStats) {
      this.lastStats = stats
      cleaner.statsUnderlying.swap
      def mb(bytes: Double) = bytes / (1024*1024)
      val message = 
        "%n\tLog cleaner thread %d cleaned log %s (dirty section = [%d, %d])%n".format(id, name, from, to) + 
        "\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n".format(mb(stats.bytesRead), 
                                                                                stats.elapsedSecs, 
                                                                                mb(stats.bytesRead/stats.elapsedSecs)) + 
        "\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.mapBytesRead), 
                                                                                           stats.elapsedIndexSecs, 
                                                                                           mb(stats.mapBytesRead)/stats.elapsedIndexSecs, 
                                                                                           100 * stats.elapsedIndexSecs/stats.elapsedSecs) +
        "\tBuffer utilization: %.1f%%%n".format(100 * stats.bufferUtilization) +
        "\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.bytesRead), 
                                                                                           stats.elapsedSecs - stats.elapsedIndexSecs, 
                                                                                           mb(stats.bytesRead)/(stats.elapsedSecs - stats.elapsedIndexSecs), 100 * (stats.elapsedSecs - stats.elapsedIndexSecs).toDouble/stats.elapsedSecs) + 
        "\tStart size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesRead), stats.messagesRead) +
        "\tEnd size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesWritten), stats.messagesWritten) + 
        "\t%.1f%% size reduction (%.1f%% fewer messages)%n".format(100.0 * (1.0 - stats.bytesWritten.toDouble/stats.bytesRead), 
                                                                   100.0 * (1.0 - stats.messagesWritten.toDouble/stats.messagesRead))
      info(message)
      if (stats.invalidMessagesRead > 0) {
        warn("\tFound %d invalid messages during compaction.".format(stats.invalidMessagesRead))
      }
    }
   
  }
}

/**
 * This class holds the actual logic for cleaning a log
 * @param id An identifier used for logging
 * @param offsetMap The map used for deduplication
 * @param ioBufferSize The size of the buffers to use. Memory usage will be 2x this number as there is a read and write buffer.
 * @param maxIoBufferSize The maximum size of a message that can appear in the log
 * @param dupBufferLoadFactor The maximum percent full for the deduplication buffer
 * @param throttler The throttler instance to use for limiting I/O rate.
 * @param time The time instance
 * @param checkDone Check if the cleaning for a partition is finished or aborted.
 */
private[log] class Cleaner(val id: Int,
                           val offsetMap: OffsetMap,
                           ioBufferSize: Int,
                           maxIoBufferSize: Int,
                           dupBufferLoadFactor: Double,
                           throttler: Throttler,
                           time: Time,
                           checkDone: (TopicAndPartition) => Unit) extends Logging {
  
  override val loggerName = classOf[LogCleaner].getName

  this.logIdent = "Cleaner " + id + ": "
  
  /* cleaning stats - one instance for the current (or next) cleaning cycle and one for the last completed cycle */
  val statsUnderlying = (new CleanerStats(time), new CleanerStats(time))
  def stats = statsUnderlying._1

  /* buffer used for read i/o */
  private var readBuffer = ByteBuffer.allocate(ioBufferSize)
  
  /* buffer used for write i/o */
  private var writeBuffer = ByteBuffer.allocate(ioBufferSize)

  /**
   * Clean the given log
   *
   * @param cleanable The log to be cleaned
   *
   * @return The first offset not cleaned
   */
  private[log] def clean(cleanable: LogToClean): Long = {
    stats.clear()
    info("Beginning cleaning of log %s.".format(cleanable.log.name))
    val log = cleanable.log

    // build the offset map
    info("Building offset map for %s...".format(cleanable.log.name))
    val upperBoundOffset = log.activeSegment.baseOffset
    val endOffset = buildOffsetMap(log, cleanable.firstDirtyOffset, upperBoundOffset, offsetMap) + 1
    stats.indexDone()
    
    // figure out the timestamp below which it is safe to remove delete tombstones
    // this position is defined to be a configurable time beneath the last modified time of the last clean segment
    val deleteHorizonMs = 
      log.logSegments(0, cleanable.firstDirtyOffset).lastOption match {
        case None => 0L
        case Some(seg) => seg.lastModified - log.config.deleteRetentionMs
    }
        
    // group the segments and clean the groups
    info("Cleaning log %s (discarding tombstones prior to %s)...".format(log.name, new Date(deleteHorizonMs)))
    for (group <- groupSegmentsBySize(log.logSegments(0, endOffset), log.config.segmentSize, log.config.maxIndexSize))
      cleanSegments(log, group, offsetMap, deleteHorizonMs)
      
    // record buffer utilization
    stats.bufferUtilization = offsetMap.utilization
    
    stats.allDone()

    endOffset
  }

  /**
   * Clean a group of segments into a single replacement segment
   *
   * @param log The log being cleaned
   * @param segments The group of segments being cleaned
   * @param map The offset map to use for cleaning segments
   * @param deleteHorizonMs The time to retain delete tombstones
   */
  private[log] def cleanSegments(log: Log,
                                 segments: Seq[LogSegment], 
                                 map: OffsetMap, 
                                 deleteHorizonMs: Long) {
    // create a new segment with the suffix .cleaned appended to both the log and index name
    val logFile = new File(segments.head.log.file.getPath + Log.CleanedFileSuffix)
    logFile.delete()
    val indexFile = new File(segments.head.index.file.getPath + Log.CleanedFileSuffix)
    indexFile.delete()
    val messages = new FileMessageSet(logFile, fileAlreadyExists = false, initFileSize = log.initFileSize(), preallocate = log.config.preallocate)
    val index = new OffsetIndex(indexFile, segments.head.baseOffset, segments.head.index.maxIndexSize)
    val cleaned = new LogSegment(messages, index, segments.head.baseOffset, segments.head.indexIntervalBytes, log.config.randomSegmentJitter, time)

    try {
      // clean segments into the new destination segment
      for (old <- segments) {
        val retainDeletes = old.lastModified > deleteHorizonMs
        info("Cleaning segment %s in log %s (last modified %s) into %s, %s deletes."
            .format(old.baseOffset, log.name, new Date(old.lastModified), cleaned.baseOffset, if(retainDeletes) "retaining" else "discarding"))
        cleanInto(log.topicAndPartition, old, cleaned, map, retainDeletes, log.config.messageFormatVersion.messageFormatVersion)
      }

      // trim excess index
      index.trimToValidSize()

      // flush new segment to disk before swap
      cleaned.flush()

      // update the modification date to retain the last modified date of the original files
      val modified = segments.last.lastModified
      cleaned.lastModified = modified

      // swap in new segment
      info("Swapping in cleaned segment %d for segment(s) %s in log %s.".format(cleaned.baseOffset, segments.map(_.baseOffset).mkString(","), log.name))
      log.replaceSegments(cleaned, segments)
    } catch {
      case e: LogCleaningAbortedException =>
        cleaned.delete()
        throw e
    }
  }

  /**
   * Clean the given source log segment into the destination segment using the key=>offset mapping
   * provided
   *
   * @param source The dirty log segment
   * @param dest The cleaned log segment
   * @param map The key=>offset mapping
   * @param retainDeletes Should delete tombstones be retained while cleaning this segment
   * @param messageFormatVersion the message format version to use after compaction
   */
  private[log] def cleanInto(topicAndPartition: TopicAndPartition,
                             source: LogSegment,
                             dest: LogSegment,
                             map: OffsetMap,
                             retainDeletes: Boolean,
                             messageFormatVersion: Byte) {
    var position = 0
    while (position < source.log.sizeInBytes) {
      checkDone(topicAndPartition)
      // read a chunk of messages and copy any that are to be retained to the write buffer to be written out
      readBuffer.clear()
      writeBuffer.clear()
      val messages = new ByteBufferMessageSet(source.log.readInto(readBuffer, position))
      throttler.maybeThrottle(messages.sizeInBytes)
      // check each message to see if it is to be retained
      var messagesRead = 0
      for (entry <- messages.shallowIterator) {
        val size = MessageSet.entrySize(entry.message)
        stats.readMessage(size)
        if (entry.message.compressionCodec == NoCompressionCodec) {
          if (shouldRetainMessage(source, map, retainDeletes, entry)) {
            val convertedMessage = entry.message.toFormatVersion(messageFormatVersion)
            ByteBufferMessageSet.writeMessage(writeBuffer, convertedMessage, entry.offset)
            stats.recopyMessage(size)
          }
          messagesRead += 1
        } else {
          // We use the absolute offset to decide whether to retain the message or not. This is handled by the
          // deep iterator.
          val messages = ByteBufferMessageSet.deepIterator(entry)
          var writeOriginalMessageSet = true
          val retainedMessages = new mutable.ArrayBuffer[MessageAndOffset]
          messages.foreach { messageAndOffset =>
            messagesRead += 1
            if (shouldRetainMessage(source, map, retainDeletes, messageAndOffset)) {
              retainedMessages += {
                if (messageAndOffset.message.magic != messageFormatVersion) {
                  writeOriginalMessageSet = false
                  new MessageAndOffset(messageAndOffset.message.toFormatVersion(messageFormatVersion), messageAndOffset.offset)
                }
                else messageAndOffset
              }
            }
            else writeOriginalMessageSet = false
          }

          // There are no messages compacted out and no message format conversion, write the original message set back
          if (writeOriginalMessageSet)
            ByteBufferMessageSet.writeMessage(writeBuffer, entry.message, entry.offset)
          else if (retainedMessages.nonEmpty)
            compressMessages(writeBuffer, entry.message.compressionCodec, messageFormatVersion, retainedMessages)
        }
      }

      position += messages.validBytes
      // if any messages are to be retained, write them out
      if (writeBuffer.position > 0) {
        writeBuffer.flip()
        val retained = new ByteBufferMessageSet(writeBuffer)
        dest.append(retained.head.offset, retained)
        throttler.maybeThrottle(writeBuffer.limit)
      }
      
      // if we read bytes but didn't get even one complete message, our I/O buffer is too small, grow it and try again
      if (readBuffer.limit > 0 && messagesRead == 0)
        growBuffers()
    }
    restoreBuffers()
  }

  private def compressMessages(buffer: ByteBuffer,
                               compressionCodec: CompressionCodec,
                               messageFormatVersion: Byte,
                               messageAndOffsets: Seq[MessageAndOffset]) {
    val messages = messageAndOffsets.map(_.message)
    if (messageAndOffsets.isEmpty) {
      MessageSet.Empty.sizeInBytes
    } else if (compressionCodec == NoCompressionCodec) {
      for (messageOffset <- messageAndOffsets)
        ByteBufferMessageSet.writeMessage(buffer, messageOffset.message, messageOffset.offset)
      MessageSet.messageSetSize(messages)
    } else {
      val magicAndTimestamp = MessageSet.magicAndLargestTimestamp(messages)
      val firstMessageOffset = messageAndOffsets.head
      val firstAbsoluteOffset = firstMessageOffset.offset
      var offset = -1L
      val timestampType = firstMessageOffset.message.timestampType
      val messageWriter = new MessageWriter(math.min(math.max(MessageSet.messageSetSize(messages) / 2, 1024), 1 << 16))
      messageWriter.write(codec = compressionCodec, timestamp = magicAndTimestamp.timestamp, timestampType = timestampType, magicValue = messageFormatVersion) { outputStream =>
        val output = new DataOutputStream(CompressionFactory(compressionCodec, outputStream))
        try {
          for (messageOffset <- messageAndOffsets) {
            val message = messageOffset.message
            offset = messageOffset.offset
            if (messageFormatVersion > Message.MagicValue_V0) {
              // The offset of the messages are absolute offset, compute the inner offset.
              val innerOffset = messageOffset.offset - firstAbsoluteOffset
              output.writeLong(innerOffset)
            } else
              output.writeLong(offset)
            output.writeInt(message.size)
            output.write(message.buffer.array, message.buffer.arrayOffset, message.buffer.limit)
          }
        } finally {
          output.close()
        }
      }
      ByteBufferMessageSet.writeMessage(buffer, messageWriter, offset)
      stats.recopyMessage(messageWriter.size + MessageSet.LogOverhead)
    }
  }

  private def shouldRetainMessage(source: kafka.log.LogSegment,
                                  map: kafka.log.OffsetMap,
                                  retainDeletes: Boolean,
                                  entry: kafka.message.MessageAndOffset): Boolean = {
    val key = entry.message.key
    if (key != null) {
      val foundOffset = map.get(key)
      /* two cases in which we can get rid of a message:
       *   1) if there exists a message with the same key but higher offset
       *   2) if the message is a delete "tombstone" marker and enough time has passed
       */
      val redundant = foundOffset >= 0 && entry.offset < foundOffset
      val obsoleteDelete = !retainDeletes && entry.message.isNull
      !redundant && !obsoleteDelete
    } else {
      stats.invalidMessage()
      false
    }
  }

  /**
   * Double the I/O buffer capacity
   */
  def growBuffers() {
    if(readBuffer.capacity >= maxIoBufferSize || writeBuffer.capacity >= maxIoBufferSize)
      throw new IllegalStateException("This log contains a message larger than maximum allowable size of %s.".format(maxIoBufferSize))
    val newSize = math.min(this.readBuffer.capacity * 2, maxIoBufferSize)
    info("Growing cleaner I/O buffers from " + readBuffer.capacity + "bytes to " + newSize + " bytes.")
    this.readBuffer = ByteBuffer.allocate(newSize)
    this.writeBuffer = ByteBuffer.allocate(newSize)
  }
  
  /**
   * Restore the I/O buffer capacity to its original size
   */
  def restoreBuffers() {
    if(this.readBuffer.capacity > this.ioBufferSize)
      this.readBuffer = ByteBuffer.allocate(this.ioBufferSize)
    if(this.writeBuffer.capacity > this.ioBufferSize)
      this.writeBuffer = ByteBuffer.allocate(this.ioBufferSize)
  }

  /**
   * Group the segments in a log into groups totaling less than a given size. the size is enforced separately for the log data and the index data.
   * We collect a group of such segments together into a single
   * destination segment. This prevents segment sizes from shrinking too much.
   *
   * @param segments The log segments to group
   * @param maxSize the maximum size in bytes for the total of all log data in a group
   * @param maxIndexSize the maximum size in bytes for the total of all index data in a group
   *
   * @return A list of grouped segments
   */
  private[log] def groupSegmentsBySize(segments: Iterable[LogSegment], maxSize: Int, maxIndexSize: Int): List[Seq[LogSegment]] = {
    var grouped = List[List[LogSegment]]()
    var segs = segments.toList
    while(!segs.isEmpty) {
      var group = List(segs.head)
      var logSize = segs.head.size
      var indexSize = segs.head.index.sizeInBytes
      segs = segs.tail
      while(!segs.isEmpty &&
            logSize + segs.head.size <= maxSize &&
            indexSize + segs.head.index.sizeInBytes <= maxIndexSize &&
            segs.head.index.lastOffset - group.last.index.baseOffset <= Int.MaxValue) {
        group = segs.head :: group
        logSize += segs.head.size
        indexSize += segs.head.index.sizeInBytes
        segs = segs.tail
      }
      grouped ::= group.reverse
    }
    grouped.reverse
  }

  /**
   * Build a map of key_hash => offset for the keys in the dirty portion of the log to use in cleaning.
   * @param log The log to use
   * @param start The offset at which dirty messages begin
   * @param end The ending offset for the map that is being built
   * @param map The map in which to store the mappings
   *
   * @return The final offset the map covers
   */
  private[log] def buildOffsetMap(log: Log, start: Long, end: Long, map: OffsetMap): Long = {
    map.clear()
    val dirty = log.logSegments(start, end).toSeq
    info("Building offset map for log %s for %d segments in offset range [%d, %d).".format(log.name, dirty.size, start, end))
    
    // Add all the dirty segments. We must take at least map.slots * load_factor,
    // but we may be able to fit more (if there is lots of duplication in the dirty section of the log)
    var offset = dirty.head.baseOffset
    require(offset == start, "Last clean offset is %d but segment base offset is %d for log %s.".format(start, offset, log.name))
    val maxDesiredMapSize = (map.slots * this.dupBufferLoadFactor).toInt
    var full = false
    for (segment <- dirty if !full) {
      checkDone(log.topicAndPartition)
      val segmentSize = segment.nextOffset() - segment.baseOffset

      require(segmentSize <= maxDesiredMapSize, "%d messages in segment %s/%s but offset map can fit only %d. You can increase log.cleaner.dedupe.buffer.size or decrease log.cleaner.threads".format(segmentSize,  log.name, segment.log.file.getName, maxDesiredMapSize))
      if (map.size + segmentSize <= maxDesiredMapSize)
        offset = buildOffsetMapForSegment(log.topicAndPartition, segment, map)
      else
        full = true
    }
    info("Offset map for log %s complete.".format(log.name))
    offset
  }

  /**
   * Add the messages in the given segment to the offset map
   *
   * @param segment The segment to index
   * @param map The map in which to store the key=>offset mapping
   *
   * @return The final offset covered by the map
   */
  private def buildOffsetMapForSegment(topicAndPartition: TopicAndPartition, segment: LogSegment, map: OffsetMap): Long = {
    var position = 0
    var offset = segment.baseOffset
    while (position < segment.log.sizeInBytes) {
      checkDone(topicAndPartition)
      readBuffer.clear()
      val messages = new ByteBufferMessageSet(segment.log.readInto(readBuffer, position))
      throttler.maybeThrottle(messages.sizeInBytes)
      val startPosition = position
      for (entry <- messages) {
        val message = entry.message
        if (message.hasKey)
          map.put(message.key, entry.offset)
        offset = entry.offset
        stats.indexMessagesRead(1)
      }
      position += messages.validBytes
      stats.indexBytesRead(messages.validBytes)

      // if we didn't read even one complete message, our read buffer may be too small
      if(position == startPosition)
        growBuffers()
    }
    restoreBuffers()
    offset
  }
}

/**
 * A simple struct for collecting stats about log cleaning
 */
private case class CleanerStats(time: Time = SystemTime) {
  var startTime, mapCompleteTime, endTime, bytesRead, bytesWritten, mapBytesRead, mapMessagesRead, messagesRead,
      messagesWritten, invalidMessagesRead = 0L
  var bufferUtilization = 0.0d
  clear()
  
  def readMessage(size: Int) {
    messagesRead += 1
    bytesRead += size
  }

  def invalidMessage() {
    invalidMessagesRead += 1
  }
  
  def recopyMessage(size: Int) {
    messagesWritten += 1
    bytesWritten += size
  }

  def indexMessagesRead(size: Int) {
    mapMessagesRead += size
  }

  def indexBytesRead(size: Int) {
    mapBytesRead += size
  }

  def indexDone() {
    mapCompleteTime = time.milliseconds
  }

  def allDone() {
    endTime = time.milliseconds
  }
  
  def elapsedSecs = (endTime - startTime)/1000.0
  
  def elapsedIndexSecs = (mapCompleteTime - startTime)/1000.0
  
  def clear() {
    startTime = time.milliseconds
    mapCompleteTime = -1L
    endTime = -1L
    bytesRead = 0L
    bytesWritten = 0L
    mapBytesRead = 0L
    mapMessagesRead = 0L
    messagesRead = 0L
    invalidMessagesRead = 0L
    messagesWritten = 0L
    bufferUtilization = 0.0d
  }
}

/**
 * Helper class for a log, its topic/partition, and the last clean position
 */
private case class LogToClean(topicPartition: TopicAndPartition, log: Log, firstDirtyOffset: Long) extends Ordered[LogToClean] {
  val cleanBytes = log.logSegments(-1, firstDirtyOffset).map(_.size).sum
  val dirtyBytes = log.logSegments(firstDirtyOffset, math.max(firstDirtyOffset, log.activeSegment.baseOffset)).map(_.size).sum
  val cleanableRatio = dirtyBytes / totalBytes.toDouble
  def totalBytes = cleanBytes + dirtyBytes
  override def compare(that: LogToClean): Int = math.signum(this.cleanableRatio - that.cleanableRatio).toInt
}
