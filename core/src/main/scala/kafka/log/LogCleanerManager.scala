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

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kafka.common.LogCleaningAbortedException
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.utils.CoreUtils._
import kafka.utils.{Logging, Pool}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.utils.Time
import org.apache.kafka.storage.internals.log.LogDirFailureChannel
import org.apache.kafka.server.metrics.KafkaMetricsGroup

import java.util.Comparator
import scala.collection.{Iterable, Seq, mutable}
import scala.jdk.CollectionConverters._

private[log] sealed trait LogCleaningState
private[log] case object LogCleaningInProgress extends LogCleaningState
private[log] case object LogCleaningAborted extends LogCleaningState
private[log] case class LogCleaningPaused(pausedCount: Int) extends LogCleaningState

private[log] class LogCleaningException(val log: UnifiedLog,
                                        private val message: String,
                                        private val cause: Throwable) extends KafkaException(message, cause)

/**
  * This class manages the state of each partition being cleaned.
  * LogCleaningState defines the cleaning states that a TopicPartition can be in.
  * 1. None                    : No cleaning state in a TopicPartition. In this state, it can become LogCleaningInProgress
  *                              or LogCleaningPaused(1). Valid previous state are LogCleaningInProgress and LogCleaningPaused(1)
  * 2. LogCleaningInProgress   : The cleaning is currently in progress. In this state, it can become None when log cleaning is finished
  *                              or become LogCleaningAborted. Valid previous state is None.
  * 3. LogCleaningAborted      : The cleaning abort is requested. In this state, it can become LogCleaningPaused(1).
  *                              Valid previous state is LogCleaningInProgress.
  * 4-a. LogCleaningPaused(1)  : The cleaning is paused once. No log cleaning can be done in this state.
  *                              In this state, it can become None or LogCleaningPaused(2).
  *                              Valid previous state is None, LogCleaningAborted or LogCleaningPaused(2).
  * 4-b. LogCleaningPaused(i)  : The cleaning is paused i times where i>= 2. No log cleaning can be done in this state.
  *                              In this state, it can become LogCleaningPaused(i-1) or LogCleaningPaused(i+1).
  *                              Valid previous state is LogCleaningPaused(i-1) or LogCleaningPaused(i+1).
  */
private[log] class LogCleanerManager(val logDirs: Seq[File],
                                     val logs: Pool[TopicPartition, UnifiedLog],
                                     val logDirFailureChannel: LogDirFailureChannel) extends Logging {
  import LogCleanerManager._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  protected override def loggerName: String = classOf[LogCleaner].getName

  // package-private for testing
  private[log] val offsetCheckpointFile = "cleaner-offset-checkpoint"

  /* the offset checkpoints holding the last cleaned point for each log */
  @volatile private var checkpoints = logDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, offsetCheckpointFile), logDirFailureChannel))).toMap

  /* the set of logs currently being cleaned */
  private val inProgress = mutable.HashMap[TopicPartition, LogCleaningState]()

  /* the set of uncleanable partitions (partitions that have raised an unexpected error during cleaning)
   *   for each log directory */
  private val uncleanablePartitions = mutable.HashMap[String, mutable.Set[TopicPartition]]()

  /* a global lock used to control all access to the in-progress set and the offset checkpoints */
  private val lock = new ReentrantLock

  /* for coordinating the pausing and the cleaning of a partition */
  private val pausedCleaningCond = lock.newCondition()

  // Visible for testing
  private[log] val gaugeMetricNameWithTag = new java.util.HashMap[String, java.util.List[java.util.Map[String, String]]]()

  /* gauges for tracking the number of partitions marked as uncleanable for each log directory */
  for (dir <- logDirs) {
    val metricTag = Map("logDirectory" -> dir.getAbsolutePath).asJava
    metricsGroup.newGauge(UncleanablePartitionsCountMetricName,
      () => inLock(lock) { uncleanablePartitions.get(dir.getAbsolutePath).map(_.size).getOrElse(0) },
      metricTag
    )
    gaugeMetricNameWithTag.computeIfAbsent(UncleanablePartitionsCountMetricName, _ => new java.util.ArrayList[java.util.Map[String, String]]())
      .add(metricTag)
  }

  /* gauges for tracking the number of uncleanable bytes from uncleanable partitions for each log directory */
  for (dir <- logDirs) {
    val metricTag = Map("logDirectory" -> dir.getAbsolutePath).asJava
    metricsGroup.newGauge(UncleanableBytesMetricName,
      () => inLock(lock) {
        uncleanablePartitions.get(dir.getAbsolutePath) match {
          case Some(partitions) =>
            val lastClean = allCleanerCheckpoints
            val now = Time.SYSTEM.milliseconds
            partitions.iterator.map { tp =>
              Option(logs.get(tp)).map {
                log =>
                  val lastCleanOffset = lastClean.get(tp)
                  val offsetsToClean = cleanableOffsets(log, lastCleanOffset, now)
                  val (_, uncleanableBytes) = calculateCleanableBytes(log, offsetsToClean.firstDirtyOffset, offsetsToClean.firstUncleanableDirtyOffset)
                  uncleanableBytes
              }.getOrElse(0L)
            }.sum
          case None => 0
        }
      },
      metricTag
    )
    gaugeMetricNameWithTag.computeIfAbsent(UncleanableBytesMetricName, _ => new java.util.ArrayList[java.util.Map[String, String]]())
      .add(metricTag)
  }

  /* a gauge for tracking the cleanable ratio of the dirtiest log */
  @volatile private var dirtiestLogCleanableRatio = 0.0
  metricsGroup.newGauge(MaxDirtyPercentMetricName, () => (100 * dirtiestLogCleanableRatio).toInt)

  /* a gauge for tracking the time since the last log cleaner run, in milli seconds */
  @volatile private var timeOfLastRun: Long = Time.SYSTEM.milliseconds
  metricsGroup.newGauge(TimeSinceLastRunMsMetricName, () => Time.SYSTEM.milliseconds - timeOfLastRun)

  /**
   * @return the position processed for all logs.
   */
  def allCleanerCheckpoints: Map[TopicPartition, Long] = {
    inLock(lock) {
      checkpoints.values.flatMap(checkpoint => {
        try {
          checkpoint.read()
        } catch {
          case e: KafkaStorageException =>
            error(s"Failed to access checkpoint file ${checkpoint.file.getName} in dir ${checkpoint.file.getParentFile.getAbsolutePath}", e)
            Map.empty[TopicPartition, Long]
        }
      }).toMap
    }
  }

  /**
    * Package private for unit test. Get the cleaning state of the partition.
    */
  private[log] def cleaningState(tp: TopicPartition): Option[LogCleaningState] = {
    inLock(lock) {
      inProgress.get(tp)
    }
  }

  /**
    * Package private for unit test. Set the cleaning state of the partition.
    */
  private[log] def setCleaningState(tp: TopicPartition, state: LogCleaningState): Unit = {
    inLock(lock) {
      inProgress.put(tp, state)
    }
  }

   /**
    * Choose the log to clean next and add it to the in-progress set. We recompute this
    * each time from the full set of logs to allow logs to be dynamically added to the pool of logs
    * the log manager maintains.
    */
  def grabFilthiestCompactedLog(time: Time, preCleanStats: PreCleanStats = new PreCleanStats()): Option[LogToClean] = {
    inLock(lock) {
      val now = time.milliseconds
      this.timeOfLastRun = now
      val lastClean = allCleanerCheckpoints

      val dirtyLogs = logs.filter {
        case (_, log) => log.config.compact
      }.filterNot {
        case (topicPartition, log) =>
          inProgress.contains(topicPartition) || isUncleanablePartition(log, topicPartition)
      }.map {
        case (topicPartition, log) => // create a LogToClean instance for each
          try {
            val lastCleanOffset = lastClean.get(topicPartition)
            val offsetsToClean = cleanableOffsets(log, lastCleanOffset, now)
            // update checkpoint for logs with invalid checkpointed offsets
            if (offsetsToClean.forceUpdateCheckpoint)
              updateCheckpoints(log.parentDirFile, partitionToUpdateOrAdd = Option(topicPartition, offsetsToClean.firstDirtyOffset))
            val compactionDelayMs = maxCompactionDelay(log, offsetsToClean.firstDirtyOffset, now)
            preCleanStats.updateMaxCompactionDelay(compactionDelayMs)

            LogToClean(topicPartition, log, offsetsToClean.firstDirtyOffset, offsetsToClean.firstUncleanableDirtyOffset, compactionDelayMs > 0)
          } catch {
            case e: Throwable => throw new LogCleaningException(log,
              s"Failed to calculate log cleaning stats for partition $topicPartition", e)
          }
      }.filter(ltc => ltc.totalBytes > 0) // skip any empty logs

      this.dirtiestLogCleanableRatio = if (dirtyLogs.nonEmpty) dirtyLogs.max.cleanableRatio else 0
      // and must meet the minimum threshold for dirty byte ratio or have some bytes required to be compacted
      val cleanableLogs = dirtyLogs.filter { ltc =>
        (ltc.needCompactionNow && ltc.cleanableBytes > 0) || ltc.cleanableRatio > ltc.log.config.minCleanableRatio
      }

      if (cleanableLogs.isEmpty)
        None
      else {
        preCleanStats.recordCleanablePartitions(cleanableLogs.size)
        val filthiest = cleanableLogs.max
        inProgress.put(filthiest.topicPartition, LogCleaningInProgress)
        Some(filthiest)
      }
    }
  }

  /**
    * Pause logs cleaning for logs that do not have compaction enabled
    * and do not have other deletion or compaction in progress.
    * This is to handle potential race between retention and cleaner threads when users
    * switch topic configuration between compacted and non-compacted topic.
    * @return retention logs that have log cleaning successfully paused
    */
  def pauseCleaningForNonCompactedPartitions(): Iterable[(TopicPartition, UnifiedLog)] = {
    inLock(lock) {
      val deletableLogs = logs.filter {
        case (_, log) => !log.config.compact // pick non-compacted logs
      }.filterNot {
        case (topicPartition, _) => inProgress.contains(topicPartition) // skip any logs already in-progress
      }

      deletableLogs.foreach {
        case (topicPartition, _) => inProgress.put(topicPartition, LogCleaningPaused(1))
      }
      deletableLogs
    }
  }

  /**
    * Find any logs that have compaction enabled. Mark them as being cleaned
    * Include logs without delete enabled, as they may have segments
    * that precede the start offset.
    */
  def deletableLogs(): Iterable[(TopicPartition, UnifiedLog)] = {
    inLock(lock) {
      val toClean = logs.filter { case (topicPartition, log) =>
        !inProgress.contains(topicPartition) && log.config.compact &&
          !isUncleanablePartition(log, topicPartition)
      }
      toClean.foreach { case (tp, _) => inProgress.put(tp, LogCleaningInProgress) }
      toClean
    }

  }

  /**
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   *  This is implemented by first abortAndPausing and then resuming the cleaning of the partition.
   */
  def abortCleaning(topicPartition: TopicPartition): Unit = {
    inLock(lock) {
      abortAndPauseCleaning(topicPartition)
      resumeCleaning(Seq(topicPartition))
    }
  }

  /**
   *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
   *  This call blocks until the cleaning of the partition is aborted and paused.
   *  1. If the partition is not in progress, mark it as paused.
   *  2. Otherwise, first mark the state of the partition as aborted.
   *  3. The cleaner thread checks the state periodically and if it sees the state of the partition is aborted, it
   *     throws a LogCleaningAbortedException to stop the cleaning task.
   *  4. When the cleaning task is stopped, doneCleaning() is called, which sets the state of the partition as paused.
   *  5. abortAndPauseCleaning() waits until the state of the partition is changed to paused.
   *  6. If the partition is already paused, a new call to this function
   *     will increase the paused count by one.
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition): Unit = {
    inLock(lock) {
      inProgress.get(topicPartition) match {
        case None =>
          inProgress.put(topicPartition, LogCleaningPaused(1))
        case Some(LogCleaningInProgress) =>
          inProgress.put(topicPartition, LogCleaningAborted)
        case Some(LogCleaningPaused(count)) =>
          inProgress.put(topicPartition, LogCleaningPaused(count + 1))
        case Some(s) =>
          throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be aborted and paused since it is in $s state.")
      }
      while (!isCleaningInStatePaused(topicPartition))
        pausedCleaningCond.await(100, TimeUnit.MILLISECONDS)
    }
  }

  /**
    *  Resume the cleaning of paused partitions.
    *  Each call of this function will undo one pause.
    */
  def resumeCleaning(topicPartitions: Iterable[TopicPartition]): Unit = {
    inLock(lock) {
      topicPartitions.foreach {
        topicPartition =>
          inProgress.get(topicPartition) match {
            case None =>
              throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be resumed since it is not paused.")
            case Some(state) =>
              state match {
                case LogCleaningPaused(count) if count == 1 =>
                  inProgress.remove(topicPartition)
                case LogCleaningPaused(count) if count > 1 =>
                  inProgress.put(topicPartition, LogCleaningPaused(count - 1))
                case s =>
                  throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be resumed since it is in $s state.")
              }
          }
      }
    }
  }

  /**
   *  Check if the cleaning for a partition is in a particular state. The caller is expected to hold lock while making the call.
   */
  private def isCleaningInState(topicPartition: TopicPartition, expectedState: LogCleaningState): Boolean = {
    inProgress.get(topicPartition) match {
      case None => false
      case Some(state) =>
        if (state == expectedState)
          true
        else
          false
    }
  }

  /**
   *  Check if the cleaning for a partition is paused. The caller is expected to hold lock while making the call.
   */
  private def isCleaningInStatePaused(topicPartition: TopicPartition): Boolean = {
    inProgress.get(topicPartition) match {
      case None => false
      case Some(state) =>
        state match {
          case _: LogCleaningPaused =>
            true
          case _ =>
            false
        }
    }
  }

  /**
   *  Check if the cleaning for a partition is aborted. If so, throw an exception.
   */
  def checkCleaningAborted(topicPartition: TopicPartition): Unit = {
    inLock(lock) {
      if (isCleaningInState(topicPartition, LogCleaningAborted))
        throw new LogCleaningAbortedException()
    }
  }

  /**
   * Update checkpoint file, adding or removing partitions if necessary.
   *
   * @param dataDir                       The File object to be updated
   * @param partitionToUpdateOrAdd        The [TopicPartition, Long] map data to be updated. pass "none" if doing remove, not add
   * @param partitionToRemove             The TopicPartition to be removed
   */
  def updateCheckpoints(dataDir: File,
                        partitionToUpdateOrAdd: Option[(TopicPartition, Long)] = None,
                        partitionToRemove: Option[TopicPartition] = None): Unit = {
    inLock(lock) {
      val checkpoint = checkpoints(dataDir)
      if (checkpoint != null) {
        try {
          val currentCheckpoint = checkpoint.read().filter { case (tp, _) => logs.keys.contains(tp) }.toMap
          // remove the partition offset if any
          var updatedCheckpoint = partitionToRemove match {
            case Some(topicPartition) => currentCheckpoint - topicPartition
            case None => currentCheckpoint
          }
          // update or add the partition offset if any
          updatedCheckpoint = partitionToUpdateOrAdd match {
            case Some(updatedOffset) => updatedCheckpoint + updatedOffset
            case None => updatedCheckpoint
          }

          checkpoint.write(updatedCheckpoint)
        } catch {
          case e: KafkaStorageException =>
            error(s"Failed to access checkpoint file ${checkpoint.file.getName} in dir ${checkpoint.file.getParentFile.getAbsolutePath}", e)
        }
      }
    }
  }

  /**
   * alter the checkpoint directory for the topicPartition, to remove the data in sourceLogDir, and add the data in destLogDir
   */
  def alterCheckpointDir(topicPartition: TopicPartition, sourceLogDir: File, destLogDir: File): Unit = {
    inLock(lock) {
      try {
        checkpoints.get(sourceLogDir).flatMap(_.read().get(topicPartition)) match {
          case Some(offset) =>
            debug(s"Removing the partition offset data in checkpoint file for '$topicPartition' " +
              s"from ${sourceLogDir.getAbsoluteFile} directory.")
            updateCheckpoints(sourceLogDir, partitionToRemove = Option(topicPartition))

            debug(s"Adding the partition offset data in checkpoint file for '$topicPartition' " +
              s"to ${destLogDir.getAbsoluteFile} directory.")
            updateCheckpoints(destLogDir, partitionToUpdateOrAdd = Option(topicPartition, offset))
          case None =>
        }
      } catch {
        case e: KafkaStorageException =>
          error(s"Failed to access checkpoint file in dir ${sourceLogDir.getAbsolutePath}", e)
      }

      val logUncleanablePartitions = uncleanablePartitions.getOrElse(sourceLogDir.toString, mutable.Set[TopicPartition]())
      if (logUncleanablePartitions.contains(topicPartition)) {
        logUncleanablePartitions.remove(topicPartition)
        markPartitionUncleanable(destLogDir.toString, topicPartition)
      }
    }
  }

  /**
   * Stop cleaning logs in the provided directory
   *
   * @param dir     the absolute path of the log dir
   */
  def handleLogDirFailure(dir: String): Unit = {
    warn(s"Stopping cleaning logs in dir $dir")
    inLock(lock) {
      checkpoints = checkpoints.filter { case (k, _) => k.getAbsolutePath != dir }
    }
  }

  /**
   * Truncate the checkpointed offset for the given partition if its checkpointed offset is larger than the given offset
   */
  def maybeTruncateCheckpoint(dataDir: File, topicPartition: TopicPartition, offset: Long): Unit = {
    inLock(lock) {
      if (logs.get(topicPartition).config.compact) {
        val checkpoint = checkpoints(dataDir)
        if (checkpoint != null) {
          val existing = checkpoint.read()
          if (existing.getOrElse(topicPartition, 0L) > offset)
            checkpoint.write(mutable.Map() ++= existing += topicPartition -> offset)
        }
      }
    }
  }

  /**
   * Save out the endOffset and remove the given log from the in-progress set, if not aborted.
   */
  def doneCleaning(topicPartition: TopicPartition, dataDir: File, endOffset: Long): Unit = {
    inLock(lock) {
      inProgress.get(topicPartition) match {
        case Some(LogCleaningInProgress) =>
          updateCheckpoints(dataDir, partitionToUpdateOrAdd = Option(topicPartition, endOffset))
          inProgress.remove(topicPartition)
        case Some(LogCleaningAborted) =>
          inProgress.put(topicPartition, LogCleaningPaused(1))
          pausedCleaningCond.signalAll()
        case None =>
          throw new IllegalStateException(s"State for partition $topicPartition should exist.")
        case s =>
          throw new IllegalStateException(s"In-progress partition $topicPartition cannot be in $s state.")
      }
    }
  }

  def doneDeleting(topicPartitions: Iterable[TopicPartition]): Unit = {
    inLock(lock) {
      topicPartitions.foreach {
        topicPartition =>
          inProgress.get(topicPartition) match {
            case Some(LogCleaningInProgress) =>
              inProgress.remove(topicPartition)
            case Some(LogCleaningAborted) =>
              inProgress.put(topicPartition, LogCleaningPaused(1))
              pausedCleaningCond.signalAll()
            case None =>
              throw new IllegalStateException(s"State for partition $topicPartition should exist.")
            case s =>
              throw new IllegalStateException(s"In-progress partition $topicPartition cannot be in $s state.")
          }
      }
    }
  }

  /**
   * Returns an immutable set of the uncleanable partitions for a given log directory
   * Only used for testing
   */
  private[log] def uncleanablePartitions(logDir: String): Set[TopicPartition] = {
    var partitions: Set[TopicPartition] = Set()
    inLock(lock) { partitions ++= uncleanablePartitions.getOrElse(logDir, partitions) }
    partitions
  }

  def markPartitionUncleanable(logDir: String, partition: TopicPartition): Unit = {
    inLock(lock) {
      uncleanablePartitions.get(logDir) match {
        case Some(partitions) =>
          partitions.add(partition)
        case None =>
          uncleanablePartitions.put(logDir, mutable.Set(partition))
      }
    }
  }

  private def isUncleanablePartition(log: UnifiedLog, topicPartition: TopicPartition): Boolean = {
    inLock(lock) {
      uncleanablePartitions.get(log.parentDir).exists(partitions => partitions.contains(topicPartition))
    }
  }

  def maintainUncleanablePartitions(): Unit = {
    // Remove deleted partitions from uncleanablePartitions
    inLock(lock) {
      // Note: we don't use retain or filterInPlace method in this function because retain is deprecated in
      // scala 2.13 while filterInPlace is not available in scala 2.12.

      // Remove deleted partitions
      uncleanablePartitions.values.foreach {
        partitions =>
          val partitionsToRemove = partitions.filterNot(logs.contains).toList
          partitionsToRemove.foreach { partitions.remove }
      }

      // Remove entries with empty partition set.
      val logDirsToRemove = uncleanablePartitions.filter {
        case (_, partitions) => partitions.isEmpty
      }.keys.toList
      logDirsToRemove.foreach { uncleanablePartitions.remove }
    }
  }

  def removeMetrics(): Unit = {
    GaugeMetricNameNoTag.foreach(metricsGroup.removeMetric)
    gaugeMetricNameWithTag.asScala.foreach { metricNameAndTags =>
      metricNameAndTags._2.asScala.foreach { tag =>
        metricsGroup.removeMetric(metricNameAndTags._1, tag)
      }
    }
    gaugeMetricNameWithTag.clear()
  }
}

/**
 * Helper class for the range of cleanable dirty offsets of a log and whether to update the checkpoint associated with
 * the log
 *
 * @param firstDirtyOffset the lower (inclusive) offset to begin cleaning from
 * @param firstUncleanableDirtyOffset the upper(exclusive) offset to clean to
 * @param forceUpdateCheckpoint whether to update the checkpoint associated with this log. if true, checkpoint should be
 *                             reset to firstDirtyOffset
 */
private case class OffsetsToClean(firstDirtyOffset: Long,
                                  firstUncleanableDirtyOffset: Long,
                                  forceUpdateCheckpoint: Boolean = false) {
}

private[log] object LogCleanerManager extends Logging {
  private val UncleanablePartitionsCountMetricName = "uncleanable-partitions-count"
  private val UncleanableBytesMetricName = "uncleanable-bytes"
  private val MaxDirtyPercentMetricName = "max-dirty-percent"
  private val TimeSinceLastRunMsMetricName = "time-since-last-run-ms"

  // Visible for testing
  private[log] val GaugeMetricNameNoTag = Set(
    MaxDirtyPercentMetricName,
    TimeSinceLastRunMsMetricName
  )

  private def isCompactAndDelete(log: UnifiedLog): Boolean = {
    log.config.compact && log.config.delete
  }

  /**
    * get max delay between the time when log is required to be compacted as determined
    * by maxCompactionLagMs and the current time.
    */
  private def maxCompactionDelay(log: UnifiedLog, firstDirtyOffset: Long, now: Long) : Long = {
    val dirtyNonActiveSegments = log.nonActiveLogSegmentsFrom(firstDirtyOffset)
    val firstBatchTimestamps = log.getFirstBatchTimestampForSegments(dirtyNonActiveSegments).stream.filter(_ > 0)

    val earliestDirtySegmentTimestamp = firstBatchTimestamps.min(Comparator.naturalOrder()).orElse(Long.MaxValue)

    val maxCompactionLagMs = math.max(log.config.maxCompactionLagMs, 0L)
    val cleanUntilTime = now - maxCompactionLagMs

    if (earliestDirtySegmentTimestamp < cleanUntilTime)
      cleanUntilTime - earliestDirtySegmentTimestamp
    else
      0L
  }

  /**
    * Returns the range of dirty offsets that can be cleaned.
    *
    * @param log the log
    * @param lastCleanOffset the last checkpointed offset
    * @param now the current time in milliseconds of the cleaning operation
    * @return OffsetsToClean containing offsets for cleanable portion of log and whether the log checkpoint needs updating
    */
  def cleanableOffsets(log: UnifiedLog, lastCleanOffset: Option[Long], now: Long): OffsetsToClean = {
    // If the log segments are abnormally truncated and hence the checkpointed offset is no longer valid;
    // reset to the log starting offset and log the error
    val (firstDirtyOffset, forceUpdateCheckpoint) = {
      val logStartOffset = log.logStartOffset
      val checkpointDirtyOffset = lastCleanOffset.getOrElse(logStartOffset)

      if (checkpointDirtyOffset < logStartOffset) {
        // Don't bother with the warning if compact and delete are enabled.
        if (!isCompactAndDelete(log))
          warn(s"Resetting first dirty offset of ${log.name} to log start offset $logStartOffset " +
            s"since the checkpointed offset $checkpointDirtyOffset is invalid.")
        (logStartOffset, true)
      } else if (checkpointDirtyOffset > log.logEndOffset) {
        // The dirty offset has gotten ahead of the log end offset. This could happen if there was data
        // corruption at the end of the log. We conservatively assume that the full log needs cleaning.
        warn(s"The last checkpoint dirty offset for partition ${log.name} is $checkpointDirtyOffset, " +
          s"which is larger than the log end offset ${log.logEndOffset}. Resetting to the log start offset $logStartOffset.")
        (logStartOffset, true)
      } else {
        (checkpointDirtyOffset, false)
      }
    }

    val minCompactionLagMs = math.max(log.config.compactionLagMs, 0L)

    // Find the first segment that cannot be cleaned. We cannot clean past:
    // 1. The active segment
    // 2. The last stable offset (including the high watermark)
    // 3. Any segments closer to the head of the log than the minimum compaction lag time
    val firstUncleanableDirtyOffset: Long = Seq(

      // we do not clean beyond the last stable offset
      Some(log.lastStableOffset),

      // the active segment is always uncleanable
      Option(log.activeSegment.baseOffset),

      // the first segment whose largest message timestamp is within a minimum time lag from now
      if (minCompactionLagMs > 0) {
        // dirty log segments
        val dirtyNonActiveSegments = log.nonActiveLogSegmentsFrom(firstDirtyOffset)
        dirtyNonActiveSegments.asScala.find { s =>
          val isUncleanable = s.largestTimestamp > now - minCompactionLagMs
          debug(s"Checking if log segment may be cleaned: log='${log.name}' segment.baseOffset=${s.baseOffset} " +
            s"segment.largestTimestamp=${s.largestTimestamp}; now - compactionLag=${now - minCompactionLagMs}; " +
            s"is uncleanable=$isUncleanable")
          isUncleanable
        }.map(_.baseOffset)
      } else None
    ).flatten.min

    debug(s"Finding range of cleanable offsets for log=${log.name}. Last clean offset=$lastCleanOffset " +
      s"now=$now => firstDirtyOffset=$firstDirtyOffset firstUncleanableOffset=$firstUncleanableDirtyOffset " +
      s"activeSegment.baseOffset=${log.activeSegment.baseOffset}")

    OffsetsToClean(firstDirtyOffset, math.max(firstDirtyOffset, firstUncleanableDirtyOffset), forceUpdateCheckpoint)
  }

  /**
   * Given the first dirty offset and an uncleanable offset, calculates the total cleanable bytes for this log
   * @return the biggest uncleanable offset and the total amount of cleanable bytes
   */
  def calculateCleanableBytes(log: UnifiedLog, firstDirtyOffset: Long, uncleanableOffset: Long): (Long, Long) = {
    val firstUncleanableSegment = log.nonActiveLogSegmentsFrom(uncleanableOffset).asScala.headOption.getOrElse(log.activeSegment)
    val firstUncleanableOffset = firstUncleanableSegment.baseOffset
    val cleanableBytes = log.logSegments(math.min(firstDirtyOffset, firstUncleanableOffset), firstUncleanableOffset).map(_.size.toLong).sum

    (firstUncleanableOffset, cleanableBytes)
  }

}
