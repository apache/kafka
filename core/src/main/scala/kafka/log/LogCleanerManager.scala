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

import com.yammer.metrics.core.Gauge
import kafka.common.LogCleaningAbortedException
import kafka.metrics.KafkaMetricsGroup
import kafka.server.LogDirFailureChannel
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.utils.CoreUtils._
import kafka.utils.{Logging, Pool}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.errors.KafkaStorageException

import scala.collection.{Iterable, immutable, mutable}

private[log] sealed trait LogCleaningState
private[log] case object LogCleaningInProgress extends LogCleaningState
private[log] case object LogCleaningAborted extends LogCleaningState
private[log] case class LogCleaningPaused(pausedCount: Int) extends LogCleaningState

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
                                     val logs: Pool[TopicPartition, Log],
                                     val logDirFailureChannel: LogDirFailureChannel) extends Logging with KafkaMetricsGroup {

  protected override def loggerName = classOf[LogCleaner].getName

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

  /* gauges for tracking the number of partitions marked as uncleanable for each log directory */
  for (dir <- logDirs) {
    newGauge(
      "uncleanable-partitions-count",
      new Gauge[Int] { def value = inLock(lock) { uncleanablePartitions.get(dir.getAbsolutePath).map(_.size).getOrElse(0) } },
      Map("logDirectory" -> dir.getAbsolutePath)
    )
  }

  /* gauges for tracking the number of uncleanable bytes from uncleanable partitions for each log directory */
    for (dir <- logDirs) {
      newGauge(
        "uncleanable-bytes",
        new Gauge[Long] {
          def value = {
            inLock(lock) {
              uncleanablePartitions.get(dir.getAbsolutePath) match {
                case Some(partitions) => {
                  val lastClean = allCleanerCheckpoints
                  val now = Time.SYSTEM.milliseconds
                  partitions.map { tp =>
                    val log = logs.get(tp)
                    val (firstDirtyOffset, firstUncleanableDirtyOffset) = LogCleanerManager.cleanableOffsets(log, tp, lastClean, now)
                    val (_, uncleanableBytes) = LogCleaner.calculateCleanableBytes(log, firstDirtyOffset, firstUncleanableDirtyOffset)
                    uncleanableBytes
                  }.sum
                }
                case _ => 0
              }
            }
          }
        },
        Map("logDirectory" -> dir.getAbsolutePath)
      )
    }

  /* a gauge for tracking the cleanable ratio of the dirtiest log */
  @volatile private var dirtiestLogCleanableRatio = 0.0
  newGauge("max-dirty-percent", new Gauge[Int] { def value = (100 * dirtiestLogCleanableRatio).toInt })

  /* a gauge for tracking the time since the last log cleaner run, in milli seconds */
  @volatile private var timeOfLastRun : Long = Time.SYSTEM.milliseconds
  newGauge("time-since-last-run-ms", new Gauge[Long] { def value = Time.SYSTEM.milliseconds - timeOfLastRun })

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
  def grabFilthiestCompactedLog(time: Time): Option[LogToClean] = {
    inLock(lock) {
      val now = time.milliseconds
      this.timeOfLastRun = now
      val lastClean = allCleanerCheckpoints
      val dirtyLogs = logs.filter {
        case (_, log) => log.config.compact  // match logs that are marked as compacted
      }.filterNot {
        case (topicPartition, log) =>
          // skip any logs already in-progress and uncleanable partitions
          inProgress.contains(topicPartition) || isUncleanablePartition(log, topicPartition)
      }.map {
        case (topicPartition, log) => // create a LogToClean instance for each
          val (firstDirtyOffset, firstUncleanableDirtyOffset) = LogCleanerManager.cleanableOffsets(log, topicPartition,
            lastClean, now)
          LogToClean(topicPartition, log, firstDirtyOffset, firstUncleanableDirtyOffset)
      }.filter(ltc => ltc.totalBytes > 0) // skip any empty logs

      this.dirtiestLogCleanableRatio = if (dirtyLogs.nonEmpty) dirtyLogs.max.cleanableRatio else 0
      // and must meet the minimum threshold for dirty byte ratio
      val cleanableLogs = dirtyLogs.filter(ltc => ltc.cleanableRatio > ltc.log.config.minCleanableRatio)
      if(cleanableLogs.isEmpty) {
        None
      } else {
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
  def pauseCleaningForNonCompactedPartitions(): Iterable[(TopicPartition, Log)] = {
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
  def deletableLogs(): Iterable[(TopicPartition, Log)] = {
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
  def abortCleaning(topicPartition: TopicPartition) {
    inLock(lock) {
      abortAndPauseCleaning(topicPartition)
      resumeCleaning(Seq(topicPartition))
    }
    info(s"The cleaning for partition $topicPartition is aborted")
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
  def abortAndPauseCleaning(topicPartition: TopicPartition) {
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

      while(!isCleaningInStatePaused(topicPartition))
        pausedCleaningCond.await(100, TimeUnit.MILLISECONDS)
    }
    info(s"The cleaning for partition $topicPartition is aborted and paused")
  }

  /**
    *  Resume the cleaning of paused partitions.
    *  Each call of this function will undo one pause.
    */
  def resumeCleaning(topicPartitions: Iterable[TopicPartition]){
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
          case LogCleaningPaused(s) =>
            true
          case _ =>
            false
        }
    }
  }

  /**
   *  Check if the cleaning for a partition is aborted. If so, throw an exception.
   */
  def checkCleaningAborted(topicPartition: TopicPartition) {
    inLock(lock) {
      if (isCleaningInState(topicPartition, LogCleaningAborted))
        throw new LogCleaningAbortedException()
    }
  }

  def updateCheckpoints(dataDir: File, update: Option[(TopicPartition,Long)]) {
    inLock(lock) {
      val checkpoint = checkpoints(dataDir)
      if (checkpoint != null) {
        try {
          val existing = checkpoint.read().filterKeys(logs.keys) ++ update
          checkpoint.write(existing)
        } catch {
          case e: KafkaStorageException =>
            error(s"Failed to access checkpoint file ${checkpoint.file.getName} in dir ${checkpoint.file.getParentFile.getAbsolutePath}", e)
        }
      }
    }
  }

  def alterCheckpointDir(topicPartition: TopicPartition, sourceLogDir: File, destLogDir: File): Unit = {
    inLock(lock) {
      try {
        checkpoints.get(sourceLogDir).flatMap(_.read().get(topicPartition)) match {
          case Some(offset) =>
            // Remove this partition from the checkpoint file in the source log directory
            updateCheckpoints(sourceLogDir, None)
            // Add offset for this partition to the checkpoint file in the source log directory
            updateCheckpoints(destLogDir, Option(topicPartition, offset))
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

  def handleLogDirFailure(dir: String) {
    info(s"Stopping cleaning logs in dir $dir")
    inLock(lock) {
      checkpoints = checkpoints.filterKeys(_.getAbsolutePath != dir)
    }
  }

  def maybeTruncateCheckpoint(dataDir: File, topicPartition: TopicPartition, offset: Long) {
    inLock(lock) {
      if (logs.get(topicPartition).config.compact) {
        val checkpoint = checkpoints(dataDir)
        if (checkpoint != null) {
          val existing = checkpoint.read()
          if (existing.getOrElse(topicPartition, 0L) > offset)
            checkpoint.write(existing + (topicPartition -> offset))
        }
      }
    }
  }

  /**
   * Save out the endOffset and remove the given log from the in-progress set, if not aborted.
   */
  def doneCleaning(topicPartition: TopicPartition, dataDir: File, endOffset: Long) {
    inLock(lock) {
      inProgress.get(topicPartition) match {
        case Some(LogCleaningInProgress) =>
          updateCheckpoints(dataDir, Option(topicPartition, endOffset))
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

  private def isUncleanablePartition(log: Log, topicPartition: TopicPartition): Boolean = {
    inLock(lock) {
      uncleanablePartitions.get(log.dir.getParent).exists(partitions => partitions.contains(topicPartition))
    }
  }
}

private[log] object LogCleanerManager extends Logging {

  def isCompactAndDelete(log: Log): Boolean = {
    log.config.compact && log.config.delete
  }


  /**
    * Returns the range of dirty offsets that can be cleaned.
    *
    * @param log the log
    * @param lastClean the map of checkpointed offsets
    * @param now the current time in milliseconds of the cleaning operation
    * @return the lower (inclusive) and upper (exclusive) offsets
    */
  def cleanableOffsets(log: Log, topicPartition: TopicPartition, lastClean: immutable.Map[TopicPartition, Long], now: Long): (Long, Long) = {

    // the checkpointed offset, ie., the first offset of the next dirty segment
    val lastCleanOffset: Option[Long] = lastClean.get(topicPartition)

    // If the log segments are abnormally truncated and hence the checkpointed offset is no longer valid;
    // reset to the log starting offset and log the error
    val logStartOffset = log.logSegments.head.baseOffset
    val firstDirtyOffset = {
      val offset = lastCleanOffset.getOrElse(logStartOffset)
      if (offset < logStartOffset) {
        // don't bother with the warning if compact and delete are enabled.
        if (!isCompactAndDelete(log))
          warn(s"Resetting first dirty offset of ${log.name} to log start offset $logStartOffset since the checkpointed offset $offset is invalid.")
        logStartOffset
      } else {
        offset
      }
    }

    val compactionLagMs = math.max(log.config.compactionLagMs, 0L)

    // find first segment that cannot be cleaned
    // neither the active segment, nor segments with any messages closer to the head of the log than the minimum compaction lag time
    // may be cleaned
    val firstUncleanableDirtyOffset: Long = Seq(

      // we do not clean beyond the first unstable offset
      log.firstUnstableOffset.map(_.messageOffset),

      // the active segment is always uncleanable
      Option(log.activeSegment.baseOffset),

      // the first segment whose largest message timestamp is within a minimum time lag from now
      if (compactionLagMs > 0) {
        // dirty log segments
        val dirtyNonActiveSegments = log.logSegments(firstDirtyOffset, log.activeSegment.baseOffset)
        dirtyNonActiveSegments.find { s =>
          val isUncleanable = s.largestTimestamp > now - compactionLagMs
          debug(s"Checking if log segment may be cleaned: log='${log.name}' segment.baseOffset=${s.baseOffset} segment.largestTimestamp=${s.largestTimestamp}; now - compactionLag=${now - compactionLagMs}; is uncleanable=$isUncleanable")
          isUncleanable
        }.map(_.baseOffset)
      } else None
    ).flatten.min

    debug(s"Finding range of cleanable offsets for log=${log.name} topicPartition=$topicPartition. Last clean offset=$lastCleanOffset now=$now => firstDirtyOffset=$firstDirtyOffset firstUncleanableOffset=$firstUncleanableDirtyOffset activeSegment.baseOffset=${log.activeSegment.baseOffset}")

    (firstDirtyOffset, firstUncleanableDirtyOffset)
  }
}
