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
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import kafka.utils.{Logging, Pool}
import kafka.server.OffsetCheckpoint
import collection.mutable
import java.util.concurrent.locks.ReentrantLock
import kafka.utils.Utils._
import java.util.concurrent.TimeUnit
import kafka.common.{LogCleaningAbortedException, TopicAndPartition}

private[log] sealed trait LogCleaningState
private[log] case object LogCleaningInProgress extends LogCleaningState
private[log] case object LogCleaningAborted extends LogCleaningState
private[log] case object LogCleaningPaused extends LogCleaningState

/**
 *  Manage the state of each partition being cleaned.
 *  If a partition is to be cleaned, it enters the LogCleaningInProgress state.
 *  While a partition is being cleaned, it can be requested to be aborted and paused. Then the partition first enters
 *  the LogCleaningAborted state. Once the cleaning task is aborted, the partition enters the LogCleaningPaused state.
 *  While a partition is in the LogCleaningPaused state, it won't be scheduled for cleaning again, until cleaning is
 *  requested to be resumed.
 */
private[log] class LogCleanerManager(val logDirs: Array[File], val logs: Pool[TopicAndPartition, Log]) extends Logging with KafkaMetricsGroup {
  
  override val loggerName = classOf[LogCleaner].getName

  // package-private for testing
  private[log] val offsetCheckpointFile = "cleaner-offset-checkpoint"
  
  /* the offset checkpoints holding the last cleaned point for each log */
  private val checkpoints = logDirs.map(dir => (dir, new OffsetCheckpoint(new File(dir, offsetCheckpointFile)))).toMap

  /* the set of logs currently being cleaned */
  private val inProgress = mutable.HashMap[TopicAndPartition, LogCleaningState]()

  /* a global lock used to control all access to the in-progress set and the offset checkpoints */
  private val lock = new ReentrantLock
  
  /* for coordinating the pausing and the cleaning of a partition */
  private val pausedCleaningCond = lock.newCondition()
  
  /* a gauge for tracking the cleanable ratio of the dirtiest log */
  @volatile private var dirtiestLogCleanableRatio = 0.0
  newGauge("max-dirty-percent", new Gauge[Int] { def value = (100 * dirtiestLogCleanableRatio).toInt })

  /**
   * @return the position processed for all logs.
   */
  def allCleanerCheckpoints(): Map[TopicAndPartition, Long] =
    checkpoints.values.flatMap(_.read()).toMap

   /**
    * Choose the log to clean next and add it to the in-progress set. We recompute this
    * every time off the full set of logs to allow logs to be dynamically added to the pool of logs
    * the log manager maintains.
    */
  def grabFilthiestLog(): Option[LogToClean] = {
    inLock(lock) {
      val lastClean = allCleanerCheckpoints()
      val dirtyLogs = logs.filter(l => l._2.config.compact)          // skip any logs marked for delete rather than dedupe
                          .filterNot(l => inProgress.contains(l._1)) // skip any logs already in-progress
                          .map(l => LogToClean(l._1, l._2,           // create a LogToClean instance for each
                                               lastClean.getOrElse(l._1, l._2.logSegments.head.baseOffset)))
                          .filter(l => l.totalBytes > 0)             // skip any empty logs
      this.dirtiestLogCleanableRatio = if (!dirtyLogs.isEmpty) dirtyLogs.max.cleanableRatio else 0
      val cleanableLogs = dirtyLogs.filter(l => l.cleanableRatio > l.log.config.minCleanableRatio) // and must meet the minimum threshold for dirty byte ratio
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
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   *  This is implemented by first abortAndPausing and then resuming the cleaning of the partition.
   */
  def abortCleaning(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      abortAndPauseCleaning(topicAndPartition)
      resumeCleaning(topicAndPartition)
      info("The cleaning for partition %s is aborted".format(topicAndPartition))
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
   */
  def abortAndPauseCleaning(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      inProgress.get(topicAndPartition) match {
        case None =>
          inProgress.put(topicAndPartition, LogCleaningPaused)
        case Some(state) =>
          state match {
            case LogCleaningInProgress =>
              inProgress.put(topicAndPartition, LogCleaningAborted)
            case s =>
              throw new IllegalStateException("Compaction for partition %s cannot be aborted and paused since it is in %s state."
                                              .format(topicAndPartition, s))
          }
      }
      while (!isCleaningInState(topicAndPartition, LogCleaningPaused))
        pausedCleaningCond.await(100, TimeUnit.MILLISECONDS)
      info("The cleaning for partition %s is aborted and paused".format(topicAndPartition))
    }
  }

  /**
   *  Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
   */
  def resumeCleaning(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      inProgress.get(topicAndPartition) match {
        case None =>
          throw new IllegalStateException("Compaction for partition %s cannot be resumed since it is not paused."
                                          .format(topicAndPartition))
        case Some(state) =>
          state match {
            case LogCleaningPaused =>
              inProgress.remove(topicAndPartition)
            case s =>
              throw new IllegalStateException("Compaction for partition %s cannot be resumed since it is in %s state."
                                              .format(topicAndPartition, s))
          }
      }
    }
    info("Compaction for partition %s is resumed".format(topicAndPartition))
  }

  /**
   *  Check if the cleaning for a partition is in a particular state. The caller is expected to hold lock while making the call.
   */
  def isCleaningInState(topicAndPartition: TopicAndPartition, expectedState: LogCleaningState): Boolean = {
    inProgress.get(topicAndPartition) match {
      case None => return false
      case Some(state) =>
        if (state == expectedState)
          return true
        else
          return false
    }
  }

  /**
   *  Check if the cleaning for a partition is aborted. If so, throw an exception.
   */
  def checkCleaningAborted(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      if (isCleaningInState(topicAndPartition, LogCleaningAborted))
        throw new LogCleaningAbortedException()
    }
  }

  def updateCheckpoints(dataDir: File, update: Option[(TopicAndPartition,Long)]) {
    inLock(lock) {
      val checkpoint = checkpoints(dataDir)
      val existing = checkpoint.read().filterKeys(logs.keys) ++ update
      checkpoint.write(existing)
    }
  }

  /**
   * Save out the endOffset and remove the given log from the in-progress set, if not aborted.
   */
  def doneCleaning(topicAndPartition: TopicAndPartition, dataDir: File, endOffset: Long) {
    inLock(lock) {
      inProgress(topicAndPartition) match {
        case LogCleaningInProgress =>
          updateCheckpoints(dataDir,Option(topicAndPartition, endOffset))
          inProgress.remove(topicAndPartition)
        case LogCleaningAborted =>
          inProgress.put(topicAndPartition, LogCleaningPaused)
          pausedCleaningCond.signalAll()
        case s =>
          throw new IllegalStateException("In-progress partition %s cannot be in %s state.".format(topicAndPartition, s))
      }
    }
  }
}
