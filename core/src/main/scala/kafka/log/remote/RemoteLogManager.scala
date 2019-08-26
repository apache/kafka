/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log.remote

import java.io.Closeable
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.{BiConsumer, Consumer, Function}

import kafka.log.{Log, LogSegment}
import kafka.server.{Defaults, FetchDataInfo, KafkaConfig, LogOffsetMetadata}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.{KafkaThread, Time, Utils}

import scala.collection.Set

class RLMScheduledThreadPool(poolSize: Int) extends Logging {

  private val scheduledThreadPool: ScheduledThreadPoolExecutor = {
    val threadPool = new ScheduledThreadPoolExecutor(poolSize)
    threadPool.setRemoveOnCancelPolicy(true)
    threadPool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
    threadPool.setThreadFactory(new ThreadFactory {
      private val sequence = new AtomicInteger()

      override def newThread(r: Runnable): Thread = {
        KafkaThread.daemon("kafka-rlm-thread-pool-" + sequence.incrementAndGet(), r)
      }
    })

    threadPool
  }

  def resizePool(size: Int): Unit = {
    scheduledThreadPool.setCorePoolSize(size)
  }

  def scheduleWithFixedDelay(runnable: CancellableRunnable, initialDelay: Long, delay: Long,
                             timeUnit: TimeUnit): ScheduledFuture[_] = {
    scheduledThreadPool.scheduleWithFixedDelay(runnable, initialDelay, delay, timeUnit)
  }

  def shutdown(): Unit = {
    scheduledThreadPool.shutdownNow()
    //waits for 2 mins to terminate the current tasks
    scheduledThreadPool.awaitTermination(2, TimeUnit.MINUTES)
  }
}

trait CancellableRunnable extends Runnable {
  @volatile private var cancelled = false;

  def cancel(): Unit = {
    cancelled = true
  }

  def isCancelled(): Boolean = {
    cancelled
  }
}

class RemoteLogManager(logFetcher: TopicPartition => Option[Log],
                       segmentCleaner: (TopicPartition, LogSegment) => Unit,
                       rlmConfig: RemoteLogManagerConfig,
                       time: Time = Time.SYSTEM) extends Logging with Closeable {

  private val leaderOrFollowerTasks: ConcurrentMap[TopicPartition, RLMTaskWithFuture] =
    new ConcurrentHashMap[TopicPartition, RLMTaskWithFuture]()

  private def createRemoteStorageManager(): RemoteStorageManager = {
    val rsm = Class.forName(rlmConfig.remoteLogStorageManagerClass)
      .getDeclaredConstructor().newInstance().asInstanceOf[RemoteStorageManager]

    val rsmProps = new util.HashMap[String, Any]()
    rlmConfig.remoteStorageConfig.foreach { case (k, v) => rsmProps.put(k, v) }
    rsmProps.put(KafkaConfig.RemoteLogRetentionMillisProp, rlmConfig.remoteLogRetentionMillis)
    rsmProps.put(KafkaConfig.RemoteLogRetentionBytesProp, rlmConfig.remoteLogRetentionBytes)
    rsm.configure(rsmProps)
    rsm
  }

  private val remoteStorageManager: RemoteStorageManager = createRemoteStorageManager()
  private val rlmIndexer: RLMIndexer = new RLMIndexer(remoteStorageManager, logFetcher)

  // add a config to take no of threads
  private val delayInMs = 30 * 1000L
  private val poolSize = 10
  private val rlmScheduledThreadPool = new RLMScheduledThreadPool(poolSize)

  /**
   * Ask RLM to monitor the given TopicPartitions, and copy inactive Log
   * Segments to remote storage. RLM updates local index files once a
   * Log Segment in a TopicPartition is copied to Remote Storage
   */
  def handleLeaderPartitions(topicPartitions: Set[TopicPartition]): Unit = {
    doHandleLeaderOrFollowerPartitions(topicPartitions, task => task.convertToLeader())
  }

  /**
   * Stops copy of LogSegment if TopicPartition ownership is moved from a broker.
   */
  def handleFollowerPartitions(topicPartitions: Set[TopicPartition]): Unit = {
    doHandleLeaderOrFollowerPartitions(topicPartitions, task => task.convertToFollower())
  }

  private def doHandleLeaderOrFollowerPartitions(topicPartitions: Set[TopicPartition],
                                                 convertToLeaderOrFollower: RLMTask => Unit) = {
    topicPartitions.foreach { topicPartition =>
      val rlmTaskWithFuture = leaderOrFollowerTasks.computeIfAbsent(topicPartition,
        new Function[TopicPartition, RLMTaskWithFuture] {
          override def apply(tp: TopicPartition): RLMTaskWithFuture = {
            val task = new RLMTask(tp)
            val future = rlmScheduledThreadPool.scheduleWithFixedDelay(task, 0, delayInMs, TimeUnit.MILLISECONDS)
            RLMTaskWithFuture(task, future)
          }
        })
      convertToLeaderOrFollower(rlmTaskWithFuture.rlmTask)
    }
  }

  /**
   * Stops partitions for copying segments, building indexes and deletes the partition in remote storage if delete flag
   * is set as true.
   *
   * @param topicPartitions Set of topic partitions with a flag to be deleted or not.
   * @return
   */
  def handleStopPartition(topicPartitions: Set[(TopicPartition, Boolean)]): Unit = {
    topicPartitions.foreach { case (tp, delete) =>
      // unassign topic partitions from RLM leader/follower
      val rlmTaskWithFuture = leaderOrFollowerTasks.remove(tp)
      if (rlmTaskWithFuture != null) {
        rlmTaskWithFuture.cancel()
      }
      // schedule delete task if necessary, this should be asynchronous
      if (delete) remoteStorageManager.deleteTopicPartition(tp)
    }
  }

  class RLMTask(tp: TopicPartition) extends CancellableRunnable with Logging {
    this.logIdent = s"[RLMTask partition:$tp] "
    @volatile var isLeader: Boolean = false

    private var readOffset: Long = rlmIndexer.getOrLoadIndexOffset(tp).getOrElse(0L)

    def convertToLeader(): Unit = {
      isLeader = true
    }

    def convertToFollower(): Unit = {
      isLeader = false
    }

    def copyLogSegmentsToRemote(): Unit = {
      logFetcher(tp).foreach { log => {
        if (isCancelled()) {
          info(s"Skipping copying log segments as the current task is cancelled")
          return
        }

        val baseOffset = log.activeSegment.baseOffset
        if (readOffset >= baseOffset) {
          info(s"Skipping building indexes, current read offset:$readOffset is more than >= active segment " +
            s"base offset:$baseOffset ")
          return
        }

        val segments = log.logSegments(readOffset + 1, baseOffset)
        segments.foreach { segment =>
          if (isCancelled() || !isLeader) {
            info(
              s"Skipping copying log segments as the current task state is changed, cancelled:$isCancelled() leader:$isLeader")
            return
          }
          val entries = remoteStorageManager.copyLogSegment(tp, segment)
          val file = segment.log.file()
          val fileName = file.getName
          val baseOffsetStr = fileName.substring(0, fileName.indexOf("."))
          rlmIndexer.maybeBuildIndexes(tp, entries, file.getParentFile, baseOffsetStr)
          readOffset = entries.get(entries.size() - 1).lastOffset
        }
      }
      }
    }

    def updateRemoteLogIndexes(): Unit = {
      remoteStorageManager.listRemoteSegments(tp, readOffset).forEach {
        rlsInfo =>
          val entries = remoteStorageManager.getRemoteLogIndexEntries(rlsInfo)
          if (!entries.isEmpty) {
            logFetcher(tp).foreach { log: Log =>
              if (isCancelled()) {
                info(s"Skipping building indexes as the current task is cancelled")
                return
              }
              val baseOffset = Log.filenamePrefixFromOffset(entries.get(0).firstOffset)
              val logDir = log.activeSegment.log.file().getParentFile
              rlmIndexer.maybeBuildIndexes(tp, entries, logDir, baseOffset)
              readOffset = entries.get(entries.size() - 1).lastOffset
            }
          }
      }
    }

    def deleteExpiredRemoteLogSegments(): Unit = {
      remoteStorageManager.cleanupLogUntil(tp, time.milliseconds() - rlmConfig.remoteLogRetentionMillis)
    }

    override def run(): Unit = {
      try {
        if (!isCancelled()) {
          //a. copy log segments to remote store
          if (isLeader) copyLogSegmentsToRemote()

          //b. fetch missing remote index files
          updateRemoteLogIndexes()

          //c. delete expired remote segments
          if (isLeader) deleteExpiredRemoteLogSegments()
        }
      } catch {
        case ex: InterruptedException =>
          warn(s"Current thread for topic-partition $tp is interrupted, this should not be rescheduled ", ex)
        case ex: Exception =>
          warn(
            s"Current task for topic-partition $tp received error but it will be scheduled for next iteration: ", ex)
      }
    }
  }

  def lookupLastOffset(tp: TopicPartition): Option[Long] = {
    rlmIndexer.lookupLastOffset(tp)
  }

  /**
   * Read topic partition data from remote
   *
   * @param fetchMaxByes
   * @param minOneMessage
   * @param tp
   * @param fetchInfo
   * @return
   */
  def read(fetchMaxByes: Int, minOneMessage: Boolean, tp: TopicPartition, fetchInfo: PartitionData): FetchDataInfo = {
    val offset = fetchInfo.fetchOffset
    val entry = rlmIndexer.lookupEntryForOffset(tp, offset)
      .getOrElse(throw new OffsetOutOfRangeException(
        s"Received request for offset $offset for partition $tp, which does not exist in remote tier"))

    val records = remoteStorageManager.read(entry, fetchInfo.maxBytes, offset, minOneMessage)

    FetchDataInfo(LogOffsetMetadata(offset), records)
  }

  /**
   * Stops all the threads and releases all the resources.
   */
  def close(): Unit = {
    Utils.closeQuietly(remoteStorageManager, "RemoteLogStorageManager")
    leaderOrFollowerTasks.values().forEach(new Consumer[RLMTaskWithFuture] {
      override def accept(taskWithFuture: RLMTaskWithFuture): Unit = taskWithFuture.cancel()
    })
    rlmScheduledThreadPool.shutdown()
  }

  case class RLMTaskWithFuture(rlmTask: RLMTask, future: Future[_]) {
    def cancel(): Unit = {
      rlmTask.cancel()
      future.cancel(true)
    }
  }

}

case class RemoteLogManagerConfig(remoteLogStorageEnable: Boolean, remoteLogStorageManagerClass: String,
                                  remoteLogRetentionBytes: Long, remoteLogRetentionMillis: Long,
                                  remoteStorageConfig: Map[String, Any], remoteLogManagerThreadPoolSize: Int,
                                  remoteLogManagerTaskIntervalMs: Long)


object RemoteLogManager {
  def REMOTE_STORAGE_MANAGER_CONFIG_PREFIX = "remote.log.storage."

  def DefaultConfig = RemoteLogManagerConfig(remoteLogStorageEnable = Defaults.RemoteLogStorageEnable, null,
    Defaults.RemoteLogRetentionBytes, TimeUnit.MINUTES.toMillis(Defaults.RemoteLogRetentionMinutes), Map.empty,
    Defaults.RemoteLogManagerThreadPoolSize, Defaults.RemoteLogManagerTaskIntervalMs)

  def createRemoteLogManagerConfig(config: KafkaConfig): RemoteLogManagerConfig = {
    var rsmProps = collection.mutable.Map[String, Any]()
    config.props.forEach(new BiConsumer[Any, Any] {
      override def accept(key: Any, value: Any): Unit = {
        key match {
          case key: String if key.startsWith(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX) =>
            rsmProps += (key.toString -> value)
          case _ =>
        }
      }
    })
    RemoteLogManagerConfig(config.remoteLogStorageEnable, config.remoteLogStorageManager,
      config.remoteLogRetentionBytes, config.remoteLogRetentionMillis, rsmProps.toMap,
      config.remoteLogManagerThreadPoolSize, config.remoteLogManagerTaskIntervalMs)
  }
}