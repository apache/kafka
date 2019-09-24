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

import kafka.cluster.Partition
import kafka.common.KafkaException
import kafka.log.Log
import kafka.server.{Defaults, FetchDataInfo, KafkaConfig, LogOffsetMetadata, RemoteStorageFetchInfo}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.{KafkaThread, Time, Utils}

import scala.collection.JavaConverters._
import scala.collection.Searching._
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

  def poolSize(): Int = scheduledThreadPool.getMaximumPoolSize

  def scheduleWithFixedDelay(runnable: Runnable, initialDelay: Long, delay: Long,
                             timeUnit: TimeUnit): ScheduledFuture[_] = {
    scheduledThreadPool.scheduleWithFixedDelay(runnable, initialDelay, delay, timeUnit)
  }

  def scheduleOnceWithDelay(runnable: Runnable, delay: Long,
                            timeUnit: TimeUnit): ScheduledFuture[_] = {
    scheduledThreadPool.schedule(runnable, delay, timeUnit)
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

class RemoteLogManager(fetchLog: TopicPartition => Option[Log],
                       updateRemoteLogStartOffset: (TopicPartition, Long) => Unit,
                       rlmConfig: RemoteLogManagerConfig,
                       time: Time = Time.SYSTEM) extends Logging with Closeable {
  private val leaderOrFollowerTasks: ConcurrentMap[TopicPartition, RLMTaskWithFuture] =
    new ConcurrentHashMap[TopicPartition, RLMTaskWithFuture]()
  private val remoteStorageFetcherThreadPool = new RemoteStorageReaderThreadPool(rlmConfig.remoteLogReaderThreads,
    rlmConfig.remoteLogReaderMaxPendingTasks, time)

  private val delayInMs = rlmConfig.remoteLogManagerTaskIntervalMs
  private val poolSize = rlmConfig.remoteLogManagerThreadPoolSize
  private val rlmScheduledThreadPool = new RLMScheduledThreadPool(poolSize)
  @volatile private var closed = false

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
  private val rlmIndexer: RLMIndexer = new RLMIndexer(remoteStorageManager, fetchLog)

  private[remote] def rlmScheduledThreadPoolSize:Int = rlmScheduledThreadPool.poolSize()
  private[remote] def leaderOrFollowerTasksSize:Int = leaderOrFollowerTasks.size()

  private def doHandleLeaderOrFollowerPartitions(topicPartition: TopicPartition,
                                                 convertToLeaderOrFollower: RLMTask => Unit): Unit = {
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

  def onLeadershipChange(partitionsBecomeLeader: Set[Partition], partitionsBecomeFollower: Set[Partition]): Unit = {
    partitionsBecomeFollower.foreach { partition =>
      doHandleLeaderOrFollowerPartitions(partition.topicPartition, task => task.convertToFollower())
    }

    partitionsBecomeLeader.foreach { partition =>
      doHandleLeaderOrFollowerPartitions(partition.topicPartition,
        task => task.convertToLeader(partition.getLeaderEpoch))
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
    this.logIdent = s"[RLMTask partition:$tp ] "
    @volatile var leaderEpoch: Int = -1
    @volatile def isLeader: Boolean = leaderEpoch >= 0

    private var readOffset: Long = rlmIndexer.getOrLoadIndexOffset(tp).getOrElse(0L)

    def convertToLeader(leaderEpochVal:Int): Unit = {
      if (leaderEpochVal < 0) throw new KafkaException(s"leaderEpoch value for topic partition $tp can not be negative")
      leaderEpoch = leaderEpochVal
    }

    def convertToFollower(): Unit = {
      leaderEpoch = -1
    }

    def copyLogSegmentsToRemote(): Unit = {
      try {
        fetchLog(tp).foreach { log => {
          if (isCancelled()) {
            info(s"Skipping copying log segments as the current task is cancelled")
            return
          }
          val highWatermark = log.highWatermark
          if (highWatermark >= 0) {
            // copy segments only till the min of highWatermark or stable-offset
            // remote storage should contain only committed/acked messages
            val fetchOffset = log.firstUnstableOffset match {
              case Some(offsetMetadata) if offsetMetadata < highWatermark => offsetMetadata
              case _ => highWatermark
            }
            if (readOffset >= fetchOffset) {
              info(s"Skipping building indexes, current read offset:$readOffset is >= committed/acked " +
                s"message offset:$fetchOffset ")
              return
            }

            val activeSegBaseOffset = log.activeSegment.baseOffset
            val sortedSegments = log.logSegments(readOffset + 1, fetchOffset).toSeq.sortBy(_.baseOffset)
            val index:Int = sortedSegments.map(x => x.baseOffset).search(activeSegBaseOffset) match {
              case Found(x) => x
              case InsertionPoint(y) => y - 1
            }
            if(index <= 0) {
              debug(s"No segments found to be copied for partition $tp with read offset: $readOffset and active baseoffset: $activeSegBaseOffset")
            } else {
              sortedSegments.slice(0, index+1).foreach { segment =>
                // store locally here as this may get updated after the below if condition is computed as false.
                val leaderEpochVal = leaderEpoch
                if (isCancelled() || !isLeader) {
                  info(
                    s"Skipping copying log segments as the current task state is changed, cancelled:$isCancelled() " +
                      s"leader:$isLeader")
                  return
                }

                val entries = remoteStorageManager.copyLogSegment(tp, segment, leaderEpochVal)
                val file = segment.log.file()
                val fileName = file.getName
                val baseOffsetStr = fileName.substring(0, fileName.indexOf("."))
                rlmIndexer.maybeBuildIndexes(tp, entries.asScala.toSeq, file.getParentFile, baseOffsetStr)
                readOffset = entries.get(entries.size() - 1).lastOffset + 1
              }
            }
          }
        }
        }
      } catch {
        case ex:Exception => error(s"Error occured while copying log segments of partition: $tp", ex)
      }
    }

    def updateRemoteLogIndexes(): Unit = {
      try {
        remoteStorageManager.listRemoteSegments(tp, readOffset + 1).forEach(new Consumer[RemoteLogSegmentInfo] {
          override def accept(rlsInfo: RemoteLogSegmentInfo): Unit = {
            val entries = remoteStorageManager.getRemoteLogIndexEntries(rlsInfo)
            if (!entries.isEmpty) {
              fetchLog(tp).foreach { log: Log =>
                if (isCancelled()) {
                  info(s"Skipping building indexes as the current task is cancelled")
                  return
                }
                val baseOffset = Log.filenamePrefixFromOffset(entries.get(0).firstOffset)
                val logDir = log.activeSegment.log.file().getParentFile
                rlmIndexer.maybeBuildIndexes(tp, entries.asScala.toSeq, logDir, baseOffset)
                readOffset = entries.get(entries.size() - 1).lastOffset
              }
            }

          }
        })
      } catch {
        case ex: Exception => error(s"Error occurred while updating remote log indexes for partition:$tp ", ex)
      }
    }

    def handleExpiredRemoteLogSegments(): Unit = {

      def handleLogStartOffsetUpdate(topicPartition: TopicPartition, logStartOffset: Long): Unit = {
        updateRemoteLogStartOffset(topicPartition, logStartOffset)
        info(s"Cleaning remote log indexes of partition $topicPartition till logStartOffset:$logStartOffset")

        // need to cleanup only when any segments beyond logStartOffset exists
        if(logStartOffset > 0) {
          // remove all indexes earlier to lso, may be rename now and they will GCed later.
          val cleanedUpIndexes = rlmIndexer.maybeLoadIndex(topicPartition).cleanupIndexesUntil(logStartOffset)

          // deleting files in the current thread for now. These can be scheduled in a separate thread pool as it is more of
          // a cleanup activity. If the broker shutsdown in the middle of this activity, restart will delete all these files
          // as part of deleting files with a suffix of [Log.DeletedFileSuffix]
          cleanedUpIndexes.foreach { x => x.deleteIfExists() }
        }
      }

      try {
        val remoteLso: Long =
          if (isLeader) {
            remoteStorageManager.cleanupLogUntil(tp, time.milliseconds() - rlmConfig.remoteLogRetentionMillis)
          } else {
            remoteStorageManager.earliestLogOffset(tp)
          }

        if (remoteLso >= 0) handleLogStartOffsetUpdate(tp, remoteLso)
      } catch {
        case ex:Exception => error(s"Error while cleaningup log segments for partition: $tp", ex)
      }
    }

    override def run(): Unit = {
      try {
        if (!isCancelled()) {
          //a. copy log segments to remote store
          if (isLeader) copyLogSegmentsToRemote()

          //b. fetch missing remote index files
          updateRemoteLogIndexes()

          //c. cleanup/delete expired remote segments
          handleExpiredRemoteLogSegments()
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
   * A remote log read task returned by asyncRead(). The caller of asyncRead() can use this object to cancel a
   * pending task or check if the task is done.
   */
  case class AsyncReadTask(future: Future[Unit]) {
    def cancel(mayInterruptIfRunning: Boolean): Boolean = {
      val r = future.cancel(mayInterruptIfRunning)
      if (r) {
        // Removed the cancelled task from task queue
        remoteStorageFetcherThreadPool.purge()
      }
      r
    }

    def isCancelled: Boolean = future.isCancelled

    def isDone: Boolean = future.isDone
  }

  /**
   * Submit a remote log read task.
   *
   * This method returns immediately. The read operation is executed in a thread pool.
   * The callback will be called when the task is done.
   *
   * @throws RejectedExecutionException if the task cannot be accepted for execution (task queue is full)
   */
  def asyncRead(fetchInfo: RemoteStorageFetchInfo, callback: (RemoteLogReadResult) => Unit): AsyncReadTask = {
    AsyncReadTask(remoteStorageFetcherThreadPool.submit(new RemoteLogReader(fetchInfo, this, callback)))
  }

  /**
   * Stops all the threads and releases all the resources.
   */
  def close(): Unit = {
    if (closed)
      warn("Trying to close an already closed RemoteLogManager")
    else this synchronized {
      Utils.closeQuietly(remoteStorageManager, "RemoteLogStorageManager")
      leaderOrFollowerTasks.values().forEach(new Consumer[RLMTaskWithFuture] {
        override def accept(taskWithFuture: RLMTaskWithFuture): Unit = taskWithFuture.cancel()
      })
      rlmScheduledThreadPool.shutdown()
      closed = true
    }
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
                                  remoteLogReaderThreads: Int,
                                  remoteLogReaderMaxPendingTasks: Int,
                                  remoteStorageConfig: Map[String, Any], remoteLogManagerThreadPoolSize: Int,
                                  remoteLogManagerTaskIntervalMs: Long)

object RemoteLogManager {
  def REMOTE_STORAGE_MANAGER_CONFIG_PREFIX = "remote.log.storage."

  def DefaultConfig = RemoteLogManagerConfig(remoteLogStorageEnable = Defaults.RemoteLogStorageEnable, null,
    Defaults.RemoteLogRetentionBytes, TimeUnit.MINUTES.toMillis(Defaults.RemoteLogRetentionMinutes),
    Defaults.RemoteLogReaderThreads, Defaults.RemoteLogReaderMaxPendingTasks, Map.empty,
    Defaults.RemoteLogManagerThreadPoolSize, Defaults.RemoteLogManagerTaskIntervalMs)

  def createRemoteLogManagerConfig(config: KafkaConfig): RemoteLogManagerConfig = {
    var rsmProps = Map[String, Any]()
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
      config.remoteLogRetentionBytes, config.remoteLogRetentionMillis,
      config.remoteLogReaderThreads, config.remoteLogReaderMaxPendingTasks, rsmProps.toMap,
      config.remoteLogManagerThreadPoolSize, config.remoteLogManagerTaskIntervalMs)
  }
}