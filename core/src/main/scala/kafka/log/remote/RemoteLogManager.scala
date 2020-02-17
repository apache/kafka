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
import java.nio.ByteBuffer
import java.util
import java.util.Optional
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.{BiConsumer, Consumer, Function}

import kafka.cluster.Partition
import kafka.common.KafkaException
import kafka.log.Log
import kafka.server.{Defaults, FetchDataInfo, KafkaConfig, LogOffsetMetadata, RemoteStorageFetchInfo}
import kafka.utils.Logging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.log.remote.storage.{ClassLoaderAwareRemoteLogMetadataManager, LogSegmentData, RemoteLogMetadataManager, RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogStorageManager}
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.{ChildFirstClassLoader, KafkaThread, Time, Utils}

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
    info(s"Resizing pool from ${scheduledThreadPool.getCorePoolSize} to $size")
    scheduledThreadPool.setCorePoolSize(size)
  }

  def poolSize(): Int = scheduledThreadPool.getMaximumPoolSize

  def scheduleWithFixedDelay(runnable: Runnable, initialDelay: Long, delay: Long,
                             timeUnit: TimeUnit): ScheduledFuture[_] = {
    info(s"Scheduling runnable $runnable with initial dalay: $initialDelay , fixed delay: $delay")
    scheduledThreadPool.scheduleWithFixedDelay(runnable, initialDelay, delay, timeUnit)
  }

  def scheduleOnceWithDelay(runnable: Runnable, delay: Long,
                            timeUnit: TimeUnit): ScheduledFuture[_] = {
    info(s"Scheduling runnable $runnable once with delay: $delay")
    scheduledThreadPool.schedule(runnable, delay, timeUnit)
  }

  def shutdown(): Unit = {
    info("Shutting down scheduled thread pool")
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
                       time: Time = Time.SYSTEM,
                       localBootStrapServers: String,
                       brokerId: Int,
                       logDir: String) extends Logging with Closeable {
  private val leaderOrFollowerTasks: ConcurrentHashMap[TopicPartition, RLMTaskWithFuture] =
    new ConcurrentHashMap[TopicPartition, RLMTaskWithFuture]()
  private val remoteStorageFetcherThreadPool = new RemoteStorageReaderThreadPool(rlmConfig.remoteLogReaderThreads,
    rlmConfig.remoteLogReaderMaxPendingTasks, time)

  private val delayInMs = rlmConfig.remoteLogManagerTaskIntervalMs
  private val poolSize = rlmConfig.remoteLogManagerThreadPoolSize
  private val rlmScheduledThreadPool = new RLMScheduledThreadPool(poolSize)
  @volatile private var closed = false

  private def createRemoteStorageManager(): RemoteLogStorageManager = {
    val classPath = rlmConfig.remoteLogStorageManagerClassPath
    val rsmClassLoader = {
      if (classPath != null && !classPath.trim.isEmpty) {
        new ChildFirstClassLoader(classPath, RemoteLogManager.getClass.getClassLoader)
      } else {
        RemoteLogManager.getClass.getClassLoader
      }
    }

    val rsm = rsmClassLoader.loadClass(rlmConfig.remoteLogStorageManagerClass)
      .getDeclaredConstructor().newInstance().asInstanceOf[RemoteLogStorageManager]
    val rsmWrapper = new ClassLoaderAwareRemoteStorageManager(rsm, rsmClassLoader)

    val rsmProps = new util.HashMap[String, Any]()
    rlmConfig.remoteStorageConfig.foreach { case (k, v) => rsmProps.put(k, v) }
    rsmProps.put(KafkaConfig.RemoteLogRetentionMillisProp, rlmConfig.remoteLogRetentionMillis)
    rsmProps.put(KafkaConfig.RemoteLogRetentionBytesProp, rlmConfig.remoteLogRetentionBytes)
    rsmWrapper.configure(rsmProps)
    rsmWrapper
  }

  private def createRemoteLogMetadataManager(): RemoteLogMetadataManager = {
    val classPath = rlmConfig.remoteLogMetadataManagerClassPath

    val rlmm: RemoteLogMetadataManager = if (classPath != null && !classPath.trim.isEmpty) {
      val rlmmClassLoader = new ChildFirstClassLoader(classPath, RemoteLogManager.getClass.getClassLoader)
      val rlmmLoaded = rlmmClassLoader.loadClass(rlmConfig.remoteLogMetadataManagerClass)
        .getDeclaredConstructor().newInstance().asInstanceOf[RemoteLogMetadataManager]
      new ClassLoaderAwareRemoteLogMetadataManager(rlmmLoaded, rlmmClassLoader)
    } else {
      this.getClass.getClassLoader.loadClass(rlmConfig.remoteLogStorageManagerClass).getDeclaredConstructor()
        .newInstance().asInstanceOf[RemoteLogMetadataManager]
    }

    val rlmmProps = new util.HashMap[String, Any]()
    rlmConfig.remoteStorageConfig.foreach { case (k, v) => rlmmProps.put(k, v) }
    rlmmProps.put("log.dir", logDir)
    rlmmProps.put(RemoteLogMetadataManager.BROKER_ID_CONFIG, brokerId)
    rlmmProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, localBootStrapServers)
    rlmm.configure(rlmmProps)
    rlmm
  }

  private val remoteLogStorageManager: RemoteLogStorageManager = createRemoteStorageManager()
  private val remoteLogMetadataManager: RemoteLogMetadataManager = createRemoteLogMetadataManager()

  private[remote] def rlmScheduledThreadPoolSize: Int = rlmScheduledThreadPool.poolSize()

  private[remote] def leaderOrFollowerTasksSize: Int = leaderOrFollowerTasks.size()

  private def doHandleLeaderOrFollowerPartitions(topicPartition: TopicPartition,
                                                 convertToLeaderOrFollower: RLMTask => Unit): Unit = {
    val rlmTaskWithFuture = leaderOrFollowerTasks.computeIfAbsent(topicPartition,
      new Function[TopicPartition, RLMTaskWithFuture] {
        override def apply(tp: TopicPartition): RLMTaskWithFuture = {
          val task = new RLMTask(tp)
          //set this upfront when it is getting initialized instead of doing it after scheduling.
          convertToLeaderOrFollower(task)
          info(s"Created a new task: $task and getting scheduled")
          val future = rlmScheduledThreadPool.scheduleWithFixedDelay(task, 0, delayInMs, TimeUnit.MILLISECONDS)
          RLMTaskWithFuture(task, future)
        }
      })
    convertToLeaderOrFollower(rlmTaskWithFuture.rlmTask)
  }

  /**
   * Callback to receive any leadership changes for the topic partitions assigned to this broker. If there are no
   * existing tasks for a given topic partition then it will assign new leader or follower task else it will convert the
   * task to respective target state(leader or follower).
   *
   * @param partitionsBecomeLeader
   * @param partitionsBecomeFollower
   */
  def onLeadershipChange(partitionsBecomeLeader: Set[Partition], partitionsBecomeFollower: Set[Partition]): Unit = {
    // Partitions logs are available when this callback is invoked.
    // Compact topics are filtered here as they are not supported with tiered storage.
    partitionsBecomeFollower.filter(partition => partition.log.exists(log => !log.config.compact))
      .foreach { partition =>
        doHandleLeaderOrFollowerPartitions(partition.topicPartition, task => task.convertToFollower())
      }

    partitionsBecomeLeader.filter(partition => partition.log.exists(log => !log.config.compact))
      .foreach { partition =>
        doHandleLeaderOrFollowerPartitions(partition.topicPartition,
          task => task.convertToLeader(partition.getLeaderEpoch))
      }
  }

  /**
   * Stops partitions for copying segments, building indexes and deletes the partition in remote storage if delete flag
   * is set as true.
   *
   * @param topicPartitions Set of topic partitions.
   * @param delete          flag to indicate whether the given topic partitions to be deleted or not.
   */
  def stopPartitions(topicPartitions: Set[TopicPartition], delete: Boolean): Unit = {
    topicPartitions.foreach { tp =>
      // unassign topic partitions from RLM leader/follower
      val rlmTaskWithFuture = leaderOrFollowerTasks.remove(tp)
      if (rlmTaskWithFuture != null) {
        rlmTaskWithFuture.cancel()
      }

      // todo-sato may need to do this asychronously as this call should not block partition#stop(delete).
      if (delete) {
        try {

          remoteLogMetadataManager.listRemoteLogSegments(tp).forEach(new Consumer[RemoteLogSegmentMetadata] {
            override def accept(t: RemoteLogSegmentMetadata): Unit = {
              deleteRemoteLogSegment(t)
            }
          })

        } catch {
          case ex: Exception => error(s"Error occurred while deleting topic partition: $tp", ex)
        }
      }
    }
  }

  private def deleteRemoteLogSegment(metadata: RemoteLogSegmentMetadata) = {
    try {
      remoteLogStorageManager.deleteLogSegment(metadata)
    } catch {
      case e: Exception => {
        //todo collect failed segment ids and store them in a dead letter topic.
      }
    }
  }

  class RLMTask(tp: TopicPartition) extends CancellableRunnable with Logging {
    this.logIdent = s"[RLMTask partition:$tp ] "
    @volatile private var leaderEpoch: Int = -1

    private def isLeader(): Boolean = leaderEpoch >= 0

    // The highest offset that is already there in the remote storage
    // When looking for new remote segments, we will only look for the remote segments that contains larger offsets
    private var readOffset: Long = {
      remoteLogMetadataManager.listRemoteLogSegments(tp).forEach(new Consumer[RemoteLogSegmentMetadata] {
        override def accept(x: RemoteLogSegmentMetadata): Unit = {

        }
      })
      0
    }

    fetchLog(tp).foreach { log => log.updateRemoteIndexHighestOffset(readOffset) }

    def convertToLeader(leaderEpochVal: Int): Unit = {
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

          // LSO indicates the offset below are ready to be consumed(high-watermark or committed)
          val lso = log.lastStableOffset
          if (lso < 0) {
            warn(s"lastStableOffset for partition $tp is $lso, which should not be negative.")
          } else if (lso > 0 && readOffset < lso) {
            // copy segments only till the min of high-watermark or stable-offset
            // remote storage should contain only committed/acked messages
            val fetchOffset = lso
            debug(s"Checking for segments to copy, readOffset: $readOffset and fetchOffset: $fetchOffset")
            val activeSegBaseOffset = log.activeSegment.baseOffset
            val sortedSegments = log.logSegments(readOffset + 1, fetchOffset).toSeq.sortBy(_.baseOffset)
            val index: Int = sortedSegments.map(x => x.baseOffset).search(activeSegBaseOffset) match {
              case Found(x) => x
              case InsertionPoint(y) => y - 1
            }
            if (index <= 0) {
              debug(s"No segments found to be copied for partition $tp with read offset: $readOffset and active " +
                s"baseoffset: $activeSegBaseOffset")
            } else {
              sortedSegments.slice(0, index).foreach { segment =>
                // store locally here as this may get updated after the below if condition is computed as false.
                val leaderEpochVal = leaderEpoch
                if (isCancelled() || !isLeader()) {
                  info(s"Skipping copying log segments as the current task state is changed, cancelled: " +
                    s"$isCancelled() leader:$isLeader()")
                  return
                }

                val file = segment.log.file()
                val fileName = file.getName
                info(s"Copying $fileName to remote storage.");
                val id = new RemoteLogSegmentId(tp, java.util.UUID.randomUUID())
                remoteLogMetadataManager.putRemoteLogSegmentData(id,
                  new RemoteLogSegmentMetadata(id, segment.baseOffset, segment.baseOffset, leaderEpochVal, null))
                val segmentData = new LogSegmentData(file, segment.lazyOffsetIndex.get.file,
                  segment.lazyTimeIndex.get.file)
                val remoteLogContext = remoteLogStorageManager.copyLogSegment(id, segmentData)

                remoteLogMetadataManager.putRemoteLogSegmentData(id,
                  new RemoteLogSegmentMetadata(id, segment.baseOffset, segment.baseOffset, leaderEpochVal,
                    System.currentTimeMillis(),
                    remoteLogContext.asBytes()))
                //todo double check on this
                readOffset = segment.readNextOffset - 1
              }
            }
          } else {
            debug(s"Skipping copying segments, current read offset:$readOffset is and LSO:$lso ")
          }
        }
        }
      } catch {
        case ex: Exception => error(s"Error occurred while copying log segments of partition: $tp", ex)
      }
    }

    def handleExpiredRemoteLogSegments(): Unit = {

      def handleLogStartOffsetUpdate(topicPartition: TopicPartition, remoteLogStartOffset: Long): Unit = {
        updateRemoteLogStartOffset(topicPartition, remoteLogStartOffset)
        debug(
          s"Cleaning remote log indexes of partition $topicPartition till remoteLogStartOffset:$remoteLogStartOffset")
        // remove all indexes earlier to lso and rename them with a suffix of [Log.DeletedFileSuffix].
        info(
          s"Deleting remote log indexes of partition $topicPartition till remoteLogStartOffset:$remoteLogStartOffset")

        // deleting files in the current thread for now. These can be scheduled in a separate thread pool as it is more of
        // a cleanup activity. If the broker shutsdown in the middle of this activity, restart will delete all these files
        // as part of deleting files with a suffix of [Log.DeletedFileSuffix]
        // cleanedUpIndexes
      }

      try {
        val remoteLogStartOffset: Option[Long] =
          if (isLeader()) {
            // cleanup remote log segments
            val cleanupTs = time.milliseconds() - rlmConfig.remoteLogRetentionMillis
            var minOffset: Long = 0L
            remoteLogMetadataManager.listRemoteLogSegments(tp).asScala.takeWhile(m => m.createdTimestamp() <= cleanupTs)
              .foreach(m => {
                deleteRemoteLogSegment(m)
                minOffset = Math.min(m.endOffset(), minOffset)
              })

            Some(minOffset)
          } else {
            val result = remoteLogMetadataManager.earliestLogOffset(tp)
            if (result.isPresent) Some(result.get()) else None
          }

        remoteLogStartOffset.foreach(x => handleLogStartOffsetUpdate(tp, x))
      } catch {
        case ex: Exception => error(s"Error while cleaningup log segments for partition: $tp", ex)
      }
    }

    override def run(): Unit = {
      try {
        if (!isCancelled()) {
          //a. copy log segments to remote store
          if (isLeader()) copyLogSegmentsToRemote()

          //b. cleanup/delete expired remote segments
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

    override def toString: String = {
      this.getClass + s"[$tp]"
    }
  }

  def read(fetchMaxByes: Int, minOneMessage: Boolean, tp: TopicPartition, fetchInfo: PartitionData): FetchDataInfo = {
    val offset = fetchInfo.fetchOffset
    val remoteLogSegmentMetadata = getRemoteLogSegmentMetadata(tp, offset)

    //todo-tier compute start position and end position from offset index
    val startPos = 0L
    val endPos = 0L
    val is = remoteLogStorageManager.fetchLogSegmentData(remoteLogSegmentMetadata, startPos, Optional.of(endPos))
    // check for int.max bounds
    val length: Int = (endPos - startPos).toInt
    val buffer = ByteBuffer.allocate(length)
    Utils.readFully(is, buffer)
    val records = MemoryRecords.readableRecords(buffer)
    FetchDataInfo(LogOffsetMetadata(offset), records)
  }

  private def getRemoteLogSegmentMetadata(tp: TopicPartition, offset: Long) = {
    val remoteLogSegmentId = remoteLogMetadataManager.getRemoteLogSegmentId(tp, offset)

    if (remoteLogSegmentId == null) throw new OffsetOutOfRangeException(
      s"Received request for offset $offset for partition $tp, which does not exist in remote tier")

    val remoteLogSegmentMetadata = remoteLogMetadataManager.getRemoteLogSegmentMetadata(remoteLogSegmentId)
    if (remoteLogSegmentMetadata == null) throw new OffsetOutOfRangeException(
      s"Received request for offset $offset for partition $tp, which does not exist in remote tier")

    remoteLogSegmentMetadata
  }

  /**
   * Search the message offset in the remote storage based on timestamp and offset.
   *
   * This method returns an option of TimestampOffset. The returned value is determined using the following ordered list of rules:
   *
   * - If there is no messages in the remote storage, return None
   * - If all the messages in the remote storage have smaller offsets, return None
   * - If all the messages in the remote storage have smaller timestamps, return None
   * - If all the messages in the remote storage have larger timestamps, or no message in the remote storage has a timestamp
   * the returned offset will be max(the earliest offset in the remote storage, startingOffset) and the timestamp will
   * be Message.NoTimestamp.
   * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
   * is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
   *
   * @param timestamp      The timestamp to search for.
   * @param startingOffset The starting offset to search.
   * @return the timestamp and offset of the first message that meets the requirements. None will be returned if there is no such message.
   */
  def findOffsetByTimestamp(tp: TopicPartition, timestamp: Long, startingOffset: Long): Option[TimestampAndOffset] = {
    //todo rlmIndexer.lookupEntryForTimestamp(tp, timestamp, startingOffset).map(entry =>
    //  remoteLogStorageManager.findOffsetByTimestamp(entry, timestamp, startingOffset))
    None
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
      Utils.closeQuietly(remoteLogStorageManager, "RemoteLogStorageManager")
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
      try {
        future.cancel(true)
      } catch {
        case ex: Exception => error(s"Error occurred while canceling the task: $rlmTask", ex)
      }
    }
  }

}

case class RemoteLogManagerConfig(remoteLogStorageEnable: Boolean, remoteLogStorageManagerClass: String,
                                  remoteLogStorageManagerClassPath: String, remoteLogRetentionBytes: Long,
                                  remoteLogRetentionMillis: Long, remoteLogReaderThreads: Int,
                                  remoteLogReaderMaxPendingTasks: Int, remoteStorageConfig: Map[String, Any],
                                  remoteLogManagerThreadPoolSize: Int, remoteLogManagerTaskIntervalMs: Long,
                                  remoteLogMetadataManagerClass: String = "",
                                  remoteLogMetadataManagerClassPath: String = "",
                                  listenerName: Option[String] = None)

object RemoteLogManager {
  def REMOTE_STORAGE_MANAGER_CONFIG_PREFIX = "remote.log.storage."

  def DefaultConfig = RemoteLogManagerConfig(remoteLogStorageEnable = Defaults.RemoteLogStorageEnable, null, null,
    Defaults.RemoteLogRetentionBytes, TimeUnit.MINUTES.toMillis(Defaults.RemoteLogRetentionMinutes),
    Defaults.RemoteLogReaderThreads, Defaults.RemoteLogReaderMaxPendingTasks, Map.empty,
    Defaults.RemoteLogManagerThreadPoolSize, Defaults.RemoteLogManagerTaskIntervalMs, Defaults.RemoteLogMetadataManager,
    Defaults.RemoteLogMetadataManagerClassPath)

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
      config.remoteLogStorageManagerClassPath,
      config.remoteLogRetentionBytes, config.remoteLogRetentionMillis,
      config.remoteLogReaderThreads, config.remoteLogReaderMaxPendingTasks, rsmProps.toMap,
      config.remoteLogManagerThreadPoolSize, config.remoteLogManagerTaskIntervalMs, config.remoteLogMetadataManager,
      config.remoteLogMetadataManagerClassPath)
  }
}