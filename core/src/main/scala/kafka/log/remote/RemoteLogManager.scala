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

import java.io.{Closeable, InputStream}
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
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.log.remote.storage.{ClassLoaderAwareRemoteLogMetadataManager, LogSegmentData, RemoteLogMetadataManager, RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteStorageManager}
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch, RemoteLogInputStream}
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
                       clusterId: String = "",
                       logDir: String) extends Logging with Closeable {
  private val leaderOrFollowerTasks: ConcurrentHashMap[TopicPartition, RLMTaskWithFuture] =
    new ConcurrentHashMap[TopicPartition, RLMTaskWithFuture]()
  private val remoteStorageFetcherThreadPool = new RemoteStorageReaderThreadPool(rlmConfig.remoteLogReaderThreads,
    rlmConfig.remoteLogReaderMaxPendingTasks, time)

  private val delayInMs = rlmConfig.remoteLogManagerTaskIntervalMs
  private val poolSize = rlmConfig.remoteLogManagerThreadPoolSize
  private val rlmScheduledThreadPool = new RLMScheduledThreadPool(poolSize)
  @volatile private var closed = false

  private def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = {
    val classPath = rlmConfig.remoteLogStorageManagerClassPath
    val rsmClassLoader = {
      if (classPath != null && !classPath.trim.isEmpty) {
        new ChildFirstClassLoader(classPath, RemoteLogManager.getClass.getClassLoader)
      } else {
        RemoteLogManager.getClass.getClassLoader
      }
    }

    val rsm = rsmClassLoader.loadClass(rlmConfig.remoteLogStorageManagerClass)
      .getDeclaredConstructor().newInstance().asInstanceOf[RemoteStorageManager]
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
      this.getClass.getClassLoader.loadClass(rlmConfig.remoteLogMetadataManagerClass).getDeclaredConstructor()
        .newInstance().asInstanceOf[RemoteLogMetadataManager]
    }

    val rlmmProps = new util.HashMap[String, Any]()
    rlmConfig.remoteStorageConfig.foreach { case (k, v) => rlmmProps.put(k, v) }
    rlmmProps.put("log.dir", logDir)
    rlmmProps.put(RemoteLogMetadataManager.BROKER_ID, brokerId)
    rlmmProps.put(RemoteLogMetadataManager.CLUSTER_ID, clusterId)
    rlmmProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, localBootStrapServers)
    rlmm.configure(rlmmProps)
    rlmm
  }

  private val remoteLogStorageManager: ClassLoaderAwareRemoteStorageManager = createRemoteStorageManager()
  private val remoteLogMetadataManager: RemoteLogMetadataManager = createRemoteLogMetadataManager()

  private val indexCache = new RemoteIndexCache(remoteStorageManager = remoteLogStorageManager, logDir = logDir)

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

  def onServerStarted(serverEndpoint: String): Unit = {
    remoteLogMetadataManager.onServerStarted(serverEndpoint)
  }

  def storageManager(): RemoteStorageManager = {
    remoteLogStorageManager.delegate()
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
    // Compact topics and internal topics are filtered here as they are not supported with tiered storage.
    def filterPartitions(partitions: Set[Partition]): Set[Partition] = {
      partitions.filter(partition => !(Topic.isInternal(partition.topic) || partition.log.exists(log => log.config.compact)))
    }

    val followerTopicPartitions = filterPartitions(partitionsBecomeFollower).map(p => p.topicPartition)
    val leaderPartitions = filterPartitions(partitionsBecomeLeader)
    remoteLogMetadataManager.onPartitionLeadershipChanges(leaderPartitions.map(p => p.topicPartition).asJava, followerTopicPartitions.asJava)

    followerTopicPartitions.foreach { tp => doHandleLeaderOrFollowerPartitions(tp, task => task.convertToFollower())}

    leaderPartitions.foreach { partition =>
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

      if (delete) {
        try {
          //todo-tier need to check whether it is really needed to delete from remote. This may be a delete request only
          //for this replica. We should delete from remote storage only if the topic partition is getting deleted.
          remoteLogMetadataManager.listRemoteLogSegments(tp).forEach(new Consumer[RemoteLogSegmentMetadata] {
            override def accept(t: RemoteLogSegmentMetadata): Unit = {
              deleteRemoteLogSegment(t)
            }
          })

          remoteLogMetadataManager.onStopPartitions(topicPartitions.asJava)
        } catch {
          case ex: Exception => error(s"Error occurred while deleting topic partition: $tp", ex)
        }
      }
    }
  }

  private def deleteRemoteLogSegment(metadata: RemoteLogSegmentMetadata) = {
    try {
      //todo-tier delete in RLMM in 2 phases to avoid dangling objects
      remoteLogMetadataManager.deleteRemoteLogSegmentMetadata(metadata.remoteLogSegmentId())
      remoteLogStorageManager.deleteLogSegment(metadata)
    } catch {
      case e: Exception => {
        //todo-tier collect failed segment ids and store them in a dead letter topic.
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
      val metadatas = remoteLogMetadataManager.listRemoteLogSegments(tp)
      if(metadatas.isEmpty) -1 // Corner case when the first segment's base offset is 1 and contains only 1 record.
      else {
        metadatas.asScala.max(new Ordering[RemoteLogSegmentMetadata]() {
          override def compare(x: RemoteLogSegmentMetadata,
                               y: RemoteLogSegmentMetadata): Int = {
            // todo-tier double check whether it covers all the cases
            (x.startOffset() - y.startOffset()).toInt
          }
        }).endOffset()
      }
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
            if (index < 0) {
              debug(s"No segments found to be copied for partition $tp with read offset: $readOffset and active " +
                s"baseoffset: $activeSegBaseOffset")
            } else {
              sortedSegments.slice(0, index).foreach { segment =>
                // store locally here as this may get updated after the below if condition is computed as false.
                val leaderEpochVal = leaderEpoch
                if (isCancelled() || !isLeader()) {
                  info(s"Skipping copying log segments as the current task state is changed, cancelled: " +
                    s"${isCancelled()} leader:${isLeader()}")
                  return
                }

                val file = segment.log.file()
                val fileName = file.getName
                info(s"Copying $fileName to remote storage.");
                val id = new RemoteLogSegmentId(tp, java.util.UUID.randomUUID())

                //todo-tier double check on this
                val endOffset = segment.readNextOffset - 1
                remoteLogMetadataManager.putRemoteLogSegmentData(new RemoteLogSegmentMetadata(id, segment.baseOffset, endOffset, segment.maxTimestampSoFar,
                                    leaderEpochVal, null))

                val segmentData = new LogSegmentData(file, segment.lazyOffsetIndex.get.file,
                  segment.lazyTimeIndex.get.file)
                val remoteLogContext = remoteLogStorageManager.copyLogSegment(id, segmentData)

                remoteLogMetadataManager.putRemoteLogSegmentData(new RemoteLogSegmentMetadata(id, segment.baseOffset, endOffset, segment.maxTimestampSoFar, leaderEpochVal, System.currentTimeMillis(), false, remoteLogContext.asBytes()))

                readOffset = endOffset
                log.updateRemoteIndexHighestOffset(readOffset)
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
        debug(s"Updating $topicPartition with remoteLogStartOffset: $remoteLogStartOffset")
        updateRemoteLogStartOffset(topicPartition, remoteLogStartOffset)
      }

      try {
        val remoteLogStartOffset: Option[Long] =
          if (isLeader()) {
            // cleanup remote log segments
            val remoteLogSegmentMetadatas = remoteLogMetadataManager.listRemoteLogSegments(tp)
            if(remoteLogSegmentMetadatas.isEmpty) None
            else {
              var maxOffset: Long = Long.MinValue
              val cleanupTs = time.milliseconds() - rlmConfig.remoteLogRetentionMillis
              remoteLogSegmentMetadatas.asScala.takeWhile(m => m.createdTimestamp() <= cleanupTs)
                .foreach(m => {
                  deleteRemoteLogSegment(m)
                  maxOffset = Math.max(m.endOffset(), maxOffset)
                })

              if(maxOffset == Long.MinValue) None else Some(maxOffset+1)
            }
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
          else fetchLog(tp).foreach { log =>
            val offset = remoteLogMetadataManager.highestLogOffset(tp)
            if (offset.isPresent) log.updateRemoteIndexHighestOffset(offset.get())
          }

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

  def lookupPositionForOffset(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, offset: Long): Long = {
    indexCache.lookupOffset(remoteLogSegmentMetadata, offset)
  }

  def read(remoteStorageFetchInfo: RemoteStorageFetchInfo): FetchDataInfo = {
    val fetchMaxBytes  = remoteStorageFetchInfo.fetchMaxByes
    val tp = remoteStorageFetchInfo.topicPartition
    val fetchInfo: PartitionData = remoteStorageFetchInfo.fetchInfo
    val fetchIsolation = remoteStorageFetchInfo.fetchIsolation

    val offset = fetchInfo.fetchOffset
    val maxBytes = Math.min(fetchMaxBytes, fetchInfo.maxBytes)
    val rlsMetadata = getRemoteLogSegmentMetadata(tp, offset)

    val startPos = lookupPositionForOffset(rlsMetadata, offset)
    var remoteSegInputStream: InputStream = null
    try {
      // Search forward for the position of the last offset that is greater than or equal to the target offset
      remoteSegInputStream = remoteLogStorageManager.fetchLogSegmentData(rlsMetadata, startPos, Int.MaxValue)
      val remoteLogInputStream = new RemoteLogInputStream(remoteSegInputStream)

      var firstBatch:RecordBatch = null
      def nextBatch(): RecordBatch = {
        firstBatch = remoteLogInputStream.nextBatch()
        firstBatch
      }
      // Look for the batch which has the desired offset
      // we will always have a batch in that segment as it is a non-compacted topic. For compacted topics, we may need
      //to read from the subsequent segments if there is no batch available for the desired offset in the current
      //segment. That means, desired offset is more than last offset of the current segment and immediate available
      //offset exists in the next segment which can be higher than the desired offset.
      while (nextBatch() != null && firstBatch.lastOffset < offset) {
      }

      if (firstBatch == null)
        return FetchDataInfo(LogOffsetMetadata(offset), MemoryRecords.EMPTY)

      val updatedFetchSize = if (remoteStorageFetchInfo.minOneMessage && firstBatch.sizeInBytes() > maxBytes) firstBatch.sizeInBytes() else maxBytes

      val buffer = ByteBuffer.allocate(updatedFetchSize)
      var remainingBytes = updatedFetchSize

      firstBatch.writeTo(buffer)
      remainingBytes -= firstBatch.sizeInBytes()

      if(remainingBytes > 0) {
        // input stream is read till (startPos - 1) while getting the batch of records earlier.
        // read the input stream until min of (EOF stream or buffer's remaining capacity).
        Utils.readFully(remoteSegInputStream, buffer)
      }
      buffer.flip()

      val records = MemoryRecords.readableRecords(buffer)
      FetchDataInfo(LogOffsetMetadata(offset), records)
    } finally {
      Utils.closeQuietly(remoteSegInputStream, "RemoteLogSegmentInputStream")
    }
  }

  private def getRemoteLogSegmentMetadata(tp: TopicPartition, offset: Long): RemoteLogSegmentMetadata = {
    val remoteLogSegmentId = remoteLogMetadataManager.getRemoteLogSegmentId(tp, offset)

    if (remoteLogSegmentId == null) throw new OffsetOutOfRangeException(
      s"Received request for offset $offset for partition $tp, which does not exist in remote tier")

    val remoteLogSegmentMetadata = remoteLogMetadataManager.getRemoteLogSegmentMetadata(remoteLogSegmentId)
    if (remoteLogSegmentMetadata == null) throw new OffsetOutOfRangeException(
      s"Received request for offset $offset for partition $tp, which does not exist in remote tier")

    remoteLogSegmentMetadata
  }

  def lookupTimestamp(rlsMetadata: RemoteLogSegmentMetadata, timestamp: Long, startingOffset: Long): Option[TimestampAndOffset] = {
    val startPos = indexCache.lookupTimestamp(rlsMetadata, timestamp, startingOffset)

    var remoteSegInputStream: InputStream = null
    try {
      // Search forward for the position of the last offset that is greater than or equal to the target offset
      remoteSegInputStream = remoteLogStorageManager.fetchLogSegmentData(rlsMetadata, startPos, Int.MaxValue)
      val remoteLogInputStream = new RemoteLogInputStream(remoteSegInputStream)
      var batch: RecordBatch = null
      def nextBatch(): RecordBatch = {
        batch = remoteLogInputStream.nextBatch()
        batch
      }
      while (nextBatch() != null) {
        if (batch.maxTimestamp >= timestamp && batch.lastOffset >= startingOffset) {
          batch.iterator.asScala.foreach(record => {
            if (record.timestamp >= timestamp && record.offset >= startingOffset)
              return Some(new TimestampAndOffset(record.timestamp, record.offset, maybeLeaderEpoch(batch.partitionLeaderEpoch)))
          })
        }
      }
      None
    } finally {
      Utils.closeQuietly(remoteSegInputStream, "RemoteLogSegmentInputStream")
    }
  }

  private def maybeLeaderEpoch(leaderEpoch: Int): Optional[Integer] = {
    if (leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH)
      Optional.empty()
    else
      Optional.of(leaderEpoch)
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
    remoteLogMetadataManager.listRemoteLogSegments(tp).asScala.foreach(rlsMetadata =>
      if (rlsMetadata.maxTimestamp() >= timestamp && rlsMetadata.endOffset() >= startingOffset) {
        val timestampOffset = lookupTimestamp(rlsMetadata, timestamp, startingOffset)
        if (timestampOffset.isDefined)
          return timestampOffset
      }
    )

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
      Utils.closeQuietly(remoteLogMetadataManager, "RemoteLogMetadataManager")
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
  def REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX = "remote.log.metadata."

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
          case key: String if key.startsWith(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX)
            || key.startsWith(REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX) =>
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
