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

import kafka.cluster.Partition
import kafka.log.UnifiedLog
import kafka.server.KafkaConfig
import kafka.utils.Logging
import org.apache.kafka.common._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{RecordBatch, RemoteLogInputStream}
import org.apache.kafka.common.utils.{ChildFirstClassLoader, KafkaThread, Time, Utils}
import org.apache.kafka.server.common.CheckpointFile.CheckpointWriteBuffer
import org.apache.kafka.server.log.remote.metadata.storage.{ClassLoaderAwareRemoteLogMetadataManager, TopicBasedRemoteLogMetadataManagerConfig}
import org.apache.kafka.server.log.remote.storage._
import org.apache.kafka.storage.internals.checkpoint.{LeaderEpochCheckpoint, LeaderEpochCheckpointFile}
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache
import org.apache.kafka.storage.internals.log.EpochEntry

import java.io._
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.security.{AccessController, PrivilegedAction}
import java.util.Optional
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import java.{lang, util}
import scala.collection.Searching._
import scala.collection.Set
import scala.jdk.CollectionConverters._

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

  def poolSize(): Int = scheduledThreadPool.getCorePoolSize

  def getIdlePercent(): Double = {
    1 - scheduledThreadPool.getActiveCount().asInstanceOf[Double] / scheduledThreadPool.getCorePoolSize.asInstanceOf[Double]
  }

  def scheduleWithFixedDelay(runnable: Runnable, initialDelay: Long, delay: Long,
                             timeUnit: TimeUnit): ScheduledFuture[_] = {
    info(s"Scheduling runnable $runnable with initial delay: $initialDelay, fixed delay: $delay")
    scheduledThreadPool.scheduleWithFixedDelay(runnable, initialDelay, delay, timeUnit)
  }

  def shutdown(): Boolean = {
    info("Shutting down scheduled thread pool")
    scheduledThreadPool.shutdownNow()
    //waits for 2 mins to terminate the current tasks
    scheduledThreadPool.awaitTermination(2, TimeUnit.MINUTES)
  }
}

trait CancellableRunnable extends Runnable {
  @volatile private var cancelled = false

  def cancel(): Unit = {
    cancelled = true
  }

  def isCancelled(): Boolean = {
    cancelled
  }
}

/**
 * This class is responsible for
 *  - initializing `RemoteStorageManager` and `RemoteLogMetadataManager` instances
 *  - receives any leader and follower replica events and partition stop events and act on them
 *  - also provides APIs to fetch indexes, metadata about remote log segments
 *  - copying log segments to remote storage
 *
 * @param rlmConfig Configuration required for remote logging subsystem(tiered storage) at the broker level.
 * @param brokerId  id of the current broker.
 * @param logDir    directory of Kafka log segments.
 */
class RemoteLogManager(rlmConfig: RemoteLogManagerConfig,
                       brokerId: Int,
                       logDir: String,
                       time: Time = Time.SYSTEM,
                       fetchLog: TopicPartition => Option[UnifiedLog])
  extends Logging with Closeable {

  // topic ids that are received on leadership changes, this map is cleared on stop partitions
  private val topicPartitionIds: ConcurrentMap[TopicPartition, Uuid] = new ConcurrentHashMap[TopicPartition, Uuid]()

  private val remoteLogStorageManager: RemoteStorageManager = createRemoteStorageManager()
  private val remoteLogMetadataManager: RemoteLogMetadataManager = createRemoteLogMetadataManager()

  private val indexCache = new RemoteIndexCache(remoteStorageManager = remoteLogStorageManager, logDir = logDir)

  private var closed = false

  private val leaderOrFollowerTasks: ConcurrentHashMap[TopicIdPartition, RLMTaskWithFuture] =
    new ConcurrentHashMap[TopicIdPartition, RLMTaskWithFuture]()

  private val rlmScheduledThreadPool = new RLMScheduledThreadPool(rlmConfig.remoteLogManagerThreadPoolSize)
  private val delayInMs = rlmConfig.remoteLogManagerTaskIntervalMs

  private[remote] def createRemoteStorageManager(): RemoteStorageManager = {
    def createDelegate(classLoader: ClassLoader): RemoteStorageManager = {
      classLoader.loadClass(rlmConfig.remoteStorageManagerClassName())
        .getDeclaredConstructor().newInstance().asInstanceOf[RemoteStorageManager]
    }

    AccessController.doPrivileged(new PrivilegedAction[RemoteStorageManager] {
      private val classPath = rlmConfig.remoteStorageManagerClassPath()

      override def run(): RemoteStorageManager = {
        if (classPath != null && classPath.trim.nonEmpty) {
          val classLoader = new ChildFirstClassLoader(classPath, this.getClass.getClassLoader)
          val delegate = createDelegate(classLoader)
          new ClassLoaderAwareRemoteStorageManager(delegate, classLoader)
        } else {
          createDelegate(this.getClass.getClassLoader)
        }
      }
    })
  }

  private def configureRSM(): Unit = {
    val rsmProps = new util.HashMap[String, Any]()
    rlmConfig.remoteStorageManagerProps().asScala.foreach { case (k, v) => rsmProps.put(k, v) }
    rsmProps.put(KafkaConfig.BrokerIdProp, brokerId)
    remoteLogStorageManager.configure(rsmProps)
  }

  private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = {
    def createDelegate(classLoader: ClassLoader) = {
      classLoader.loadClass(rlmConfig.remoteLogMetadataManagerClassName())
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[RemoteLogMetadataManager]
    }

    AccessController.doPrivileged(new PrivilegedAction[RemoteLogMetadataManager] {
      private val classPath = rlmConfig.remoteLogMetadataManagerClassPath

      override def run(): RemoteLogMetadataManager = {
        if (classPath != null && classPath.trim.nonEmpty) {
          val classLoader = new ChildFirstClassLoader(classPath, this.getClass.getClassLoader)
          val delegate = createDelegate(classLoader)
          new ClassLoaderAwareRemoteLogMetadataManager(delegate, classLoader)
        } else {
          createDelegate(this.getClass.getClassLoader)
        }
      }
    })
  }

  private def configureRLMM(): Unit = {
    val rlmmProps = new util.HashMap[String, Any]()
    rlmConfig.remoteLogMetadataManagerProps().asScala.foreach { case (k, v) => rlmmProps.put(k, v) }
    rlmmProps.put(KafkaConfig.BrokerIdProp, brokerId)
    rlmmProps.put(KafkaConfig.LogDirProp, logDir)
    remoteLogMetadataManager.configure(rlmmProps)
  }

  def startup(): Unit = {
    // Initialize and configure RSM and RLMM. This will start RSM, RLMM resources which may need to start resources
    // in connecting to the brokers or remote storages.
    configureRSM()
    configureRLMM()
  }

  def storageManager(): RemoteStorageManager = {
    remoteLogStorageManager
  }

  /**
   * Deletes the internal topic partition info if delete flag is set as true.
   *
   * @param topicPartition topic partition to be stopped.
   * @param delete         flag to indicate whether the given topic partitions to be deleted or not.
   */
  def stopPartitions(topicPartition: TopicPartition, delete: Boolean): Unit = {
    if (delete) {
      // Delete from internal datastructures only if it is to be deleted.
      val topicIdPartition = topicPartitionIds.remove(topicPartition)
      debug(s"Removed partition: $topicIdPartition from topicPartitionIds")
    }
  }

  def fetchRemoteLogSegmentMetadata(topicPartition: TopicPartition,
                                    epochForOffset: Int,
                                    offset: Long): Optional[RemoteLogSegmentMetadata] = {
    val topicId = topicPartitionIds.get(topicPartition)

    if (topicId == null) {
      throw new KafkaException("No topic id registered for topic partition: " + topicPartition)
    }
    remoteLogMetadataManager.remoteLogSegmentMetadata(new TopicIdPartition(topicId, topicPartition), epochForOffset, offset)
  }

  private def lookupTimestamp(rlsMetadata: RemoteLogSegmentMetadata, timestamp: Long, startingOffset: Long): Option[TimestampAndOffset] = {
    val startPos = indexCache.lookupTimestamp(rlsMetadata, timestamp, startingOffset)

    var remoteSegInputStream: InputStream = null
    try {
      // Search forward for the position of the last offset that is greater than or equal to the startingOffset
      remoteSegInputStream = remoteLogStorageManager.fetchLogSegment(rlsMetadata, startPos)
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
   * - If there are no messages in the remote storage, return None
   * - If all the messages in the remote storage have smaller offsets, return None
   * - If all the messages in the remote storage have smaller timestamps, return None
   * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
   * is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
   *
   * @param tp               topic partition in which the offset to be found.
   * @param timestamp        The timestamp to search for.
   * @param startingOffset   The starting offset to search.
   * @param leaderEpochCache LeaderEpochFileCache of the topic partition.
   * @return the timestamp and offset of the first message that meets the requirements. None will be returned if there
   *         is no such message.
   */
  def findOffsetByTimestamp(tp: TopicPartition,
                            timestamp: Long,
                            startingOffset: Long,
                            leaderEpochCache: LeaderEpochFileCache): Option[TimestampAndOffset] = {
    val topicId = topicPartitionIds.get(tp)
    if (topicId == null) {
      throw new KafkaException("Topic id does not exist for topic partition: " + tp)
    }

    // Get the respective epoch in which the starting-offset exists.
    var maybeEpoch = leaderEpochCache.epochForOffset(startingOffset)
    while (maybeEpoch.isPresent) {
      val epoch = maybeEpoch.getAsInt
      remoteLogMetadataManager.listRemoteLogSegments(new TopicIdPartition(topicId, tp), epoch).asScala
        .foreach(rlsMetadata =>
          if (rlsMetadata.maxTimestampMs() >= timestamp && rlsMetadata.endOffset() >= startingOffset) {
            val timestampOffset = lookupTimestamp(rlsMetadata, timestamp, startingOffset)
            if (timestampOffset.isDefined)
              return timestampOffset
          }
        )

      // Move to the next epoch if not found with the current epoch.
      maybeEpoch = leaderEpochCache.nextEpoch(epoch)
    }
    None
  }

  trait CancellableRunnable extends Runnable {
    @volatile private var cancelled = false

    def cancel(): Unit = {
      cancelled = true
    }

    def isCancelled(): Boolean = {
      cancelled
    }
  }

  /**
   * Returns the leader epoch checkpoint by truncating with the given start[exclusive] and end[inclusive] offset
   *
   * @param log         The actual log from where to take the leader-epoch checkpoint
   * @param startOffset The start offset of the checkpoint file (exclusive in the truncation).
   *                    If start offset is 6, then it will retain an entry at offset 6.
   * @param endOffset   The end offset of the checkpoint file (inclusive in the truncation)
   *                    If end offset is 100, then it will remove the entries greater than or equal to 100.
   * @return the truncated leader epoch checkpoint
   */
  private[remote] def getLeaderEpochCheckpoint(log: UnifiedLog, startOffset: Long, endOffset: Long): InMemoryLeaderEpochCheckpoint = {
    val checkpoint = new InMemoryLeaderEpochCheckpoint()
    log.leaderEpochCache
      .map(cache => cache.writeTo(checkpoint))
      .foreach { x =>
        if (startOffset >= 0) {
          x.truncateFromStart(startOffset)
        }
        x.truncateFromEnd(endOffset)
      }
    checkpoint
  }

  private[remote] class RLMTask(tpId: TopicIdPartition) extends CancellableRunnable with Logging {
    this.logIdent = s"[RemoteLogManager=$brokerId partition=$tpId] "
    @volatile private var leaderEpoch: Int = -1

    private def isLeader(): Boolean = leaderEpoch >= 0

    // The readOffset is None initially for a new leader RLMTask, and needs to be fetched inside the task's run() method.
    @volatile private var copiedOffsetOption: Option[Long] = None

    def convertToLeader(leaderEpochVal: Int): Unit = {
      if (leaderEpochVal < 0) {
        throw new KafkaException(s"leaderEpoch value for topic partition $tpId can not be negative")
      }
      if (this.leaderEpoch != leaderEpochVal) {
        leaderEpoch = leaderEpochVal
      }
      // Reset readOffset, so that it is set in next run of RLMTask
      copiedOffsetOption = None
    }

    def convertToFollower(): Unit = {
      leaderEpoch = -1
    }

    def handleCopyLogSegmentsToRemote(): Unit = {
      if (isCancelled())
        return

      def maybeUpdateReadOffset(): Unit = {
        if (copiedOffsetOption.isEmpty) {
          info(s"Find the highest remote offset for partition: $tpId after becoming leader, leaderEpoch: $leaderEpoch")

          // This is found by traversing from the latest leader epoch from leader epoch history and find the highest offset
          // of a segment with that epoch copied into remote storage. If it can not find an entry then it checks for the
          // previous leader epoch till it finds an entry, If there are no entries till the earliest leader epoch in leader
          // epoch cache then it starts copying the segments from the earliest epoch entry’s offset.
          copiedOffsetOption = Some(findHighestRemoteOffset(tpId))
        }
      }

      try {
        maybeUpdateReadOffset()
        val copiedOffset = copiedOffsetOption.get
        fetchLog(tpId.topicPartition()).foreach { log =>
          // LSO indicates the offset below are ready to be consumed(high-watermark or committed)
          val lso = log.lastStableOffset
          if (lso < 0) {
            warn(s"lastStableOffset for partition $tpId is $lso, which should not be negative.")
          } else if (lso > 0 && copiedOffset < lso) {
            // Copy segments only till the min of high-watermark or stable-offset as remote storage should contain
            // only committed/acked messages
            val toOffset = lso
            debug(s"Checking for segments to copy, copiedOffset: $copiedOffset and toOffset: $toOffset")
            val activeSegBaseOffset = log.activeSegment.baseOffset
            // log-start-offset can be ahead of the read-offset, when:
            // 1) log-start-offset gets incremented via delete-records API (or)
            // 2) enabling the remote log for the first time
            val fromOffset = Math.max(copiedOffset + 1, log.logStartOffset)
            val sortedSegments = log.logSegments(fromOffset, toOffset).toSeq.sortBy(_.baseOffset)
            val activeSegIndex: Int = sortedSegments.map(x => x.baseOffset).search(activeSegBaseOffset) match {
              case Found(x) => x
              case InsertionPoint(y) => y - 1
            }
            // sortedSegments becomes empty list when fromOffset and toOffset are same, and activeSegIndex becomes -1
            if (activeSegIndex < 0) {
              debug(s"No segments found to be copied for partition $tpId with copiedOffset: $copiedOffset and " +
                s"active segment's base-offset: $activeSegBaseOffset")
            } else {
              sortedSegments.slice(0, activeSegIndex).foreach { segment =>
                if (isCancelled() || !isLeader()) {
                  info(s"Skipping copying log segments as the current task state is changed, cancelled: " +
                    s"${isCancelled()} leader:${isLeader()}")
                  return
                }

                val logFile = segment.log.file()
                val logFileName = logFile.getName

                info(s"Copying $logFileName to remote storage.")
                val id = new RemoteLogSegmentId(tpId, Uuid.randomUuid())

                val nextOffset = segment.readNextOffset
                val endOffset = nextOffset - 1
                val producerIdSnapshotFile: File = log.producerStateManager.fetchSnapshot(nextOffset).orElse(null)

                val epochEntries = getLeaderEpochCheckpoint(log, segment.baseOffset, nextOffset).read()
                val segmentLeaderEpochs: util.Map[Integer, lang.Long] = new util.HashMap[Integer, lang.Long](epochEntries.size())
                epochEntries.forEach(entry =>
                  segmentLeaderEpochs.put(Integer.valueOf(entry.epoch), lang.Long.valueOf(entry.startOffset))
                )

                val remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(id, segment.baseOffset, endOffset,
                  segment.largestTimestamp, brokerId, time.milliseconds(), segment.log.sizeInBytes(),
                  segmentLeaderEpochs)

                remoteLogMetadataManager.addRemoteLogSegmentMetadata(remoteLogSegmentMetadata).get()

                val leaderEpochsIndex = getLeaderEpochCheckpoint(log, startOffset = -1, nextOffset).readAsByteBuffer()
                val segmentData = new LogSegmentData(logFile.toPath, toPathIfExists(segment.lazyOffsetIndex.get.file),
                  toPathIfExists(segment.lazyTimeIndex.get.file), Optional.ofNullable(toPathIfExists(segment.txnIndex.file)),
                  producerIdSnapshotFile.toPath, leaderEpochsIndex)
                remoteLogStorageManager.copyLogSegmentData(remoteLogSegmentMetadata, segmentData)

                val rlsmAfterSegmentCopy = new RemoteLogSegmentMetadataUpdate(id, time.milliseconds(),
                  RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId)

                remoteLogMetadataManager.updateRemoteLogSegmentMetadata(rlsmAfterSegmentCopy).get()

                copiedOffsetOption = Some(endOffset)
                log.updateHighestOffsetInRemoteStorage(endOffset)
                info(s"Copied $logFileName to remote storage with segment-id: ${rlsmAfterSegmentCopy.remoteLogSegmentId()}")
              }
            }
          } else {
            debug(s"Skipping copying segments, current read-offset:$copiedOffset, and LSO:$lso")
          }
        }
      } catch {
        case ex: Exception =>
          if (!isCancelled()) {
            error(s"Error occurred while copying log segments of partition: $tpId", ex)
          }
      }
    }

    private def toPathIfExists(file: File): Path = {
      if (file.exists()) file.toPath else null
    }

    override def run(): Unit = {
      if (isCancelled())
        return

      try {
        if (isLeader()) {
          // Copy log segments to remote storage
          handleCopyLogSegmentsToRemote()
        }
      } catch {
        case ex: InterruptedException =>
          if (!isCancelled()) {
            warn(s"Current thread for topic-partition-id $tpId is interrupted, this task won't be rescheduled. " +
              s"Reason: ${ex.getMessage}")
          }
        case ex: Exception =>
          if (!isCancelled()) {
            warn(s"Current task for topic-partition $tpId received error but it will be scheduled. " +
              s"Reason: ${ex.getMessage}")
          }
      }
    }

    override def toString: String = {
      this.getClass.toString + s"[$tpId]"
    }
  }

  def findHighestRemoteOffset(topicIdPartition: TopicIdPartition): Long = {
    var offset: Optional[lang.Long] = Optional.empty()
    fetchLog(topicIdPartition.topicPartition()).foreach { log =>
      log.leaderEpochCache.foreach(cache => {
        var epoch = cache.latestEpoch
        while (!offset.isPresent && epoch.isPresent) {
          offset = remoteLogMetadataManager.highestOffsetForEpoch(topicIdPartition, epoch.getAsInt)
          epoch = cache.findPreviousEpoch(epoch.getAsInt)
        }
      })
    }
    offset.orElse(-1L)
  }


  private def doHandleLeaderOrFollowerPartitions(topicPartition: TopicIdPartition,
                                                 convertToLeaderOrFollower: RLMTask => Unit): Unit = {
    val rlmTaskWithFuture = leaderOrFollowerTasks.computeIfAbsent(topicPartition, (tp: TopicIdPartition) => {
      val task = new RLMTask(tp)
      // set this upfront when it is getting initialized instead of doing it after scheduling.
      convertToLeaderOrFollower(task)
      info(s"Created a new task: $task and getting scheduled")
      val future = rlmScheduledThreadPool.scheduleWithFixedDelay(task, 0, delayInMs, TimeUnit.MILLISECONDS)
      RLMTaskWithFuture(task, future)
    })
    convertToLeaderOrFollower(rlmTaskWithFuture.rlmTask)
  }

  /**
   * Callback to receive any leadership changes for the topic partitions assigned to this broker. If there are no
   * existing tasks for a given topic partition then it will assign new leader or follower task else it will convert the
   * task to respective target state(leader or follower).
   *
   * @param partitionsBecomeLeader   partitions that have become leaders on this broker.
   * @param partitionsBecomeFollower partitions that have become followers on this broker.
   * @param topicIds                 topic name to topic id mappings.
   */
  def onLeadershipChange(partitionsBecomeLeader: Set[Partition],
                         partitionsBecomeFollower: Set[Partition],
                         topicIds: util.Map[String, Uuid]): Unit = {
    trace(s"Received leadership changes for partitionsBecomeLeader: $partitionsBecomeLeader " +
      s"and partitionsBecomeLeader: $partitionsBecomeLeader")

    // Partitions logs are available when this callback is invoked.
    // Compact topics and internal topics are filtered here as they are not supported with tiered storage.
    def nonSupported(partition: Partition): Boolean = {
      Topic.isInternal(partition.topic) ||
        partition.log.exists(log => log.config.compact || !log.config.remoteLogConfig.remoteStorageEnable) ||
        partition.topicPartition.topic().equals(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME)
    }

    val leaderPartitions = partitionsBecomeLeader.filterNot(nonSupported)
      .map(p => new TopicIdPartition(topicIds.get(p.topic), p.topicPartition) -> p).toMap
    val followerPartitions = partitionsBecomeFollower.filterNot(nonSupported)
      .map(p => new TopicIdPartition(topicIds.get(p.topic), p.topicPartition))

    def cacheTopicPartitionIds(topicIdPartition: TopicIdPartition): Unit = {
      val previousTopicId = topicPartitionIds.put(topicIdPartition.topicPartition(), topicIdPartition.topicId())
      if (previousTopicId != null && previousTopicId != topicIdPartition.topicId()) {
        warn(s"Previous cached topic id $previousTopicId for ${topicIdPartition.topicPartition()} does " +
          s"not match update topic id ${topicIdPartition.topicId()}")
      }
    }

    if (leaderPartitions.nonEmpty || followerPartitions.nonEmpty) {
      debug(s"Effective topic partitions after filtering compact and internal topics, " +
        s"leaders: ${leaderPartitions.keySet} and followers: $followerPartitions")

      leaderPartitions.keySet.foreach(cacheTopicPartitionIds)
      followerPartitions.foreach(cacheTopicPartitionIds)

      remoteLogMetadataManager.onPartitionLeadershipChanges(leaderPartitions.keySet.asJava, followerPartitions.asJava)
      followerPartitions.foreach {
        topicIdPartition => {
          doHandleLeaderOrFollowerPartitions(topicIdPartition, _.convertToFollower())
        }
      }
      leaderPartitions.foreach {
        case (topicIdPartition, partition) =>
          doHandleLeaderOrFollowerPartitions(topicIdPartition, _.convertToLeader(partition.getLeaderEpoch))
      }
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

  class InMemoryLeaderEpochCheckpoint extends LeaderEpochCheckpoint {
    private var epochs: util.List[EpochEntry] = util.Collections.emptyList()

    override def write(epochs: util.Collection[EpochEntry]): Unit = this.epochs = new util.ArrayList[EpochEntry](epochs)

    override def read(): util.List[EpochEntry] = util.Collections.unmodifiableList(epochs)

    def readAsByteBuffer(): ByteBuffer = {
      val stream = new ByteArrayOutputStream()
      val writer = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8))
      val writeBuffer = new CheckpointWriteBuffer[EpochEntry](writer, 0, LeaderEpochCheckpointFile.FORMATTER)
      try {
        writeBuffer.write(epochs)
        writer.flush()
        ByteBuffer.wrap(stream.toByteArray)
      } finally {
        writer.close()
      }
    }
  }

  /**
   * Closes and releases all the resources like RemoterStorageManager and RemoteLogMetadataManager.
   */
  def close(): Unit = {
    this synchronized {
      if (!closed) {
        leaderOrFollowerTasks.values().forEach(_.cancel())
        Utils.closeQuietly(remoteLogStorageManager, "RemoteLogStorageManager")
        Utils.closeQuietly(remoteLogMetadataManager, "RemoteLogMetadataManager")
        Utils.closeQuietly(indexCache, "RemoteIndexCache")
        rlmScheduledThreadPool.shutdown()
        leaderOrFollowerTasks.clear()
        closed = true
      }
    }
  }

}