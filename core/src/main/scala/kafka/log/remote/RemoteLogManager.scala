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
import kafka.log.{AbortedTxn, Log, OffsetPosition}
import kafka.metrics.KafkaMetricsGroup
import kafka.server._
import kafka.server.checkpoints.{CheckpointWriteBuffer, LeaderEpochCheckpoint, LeaderEpochCheckpointFile}
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import kafka.utils.Implicits._
import kafka.utils.Logging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common._
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.FetchResponseData.AbortedTransaction
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch, RemoteLogInputStream}
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.{ChildFirstClassLoader, KafkaThread, Time, Utils}
import org.apache.kafka.server.log.remote.metadata.storage.{ClassLoaderAwareRemoteLogMetadataManager, TopicBasedRemoteLogMetadataManagerConfig}
import org.apache.kafka.server.log.remote.storage._

import java.io.{BufferedWriter, ByteArrayOutputStream, Closeable, InputStream, OutputStreamWriter}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.{AccessController, PrivilegedAction}
import java.util.Optional
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import java.{lang, util}
import scala.collection.Searching._
import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, Set, mutable}
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

  def poolSize(): Int = scheduledThreadPool.getMaximumPoolSize

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

class RemoteLogManager(fetchLog: TopicPartition => Option[Log],
                       updateRemoteLogStartOffset: (TopicPartition, Long) => Unit,
                       rlmConfig: RemoteLogManagerConfig,
                       time: Time = Time.SYSTEM,
                       brokerId: Int,
                       clusterId: String = "",
                       logDir: String,
                       brokerTopicStats: BrokerTopicStats) extends Logging with Closeable with KafkaMetricsGroup  {
  private val leaderOrFollowerTasks: ConcurrentHashMap[TopicIdPartition, RLMTaskWithFuture] =
    new ConcurrentHashMap[TopicIdPartition, RLMTaskWithFuture]()
  private val remoteStorageFetcherThreadPool = new RemoteStorageReaderThreadPool(rlmConfig.remoteLogReaderThreads,
    rlmConfig.remoteLogReaderMaxPendingTasks, time)

  private val delayInMs = rlmConfig.remoteLogManagerTaskIntervalMs
  private val poolSize = rlmConfig.remoteLogManagerThreadPoolSize
  private val rlmScheduledThreadPool = new RLMScheduledThreadPool(poolSize)

  // topic ids that are received on leadership changes, this map is NEVER cleared
  private val topicIds: mutable.Map[String, Uuid] = mutable.Map.empty

  @volatile private var closed = false

  newGauge("RemoteLogManagerTasksAvgIdlePercent", () => {
    rlmScheduledThreadPool.getIdlePercent()
  })

  private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = {
    AccessController.doPrivileged(new PrivilegedAction[ClassLoaderAwareRemoteStorageManager] {
      private val classPath = rlmConfig.remoteStorageManagerClassPath()

      override def run(): ClassLoaderAwareRemoteStorageManager = {
        val classLoader =
          if (classPath != null && classPath.trim.nonEmpty) {
            new ChildFirstClassLoader(classPath, this.getClass.getClassLoader)
          } else {
            this.getClass.getClassLoader
          }
        val delegate = classLoader.loadClass(rlmConfig.remoteStorageManagerClassName())
          .getDeclaredConstructor().newInstance().asInstanceOf[RemoteStorageManager]
        new ClassLoaderAwareRemoteStorageManager(delegate, classLoader)
      }
    })
  }

  private def configureRSM(): Unit = {
    val rsmProps = new util.HashMap[String, Any]()
    rlmConfig.remoteStorageManagerProps().asScala.foreach { case (k, v) => rsmProps.put(k, v) }
    rsmProps.put(KafkaConfig.BrokerIdProp, brokerId)
    rsmProps.put("cluster.id", clusterId)
    remoteLogStorageManager.configure(rsmProps)
  }

  private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = {
    AccessController.doPrivileged(new PrivilegedAction[RemoteLogMetadataManager] {
      private val classPath = rlmConfig.remoteLogMetadataManagerClassPath

      override def run(): RemoteLogMetadataManager = {
        var classLoader = this.getClass.getClassLoader
        if (classPath != null && classPath.trim.nonEmpty) {
          classLoader = new ChildFirstClassLoader(classPath, classLoader)
          val delegate = classLoader.loadClass(rlmConfig.remoteLogMetadataManagerClassName())
            .getDeclaredConstructor()
            .newInstance()
            .asInstanceOf[RemoteLogMetadataManager]
          new ClassLoaderAwareRemoteLogMetadataManager(delegate, classLoader)
        } else {
          classLoader.loadClass(rlmConfig.remoteLogMetadataManagerClassName())
            .getDeclaredConstructor()
            .newInstance()
            .asInstanceOf[RemoteLogMetadataManager]
        }
      }
    })
  }

  private def configureRLMM(endPoint: Endpoint): Unit = {
    val rlmmProps = new util.HashMap[String, Any]()
    rlmConfig.remoteLogMetadataManagerProps().asScala.foreach { case (k, v) => rlmmProps.put(k, v) }
    rlmmProps.put(KafkaConfig.LogDirProp, logDir)
    rlmmProps.put(KafkaConfig.BrokerIdProp, brokerId)
    rlmmProps.put("cluster.id", clusterId)
    rlmmProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, endPoint.host + ":" + endPoint.port)
    rlmmProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, endPoint.securityProtocol.name)
    remoteLogMetadataManager.configure(rlmmProps)
  }

  private val remoteLogStorageManager: ClassLoaderAwareRemoteStorageManager = createRemoteStorageManager()
  val remoteLogMetadataManager: RemoteLogMetadataManager = createRemoteLogMetadataManager()

  private val indexCache = new RemoteIndexCache(remoteStorageManager = remoteLogStorageManager, logDir = logDir)

  private[remote] def rlmScheduledThreadPoolSize: Int = rlmScheduledThreadPool.poolSize()

  private[remote] def leaderOrFollowerTasksSize: Int = leaderOrFollowerTasks.size()

  private def doHandleLeaderOrFollowerPartitions(topicPartition: TopicIdPartition,
                                                 convertToLeaderOrFollower: RLMTask => Unit): Unit = {
    var conversionRequired = true
    val rlmTaskWithFuture = leaderOrFollowerTasks.computeIfAbsent(topicPartition, (tp: TopicIdPartition) => {
      val task = new RLMTask(tp)
      // set this upfront when it is getting initialized instead of doing it after scheduling.
      convertToLeaderOrFollower(task)
      conversionRequired = false
      info(s"Created a new task: $task and getting scheduled")
      val future = rlmScheduledThreadPool.scheduleWithFixedDelay(task, 0, delayInMs, TimeUnit.MILLISECONDS)
      RLMTaskWithFuture(task, future)
    })
    if (conversionRequired) {
      convertToLeaderOrFollower(rlmTaskWithFuture.rlmTask)
    }
  }

  def onEndpointCreated(serverEndPoint: Endpoint): Unit = {
    // initialize and configure RSM and RLMM
    configureRSM()
    configureRLMM(serverEndPoint)
  }

  def storageManager(): RemoteStorageManager = {
    remoteLogStorageManager
  }

  /**
   * Callback to receive any leadership changes for the topic partitions assigned to this broker. If there are no
   * existing tasks for a given topic partition then it will assign new leader or follower task else it will convert the
   * task to respective target state(leader or follower).
   *
   * @param partitionsBecomeLeader   partitions that have become leaders on this broker.
   * @param partitionsBecomeFollower partitions that have become followers on this broker.
   */
  def onLeadershipChange(partitionsBecomeLeader: Set[Partition],
                         partitionsBecomeFollower: Set[Partition],
                         topicIds: util.Map[String, Uuid]): Unit = {
    trace(s"Received leadership changes for partitionsBecomeLeader: $partitionsBecomeLeader " +
      s"and partitionsBecomeLeader: $partitionsBecomeLeader")
    // Compact topics and internal topics are filtered here as they are not supported with tiered storage.

    def nonSupported(partition: Partition): Boolean = {
      Topic.isInternal(partition.topic) ||
        partition.topic.equals(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME) ||
        partition.log.exists(log => !log.remoteLogEnabled())
    }

    val leaderPartitions = partitionsBecomeLeader.filterNot(nonSupported)
      .map(p => new TopicIdPartition(topicIds.get(p.topic), p.topicPartition) -> p).toMap
    val followerPartitions = partitionsBecomeFollower.filterNot(nonSupported)
      .map(p => new TopicIdPartition(topicIds.get(p.topic), p.topicPartition))
    if (leaderPartitions.nonEmpty || followerPartitions.nonEmpty) {
      debug(s"Effective topic partitions after filtering compact and internal topics, " +
        s"leaders: ${leaderPartitions.keySet} and followers: $followerPartitions")
      topicIds.forEach((topic, uuid) => this.topicIds.put(topic, uuid))
      remoteLogMetadataManager.onPartitionLeadershipChanges(leaderPartitions.keySet.asJava, followerPartitions.asJava)
      followerPartitions.foreach {
        topicIdPartition => doHandleLeaderOrFollowerPartitions(topicIdPartition, _.convertToFollower())
      }
      leaderPartitions.foreach {
        case (topicIdPartition, partition) =>
          doHandleLeaderOrFollowerPartitions(topicIdPartition, _.convertToLeader(partition.getLeaderEpoch))
      }
    }
  }

  /**
   * Stops partitions for copying segments, building indexes and deletes the partition in remote storage if delete flag
   * is set as true.
   *
   * @param allPartitions  topic partitions that needs to be stopped.
   * @param delete      flag to indicate whether the given topic partitions to be deleted or not.
   */
  def stopPartitions(allPartitions: Set[TopicPartition], delete: Boolean, errorHandler: (TopicPartition, Throwable) => Unit): Unit = {
    debug(s"Stopping ${allPartitions.size} partitions, delete: $delete")
    val partitionsByTopic = allPartitions.groupBy(_.topic())
    partitionsByTopic.forKeyValue((topic, partitions) => {
      // FIXME: When to remove the topicId from topicIds map? (leaving them can lead to memory leak)
      val topicId = topicIds.get(topic)
      if (topicId.isDefined) {
        val tpIds = partitions.map(new TopicIdPartition(topicId.get, _))
        tpIds.foreach(tpId => {
          val partition = tpId.topicPartition()
          try {
            val task = leaderOrFollowerTasks.remove(tpId)
            if (task != null) {
              info(s"Cancelling the RLM task for tp: $partition")
              task.cancel()
            }
            if (delete) {
              debug(s"Deleting the remote log segments for partition: $tpId")
              remoteLogMetadataManager.listRemoteLogSegments(tpId).forEachRemaining(elt => deleteRemoteLogSegment(elt, _ => true))
            }
          } catch {
            case ex: Throwable => errorHandler(partition, ex)
          }
        })
        if (delete) {
          // NOTE: this#stopPartitions method is called when Replica state changes to Offline and ReplicaDeletionStarted
          remoteLogMetadataManager.onStopPartitions(tpIds.asJava)
        }
      }
    })
  }

  private def deleteRemoteLogSegment(segmentMetadata: RemoteLogSegmentMetadata, predicate: RemoteLogSegmentMetadata => Boolean): Boolean = {
    if (predicate(segmentMetadata)) {
      // Publish delete segment started event.
      remoteLogMetadataManager.updateRemoteLogSegmentMetadata(
        new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(), time.milliseconds(),
          RemoteLogSegmentState.DELETE_SEGMENT_STARTED, brokerId))

      // Delete the segment in remote storage.
      remoteLogStorageManager.deleteLogSegmentData(segmentMetadata)

      // Publish delete segment finished event.
      remoteLogMetadataManager.updateRemoteLogSegmentMetadata(
        new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(), time.milliseconds(),
          RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, brokerId))
      true
    } else false
  }

  /**
   * Returns the leader epoch checkpoint by truncating with the given start[exclusive] and end[inclusive] offset
   * @param log         The actual log from where to take the leader-epoch checkpoint
   * @param startOffset The start offset of the checkpoint file (exclusive in the truncation).
   *                    If start offset is 6, then it will retain an entry at offset 6.
   * @param endOffset   The end offset of the checkpoint file (inclusive in the truncation)
   *                    If end offset is 100, then it will remove the entries greater than or equal to 100.
   * @return the truncated leader epoch checkpoint
   */
  private[remote] def getLeaderEpochCheckpoint(log: Log, startOffset: Long, endOffset: Long): InMemoryLeaderEpochCheckpoint = {
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

  class RLMTask(tpId: TopicIdPartition) extends CancellableRunnable with Logging {
    this.logIdent = s"[RemoteLogManager=$brokerId partition=$tpId] "
    @volatile private var leaderEpoch: Int = -1

    private def isLeader(): Boolean = leaderEpoch >= 0

    // The readOffset is None initially for a new leader RLMTask,
    // and needs to be fetched inside the task's run() method.
    private var readOffsetOption: Option[Long] = None

    //todo-updating log with remote index highest offset -- should this be required?
    // fetchLog(tp.topicPartition()).foreach { log => log.updateRemoteIndexHighestOffset(readOffset) }

    def convertToLeader(leaderEpochVal: Int): Unit = {
      if (leaderEpochVal < 0) {
        throw new KafkaException(s"leaderEpoch value for topic partition $tpId can not be negative")
      }
      if (this.leaderEpoch != leaderEpochVal) {
        leaderEpoch = leaderEpochVal
      }
      // Reset readOffset, so that it is set in next run of RLMTask
      readOffsetOption = None
    }

    def convertToFollower(): Unit = {
      leaderEpoch = -1
    }

    def copyLogSegmentsToRemote(): Unit = {
      if (isCancelled())
        return

      def maybeUpdateReadOffset(): Unit = {
        if (readOffsetOption.isEmpty) {
          info(s"Find the highest remote offset for partition: $tpId after becoming leader, leaderEpoch: $leaderEpoch")

          // This is found by traversing from the latest leader epoch from leader epoch history and find the highest offset
          // of a segment with that epoch copied into remote storage. If it can not find an entry then it checks for the
          // previous leader epoch till it finds an entry, If there are no entries till the earliest leader epoch in leader
          // epoch cache then it starts copying the segments from the earliest epoch entry’s offset.
          readOffsetOption = Some(findHighestRemoteOffset(tpId))
        }
      }

      try {
        maybeUpdateReadOffset()
        val readOffset = readOffsetOption.get
        fetchLog(tpId.topicPartition()).foreach { log =>
          // LSO indicates the offset below are ready to be consumed(high-watermark or committed)
          val lso = log.lastStableOffset
          if (lso < 0) {
            warn(s"lastStableOffset for partition $tpId is $lso, which should not be negative.")
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
              debug(s"No segments found to be copied for partition $tpId with read offset: $readOffset and active " +
                s"baseoffset: $activeSegBaseOffset")
            } else {
              sortedSegments.slice(0, index).foreach { segment =>
                // store locally here as this may get updated after the below if condition is computed as false.
                if (isCancelled() || !isLeader()) {
                  info(s"Skipping copying log segments as the current task state is changed, cancelled: " +
                    s"${isCancelled()} leader:${isLeader()}")
                  return
                }

                val logFile = segment.log.file()
                val fileName = logFile.getName
                info(s"Copying $fileName to remote storage.")
                val id = new RemoteLogSegmentId(tpId, Uuid.randomUuid())

                val nextOffset = segment.readNextOffset
                //todo-tier double check on this
                val endOffset = nextOffset - 1
                val producerIdSnapshotFile = log.producerStateManager.fetchSnapshot(nextOffset).orNull

                val segmentLeaderEpochs = getLeaderEpochCheckpoint(log, segment.baseOffset, nextOffset).read().map {
                  case EpochEntry(epoch, startOffset) => Integer.valueOf(epoch) -> lang.Long.valueOf(startOffset)
                }.toMap.asJava

                val remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(id, segment.baseOffset, endOffset,
                  segment.largestTimestamp, brokerId, time.milliseconds(), segment.log.sizeInBytes(),
                  segmentLeaderEpochs)

                remoteLogMetadataManager.addRemoteLogSegmentMetadata(remoteLogSegmentMetadata)

                val leaderEpochsIndex = getLeaderEpochCheckpoint(log, startOffset = -1, nextOffset).readAsByteBuffer()
                val segmentData = new LogSegmentData(logFile.toPath, segment.lazyOffsetIndex.get.path,
                  segment.lazyTimeIndex.get.path, Optional.ofNullable(segment.txnIndex.path),
                  producerIdSnapshotFile.toPath, leaderEpochsIndex)
                remoteLogStorageManager.copyLogSegmentData(remoteLogSegmentMetadata, segmentData)

                val rlsmAfterCreate = new RemoteLogSegmentMetadataUpdate(id, time.milliseconds(),
                  RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId)

                remoteLogMetadataManager.updateRemoteLogSegmentMetadata(rlsmAfterCreate)
                brokerTopicStats.topicStats(tpId.topicPartition().topic())
                  .remoteBytesOutRate.mark(remoteLogSegmentMetadata.segmentSizeInBytes())
                readOffsetOption = Some(endOffset)
                //todo-tier-storage
                log.updateRemoteIndexHighestOffset(endOffset)
                info(s"Copied $fileName to remote storage with segment-id: ${rlsmAfterCreate.remoteLogSegmentId()}")
              }
            }
          } else {
            debug(s"Skipping copying segments, current read offset:$readOffset is and LSO:$lso ")
          }
        }
      } catch {
        case ex: Exception =>
          brokerTopicStats.topicStats(tpId.topicPartition().topic()).failedRemoteWriteRequestRate.mark()
          if (!isCancelled()) {
            error(s"Error occurred while copying log segments of partition: $tpId", ex)
          }
      }
    }

    def handleExpiredRemoteLogSegments(): Unit = {
      if (isCancelled())
        return

      def handleLogStartOffsetUpdate(topicPartition: TopicPartition, remoteLogStartOffset: Long): Unit = {
        debug(s"Updating $topicPartition with remoteLogStartOffset: $remoteLogStartOffset")
        updateRemoteLogStartOffset(topicPartition, remoteLogStartOffset)
      }

      try {
        // cleanup remote log segments and update the log start offset if applicable.
        // Compute total size, this can be pushed to RLMM by introducing a new method instead of going through
        // the collection every time.
        val segmentMetadataList = remoteLogMetadataManager.listRemoteLogSegments(tpId).asScala.toSeq
        if (segmentMetadataList.nonEmpty) {
          fetchLog(tpId.topicPartition()).foreach { log =>
            val retentionMs = log.config.retentionMs
            val totalSize = log.size + segmentMetadataList.map(_.segmentSizeInBytes()).sum
            var (checkTimestampRetention, cleanupTs) = (retentionMs > -1, time.milliseconds() - retentionMs)
            var remainingSize = totalSize - log.config.retentionSize
            var checkSizeRetention = log.config.retentionSize > -1

            def deleteRetentionTimeBreachedSegments(metadata: RemoteLogSegmentMetadata): Boolean = {
              val isSegmentDeleted = deleteRemoteLogSegment(metadata, _.maxTimestampMs() <= cleanupTs)
              if (isSegmentDeleted && remainingSize > 0) {
                remainingSize -= metadata.segmentSizeInBytes()
              }
              isSegmentDeleted
            }

            def deleteRetentionSizeBreachedSegments(metadata: RemoteLogSegmentMetadata): Boolean = {
              deleteRemoteLogSegment(metadata, metadata =>
                // Assumption that segments contain size > 0
                if (remainingSize > 0) {
                  remainingSize -= metadata.segmentSizeInBytes()
                  remainingSize >= 0
                } else false
              )
            }

            // Get earliest leader epoch and start deleting the segments.
            var logStartOffset: Option[Long] = None
            log.leaderEpochCache.foreach { cache =>
              cache.epochEntries.find { epochEntry =>
                // segmentsIterator returns the segments in the ascending order.
                val segmentsIterator = remoteLogMetadataManager.listRemoteLogSegments(tpId, epochEntry.epoch)
                // Continue checking for one of the time or size retentions are valid for next segment if available.
                while ((checkTimestampRetention || checkSizeRetention) && segmentsIterator.hasNext) {
                  val metadata = segmentsIterator.next()
                  var isSegmentDeleted = false
                  if (checkTimestampRetention) {
                    if (deleteRetentionTimeBreachedSegments(metadata)) {
                      // It is fine to have logStartOffset as metadata.endOffset() + 1 as the segment offset intervals
                      // are ascending with in an epoch
                      logStartOffset = Some(metadata.endOffset() + 1)
                      info(s"Deleted remote log segment ${metadata.remoteLogSegmentId()} due to retention time " +
                        s"${retentionMs}ms breach based on the largest record timestamp in the segment")
                      isSegmentDeleted = true
                    } else {
                      // If we have any segment that is having the timestamp not eligible for deletion then
                      // we will skip all the subsequent segments for the time retention checks.
                      checkTimestampRetention = false
                    }
                  }
                  if (checkSizeRetention && !isSegmentDeleted) {
                    if (deleteRetentionSizeBreachedSegments(metadata)) {
                      logStartOffset = Some(metadata.endOffset() + 1)
                      info(s"Deleted remote log segment ${metadata.remoteLogSegmentId()} due to retention size " +
                        s"${log.config.retentionSize} breach. Log size after deletion will be ${remainingSize + log.config.retentionSize}.")
                    } else {
                      // If we have exhausted of segments eligible for retention size, we will skip the subsequent
                      // segments.
                      checkSizeRetention = false
                    }
                  }
                }
                // Return only when both the retention checks are exhausted.
                checkTimestampRetention && checkSizeRetention
              }
            }
            logStartOffset.foreach(handleLogStartOffsetUpdate(tpId.topicPartition(), _))
          }
        }
      } catch {
        case ex: Exception =>
          if (!isCancelled()) {
            error(s"Error while cleaning up log segments for partition: $tpId", ex)
          }
      }
    }

    override def run(): Unit = {
      if (isCancelled())
        return

      try {
        if (isLeader()) {
          // a. copy log segments to remote store
          copyLogSegmentsToRemote()
          // b. cleanup/delete expired remote segments
          // Followers will cleanup the local log cleanup based on the local logStartOffset.
          // We do not need any cleanup on followers from remote segments perspective.
          handleExpiredRemoteLogSegments()
        } else {
          fetchLog(tpId.topicPartition()).foreach { log =>
            val offset = findHighestRemoteOffset(tpId)
            log.updateRemoteIndexHighestOffset(offset)
          }
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
        while (!offset.isPresent && epoch.isDefined) {
          offset = remoteLogMetadataManager.highestOffsetForEpoch(topicIdPartition, epoch.get)
          epoch = cache.findPreviousEpoch(epoch.get)
        }
      })
    }
    offset.orElse(-1L)
  }

  def lookupPositionForOffset(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, offset: Long): Int = {
    indexCache.lookupOffset(remoteLogSegmentMetadata, offset)
  }

  def read(remoteStorageFetchInfo: RemoteStorageFetchInfo): FetchDataInfo = {
    val fetchMaxBytes  = remoteStorageFetchInfo.fetchMaxBytes
    val tp = remoteStorageFetchInfo.topicPartition
    val fetchInfo: PartitionData = remoteStorageFetchInfo.fetchInfo

    val includeAbortedTxns = remoteStorageFetchInfo.fetchIsolation == FetchTxnCommitted

    val offset = fetchInfo.fetchOffset
    val maxBytes = Math.min(fetchMaxBytes, fetchInfo.maxBytes)

    // get the epoch for the requested  offset from local leader epoch cache
    // FIXME(@kamal), use the epochForOffset API instead of latest epoch.
    //  val epoch = fetchLog(tp).map(log => log.leaderEpochCache.map(cache => cache.epochForOffset()))
    var rlsMetadata: Optional[RemoteLogSegmentMetadata] = Optional.empty()
    fetchLog(tp).foreach { log =>
      log.leaderEpochCache.foreach(cache => {
        var epoch = cache.latestEpoch
        while (!rlsMetadata.isPresent && epoch.isDefined) {
          rlsMetadata = fetchRemoteLogSegmentMetadata(tp, epoch.get, offset)
          epoch = cache.findPreviousEpoch(epoch.get)
        }
      })
    }

    if (!rlsMetadata.isPresent) {
      throw new OffsetOutOfRangeException(
        s"Received request for offset $offset for partition $tp which does not exist in remote tier. Try again later.")
    }

    val startPos = lookupPositionForOffset(rlsMetadata.get(), offset)
    var remoteSegInputStream: InputStream = null
    try {
      // Search forward for the position of the last offset that is greater than or equal to the target offset
      remoteSegInputStream = remoteLogStorageManager.fetchLogSegment(rlsMetadata.get(), startPos)
      val remoteLogInputStream = new RemoteLogInputStream(remoteSegInputStream)

      def findFirstBatch(): RecordBatch = {
        var nextBatch: RecordBatch = null

        def iterateNextBatch(): RecordBatch = {
          nextBatch = remoteLogInputStream.nextBatch()
          nextBatch
        }
        // Look for the batch which has the desired offset
        // we will always have a batch in that segment as it is a non-compacted topic. For compacted topics, we may need
        //to read from the subsequent segments if there is no batch available for the desired offset in the current
        //segment. That means, desired offset is more than last offset of the current segment and immediate available
        //offset exists in the next segment which can be higher than the desired offset.
        while (iterateNextBatch() != null && nextBatch.lastOffset < offset) {
        }
        nextBatch
      }

      val firstBatch = findFirstBatch()

      if (firstBatch == null)
        return FetchDataInfo(LogOffsetMetadata(offset), MemoryRecords.EMPTY,
          abortedTransactions = if(includeAbortedTxns) Some(List.empty) else None)

      val updatedFetchSize =
        if (remoteStorageFetchInfo.minOneMessage && firstBatch.sizeInBytes() > maxBytes) firstBatch.sizeInBytes()
        else maxBytes

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

      var fetchDataInfo = FetchDataInfo(LogOffsetMetadata(offset), MemoryRecords.readableRecords(buffer))
      if (includeAbortedTxns) {
        fetchDataInfo = addAbortedTransactions(firstBatch.baseOffset(), rlsMetadata.get(), fetchDataInfo)
      }
      fetchDataInfo
    } finally {
      Utils.closeQuietly(remoteSegInputStream, "RemoteLogSegmentInputStream")
    }
  }

  private[remote] def addAbortedTransactions(startOffset: Long,
                                             segmentMetadata: RemoteLogSegmentMetadata,
                                             fetchInfo: FetchDataInfo): FetchDataInfo = {
    val fetchSize = fetchInfo.records.sizeInBytes
    val startOffsetPosition = OffsetPosition(fetchInfo.fetchOffsetMetadata.messageOffset,
      fetchInfo.fetchOffsetMetadata.relativePositionInSegment)

    val offsetIndex = indexCache.getIndexEntry(segmentMetadata).offsetIndex
    val upperBoundOffset = offsetIndex.fetchUpperBoundOffset(startOffsetPosition, fetchSize)
      .map(_.offset).getOrElse(segmentMetadata.endOffset()+1)

    val abortedTransactions = ListBuffer.empty[AbortedTransaction]
    def accumulator(abortedTxn: List[AbortedTxn]): Unit = abortedTransactions ++= abortedTxn.map(_.asAbortedTransaction)

    collectAbortedTransactions(startOffset, upperBoundOffset, segmentMetadata, accumulator)

    FetchDataInfo(fetchOffsetMetadata = fetchInfo.fetchOffsetMetadata,
      records = fetchInfo.records,
      firstEntryIncomplete = fetchInfo.firstEntryIncomplete,
      abortedTransactions = Some(abortedTransactions.toList))
  }

  private[remote] def collectAbortedTransactions(startOffset: Long,
                                                 upperBoundOffset: Long,
                                                 segmentMetadata: RemoteLogSegmentMetadata,
                                                 accumulator: List[AbortedTxn] => Unit): Unit = {
    val topicPartition = segmentMetadata.topicIdPartition().topicPartition()
    val localLogSegments = fetchLog(topicPartition).map(log => log.logSegments.iterator).getOrElse(Iterator.empty)

    var searchInLocalLog = false
    var nextSegmentMetadataOpt = Option.apply(segmentMetadata)
    var txnIndexOpt = nextSegmentMetadataOpt.map(metadata => indexCache.getIndexEntry(metadata).txnIndex)
    while (txnIndexOpt.isDefined) {
      val searchResult = txnIndexOpt.get.collectAbortedTxns(startOffset, upperBoundOffset)
      accumulator(searchResult.abortedTransactions)
      if (!searchResult.isComplete) {
        if (!searchInLocalLog) {
          nextSegmentMetadataOpt = nextSegmentMetadataOpt.flatMap(x => findNextSegmentMetadata(x))
          txnIndexOpt = nextSegmentMetadataOpt.map(x => indexCache.getIndexEntry(x).txnIndex)
          if (txnIndexOpt.isEmpty) {
            searchInLocalLog = true
          }
        }
        if (searchInLocalLog) {
          txnIndexOpt = if (localLogSegments.hasNext) Some(localLogSegments.next().txnIndex) else None
        }
      } else {
        return
      }
    }
  }

  private[remote] def findNextSegmentMetadata(segmentMetadata: RemoteLogSegmentMetadata): Option[RemoteLogSegmentMetadata] = {
    val topicPartition = segmentMetadata.topicIdPartition().topicPartition()
    val nextSegmentBaseOffset = segmentMetadata.endOffset()+1
    var epoch = Option(segmentMetadata.segmentLeaderEpochs().lastEntry().getKey.toInt)
    var result: Option[RemoteLogSegmentMetadata] = Option.empty;
    fetchLog(topicPartition).foreach ( log => {
      log.leaderEpochCache.foreach( cache => {
        while (result.isEmpty && epoch.isDefined) {
          result = Option(fetchRemoteLogSegmentMetadata(topicPartition, epoch.get, nextSegmentBaseOffset).orElse(null))
          epoch = cache.findNextEpoch(epoch.get)
        }
      })
    })
    result
  }

  def fetchRemoteLogSegmentMetadata(tp: TopicPartition,
                                    epochForOffset: Int,
                                    offset: Long): Optional[RemoteLogSegmentMetadata] = {
    val topicIdPartition =
      topicIds.get(tp.topic()) match {
        case Some(uuid) => Some(new TopicIdPartition(uuid, tp))
        case None => None
      }

    if (topicIdPartition.isEmpty) {
      throw new KafkaException("No topic id registered for topic partition: " + tp)
    }
    remoteLogMetadataManager.remoteLogSegmentMetadata(topicIdPartition.get, epochForOffset, offset)
  }

  def lookupTimestamp(rlsMetadata: RemoteLogSegmentMetadata, timestamp: Long, startingOffset: Long): Option[TimestampAndOffset] = {
    val startPos = indexCache.lookupTimestamp(rlsMetadata, timestamp, startingOffset)

    var remoteSegInputStream: InputStream = null
    try {
      // Search forward for the position of the last offset that is greater than or equal to the target offset
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
  def findOffsetByTimestamp(tp: TopicPartition,
                            timestamp: Long,
                            startingOffset: Long,
                            leaderEpochCache: LeaderEpochFileCache): Option[TimestampAndOffset] = {
    val topicId = topicIds.get(tp.topic())
    if (topicId.isEmpty) {
      throw new KafkaException("Topic id does not exist for topic partition: " + tp)
    }
    // Get the respective epoch in which the starting offset exists.
    var maybeEpoch = leaderEpochCache.epochForOffset(startingOffset);
    while (maybeEpoch.nonEmpty) {
      remoteLogMetadataManager.listRemoteLogSegments(new TopicIdPartition(topicId.get, tp), maybeEpoch.get).asScala
        .foreach(rlsMetadata =>
          if (rlsMetadata.maxTimestampMs() >= timestamp && rlsMetadata.endOffset() >= startingOffset) {
            val timestampOffset = lookupTimestamp(rlsMetadata, timestamp, startingOffset)
            if (timestampOffset.isDefined) {
              return timestampOffset
            }
          }
        )

      // Move to the next epoch if not found with the current epoch.
      maybeEpoch = leaderEpochCache.findNextEpoch(maybeEpoch.get)
    }
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
  def asyncRead(fetchInfo: RemoteStorageFetchInfo, callback: RemoteLogReadResult => Unit): AsyncReadTask = {
    AsyncReadTask(remoteStorageFetcherThreadPool.submit(new RemoteLogReader(fetchInfo, this, brokerTopicStats, callback)))
  }

  /**
   * Stops all the threads and releases all the resources.
   */
  def close(): Unit = {
    if (closed)
      warn("Trying to close an already closed RemoteLogManager")
    else this synchronized {
      // Write lock is not taken when closing this class. As, the read lock is held by other threads which might be
      // waiting on the producer future (or) trying to consume the metadata record for strong consistency.
      if (!closed) {
        // During segment copy, the RLM task publishes an event and tries to consume the same for strong consistency.
        // The active RLM task might be waiting on the producer future (or) trying to consume the record.
        // So, tasks should be cancelled first, close the RLMM, RSM, then shutdown the thread pool to close the active
        // tasks.
        leaderOrFollowerTasks.values().forEach(_.cancel())
        Utils.closeQuietly(remoteLogMetadataManager, "RemoteLogMetadataManager")
        Utils.closeQuietly(remoteLogStorageManager, "RemoteLogStorageManager")
        rlmScheduledThreadPool.shutdown()
        remoteStorageFetcherThreadPool.shutdown()
        leaderOrFollowerTasks.clear()
        closed = true
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
    private var epochs: Seq[EpochEntry] = Seq()
    override def write(epochs: Iterable[EpochEntry]): Unit = this.epochs = epochs.toSeq
    override def read(): Seq[EpochEntry] = this.epochs

    def readAsByteBuffer(): ByteBuffer = {
      val stream = new ByteArrayOutputStream()
      val writer = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8))
      val writeBuffer = new CheckpointWriteBuffer[EpochEntry](writer, version = 0, LeaderEpochCheckpointFile.Formatter)
      try {
        writeBuffer.write(epochs)
        writer.flush()
        ByteBuffer.wrap(stream.toByteArray)
      } finally {
        writer.close()
      }
    }
  }
}