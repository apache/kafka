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

import java.io.{Closeable, File, InputStream}
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Collections, Optional}
import java.lang
import kafka.cluster.Partition
import kafka.log.Log
import kafka.metrics.KafkaMetricsGroup
import kafka.server.checkpoints.LeaderEpochCheckpointFile
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import kafka.server.{BrokerTopicStats, FetchDataInfo, FetchTxnCommitted, KafkaConfig, LogOffsetMetadata, RemoteStorageFetchInfo}
import kafka.utils.Logging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.{Endpoint, KafkaException, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.FetchResponseData.AbortedTransaction
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch, RemoteLogInputStream}
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.{ChildFirstClassLoader, KafkaThread, Time, Utils}
import org.apache.kafka.server.log.remote.metadata.storage.{ClassLoaderAwareRemoteLogMetadataManager, TopicBasedRemoteLogMetadataManagerConfig}
import org.apache.kafka.server.log.remote.storage.{LogSegmentData, RemoteLogManagerConfig, RemoteLogMetadataManager, RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, RemoteStorageManager}

import scala.collection.mutable
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

  def poolSize(): Int = scheduledThreadPool.getMaximumPoolSize

  def getIdlePercent(): Double = {
    1 - scheduledThreadPool.getActiveCount().asInstanceOf[Double] / scheduledThreadPool.getCorePoolSize.asInstanceOf[Double]
  }

  def scheduleWithFixedDelay(runnable: Runnable, initialDelay: Long, delay: Long,
                             timeUnit: TimeUnit): ScheduledFuture[_] = {
    info(s"Scheduling runnable $runnable with initial delay: $initialDelay, fixed delay: $delay")
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

  // topic ids received on leadership changes
  private val topicIds: mutable.Map[String, Uuid] = mutable.Map.empty

  @volatile private var closed = false

  newGauge("RemoteLogManagerTasksAvgIdlePercent", () => {
    rlmScheduledThreadPool.getIdlePercent()
  })

  private def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = {
    val classPath = rlmConfig.remoteStorageManagerClassPath()
    val rsmClassLoader = {
      if (classPath != null && classPath.trim.nonEmpty) {
        new ChildFirstClassLoader(classPath, this.getClass.getClassLoader)
      } else {
        this.getClass.getClassLoader
      }
    }

    val rsm = rsmClassLoader.loadClass(rlmConfig.remoteStorageManagerClassName())
      .getDeclaredConstructor().newInstance().asInstanceOf[RemoteStorageManager]
    new ClassLoaderAwareRemoteStorageManager(rsm, rsmClassLoader)
  }

  private def configureRSM(): Unit = {
    val rsmProps = new util.HashMap[String, Any]()
    rlmConfig.remoteStorageManagerProps().asScala.foreach { case (k, v) => rsmProps.put(k, v) }
    rsmProps.put(KafkaConfig.BrokerIdProp, brokerId)
    rsmProps.put("cluster.id", clusterId)
    remoteLogStorageManager.configure(rsmProps)
  }

  private def createRemoteLogMetadataManager(): RemoteLogMetadataManager = {
    val classPath = rlmConfig.remoteLogMetadataManagerClassPath

    val rlmm: RemoteLogMetadataManager = if (classPath != null && classPath.trim.nonEmpty) {
      val rlmmClassLoader = new ChildFirstClassLoader(classPath, this.getClass.getClassLoader)
      val rlmmLoaded = rlmmClassLoader.loadClass(rlmConfig.remoteLogMetadataManagerClassName())
        .getDeclaredConstructor().newInstance().asInstanceOf[RemoteLogMetadataManager]
      new ClassLoaderAwareRemoteLogMetadataManager(rlmmLoaded, rlmmClassLoader)
    } else {
      this.getClass.getClassLoader.loadClass(rlmConfig.remoteLogMetadataManagerClassName()).getDeclaredConstructor()
        .newInstance().asInstanceOf[RemoteLogMetadataManager]
    }

    rlmm
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
    remoteLogStorageManager.delegate()
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
    debug(s"Received leadership changes for leaders: $partitionsBecomeLeader and followers: $partitionsBecomeFollower")
    topicIds.forEach((topic, uuid) => this.topicIds.put(topic, uuid))

    // Partitions logs are available when this callback is invoked.
    // Compact topics and internal topics are filtered here as they are not supported with tiered storage.
    def filterPartitions(partitions: Set[Partition]): Set[Partition] = {
      partitions.filterNot(partition => Topic.isInternal(partition.topic) ||
        partition.topic.equals(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME) ||
        partition.log.exists(log => !log.remoteLogEnabled())
      )
    }

    val followerTopicPartitions = filterPartitions(partitionsBecomeFollower).map(partition =>
      new TopicIdPartition(topicIds.get(partition.topic), partition.topicPartition))

    val filteredLeaderPartitions = filterPartitions(partitionsBecomeLeader)
    val leaderTopicPartitions = filteredLeaderPartitions.map(partition =>
      new TopicIdPartition(topicIds.get(partition.topic), partition.topicPartition))

    debug(s"Effective topic partitions after filtering compact and internal topics, leaders: $leaderTopicPartitions " +
      s"and followers: $followerTopicPartitions")

    if (leaderTopicPartitions.nonEmpty || followerTopicPartitions.nonEmpty) {
     remoteLogMetadataManager.onPartitionLeadershipChanges(leaderTopicPartitions.asJava, followerTopicPartitions.asJava)
    }

    followerTopicPartitions.foreach { tp => doHandleLeaderOrFollowerPartitions(tp, task => task.convertToFollower())}

    filteredLeaderPartitions.foreach { partition =>
      doHandleLeaderOrFollowerPartitions(new TopicIdPartition(topicIds.get(partition.topic), partition.topicPartition),
        task => task.convertToLeader(partition.getLeaderEpoch))
    }
  }

  /**
   * Stops partitions for copying segments, building indexes and deletes the partition in remote storage if delete flag
   * is set as true.
   *
   * @param topicPartition  topic partition to be stopped.
   * @param delete          flag to indicate whether the given topic partitions to be deleted or not.
   */
  def stopPartitions(topicPartition: TopicPartition, delete: Boolean): Unit = {
    // unassign topic partitions from RLM leader/follower
    val topicIdPartition = topicIds.remove(topicPartition.topic())
       match {
        case Some(uuid) => Some(new TopicIdPartition(uuid, topicPartition))
        case None => None
      }

    if (topicIdPartition.isDefined) {
      val rlmTaskWithFuture = leaderOrFollowerTasks.remove(topicIdPartition.get)
      if (rlmTaskWithFuture != null) {
        rlmTaskWithFuture.cancel()
      }
    }

    if (delete) {
      try {
        //todo-tier need to check whether it is really needed to delete from remote. This may be a delete request only
        //for this replica. We should delete from remote storage only if the topic partition is getting deleted.
        topicIdPartition.foreach(idPartition => {
          remoteLogMetadataManager.listRemoteLogSegments(idPartition).forEachRemaining(elt => deleteRemoteLogSegment(elt, _ => true))
          remoteLogMetadataManager.onStopPartitions(Collections.singleton(idPartition))
        })
      } catch {
        case ex: Exception => error(s"Error occurred while deleting topic partition: $topicPartition", ex)
      }
    }
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

  class RLMTask(tp: TopicIdPartition) extends CancellableRunnable with Logging {
    this.logIdent = s"[RemoteLogManager=$brokerId partition=$tp] "
    @volatile private var leaderEpoch: Int = -1

    private def isLeader(): Boolean = leaderEpoch >= 0

    private var readOffset: Long = -1

    //todo-updating log with remote index highest offset -- should this be required?
    // fetchLog(tp.topicPartition()).foreach { log => log.updateRemoteIndexHighestOffset(readOffset) }

    def convertToLeader(leaderEpochVal: Int): Unit = {
      if (leaderEpochVal < 0) {
        throw new KafkaException(s"leaderEpoch value for topic partition $tp can not be negative")
      }
      if (this.leaderEpoch != leaderEpochVal) {
        leaderEpoch = leaderEpochVal
        info(s"Find the highest remote offset for partition: $tp after becoming leader, leaderEpoch: $leaderEpoch")
        readOffset = findHighestRemoteOffset(tp)
      }
    }

    def convertToFollower(): Unit = {
      leaderEpoch = -1
    }

    def copyLogSegmentsToRemote(): Unit = {
      try {
        fetchLog(tp.topicPartition()).foreach { log => {
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
                if (isCancelled() || !isLeader()) {
                  info(s"Skipping copying log segments as the current task state is changed, cancelled: " +
                    s"${isCancelled()} leader:${isLeader()}")
                  return
                }

                val logFile = segment.log.file()
                val fileName = logFile.getName
                info(s"Copying $fileName to remote storage.")
                val id = new RemoteLogSegmentId(tp, Uuid.randomUuid())

                val nextOffset = segment.readNextOffset
                //todo-tier double check on this
                val endOffset = nextOffset - 1
                val producerIdSnapshotFile = log.producerStateManager.fetchSnapshot(nextOffset).orNull

                def createLeaderEpochs(): ByteBuffer = {
                  val leaderEpochStateFile = new File(logFile.getParentFile, "leader-epoch-checkpoint-" + nextOffset)
                  try {
                    log.leaderEpochCache
                      .map(cache => cache.writeTo(new LeaderEpochCheckpointFile(leaderEpochStateFile)))
                      .foreach(x => {
                        x.truncateFromEnd(nextOffset)
                      })

                    ByteBuffer.wrap(Files.readAllBytes(leaderEpochStateFile.toPath))
                  } finally {
                    try {
                      Files.delete(leaderEpochStateFile.toPath)
                    } catch {
                      case ex: Exception => warn(s"Error occurred while deleting leader epoch file: $leaderEpochStateFile", ex)
                    }
                  }
                }

                def createLeaderEpochEntries(startOffset: Long): Option[collection.Seq[EpochEntry]] = {
                  val leaderEpochStateFile = new File(logFile.getParentFile,
                    "leader-epoch-checkpoint-entries-" + startOffset + "-" + nextOffset)
                  try {
                    val checkpointFile = {
                      val file = new LeaderEpochCheckpointFile(leaderEpochStateFile)
                      log.leaderEpochCache
                        .map(cache => cache.writeTo(file))
                        .map(x => {
                          x.truncateFromStart(startOffset)
                          x.truncateFromEnd(nextOffset)
                          file
                        })
                    }
                    checkpointFile.map(x => x.read())
                  } finally {
                    try {
                      Files.delete(leaderEpochStateFile.toPath)
                    } catch {
                      case ex: Exception => warn(s"Error occurred while deleting leader epoch file: $leaderEpochStateFile", ex)
                    }
                  }
                }

                val leaderEpochs = createLeaderEpochs()
                val segmentLeaderEpochEntries = createLeaderEpochEntries(segment.baseOffset)
                val segmentLeaderEpochs: util.HashMap[Integer, java.lang.Long] = new util.HashMap()
                if (segmentLeaderEpochEntries.isDefined) {
                  segmentLeaderEpochEntries.get.foreach(entry => segmentLeaderEpochs.put(entry.epoch, entry.startOffset))
                } else {
                  val epoch = log.leaderEpochCache.flatMap(x => x.latestEntry.map(y => y.epoch)).getOrElse(0)
                  segmentLeaderEpochs.put(epoch, segment.baseOffset)
                }

                val remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(id, segment.baseOffset, endOffset,
                  segment.largestTimestamp, brokerId, time.milliseconds(), segment.log.sizeInBytes(),
                  segmentLeaderEpochs)

                remoteLogMetadataManager.addRemoteLogSegmentMetadata(remoteLogSegmentMetadata)

                val segmentData = new LogSegmentData(logFile.toPath, segment.lazyOffsetIndex.get.path,
                  segment.lazyTimeIndex.get.path, Optional.ofNullable(segment.txnIndex.path),
                  producerIdSnapshotFile.toPath, leaderEpochs)
                remoteLogStorageManager.copyLogSegmentData(remoteLogSegmentMetadata, segmentData)

                val rlsmAfterCreate = new RemoteLogSegmentMetadataUpdate(id, time.milliseconds(),
                  RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId)

                remoteLogMetadataManager.updateRemoteLogSegmentMetadata(rlsmAfterCreate)
                brokerTopicStats.topicStats(tp.topicPartition().topic())
                  .remoteBytesOutRate.mark(remoteLogSegmentMetadata.segmentSizeInBytes())
                readOffset = endOffset
                //todo-tier-storage
                log.updateRemoteIndexHighestOffset(readOffset)
                info(s"Copied $fileName to remote storage.")
              }
            }
          } else {
            debug(s"Skipping copying segments, current read offset:$readOffset is and LSO:$lso ")
          }
        }
        }
      } catch {
        case ex: Exception =>
          brokerTopicStats.topicStats(tp.topicPartition().topic()).failedRemoteWriteRequestRate.mark()
          error(s"Error occurred while copying log segments of partition: $tp", ex)
      }
    }

    def handleExpiredRemoteLogSegments(): Unit = {

      def handleLogStartOffsetUpdate(topicPartition: TopicPartition, remoteLogStartOffset: Long): Unit = {
        debug(s"Updating $topicPartition with remoteLogStartOffset: $remoteLogStartOffset")
        updateRemoteLogStartOffset(topicPartition, remoteLogStartOffset)
      }

      try {
        if (isLeader()) {
          // cleanup remote log segments and update the log start offset if applicable.
          val remoteLogSegmentMetadatas = remoteLogMetadataManager.listRemoteLogSegments(tp)
          if (!remoteLogSegmentMetadatas.hasNext)
            None
          else {
            var maxStartOffset: Option[Long] = None

            fetchLog(tp.topicPartition()).foreach(log => {
              val retentionMs = log.config.retentionMs
              var (checkTimeStampRetention, cleanupTs) =
                if (retentionMs < 0) (false, time.milliseconds())
                else (true, time.milliseconds() - retentionMs)

              // Compute total size, this can be pushed to RLMM by introducing a new method instead of going through
              // the collection every time.
              var totalSize = log.size
              remoteLogMetadataManager.listRemoteLogSegments(tp)
                .forEachRemaining(metadata => totalSize += metadata.segmentSizeInBytes())

              var remainingSize = totalSize - log.config.retentionSize
              var checkSizeRetention = log.config.retentionSize > -1

              def deleteRetentionTimeBreachedSegments(segmentMetadata: RemoteLogSegmentMetadata): Boolean = {
                deleteRemoteLogSegment(segmentMetadata, segmentMetadata => segmentMetadata.maxTimestampMs() <= cleanupTs)
              }

              def deleteRetentionSizeBreachedSegments(segmentMetadata: RemoteLogSegmentMetadata): Boolean = {
                deleteRemoteLogSegment(segmentMetadata,
                  segmentMetadata => {
                    // Assumption that segments contain size > 0
                    if (remainingSize > 0) {
                      remainingSize = remainingSize - segmentMetadata.segmentSizeInBytes()
                      remainingSize >= 0
                    } else false
                  })
              }

              // Get earliest leader epoch and start deleting the segments.
              log.leaderEpochCache.foreach(cache => {
                cache.epochEntries.find(epoch => {
                  val segmentsIterator = remoteLogMetadataManager.listRemoteLogSegments(tp, epoch.epoch)
                  // Continue checking for one of the time or size retentions are valid for next segment if available.
                  while ((checkTimeStampRetention || checkSizeRetention) && segmentsIterator.hasNext) {
                    val segmentMetadata = segmentsIterator.next()
                    // Set the max start offset, `segmentsIterator` is already returns them in ascending order.
                    // FIXME(@kamalcph): If all the remote log segments are eligible for deletion, then the maxStartOffset should be set to None.
                    maxStartOffset = Some(segmentMetadata.startOffset())

                    var segmentDeletedWithRetentionTime = false

                    if (checkTimeStampRetention) {
                      if (deleteRetentionTimeBreachedSegments(segmentMetadata)) {
                        info(s"Deleted remote log segment based on retention time: ${segmentMetadata.remoteLogSegmentId()}")
                        segmentDeletedWithRetentionTime = true
                      } else {
                        // If we have any segment that is having the timestamp not eligible for deletion then
                        // we will skip all the subsequent segments for the time retention checks.
                        checkTimeStampRetention = false
                      }
                    } else if (checkSizeRetention && !segmentDeletedWithRetentionTime) {
                      if (deleteRetentionSizeBreachedSegments(segmentMetadata)) {
                        info(s"Deleted remote log segment based on retention size: ${segmentMetadata.remoteLogSegmentId()}")
                      } else {
                        // If we have exhausted of segments eligible for retention size, we will skip the subsequent
                        // segments.
                        checkSizeRetention = false
                      }
                    }
                  }

                  // Return only when both the retention checks are exhausted.
                  checkTimeStampRetention && checkSizeRetention
                })
              })

              maxStartOffset.foreach(x => handleLogStartOffsetUpdate(tp.topicPartition(), x))
            })
          }
        }
      } catch {
        case ex: Exception => error(s"Error while cleaning up log segments for partition: $tp", ex)
      }
    }

    override def run(): Unit = {
      try {
        if (!isCancelled()) {
          if (isLeader()) {
            //a. copy log segments to remote store
            copyLogSegmentsToRemote()
          } else {
            fetchLog(tp.topicPartition()).foreach { log =>
              val offset = findHighestRemoteOffset(tp)
              log.updateRemoteIndexHighestOffset(offset)
            }
          }
          // b. cleanup/delete expired remote segments
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
      this.getClass.toString + s"[$tp]"
    }
  }

  // This is found by traversing from the latest leader epoch from leader epoch history and find the highest offset
  // of a segment with that epoch copied into remote storage. If it can not find an entry then it checks for the
  // previous leader epoch till it finds an entry, If there are no entries till the earliest leader epoch in leader
  // epoch cache then it returns -1.
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
    // val epoch = fetchLog(tp).map(log => log.leaderEpochCache.map(cache => cache.epochForOffset()))
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

      val abortedTxns = if (includeAbortedTxns) {
        Some(collectAbortedTransactions(rlsMetadata.get(), firstBatch.baseOffset(), updatedFetchSize))
      }  else {
        None
      }
      FetchDataInfo(LogOffsetMetadata(offset), MemoryRecords.readableRecords(buffer), abortedTransactions = abortedTxns)
    } finally {
      Utils.closeQuietly(remoteSegInputStream, "RemoteLogSegmentInputStream")
    }
  }

  private def collectAbortedTransactions(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, offset:Long,
                                         fetchSize:Int): List[AbortedTransaction] = {
    // TxnIndexSearchResult will be useful whether to search through the next segments or not.
    indexCache.collectAbortedTransaction(remoteLogSegmentMetadata, offset, fetchSize).abortedTransactions
      .map(_.asAbortedTransaction)
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
      Utils.closeQuietly(remoteLogStorageManager, "RemoteLogStorageManager")
      Utils.closeQuietly(remoteLogMetadataManager, "RemoteLogMetadataManager")
      leaderOrFollowerTasks.values().forEach((taskWithFuture: RLMTaskWithFuture) => taskWithFuture.cancel())
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