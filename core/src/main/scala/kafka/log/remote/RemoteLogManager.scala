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
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.server.epoch.LeaderEpochFileCache
import kafka.utils.Logging
import org.apache.kafka.common._
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{RecordBatch, RemoteLogInputStream}
import org.apache.kafka.common.utils.{ChildFirstClassLoader, Utils}
import org.apache.kafka.server.log.remote.metadata.storage.ClassLoaderAwareRemoteLogMetadataManager
import org.apache.kafka.server.log.remote.storage.{ClassLoaderAwareRemoteStorageManager, RemoteLogManagerConfig, RemoteLogMetadataManager, RemoteLogSegmentMetadata, RemoteStorageManager}

import java.io.{Closeable, InputStream}
import java.security.{AccessController, PrivilegedAction}
import java.util
import java.util.Optional
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import scala.collection.Set
import scala.jdk.CollectionConverters._

/**
 * This class is responsible for
 *  - initializing `RemoteStorageManager` and `RemoteLogMetadataManager` instances.
 *  - receives any leader and follower replica events and partition stop events and act on them
 *  - also provides APIs to fetch indexes, metadata about remote log segments.
 *
 * @param rlmConfig Configuration required for remote logging subsystem(tiered storage) at the broker level.
 * @param brokerId  id of the current broker.
 * @param logDir    directory of Kafka log segments.
 */
class RemoteLogManager(rlmConfig: RemoteLogManagerConfig,
                       brokerId: Int,
                       logDir: String) extends Logging with Closeable with KafkaMetricsGroup {

  // topic ids received on leadership changes
  private val topicPartitionIds: ConcurrentMap[TopicPartition, Uuid] = new ConcurrentHashMap[TopicPartition, Uuid]()

  private val remoteLogStorageManager: RemoteStorageManager = createRemoteStorageManager()
  private val remoteLogMetadataManager: RemoteLogMetadataManager = createRemoteLogMetadataManager()

  private val indexCache = new RemoteIndexCache(remoteStorageManager = remoteLogStorageManager, logDir = logDir)

  private var closed = false

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
    debug(s"Received leadership changes for leaders: $partitionsBecomeLeader and followers: $partitionsBecomeFollower")

    // Partitions logs are available when this callback is invoked.
    // Compact topics and internal topics are filtered here as they are not supported with tiered storage.
    def filterPartitions(partitions: Set[Partition]): Set[TopicIdPartition] = {
      // We are not specifically checking for internal topics etc here as `log.remoteLogEnabled()` already handles that.
      partitions.filter(partition => partition.log.exists(log => log.remoteLogEnabled()))
        .map(partition => new TopicIdPartition(topicIds.get(partition.topic), partition.topicPartition))
    }

    val followerTopicPartitions = filterPartitions(partitionsBecomeFollower)
    val leaderTopicPartitions = filterPartitions(partitionsBecomeLeader)
    debug(s"Effective topic partitions after filtering compact and internal topics, leaders: $leaderTopicPartitions " +
      s"and followers: $followerTopicPartitions")

    if (leaderTopicPartitions.nonEmpty || followerTopicPartitions.nonEmpty) {
      leaderTopicPartitions.foreach(x => topicPartitionIds.put(x.topicPartition(), x.topicId()))
      followerTopicPartitions.foreach(x => topicPartitionIds.put(x.topicPartition(), x.topicId()))

      remoteLogMetadataManager.onPartitionLeadershipChanges(leaderTopicPartitions.asJava, followerTopicPartitions.asJava)
    }
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
    while (maybeEpoch.nonEmpty) {
      val epoch = maybeEpoch.get
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

  /**
   * Closes and releases all the resources like RemoterStorageManager and RemoteLogMetadataManager.
   */
  def close(): Unit = {
    this synchronized {
      if (!closed) {
        Utils.closeQuietly(remoteLogStorageManager, "RemoteLogStorageManager")
        Utils.closeQuietly(remoteLogMetadataManager, "RemoteLogMetadataManager")
        Utils.closeQuietly(indexCache, "RemoteIndexCache")
        closed = true
      }
    }
  }

}