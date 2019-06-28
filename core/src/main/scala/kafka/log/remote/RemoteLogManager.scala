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
import java.nio.file.{DirectoryStream, Files, Path}
import java.util
import java.util.concurrent._
import java.util.function.BiConsumer

import kafka.log.{Log, LogSegment}
import kafka.server.{Defaults, FetchDataInfo, KafkaConfig, LogOffsetMetadata}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, Set}

class RemoteLogManager(logFetcher: TopicPartition => Option[Log], rlmConfig: RemoteLogManagerConfig) extends Logging with Closeable {

  private val watchedSegments: BlockingQueue[LogSegmentEntry] = new LinkedBlockingQueue[LogSegmentEntry]()
  private val polledDirs = new ConcurrentHashMap[TopicPartition, Path]().asScala
  private val maxOffsets: util.Map[TopicPartition, Long] = new ConcurrentHashMap[TopicPartition, Long]()

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
  private val rlmFollower: RLMFollower = new RLMFollower(remoteStorageManager, logFetcher, rlmIndexer)

  private val pollerDirsRunnable: Runnable = new Runnable() {
    override def run(): Unit = {
      polledDirs.foreach { case (tp, path) =>
        val dirPath: Path = path
        val tp: TopicPartition = Log.parseTopicPartitionName(dirPath.toFile)

        val maxOffset = maxOffsets.get(tp)
        val paths = Files.newDirectoryStream(dirPath, new DirectoryStream.Filter[Path]() {
          override def accept(path: Path): Boolean = {
            val fileName = path.toFile.getName
            fileName.endsWith(Log.LogFileSuffix) && (Log.offsetFromFileName(fileName) > maxOffset)
          }
        })
        try {
          //todo-satish avoid polling log directories as it may introduce IO ops to read directory metadata
          // we need to introduce a callback whenever log rollover occurs for a topic partition.
          val logFiles: List[String] = paths.iterator().asScala
            .map(x => x.toFile.getName).toList

          if (logFiles.size > 1) {
            val sortedPaths = logFiles.sorted
            val passiveLogFiles = sortedPaths.slice(0, sortedPaths.size - 1)
            logFetcher(tp).foreach(log => {
              val lastSegmentOffset = Log.offsetFromFileName(passiveLogFiles.last)
              val segments = log.logSegments(maxOffset + 1, lastSegmentOffset)
              segments.foreach(segment => watchedSegments.add(LogSegmentEntry(tp, segment.baseOffset, segment)))
              maxOffsets.put(tp, lastSegmentOffset)
            })
          }
        } finally {
          Utils.closeQuietly(paths, "paths")
        }
      }
    }
  }

  private val pollerExecutorService: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  //todo time intervals should be made configurable with default values
  pollerExecutorService.scheduleWithFixedDelay(pollerDirsRunnable, 30, 30, TimeUnit.SECONDS)

  //todo configurable no of tasks/threads
  private val executorService = Executors.newSingleThreadExecutor()
  executorService.submit(new Runnable() {
    override def run(): Unit = {
      try {
        while (true) {
          var segmentEntry: LogSegmentEntry = null
          try {
            segmentEntry = watchedSegments.take()
            val segment = segmentEntry.logSegment

            val tp = segmentEntry.topicPartition

            //todo-satish Not all LogSegments on different replicas are same. So, we need to avoid duplicating
            // log-segments in remote tier with similar offset ranges.
            val entries = remoteStorageManager.copyLogSegment(tp, segment)
            val file = segment.log.file()
            val fileName = file.getName
            val baseOffsetStr = fileName.substring(0, fileName.indexOf("."))
            rlmIndexer.maybeBuildIndexes(tp, entries, file, baseOffsetStr)
          } catch {
            case ex: InterruptedException => throw ex
            case ex: Throwable =>
              logger.error(s"Error occurred while building indexes in RLM leader for segment entry $segmentEntry", ex)
          }
        }
      } catch {
        case ex: InterruptedException =>
          logger.info("Current thread is interrupted with ", ex)
      }
    }
  })

  /**
   * Ask RLM to monitor the given TopicPartitions, and copy inactive Log
   * Segments to remote storage. RLM updates local index files once a
   * Log Segment in a TopicPartition is copied to Remote Storage
   */
  def handleLeaderPartitions(topicPartitions: Set[TopicPartition]): Boolean = {

    // stop begin followers as they become leaders.
    rlmFollower.removeFollowers(JavaConverters.asJavaCollectionConverter(topicPartitions).asJavaCollection)

    topicPartitions.foreach(tp =>
      logFetcher(tp).foreach(log => {
        val segments: util.List[LogSegmentEntry] = new util.ArrayList[LogSegmentEntry]()
        log.logSegments.foreach(x => {
          segments.add(LogSegmentEntry(tp, x.baseOffset, x))
        })

        // remove the last segment which is active
        if (segments.size() > 0) segments.remove(segments.size() - 1)

        if (segments.size() > 0) maxOffsets.put(tp, segments.get(segments.size() - 1).baseOffset)
        watchedSegments.addAll(segments)

        // add the polling dirs to the list.
        polledDirs.put(tp, log.dir.toPath)
      }))

    true
  }

  /**
   * Stops copy of LogSegment if TopicPartition ownership is moved from a broker.
   */
  def handleFollowerPartitions(topicPartitions: Set[TopicPartition]): Boolean = {

    // remove these partitions as they become followers
    topicPartitions.foreach(tp => {
      logFetcher(tp).foreach(log => log.logSegments
        .foreach(logSegment => {
          watchedSegments.remove(LogSegmentEntry(tp, logSegment.baseOffset, logSegment))
          remoteStorageManager.cancelCopyingLogSegment(tp)
        }))
    })

    //cancel the watched directories for topic/partition.
    topicPartitions.foreach(tp => {
      polledDirs.remove(tp)
    })

    //these partitions become followers
    rlmFollower.addFollowers(JavaConverters.asJavaCollectionConverter(topicPartitions).asJavaCollection)

    true
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
      .getOrElse(throw new OffsetOutOfRangeException(s"Received request for offset $offset for partition $tp, which does not exist in remote tier"))

    val records = remoteStorageManager.read(entry, fetchInfo.maxBytes, offset, minOneMessage)

    FetchDataInfo(LogOffsetMetadata(offset), records)
  }

  /**
   * Stops all the threads and releases all the resources.
   */
  def close(): Unit = {
    Utils.closeQuietly(remoteStorageManager, "RemoteLogStorageManager")
    executorService.shutdownNow()
    pollerExecutorService.shutdownNow()
  }

}

case class RemoteLogManagerConfig(remoteLogStorageEnable: Boolean,
                                  remoteLogStorageManagerClass: String,
                                  remoteLogRetentionBytes: Long,
                                  remoteLogRetentionMillis: Long,
                                  remoteStorageConfig: scala.collection.immutable.Map[String, Any])

private case class LogSegmentEntry(topicPartition: TopicPartition,
                                   baseOffset: Long,
                                   logSegment: LogSegment) {
  override def hashCode(): Int = {
    val fields = Seq(topicPartition, baseOffset)
    fields.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case that: LogSegmentEntry => topicPartition.equals(that.topicPartition) && baseOffset.equals(that.baseOffset)
      case _ => false
    }
  }
}

object RemoteLogManager {
  val REMOTE_STORAGE_MANAGER_CONFIG_PREFIX = "remote.log.storage."
  val DefaultConfig = RemoteLogManagerConfig(remoteLogStorageEnable = Defaults.RemoteLogStorageEnable, null,
    Defaults.RemoteLogRetentionBytes, TimeUnit.MINUTES.toMillis(Defaults.RemoteLogRetentionMinutes), Map.empty)

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
      config.remoteLogRetentionBytes, config.remoteLogRetentionMillis, rsmProps.toMap)
  }
}
