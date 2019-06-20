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
import java.nio.file.{Files, Path}
import java.util
import java.util.concurrent._

import kafka.log._
import kafka.server.{FetchDataInfo, LogReadResult}
import kafka.utils.Logging
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{Configurable, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, Set}

class RemoteLogManager(logManager: LogManager) extends Logging with Configurable with Closeable {
  private var remoteStorageManager: RemoteStorageManager = _
  private var rlmFollower: RLMFollower = _
  private var rlmIndexer: RLMIndexer = _

  private val watchedSegments: BlockingQueue[LogSegmentEntry] = new LinkedBlockingQueue[LogSegmentEntry]()

  private val polledDirs: util.Map[TopicPartition, Path] = new ConcurrentHashMap[TopicPartition, Path]()
  private val maxOffsets: util.Map[TopicPartition, Long] = new ConcurrentHashMap[TopicPartition, Long]()

  private val pollerDirsRunnable: Runnable = () => {
    polledDirs.forEach((tp: TopicPartition, path: Path) => {
      val dirPath: Path = path
      val tp: TopicPartition = Log.parseTopicPartitionName(dirPath.toFile)

      val maxOffset = maxOffsets.get(tp)
      //todo-satish avoid polling log directories as it may introduce IO ops to read directory metadata
      // we need to introduce a callback whenever log rollover occurs for a topic partition.
      val logFiles: List[String] = Files.newDirectoryStream(dirPath, (entry: Path) => {
        val fileName = entry.getFileName.toFile.getName
        fileName.endsWith(Log.LogFileSuffix) && (Log.offsetFromFileName(fileName) > maxOffset)
      }).iterator().asScala
        .map(x => x.getFileName.toFile.getName).toList

      if (logFiles.size > 1) {
        val sortedPaths = logFiles.sorted
        val passiveLogFiles = sortedPaths.slice(0, sortedPaths.size - 1)
        logManager.getLog(tp).foreach(log => {
          val lastSegmentOffset = Log.offsetFromFileName(passiveLogFiles.last)
          val segments = log.logSegments(maxOffset + 1, lastSegmentOffset)
          segments.foreach(segment => watchedSegments.add(LogSegmentEntry(tp, segment.baseOffset, segment)))
          maxOffsets.put(tp, lastSegmentOffset)
        })
      }
    })
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

            //todo-satish Not all LogSegments on different replicas are same. So, we need to avoid duplicating log-segments in remote
            // tier with similar offset ranges.
            val entries = remoteStorageManager.copyLogSegment(tp, segment)
            val file = segment.log.file()
            val fileName = file.getName
            val baseOffsetStr = fileName.substring(0, fileName.indexOf("."))
            rlmIndexer.maybeBuildIndexes(tp, entries, file, baseOffsetStr)
          } catch {
            case ex: InterruptedException => throw ex
            case ex: Throwable => //todo-satish log the message here for failed log copying
              logger.warn("Error occured while building indexes in RLM leader", ex)
          }
        }
      } catch {
        case ex: InterruptedException => // log the interrupted error
          logger.info("Current thread is interrupted with ", ex)
      }
    }
  })

  /**
   * Configure this class with the given key-value pairs
   */
  override def configure(configs: util.Map[String, _]): Unit = {
    remoteStorageManager = Class.forName(configs.get("remote.log.storage.manager.class").toString)
      .getDeclaredConstructor().newInstance().asInstanceOf[RemoteStorageManager]
    //todo-satish filter configs with remote storage manager having key with prefix "remote.log.storage.manager.prop"
    remoteStorageManager.configure(configs)
    rlmIndexer = new RLMIndexer(remoteStorageManager, logManager)
    //    remoteStorageManager.configure(configs.filterKeys( key => key.startsWith("remote.log.storage.manager")).)
    rlmFollower = new RLMFollower(remoteStorageManager, logManager, rlmIndexer)
  }

  /**
   * Ask RLM to monitor the given TopicPartitions, and copy inactive Log
   * Segments to remote storage. RLM updates local index files once a
   * Log Segment in a TopicPartition is copied to Remote Storage
   */
  def handleLeaderPartitions(topicPartitions: Set[TopicPartition]): Boolean = {

    // stop begin followers as they become leaders.
    rlmFollower.removeFollowers(JavaConverters.asJavaCollection(topicPartitions))

    topicPartitions.foreach(tp =>
      logManager.getLog(tp).foreach(log => {
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
      logManager.getLog(tp).foreach(log => log.logSegments
        .foreach(logSegment => {
          watchedSegments.remove(logSegment)
          remoteStorageManager.cancelCopyingLogSegment(tp)
        }))
    })

    //cancel the watched directories for topic/partition.
    topicPartitions.foreach(tp => {
      polledDirs.remove(tp)
    })

    //these partitions become followers
    rlmFollower.addFollowers(JavaConverters.asJavaCollection(topicPartitions))

    true
  }

  /**
   * Read topic partition data from remote
   *
   * @param fetchMaxByes
   * @param hardMaxBytesLimit
   * @param tp
   * @param fetchInfo
   * @return
   */
  def read(fetchMaxByes: Int, hardMaxBytesLimit: Boolean, tp: TopicPartition, fetchInfo: PartitionData): LogReadResult = {
    val offset = fetchInfo.fetchOffset
    val entry = rlmIndexer.lookupEntryForOffset(tp, offset).getOrElse(throw new RuntimeException)
    //todo need to get other information to build the expected LogReadResult
    val records = remoteStorageManager.read(entry, fetchInfo.maxBytes, offset)
    null
  }

  /**
   * Stops all the threads and closes the instance.
   */
  def close() = {
    Utils.closeQuietly(remoteStorageManager, "RemoteLogStorageManager")
    executorService.shutdownNow()
    pollerExecutorService.shutdownNow()
  }

}

case class RemoteLogManagerConfig(remoteLogStorageEnable: Boolean,
                                  remoteLogStorageManagerClass: String,
                                  remoteLogRetentionBytes: Long,
                                  remoteLogRetentionMillis: Long)

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
