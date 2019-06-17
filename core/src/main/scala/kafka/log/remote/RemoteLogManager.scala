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

import java.io.{Closeable, File}
import java.nio.file.{Files, Path}
import java.util
import java.util.concurrent._

import kafka.log._
import kafka.server.LogReadResult
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{Configurable, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.Set

class RemoteLogManager(logManager: LogManager) extends Configurable with Closeable {
  private var remoteStorageManager: RemoteStorageManager = _

  private val watchedSegments: BlockingQueue[LogSegmentEntry] = new LinkedBlockingQueue[LogSegmentEntry]()

  private val remoteLogIndexes: ConcurrentNavigableMap[Long, RemoteLogIndex] = new ConcurrentSkipListMap[Long, RemoteLogIndex]()
  private val remoteOffsetIndexes: ConcurrentNavigableMap[Long, OffsetIndex] = new ConcurrentSkipListMap[Long, OffsetIndex]()
  private val remoteTimeIndexes: ConcurrentNavigableMap[Long, TimeIndex] = new ConcurrentSkipListMap[Long, TimeIndex]()

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
            val prefix = fileName.substring(0, fileName.indexOf("."))
            buildIndexes(entries, file, prefix)
          } catch {
            case ex: InterruptedException => throw ex
            case _: Throwable => //todo-satish log the message here for failed log copying and add them again in pending segments.
              if (segmentEntry != null) watchedSegments.put(segmentEntry)
          }
        }
      } catch {
        case _: InterruptedException => // log the interrupted error
      }
    }
  })

  private def buildIndexes(entries: Seq[RemoteLogIndexEntry], parentDir: File, prefix: String) = {
    val startOffset = prefix.toLong
    val remoteLogIndex = new RemoteLogIndex(startOffset, new File(parentDir, prefix + ".remoteLogIndex"))
    val remoteTimeIndex = new TimeIndex(new File(parentDir, prefix + ".remoteTimeIndex"), startOffset)
    val remoteOffsetIndex = new OffsetIndex(new File(parentDir, prefix + ".remoteOffsetIndex"), startOffset)
    var position: Integer = 0
    var minOffset: Long = 0
    var minTimeStamp: Long = 0

    entries.foreach(entry => {
      if (entry.firstOffset < minOffset) minOffset = entry.firstOffset
      if (entry.firstTimeStamp < minTimeStamp) minTimeStamp = entry.firstTimeStamp
      remoteLogIndex.append(entry)
      position += 16 + entry.entryLength
      remoteOffsetIndex.append(entry.firstOffset, position)
      remoteTimeIndex.maybeAppend(entry.firstTimeStamp, position.toLong)
    })

    remoteLogIndex.flush()
    remoteOffsetIndex.flush()
    remoteTimeIndex.flush()

    remoteLogIndexes.put(minOffset, remoteLogIndex)
    remoteOffsetIndexes.put(minOffset, remoteOffsetIndex)
    remoteTimeIndexes.put(minTimeStamp, remoteTimeIndex)
  }

  /**
   * Configure this class with the given key-value pairs
   */
  override def configure(configs: util.Map[String, _]): Unit = {
    remoteStorageManager = Class.forName(configs.get("remote.log.storage.manager.class").toString)
      .getDeclaredConstructor().newInstance().asInstanceOf[RemoteStorageManager]
    //todo-satish filter configs with remote storage manager having key with prefix "remote.log.storage.manager.prop"
    remoteStorageManager.configure(configs)
    //    remoteStorageManager.configure(configs.filterKeys( key => key.startsWith("remote.log.storage.manager")).)
  }

  /**
   * Ask RLM to monitor the given TopicPartitions, and copy inactive Log
   * Segments to remote storage. RLM updates local index files once a
   * Log Segment in a TopicPartition is copied to Remote Storage
   */
  def addPartitions(topicPartitions: Set[TopicPartition]): Boolean = {
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
  def removePartitions(topicPartitions: Set[TopicPartition]): Boolean = {
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
    //todo get the nearest offset from indexes.
    val offsetPosition = remoteOffsetIndexes.floorEntry(fetchInfo.fetchOffset).getValue.lookup(fetchInfo.fetchOffset)
    //todo read RemoteLogIndexEntry
    remoteStorageManager.read(null, fetchInfo.maxBytes, fetchInfo.fetchOffset)
    null
  }

  /**
   * Stops all the threads and closes the instance.
   */
  def close() = {
    Utils.closeQuietly(remoteStorageManager, "RemoteLogStorageManager")
    remoteLogIndexes.values().forEach(x => Utils.closeQuietly(x, "RemoteLogIndex"))
    remoteOffsetIndexes.values().forEach(x => Utils.closeQuietly(x, "RemoteOffsetIndex"))
    remoteTimeIndexes.values().forEach(x => Utils.closeQuietly(x, "RemoteTimeIndex"))
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
