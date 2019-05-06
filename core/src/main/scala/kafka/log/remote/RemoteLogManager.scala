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
import java.util
import java.util.concurrent.{BlockingQueue, ConcurrentHashMap, Executors, LinkedBlockingDeque}

import kafka.log.{LogManager, LogSegment, OffsetIndex, TimeIndex}
import kafka.server.LogReadResult
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{Configurable, TopicPartition}

import scala.collection.Set

class RemoteLogManager(logManager: LogManager) extends Configurable with Closeable {
  var remoteStorageManager: RemoteStorageManager = _

  val watchedSegments: BlockingQueue[LogSegment] = new LinkedBlockingDeque[LogSegment]()

  val remoteLogIndexes: util.Map[Long, RemoteLogIndex] = new ConcurrentHashMap[Long, RemoteLogIndex]()
  val remoteOffsetIndexes: util.Map[Long, OffsetIndex] = new ConcurrentHashMap[Long, OffsetIndex]()
  val remoteTimeIndexes: util.Map[Long, TimeIndex] = new ConcurrentHashMap[Long, TimeIndex]()

  //todo configurable no of tasks/threads
  val executorService = Executors.newSingleThreadExecutor()
  executorService.submit(new Runnable() {
    override def run(): Unit = {
      while (true) {
        try {
          val segment = watchedSegments.take()

          //todo-satish Not all LogSegments on different replicas are same. So, we need to avoid duplicating log-segments in remote
          // tier with similar offset ranges.
          val tuple = remoteStorageManager.copyLogSegment(segment)
          //todo-satish need to explore whether the above should return RDI as each RemoteLogIndexEntry has RDI. That entry
          // can be optimized not to have RDI value for each entry.
          val rdi = tuple._1
          val entries = tuple._2
          val file = segment.log.file()
          val prefix = file.getName.substring(0, file.getName.indexOf("."))
          buildIndexes(entries, file, prefix)
        } catch {
          case _: InterruptedException => return
          case _: Throwable => //todo-satish log the message here for failed log copying and add them again in pending segments.
        }
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
      //todo compute position
      remoteOffsetIndex.append(entry.firstOffset, position)
      remoteTimeIndex.maybeAppend(entry.firstTimeStamp, entry.firstOffset)
    })

    remoteLogIndex.flush()
    remoteOffsetIndex.flush()
    remoteTimeIndex.flush()

    remoteLogIndexes.put(minOffset, remoteLogIndex)
    remoteOffsetIndexes.put(minOffset, remoteOffsetIndex)
    remoteTimeIndexes.put(minOffset, remoteTimeIndex)
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
    // schedule monitoring topic/partition directories to fetch log segments and push it to remote storage.
    val dirs: util.List[LogSegment] = new util.ArrayList[LogSegment]()
    topicPartitions.foreach(tp => logManager.getLog(tp)
      .foreach(log => {
        log.logSegments.foreach(x => {
          if (x != log.activeSegment) dirs.add(x)
        })
      }))

    watchedSegments.addAll(dirs)
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
          remoteStorageManager.cancelCopyingLogSegment(logSegment)
        }))
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
  }

}

case class RemoteLogManagerConfig(remoteLogStorageEnable: Boolean,
                                  remoteLogStorageManagerClass: String,
                                  remoteLogRetentionBytes: Long,
                                  remoteLogRetentionMillis: Long)
