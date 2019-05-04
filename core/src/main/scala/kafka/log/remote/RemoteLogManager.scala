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

import java.io.File
import java.nio.file.Files
import java.util
import java.util.concurrent.{BlockingQueue, Executors, LinkedBlockingDeque}

import kafka.log.{LogManager, LogSegment}
import kafka.server.LogReadResult
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.{Configurable, TopicPartition}

import scala.collection.Set

class RemoteLogManager(logManager: LogManager) extends Configurable {
  var remoteStorageManager: RemoteStorageManager = _

  val watchedSegments: BlockingQueue[LogSegment] = new LinkedBlockingDeque[LogSegment]()

  //todo configurable no of tasks/threads
  val threadPool = Executors.newSingleThreadExecutor()
  threadPool.submit(new Runnable() {
    override def run(): Unit = {
      while (true) {
        val segment = watchedSegments.take()

        try {
          val tuple = remoteStorageManager.copyLogSegment(segment)
          val rdi = tuple._1
          val entries = tuple._2
          val file = segment.log.file()
          val prefix = file.getName.substring(0, file.getName.indexOf("."))
          val remoteLogIndex = new RemoteLogIndex(prefix.toLong, Files.createFile(new File(file.getParentFile, prefix).toPath).toFile)
          entries.foreach(entry => remoteLogIndex.append(entry))
          remoteLogIndex.flush()
          remoteLogIndex.close()
        } catch {
          case _: Throwable => //todo log the message here for failed log copying and add them again in pending segments.
        }
      }
    }
  })

  /**
   * Configure this class with the given key-value pairs
   */
  override def configure(configs: util.Map[String, _]): Unit = {
    remoteStorageManager = Class.forName(configs.get("remote.log.storage.manager.class").toString)
      .getDeclaredConstructor().newInstance().asInstanceOf[RemoteStorageManager]
    //todo filter configs with remote storage manager having key with prefix "remote.log.storage.manager.prop"
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
        val segment = log.activeSegment
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
    false
  }

  /**
   * Read topic partition data from remote
   *
   * @param fetchMaxByes
   * @param hardMaxBytesLimit
   * @param readPartitionInfo
   * @return
   */
  def read(fetchMaxByes: Int, hardMaxBytesLimit: Boolean, readPartitionInfo: Seq[(TopicPartition, PartitionData)]): LogReadResult = {
    null
  }

  /**
   * Stops all the threads and closes the instance.
   */
  def shutdown(): Unit = {
    remoteStorageManager.shutdown()
  }

}

case class RemoteLogManagerConfig(remoteLogStorageEnable: Boolean,
                                  remoteLogStorageManagerClass: String,
                                  remoteLogRetentionBytes: Long,
                                  remoteLogRetentionMillis: Long)
