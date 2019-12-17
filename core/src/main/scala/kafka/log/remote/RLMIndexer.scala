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
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.function.{Consumer, Function}

import kafka.log.Log
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

object RLMIndexer {
  val UNKNOWN_INDEX: Long = -1L
}

class RLMIndexer(rsm: RemoteStorageManager, logFetcher: TopicPartition => Option[Log]) extends AutoCloseable {

  private val remoteIndexes: util.concurrent.ConcurrentMap[TopicPartition, TopicPartitionRemoteIndex] = new ConcurrentHashMap[TopicPartition, TopicPartitionRemoteIndex]()

  def lookupLastOffset(tp: TopicPartition): Option[Long] = {
    maybeLoadIndex(tp).flatMap(_.lastOffset)
  }

  def lookupEntryForOffset(tp: TopicPartition, offset: Long): Option[RemoteLogIndexEntry] = {
    maybeLoadIndex(tp).flatMap(_.lookupEntryForOffset(offset))
  }

  def lookupEntryForTimestamp(tp: TopicPartition, timestamp: Long, startingOffset: Long): Option[RemoteLogIndexEntry] = {
    maybeLoadIndex(tp).flatMap(_.lookupEntryForTimestamp(timestamp, startingOffset))
  }

  /**
   *
   * @param tp
   * @return the offset of the topic-partition that is already indexed if it has done earlier, else it returns -1.
   */
  def getOrLoadIndexOffset(tp: TopicPartition): Option[Long] = {
    maybeLoadIndex(tp).flatMap(_.lastOffset)
  }

  def maybeLoadIndex(tp: TopicPartition): Option[TopicPartitionRemoteIndex] = {
    Option(remoteIndexes.computeIfAbsent(tp, new Function[TopicPartition, TopicPartitionRemoteIndex]() {
      override def apply(tp: TopicPartition): TopicPartitionRemoteIndex = {
        val log = logFetcher(tp).getOrElse(
          throw new RuntimeException("This broker is not a leader or a a follower for the given topic partition " + tp))

        val parentDir = log.dir
        TopicPartitionRemoteIndex.open(tp, parentDir)
      }
    }))
  }

  def maybeBuildIndexes(tp: TopicPartition, entries: Seq[RemoteLogIndexEntry], parentDir: File, baseOffsetStr: String): Boolean = {
    if (entries.nonEmpty) {
      val indexEntry = remoteIndexes.computeIfAbsent(tp, new Function[TopicPartition, TopicPartitionRemoteIndex] {
        override def apply(x: TopicPartition): TopicPartitionRemoteIndex = TopicPartitionRemoteIndex.open(x, parentDir)
      })
      val maybeLong = indexEntry.appendEntries(entries, baseOffsetStr)
      maybeLong.isDefined && maybeLong.get >= 0
    } else false
  }

  override def close(): Unit = {
    remoteIndexes.values().forEach(new Consumer[TopicPartitionRemoteIndex] {
      override def accept(x: TopicPartitionRemoteIndex): Unit = Utils.closeQuietly(x, "RLMIndexEntry")
    })
  }
}
