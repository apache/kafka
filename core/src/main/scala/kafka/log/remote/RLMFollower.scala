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

import java.util
import java.util.Comparator
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.function.Consumer

import kafka.log.Log
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters
import scala.concurrent.JavaConversions

class RLMFollower(remoteStorageManager: RemoteStorageManager, logFetcher: TopicPartition => Option[Log], rlmIndexer: RLMIndexer) extends Logging {

  private def createConcurrentSet[T](): util.Set[T] = util.Collections.newSetFromMap(
    new java.util.concurrent.ConcurrentHashMap[T, java.lang.Boolean])

  val watchedTopicPartitions: util.Set[TopicPartition] = createConcurrentSet()

  private val followerIndexFetcher: Runnable = new Runnable() {
    override def run(): Unit = {
      try {
        watchedTopicPartitions.forEach(new Consumer[TopicPartition] {
          override def accept(tp: TopicPartition): Unit = {
            val remoteLogSegmentInfos = rlmIndexer.getOrLoadIndexOffset(tp) match {
              case Some(offset) =>
                remoteStorageManager.listRemoteSegments(tp, offset)
              case None =>
                remoteStorageManager.listRemoteSegments(tp)
            }
            remoteLogSegmentInfos.forEach(segInfo => {
              val indexEntries = remoteStorageManager.getRemoteLogIndexEntries(segInfo)
              logFetcher(tp).map(log => log.dir).foreach(dir => {
                rlmIndexer.maybeBuildIndexes(tp, indexEntries, dir, Log.filenamePrefixFromOffset(segInfo.baseOffset))
              })
            })
          }
        })
      } catch {
        case ex: Exception =>
          logger.error("Exception occurred while building indexes for follower", ex)
      }
    }
  }

  private val pollerExecutorService: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  //todo time intervals should be made configurable with default values
  pollerExecutorService.scheduleWithFixedDelay(followerIndexFetcher, 30, 30, TimeUnit.SECONDS)

  def removeFollowers(topicPartitions: util.Collection[TopicPartition]): Unit = {
    watchedTopicPartitions.removeAll(topicPartitions)
  }

  def addFollowers(topicPartitions: util.Collection[TopicPartition]): Unit = {
    watchedTopicPartitions.addAll(topicPartitions)
  }

}
