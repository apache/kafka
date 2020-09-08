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

import kafka.server.{BrokerTopicStats, FetchDataInfo, RemoteStorageFetchInfo}
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time

class RemoteLogReader(fetchInfo: RemoteStorageFetchInfo, rlm: RemoteLogManager, brokerTopicStats: BrokerTopicStats, callback: (RemoteLogReadResult) => Unit)
  extends RemoteStorageTask[Unit] with Logging {
  brokerTopicStats.topicStats(fetchInfo.topicPartition.topic()).remoteReadRequestRate.mark()

  override def execute(): Unit = {
    val result = {
      try {
        val r = rlm.read(fetchInfo)
        brokerTopicStats.topicStats(fetchInfo.topicPartition.topic()).remoteBytesInRate.mark(r.records.sizeInBytes())
        RemoteLogReadResult(Some(r), None)
      } catch {
        case e: Exception =>
          brokerTopicStats.topicStats(fetchInfo.topicPartition.topic()).failedRemoteReadRequestRate.mark()
          error("Error due to", e)
          RemoteLogReadResult(None, Some(e))
      }
    }
    callback(result)
  }
}

case class RemoteLogReadResult(info: Option[FetchDataInfo], error: Option[Throwable] = None)

class RemoteStorageReaderThreadPool(numThreads: Int, maxPendingTasks: Int, time: Time)
  extends RemoteStorageThreadPool(RemoteStorageReaderThreadPool.name, RemoteStorageReaderThreadPool.threadNamePrefix,
    numThreads, maxPendingTasks, time, RemoteStorageReaderThreadPool.metricNamePrefix)

object RemoteStorageReaderThreadPool {
  val name = "Remote Log Reader Thread Pool"
  val threadNamePrefix = "RemoteLogReader"
  val metricNamePrefix = "RemoteLogReader"
}
