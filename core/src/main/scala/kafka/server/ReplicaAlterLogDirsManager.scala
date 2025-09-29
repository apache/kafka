/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.server

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.common.DirectoryEventHandler
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

class ReplicaAlterLogDirsManager(brokerConfig: KafkaConfig,
                                 replicaManager: ReplicaManager,
                                 quotaManager: ReplicationQuotaManager,
                                 brokerTopicStats: BrokerTopicStats,
                                 directoryEventHandler: DirectoryEventHandler = DirectoryEventHandler.NOOP
                                )
  extends AbstractFetcherManager[ReplicaAlterLogDirsThread](
    name = s"ReplicaAlterLogDirsManager on broker ${brokerConfig.brokerId}",
    clientId = "ReplicaAlterLogDirs",
    numFetchers = brokerConfig.getNumReplicaAlterLogDirsThreads) {

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaAlterLogDirsThread = {
    val threadName = s"ReplicaAlterLogDirsThread-$fetcherId"
    val leader = new LocalLeaderEndPoint(sourceBroker, brokerConfig, replicaManager, quotaManager)
    new ReplicaAlterLogDirsThread(threadName, leader, failedPartitions, replicaManager,
      quotaManager, brokerTopicStats, brokerConfig.replicaFetchBackoffMs, directoryEventHandler)
  }

  override protected def addPartitionsToFetcherThread(fetcherThread: ReplicaAlterLogDirsThread,
                                                      initialOffsetAndEpochs: collection.Map[TopicPartition, InitialFetchState]): Unit = {
    val addedPartitions = fetcherThread.addPartitions(initialOffsetAndEpochs)
    val (addedInitialOffsets, notAddedInitialOffsets) = initialOffsetAndEpochs.partition { case (tp, _) =>
      addedPartitions.contains(tp)
    }

    if (addedInitialOffsets.nonEmpty)
      info(s"Added log dir fetcher for partitions with initial offsets $addedInitialOffsets")

    if (notAddedInitialOffsets.nonEmpty)
      info(s"Failed to add log dir fetch for partitions ${notAddedInitialOffsets.keySet} " +
        s"since the log dir reassignment has already completed")
  }

  def shutdown(): Unit = {
    info("shutting down")
    closeAllFetchers()
    info("shutdown completed")
  }
}
