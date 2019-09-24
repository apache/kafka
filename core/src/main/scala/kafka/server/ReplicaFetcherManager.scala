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

import com.yammer.metrics.core.Gauge
import kafka.cluster.BrokerEndPoint
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

import scala.collection.Map

class ReplicaFetcherManager(brokerConfig: KafkaConfig,
                            protected val replicaManager: ReplicaManager,
                            metrics: Metrics,
                            time: Time,
                            threadNamePrefix: Option[String] = None,
                            quotaManager: ReplicationQuotaManager)
      extends AbstractFetcherManager[ReplicaFetcherThread](
        name = "ReplicaFetcherManager on broker " + brokerConfig.brokerId,
        clientId = "Replica",
        numFetchers = brokerConfig.numReplicaFetchers) {

  newGauge(
    "ReassignmentMaxLagOnFollower",
    new Gauge[Long] {
      // current max lag across all fetchers/topics/partitions
      def value: Long = fetcherThreadMap.foldLeft(0L)((curMaxAll, fetcherThreadMapEntry) => {
        val replicaMgr = fetcherThreadMapEntry._2.replicaManager
        fetcherThreadMapEntry._2.fetcherLagStats.stats.foldLeft(0L)((curMaxThread, fetcherLagStatsEntry) => {
          val partitionAndLagMetrics = replicaMgr.getPartition(fetcherLagStatsEntry._1.topicPartition) match {
            case HostedPartition.Online(p) => Some(p -> fetcherLagStatsEntry._2)
            case _ => None
          }
          if (partitionAndLagMetrics.exists(_._1.isAddingLocalReplica))
            curMaxThread.max(partitionAndLagMetrics.get._2.lag)
          else
            0L
        }).max(curMaxAll)
      })
    },
    Map("clientId" -> "Replica")
  )

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
    val prefix = threadNamePrefix.map(tp => s"$tp:").getOrElse("")
    val threadName = s"${prefix}ReplicaFetcherThread-$fetcherId-${sourceBroker.id}"
    new ReplicaFetcherThread(threadName, fetcherId, sourceBroker, brokerConfig, failedPartitions, replicaManager,
      metrics, time, quotaManager)
  }

  def shutdown(): Unit = {
    info("shutting down")
    closeAllFetchers()
    info("shutdown completed")
  }
}
