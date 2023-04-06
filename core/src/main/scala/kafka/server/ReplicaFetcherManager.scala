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

import kafka.cluster.BrokerEndPoint
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.server.common.MetadataVersion

class ReplicaFetcherManager(brokerConfig: KafkaConfig,
                            protected val replicaManager: ReplicaManager,
                            metrics: Metrics,
                            time: Time,
                            threadNamePrefix: Option[String] = None,
                            quotaManager: ReplicationQuotaManager,
                            metadataVersionSupplier: () => MetadataVersion,
                            brokerEpochSupplier: () => Long)
      extends AbstractFetcherManager[ReplicaFetcherThread](
        name = "ReplicaFetcherManager on broker " + brokerConfig.brokerId,
        clientId = "Replica",
        numFetchers = brokerConfig.numReplicaFetchers) {

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
    val prefix = threadNamePrefix.map(tp => s"$tp:").getOrElse("")
    val threadName = s"${prefix}ReplicaFetcherThread-$fetcherId-${sourceBroker.id}"
    val logContext = new LogContext(s"[ReplicaFetcher replicaId=${brokerConfig.brokerId}, leaderId=${sourceBroker.id}, " +
      s"fetcherId=$fetcherId] ")
    val endpoint = new BrokerBlockingSender(sourceBroker, brokerConfig, metrics, time, fetcherId,
      s"broker-${brokerConfig.brokerId}-fetcher-$fetcherId", logContext)
    val fetchSessionHandler = new FetchSessionHandler(logContext, sourceBroker.id)
    val leader = new RemoteLeaderEndPoint(logContext.logPrefix, endpoint, fetchSessionHandler, brokerConfig,
      replicaManager, quotaManager, metadataVersionSupplier, brokerEpochSupplier)
    new ReplicaFetcherThread(threadName, leader, brokerConfig, failedPartitions, replicaManager,
      quotaManager, logContext.logPrefix, metadataVersionSupplier)
  }

  def shutdown(): Unit = {
    info("shutting down")
    closeAllFetchers()
    info("shutdown completed")
  }
}
