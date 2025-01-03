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

import kafka.log.LogManager
import kafka.log.remote.RemoteLogManager
import kafka.network.SocketServer
import kafka.utils.Logging
import org.apache.kafka.common.ClusterResource
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.{Metrics, MetricsReporter}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.security.CredentialProvider
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.NodeToControllerChannelManager
import org.apache.kafka.server.metrics.{KafkaMetricsGroup, KafkaYammerMetrics, LinuxIoMetricsCollector}
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.LogDirFailureChannel
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.time.Duration
import scala.collection.Seq
import scala.jdk.CollectionConverters._

object KafkaBroker {
  //properties for MetricsContext
  private val MetricsTypeName: String = "KafkaServer"

  private[server] def notifyClusterListeners(clusterId: String,
                                             clusterListeners: Seq[AnyRef]): Unit = {
    val clusterResourceListeners = new ClusterResourceListeners
    clusterResourceListeners.maybeAddAll(clusterListeners.asJava)
    clusterResourceListeners.onUpdate(new ClusterResource(clusterId))
  }

  private[server] def notifyMetricsReporters(clusterId: String,
                                             config: KafkaConfig,
                                             metricsReporters: Seq[AnyRef]): Unit = {
    val metricsContext = Server.createKafkaMetricsContext(config, clusterId)
    metricsReporters.foreach {
      case x: MetricsReporter => x.contextChange(metricsContext)
      case _ => //do nothing
    }
  }

  /**
   * The log message that we print when the broker has been successfully started.
   * The ducktape system tests look for a line matching the regex 'Kafka\s*Server.*started'
   * to know when the broker is started, so it is best not to change this message -- but if
   * you do change it, be sure to make it match that regex or the system tests will fail.
   */
  val STARTED_MESSAGE = "Kafka Server started"

  val MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS: Long = 120000
}

trait KafkaBroker extends Logging {
  // Number of shards to split FetchSessionCache into. This is to reduce contention when trying to
  // acquire lock while handling Fetch requests.
  val NumFetchSessionCacheShards: Int = 8

  def authorizer: Option[Authorizer]
  def brokerState: BrokerState
  def clusterId: String
  def config: KafkaConfig
  def dataPlaneRequestHandlerPool: KafkaRequestHandlerPool
  def dataPlaneRequestProcessor: KafkaApis
  def kafkaScheduler: Scheduler
  def kafkaYammerMetrics: KafkaYammerMetrics
  def logDirFailureChannel: LogDirFailureChannel
  def logManager: LogManager
  def remoteLogManagerOpt: Option[RemoteLogManager]
  def metrics: Metrics
  def quotaManagers: QuotaFactory.QuotaManagers
  def replicaManager: ReplicaManager
  def socketServer: SocketServer
  def metadataCache: MetadataCache
  def groupCoordinator: GroupCoordinator
  def boundPort(listenerName: ListenerName): Int
  def startup(): Unit
  def awaitShutdown(): Unit
  def shutdown(): Unit = shutdown(Duration.ofMinutes(5))
  def shutdown(timeout: Duration): Unit
  def isShutdown(): Boolean
  def brokerTopicStats: BrokerTopicStats
  def credentialProvider: CredentialProvider
  def clientToControllerChannelManager: NodeToControllerChannelManager
  def tokenCache: DelegationTokenCache

  // For backwards compatibility, we need to keep older metrics tied
  // to their original name when this class was named `KafkaServer`
  private val metricsGroup = new KafkaMetricsGroup(Server.MetricsPrefix, KafkaBroker.MetricsTypeName)

  metricsGroup.newGauge("BrokerState", () => brokerState.value)
  metricsGroup.newGauge("ClusterId", () => clusterId)
  metricsGroup.newGauge("yammer-metrics-count", () =>  KafkaYammerMetrics.defaultRegistry.allMetrics.size)

  private val linuxIoMetricsCollector = new LinuxIoMetricsCollector("/proc", Time.SYSTEM)

  if (linuxIoMetricsCollector.usable()) {
    metricsGroup.newGauge("linux-disk-read-bytes", () => linuxIoMetricsCollector.readBytes())
    metricsGroup.newGauge("linux-disk-write-bytes", () => linuxIoMetricsCollector.writeBytes())
  }
}
