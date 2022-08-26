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

import com.yammer.metrics.core.MetricName
import kafka.coordinator.group.GroupCoordinator
import kafka.log.LogManager
import kafka.metrics.{KafkaMetricsGroup, LinuxIoMetricsCollector}
import kafka.network.SocketServer
import kafka.security.CredentialProvider
import kafka.utils.KafkaScheduler
import org.apache.kafka.common.ClusterResource
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.{Metrics, MetricsReporter}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.metrics.KafkaYammerMetrics

import scala.collection.Seq
import scala.jdk.CollectionConverters._

object KafkaBroker {
  //properties for MetricsContext
  val MetricsTypeName: String = "KafkaServer"

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
}

trait KafkaBroker extends KafkaMetricsGroup {

  def authorizer: Option[Authorizer]
  def brokerState: BrokerState
  def clusterId: String
  def config: KafkaConfig
  def dataPlaneRequestHandlerPool: KafkaRequestHandlerPool
  def dataPlaneRequestProcessor: KafkaApis
  def kafkaScheduler: KafkaScheduler
  def kafkaYammerMetrics: KafkaYammerMetrics
  def logManager: LogManager
  def metrics: Metrics
  def quotaManagers: QuotaFactory.QuotaManagers
  def replicaManager: ReplicaManager
  def socketServer: SocketServer
  def metadataCache: MetadataCache
  def groupCoordinator: GroupCoordinator
  def boundPort(listenerName: ListenerName): Int
  def startup(): Unit
  def awaitShutdown(): Unit
  def shutdown(): Unit
  def brokerTopicStats: BrokerTopicStats
  def credentialProvider: CredentialProvider
  def clientToControllerChannelManager: BrokerToControllerChannelManager

  // For backwards compatibility, we need to keep older metrics tied
  // to their original name when this class was named `KafkaServer`
  override def metricName(name: String, metricTags: scala.collection.Map[String, String]): MetricName = {
    explicitMetricName(Server.MetricsPrefix, KafkaBroker.MetricsTypeName, name, metricTags)
  }

  newGauge("BrokerState", () => brokerState.value)
  newGauge("ClusterId", () => clusterId)
  newGauge("yammer-metrics-count", () =>  KafkaYammerMetrics.defaultRegistry.allMetrics.size)

  private val linuxIoMetricsCollector = new LinuxIoMetricsCollector("/proc", Time.SYSTEM, logger.underlying)

  if (linuxIoMetricsCollector.usable()) {
    newGauge("linux-disk-read-bytes", () => linuxIoMetricsCollector.readBytes())
    newGauge("linux-disk-write-bytes", () => linuxIoMetricsCollector.writeBytes())
  }
}
