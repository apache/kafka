/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import kafka.metrics.LinuxIoMetricsCollector
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.metrics.{KafkaMetricsContext, MetricConfig, Metrics, MetricsReporter, Sensor}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.slf4j.Logger

import java.util
import java.util.concurrent.TimeUnit


trait Server {
  def startup(): Unit
  def shutdown(): Unit
  def awaitShutdown(): Unit
}

object Server {
  val MetricsPrefix: String = "kafka.server"
  val ClusterIdLabel: String = "kafka.cluster.id"
  val BrokerIdLabel: String = "kafka.broker.id"
  val NodeIdLabel: String = "kafka.node.id"

  def initializeMetrics(
    config: KafkaConfig,
    time: Time,
    clusterId: String
  ): Metrics = {
    val metricsContext = createKafkaMetricsContext(config, clusterId)
    buildMetrics(config, time, metricsContext)
  }

  def maybeRegisterLinuxMetrics(
    config: KafkaConfig,
    time: Time,
    logger: Logger
  ): Unit = {
    val linuxIoMetricsCollector = new LinuxIoMetricsCollector("/proc", time, logger)
    linuxIoMetricsCollector.maybeRegisterMetrics(KafkaYammerMetrics.defaultRegistry())
  }

  private def buildMetrics(
    config: KafkaConfig,
    time: Time,
    metricsContext: KafkaMetricsContext
  ): Metrics = {
    val metricConfig = buildMetricsConfig(config)
    new Metrics(metricConfig, new util.ArrayList[MetricsReporter](), time, true, metricsContext)
  }

  def buildMetricsConfig(
    kafkaConfig: KafkaConfig
  ): MetricConfig = {
    new MetricConfig()
      .samples(kafkaConfig.metricNumSamples)
      .recordLevel(Sensor.RecordingLevel.forName(kafkaConfig.metricRecordingLevel))
      .timeWindow(kafkaConfig.metricSampleWindowMs, TimeUnit.MILLISECONDS)
  }

  private[server] def createKafkaMetricsContext(
    config: KafkaConfig,
    clusterId: String
  ): KafkaMetricsContext = {
    val contextLabels = new java.util.HashMap[String, Object]
    contextLabels.put(ClusterIdLabel, clusterId)

    if (config.usesSelfManagedQuorum) {
      contextLabels.put(NodeIdLabel, config.nodeId.toString)
    } else {
      contextLabels.put(BrokerIdLabel, config.brokerId.toString)
    }

    contextLabels.putAll(config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX))
    new KafkaMetricsContext(MetricsPrefix, contextLabels)
  }

  sealed trait ProcessStatus
  case object SHUTDOWN extends ProcessStatus
  case object STARTING extends ProcessStatus
  case object STARTED extends ProcessStatus
  case object SHUTTING_DOWN extends ProcessStatus
}
