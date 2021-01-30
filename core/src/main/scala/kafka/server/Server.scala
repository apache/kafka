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

import java.util.concurrent.TimeUnit

import kafka.log.LogConfig.{CleanupPolicyProp, CompressionTypeProp, DeleteRetentionMsProp, FileDeleteDelayMsProp, FlushMessagesProp, FlushMsProp, IndexIntervalBytesProp, MaxCompactionLagMsProp, MaxMessageBytesProp, MessageDownConversionEnableProp, MessageFormatVersionProp, MessageTimestampDifferenceMaxMsProp, MessageTimestampTypeProp, MinCleanableDirtyRatioProp, MinCompactionLagMsProp, MinInSyncReplicasProp, PreAllocateEnableProp, RetentionBytesProp, RetentionMsProp, SegmentBytesProp, SegmentIndexBytesProp, SegmentJitterMsProp, SegmentMsProp, UncleanLeaderElectionEnableProp}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.metrics.{JmxReporter, KafkaMetricsContext, MetricConfig, Metrics, MetricsReporter, Sensor}
import org.apache.kafka.common.utils.Time

trait Server {
  def startup(): Unit
  def shutdown(): Unit
  def awaitShutdown(): Unit
}

object Server {
  /**
   * Copy the subset of properties that are relevant to Logs. The individual properties
   * are listed here since the names are slightly different in each Config class...
   */
  private[kafka] def copyKafkaConfigToLog(kafkaConfig: KafkaConfig): java.util.Map[String, Object] = {
    val logProps = new java.util.HashMap[String, Object]()
    logProps.put(SegmentBytesProp, kafkaConfig.logSegmentBytes)
    logProps.put(SegmentMsProp, kafkaConfig.logRollTimeMillis)
    logProps.put(SegmentJitterMsProp, kafkaConfig.logRollTimeJitterMillis)
    logProps.put(SegmentIndexBytesProp, kafkaConfig.logIndexSizeMaxBytes)
    logProps.put(FlushMessagesProp, kafkaConfig.logFlushIntervalMessages)
    logProps.put(FlushMsProp, kafkaConfig.logFlushIntervalMs)
    logProps.put(RetentionBytesProp, kafkaConfig.logRetentionBytes)
    logProps.put(RetentionMsProp, kafkaConfig.logRetentionTimeMillis: java.lang.Long)
    logProps.put(MaxMessageBytesProp, kafkaConfig.messageMaxBytes)
    logProps.put(IndexIntervalBytesProp, kafkaConfig.logIndexIntervalBytes)
    logProps.put(DeleteRetentionMsProp, kafkaConfig.logCleanerDeleteRetentionMs)
    logProps.put(MinCompactionLagMsProp, kafkaConfig.logCleanerMinCompactionLagMs)
    logProps.put(MaxCompactionLagMsProp, kafkaConfig.logCleanerMaxCompactionLagMs)
    logProps.put(FileDeleteDelayMsProp, kafkaConfig.logDeleteDelayMs)
    logProps.put(MinCleanableDirtyRatioProp, kafkaConfig.logCleanerMinCleanRatio)
    logProps.put(CleanupPolicyProp, kafkaConfig.logCleanupPolicy)
    logProps.put(MinInSyncReplicasProp, kafkaConfig.minInSyncReplicas)
    logProps.put(CompressionTypeProp, kafkaConfig.compressionType)
    logProps.put(UncleanLeaderElectionEnableProp, kafkaConfig.uncleanLeaderElectionEnable)
    logProps.put(PreAllocateEnableProp, kafkaConfig.logPreAllocateEnable)
    logProps.put(MessageFormatVersionProp, kafkaConfig.logMessageFormatVersion.version)
    logProps.put(MessageTimestampTypeProp, kafkaConfig.logMessageTimestampType.name)
    logProps.put(MessageTimestampDifferenceMaxMsProp, kafkaConfig.logMessageTimestampDifferenceMaxMs: java.lang.Long)
    logProps.put(MessageDownConversionEnableProp, kafkaConfig.logMessageDownConversionEnable: java.lang.Boolean)
    logProps
  }

  def initializeMetrics(
    config: KafkaConfig,
    time: Time,
    clusterId: String
  ): Metrics = {
    val jmxReporter = new JmxReporter()
    jmxReporter.configure(config.originals)

    val reporters = new java.util.ArrayList[MetricsReporter]
    reporters.add(jmxReporter)

    val metricConfig = buildMetricsConfig(config)
    val metricsContext = createKafkaMetricsContext(clusterId, config)
    new Metrics(metricConfig, reporters, time, true, metricsContext)
  }

  def buildMetricsConfig(
    kafkaConfig: KafkaConfig
  ): MetricConfig = {
    new MetricConfig()
      .samples(kafkaConfig.metricNumSamples)
      .recordLevel(Sensor.RecordingLevel.forName(kafkaConfig.metricRecordingLevel))
      .timeWindow(kafkaConfig.metricSampleWindowMs, TimeUnit.MILLISECONDS)
  }

  val MetricsPrefix: String = "kafka.server"
  private val ClusterIdLabel: String = "kafka.cluster.id"
  private val BrokerIdLabel: String = "kafka.broker.id"

  private[server] def createKafkaMetricsContext(
    clusterId: String,
    config: KafkaConfig
  ): KafkaMetricsContext = {
    val contextLabels = new java.util.HashMap[String, Object]
    contextLabels.put(ClusterIdLabel, clusterId)
    contextLabels.put(BrokerIdLabel, config.brokerId.toString)
    contextLabels.putAll(config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX))
    val metricsContext = new KafkaMetricsContext(MetricsPrefix, contextLabels)
    metricsContext
  }
}
