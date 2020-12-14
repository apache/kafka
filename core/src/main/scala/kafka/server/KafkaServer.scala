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

import java.util
import java.util.concurrent.CompletableFuture

import kafka.metrics.{KafkaMetricsReporter, KafkaYammerMetrics}
import kafka.raft.RaftManager
import kafka.server.KafkaServer.{BrokerRole, ControllerRole}
import kafka.utils.{Logging, Mx4jLoader}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, MetricsReporter}
import org.apache.kafka.common.utils.{AppInfoParser, Time}

import scala.jdk.CollectionConverters._

trait KafkaServer {
  def startup(): Unit
  def shutdown(): Unit
  def awaitShutdown(): Unit

  def initializeMetrics(
    config: KafkaConfig,
    time: Time,
    clusterId: String
  ): Metrics = {
    KafkaYammerMetrics.INSTANCE.configure(config.originals)

    val jmxReporter = new JmxReporter()
    jmxReporter.configure(config.originals)

    val reporters = new util.ArrayList[MetricsReporter]
    reporters.add(jmxReporter)

    val metricConfig = KafkaBroker.metricConfig(config)
    val metricsContext = KafkaBroker.createKafkaMetricsContext(clusterId, config)
    new Metrics(metricConfig, reporters, time, true, metricsContext)
  }
}

class LegacyServer(
  config: KafkaConfig,
  time: Time,
  threadNamePrefix: Option[String],
  kafkaMetricsReporters: Seq[KafkaMetricsReporter]
) extends KafkaServer with Logging {

  val broker: LegacyBroker = new LegacyBroker(
    config,
    time,
    threadNamePrefix,
    kafkaMetricsReporters
  )

  override def startup(): Unit = {
    broker.startup()
  }

  override def shutdown(): Unit = {
    broker.shutdown()
  }

  override def awaitShutdown(): Unit = {
    broker.awaitShutdown()
  }
}

class Kip500Server(
  config: KafkaConfig,
  time: Time,
  threadNamePrefix: Option[String],
  kafkaMetricsReporters: Seq[KafkaMetricsReporter]
) extends KafkaServer with Logging {

  private val roles = config.processRoles

  if (roles.distinct.length != roles.size) {
    throw new ConfigException(s"Duplicate role names found in roles config $roles")
  }

  if (config.controllerConnect == null || config.controllerConnect.isEmpty) {
    throw new ConfigException(s"You must specify a value for ${KafkaConfig.ControllerConnectProp}")
  }

  private val (metaProps, offlineDirs) = loadMetaProperties()
  private val metrics = configureMetrics(metaProps)

  AppInfoParser.registerAppInfo(KafkaBroker.metricsPrefix,
    config.controllerId.toString, metrics, time.milliseconds())
  KafkaBroker.notifyClusterListeners(metaProps.clusterId.toString,
    kafkaMetricsReporters ++ metrics.reporters.asScala)

  private val metadataPartition = new TopicPartition("__cluster_metadata", 0)
  private val raftManager = new RaftManager(metaProps, metadataPartition, config, time, metrics)

  val broker: Option[Kip500Broker] = if (roles.contains(BrokerRole)) {
    Some(new Kip500Broker(
      config,
      metaProps,
      raftManager.metaLogManager,
      time,
      metrics,
      threadNamePrefix,
      offlineDirs
    ))
  } else {
    None
  }

  val controller: Option[Kip500Controller] = if (roles.contains(ControllerRole)) {
    Some(new Kip500Controller(
      metaProps,
      config,
      raftManager.metaLogManager,
      Some(raftManager),
      time,
      metrics,
      threadNamePrefix,
      CompletableFuture.completedFuture(config.controllerConnect)
    ))
  } else {
    None
  }

  private def configureMetrics(metaProps: MetaProperties): Metrics = {
    KafkaYammerMetrics.INSTANCE.configure(config.originals)
    val jmxReporter = new JmxReporter()
    jmxReporter.configure(config.originals)

    val reporters = new util.ArrayList[MetricsReporter]
    reporters.add(jmxReporter)

    val metricConfig = KafkaBroker.metricConfig(config)
    val metricsContext = KafkaBroker.createKafkaMetricsContext(metaProps, config)

    new Metrics(metricConfig, reporters, time, true, metricsContext)
  }

  private def loadMetaProperties(): (MetaProperties, Seq[String]) = {
    val logDirs = config.logDirs ++ Seq(config.metadataLogDir)
    val (rawMetaProperties, offlineDirs) = BrokerMetadataCheckpoint.getBrokerMetadataAndOfflineDirs(logDirs)

    if (offlineDirs.contains(config.metadataLogDir)) {
      throw new RuntimeException("Cannot start server since `meta.properties` could not be " +
        s"loaded from ${config.metadataLogDir}")
    }

    (MetaProperties.parse(rawMetaProperties, roles.toSet), offlineDirs.toSeq)
  }

  def startup(): Unit = {
    Mx4jLoader.maybeLoad()
    raftManager.startup()
    controller.foreach(_.startup())
    broker.foreach(_.startup())
  }

  def shutdown(): Unit = {
    broker.foreach(_.shutdown())
    raftManager.shutdown()
    controller.foreach(_.shutdown())
  }

  def awaitShutdown(): Unit = {
    broker.foreach(_.awaitShutdown())
    controller.foreach(_.awaitShutdown())
  }

}

object KafkaServer {
  def apply(
    config: KafkaConfig,
    time: Time = Time.SYSTEM,
    threadNamePrefix: Option[String] = None,
    kafkaMetricsReporters: Seq[KafkaMetricsReporter] = List()
  ): KafkaServer = {
    val roles = config.processRoles
    if (roles == null || roles.isEmpty) {
      new LegacyServer(config, time, threadNamePrefix, kafkaMetricsReporters)
    } else {
      new Kip500Server(config, time, threadNamePrefix, kafkaMetricsReporters)
    }
  }

  sealed trait ProcessStatus
  case object SHUTDOWN extends ProcessStatus
  case object STARTING extends ProcessStatus
  case object STARTED extends ProcessStatus
  case object SHUTTING_DOWN extends ProcessStatus

  sealed trait ProcessRole
  case object BrokerRole extends ProcessRole
  case object ControllerRole extends ProcessRole
}
