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

import kafka.common.{InconsistentBrokerIdException, InconsistentControllerIdException, KafkaException}
import kafka.metrics.{KafkaMetricsReporter, KafkaYammerMetrics}
import kafka.raft.KafkaRaftManager
import kafka.server.KafkaRaftServer.{BrokerRole, ControllerRole}
import kafka.utils.{CoreUtils, Logging, Mx4jLoader, VerifiableProperties}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.{AppInfoParser, Time}
import org.apache.kafka.raft.internals.StringSerde

/**
 * This class implements the KIP-500 server which relies on a self-managed
 * Raft quorum for maintaining cluster metadata. It is responsible for
 * constructing the controller and/or broker based on the `process.roles`
 * configuration and for managing their basic lifecycle (startup and shutdown).
 *
 * Note that this server is a work in progress and relies on stubbed
 * implementations of the controller [[ControllerServer]] and broker
 * [[BrokerServer]].
 */
class KafkaRaftServer(
  config: KafkaConfig,
  time: Time,
  threadNamePrefix: Option[String]
) extends Server with Logging {

  KafkaMetricsReporter.startReporters(VerifiableProperties(config.originals))
  KafkaYammerMetrics.INSTANCE.configure(config.originals)

  private val (metaProps, _) = KafkaRaftServer.loadMetaProperties(config)

  private val metrics = Server.initializeMetrics(
    config,
    time,
    metaProps
  )

  private val raftManager = new KafkaRaftManager(
    metaProps,
    config,
    new StringSerde,
    KafkaRaftServer.MetadataPartition,
    time,
    metrics,
    threadNamePrefix
  )

  private val broker: Option[BrokerServer] = if (config.processRoles.contains(BrokerRole)) {
    Some(new BrokerServer())
  } else {
    None
  }

  private val controller: Option[ControllerServer] = if (config.processRoles.contains(ControllerRole)) {
    Some(new ControllerServer())
  } else {
    None
  }

  override def startup(): Unit = {
    Mx4jLoader.maybeLoad()
    raftManager.startup()
    controller.foreach(_.startup())
    broker.foreach(_.startup())
    AppInfoParser.registerAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
  }

  override def shutdown(): Unit = {
    broker.foreach(_.shutdown())
    raftManager.shutdown()
    controller.foreach(_.shutdown())
    CoreUtils.swallow(AppInfoParser.unregisterAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics), this)

  }

  override def awaitShutdown(): Unit = {
    broker.foreach(_.awaitShutdown())
    controller.foreach(_.awaitShutdown())
  }

}

object KafkaRaftServer {
  val MetadataTopic = "@metadata"
  val MetadataPartition = new TopicPartition(MetadataTopic, 0)

  sealed trait ProcessRole
  case object BrokerRole extends ProcessRole
  case object ControllerRole extends ProcessRole

  def loadMetaProperties(config: KafkaConfig): (MetaProperties, Seq[String]) = {
    val logDirs = config.logDirs :+ config.metadataLogDir
    val (rawMetaProperties, offlineDirs) = BrokerMetadataCheckpoint.
      getBrokerMetadataAndOfflineDirs(logDirs, ignoreMissing = false)

    if (offlineDirs.contains(config.metadataLogDir)) {
      throw new KafkaException("Cannot start server since `meta.properties` could not be " +
        s"loaded from ${config.metadataLogDir}")
    }

    val metaProperties = MetaProperties.parse(rawMetaProperties, config.processRoles)

    val configuredBrokerId = if (config.brokerId < 0) None else Some(config.brokerId)
    if (configuredBrokerId != metaProperties.brokerId) {
      throw new InconsistentBrokerIdException(
        s"Configured broker.id ${config.brokerId} doesn't match stored broker.id ${metaProperties.brokerId} in " +
          "meta.properties. If you moved your data, make sure your configured broker.id matches. " +
          "If you intend to create a new broker, you should remove all data in your data directories (log.dirs).")
    }

    val configuredControllerId = if (config.controllerId < 0) None else Some(config.controllerId)
    if (configuredControllerId != metaProperties.controllerId) {
      throw new InconsistentControllerIdException(
        s"Configured controller.id ${config.controllerId} doesn't match stored controller.id ${metaProperties.controllerId} in " +
          "meta.properties. If you moved your data, make sure your configured controller.id matches. " +
          "If you intend to create a new broker, you should remove all data in your data directories (log.dirs).")
    }

    (metaProperties, offlineDirs.toSeq)
  }

}
