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

import java.io.File
import java.util.concurrent.CompletableFuture

import kafka.common.{InconsistentNodeIdException, KafkaException}
import kafka.log.Log
import kafka.metrics.{KafkaMetricsReporter, KafkaYammerMetrics}
import kafka.raft.KafkaRaftManager
import kafka.server.KafkaRaftServer.{BrokerRole, ControllerRole}
import kafka.utils.{CoreUtils, Logging, Mx4jLoader, VerifiableProperties}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.utils.{AppInfoParser, Time}
import org.apache.kafka.metadata.ApiMessageAndVersion
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.raft.metadata.{MetaLogRaftShim, MetadataRecordSerde}

import scala.collection.Seq

/**
 * This class implements the KRaft (Kafka Raft) mode server which relies
 * on a KRaft quorum for maintaining cluster metadata. It is responsible for
 * constructing the controller and/or broker based on the `process.roles`
 * configuration and for managing their basic lifecycle (startup and shutdown).
 *
 * Note that this server is a work in progress and we are releasing it as
 * early access in 2.8.0.
 */
class KafkaRaftServer(
  config: KafkaConfig,
  time: Time,
  threadNamePrefix: Option[String]
) extends Server with Logging {

  KafkaMetricsReporter.startReporters(VerifiableProperties(config.originals))
  KafkaYammerMetrics.INSTANCE.configure(config.originals)

  private val (metaProps, offlineDirs) = KafkaRaftServer.initializeLogDirs(config)

  private val metrics = Server.initializeMetrics(
    config,
    time,
    metaProps.clusterId.toString
  )

  private val controllerQuorumVotersFuture = CompletableFuture.completedFuture(
    RaftConfig.parseVoterConnections(config.quorumVoters))

  private val raftManager = new KafkaRaftManager[ApiMessageAndVersion](
    metaProps,
    config,
    new MetadataRecordSerde,
    KafkaRaftServer.MetadataPartition,
    KafkaRaftServer.MetadataTopicId,
    time,
    metrics,
    threadNamePrefix,
    controllerQuorumVotersFuture
  )

  private val metaLogShim = new MetaLogRaftShim(raftManager.kafkaRaftClient, config.nodeId)

  private val broker: Option[BrokerServer] = if (config.processRoles.contains(BrokerRole)) {
    Some(new BrokerServer(
      config,
      metaProps,
      metaLogShim,
      time,
      metrics,
      threadNamePrefix,
      offlineDirs,
      controllerQuorumVotersFuture,
      Server.SUPPORTED_FEATURES
    ))
  } else {
    None
  }

  private val controller: Option[ControllerServer] = if (config.processRoles.contains(ControllerRole)) {
    Some(new ControllerServer(
      metaProps,
      config,
      metaLogShim,
      raftManager,
      time,
      metrics,
      threadNamePrefix,
      controllerQuorumVotersFuture
    ))
  } else {
    None
  }

  override def startup(): Unit = {
    Mx4jLoader.maybeLoad()
    raftManager.startup()
    controller.foreach(_.startup())
    broker.foreach(_.startup())
    AppInfoParser.registerAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
    info(KafkaBroker.STARTED_MESSAGE)
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
  val MetadataTopicId = Uuid.METADATA_TOPIC_ID

  sealed trait ProcessRole
  case object BrokerRole extends ProcessRole
  case object ControllerRole extends ProcessRole

  /**
   * Initialize the configured log directories, including both [[KafkaConfig.MetadataLogDirProp]]
   * and [[KafkaConfig.LogDirProp]]. This method performs basic validation to ensure that all
   * directories are accessible and have been initialized with consistent `meta.properties`.
   *
   * @param config The process configuration
   * @return A tuple containing the loaded meta properties (which are guaranteed to
   *         be consistent across all log dirs) and the offline directories
   */
  def initializeLogDirs(config: KafkaConfig): (MetaProperties, Seq[String]) = {
    val logDirs = (config.logDirs.toSet + config.metadataLogDir).toSeq
    val (rawMetaProperties, offlineDirs) = BrokerMetadataCheckpoint.
      getBrokerMetadataAndOfflineDirs(logDirs, ignoreMissing = false)

    if (offlineDirs.contains(config.metadataLogDir)) {
      throw new KafkaException("Cannot start server since `meta.properties` could not be " +
        s"loaded from ${config.metadataLogDir}")
    }

    val metadataPartitionDirName = Log.logDirName(MetadataPartition)
    val onlineNonMetadataDirs = logDirs.diff(offlineDirs :+ config.metadataLogDir)
    onlineNonMetadataDirs.foreach { logDir =>
      val metadataDir = new File(logDir, metadataPartitionDirName)
      if (metadataDir.exists) {
        throw new KafkaException(s"Found unexpected metadata location in data directory `$metadataDir` " +
          s"(the configured metadata directory is ${config.metadataLogDir}).")
      }
    }

    val metaProperties = MetaProperties.parse(rawMetaProperties)
    if (config.nodeId != metaProperties.nodeId) {
      throw new InconsistentNodeIdException(
        s"Configured node.id `${config.nodeId}` doesn't match stored node.id `${metaProperties.nodeId}' in " +
          "meta.properties. If you moved your data, make sure your configured controller.id matches. " +
          "If you intend to create a new broker, you should remove all data in your data directories (log.dirs).")
    }

    (metaProperties, offlineDirs.toSeq)
  }

}
