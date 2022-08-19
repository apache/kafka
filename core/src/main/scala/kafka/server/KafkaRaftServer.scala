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
import kafka.common.InconsistentNodeIdException
import kafka.log.{LogConfig, UnifiedLog}
import kafka.metrics.KafkaMetricsReporter
import kafka.raft.KafkaRaftManager
import kafka.server.KafkaRaftServer.{BrokerRole, ControllerRole}
import kafka.server.metadata.BrokerServerMetrics
import kafka.utils.{CoreUtils, Logging, Mx4jLoader, VerifiableProperties}
import org.apache.kafka.common.config.{ConfigDef, ConfigResource}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.utils.{AppInfoParser, Time}
import org.apache.kafka.common.{KafkaException, Uuid}
import org.apache.kafka.controller.{BootstrapMetadata, QuorumControllerMetrics}
import org.apache.kafka.metadata.{KafkaConfigSchema, MetadataRecordSerde}
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion}
import org.apache.kafka.server.fault.{LoggingFaultHandler, ProcessExitingFaultHandler}
import org.apache.kafka.server.metrics.KafkaYammerMetrics

import java.nio.file.Paths
import scala.collection.Seq
import scala.compat.java8.FunctionConverters.asJavaSupplier
import scala.jdk.CollectionConverters._

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

  this.logIdent = s"[KafkaRaftServer nodeId=${config.nodeId}] "
  KafkaMetricsReporter.startReporters(VerifiableProperties(config.originals))
  KafkaYammerMetrics.INSTANCE.configure(config.originals)

  private val (metaProps, bootstrapMetadata, offlineDirs) = KafkaRaftServer.initializeLogDirs(config)

  private val metrics = Server.initializeMetrics(
    config,
    time,
    metaProps.clusterId
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

  private val broker: Option[BrokerServer] = if (config.processRoles.contains(BrokerRole)) {
    val brokerMetrics = BrokerServerMetrics(metrics)
    val fatalFaultHandler = new ProcessExitingFaultHandler()
    val metadataLoadingFaultHandler = new LoggingFaultHandler("metadata loading",
        () => brokerMetrics.metadataLoadErrorCount.getAndIncrement())
    val metadataApplyingFaultHandler = new LoggingFaultHandler("metadata application",
      () => brokerMetrics.metadataApplyErrorCount.getAndIncrement())
    Some(new BrokerServer(
      config,
      metaProps,
      raftManager,
      time,
      metrics,
      brokerMetrics,
      threadNamePrefix,
      offlineDirs,
      controllerQuorumVotersFuture,
      fatalFaultHandler,
      metadataLoadingFaultHandler,
      metadataApplyingFaultHandler
    ))
  } else {
    None
  }

  private val controller: Option[ControllerServer] = if (config.processRoles.contains(ControllerRole)) {
    val controllerMetrics = new QuorumControllerMetrics(KafkaYammerMetrics.defaultRegistry(), time)
    val metadataFaultHandler = new LoggingFaultHandler("controller metadata",
      () => controllerMetrics.incrementMetadataErrorCount())
    val fatalFaultHandler = new ProcessExitingFaultHandler()
    Some(new ControllerServer(
      metaProps,
      config,
      raftManager,
      time,
      metrics,
      controllerMetrics,
      threadNamePrefix,
      controllerQuorumVotersFuture,
      KafkaRaftServer.configSchema,
      raftManager.apiVersions,
      bootstrapMetadata,
      metadataFaultHandler,
      fatalFaultHandler
    ))
  } else {
    None
  }

  override def startup(): Unit = {
    Mx4jLoader.maybeLoad()
    // Note that we startup `RaftManager` first so that the controller and broker
    // can register listeners during initialization.
    raftManager.startup()
    controller.foreach(_.startup())
    broker.foreach(_.startup())
    AppInfoParser.registerAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
    info(KafkaBroker.STARTED_MESSAGE)
  }

  override def shutdown(): Unit = {
    broker.foreach(_.shutdown())
    // The order of shutdown for `RaftManager` and `ControllerServer` is backwards
    // compared to `startup()`. This is because the `SocketServer` implementation that
    // we rely on to receive requests is owned by `ControllerServer`, so we need it
    // to stick around until graceful shutdown of `RaftManager` can be completed.
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
  val MetadataTopic = Topic.METADATA_TOPIC_NAME
  val MetadataPartition = Topic.METADATA_TOPIC_PARTITION
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
  def initializeLogDirs(config: KafkaConfig): (MetaProperties, BootstrapMetadata, Seq[String]) = {
    val logDirs = (config.logDirs.toSet + config.metadataLogDir).toSeq
    val (rawMetaProperties, offlineDirs) = BrokerMetadataCheckpoint.
      getBrokerMetadataAndOfflineDirs(logDirs, ignoreMissing = false)

    if (offlineDirs.contains(config.metadataLogDir)) {
      throw new KafkaException("Cannot start server since `meta.properties` could not be " +
        s"loaded from ${config.metadataLogDir}")
    }

    val metadataPartitionDirName = UnifiedLog.logDirName(MetadataPartition)
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

    // Load the bootstrap metadata file. In the case of an upgrade from older KRaft where there is no bootstrap metadata,
    // read the IBP from config in order to bootstrap the equivalent metadata version.
    def getUserDefinedIBPVersionOrThrow(): MetadataVersion = {
      if (config.originals.containsKey(KafkaConfig.InterBrokerProtocolVersionProp)) {
        MetadataVersion.fromVersionString(config.interBrokerProtocolVersionString)
      } else {
        throw new KafkaException(s"Cannot upgrade from KRaft version prior to 3.3 without first setting ${KafkaConfig.InterBrokerProtocolVersionProp} on each broker.")
      }
    }
    val bootstrapMetadata = BootstrapMetadata.load(Paths.get(config.metadataLogDir), asJavaSupplier(() => getUserDefinedIBPVersionOrThrow()))

    (metaProperties, bootstrapMetadata, offlineDirs.toSeq)
  }

  val configSchema = new KafkaConfigSchema(Map(
    ConfigResource.Type.BROKER -> new ConfigDef(KafkaConfig.configDef),
    ConfigResource.Type.TOPIC -> LogConfig.configDefCopy,
  ).asJava, LogConfig.AllTopicConfigSynonyms)
}
