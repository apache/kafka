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
import kafka.log.UnifiedLog
import kafka.utils.{CoreUtils, Logging, Mx4jLoader}
import org.apache.kafka.common.config.{ConfigDef, ConfigResource}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.utils.{AppInfoParser, Time}
import org.apache.kafka.common.{KafkaException, Uuid}
import org.apache.kafka.metadata.KafkaConfigSchema
import org.apache.kafka.metadata.bootstrap.{BootstrapDirectory, BootstrapMetadata}
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.VerificationFlag.{REQUIRE_AT_LEAST_ONE_VALID, REQUIRE_METADATA_LOG_DIR}
import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble}
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.{ProcessRole, ServerSocketFactory}
import org.apache.kafka.server.config.ServerTopicConfigSynonyms
import org.apache.kafka.storage.internals.log.LogConfig
import org.slf4j.Logger

import java.util
import java.util.{Optional, OptionalInt}
import scala.jdk.CollectionConverters._

/**
 * This class implements the KRaft (Kafka Raft) mode server which relies
 * on a KRaft quorum for maintaining cluster metadata. It is responsible for
 * constructing the controller and/or broker based on the `process.roles`
 * configuration and for managing their basic lifecycle (startup and shutdown).
 *
 */
class KafkaRaftServer(
  config: KafkaConfig,
  time: Time,
) extends Server with Logging {

  this.logIdent = s"[KafkaRaftServer nodeId=${config.nodeId}] "

  private val (metaPropsEnsemble, bootstrapMetadata) =
    KafkaRaftServer.initializeLogDirs(config, this.logger.underlying, this.logIdent)

  private val metrics = Server.initializeMetrics(
    config,
    time,
    metaPropsEnsemble.clusterId().get()
  )

  private val sharedServer = new SharedServer(
    config,
    metaPropsEnsemble,
    time,
    metrics,
    CompletableFuture.completedFuture(QuorumConfig.parseVoterConnections(config.quorumConfig.voters)),
    QuorumConfig.parseBootstrapServers(config.quorumConfig.bootstrapServers),
    new StandardFaultHandlerFactory(),
    ServerSocketFactory.INSTANCE,
  )

  private val broker: Option[BrokerServer] = if (config.processRoles.contains(ProcessRole.BrokerRole)) {
    Some(new BrokerServer(sharedServer))
  } else {
    None
  }

  private val controller: Option[ControllerServer] = if (config.processRoles.contains(ProcessRole.ControllerRole)) {
    Some(new ControllerServer(
      sharedServer,
      KafkaRaftServer.configSchema,
      bootstrapMetadata,
    ))
  } else {
    None
  }

  override def startup(): Unit = {
    Mx4jLoader.maybeLoad()
    // Controller component must be started before the broker component so that
    // the controller endpoints are passed to the KRaft manager
    controller.foreach(_.startup())
    broker.foreach(_.startup())
    AppInfoParser.registerAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
    info(KafkaBroker.STARTED_MESSAGE)
  }

  override def shutdown(): Unit = {
    // In combined mode, we want to shut down the broker first, since the controller may be
    // needed for controlled shutdown. Additionally, the controller shutdown process currently
    // stops the raft client early on, which would disrupt broker shutdown.
    broker.foreach(_.shutdown())
    controller.foreach(_.shutdown())
    CoreUtils.swallow(AppInfoParser.unregisterAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics), this)
  }

  override def awaitShutdown(): Unit = {
    broker.foreach(_.awaitShutdown())
    controller.foreach(_.awaitShutdown())
  }
}

object KafkaRaftServer {
  val MetadataTopic = Topic.CLUSTER_METADATA_TOPIC_NAME
  val MetadataPartition = Topic.CLUSTER_METADATA_TOPIC_PARTITION
  val MetadataTopicId = Uuid.METADATA_TOPIC_ID

  /**
   * Initialize the configured log directories, including both [[KRaftConfigs.MetadataLogDirProp]]
   * and [[KafkaConfig.LOG_DIR_PROP]]. This method performs basic validation to ensure that all
   * directories are accessible and have been initialized with consistent `meta.properties`.
   *
   * @param config The process configuration
   * @return A tuple containing the loaded meta properties (which are guaranteed to
   *         be consistent across all log dirs) and the offline directories
   */
  def initializeLogDirs(
    config: KafkaConfig,
    log: Logger,
    logPrefix: String
  ): (MetaPropertiesEnsemble, BootstrapMetadata) = {
    // Load and verify the original ensemble.
    val loader = new MetaPropertiesEnsemble.Loader()
    loader.addMetadataLogDir(config.metadataLogDir)
          .addLogDirs(config.logDirs.asJava)
    val initialMetaPropsEnsemble = loader.load()
    val verificationFlags = util.EnumSet.of(REQUIRE_AT_LEAST_ONE_VALID, REQUIRE_METADATA_LOG_DIR)
    initialMetaPropsEnsemble.verify(Optional.empty(), OptionalInt.of(config.nodeId), verificationFlags)

    // Check that the __cluster_metadata-0 topic does not appear outside the metadata directory.
    val metadataPartitionDirName = UnifiedLog.logDirName(MetadataPartition)
    initialMetaPropsEnsemble.logDirProps().keySet().forEach(logDir => {
      if (!logDir.equals(config.metadataLogDir)) {
        val clusterMetadataTopic = new File(logDir, metadataPartitionDirName)
        if (clusterMetadataTopic.exists) {
          throw new KafkaException(s"Found unexpected metadata location in data directory `$clusterMetadataTopic` " +
            s"(the configured metadata directory is ${config.metadataLogDir}).")
        }
      }
    })

    // Set directory IDs on all directories. Rewrite the files if needed.
    val metaPropsEnsemble = {
      val copier = new MetaPropertiesEnsemble.Copier(initialMetaPropsEnsemble)
      initialMetaPropsEnsemble.nonFailedDirectoryProps().forEachRemaining(e => {
        val logDir = e.getKey
        val metaProps = e.getValue
        if (!metaProps.isPresent()) {
          throw new RuntimeException(s"No `meta.properties` found in $logDir (have you run `kafka-storage.sh` " +
            "to format the directory?)")
        }
        if (!metaProps.get().nodeId().isPresent()) {
          throw new RuntimeException(s"Error: node ID not found in $logDir")
        }
        if (!metaProps.get().clusterId().isPresent()) {
          throw new RuntimeException(s"Error: cluster ID not found in $logDir")
        }
        val builder = new MetaProperties.Builder(metaProps.get())
        if (!builder.directoryId().isPresent()) {
          builder.setDirectoryId(copier.generateValidDirectoryId())
        }
        copier.setLogDirProps(logDir, builder.build())
        copier.setPreWriteHandler((logDir, _, _) => {
          log.info("{}Rewriting {}{}meta.properties", logPrefix, logDir, File.separator)
        })
      })
      copier.writeLogDirChanges()
      copier.copy()
    }

    // Load the BootstrapMetadata.
    val bootstrapDirectory = new BootstrapDirectory(config.metadataLogDir,
      Optional.ofNullable(config.interBrokerProtocolVersionString))
    val bootstrapMetadata = bootstrapDirectory.read()
    (metaPropsEnsemble, bootstrapMetadata)
  }

  val configSchema = new KafkaConfigSchema(Map(
    ConfigResource.Type.BROKER -> new ConfigDef(KafkaConfig.configDef),
    ConfigResource.Type.TOPIC -> LogConfig.configDefCopy,
  ).asJava, ServerTopicConfigSynonyms.ALL_TOPIC_CONFIG_SYNONYMS)
}
