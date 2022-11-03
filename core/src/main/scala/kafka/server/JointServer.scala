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

import kafka.raft.KafkaRaftManager
import kafka.server.KafkaRaftServer.{BrokerRole, ControllerRole}
import kafka.server.metadata.BrokerServerMetrics
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.controller.QuorumControllerMetrics
import org.apache.kafka.image.loader.MetadataLoader
import org.apache.kafka.image.publisher.{SnapshotEmitter, SnapshotGenerator}
import org.apache.kafka.metadata.MetadataRecordSerde
import org.apache.kafka.raft.RaftConfig.AddressSpec
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.server.fault.{FaultHandler, LoggingFaultHandler, ProcessExitingFaultHandler}
import org.apache.kafka.server.metrics.KafkaYammerMetrics

import java.util
import java.util.concurrent.{CompletableFuture, TimeUnit}


/**
 * Creates a fault handler.
 */
trait FaultHandlerFactory {
  def build(
     name: String,
     fatal: Boolean,
     action: Runnable
  ): FaultHandler
}

/**
 * The standard FaultHandlerFactory which is used when we're not in a junit test.
 */
class StandardFaultHandlerFactory extends FaultHandlerFactory {
  override def build(
    name: String,
    fatal: Boolean,
    action: Runnable
  ): FaultHandler = {
    if (fatal) {
      new ProcessExitingFaultHandler(action)
    } else {
      new LoggingFaultHandler(name, action)
    }
  }
}

/**
 * The JointServer manages the components which are shared between the BrokerServer and
 * ControllerServer. These shared components include the Raft manager, snapshot generator,
 * and metadata loader. A KRaft server running in combined mode as both a broker and a controller
 * will still contain only a single JointServer instance.
 *
 * The JointServer will be started as soon as either the broker or the controller needs it,
 * via the appropriate function (startForBroker or startForController). Similarly, it will be
 * stopped as soon as neither the broker nor the controller need it, via stopForBroker or
 * stopForController. One way of thinking about this is that both the broker and the controller
 * could hold a "reference" to this class, and we don't truly stop it until both have dropped
 * their reference. We opted to use two booleans here rather than a reference count in order to
 * make debugging easier and reduce the chance of resource leaks.
 */
class JointServer(
  val config: KafkaConfig,
  val metaProps: MetaProperties,
  val time: Time,
  val metrics: Metrics,
  val threadNamePrefix: Option[String],
  val controllerQuorumVotersFuture: CompletableFuture[util.Map[Integer, AddressSpec]],
  val faultHandlerFactory: FaultHandlerFactory
) extends Logging {
  private val logContext: LogContext = new LogContext(s"[JointServer id=${config.nodeId}] ")
  this.logIdent = logContext.logPrefix

  var usedByBroker: Boolean = false
  var usedByController: Boolean = false
  var raftManager: KafkaRaftManager[ApiMessageAndVersion] = _
  var snapshotEmitter: SnapshotEmitter = _
  var snapshotGenerator: SnapshotGenerator = _
  var brokerMetrics: BrokerServerMetrics = _
  var controllerMetrics: QuorumControllerMetrics = _
  var metadataLoader: MetadataLoader = _

  def isUsed(): Boolean = synchronized {
    usedByController || usedByBroker
  }

  /**
   * The start function called by the broker.
   */
  def startForBroker(): Unit = synchronized {
    if (!isUsed()) {
      start()
    }
    usedByBroker = true
  }

  /**
   * The start function called by the controller.
   */
  def startForController(): Unit = synchronized {
    if (!isUsed()) {
      start()
    }
    usedByController = true
  }

  /**
   * The stop function called by the broker.
   */
  def stopForBroker(): Unit = synchronized {
    if (usedByBroker) {
      usedByBroker = false
      if (!isUsed()) stop()
    }
  }

  /**
   * The stop function called by the controller.
   */
  def stopForController(): Unit = synchronized {
    if (usedByController) {
      usedByController = false
      if (!isUsed()) stop()
    }
  }

  /**
   * The fault handler to use when metadata loading fails.
   */
  def metadataLoaderFaultHandler: FaultHandler = faultHandlerFactory.build("metadata loading",
    config.processRoles.contains(ControllerRole),
    () => {
      if (snapshotGenerator != null) {
        snapshotGenerator.disable("there was a metadata loading error")
      }
      if (brokerMetrics != null) brokerMetrics.metadataLoadErrorCount.getAndIncrement()
      if (controllerMetrics != null) controllerMetrics.incrementMetadataErrorCount()
    })

  /**
   * The fault handler to use when the initial broker metadata load fails.
   */
  def initialBrokerMetadataLoadFaultHandler: FaultHandler = faultHandlerFactory.build("initial metadata loading",
    true,
    () => {
      if (snapshotGenerator != null) {
        snapshotGenerator.disable("there was an initial metadata loading error")
      }
      if (brokerMetrics != null) brokerMetrics.metadataApplyErrorCount.getAndIncrement()
      if (controllerMetrics != null) controllerMetrics.incrementMetadataErrorCount()
    })

  /**
   * The fault handler to use when the QuorumController experiences a fault.
   */
  def quorumControllerFaultHandler: FaultHandler = faultHandlerFactory.build("quorum controller",
    true,
    () => {}
  )

  /**
   * The fault handler to use when metadata cannot be published.
   */
  def metadataPublishingFaultHandler: FaultHandler = faultHandlerFactory.build("metadata publishing",
    false,
    () => {
      if (brokerMetrics != null) brokerMetrics.metadataApplyErrorCount.getAndIncrement()
      if (controllerMetrics != null) controllerMetrics.incrementMetadataErrorCount()
    })

  private def start(): Unit = synchronized {
    info("Starting JointServer")
    try {
      config.dynamicConfig.initialize(zkClientOpt = None)

      if (config.processRoles.contains(BrokerRole)) {
        brokerMetrics = BrokerServerMetrics(metrics)
      }
      if (config.processRoles.contains(ControllerRole)) {
        controllerMetrics = new QuorumControllerMetrics(KafkaYammerMetrics.defaultRegistry(), time)
      }
      raftManager = new KafkaRaftManager[ApiMessageAndVersion](
        metaProps,
        config,
        new MetadataRecordSerde,
        KafkaRaftServer.MetadataPartition,
        KafkaRaftServer.MetadataTopicId,
        time,
        metrics,
        threadNamePrefix,
        controllerQuorumVotersFuture)
      raftManager.startup()
      snapshotEmitter = new SnapshotEmitter.Builder().
        setNodeId(config.nodeId).
        setRaftClient(raftManager.client).
        build()
      snapshotGenerator = new SnapshotGenerator.Builder(snapshotEmitter).
        setTime(time).
        setNodeId(config.nodeId).
        setFaultHandler(new LoggingFaultHandler("snapshot generation", () => { })).
        setMinBytesSinceLastSnapshot(config.metadataSnapshotMaxNewRecordBytes).
        setMaxTimeSinceLastSnapshotNs(TimeUnit.MILLISECONDS.toNanos(config.metadataSnapshotMaxIntervalMsProp)).
        build()
      val loaderBuilder = new MetadataLoader.Builder().
        setTime(time).
        setNodeId(config.nodeId).
        setFaultHandler(metadataLoaderFaultHandler)
      if (config.processRoles.contains(BrokerRole)) {
        // In broker-only mode or combined mode, we want to expose the broker metrics.
        loaderBuilder.setMetadataLoaderMetrics(brokerMetrics)
      }
      metadataLoader = loaderBuilder.build()
      metadataLoader.installPublishers(util.Arrays.asList(snapshotGenerator))
      raftManager.client.register(metadataLoader)
      debug("Completed JointServer startup.")
    } catch {
      case e: Throwable => {
        error("Got exception while starting JointServer", e)
        stop()
      }
    }
  }

  private def stop(): Unit = {
    info("Stopping JointServer")

    // Begin shutting these down in parallel to save time.
    if (metadataLoader != null) metadataLoader.beginShutdown()
    if (snapshotGenerator != null) snapshotGenerator.beginShutdown()

    // Close and null out components in reverse order of construction.
    if (metadataLoader != null) {
      CoreUtils.swallow(metadataLoader.close(), this)
      metadataLoader = null
    }
    if (snapshotGenerator != null) {
      CoreUtils.swallow(snapshotGenerator.close(), this)
      snapshotGenerator = null
    }
    snapshotEmitter = null
    if (raftManager != null) {
      CoreUtils.swallow(raftManager.shutdown(), this)
      raftManager = null
    }
    if (controllerMetrics != null) {
      CoreUtils.swallow(controllerMetrics.close(), this)
      controllerMetrics = null
    }
    if (brokerMetrics != null) {
      CoreUtils.swallow(brokerMetrics.close(), this)
      brokerMetrics = null
    }
    // Clear all reconfigurable instances stored in DynamicBrokerConfig
    config.dynamicConfig.clear()
  }
}
