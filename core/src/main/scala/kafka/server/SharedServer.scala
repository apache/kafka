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
import kafka.server.Server.MetricsPrefix
import kafka.server.metadata.BrokerServerMetrics
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.{AppInfoParser, LogContext, Time}
import org.apache.kafka.controller.QuorumControllerMetrics
import org.apache.kafka.image.loader.MetadataLoader
import org.apache.kafka.image.publisher.{SnapshotEmitter, SnapshotGenerator}
import org.apache.kafka.metadata.MetadataRecordSerde
import org.apache.kafka.raft.RaftConfig.AddressSpec
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.server.fault.{FaultHandler, LoggingFaultHandler, ProcessExitingFaultHandler}
import org.apache.kafka.server.metrics.KafkaYammerMetrics

import java.util
import java.util.Collections
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicReference


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
 * The SharedServer manages the components which are shared between the BrokerServer and
 * ControllerServer. These shared components include the Raft manager, snapshot generator,
 * and metadata loader. A KRaft server running in combined mode as both a broker and a controller
 * will still contain only a single SharedServer instance.
 *
 * The SharedServer will be started as soon as either the broker or the controller needs it,
 * via the appropriate function (startForBroker or startForController). Similarly, it will be
 * stopped as soon as neither the broker nor the controller need it, via stopForBroker or
 * stopForController. One way of thinking about this is that both the broker and the controller
 * could hold a "reference" to this class, and we don't truly stop it until both have dropped
 * their reference. We opted to use two booleans here rather than a reference count in order to
 * make debugging easier and reduce the chance of resource leaks.
 */
class SharedServer(
  private val sharedServerConfig: KafkaConfig,
  val metaProps: MetaProperties,
  val time: Time,
  private val _metrics: Metrics,
  val threadNamePrefix: Option[String],
  val controllerQuorumVotersFuture: CompletableFuture[util.Map[Integer, AddressSpec]],
  val faultHandlerFactory: FaultHandlerFactory
) extends Logging {
  private val logContext: LogContext = new LogContext(s"[SharedServer id=${sharedServerConfig.nodeId}] ")
  this.logIdent = logContext.logPrefix
  private var started = false
  private var usedByBroker: Boolean = false
  private var usedByController: Boolean = false
  val brokerConfig = new KafkaConfig(sharedServerConfig.props, false, None)
  val controllerConfig = new KafkaConfig(sharedServerConfig.props, false, None)
  @volatile var metrics: Metrics = _metrics
  @volatile var raftManager: KafkaRaftManager[ApiMessageAndVersion] = _
  @volatile var brokerMetrics: BrokerServerMetrics = _
  @volatile var controllerMetrics: QuorumControllerMetrics = _
  @volatile var loader: MetadataLoader = _
  val snapshotsDiabledReason = new AtomicReference[String](null)
  @volatile var snapshotEmitter: SnapshotEmitter = _
  @volatile var snapshotGenerator: SnapshotGenerator = _

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
  def metadataLoaderFaultHandler: FaultHandler = faultHandlerFactory.build(
    name = "metadata loading",
    fatal = sharedServerConfig.processRoles.contains(ControllerRole),
    action = () => SharedServer.this.synchronized {
      if (brokerMetrics != null) brokerMetrics.metadataLoadErrorCount.getAndIncrement()
      if (controllerMetrics != null) controllerMetrics.incrementMetadataErrorCount()
      snapshotsDiabledReason.compareAndSet(null, "metadata loading fault")
    })

  /**
   * The fault handler to use when the initial broker metadata load fails.
   */
  def initialBrokerMetadataLoadFaultHandler: FaultHandler = faultHandlerFactory.build(
    name = "initial broker metadata loading",
    fatal = true,
    action = () => SharedServer.this.synchronized {
      if (brokerMetrics != null) brokerMetrics.metadataApplyErrorCount.getAndIncrement()
      if (controllerMetrics != null) controllerMetrics.incrementMetadataErrorCount()
      snapshotsDiabledReason.compareAndSet(null, "initial broker metadata loading fault")
    })

  /**
   * The fault handler to use when the QuorumController experiences a fault.
   */
  def quorumControllerFaultHandler: FaultHandler = faultHandlerFactory.build(
    name = "quorum controller",
    fatal = true,
    action = () => SharedServer.this.synchronized {
      if (controllerMetrics != null) controllerMetrics.incrementMetadataErrorCount()
      snapshotsDiabledReason.compareAndSet(null, "quorum controller fault")
    })

  /**
   * The fault handler to use when metadata cannot be published.
   */
  def metadataPublishingFaultHandler: FaultHandler = faultHandlerFactory.build(
    name = "metadata publishing",
    fatal = false,
    action = () => SharedServer.this.synchronized {
      if (brokerMetrics != null) brokerMetrics.metadataApplyErrorCount.getAndIncrement()
      if (controllerMetrics != null) controllerMetrics.incrementMetadataErrorCount()
      // Note: snapshot generation does not need to be disabled for a publishing fault.
    })

  private def start(): Unit = synchronized {
    if (started) {
      debug("SharedServer has already been started.")
    } else {
      info("Starting SharedServer")
      try {
        if (metrics == null) {
          // Recreate the metrics object if we're restarting a stopped SharedServer object.
          // This is only done in tests.
          metrics = new Metrics()
        }
        sharedServerConfig.dynamicConfig.initialize(zkClientOpt = None)

        if (sharedServerConfig.processRoles.contains(BrokerRole)) {
          brokerMetrics = BrokerServerMetrics(metrics)
        }
        if (sharedServerConfig.processRoles.contains(ControllerRole)) {
          controllerMetrics = new QuorumControllerMetrics(KafkaYammerMetrics.defaultRegistry(), time)
        }
        raftManager = new KafkaRaftManager[ApiMessageAndVersion](
          metaProps,
          sharedServerConfig,
          new MetadataRecordSerde,
          KafkaRaftServer.MetadataPartition,
          KafkaRaftServer.MetadataTopicId,
          time,
          metrics,
          threadNamePrefix,
          controllerQuorumVotersFuture)
        raftManager.startup()

        if (sharedServerConfig.processRoles.contains(ControllerRole)) {
          val loaderBuilder = new MetadataLoader.Builder().
            setNodeId(metaProps.nodeId).
            setTime(time).
            setThreadNamePrefix(threadNamePrefix.getOrElse("")).
            setFaultHandler(metadataLoaderFaultHandler).
            setHighWaterMarkAccessor(() => raftManager.client.highWatermark())
          if (brokerMetrics != null) {
            loaderBuilder.setMetadataLoaderMetrics(brokerMetrics)
          }
          loader = loaderBuilder.build()
          snapshotEmitter = new SnapshotEmitter.Builder().
            setNodeId(metaProps.nodeId).
            setRaftClient(raftManager.client).
            build()
          snapshotGenerator = new SnapshotGenerator.Builder(snapshotEmitter).
            setNodeId(metaProps.nodeId).
            setTime(time).
            setFaultHandler(metadataPublishingFaultHandler).
            setMaxBytesSinceLastSnapshot(sharedServerConfig.metadataSnapshotMaxNewRecordBytes).
            setMaxTimeSinceLastSnapshotNs(TimeUnit.MILLISECONDS.toNanos(sharedServerConfig.metadataSnapshotMaxIntervalMs)).
            setDisabledReason(snapshotsDiabledReason).
            build()
          raftManager.register(loader)
          try {
            loader.installPublishers(Collections.singletonList(snapshotGenerator))
          } catch {
            case t: Throwable => {
              error("Unable to install metadata publishers", t)
              throw new RuntimeException("Unable to install metadata publishers.", t)
            }
          }
        }
        debug("Completed SharedServer startup.")
        started = true
      } catch {
        case e: Throwable => {
          error("Got exception while starting SharedServer", e)
          stop()
        }
      }
    }
  }

  def ensureNotRaftLeader(): Unit = synchronized {
    // Ideally, this would just resign our leadership, if we had it. But we don't have an API in
    // RaftManager for that yet, so shut down the RaftManager.
    if (raftManager != null) {
      CoreUtils.swallow(raftManager.shutdown(), this)
      raftManager = null
    }
  }

  private def stop(): Unit = synchronized {
    if (!started) {
      debug("SharedServer is not running.")
    } else {
      info("Stopping SharedServer")
      if (loader != null) {
        CoreUtils.swallow(loader.beginShutdown(), this)
      }
      if (snapshotGenerator != null) {
        CoreUtils.swallow(snapshotGenerator.beginShutdown(), this)
      }
      if (loader != null) {
        CoreUtils.swallow(loader.close(), this)
        loader = null
      }
      if (snapshotGenerator != null) {
        CoreUtils.swallow(snapshotGenerator.close(), this)
        snapshotGenerator = null
      }
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
      if (metrics != null) {
        CoreUtils.swallow(metrics.close(), this)
        metrics = null
      }
      CoreUtils.swallow(AppInfoParser.unregisterAppInfo(MetricsPrefix, sharedServerConfig.nodeId.toString, metrics), this)
      started = false
    }
  }
}
