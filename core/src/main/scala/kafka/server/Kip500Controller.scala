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

import java.io.File
import java.util.concurrent.locks.ReentrantLock
import java.util
import java.util.concurrent.CompletableFuture

import kafka.metrics.{KafkaMetricsGroup, KafkaMetricsReporter}
import kafka.network.SocketServer
import kafka.security.CredentialProvider
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, MetricsReporter}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{ClusterResource, Endpoint}
import org.apache.kafka.server.authorizer.{Authorizer, AuthorizerServerInfo}

import scala.jdk.CollectionConverters._

object Kip500Controller {
  sealed trait Status
  case object SHUTDOWN extends Status
  case object STARTING extends Status
  case object STARTED extends Status
  case object SHUTTING_DOWN extends Status

  def readMetaProperties(config: KafkaConfig): MetaProperties = {
    val file = new File(config.metadataLogDir, "meta.properties")
    val checkpoint = new BrokerMetadataCheckpoint(file)
    val properties = checkpoint.read()
    if (properties.isEmpty) {
      throw new RuntimeException(s"Unable to read ${config.metadataLogDir}")
    }
    MetaProperties(properties.get)
  }
}

private[kafka] case class ControllerAuthorizerInfo(clusterResource: ClusterResource,
                                                   brokerId: Int,
                                                   endpoints: util.List[Endpoint],
                                                   interBrokerEndpoint: Endpoint) extends AuthorizerServerInfo

/**
 * A KIP-500 Kafka controller.
 */
class Kip500Controller(val config: KafkaConfig,
                       val time: Time, 
                       val threadNamePrefix: Option[String],
                       val kafkaMetricsReporters: Seq[KafkaMetricsReporter])
                          extends Logging with KafkaMetricsGroup {
  import Kip500Controller._

  val lock = new ReentrantLock()
  val awaitShutdownCond = lock.newCondition()
  var status: Status = SHUTDOWN

  var metaProperties: MetaProperties = null
  var authorizer: Option[Authorizer] = null
  var metrics: Metrics = null
  var tokenCache: DelegationTokenCache = null
  var credentialProvider: CredentialProvider = null
  var socketServer: SocketServer = null

  private def maybeChangeStatus(from: Status, to: Status): Boolean = {
    lock.lock()
    try {
      if (status != from) return false
      status = to
      if (to == SHUTDOWN) awaitShutdownCond.signalAll()
    } finally {
      lock.unlock()
    }
    true
  }

  def startup(): Unit = {
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return
    try {
      maybeChangeStatus(STARTING, STARTED)
      // TODO: initialize the log dir(s)
      this.logIdent = new LogContext(s"[Kip500Controller id=${config.controllerId}] ").logPrefix()
      metaProperties = readMetaProperties(config)
      val clusterId = metaProperties.clusterId.toString
      info(s"starting with clusterId = ${metaProperties.clusterId}")

      val javaListeners = config.controllerListeners.map(_.toJava).asJava
      authorizer = config.authorizer
      authorizer.foreach(_.configure(config.originals))

      val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = authorizer match {
        case Some(authZ) =>
          // It would be nice to remove some of the broker-specific assumptions from
          // AuthorizerServerInfo, such as the assumption that there is an inter-broker
          // listener, or that ID is named brokerId.
          val controllerAuthorizerInfo = ControllerAuthorizerInfo(
            new ClusterResource(clusterId), config.controllerId, javaListeners, javaListeners.get(0))
          authZ.start(controllerAuthorizerInfo).asScala.map { case (ep, cs) =>
            ep -> cs.toCompletableFuture
          }.toMap
        case None =>
          javaListeners.asScala.map {
            ep => ep -> CompletableFuture.completedFuture[Void](null)
          }.toMap
      }
      val jmxReporter = new JmxReporter()
      jmxReporter.configure(config.originals)
      val metricConfig = KafkaBroker.metricConfig(config)
      val metricsContext = KafkaBroker.createKafkaMetricsContext(clusterId, config)
      val reporters = new util.ArrayList[MetricsReporter]
      reporters.add(jmxReporter)
      metrics = new Metrics(metricConfig, reporters, time, true, metricsContext)
      KafkaBroker.notifyClusterListeners(clusterId,
        kafkaMetricsReporters ++ metrics.reporters.asScala)

      tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
      credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)
      socketServer = new SocketServer(config, metrics, time, credentialProvider)
      socketServer.startup(startProcessingRequests = false)
      socketServer.startProcessingRequests(authorizerFutures)

    } catch {
      case e: Throwable =>
        maybeChangeStatus(STARTING, STARTED)
        fatal("Fatal error during controller startup. Prepare to shutdown", e)
        shutdown()
        throw e
    }
  }

  def shutdown(): Unit = {
    if (!maybeChangeStatus(STARTED, SHUTTING_DOWN)) return
    try {
      info("shutting down")
      if (socketServer != null)
        CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
      if (socketServer != null)
        CoreUtils.swallow(socketServer.shutdown(), this)
    } catch {
      case e: Throwable =>
        fatal("Fatal error during controller shutdown.", e)
        throw e
    } finally {
      maybeChangeStatus(SHUTTING_DOWN, SHUTDOWN)
    }
  }

  def awaitShutdown(): Unit = {
    lock.lock()
    try {
      while (true) {
        if (status == SHUTDOWN) return
        awaitShutdownCond.awaitUninterruptibly()
      }
    } finally {
      lock.unlock()
    }
  }
}
