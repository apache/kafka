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

import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.atomic.AtomicReference
import kafka.raft.RaftManager
import kafka.server.metadata.ZkMetadataCache
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.common.{Node, Reconfigurable}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.server.{ControllerRequestCompletionHandler, NodeToControllerChannelManager}
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.server.util.{InterBrokerSendThread, RequestAndCompletionHandler}

import java.util
import java.util.Optional
import scala.collection.Seq
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

case class ControllerInformation(
  node: Option[Node],
  listenerName: ListenerName,
  securityProtocol: SecurityProtocol,
  saslMechanism: String,
  isZkController: Boolean
)

trait ControllerNodeProvider {
  def getControllerInfo(): ControllerInformation
}

class MetadataCacheControllerNodeProvider(
  val metadataCache: ZkMetadataCache,
  val config: KafkaConfig
) extends ControllerNodeProvider {

  private val zkControllerListenerName = config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
  private val zkControllerSecurityProtocol = config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)
  private val zkControllerSaslMechanism = config.saslMechanismInterBrokerProtocol

  private val kraftControllerListenerName = if (config.controllerListenerNames.nonEmpty)
    new ListenerName(config.controllerListenerNames.head) else null
  private val kraftControllerSecurityProtocol = Option(kraftControllerListenerName)
    .map( listener => config.effectiveListenerSecurityProtocolMap.getOrElse(
      listener, SecurityProtocol.forName(kraftControllerListenerName.value())))
    .orNull
  private val kraftControllerSaslMechanism = config.saslMechanismControllerProtocol

  private val emptyZkControllerInfo =  ControllerInformation(
    None,
    zkControllerListenerName,
    zkControllerSecurityProtocol,
    zkControllerSaslMechanism,
    isZkController = true)

  override def getControllerInfo(): ControllerInformation = {
    metadataCache.getControllerId.map {
      case ZkCachedControllerId(id) => ControllerInformation(
        metadataCache.getAliveBrokerNode(id, zkControllerListenerName),
        zkControllerListenerName,
        zkControllerSecurityProtocol,
        zkControllerSaslMechanism,
        isZkController = true)
      case KRaftCachedControllerId(id) => ControllerInformation(
        metadataCache.getAliveBrokerNode(id, kraftControllerListenerName),
        kraftControllerListenerName,
        kraftControllerSecurityProtocol,
        kraftControllerSaslMechanism,
        isZkController = false)
    }.getOrElse(emptyZkControllerInfo)
  }
}

object RaftControllerNodeProvider {
  def apply(
    raftManager: RaftManager[ApiMessageAndVersion],
    config: KafkaConfig,
    controllerQuorumVoterNodes: Seq[Node]
  ): RaftControllerNodeProvider = {
    val controllerListenerName = new ListenerName(config.controllerListenerNames.head)
    val controllerSecurityProtocol = config.effectiveListenerSecurityProtocolMap.getOrElse(controllerListenerName, SecurityProtocol.forName(controllerListenerName.value()))
    val controllerSaslMechanism = config.saslMechanismControllerProtocol
    new RaftControllerNodeProvider(
      raftManager,
      controllerQuorumVoterNodes,
      controllerListenerName,
      controllerSecurityProtocol,
      controllerSaslMechanism
    )
  }
}

/**
 * Finds the controller node by checking the metadata log manager.
 * This provider is used when we are using a Raft-based metadata quorum.
 */
class RaftControllerNodeProvider(
  val raftManager: RaftManager[ApiMessageAndVersion],
  controllerQuorumVoterNodes: Seq[Node],
  val listenerName: ListenerName,
  val securityProtocol: SecurityProtocol,
  val saslMechanism: String
) extends ControllerNodeProvider with Logging {
  val idToNode = controllerQuorumVoterNodes.map(node => node.id() -> node).toMap

  override def getControllerInfo(): ControllerInformation =
    ControllerInformation(raftManager.leaderAndEpoch.leaderId.asScala.map(idToNode),
      listenerName, securityProtocol, saslMechanism, isZkController = false)
}

/**
 * This class manages the connection between a broker and the controller. It runs a single
 * [[NodeToControllerRequestThread]] which uses the broker's metadata cache as its own metadata to find
 * and connect to the controller. The channel is async and runs the network connection in the background.
 * The maximum number of in-flight requests are set to one to ensure orderly response from the controller, therefore
 * care must be taken to not block on outstanding requests for too long.
 */
class NodeToControllerChannelManagerImpl(
  controllerNodeProvider: ControllerNodeProvider,
  time: Time,
  metrics: Metrics,
  config: KafkaConfig,
  channelName: String,
  threadNamePrefix: String,
  retryTimeoutMs: Long
) extends NodeToControllerChannelManager with Logging {
  private val logContext = new LogContext(s"[NodeToControllerChannelManager id=${config.nodeId} name=${channelName}] ")
  private val manualMetadataUpdater = new ManualMetadataUpdater()
  private val apiVersions = new ApiVersions()
  private val requestThread = newRequestThread

  def start(): Unit = {
    requestThread.start()
  }

  def shutdown(): Unit = {
    requestThread.shutdown()
    info(s"Node to controller channel manager for $channelName shutdown")
  }

  private[server] def newRequestThread = {
    def buildNetworkClient(controllerInfo: ControllerInformation) = {
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        controllerInfo.securityProtocol,
        JaasContext.Type.SERVER,
        config,
        controllerInfo.listenerName,
        controllerInfo.saslMechanism,
        time,
        config.saslInterBrokerHandshakeRequestEnable,
        logContext
      )
      channelBuilder match {
        case reconfigurable: Reconfigurable => config.addReconfigurable(reconfigurable)
        case _ =>
      }
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        channelName,
        Map("BrokerId" -> config.brokerId.toString).asJava,
        false,
        channelBuilder,
        logContext
      )
      new NetworkClient(
        selector,
        manualMetadataUpdater,
        config.brokerId.toString,
        1,
        50,
        50,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Math.min(Int.MaxValue, Math.min(config.controllerSocketTimeoutMs, retryTimeoutMs)).toInt, // request timeout should not exceed the provided retry timeout
        config.connectionSetupTimeoutMs,
        config.connectionSetupTimeoutMaxMs,
        time,
        true,
        apiVersions,
        logContext
      )
    }
    val threadName = s"${threadNamePrefix}to-controller-${channelName}-channel-manager"

    val controllerInformation = controllerNodeProvider.getControllerInfo()
    new NodeToControllerRequestThread(
      buildNetworkClient(controllerInformation),
      controllerInformation.isZkController,
      buildNetworkClient,
      manualMetadataUpdater,
      controllerNodeProvider,
      config,
      time,
      threadName,
      retryTimeoutMs
    )
  }

  /**
   * Send request to the controller.
   *
   * @param request         The request to be sent.
   * @param callback        Request completion callback.
   */
  def sendRequest(
    request: AbstractRequest.Builder[_ <: AbstractRequest],
    callback: ControllerRequestCompletionHandler
  ): Unit = {
    requestThread.enqueue(NodeToControllerQueueItem(
      time.milliseconds(),
      request,
      callback
    ))
  }

  def controllerApiVersions(): Optional[NodeApiVersions] = {
    requestThread.activeControllerAddress().flatMap { activeController =>
      Option(apiVersions.get(activeController.idString))
    }.asJava
  }
}

case class NodeToControllerQueueItem(
  createdTimeMs: Long,
  request: AbstractRequest.Builder[_ <: AbstractRequest],
  callback: ControllerRequestCompletionHandler
)

class NodeToControllerRequestThread(
  initialNetworkClient: KafkaClient,
  var isNetworkClientForZkController: Boolean,
  networkClientFactory: ControllerInformation => KafkaClient,
  metadataUpdater: ManualMetadataUpdater,
  controllerNodeProvider: ControllerNodeProvider,
  config: KafkaConfig,
  time: Time,
  threadName: String,
  retryTimeoutMs: Long
) extends InterBrokerSendThread(
  threadName,
  initialNetworkClient,
  Math.min(Int.MaxValue, Math.min(config.controllerSocketTimeoutMs, retryTimeoutMs)).toInt,
  time,
  false
) with Logging {

  this.logIdent = logPrefix

  private def maybeResetNetworkClient(controllerInformation: ControllerInformation): Unit = {
    if (isNetworkClientForZkController != controllerInformation.isZkController) {
      debug("Controller changed to " + (if (isNetworkClientForZkController) "kraft" else "zk") + " mode. " +
        s"Resetting network client with new controller information : ${controllerInformation}")
      // Close existing network client.
      val oldClient = networkClient
      oldClient.initiateClose()
      oldClient.close()

      isNetworkClientForZkController = controllerInformation.isZkController
      updateControllerAddress(controllerInformation.node.orNull)
      controllerInformation.node.foreach(n => metadataUpdater.setNodes(Seq(n).asJava))
      networkClient = networkClientFactory(controllerInformation)
    }
  }

  private val requestQueue = new LinkedBlockingDeque[NodeToControllerQueueItem]()
  private val activeController = new AtomicReference[Node](null)

  // Used for testing
  @volatile
  private[server] var started = false

  def activeControllerAddress(): Option[Node] = {
    Option(activeController.get())
  }

  private def updateControllerAddress(newActiveController: Node): Unit = {
    activeController.set(newActiveController)
  }

  def enqueue(request: NodeToControllerQueueItem): Unit = {
    if (!started) {
      throw new IllegalStateException("Cannot enqueue a request if the request thread is not running")
    }
    requestQueue.add(request)
    if (activeControllerAddress().isDefined) {
      wakeup()
    }
  }

  def queueSize: Int = {
    requestQueue.size
  }

  override def generateRequests(): util.Collection[RequestAndCompletionHandler] = {
    val currentTimeMs = time.milliseconds()
    val requestIter = requestQueue.iterator()
    while (requestIter.hasNext) {
      val request = requestIter.next
      if (currentTimeMs - request.createdTimeMs >= retryTimeoutMs) {
        requestIter.remove()
        request.callback.onTimeout()
      } else {
        val controllerAddress = activeControllerAddress()
        if (controllerAddress.isDefined) {
          requestIter.remove()
          return util.Collections.singletonList(new RequestAndCompletionHandler(
            time.milliseconds(),
            controllerAddress.get,
            request.request,
            response => handleResponse(request)(response)
          ))
        }
      }
    }
    util.Collections.emptyList()
  }

  private[server] def handleResponse(queueItem: NodeToControllerQueueItem)(response: ClientResponse): Unit = {
    debug(s"Request ${queueItem.request} received $response")
    if (response.authenticationException != null) {
      error(s"Request ${queueItem.request} failed due to authentication error with controller",
        response.authenticationException)
      queueItem.callback.onComplete(response)
    } else if (response.versionMismatch != null) {
      error(s"Request ${queueItem.request} failed due to unsupported version error",
        response.versionMismatch)
      queueItem.callback.onComplete(response)
    } else if (response.wasDisconnected()) {
      updateControllerAddress(null)
      requestQueue.putFirst(queueItem)
    } else if (response.responseBody().errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
      debug(s"Request ${queueItem.request} received NOT_CONTROLLER exception. Disconnecting the " +
        s"connection to the stale controller ${activeControllerAddress().map(_.idString).getOrElse("null")}")
      // just close the controller connection and wait for metadata cache update in doWork
      activeControllerAddress().foreach { controllerAddress =>
        try {
          // We don't care if disconnect has an error, just log it and get a new network client
          networkClient.disconnect(controllerAddress.idString)
        } catch {
          case t: Throwable => error("Had an error while disconnecting from NetworkClient.", t)
        }
        updateControllerAddress(null)
      }

      requestQueue.putFirst(queueItem)
    } else {
      queueItem.callback.onComplete(response)
    }
  }

  override def doWork(): Unit = {
    val controllerInformation = controllerNodeProvider.getControllerInfo()
    maybeResetNetworkClient(controllerInformation)
    if (activeControllerAddress().isDefined) {
      super.pollOnce(Long.MaxValue)
    } else {
      debug("Controller isn't cached, looking for local metadata changes")
      controllerInformation.node match {
        case Some(controllerNode) =>
          info(s"Recorded new controller, from now on will use node $controllerNode")
          updateControllerAddress(controllerNode)
          metadataUpdater.setNodes(Seq(controllerNode).asJava)
        case None =>
          // need to backoff to avoid tight loops
          debug("No controller provided, retrying after backoff")
          super.pollOnce(100)
      }
    }
  }

  override def start(): Unit = {
    super.start()
    started = true
  }
}
