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
import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
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
import org.apache.kafka.server.common.ApiMessageAndVersion

import scala.collection.Seq
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._


case class ControllerNodeAndEpoch(
  node: Node,
  epoch: Int
) {
  def id: Int = node.id
}

trait ControllerNodeProvider {
  def get(): Option[ControllerNodeAndEpoch]
  def listenerName: ListenerName
  def securityProtocol: SecurityProtocol
  def saslMechanism: String
}

object ZkMetadataCacheControllerNodeProvider {
  def apply(
    config: KafkaConfig,
    metadataCache: ZkMetadataCache,
  ): ZkMetadataCacheControllerNodeProvider = {
    val listenerName = config.controlPlaneListenerName
      .getOrElse(config.interBrokerListenerName)

    val securityProtocol = config.controlPlaneSecurityProtocol
      .getOrElse(config.interBrokerSecurityProtocol)

    new ZkMetadataCacheControllerNodeProvider(
      metadataCache,
      listenerName,
      securityProtocol,
      config.saslMechanismInterBrokerProtocol
    )
  }
}

class ZkMetadataCacheControllerNodeProvider(
  val metadataCache: ZkMetadataCache,
  val listenerName: ListenerName,
  val securityProtocol: SecurityProtocol,
  val saslMechanism: String
) extends ControllerNodeProvider {
  override def get(): Option[ControllerNodeAndEpoch] = {
    metadataCache.currentController.flatMap { controllerAndEpoch =>
      val controllerId = controllerAndEpoch.id
      metadataCache.getAliveBrokerNode(controllerId, listenerName).map { controllerNode =>
        ControllerNodeAndEpoch(controllerNode, controllerAndEpoch.epoch)
      }
    }
  }
}

object KRaftControllerNodeProvider {
  def apply(
    raftManager: RaftManager[ApiMessageAndVersion],
    config: KafkaConfig,
    controllerQuorumVoterNodes: Seq[Node]
  ): KRaftControllerNodeProvider = {
    val controllerListenerName = new ListenerName(config.controllerListenerNames.head)
    val controllerSecurityProtocol = config.effectiveListenerSecurityProtocolMap.getOrElse(controllerListenerName, SecurityProtocol.forName(controllerListenerName.value()))
    val controllerSaslMechanism = config.saslMechanismControllerProtocol
    new KRaftControllerNodeProvider(
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
class KRaftControllerNodeProvider(
  val raftManager: RaftManager[ApiMessageAndVersion],
  controllerQuorumVoterNodes: Seq[Node],
  val listenerName: ListenerName,
  val securityProtocol: SecurityProtocol,
  val saslMechanism: String
) extends ControllerNodeProvider with Logging {
  private val idToNode = controllerQuorumVoterNodes.map(node => node.id() -> node).toMap

  override def get(): Option[ControllerNodeAndEpoch] = {
    val leaderAndEpoch = raftManager.leaderAndEpoch
    leaderAndEpoch.leaderId.asScala.map { controllerId =>
      val controllerNode: Node = idToNode.getOrElse(controllerId,
        throw new IllegalStateException(s"Unable to find controller $controllerId " +
          s"among configured voters $controllerQuorumVoterNodes")
      )
      ControllerNodeAndEpoch(controllerNode, leaderAndEpoch.epoch)
    }
  }
}

object BrokerToControllerChannelManager {
  def apply(
    controllerNodeProvider: ControllerNodeProvider,
    time: Time,
    metrics: Metrics,
    config: KafkaConfig,
    channelName: String,
    threadNamePrefix: Option[String],
    retryTimeoutMs: Long
  ): BrokerToControllerChannelManager = {
    new BrokerToControllerChannelManagerImpl(
      controllerNodeProvider,
      time,
      metrics,
      config,
      channelName,
      threadNamePrefix,
      retryTimeoutMs
    )
  }
}

trait BrokerToControllerChannelManager {
  def start(): Unit
  def shutdown(): Unit
  def controllerApiVersions(): Option[NodeApiVersions]

  /**
   * Send a request to the controller.
   *
   * @param request The request to be sent to the controller
   * @param callback The callback to be notified when the request has completed (either
   *                 successfully or unsuccessfully)
   */
  def sendRequest(
    request: AbstractRequest.Builder[_ <: AbstractRequest],
    callback: ControllerRequestCompletionHandler
  ): Unit = {
    sendRequest(
      request = request,
      minControllerEpoch = 0,
      callback = callback
    )
  }

  /**
   * Send a request to the controller.
   *
   * @param request The request to be sent to the controller
   * @param minControllerEpoch In some cases, we may learn of a new controller and epoch through a separate
   *                           channel than the `ControllerNodeProvider`. For example, the `LeaderAndIsr`
   *                           request includes the ID of the controller sending it as well as its epoch.
   *                           In these cases, it is useful to ensure that the request does not get
   *                           inadvertently sent to an older controller.
   * @param callback The callback to be notified when the request has completed (either
   *                 successfully or unsuccessfully)
   */
  def sendRequest(
    request: AbstractRequest.Builder[_ <: AbstractRequest],
    minControllerEpoch: Int,
    callback: ControllerRequestCompletionHandler
  ): Unit
}

/**
 * This class manages the connection between a broker and the controller. It runs a single
 * [[BrokerToControllerRequestThread]] which uses the broker's metadata cache as its own metadata to find
 * and connect to the controller. The channel is async and runs the network connection in the background.
 * The maximum number of in-flight requests are set to one to ensure orderly response from the controller, therefore
 * care must be taken to not block on outstanding requests for too long.
 */
class BrokerToControllerChannelManagerImpl(
  controllerNodeProvider: ControllerNodeProvider,
  time: Time,
  metrics: Metrics,
  config: KafkaConfig,
  channelName: String,
  threadNamePrefix: Option[String],
  retryTimeoutMs: Long
) extends BrokerToControllerChannelManager with Logging {
  private val logContext = new LogContext(s"[BrokerToControllerChannelManager broker=${config.brokerId} name=$channelName] ")
  private val manualMetadataUpdater = new ManualMetadataUpdater()
  private val apiVersions = new ApiVersions()
  private val requestThread = newRequestThread

  def start(): Unit = {
    requestThread.start()
  }

  def shutdown(): Unit = {
    requestThread.shutdown()
    info(s"Broker to controller channel manager for $channelName shutdown")
  }

  private[server] def newRequestThread = {
    val networkClient = {
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        controllerNodeProvider.securityProtocol,
        JaasContext.Type.SERVER,
        config,
        controllerNodeProvider.listenerName,
        controllerNodeProvider.saslMechanism,
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
        config.requestTimeoutMs,
        config.connectionSetupTimeoutMs,
        config.connectionSetupTimeoutMaxMs,
        time,
        true,
        apiVersions,
        logContext
      )
    }
    val threadName = threadNamePrefix match {
      case None => s"BrokerToControllerChannelManager broker=${config.brokerId} name=$channelName"
      case Some(name) => s"$name:BrokerToControllerChannelManager broker=${config.brokerId} name=$channelName"
    }

    new BrokerToControllerRequestThread(
      networkClient,
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
   * @param minControllerEpoch Minimum controller epoch that this request should be sent to
   * @param callback        Request completion callback.
   */
  def sendRequest(
    request: AbstractRequest.Builder[_ <: AbstractRequest],
    minControllerEpoch: Int,
    callback: ControllerRequestCompletionHandler
  ): Unit = {
    requestThread.enqueue(BrokerToControllerQueueItem(
      time.milliseconds(),
      request,
      minControllerEpoch,
      callback
    ))
  }

  def controllerApiVersions(): Option[NodeApiVersions] = {
    requestThread.activeControllerOpt().flatMap { controller =>
      Option(apiVersions.get(controller.node.idString))
    }
  }
}

abstract class ControllerRequestCompletionHandler extends RequestCompletionHandler {

  /**
   * Fire when the request transmission time passes the caller defined deadline on the channel queue.
   * It covers the total waiting time including retries which might be the result of individual request timeout.
   */
  def onTimeout(): Unit
}

case class BrokerToControllerQueueItem(
  createdTimeMs: Long,
  request: AbstractRequest.Builder[_ <: AbstractRequest],
  minControllerEpoch: Int,
  callback: ControllerRequestCompletionHandler
)

class BrokerToControllerRequestThread(
  networkClient: KafkaClient,
  metadataUpdater: ManualMetadataUpdater,
  controllerNodeProvider: ControllerNodeProvider,
  config: KafkaConfig,
  time: Time,
  threadName: String,
  retryTimeoutMs: Long
) extends InterBrokerSendThread(threadName, networkClient, config.controllerSocketTimeoutMs, time, isInterruptible = false) {

  private val requestQueue = new LinkedBlockingDeque[BrokerToControllerQueueItem]()
  private val activeController = new AtomicReference[ControllerNodeAndEpoch](null)

  // Used for testing
  @volatile
  private[server] var started = false

  def activeControllerOpt(): Option[ControllerNodeAndEpoch] = {
    Option(activeController.get())
  }

  private def updateControllerAddress(newActiveController: ControllerNodeAndEpoch): Unit = {
    activeController.set(newActiveController)
  }

  def enqueue(request: BrokerToControllerQueueItem): Unit = {
    if (!started) {
      throw new IllegalStateException("Cannot enqueue a request if the request thread is not running")
    }
    requestQueue.add(request)
    if (activeControllerOpt().isDefined) {
      wakeup()
    }
  }

  def queueSize: Int = {
    requestQueue.size
  }

  override def generateRequests(): Iterable[RequestAndCompletionHandler] = {
    val currentTimeMs = time.milliseconds()
    val requestIter = requestQueue.iterator()
    val controllerOpt = activeControllerOpt()

    while (requestIter.hasNext) {
      val request = requestIter.next
      if (currentTimeMs - request.createdTimeMs >= retryTimeoutMs) {
        requestIter.remove()
        request.callback.onTimeout()
      } else {
        controllerOpt.foreach { activeController =>
          if (activeController.epoch >= request.minControllerEpoch) {
            requestIter.remove()
            return Some(RequestAndCompletionHandler(
              time.milliseconds(),
              activeController.node,
              request.request,
              handleResponse(request)
            ))
          }
        }
      }
    }
    None
  }

  private[server] def handleResponse(queueItem: BrokerToControllerQueueItem)(response: ClientResponse): Unit = {
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
      // just close the controller connection and wait for metadata cache update in doWork
      activeControllerOpt().foreach { activeController =>
        networkClient.disconnect(activeController.node.idString)
        updateControllerAddress(null)
      }

      requestQueue.putFirst(queueItem)
    } else {
      queueItem.callback.onComplete(response)
    }
  }

  override def doWork(): Unit = {
    if (activeControllerOpt().isDefined) {
      super.pollOnce(Long.MaxValue)
    } else {
      debug("Controller isn't cached, looking for local metadata changes")
      controllerNodeProvider.get() match {
        case Some(controllerNodeAndEpoch) =>
          info(s"Recorded new controller, from now on will use broker $controllerNodeAndEpoch")
          updateControllerAddress(controllerNodeAndEpoch)
          metadataUpdater.setNodes(Seq(controllerNodeAndEpoch.node).asJava)
        case None =>
          // need to backoff to avoid tight loops
          debug("No controller defined in metadata cache, retrying after backoff")
          super.pollOnce(maxTimeoutMs = 100)
      }
    }
  }

  override def start(): Unit = {
    super.start()
    started = true
  }
}
