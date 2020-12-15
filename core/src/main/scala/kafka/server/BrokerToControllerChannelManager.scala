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

import java.util.Collections

import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.common.Node
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.{ExponentialBackoff, LogContext, Time}
import org.apache.kafka.metalog.MetaLogManager

import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._


trait ControllerNodeProvider {
  def controllerNode(): Option[Node]
}

/**
 * Finds the controller node by looking in the MetadataCache.
 * This provider is used when we are in legacy mode.
 */
class MetadataCacheControllerNodeProvider(val metadataCache: kafka.server.MetadataCache,
                                          val listenerName: ListenerName) extends ControllerNodeProvider {
  override def controllerNode(): Option[Node] = {
    val snapshot = metadataCache.readState()
    snapshot.controllerId match {
      case None => None
      case Some(id) => {
          snapshot.aliveNodes.get(id) match {
          case None => None
          case Some(listenerMap) => listenerMap.get(listenerName)
        }
      }
    }
  }
}

/**
 * Finds the controller node by checking the metadata log manager.
 * This provider is used when we are in KIP-500 mode.
 */
class RaftControllerNodeProvider(val metaLogManager: MetaLogManager,
                                 controllerConnectNodes: Seq[Node]) extends ControllerNodeProvider with Logging {
  val idToNode = controllerConnectNodes.map(node => node.id() -> node).toMap

  override def controllerNode(): Option[Node] = {
    val leader = metaLogManager.leader()
    if (leader == null) {
      None
    } else if (leader.nodeId() < 0) {
      None
    } else {
      idToNode.get(leader.nodeId())
    }
  }
}

/**
 * This class manages the connection between a broker and the controller. It runs a single
 * {@link BrokerToControllerRequestThread} which uses the broker's metadata cache as its own metadata to find
 * and connect to the controller. The channel is async and runs the network connection in the background.
 * The maximum number of in-flight requests are set to one to ensure orderly response from the controller, therefore
 * care must be taken to not block on outstanding requests for too long.
 */
class BrokerToControllerChannelManager(controllerNodeProvider: ControllerNodeProvider,
                                       time: Time,
                                       metrics: Metrics,
                                       config: KafkaConfig,
                                       managerName: String,
                                       threadNamePrefix: Option[String] = None,
                                       val configuredClient: Option[KafkaClient] = None) extends Logging {
  private val logContext = new LogContext(s"[broker-${config.brokerId}-to-controller-$managerName] ")
  private val manualMetadataUpdater = new ManualMetadataUpdater()
  private val requestThread = newRequestThread

  def start(): Unit = {
    requestThread.start()
  }

  def shutdown(): Unit = {
    requestThread.shutdown()
    requestThread.awaitShutdown()
  }

  private[server] def newRequestThread = {
    val networkClient = configuredClient.getOrElse {
      val brokerToControllerListenerName =
        config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
      val brokerToControllerSecurityProtocol =
        config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        brokerToControllerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        brokerToControllerListenerName,
        config.saslMechanismInterBrokerProtocol,
        time,
        config.saslInterBrokerHandshakeRequestEnable,
        logContext
      )
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        s"${managerName}BrokerToControllerChannel",
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
        ClientDnsLookup.USE_ALL_DNS_IPS,
        time,
        false,
        new ApiVersions,
        logContext
      )
    }
    val threadName = threadNamePrefix match {
      case None => s"broker-${config.brokerId}-to-controller-${managerName}-sender"
      case Some(name) => s"$name-broker-${config.brokerId}-to-controller-${managerName}-sender"
    }

    new BrokerToControllerRequestThread(networkClient,
      manualMetadataUpdater,
      controllerNodeProvider,
      config,
      time,
      threadName)
  }

  /**
   * Send request to the controller.
   *
   * @param request         The request to be sent.
   * @param callback        Request completion callback.
   * @param retryDeadlineMs The retry deadline which will only be checked after receiving a response.
   *                        This means that in the worst case, the total timeout would be twice of
   *                        the configured timeout.
   */
  def sendRequest(request: AbstractRequest.Builder[_ <: AbstractRequest],
                  callback: ControllerRequestCompletionHandler,
                  retryDeadlineMs: Long): Unit = {
    requestThread.sendRequest(request, callback, retryDeadlineMs)
  }

  def sendRequest(request: AbstractRequest.Builder[_ <: AbstractRequest],
                  callback: RequestCompletionHandler): Unit = {
    val completionHandler = new ControllerRequestCompletionHandler {
      override def onTimeout(): Unit = throw new IllegalStateException()

      override def onComplete(response: ClientResponse): Unit = callback.onComplete(response)
    }

    sendRequest(request, completionHandler, retryDeadlineMs = Long.MaxValue)
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
  request: AbstractRequest.Builder[_ <: AbstractRequest],
  callback: ControllerRequestCompletionHandler,
  deadlineMs: Long
)


class BrokerToControllerRequestThread(networkClient: KafkaClient,
                                      metadataUpdater: ManualMetadataUpdater,
                                      val controllerNodeProvider: ControllerNodeProvider,
                                      config: KafkaConfig,
                                      time: Time,
                                      threadName: String)
  extends InterBrokerSendThread(threadName, networkClient, config.controllerSocketTimeoutMs, time, isInterruptible = false) {

  val sendableRequests = new mutable.ListBuffer[BrokerToControllerQueueItem]()

  def sendRequest(request: AbstractRequest.Builder[_ <: AbstractRequest],
                  callback: ControllerRequestCompletionHandler,
                  deadlineMs: Long): Unit =
    sendRequest(BrokerToControllerQueueItem(request, callback, deadlineMs))

  def sendRequest(request: BrokerToControllerQueueItem): Unit = synchronized {
    sendableRequests += request
    networkClient.wakeup()
  }

  private val exponentialBackoff = new ExponentialBackoff(100, 2, 30000, 0.1)
  private var waitForControllerRetries = 0L
  private var curController: Option[Node] = None

  def generateRequests(): (Iterable[RequestAndCompletionHandler], Long) = synchronized {
    if (sendableRequests.isEmpty) {
      waitForControllerRetries = 0
      (Seq(), Long.MaxValue)
    } else {
      val nextController: Option[Node] = controllerNodeProvider.controllerNode()
      if (nextController.isEmpty) {
        curController = None
        val waitMs = exponentialBackoff.backoff(waitForControllerRetries)
        waitForControllerRetries = waitForControllerRetries + 1
        (Seq(), waitMs)
      } else {
        waitForControllerRetries = 0
        if (curController.isEmpty || curController.get != nextController.get) {
          curController = nextController
          metadataUpdater.setNodes(Collections.singletonList(curController.get))
          info(s"Controller node is now ${curController}")
        }
        val requestsToSend = sendableRequests.map {
          request => RequestAndCompletionHandler(curController.get,
              request.request, handleResponse(curController.get, request))
        }
        sendableRequests.clear()
        (requestsToSend, Long.MaxValue)
      }
    }
  }

  override def doWork(): Unit = {
    val (requests, backoffMs) = generateRequests()
    requests.foreach(super.sendRequest)
    super.poll(backoffMs)
  }

  private def maybeRetryRequest(
    request: BrokerToControllerQueueItem,
  ): Unit = {
    if (time.milliseconds() > request.deadlineMs) {
      request.callback.onTimeout()
    } else {
      sendRequest(request)
    }
  }

  private[server] def handleResponse(controller: Node, request: BrokerToControllerQueueItem)(response: ClientResponse): Unit = {
    if (response.authenticationException() != null || response.versionMismatch() != null) {
      request.callback.onComplete(response)
    } else if (response.wasDisconnected()) {
      debug(s"Unable to send control request to node ${controller.id()}: disconnected.")
      maybeRetryRequest(request)
    } else if (response.responseBody().errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
      debug(s"Unable to send control request to node ${controller.id()}: received NOT_CONTROLLER response.")
      // Disconnect the current connection.  This will trigger exponential backoff behavior so that
      // we don't sit in a tight loop and try to send messages to the ex-controller.
      networkClient.close(controller.idString())
      maybeRetryRequest(request)
    } else {
      request.callback.onComplete(response)
    }
  }
}
