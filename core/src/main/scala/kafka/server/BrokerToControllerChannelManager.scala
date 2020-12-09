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
import java.util.concurrent.LinkedBlockingDeque

import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.{ExponentialBackoff, LogContext, Time}
import org.apache.kafka.common.Node
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.JaasContext
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
                                 controllerConnectNodes: Seq[Node]) extends ControllerNodeProvider {
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
                                       threadNamePrefix: Option[String] = None) extends Logging {
  private val requestQueue = new LinkedBlockingDeque[BrokerToControllerQueueItem]
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
    val brokerToControllerListenerName = config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
    val brokerToControllerSecurityProtocol = config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)

    val networkClient = {
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
        0,
        0,
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
      requestQueue,
      controllerNodeProvider,
      config,
      time,
      threadName)
  }

  def sendRequest(request: AbstractRequest.Builder[_ <: AbstractRequest],
                           callback: RequestCompletionHandler): Unit = {
    requestQueue.put(BrokerToControllerQueueItem(request, callback))
    requestThread.wakeup()
  }
}

case class BrokerToControllerQueueItem(request: AbstractRequest.Builder[_ <: AbstractRequest],
                                       callback: RequestCompletionHandler)

class BrokerToControllerRequestThread(networkClient: KafkaClient,
                                      metadataUpdater: ManualMetadataUpdater,
                                      requestQueue: LinkedBlockingDeque[BrokerToControllerQueueItem],
                                      val controllerNodeProvider: ControllerNodeProvider,
                                      config: KafkaConfig,
                                      time: Time,
                                      threadName: String)
  extends InterBrokerSendThread(threadName, networkClient, time, isInterruptible = false) {

  private val exponentialBackoff = new ExponentialBackoff(100, 2, 30000, 0.1)
  private var waitForControllerRetries = 0L
  private var curController: Option[Node] = None

  override def requestTimeoutMs: Int = config.controllerSocketTimeoutMs

  override def generateRequests(): (Iterable[RequestAndCompletionHandler], Long) = {
    val nextController: Option[Node] = controllerNodeProvider.controllerNode()
    if (nextController.isEmpty) {
      curController = None
      val waitMs = exponentialBackoff.backoff(waitForControllerRetries)
      waitForControllerRetries = waitForControllerRetries + 1
      (Seq(), waitMs)
    } else {
      waitForControllerRetries = 0
      if (curController.isEmpty || curController.get != nextController.get) {
        metadataUpdater.setNodes(Collections.singletonList(nextController.get))
        curController = nextController
        info(s"Controller node is now ${curController}")
      }
      val requestsToSend = new mutable.Queue[RequestAndCompletionHandler]
      val topRequest = requestQueue.poll()
      if (topRequest != null) {
        val request = RequestAndCompletionHandler(curController.get,
          topRequest.request, handleResponse(curController.get, topRequest))
        requestsToSend.enqueue(request)
      }
      (requestsToSend, Long.MaxValue)
    }
  }

  private[server] def handleResponse(controller: Node, request: BrokerToControllerQueueItem)(response: ClientResponse): Unit = {
    if (response.wasDisconnected()) {
      info(s"Unable to send control request to node ${controller.id()}: disconnected.")
      requestQueue.putFirst(request)
    } else if (response.responseBody().errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
      info(s"Unable to send control request to node ${controller.id()}: received NOT_CONTROLLER response.")
      // Disconnect the current connection.  This will trigger exponential backoff behavior so that
      // we don't sit in a tight loop and try to send messages to the ex-controller.
      networkClient.close(controller.idString())
      requestQueue.putFirst(request)
    } else {
      request.callback.onComplete(response)
    }
  }
}
