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

import java.util.concurrent.{LinkedBlockingDeque, TimeUnit}

import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.common.Node
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.{LogContext, Time}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

trait BrokerToControllerChannelManager {

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
                  retryDeadlineMs: Long): Unit

  def start(): Unit

  def shutdown(): Unit
}

/**
 * This class manages the connection between a broker and the controller. It runs a single
 * {@link BrokerToControllerRequestThread} which uses the broker's metadata cache as its own metadata to find
 * and connect to the controller. The channel is async and runs the network connection in the background.
 * The maximum number of in-flight requests are set to one to ensure orderly response from the controller, therefore
 * care must be taken to not block on outstanding requests for too long.
 */
class BrokerToControllerChannelManagerImpl(metadataCache: kafka.server.MetadataCache,
                                           time: Time,
                                           metrics: Metrics,
                                           config: KafkaConfig,
                                           channelName: String,
                                           threadNamePrefix: Option[String] = None) extends BrokerToControllerChannelManager with Logging {
  private val requestQueue = new LinkedBlockingDeque[BrokerToControllerQueueItem]
  private val logContext = new LogContext(s"[broker-${config.brokerId}-to-controller] ")
  private val manualMetadataUpdater = new ManualMetadataUpdater()
  private val requestThread = newRequestThread

  override def start(): Unit = {
    requestThread.start()
  }

  override def shutdown(): Unit = {
    requestThread.shutdown()
    requestThread.awaitShutdown()
    info(s"Broker to controller channel manager for $channelName shutdown")
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
        ClientDnsLookup.USE_ALL_DNS_IPS,
        time,
        false,
        new ApiVersions,
        logContext
      )
    }
    val threadName = threadNamePrefix match {
      case None => s"broker-${config.brokerId}-to-controller-send-thread"
      case Some(name) => s"$name:broker-${config.brokerId}-to-controller-send-thread"
    }

    new BrokerToControllerRequestThread(networkClient, manualMetadataUpdater, requestQueue, metadataCache, config,
      brokerToControllerListenerName, time, threadName)
  }

  override def sendRequest(request: AbstractRequest.Builder[_ <: AbstractRequest],
                           callback: ControllerRequestCompletionHandler,
                           retryDeadlineMs: Long): Unit = {
    requestQueue.put(BrokerToControllerQueueItem(request, callback, retryDeadlineMs))
    requestThread.wakeup()
  }
}

abstract class ControllerRequestCompletionHandler extends RequestCompletionHandler {

  /**
   * Fire when the request transmission time passes the caller defined deadline on the channel queue.
   * It covers the total waiting time including retries which might be the result of individual request timeout.
   */
  def onTimeout(): Unit
}

case class BrokerToControllerQueueItem(request: AbstractRequest.Builder[_ <: AbstractRequest],
                                       callback: ControllerRequestCompletionHandler,
                                       deadlineMs: Long)

class BrokerToControllerRequestThread(networkClient: KafkaClient,
                                      metadataUpdater: ManualMetadataUpdater,
                                      requestQueue: LinkedBlockingDeque[BrokerToControllerQueueItem],
                                      metadataCache: kafka.server.MetadataCache,
                                      config: KafkaConfig,
                                      listenerName: ListenerName,
                                      time: Time,
                                      threadName: String)
  extends InterBrokerSendThread(threadName, networkClient, time, isInterruptible = false) {

  private var activeController: Option[Node] = None

  override def requestTimeoutMs: Int = config.controllerSocketTimeoutMs

  override def generateRequests(): Iterable[RequestAndCompletionHandler] = {
    val requestsToSend = new mutable.Queue[RequestAndCompletionHandler]
    val topRequest = requestQueue.poll()
    if (topRequest != null) {
      val request = RequestAndCompletionHandler(
        activeController.get,
        topRequest.request,
        handleResponse(topRequest)
      )

      requestsToSend.enqueue(request)
    }
    requestsToSend
  }

  private[server] def handleResponse(request: BrokerToControllerQueueItem)(response: ClientResponse): Unit = {
    if (hasTimedOut(request, response)) {
      request.callback.onTimeout()
    } else if (response.wasDisconnected()) {
      activeController = None
      requestQueue.putFirst(request)
    } else if (response.responseBody().errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
      // just close the controller connection and wait for metadata cache update in doWork
      networkClient.close(activeController.get.idString)
      activeController = None
      requestQueue.putFirst(request)
    } else {
      request.callback.onComplete(response)
    }
  }

  private def hasTimedOut(request: BrokerToControllerQueueItem, response: ClientResponse): Boolean = {
    response.receivedTimeMs() > request.deadlineMs
  }

  private[server] def backoff(): Unit = pause(100, TimeUnit.MILLISECONDS)

  override def doWork(): Unit = {
    if (activeController.isDefined) {
      super.doWork()
    } else {
      debug("Controller isn't cached, looking for local metadata changes")
      val controllerOpt = metadataCache.getControllerId.flatMap(metadataCache.getAliveBroker)
      if (controllerOpt.isDefined) {
        if (activeController.isEmpty || activeController.exists(_.id != controllerOpt.get.id))
          info(s"Recorded new controller, from now on will use broker ${controllerOpt.get.id}")
        activeController = Option(controllerOpt.get.node(listenerName))
        metadataUpdater.setNodes(metadataCache.getAliveBrokers.map(_.node(listenerName)).asJava)
      } else {
        // need to backoff to avoid tight loops
        debug("No controller defined in metadata cache, retrying after backoff")
        backoff()
      }
    }
  }
}
