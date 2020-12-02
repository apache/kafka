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

import java.util.concurrent.{Future, LinkedBlockingDeque, TimeUnit}

import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.Node
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol

import scala.collection.mutable
import scala.jdk.CollectionConverters._


trait BrokerToControllerChannelManager {
  def sendRequest(request: AbstractRequest.Builder[_ <: AbstractRequest],
                  callback: RequestCompletionHandler): Unit

  def clusterId(): Future[String]

  def start(): Unit

  def shutdown(): Unit
}


/**
 * This abstract class manages the connection between a broker and the controller. It runs a single
 * {@link BrokerToControllerRequestThread} which finds and connects to the controller. The channel is async and runs
 * the network connection in the background. The maximum number of in-flight requests are set to one to ensure orderly
 * response from the controller, therefore care must be taken to not block on outstanding requests for too long.
 */
abstract class AbstractBrokerToControllerChannelManager(time: Time,
                                                        metrics: Metrics,
                                                        config: KafkaConfig,
                                                        threadNamePrefix: Option[String] = None) extends BrokerToControllerChannelManager with Logging {
  protected val requestQueue = new LinkedBlockingDeque[BrokerToControllerQueueItem]
  protected val logContext = new LogContext(s"[broker-${config.brokerId}-to-controller] ")
  protected val manualMetadataUpdater = new ManualMetadataUpdater()

  protected def requestThread: BrokerToControllerRequestThread
  protected def brokerToControllerListenerName: ListenerName
  protected def brokerToControllerSecurityProtocol: SecurityProtocol
  protected def brokerToControllerSaslMechanism: String

  protected def instantiateRequestThread(networkClient: NetworkClient,
                                         brokerToControllerListenerName: ListenerName,
                                         threadName: String): BrokerToControllerRequestThread

  override def start(): Unit = {
    requestThread.start()
  }

  override def shutdown(): Unit = {
    requestThread.shutdown()
    requestThread.awaitShutdown()
  }

  private[server] def newRequestThread = {
    val networkClient = {
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        brokerToControllerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        brokerToControllerListenerName,
        brokerToControllerSaslMechanism,
        time,
        config.saslInterBrokerHandshakeRequestEnable,
        logContext
      )
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "BrokerToControllerChannel",
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
      case None => s"broker-${config.brokerId}-to-controller-send-thread"
      case Some(name) => s"$name:broker-${config.brokerId}-to-controller-send-thread"
    }

    instantiateRequestThread(networkClient, brokerToControllerListenerName, threadName)
  }

  override def sendRequest(request: AbstractRequest.Builder[_ <: AbstractRequest],
                           callback: RequestCompletionHandler): Unit = {
    requestQueue.put(BrokerToControllerQueueItem(request, callback))
    requestThread.wakeup()
  }
}

case class BrokerToControllerQueueItem(request: AbstractRequest.Builder[_ <: AbstractRequest],
                                       callback: RequestCompletionHandler)

abstract class BrokerToControllerRequestThread(networkClient: KafkaClient,
                                               requestQueue: LinkedBlockingDeque[BrokerToControllerQueueItem],
                                               config: KafkaConfig,
                                               time: Time,
                                               threadName: String)
  extends InterBrokerSendThread(threadName, networkClient, time, isInterruptible = false) {

  protected var activeController: Option[Node] = None

  override def requestTimeoutMs: Int = config.controllerSocketTimeoutMs

  override def generateRequests(): Iterable[RequestAndCompletionHandler] = {
    val requestsToSend = new mutable.Queue[RequestAndCompletionHandler]
    val topRequest = requestQueue.poll()
    if (topRequest != null) {
      val request = RequestAndCompletionHandler(
        activeController.get,
        topRequest.request,
        handleResponse(topRequest),
        )
      requestsToSend.enqueue(request)
    }
    requestsToSend
  }

  private[server] def handleResponse(request: BrokerToControllerQueueItem)(response: ClientResponse): Unit = {
    if (response.wasDisconnected()) {
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

  private[server] def backoff(): Unit = pause(100, TimeUnit.MILLISECONDS)
}
