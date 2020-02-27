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

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import java.util.Collections

import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.common.requests.{AbstractRequest, MetadataRequest, MetadataResponse}
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{KafkaFuture, Node}
import org.apache.kafka.common.internals.{KafkaFutureImpl, Topic}
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.JaasContext

import scala.collection.JavaConverters._
import scala.collection.mutable

class BrokerToControllerChannelManager(metadataCache: MetadataCache,
                                       time: Time,
                                       metrics: Metrics,
                                       config: KafkaConfig,
                                       threadNamePrefix: Option[String] = None) extends Logging {

  private val requestQueue = new LinkedBlockingQueue[BrokerToControllerQueueItem]
  private val logContext = new LogContext(s"[broker-${config.brokerId}-to-controller] ")
  private val manualMetadataUpdater = new ManualMetadataUpdater()
  private val requestThread = newRequestThread

  def start(): Unit = {
    requestThread.start()
  }

  def shutdown(): Unit = {
    requestThread.shutdown()
    requestThread.awaitShutdown()
  }

  def getPartitionCount(topicName: String): KafkaFuture[Int] = {
    val topic = new MetadataRequestTopic().setName(Topic.GROUP_METADATA_TOPIC_NAME)
    val completionFuture = new KafkaFutureImpl[Int]
    sendRequest(new MetadataRequest.Builder(
      new MetadataRequestData().setTopics(Collections.singletonList(topic))), response => {
      val metadataResponse = response.responseBody().asInstanceOf[MetadataResponse]
      val topicOpt = metadataResponse.topicMetadata().asScala.find(t => topicName.equals(t.topic))
      if (topicOpt.isEmpty) {
        completionFuture.completeExceptionally(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception)
      } else if (topicOpt.get.error != Errors.NONE) {
        completionFuture.completeExceptionally(topicOpt.get.error.exception)
      } else {
        completionFuture.complete(topicOpt.get.partitionMetadata.size)
      }
    })
    completionFuture
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
        ClientDnsLookup.DEFAULT,
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

  private[server] def sendRequest(request: AbstractRequest.Builder[_ <: AbstractRequest],
                  callback: RequestCompletionHandler): Unit = {
    requestQueue.put(BrokerToControllerQueueItem(request, callback))
  }
}

case class BrokerToControllerQueueItem(request: AbstractRequest.Builder[_ <: AbstractRequest],
                     callback: RequestCompletionHandler)

class BrokerToControllerRequestThread(networkClient: KafkaClient,
                                      metadataUpdater: ManualMetadataUpdater,
                                      requestQueue: BlockingQueue[BrokerToControllerQueueItem],
                                      metadataCache: MetadataCache,
                                      config: KafkaConfig,
                                      listenerName: ListenerName,
                                      time: Time,
                                      threadName: String)
  extends InterBrokerSendThread(threadName, networkClient, time, isInterruptible = false, exitOnError = false) {

  private var activeController: Option[Node] = None

  override def requestTimeoutMs: Int = config.controllerSocketTimeoutMs

  override def generateRequests(): Iterable[RequestAndCompletionHandler] = {
    val requestsToSend = new mutable.Queue[RequestAndCompletionHandler]
    while (requestQueue.peek() != null) {
      val topRequest = requestQueue.take()
      val request = RequestAndCompletionHandler(activeController.get, topRequest.request, handleResponse(topRequest))
      requestsToSend.enqueue(request)
    }
    requestsToSend
  }

  private[server] def handleResponse(request: BrokerToControllerQueueItem)(response: ClientResponse): Unit = {
    if (response.wasDisconnected()) {
      activeController = None
      requestQueue.put(request)
    } else if (response.responseBody().errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
      // just close the controller connection and wait for metadata cache update in doWork
      networkClient.close(activeController.get.idString)
      activeController = None
      requestQueue.put(request)
    } else {
      request.callback.onComplete(response)
    }
  }

  private[server] def backoff(): Unit = pause(100, TimeUnit.MILLISECONDS)

  override def doWork(): Unit = {
    try {
      if (activeController.isDefined) {
        super.doWork()
      } else {
        info("Controller isn't cached, looking for local metadata changes")
        val controllerOpt = metadataCache.getControllerId.flatMap(metadataCache.getAliveBroker)
        if (controllerOpt.isDefined) {
          if (activeController.isEmpty || activeController.exists(_.id != controllerOpt.get.id))
            info(s"Recorded new controller, from now on will use broker ${controllerOpt.get.id}")
          activeController = Option(controllerOpt.get.node(listenerName))
          metadataUpdater.setNodes(metadataCache.getAliveBrokers.map(_.node(listenerName)).asJava)
        } else {
          // need to backoff to avoid tight loops
          warn("No controller defined in metadata cache, retrying after backoff")
          backoff()
        }
      }
    } catch {
      case e: Exception =>
        activeController match {
          case Some(controller) => {}
            warn(s"Broker ${config.brokerId}'s connection to active controller $controller was unsuccessful", e)
            networkClient.close(controller.idString)
            activeController = None
          case None =>
            warn(s"No active controller", e)
            backoff()
        }
    }
  }
}
