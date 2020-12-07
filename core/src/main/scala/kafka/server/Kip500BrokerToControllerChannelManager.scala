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
import java.util.concurrent.{CompletableFuture, Future, LinkedBlockingDeque}

import org.apache.kafka.clients.{KafkaClient, NetworkClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.requests.{MetadataRequest, MetadataResponse}
import org.apache.kafka.common.utils.Time

import scala.util.Random

/**
 * This class uses the controller.connect configuration to find and connect to the controller and uses
 * MetadataRequest to identify the active one.
 */
class Kip500BrokerToControllerChannelManager(time: Time,
                                             metrics: Metrics,
                                             config: KafkaConfig,
                                             channelName: String,
                                             threadNamePrefix: Option[String] = None) extends
  AbstractBrokerToControllerChannelManager(time, metrics, config, channelName, threadNamePrefix) {

  val clusterIdFuture = new CompletableFuture[String]()
  override def clusterId(): Future[String] = clusterIdFuture

  val _brokerToControllerListenerName = new ListenerName(config.controllerListenerNames.head)
  if (_brokerToControllerListenerName.value().isEmpty) {
    throw new IllegalStateException(s"Must set at least one value for ${KafkaConfig.ControllerListenerNamesProp}")
  }
  val _brokerToControllerSecurityProtocol =
    config.listenerSecurityProtocolMap.get(_brokerToControllerListenerName).getOrElse(
      throw new IllegalStateException(s"No mapping in ${KafkaConfig.ListenerSecurityProtocolMapProp} for ${_brokerToControllerListenerName.value()}"))
  val _brokerToControllerSaslMechanism = null // TODO: where will we define this?

  val _requestThread = newRequestThread
  override protected def requestThread = _requestThread

  override protected def brokerToControllerListenerName = _brokerToControllerListenerName

  override protected def brokerToControllerSecurityProtocol = _brokerToControllerSecurityProtocol

  override protected def brokerToControllerSaslMechanism = _brokerToControllerSaslMechanism

  override protected def instantiateRequestThread(networkClient: NetworkClient,
                                                  brokerToControllerListenerName: ListenerName,
                                                  threadName: String) = {
    new Kip500BrokerToControllerRequestThread(networkClient, requestQueue, config, time, threadName, clusterIdFuture)
  }

}

class Kip500BrokerToControllerRequestThread(networkClient: KafkaClient,
                                            requestQueue: LinkedBlockingDeque[BrokerToControllerQueueItem],
                                            config: KafkaConfig,
                                            time: Time,
                                            threadName: String,
                                            clusterIdFuture: CompletableFuture[String])
  extends BrokerToControllerRequestThread(networkClient, requestQueue, config, time, threadName) {
  private var lastTriedActiveController: Option[Node] = None
  val random = new Random
  val nodes = config.controllerConnectNodes.toList

  override def shutdown(): Unit = {
    if (!clusterIdFuture.isDone) {
      clusterIdFuture.cancel(true)
    }
    super.shutdown()
  }

  override def doWork(): Unit = {
    if (activeController.isDefined) {
      trace(s"Active controller is defined: ${activeController.get}")
      lastTriedActiveController = activeController
      super.doWork()
    } else {
      val metadataRequestData = new MetadataRequestData()
        .setAllowAutoTopicCreation(false)
        .setIncludeClusterAuthorizedOperations(false)
        .setIncludeTopicAuthorizedOperations(false)
        .setTopics(Collections.emptyList())
      // select a random node that is not the one we were previously using (if any)
      val nodesToChooseFrom = nodes.filter {
        lastTriedActiveController.isEmpty || _.id() != lastTriedActiveController.get.id()
      }
      val node = if(nodesToChooseFrom.isEmpty) {
        nodes(0)
      } else {
        nodesToChooseFrom(random.nextInt(nodesToChooseFrom.length))
      }
      info(s"Active Controller isn't known, sending metadata request data $metadataRequestData to node $node")
      // The base class implementation assumes the active controller is always set,
      // so we have to set it to the chosen node in order for this request to succeed.
      activeController = Some(node) // temporary for just this request
      lastTriedActiveController = activeController
      requestQueue.putFirst(BrokerToControllerQueueItem(new MetadataRequest.Builder(metadataRequestData),
        response => {
          val metadataResponse = response.responseBody().asInstanceOf[MetadataResponse]
          if (metadataResponse.controller().id() > 0) {
            activeController = Option(metadataResponse.controller())
            lastTriedActiveController = activeController
            if (!clusterIdFuture.isDone) {
              clusterIdFuture.complete(metadataResponse.clusterId())
            }
          } else {
            // need to backoff to avoid tight loops
            info(s"No active Controller defined; retrying after backoff")
            activeController = None // so we try again
            backoff()
          }
        }
      ))
      super.doWork() // submit it!
    }
  }
}
