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

import java.util.concurrent.{CompletableFuture, Future, LinkedBlockingDeque}

import org.apache.kafka.clients.{KafkaClient, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.utils.Time

import scala.jdk.CollectionConverters._

/**
 * This class uses the broker's metadata cache as its own metadata to find and connect to the controller.
 */
class LegacyBrokerToControllerChannelManager(metadataCache: kafka.server.MetadataCache,
                                             time: Time,
                                             metrics: Metrics,
                                             config: KafkaConfig,
                                             clusterId: String,
                                             channelName: String,
                                             threadNamePrefix: Option[String] = None) extends
  AbstractBrokerToControllerChannelManager(time, metrics, config, channelName, threadNamePrefix) {
  val _brokerToControllerListenerName = config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
  val _brokerToControllerSecurityProtocol = config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)
  val _brokerToControllerSaslMechanism = config.saslMechanismInterBrokerProtocol

  val _requestThread = newRequestThread
  override protected def requestThread = _requestThread

  override protected def brokerToControllerListenerName = _brokerToControllerListenerName

  override protected def brokerToControllerSecurityProtocol = _brokerToControllerSecurityProtocol

  override protected def brokerToControllerSaslMechanism = _brokerToControllerSaslMechanism

  override protected def instantiateRequestThread(networkClient: NetworkClient,
                                          brokerToControllerListenerName: ListenerName,
                                          threadName: String) = {
    new LegacyBrokerToControllerRequestThread(networkClient, manualMetadataUpdater, requestQueue, metadataCache, config,
      brokerToControllerListenerName, time, threadName)
  }

  val clusterIdFuture = new CompletableFuture[String]()
  clusterIdFuture.complete(clusterId)
  override def clusterId(): Future[String] = clusterIdFuture
}

class LegacyBrokerToControllerRequestThread(networkClient: KafkaClient,
                                            metadataUpdater: ManualMetadataUpdater,
                                            requestQueue: LinkedBlockingDeque[BrokerToControllerQueueItem],
                                            metadataCache: kafka.server.MetadataCache,
                                            config: KafkaConfig,
                                            listenerName: ListenerName,
                                            time: Time,
                                            threadName: String)
  extends BrokerToControllerRequestThread(networkClient, requestQueue, config, time, threadName) {

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