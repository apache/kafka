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

import org.apache.kafka.clients.{Metadata, MockClient, NodeApiVersions}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.Node
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion
import org.apache.kafka.common.message.BrokerRegistrationRequestData.{Listener, ListenerCollection}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys.{BROKER_HEARTBEAT, BROKER_REGISTRATION, CONTROLLER_REGISTRATION}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.server.util.MockTime

import java.util.Properties
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
import scala.jdk.CollectionConverters._

class SimpleControllerNodeProvider extends ControllerNodeProvider {
  val node = new AtomicReference[Node](null)

  def listenerName: ListenerName = new ListenerName("PLAINTEXT")

  def securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT

  def saslMechanism: String = SaslConfigs.DEFAULT_SASL_MECHANISM

  override def getControllerInfo(): ControllerInformation = ControllerInformation(Option(node.get()),
    listenerName, securityProtocol, saslMechanism, isZkController = false)
}

class RegistrationTestContext(
  properties: Properties
) {
  val config = new KafkaConfig(properties)
  val time = new MockTime(1, 1)
  val highestMetadataOffset = new AtomicLong(0)
  val metadata = new Metadata(1000, 1000, 1000, new LogContext(), new ClusterResourceListeners())
  val mockClient = new MockClient(time, metadata)
  val controllerNodeProvider = new SimpleControllerNodeProvider()
  val nodeApiVersions = NodeApiVersions.create(Seq(BROKER_REGISTRATION, BROKER_HEARTBEAT, CONTROLLER_REGISTRATION).map {
    apiKey => new ApiVersion().setApiKey(apiKey.id).
      setMinVersion(apiKey.oldestVersion()).setMaxVersion(apiKey.latestVersion())
  }.toList.asJava)
  val mockChannelManager = new MockNodeToControllerChannelManager(mockClient,
    time, controllerNodeProvider, nodeApiVersions)
  val clusterId = "x4AJGXQSRnephtTZzujw4w"
  val advertisedListeners = new ListenerCollection()
  val controllerEpoch = new AtomicInteger(123)
  config.effectiveAdvertisedListeners.foreach { ep =>
    advertisedListeners.add(new Listener().setHost(ep.host).
      setName(ep.listenerName.value()).
      setPort(ep.port.shortValue()).
      setSecurityProtocol(ep.securityProtocol.id))
  }

  def poll(): Unit = {
    mockClient.wakeup()
    mockChannelManager.poll()
  }
}
