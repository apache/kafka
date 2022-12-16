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

package unit.kafka.server

import kafka.server.{BrokerToControllerChannelManager, ControllerInformation, ControllerNodeProvider, ControllerRequestCompletionHandler}
import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterConfigProperty, ClusterTest, Type}
import kafka.test.junit.ClusterTestExtensions
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.message.{BrokerRegistrationRequestData, BrokerRegistrationResponseData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{BrokerRegistrationRequest, BrokerRegistrationResponse}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, Uuid}
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{Tag, Timeout}

import java.util.concurrent.{CompletableFuture, TimeUnit, TimeoutException}

/**
 * This test simulates a broker registering with the KRaft quorum under different configurations.
 */
@Timeout(120)
@Tag("integration")
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class BrokerRegistrationRequestTest {

  def brokerToControllerChannelManager(clusterInstance: ClusterInstance): BrokerToControllerChannelManager = {
    BrokerToControllerChannelManager(
      new ControllerNodeProvider() {
        def node: Option[Node] = Some(new Node(
          clusterInstance.anyControllerSocketServer().config.nodeId,
          "127.0.0.1",
          clusterInstance.anyControllerSocketServer().boundPort(clusterInstance.controllerListenerName().get()),
        ))

        def listenerName: ListenerName = clusterInstance.controllerListenerName().get()

        val securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT

        val saslMechanism: String = ""

        def isZkController: Boolean = !clusterInstance.isKRaftTest

        override def getControllerInfo(): ControllerInformation =
          ControllerInformation(node, listenerName, securityProtocol, saslMechanism, isZkController)
      },
      Time.SYSTEM,
      new Metrics(),
      clusterInstance.anyControllerSocketServer().config,
      "heartbeat",
      Some("heartbeat"),
      10000
    )
  }

  def sendAndRecieve(
    channelManager: BrokerToControllerChannelManager,
    req: BrokerRegistrationRequestData
  ): BrokerRegistrationResponseData = {
    val responseFuture = new CompletableFuture[BrokerRegistrationResponseData]()
    channelManager.sendRequest(new BrokerRegistrationRequest.Builder(req), new ControllerRequestCompletionHandler() {
      override def onTimeout(): Unit = responseFuture.completeExceptionally(new TimeoutException())

      override def onComplete(response: ClientResponse): Unit =
        responseFuture.complete(response.responseBody().asInstanceOf[BrokerRegistrationResponse].data())
    })
    responseFuture.get(30, TimeUnit.SECONDS)
  }

  def registerBroker(
    channelManager: BrokerToControllerChannelManager,
    clusterId: String,
    brokerId: Int,
    zk: Boolean,
    ibpToSend: Option[(MetadataVersion, MetadataVersion)]
  ): Errors = {
    val features = new BrokerRegistrationRequestData.FeatureCollection()

    ibpToSend foreach {
      case (min, max) =>
        features.add(new BrokerRegistrationRequestData.Feature()
          .setName(MetadataVersion.FEATURE_NAME)
          .setMinSupportedVersion(min.featureLevel())
          .setMaxSupportedVersion(max.featureLevel())
        )
    }

    val req = new BrokerRegistrationRequestData()
      .setBrokerId(brokerId)
      .setClusterId(clusterId)
      .setIncarnationId(Uuid.randomUuid())
      .setIsMigratingZkBroker(zk)
      .setFeatures(features)

    Errors.forCode(sendAndRecieve(channelManager, req).errorCode())
  }

  @ClusterTest(clusterType = Type.KRAFT, brokers = 0, controllers = 1, metadataVersion = MetadataVersion.IBP_3_4_IV0,
    serverProperties = Array(new ClusterConfigProperty(key = "zookeeper.metadata.migration.enable", value = "false")))
  def testRegisterZkWithKRaftMigrationDisabled(clusterInstance: ClusterInstance): Unit = {
    val clusterId = clusterInstance.clusterId()
    val channelManager = brokerToControllerChannelManager(clusterInstance)
    try {
      channelManager.start()

      assertEquals(
        Errors.BROKER_ID_NOT_REGISTERED,
        registerBroker(channelManager, clusterId, 100, true, Some((MetadataVersion.IBP_3_3_IV0, MetadataVersion.IBP_3_3_IV0))))

      assertEquals(
        Errors.BROKER_ID_NOT_REGISTERED,
        registerBroker(channelManager, clusterId, 100, true, None))

      assertEquals(
        Errors.BROKER_ID_NOT_REGISTERED,
        registerBroker(channelManager, clusterId, 100, true, Some((MetadataVersion.IBP_3_4_IV0, MetadataVersion.IBP_3_4_IV0))))

      assertEquals(
        Errors.NONE,
        registerBroker(channelManager, clusterId, 100, false, Some((MetadataVersion.IBP_3_4_IV0, MetadataVersion.IBP_3_4_IV0))))
    } finally {
      channelManager.shutdown()
    }
  }

  @ClusterTest(clusterType = Type.KRAFT, brokers = 0, controllers = 1, metadataVersion = MetadataVersion.IBP_3_3_IV3,
    serverProperties = Array(new ClusterConfigProperty(key = "zookeeper.metadata.migration.enable", value = "true")))
  def testRegisterZkWithKRaftOldMetadataVersion(clusterInstance: ClusterInstance): Unit = {
    val clusterId = clusterInstance.clusterId()
    val channelManager = brokerToControllerChannelManager(clusterInstance)
    try {
      channelManager.start()

      assertEquals(
        Errors.BROKER_ID_NOT_REGISTERED,
        registerBroker(channelManager, clusterId, 100, true, Some((MetadataVersion.IBP_3_3_IV0, MetadataVersion.IBP_3_3_IV0))))

      assertEquals(
        Errors.BROKER_ID_NOT_REGISTERED,
        registerBroker(channelManager, clusterId, 100, true, None))

      assertEquals(
        Errors.BROKER_ID_NOT_REGISTERED,
        registerBroker(channelManager, clusterId, 100, true, Some((MetadataVersion.IBP_3_4_IV0, MetadataVersion.IBP_3_4_IV0))))

      assertEquals(
        Errors.NONE,
        registerBroker(channelManager, clusterId, 100, false, Some((MetadataVersion.IBP_3_3_IV3, MetadataVersion.IBP_3_4_IV0))))
    } finally {
      channelManager.shutdown()
    }
  }

  @ClusterTest(clusterType = Type.KRAFT, brokers = 0, controllers = 1, metadataVersion = MetadataVersion.IBP_3_4_IV0,
    serverProperties = Array(new ClusterConfigProperty(key = "zookeeper.metadata.migration.enable", value = "true")))
  def testRegisterZkWithKRaftMigrationEnabled(clusterInstance: ClusterInstance): Unit = {
    val clusterId = clusterInstance.clusterId()
    val channelManager = brokerToControllerChannelManager(clusterInstance)
    try {
      channelManager.start()

      assertEquals(
        Errors.NONE,
        registerBroker(channelManager, clusterId, 100, true, Some((MetadataVersion.IBP_3_4_IV0, MetadataVersion.IBP_3_4_IV0))))

      assertEquals(
        Errors.UNSUPPORTED_VERSION,
        registerBroker(channelManager, clusterId, 100, true, None))

      assertEquals(
        Errors.UNSUPPORTED_VERSION,
        registerBroker(channelManager, clusterId, 100, true, Some((MetadataVersion.IBP_3_3_IV3, MetadataVersion.IBP_3_3_IV3))))

      assertEquals(
        Errors.NONE,
        registerBroker(channelManager, clusterId, 100, false, Some((MetadataVersion.IBP_3_4_IV0, MetadataVersion.IBP_3_4_IV0))))
    } finally {
      channelManager.shutdown()
    }
  }
}
