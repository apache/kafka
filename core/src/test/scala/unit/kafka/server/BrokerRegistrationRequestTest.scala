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

import kafka.test.ClusterInstance
import kafka.test.annotation._
import kafka.test.junit.ClusterTestExtensions
import kafka.test.junit.RaftClusterInvocationContext.RaftClusterInstance
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.{BrokerRegistrationRequestData, CreateTopicsRequestData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, Uuid}
import org.apache.kafka.server.{ControllerRequestCompletionHandler, NodeToControllerChannelManager}
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
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

  def brokerToControllerChannelManager(clusterInstance: ClusterInstance): NodeToControllerChannelManager = {
    new NodeToControllerChannelManagerImpl(
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
      "test-heartbeat-",
      10000
    )
  }

  def sendAndReceive[T <: AbstractRequest, R <: AbstractResponse](
    channelManager: NodeToControllerChannelManager,
    reqBuilder: AbstractRequest.Builder[T],
    timeoutMs: Int
  ): R = {
    val responseFuture = new CompletableFuture[R]()
    channelManager.sendRequest(reqBuilder, new ControllerRequestCompletionHandler() {
      override def onTimeout(): Unit = responseFuture.completeExceptionally(new TimeoutException())

      override def onComplete(response: ClientResponse): Unit =
        responseFuture.complete(response.responseBody().asInstanceOf[R])
    })
    responseFuture.get(timeoutMs, TimeUnit.MILLISECONDS)
  }

  def registerBroker(
    channelManager: NodeToControllerChannelManager,
    clusterId: String,
    brokerId: Int,
    zkEpoch: Option[Long],
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
      .setIsMigratingZkBroker(zkEpoch.isDefined)
      .setFeatures(features)

    val resp = sendAndReceive[BrokerRegistrationRequest, BrokerRegistrationResponse](
      channelManager, new BrokerRegistrationRequest.Builder(req), 30000)
    Errors.forCode(resp.data().errorCode())
  }


  def createTopics(channelManager: NodeToControllerChannelManager,
                   topicName: String): Errors = {
    val createTopics = new CreateTopicsRequestData()
    createTopics.setTopics(new CreateTopicsRequestData.CreatableTopicCollection())
    createTopics.topics().add(new CreatableTopic().setName(topicName).setNumPartitions(10).setReplicationFactor(1))
    createTopics.setTimeoutMs(500)

    val req = new CreateTopicsRequest.Builder(createTopics)
    val resp = sendAndReceive[CreateTopicsRequest, CreateTopicsResponse](channelManager, req, 3000).data()
    Errors.forCode(resp.topics().find(topicName).errorCode())
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
        registerBroker(channelManager, clusterId, 100, Some(1), Some((MetadataVersion.IBP_3_3_IV0, MetadataVersion.IBP_3_3_IV0))))

      assertEquals(
        Errors.BROKER_ID_NOT_REGISTERED,
        registerBroker(channelManager, clusterId, 100, Some(1), None))

      assertEquals(
        Errors.BROKER_ID_NOT_REGISTERED,
        registerBroker(channelManager, clusterId, 100, Some(1), Some((MetadataVersion.IBP_3_4_IV0, MetadataVersion.IBP_3_4_IV0))))

      assertEquals(
        Errors.NONE,
        registerBroker(channelManager, clusterId, 100, None, Some((MetadataVersion.IBP_3_4_IV0, MetadataVersion.IBP_3_4_IV0))))
    } finally {
      channelManager.shutdown()
    }
  }

  @ClusterTest(clusterType = Type.KRAFT, brokers = 0, controllers = 1, metadataVersion = MetadataVersion.IBP_3_3_IV3,
    serverProperties = Array(new ClusterConfigProperty(key = "zookeeper.metadata.migration.enable", value = "false")))
  def testRegisterZkWith33Controller(clusterInstance: ClusterInstance): Unit = {
    // Verify that a controller running an old metadata.version cannot register a ZK broker
    val clusterId = clusterInstance.clusterId()
    val channelManager = brokerToControllerChannelManager(clusterInstance)
    try {
      channelManager.start()
      // Invalid registration (isMigratingZkBroker, but MV does not support migrations)
      assertEquals(
        Errors.BROKER_ID_NOT_REGISTERED,
        registerBroker(channelManager, clusterId, 100, Some(1), Some((MetadataVersion.IBP_3_3_IV0, MetadataVersion.IBP_3_3_IV3))))

      // No features (MV) sent with registration, controller can't verify
      assertEquals(
        Errors.BROKER_ID_NOT_REGISTERED,
        registerBroker(channelManager, clusterId, 100, Some(1), None))

      // Given MV is too high for controller to support
      assertEquals(
        Errors.BROKER_ID_NOT_REGISTERED,
        registerBroker(channelManager, clusterId, 100, Some(1), Some((MetadataVersion.IBP_3_4_IV0, MetadataVersion.IBP_3_4_IV0))))

      // Controller supports this MV and isMigratingZkBroker is false, so this one works
      assertEquals(
        Errors.NONE,
        registerBroker(channelManager, clusterId, 100, None, Some((MetadataVersion.IBP_3_3_IV3, MetadataVersion.IBP_3_4_IV0))))
    } finally {
      channelManager.shutdown()
    }
  }

  @ClusterTest(
    clusterType = Type.KRAFT,
    brokers = 1,
    controllers = 1,
    metadataVersion = MetadataVersion.IBP_3_4_IV0,
    autoStart = AutoStart.NO,
    serverProperties = Array(new ClusterConfigProperty(key = "zookeeper.metadata.migration.enable", value = "true")))
  def testRegisterZkWithKRaftMigrationEnabled(clusterInstance: ClusterInstance): Unit = {
    clusterInstance.asInstanceOf[RaftClusterInstance].controllers().forEach(_.startup())

    val clusterId = clusterInstance.clusterId()
    val channelManager = brokerToControllerChannelManager(clusterInstance)
    try {
      channelManager.start()

      assertEquals(
        Errors.NONE,
        registerBroker(channelManager, clusterId, 100, Some(1), Some((MetadataVersion.IBP_3_4_IV0, MetadataVersion.IBP_3_4_IV0))))

      assertEquals(
        Errors.UNSUPPORTED_VERSION,
        registerBroker(channelManager, clusterId, 100, Some(1), None))

      assertEquals(
        Errors.UNSUPPORTED_VERSION,
        registerBroker(channelManager, clusterId, 100, Some(1), Some((MetadataVersion.IBP_3_3_IV3, MetadataVersion.IBP_3_3_IV3))))

      // Cannot register KRaft broker when in pre-migration
      assertEquals(
        Errors.BROKER_ID_NOT_REGISTERED,
        registerBroker(channelManager, clusterId, 100, None, Some((MetadataVersion.IBP_3_4_IV0, MetadataVersion.IBP_3_4_IV0))))
    } finally {
      channelManager.shutdown()
    }
  }

  /**
   * Start a KRaft cluster with migrations enabled, verify that the controller does not accept metadata changes
   * through the RPCs. The migration never proceeds past pre-migration since no ZK brokers are registered.
   */
  @ClusterTests(Array(
    new ClusterTest(clusterType = Type.KRAFT, autoStart = AutoStart.NO, controllers = 1, metadataVersion = MetadataVersion.IBP_3_4_IV0,
      serverProperties = Array(new ClusterConfigProperty(key = "zookeeper.metadata.migration.enable", value = "true")))
  ))
  def testNoMetadataChangesInPreMigrationMode(clusterInstance: ClusterInstance): Unit = {
    clusterInstance.asInstanceOf[RaftClusterInstance].controllers().forEach(_.startup())

    val channelManager = brokerToControllerChannelManager(clusterInstance)
    try {
      channelManager.start()
      assertThrows(classOf[TimeoutException], () => createTopics(channelManager, "test-pre-migration"))
    } finally {
      channelManager.shutdown()
    }
  }
}
