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

import kafka.test.{ClusterConfig, ClusterInstance}
import kafka.test.annotation.{ClusterConfigProperty, ClusterTest, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.junit.ZkClusterInvocationContext.ZkClusterInstance
import kafka.testkit.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.common.Uuid
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.{KRaftConfigs, ZkConfigs}
import org.junit.jupiter.api.Assertions.{assertThrows, fail}
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{Tag, Timeout}

import java.util.Optional
import java.util.concurrent.{TimeUnit, TimeoutException}
import scala.jdk.CollectionConverters._


/**
 * This test creates a full ZK cluster and a controller-only KRaft cluster and configures the ZK brokers to register
 * themselves with the KRaft controller. This is mainly a happy-path test since the only way to reliably test the
 * failure paths is to use timeouts. See {@link unit.kafka.server.BrokerRegistrationRequestTest} for integration test
 * of just the broker registration path.
 */
@Timeout(120)
@Tag("integration")
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class KafkaServerKRaftRegistrationTest {

  @ClusterTest(types = Array(Type.ZK), brokers = 3, metadataVersion = MetadataVersion.IBP_3_4_IV0, serverProperties = Array(
    new ClusterConfigProperty(key = "inter.broker.listener.name", value = "EXTERNAL"),
    new ClusterConfigProperty(key = "listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "advertised.listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "listener.security.protocol.map", value = "EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
  ))
  def testRegisterZkBrokerInKraft(zkCluster: ClusterInstance): Unit = {
    val clusterId = zkCluster.clusterId()

    // Bootstrap the ZK cluster ID into KRaft
    val kraftCluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_4_IV0).
        setClusterId(Uuid.fromString(clusterId)).
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build())
      .setConfigProp(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      .setConfigProp(ZkConfigs.ZK_CONNECT_CONFIG, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Enable migration configs and restart brokers
      val serverProperties = new java.util.HashMap[String, String](zkCluster.config().serverProperties())
      serverProperties.put(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      serverProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig())
      serverProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
      serverProperties.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      val clusterConfig = ClusterConfig.builder(zkCluster.config())
        .setServerProperties(serverProperties)
        .build()
      zkCluster.asInstanceOf[ZkClusterInstance].rollingBrokerRestart(Optional.of(clusterConfig))
      zkCluster.waitForReadyBrokers()

      try {
        // Wait until all three ZK brokers are registered with KRaft controller
        readyFuture.get(30, TimeUnit.SECONDS)
      } catch {
        case _: TimeoutException => fail("Did not see 3 brokers within 30 seconds")
        case t: Throwable => fail("Had some other error waiting for brokers", t)
      }
    } finally {
      shutdownInSequence(zkCluster, kraftCluster)
    }
  }

  @ClusterTest(types = Array(Type.ZK), brokers = 3, metadataVersion = MetadataVersion.IBP_3_3_IV0)
  def testRestartOldIbpZkBrokerInMigrationMode(zkCluster: ClusterInstance): Unit = {
    // Bootstrap the ZK cluster ID into KRaft
    val clusterId = zkCluster.clusterId()
    val kraftCluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_4_IV0).
        setClusterId(Uuid.fromString(clusterId)).
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build())
      .setConfigProp(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      .setConfigProp(ZkConfigs.ZK_CONNECT_CONFIG, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()

      // Enable migration configs and restart brokers
      val serverProperties = new java.util.HashMap[String, String](zkCluster.config().serverProperties())
      serverProperties.put(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      serverProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig())
      serverProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
      serverProperties.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      val clusterConfig = ClusterConfig.builder(zkCluster.config())
        .setServerProperties(serverProperties)
        .build()
      assertThrows(classOf[IllegalArgumentException], () => zkCluster.asInstanceOf[ZkClusterInstance].rollingBrokerRestart(Optional.of(clusterConfig)))
    } finally {
      shutdownInSequence(zkCluster, kraftCluster)
    }
  }

  def shutdownInSequence(zkCluster: ClusterInstance, kraftCluster: KafkaClusterTestKit): Unit = {
    zkCluster.brokerIds().forEach(zkCluster.shutdownBroker(_))
    kraftCluster.close()
    zkCluster.stop()
  }
}
