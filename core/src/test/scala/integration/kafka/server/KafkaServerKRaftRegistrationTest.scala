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
import kafka.test.annotation.{ClusterTest, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.testkit.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.common.Uuid
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{Tag, Timeout}

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

  @ClusterTest(clusterType = Type.ZK, brokers = 3, metadataVersion = MetadataVersion.IBP_3_4_IV0)
  def testRegisterZkBrokerInKraft(zkCluster: ClusterInstance): Unit = {
    val clusterId = zkCluster.clusterId()

    // Bootstrap the ZK cluster ID into KRaft
    val kraftCluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_4_IV0).
        setClusterId(Uuid.fromString(clusterId)).
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build())
      .setConfigProp(KafkaConfig.MigrationEnabledProp, "true")
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Enable migration configs and restart brokers
      val props = kraftCluster.controllerClientProperties()
      val voters = props.get(RaftConfig.QUORUM_VOTERS_CONFIG)
      zkCluster.config().serverProperties().put(KafkaConfig.MigrationEnabledProp, "true")
      zkCluster.config().serverProperties().put(RaftConfig.QUORUM_VOTERS_CONFIG, voters)
      zkCluster.config().serverProperties().put(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
      zkCluster.config().serverProperties().put(KafkaConfig.ListenerSecurityProtocolMapProp, "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      zkCluster.rollingBrokerRestart()
      zkCluster.waitForReadyBrokers()

      try {
        // Wait until all three ZK brokers are registered with KRaft controller
        readyFuture.get(30, TimeUnit.SECONDS)
      } catch {
        case _: TimeoutException => fail("Did not see 3 brokers within 30 seconds")
        case t: Throwable => fail("Had some other error waiting for brokers", t)
      }
    } finally {
      kraftCluster.close()
    }
  }
}
