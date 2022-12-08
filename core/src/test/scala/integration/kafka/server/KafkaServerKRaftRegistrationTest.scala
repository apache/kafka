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

import kafka.test.annotation.{ClusterTemplate, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.junit.ZkClusterInvocationContext.ZkClusterInstance
import kafka.test.{ClusterConfig, ClusterGenerator, ClusterInstance}
import kafka.testkit.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.common.Uuid
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{Tag, Timeout}

import java.util.concurrent.{TimeUnit, TimeoutException}
import scala.jdk.CollectionConverters._

object KafkaServerKRaftRegistrationTest {

  def buildClusterConfig(name: String,
                         zkMetadataVersion: MetadataVersion,
                         kraftMetadataVersion: MetadataVersion,
                         success: Boolean): ClusterConfig = {
    val config = ClusterConfig.defaultClusterBuilder()
      .name(name)
      .`type`(Type.ZK)
      .brokers(3)
      .metadataVersion(zkMetadataVersion)
      .build()
    config.serverProperties().put("test.kraft.metadata.version", kraftMetadataVersion.version())
    config.serverProperties().put("test.success", success.toString)
    config
  }

  def generateTestRuns(clusterGenerator: ClusterGenerator): Unit = {
    clusterGenerator.accept(buildClusterConfig("3.3->3.3", MetadataVersion.IBP_3_3_IV3, MetadataVersion.IBP_3_3_IV3, success = false))
    clusterGenerator.accept(buildClusterConfig("3.4->3.3", MetadataVersion.IBP_3_4_IV0, MetadataVersion.IBP_3_3_IV3, success = false))
    clusterGenerator.accept(buildClusterConfig("3.3->3.4", MetadataVersion.IBP_3_3_IV3, MetadataVersion.IBP_3_4_IV0, success = false))
    clusterGenerator.accept(buildClusterConfig("3.4->3.4", MetadataVersion.IBP_3_4_IV0, MetadataVersion.IBP_3_4_IV0, success = true))
  }
}

@Timeout(120)
@Tag("integration")
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class KafkaServerKRaftRegistrationTest {

  @ClusterTemplate("generateTestRuns")
  def testRegisterZkBrokerInKraft(zkCluster: ClusterInstance, config: ClusterConfig): Unit = {
    val clusterId = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient.getClusterId.get
    val kraftMetadataVersion = MetadataVersion.fromVersionString(config.serverProperties().get("test.kraft.metadata.version").toString)
    val shouldSucceed = config.serverProperties().get("test.success").toString.toBoolean

    // Bootstrap the ZK cluster ID into KRaft
    val kraftCluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(kraftMetadataVersion).
        setClusterId(Uuid.fromString(clusterId)).
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build()).build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = if (shouldSucceed) {
        kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)
      } else {
        kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(1)
      }

      // Enable migration configs and restart brokers
      val props = kraftCluster.controllerClientProperties()
      val voters = props.get(RaftConfig.QUORUM_VOTERS_CONFIG)
      zkCluster.config().serverProperties().put(KafkaConfig.MigrationEnabledProp, "true")
      zkCluster.config().serverProperties().put(RaftConfig.QUORUM_VOTERS_CONFIG, voters)
      zkCluster.config().serverProperties().put(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
      zkCluster.config().serverProperties().put(KafkaConfig.ListenerSecurityProtocolMapProp, "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      zkCluster.rollingBrokerRestart()
      zkCluster.waitForReadyBrokers()

      if (shouldSucceed) {
        try {
          readyFuture.get(30, TimeUnit.SECONDS)
        } catch {
          case _: TimeoutException => fail("Did not see 3 brokers within 30 seconds")
          case t: Throwable => fail("Had some other error waiting for brokers", t)
        }
      } else {
        try {
          readyFuture.get(10, TimeUnit.SECONDS)
          fail("Should not have any brokers after 10 seconds")
        } catch {
          case _: TimeoutException => // pass
          case _: AssertionError => // pass
          case t: Throwable => fail("Had some other error waiting for brokers", t)
        }
      }
    } finally {
      zkCluster.stop()
      kraftCluster.close()
    }
  }
}
