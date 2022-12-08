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
import kafka.test.annotation.{AutoStart, ClusterTest, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.junit.ZkClusterInvocationContext.ZkClusterInstance
import kafka.testkit.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.common.Uuid
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.extension.ExtendWith


@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class KafkaServerKRaftRegistrationTest {

  @ClusterTest(clusterType = Type.ZK, metadataVersion = MetadataVersion.IBP_3_4_IV0, autoStart = AutoStart.NO)
  def testRegisterZkBrokerInKraft(zkCluster: ClusterInstance): Unit = {
    zkCluster.start()
    val clusterId = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient.getClusterId.get

    // Bootstrap the ZK cluster ID into KRaft
    val kraftCluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setClusterId(Uuid.fromString(clusterId)).
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build()).build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val props = kraftCluster.controllerClientProperties()
      val voters = props.get(RaftConfig.QUORUM_VOTERS_CONFIG)

      // Enable migration configs
      zkCluster.config().serverProperties().put(KafkaConfig.MigrationEnabledProp, "true")
      zkCluster.config().serverProperties().put(RaftConfig.QUORUM_VOTERS_CONFIG, voters)
      zkCluster.config().serverProperties().put(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
      zkCluster.config().serverProperties().put(KafkaConfig.ListenerSecurityProtocolMapProp, "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      zkCluster.rollingBrokerRestart()
      Thread.sleep(10000)
    } finally {
      zkCluster.stop()
      kraftCluster.close()
    }
  }
}
