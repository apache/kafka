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
import kafka.testkit.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.extension.ExtendWith


@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class KafkaServerKRaftRegistrationTest {

  @ClusterTest(clusterType = Type.ZK, metadataVersion = MetadataVersion.IBP_3_4_IV0, autoStart = AutoStart.NO)
  def testRegisterZkBrokerInKraft(clusterInstance: ClusterInstance): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build()).build()
    try {
      cluster.format()
      cluster.startup()
      val props = cluster.controllerClientProperties()
      val voters = props.get(RaftConfig.QUORUM_VOTERS_CONFIG)

      clusterInstance.config().serverProperties().put(KafkaConfig.MigrationEnabledProp, "true")
      clusterInstance.config().serverProperties().put(RaftConfig.QUORUM_VOTERS_CONFIG, voters)
      clusterInstance.config().serverProperties().put(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
      clusterInstance.start()

      Thread.sleep(60000)
      clusterInstance.stop()
    } finally {
      cluster.close()
    }
  }
}
