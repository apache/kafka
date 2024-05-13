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

import java.util.Properties

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.metrics.MetricsContext
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.config.{KRaftConfigs, ZkConfigs}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class ServerTest {

  @Test
  def testCreateSelfManagedKafkaMetricsContext(): Unit = {
    val nodeId = 0
    val clusterId = Uuid.randomUuid().toString

    val props = new Properties()
    props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.put(KRaftConfigs.NODE_ID_CONFIG, nodeId.toString)
    props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"${nodeId + 1}@localhost:9093")
    props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    val config = KafkaConfig.fromProps(props)

    val context = Server.createKafkaMetricsContext(config, clusterId)
    assertEquals(Map(
      MetricsContext.NAMESPACE -> Server.MetricsPrefix,
      Server.ClusterIdLabel -> clusterId,
      Server.NodeIdLabel -> nodeId.toString
    ), context.contextLabels.asScala)
  }

  @Test
  def testCreateZkKafkaMetricsContext(): Unit = {
    val brokerId = 0
    val clusterId = Uuid.randomUuid().toString

    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, brokerId.toString)
    props.put(ZkConfigs.ZK_CONNECT_CONFIG, "127.0.0.1:0")
    val config = KafkaConfig.fromProps(props)

    val context = Server.createKafkaMetricsContext(config, clusterId)
    assertEquals(Map(
      MetricsContext.NAMESPACE -> Server.MetricsPrefix,
      Server.ClusterIdLabel -> clusterId,
      Server.BrokerIdLabel -> brokerId.toString
    ), context.contextLabels.asScala)
  }

}
