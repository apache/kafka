/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.integration.KafkaServerTestHarness
import kafka.network.{DataPlaneAcceptor, SocketServer}
import kafka.server.Server.STARTED
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.metadata.BrokerState
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertNull}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/**
 * Created by dengziming on 4/19/22.
 */
class BrokerServerTest extends KafkaServerTestHarness {

  override def generateConfigs: Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(3, null).map(KafkaConfig.fromProps).toSeq
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testBrokerServerStarted(quorum: String): Unit = {
    for (server <- brokers) {
      assertEquals(STARTED, server.asInstanceOf[BrokerServer].status)
      TestUtils.waitUntilTrue(() => BrokerState.RUNNING == server.brokerState, "Timeout waiting for BrokerServer starting")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testSocketServerMetricNames(quorum: String): Unit = {
    for (server <- brokers) {
      TestUtils.waitUntilTrue(() => BrokerState.RUNNING == server.brokerState, "Timeout waiting for BrokerServer starting")
      checkMetricNames(server.metrics, server.socketServer)
    }
  }

  private def checkMetricNames(metrics: Metrics, server: SocketServer): Unit = {
    assertNull(metrics.metric(metrics.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}NotExists", SocketServer.MetricsGroup)))

    // SocketServer metric
    assertNotNull(metrics.sensor(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}MemoryPoolUtilization"))
    assertNotNull(metrics.metric(metrics.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}MemoryPoolAvgDepletedPercent", SocketServer.MetricsGroup)))
    assertNotNull(metrics.metric(metrics.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}MemoryPoolDepletedTimeTotal", SocketServer.MetricsGroup)))
    assertNotNull(TestUtils.metric(server.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}${DataPlaneAcceptor.MetricPrefix}NetworkProcessorAvgIdlePercent", Map.empty)))
    assertNotNull(TestUtils.metric(server.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}MemoryPoolAvailable", Map.empty)))
    assertNotNull(TestUtils.metric(server.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}MemoryPoolUsed", Map.empty)))
    assertNotNull(TestUtils.metric(server.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}${DataPlaneAcceptor.MetricPrefix}ExpiredConnectionsKilledCount", Map.empty)))

    // KafkaRequestHandlerPool metric
    assertNotNull(TestUtils.metric(server.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}RequestHandlerAvgIdlePercent", Map.empty)))
  }
}
