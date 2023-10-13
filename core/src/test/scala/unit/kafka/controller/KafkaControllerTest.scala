/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.controller

import kafka.server.metadata.ZkMetadataCache
import kafka.server.{BrokerFeatures, DelegationTokenManager, KafkaConfig}
import kafka.utils.TestUtils
import kafka.zk.{BrokerInfo, KafkaZkClient}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.MockTime
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{mock, mockConstruction, times, verify, verifyNoMoreInteractions}

class KafkaControllerTest {
  var config: KafkaConfig = _

  @BeforeEach
  def setUp(): Unit = {
    val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
    config = KafkaConfig.fromProps(props)
  }

  @Test
  def testRemoveMetricsOnClose(): Unit = {
    val mockMetricsGroupCtor = mockConstruction(classOf[KafkaMetricsGroup])
    try {
      val kafkaController = new KafkaController(
        config = config,
        zkClient = mock(classOf[KafkaZkClient]),
        time = new MockTime(),
        metrics = mock(classOf[Metrics]),
        initialBrokerInfo = mock(classOf[BrokerInfo]),
        initialBrokerEpoch = 0,
        tokenManager = mock(classOf[DelegationTokenManager]),
        brokerFeatures = mock(classOf[BrokerFeatures]),
        featureCache = mock(classOf[ZkMetadataCache])
      )

      // shutdown kafkaController so that metrics are removed
      kafkaController.shutdown()

      val mockMetricsGroup = mockMetricsGroupCtor.constructed.get(0)
      val numMetricsRegistered = KafkaController.MetricNames.size
      verify(mockMetricsGroup, times(numMetricsRegistered)).newGauge(anyString(), any())
      KafkaController.MetricNames.foreach(metricName => verify(mockMetricsGroup).newGauge(ArgumentMatchers.eq(metricName), any()))
      // verify that each metric is removed
      verify(mockMetricsGroup, times(numMetricsRegistered)).removeMetric(anyString())
      KafkaController.MetricNames.foreach(verify(mockMetricsGroup).removeMetric(_))

      // assert that we have verified all invocations on
      verifyNoMoreInteractions(mockMetricsGroup)
    } finally {
      mockMetricsGroupCtor.close()
    }
  }

}
