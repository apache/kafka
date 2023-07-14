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

package unit.kafka.server

import kafka.network.DataPlaneAcceptor.ThreadPrefix
import kafka.network.RequestChannel
import kafka.server.{ApiRequestHandler, KafkaRequestHandlerPool}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, mockConstruction, verify, verifyNoMoreInteractions}

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * @Author: deqi.hu@shopee.com
 * @Date: 2023/7/14 17:51
 */
class KafkaRequestHandlerTest {

  @Test
  def testRemoveMetricsOnClose(): Unit = {
    val mockMetricsGroupCtor = mockConstruction(classOf[KafkaMetricsGroup])
    try {
      val requestHandlerAvgIdleMetricName = "testRequestHandlerAvgIdleMetricName"
      val kafkaRequestHandlerPool = new KafkaRequestHandlerPool(
        1,
        mock(classOf[RequestChannel]),
        mock(classOf[ApiRequestHandler]),
        Time.SYSTEM,
        0, // so that easy test metrics remove, without caring other executing
        requestHandlerAvgIdleMetricName,
        ThreadPrefix
      )
      val metricsToVerify = new java.util.HashSet[String]()
      kafkaRequestHandlerPool.metricNames.foreach(metricsToVerify.add)

      // shutdown kafkaRequestHandlerPool so that metrics are removed
      kafkaRequestHandlerPool.shutdown()

      val mockMetricsGroup = mockMetricsGroupCtor.constructed.get(0)
      metricsToVerify.asScala.foreach(metricName => verify(mockMetricsGroup).newMeter(ArgumentMatchers.eq(metricName), any(), any()))
      metricsToVerify.asScala.foreach(verify(mockMetricsGroup).removeMetric(_))

      // assert that we have verified all invocations on
      verifyNoMoreInteractions(mockMetricsGroup)
    } finally {
      mockMetricsGroupCtor.close()
    }
  }
}
