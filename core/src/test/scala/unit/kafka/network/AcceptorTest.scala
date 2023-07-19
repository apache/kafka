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

package unit.kafka.network

import kafka.cluster.EndPoint
import kafka.network.{ConnectionQuotas, ControlPlaneAcceptor, DataPlaneAcceptor, RequestChannel, SocketServer}
import kafka.security.CredentialProvider
import kafka.server.{ApiVersionManager, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, mockConstruction, verify, verifyNoMoreInteractions}

import scala.jdk.CollectionConverters._

class AcceptorTest {

  @Test
  def testRemoveMetricsOnClose(): Unit = {
    val mockMetricsGroupCtor = mockConstruction(classOf[KafkaMetricsGroup])
    try {
      val socketServer = mock(classOf[SocketServer])
      val securityProtocol = SecurityProtocol.PLAINTEXT
      val dataPlaneEndpoint = new EndPoint("localhost", 9092, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol)
      val controllerPlaneEndpoint = new EndPoint("localhost", 9093, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol)
      val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
      val connectionQuotas = mock(classOf[ConnectionQuotas])
      val dataPlaneRequestChannel = mock(classOf[RequestChannel])
      val controllerPlaneRequestChannel = mock(classOf[RequestChannel])
      val metrics = mock(classOf[Metrics])
      val credentialProvider = mock(classOf[CredentialProvider])
      val logContext = mock(classOf[LogContext])
      val memoryPool = mock(classOf[MemoryPool])
      val apiVersionManager = mock(classOf[ApiVersionManager])

      val dataPlaneAcceptor = new DataPlaneAcceptor(
        socketServer,
        dataPlaneEndpoint,
        KafkaConfig.fromProps(props),
        0,
        connectionQuotas,
        Time.SYSTEM,
        false,
        dataPlaneRequestChannel,
        metrics,
        credentialProvider,
        logContext,
        memoryPool,
        apiVersionManager
      )
      val meterMetricNamesWithTagToVerify = new java.util.HashMap[com.yammer.metrics.core.MetricName, java.util.Map[String, String]]()
      dataPlaneAcceptor.meterMetricNamesWithTag.asScala.foreach { metricNameAndTags =>
        meterMetricNamesWithTagToVerify.put(metricNameAndTags._1, metricNameAndTags._2)
      }
      // shutdown `dataPlaneAcceptor` so that metrics are removed
      dataPlaneAcceptor.close()
      val mockMetricsGroupForDataPlane = mockMetricsGroupCtor.constructed.get(0)
      meterMetricNamesWithTagToVerify.asScala.foreach { metricNameAndTags =>
        verify(mockMetricsGroupForDataPlane).newMeter(ArgumentMatchers.eq(metricNameAndTags._1), any(), any())
      }
      meterMetricNamesWithTagToVerify.asScala.foreach { metricNameAndTags =>
        verify(mockMetricsGroupForDataPlane).removeMetric(ArgumentMatchers.eq(metricNameAndTags._1.getName),
          ArgumentMatchers.eq(metricNameAndTags._2))
      }
      // assert that we have verified all invocations on
      verifyNoMoreInteractions(mockMetricsGroupForDataPlane)

      val controllerPlaneAcceptor = new ControlPlaneAcceptor(
        socketServer,
        controllerPlaneEndpoint,
        KafkaConfig.fromProps(props),
        0,
        connectionQuotas,
        Time.SYSTEM,
        controllerPlaneRequestChannel,
        metrics,
        credentialProvider,
        logContext,
        memoryPool,
        apiVersionManager
      )
      meterMetricNamesWithTagToVerify.clear()
      controllerPlaneAcceptor.meterMetricNamesWithTag.asScala.foreach { metricNameAndTags =>
        meterMetricNamesWithTagToVerify.put(metricNameAndTags._1, metricNameAndTags._2)
      }
      // shutdown `controllerPlaneAcceptor` so that metrics are removed
      controllerPlaneAcceptor.close()
      val mockMetricsGroupForControllerPlane = mockMetricsGroupCtor.constructed.get(1)
      meterMetricNamesWithTagToVerify.asScala.foreach { metricNameAndTags =>
        verify(mockMetricsGroupForControllerPlane).newMeter(ArgumentMatchers.eq(metricNameAndTags._1), any(), any())
      }
      meterMetricNamesWithTagToVerify.asScala.foreach { metricNameAndTags =>
        verify(mockMetricsGroupForControllerPlane).removeMetric(ArgumentMatchers.eq(metricNameAndTags._1.getName),
          ArgumentMatchers.eq(metricNameAndTags._2))
      }
      // assert that we have verified all invocations on
      verifyNoMoreInteractions(mockMetricsGroupForControllerPlane)
    } finally {
      mockMetricsGroupCtor.close()
    }
  }
}
