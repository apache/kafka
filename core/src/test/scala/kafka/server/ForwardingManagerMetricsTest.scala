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

package kafka.server

import java.util.Collections
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.Metrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._
import scala.util.Using

final class ForwardingManagerMetricsTest {
  @Test
  def testMetricsNames(): Unit = {
    val metrics = new Metrics()
    val expectedGroup = "ForwardingManager"

    val expectedMetrics = Set(
      new MetricName("QueueTimeMs.p99", expectedGroup, "", Collections.emptyMap()),
      new MetricName("QueueTimeMs.p999", expectedGroup, "", Collections.emptyMap()),
      new MetricName("QueueLength", expectedGroup, "", Collections.emptyMap()),
      new MetricName("RemoteTimeMs.p99", expectedGroup, "", Collections.emptyMap()),
      new MetricName("RemoteTimeMs.p999", expectedGroup, "", Collections.emptyMap())
    )

    Using(ForwardingManagerMetrics(metrics)) { _ =>
      val metricsMap = metrics.metrics().asScala.filter { case (name, _) => name.group == expectedGroup }
      assertEquals(metricsMap.size, expectedMetrics.size)
      metricsMap.foreach { case (name, _) =>
        assertTrue(expectedMetrics.contains(name))
      }
    }

    val metricsMap = metrics.metrics().asScala.filter { case (name, _) => name.group == expectedGroup }
    assertEquals(0, metricsMap.size)
  }

  @Test
  def testQueueTimeMs(): Unit = {
    val metrics = new Metrics()

    Using(ForwardingManagerMetrics(metrics)) { forwardingManagerMetrics =>
      val queueTimeMsP99 = metrics.metrics().get(forwardingManagerMetrics.queueTimeMsHist.latencyP99Name)
      val queueTimeMsP999 = metrics.metrics().get(forwardingManagerMetrics.queueTimeMsHist.latencyP999Name)
      assertEquals(Double.NaN, queueTimeMsP99.metricValue.asInstanceOf[Double])
      assertEquals(Double.NaN, queueTimeMsP999.metricValue.asInstanceOf[Double])
      for(i <- 0 to 999) {
        forwardingManagerMetrics.queueTimeMsHist.record(i)
      }
      assertEquals(990.0, queueTimeMsP99.metricValue.asInstanceOf[Double])
      assertEquals(999.0, queueTimeMsP999.metricValue.asInstanceOf[Double])
    }
  }

  @Test
  def testQueueLength(): Unit = {
    val metrics = new Metrics()

    Using(ForwardingManagerMetrics(metrics)) { forwardingManagerMetrics =>
      val queueLength = metrics.metrics().get(forwardingManagerMetrics.queueLengthName)
      assertEquals(0, queueLength.metricValue.asInstanceOf[Int])
      forwardingManagerMetrics.queueLength.getAndIncrement()
      assertEquals(1, queueLength.metricValue.asInstanceOf[Int])
    }
  }

  @Test
  def testRemoteTimeMs(): Unit = {
    val metrics = new Metrics()

    Using(ForwardingManagerMetrics(metrics)) { forwardingManagerMetrics =>
      val remoteTimeMsP99 = metrics.metrics().get(forwardingManagerMetrics.remoteTimeMsHist.latencyP99Name)
      val remoteTimeMsP999 = metrics.metrics().get(forwardingManagerMetrics.remoteTimeMsHist.latencyP999Name)
      assertEquals(Double.NaN, remoteTimeMsP99.metricValue.asInstanceOf[Double])
      assertEquals(Double.NaN, remoteTimeMsP999.metricValue.asInstanceOf[Double])
      for (i <- 0 to 999) {
        forwardingManagerMetrics.remoteTimeMsHist.record(i)
      }
      assertEquals(990.0, remoteTimeMsP99.metricValue.asInstanceOf[Double])
      assertEquals(999.0, remoteTimeMsP999.metricValue.asInstanceOf[Double])
    }
  }

}
