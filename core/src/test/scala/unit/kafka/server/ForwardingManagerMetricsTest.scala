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

import kafka.server.ForwardingManagerMetrics
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.Metrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Collections
import scala.jdk.CollectionConverters._

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

    var metricsMap = metrics.metrics().asScala.filter { case (name, _) => name.group == expectedGroup }
    assertEquals(0, metricsMap.size)

    ForwardingManagerMetrics(metrics, 1000)
    metricsMap = metrics.metrics().asScala.filter { case (name, _) => name.group == expectedGroup }
    assertEquals(metricsMap.size, expectedMetrics.size)
    metricsMap.foreach { case (name, _) =>
      assertTrue(expectedMetrics.contains(name))
    }
  }

  @Test
  def testQueueTimeMs(): Unit = {
    val metrics = new Metrics()

    val forwardingManagerMetrics = ForwardingManagerMetrics(metrics, 1000)
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

  @Test
  def testQueueLength(): Unit = {
    val metrics = new Metrics()

    val forwardingManagerMetrics = ForwardingManagerMetrics(metrics, 1000)
    val queueLength = metrics.metrics().get(forwardingManagerMetrics.queueLengthName)
    assertEquals(0, queueLength.metricValue.asInstanceOf[Int])
    forwardingManagerMetrics.queueLength.getAndIncrement()
    assertEquals(1, queueLength.metricValue.asInstanceOf[Int])
  }

  @Test
  def testRemoteTimeMs(): Unit = {
    val metrics = new Metrics()

    val forwardingManagerMetrics = ForwardingManagerMetrics(metrics, 1000)
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

  @Test
  def testTimeoutMs(): Unit = {
    val metrics = new Metrics()
    val timeoutMs = 500
    val forwardingManagerMetrics = ForwardingManagerMetrics(metrics, timeoutMs)
    val queueTimeMsP99 = metrics.metrics().get(forwardingManagerMetrics.queueTimeMsHist.latencyP99Name)
    val queueTimeMsP999 = metrics.metrics().get(forwardingManagerMetrics.queueTimeMsHist.latencyP999Name)
    assertEquals(Double.NaN, queueTimeMsP99.metricValue.asInstanceOf[Double])
    assertEquals(Double.NaN, queueTimeMsP999.metricValue.asInstanceOf[Double])
    for(i <- 0 to 99) {
      forwardingManagerMetrics.queueTimeMsHist.record(i)
    }
    forwardingManagerMetrics.queueTimeMsHist.record(1000)

    assertEquals(99, queueTimeMsP99.metricValue.asInstanceOf[Double])
    assertEquals(timeoutMs * 0.999, queueTimeMsP999.metricValue.asInstanceOf[Double])
  }
}
