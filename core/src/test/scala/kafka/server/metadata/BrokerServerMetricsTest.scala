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

package kafka.server.metadata

import java.util.Collections
import kafka.utils.TestUtils
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.image.MetadataProvenance
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

final class BrokerServerMetricsTest {
  @Test
  def testMetricsExported(): Unit = {
    val metrics = new Metrics()
    val expectedGroup = "broker-metadata-metrics"

    // Metric description is not use for metric name equality
    val expectedMetrics = Set(
      new MetricName("last-applied-record-offset", expectedGroup, "", Collections.emptyMap()),
      new MetricName("last-applied-record-timestamp", expectedGroup, "", Collections.emptyMap()),
      new MetricName("last-applied-record-lag-ms", expectedGroup, "", Collections.emptyMap()),
      new MetricName("metadata-load-error-count", expectedGroup, "", Collections.emptyMap()),
      new MetricName("metadata-apply-error-count", expectedGroup, "", Collections.emptyMap())
    )
     
    TestUtils.resource(BrokerServerMetrics(metrics)) { brokerMetrics =>
      val metricsMap = metrics.metrics().asScala.filter{ case (name, _) => name.group == expectedGroup }
      assertEquals(expectedMetrics.size, metricsMap.size)
      metricsMap.foreach { case (name, metric) =>
        assertTrue(expectedMetrics.contains(name))
      }
    }

    val metricsMap = metrics.metrics().asScala.filter{ case (name, _) => name.group == expectedGroup }
    assertEquals(0, metricsMap.size)
  }

  @Test
  def testLastAppliedRecordOffset(): Unit = {
    val metrics = new Metrics()
    TestUtils.resource(BrokerServerMetrics(metrics)) { brokerMetrics =>
      val offsetMetric = metrics.metrics().get(brokerMetrics.lastAppliedRecordOffsetName)
      assertEquals(-1L, offsetMetric.metricValue.asInstanceOf[Long])

      // Update metric value and check
      val expectedValue = 1000
      brokerMetrics.updateLastAppliedImageProvenance(new MetadataProvenance(
        expectedValue,
        brokerMetrics.lastAppliedImageProvenance.get().epoch(),
        brokerMetrics.lastAppliedTimestamp()));
      assertEquals(expectedValue, offsetMetric.metricValue.asInstanceOf[Long])
    }
  }

  @Test
  def testLastAppliedRecordTimestamp(): Unit = {
    val time = new MockTime()
    val metrics = new Metrics(time)
    TestUtils.resource(BrokerServerMetrics(metrics)) { brokerMetrics =>
      time.sleep(1000)
      val timestampMetric = metrics.metrics().get(brokerMetrics.lastAppliedRecordTimestampName)
      val lagMetric = metrics.metrics().get(brokerMetrics.lastAppliedRecordLagMsName)

      assertEquals(-1L, timestampMetric.metricValue.asInstanceOf[Long])
      assertEquals(time.milliseconds + 1, lagMetric.metricValue.asInstanceOf[Long])

      // Update metric value and check
      val timestamp = 500L

      brokerMetrics.updateLastAppliedImageProvenance(new MetadataProvenance(
        brokerMetrics.lastAppliedOffset(),
        brokerMetrics.lastAppliedImageProvenance.get().epoch(),
        timestamp))
      assertEquals(timestamp, timestampMetric.metricValue.asInstanceOf[Long])
      assertEquals(time.milliseconds - timestamp, lagMetric.metricValue.asInstanceOf[Long])
    }
  }

  @Test
  def testMetadataLoadErrorCount(): Unit = {
    val time = new MockTime()
    val metrics = new Metrics(time)
    TestUtils.resource(BrokerServerMetrics(metrics)) { brokerMetrics =>
      val metadataLoadErrorCountMetric = metrics.metrics().get(brokerMetrics.metadataLoadErrorCountName)

      assertEquals(0L, metadataLoadErrorCountMetric.metricValue.asInstanceOf[Long])

      // Update metric value and check
      val errorCount = 100
      brokerMetrics.metadataLoadErrorCount.set(errorCount)
      assertEquals(errorCount, metadataLoadErrorCountMetric.metricValue.asInstanceOf[Long])
    }
  }

  @Test
  def testMetadataApplyErrorCount(): Unit = {
    val time = new MockTime()
    val metrics = new Metrics(time)
    TestUtils.resource(BrokerServerMetrics(metrics)) { brokerMetrics =>
      val metadataApplyErrorCountMetric = metrics.metrics().get(brokerMetrics.metadataApplyErrorCountName)

      assertEquals(0L, metadataApplyErrorCountMetric.metricValue.asInstanceOf[Long])

      // Update metric value and check
      val errorCount = 100
      brokerMetrics.metadataApplyErrorCount.set(errorCount)
      assertEquals(errorCount, metadataApplyErrorCountMetric.metricValue.asInstanceOf[Long])
    }
  }
}
