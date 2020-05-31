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

package kafka.utils

import java.util.concurrent.TimeUnit

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.{KafkaMetric, MetricConfig}
import org.apache.kafka.common.metrics.stats.{Rate, Value}

import scala.jdk.CollectionConverters._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.assertThrows

class QuotaUtilsTest {

  private val time = new MockTime
  private val windowSizeMs = 1000
  private val maxThrottleTimeMs = 500

  @Test
  def testThrottleTimeObservedRateEqualsQuota(): Unit = {
    val observedValue = 16.5
    assertEquals(0, QuotaUtils.throttleTime(observedValue, observedValue, windowSizeMs))

    // should be independent of window size
    assertEquals(0, QuotaUtils.throttleTime(observedValue, observedValue, windowSizeMs + 1))
  }

  @Test
  def testThrottleTimeObservedRateBelowQuota(): Unit = {
    val observedValue = 16.5
    val quota = 20.4
    assertTrue(QuotaUtils.throttleTime(observedValue, quota, windowSizeMs) < 0)

    // should be independent of window size
    assertTrue(QuotaUtils.throttleTime(observedValue, quota, windowSizeMs + 1) < 0)
  }

  @Test
  def testThrottleTimeObservedRateAboveQuota(): Unit = {
    val quota = 50.0
    val observedValue = 100.0
    assertEquals(windowSizeMs, QuotaUtils.throttleTime(observedValue, quota, windowSizeMs))
  }

  @Test
  def testBoundedThrottleTimeObservedRateEqualsQuota(): Unit = {
    val observedValue = 18.2
    assertEquals(0, QuotaUtils.boundedThrottleTime(observedValue, observedValue, windowSizeMs, maxThrottleTimeMs))

    // should be independent of window size
    assertEquals(0, QuotaUtils.boundedThrottleTime(observedValue, observedValue, windowSizeMs + 1, maxThrottleTimeMs))
  }

  @Test
  def testBoundedThrottleTimeObservedRateBelowQuota(): Unit = {
    val observedValue = 16.5
    val quota = 22.4
    assertTrue(QuotaUtils.boundedThrottleTime(observedValue, quota, windowSizeMs, maxThrottleTimeMs) < 0)

    // should be independent of window size
    assertTrue(QuotaUtils.boundedThrottleTime(observedValue, quota, windowSizeMs + 1, maxThrottleTimeMs) < 0)
  }

  @Test
  def testBoundedThrottleTimeObservedRateAboveQuotaBelowLimit(): Unit = {
    val quota = 50.0
    val observedValue = 55.0
    assertEquals(100, QuotaUtils.boundedThrottleTime(observedValue, quota, windowSizeMs, maxThrottleTimeMs))
  }

  @Test
  def testBoundedThrottleTimeObservedRateAboveQuotaAboveLimit(): Unit = {
    val quota = 50.0
    val observedValue = 100.0
    assertEquals(maxThrottleTimeMs, QuotaUtils.boundedThrottleTime(observedValue, quota, windowSizeMs, maxThrottleTimeMs))
  }

  @Test
  def testRateMetricWindowSizeReturnsWindowForRateMetric(): Unit = {
    val metricName = new MetricName("rate-metric", "test-group", "test metric", Map.empty.asJava)
    val windowSizeSeconds = 2
    val cfg = new MetricConfig().timeWindow(windowSizeSeconds, TimeUnit.SECONDS)
    val testMetric = new KafkaMetric(new Object(), metricName, new Rate(), cfg, time);

    assertEquals(windowSizeSeconds * 1000, QuotaUtils.rateMetricWindowSize(testMetric, time.milliseconds()))
  }

  @Test
  def testRateMetricWindowSizeThrowsExceptionIfProvidedNonRateMetric(): Unit = {
    val metricName = new MetricName("value-metric", "test-group", "test metric", Map.empty.asJava)
    val testMetric = new KafkaMetric(new Object(), metricName, new Value(), new MetricConfig, time);

    assertThrows[IllegalArgumentException] {
      QuotaUtils.rateMetricWindowSize(testMetric, time.milliseconds())
    }
  }
}
