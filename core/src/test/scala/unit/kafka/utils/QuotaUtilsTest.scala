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
import org.apache.kafka.common.metrics.{KafkaMetric, MetricConfig, Quota, QuotaViolationException}
import org.apache.kafka.common.metrics.stats.{Rate, Value}

import scala.jdk.CollectionConverters._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class QuotaUtilsTest {

  private val time = new MockTime
  private val numSamples = 10
  private val sampleWindowSec = 1
  private val maxThrottleTimeMs = 500
  private val metricName = new MetricName("test-metric", "groupA", "testA", Map.empty.asJava)

  @Test
  def testThrottleTimeObservedRateEqualsQuota(): Unit = {
    val numSamples = 10
    val observedValue = 16.5

    assertEquals(0, throttleTime(observedValue, observedValue, numSamples))

    // should be independent of window size
    assertEquals(0, throttleTime(observedValue, observedValue, numSamples + 1))
  }

  @Test
  def testThrottleTimeObservedRateBelowQuota(): Unit = {
    val observedValue = 16.5
    val quota = 20.4
    assertTrue(throttleTime(observedValue, quota, numSamples) < 0)

    // should be independent of window size
    assertTrue(throttleTime(observedValue, quota, numSamples + 1) < 0)
  }

  @Test
  def testThrottleTimeObservedRateAboveQuota(): Unit = {
    val quota = 50.0
    val observedValue = 100.0
    assertEquals(2000, throttleTime(observedValue, quota, 3))
  }

  @Test
  def testBoundedThrottleTimeObservedRateEqualsQuota(): Unit = {
    val observedValue = 18.2
    assertEquals(0, boundedThrottleTime(observedValue, observedValue, numSamples, maxThrottleTimeMs))

    // should be independent of window size
    assertEquals(0, boundedThrottleTime(observedValue, observedValue, numSamples + 1, maxThrottleTimeMs))
  }

  @Test
  def testBoundedThrottleTimeObservedRateBelowQuota(): Unit = {
    val observedValue = 16.5
    val quota = 22.4

    assertTrue(boundedThrottleTime(observedValue, quota, numSamples, maxThrottleTimeMs) < 0)

    // should be independent of window size
    assertTrue(boundedThrottleTime(observedValue, quota, numSamples + 1, maxThrottleTimeMs) < 0)
  }

  @Test
  def testBoundedThrottleTimeObservedRateAboveQuotaBelowLimit(): Unit = {
    val quota = 50.0
    val observedValue = 55.0
    assertEquals(100, boundedThrottleTime(observedValue, quota, 2, maxThrottleTimeMs))
  }

  @Test
  def testBoundedThrottleTimeObservedRateAboveQuotaAboveLimit(): Unit = {
    val quota = 50.0
    val observedValue = 100.0
    assertEquals(maxThrottleTimeMs, boundedThrottleTime(observedValue, quota, numSamples, maxThrottleTimeMs))
  }

  @Test
  def testThrottleTimeThrowsExceptionIfProvidedNonRateMetric(): Unit = {
    val testMetric = new KafkaMetric(new Object(), metricName, new Value(), new MetricConfig, time);

    assertThrows(classOf[IllegalArgumentException], () => QuotaUtils.throttleTime(new QuotaViolationException(testMetric, 10.0, 20.0), time.milliseconds))
  }

  @Test
  def testBoundedThrottleTimeThrowsExceptionIfProvidedNonRateMetric(): Unit = {
    val testMetric = new KafkaMetric(new Object(), metricName, new Value(), new MetricConfig, time);

    assertThrows(classOf[IllegalArgumentException], () => QuotaUtils.boundedThrottleTime(new QuotaViolationException(testMetric, 10.0, 20.0),
      maxThrottleTimeMs, time.milliseconds))
  }

  // the `metric` passed into the returned QuotaViolationException will return windowSize = 'numSamples' - 1
  private def quotaViolationException(observedValue: Double, quota: Double, numSamples: Int): QuotaViolationException = {
    val metricConfig = new MetricConfig()
      .timeWindow(sampleWindowSec, TimeUnit.SECONDS)
      .samples(numSamples)
      .quota(new Quota(quota, true))
    val metric = new KafkaMetric(new Object(), metricName, new Rate(), metricConfig, time)
    new QuotaViolationException(metric, observedValue, quota)
  }

  private def throttleTime(observedValue: Double, quota: Double, numSamples: Int): Long = {
    val e = quotaViolationException(observedValue, quota, numSamples)
    QuotaUtils.throttleTime(e, time.milliseconds)
  }

  private def boundedThrottleTime(observedValue: Double, quota: Double, numSamples: Int, maxThrottleTime: Long): Long = {
    val e = quotaViolationException(observedValue, quota, numSamples)
    QuotaUtils.boundedThrottleTime(e, maxThrottleTime, time.milliseconds)
  }
}
