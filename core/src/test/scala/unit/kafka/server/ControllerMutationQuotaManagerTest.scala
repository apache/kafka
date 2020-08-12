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

import java.util.concurrent.TimeUnit

import kafka.server.QuotaType.ControllerMutation
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.metrics.QuotaViolationException
import org.apache.kafka.common.metrics.stats.TokenBucket
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert._
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Test

class StrictControllerMutationQuotaTest {
  @Test
  def testControllerMutationQuotaViolation(): Unit = {
    val time = new MockTime(0, System.currentTimeMillis, 0)
    val metrics = new Metrics(time)
    val sensor = metrics.sensor("sensor", new MetricConfig()
      .quota(Quota.upperBound(10))
      .timeWindow(1, TimeUnit.SECONDS)
      .samples(10))
    val metricName = metrics.metricName("rate", "test-group")
    assertTrue(sensor.add(metricName, new TokenBucket))

    val quota = new StrictControllerMutationQuota(time, sensor)
    assertFalse(quota.isExceeded)

    // Recording a first value at T to bring the tokens to 10. Value is accepted
    // because the quota is not exhausted yet.
    quota.record(90)
    assertFalse(quota.isExceeded)
    assertEquals(0, quota.throttleTime)

    // Recording a second value at T to bring the tokens to -80. Value is accepted
    quota.record(90)
    assertFalse(quota.isExceeded)
    assertEquals(0, quota.throttleTime)

    // Recording a third value at T is rejected immediately because there are not
    // tokens available in the bucket.
    assertThrows(classOf[ThrottlingQuotaExceededException],
      () => quota.record(90))
    assertTrue(quota.isExceeded)
    assertEquals(8000, quota.throttleTime)

    // Throttle time is adjusted with time
    time.sleep(5000)
    assertEquals(3000, quota.throttleTime)

    metrics.close()
  }
}

class PermissiveControllerMutationQuotaTest {
  @Test
  def testControllerMutationQuotaViolation(): Unit = {
    val time = new MockTime(0, System.currentTimeMillis, 0)
    val metrics = new Metrics(time)
    val sensor = metrics.sensor("sensor", new MetricConfig()
      .quota(Quota.upperBound(10))
      .timeWindow(1, TimeUnit.SECONDS)
      .samples(10))
    val metricName = metrics.metricName("rate", "test-group")
    assertTrue(sensor.add(metricName, new TokenBucket))

    val quota = new PermissiveControllerMutationQuota(time, sensor)
    assertFalse(quota.isExceeded)

    // Recording a first value at T to bring the tokens 10. Value is accepted
    // because the quota is not exhausted yet.
    quota.record(90)
    assertFalse(quota.isExceeded)
    assertEquals(0, quota.throttleTime)

    // Recording a second value at T to bring the tokens to -80. Value is accepted
    quota.record(90)
    assertFalse(quota.isExceeded)
    assertEquals(8000, quota.throttleTime)

    // Recording a second value at T to bring the tokens to -170. Value is accepted
    // even though the quota is exhausted.
    quota.record(90)
    assertFalse(quota.isExceeded) // quota is never exceeded
    assertEquals(17000, quota.throttleTime)

    // Throttle time is adjusted with time
    time.sleep(5000)
    assertEquals(12000, quota.throttleTime)

    metrics.close()
  }
}

class ControllerMutationQuotaManagerTest extends BaseClientQuotaManagerTest {
  private val User = "ANONYMOUS"
  private val ClientId = "test-client"

  private val config = ClientQuotaManagerConfig(
    numQuotaSamples = 10,
    quotaWindowSizeSeconds = 1
  )

  private def withQuotaManager(f: ControllerMutationQuotaManager => Unit): Unit = {
    val quotaManager = new ControllerMutationQuotaManager(config, metrics, time,"", None)
    try {
      f(quotaManager)
    } finally {
      quotaManager.shutdown()
    }
  }

  @Test
  def testThrottleTime(): Unit = {
    import ControllerMutationQuotaManager._

    val time = new MockTime(0, System.currentTimeMillis, 0)
    val metrics = new Metrics(time)
    val sensor = metrics.sensor("sensor")
    val metricName = metrics.metricName("tokens", "test-group")
    sensor.add(metricName, new TokenBucket)
    val metric = metrics.metric(metricName)

    assertEquals(0, throttleTimeMs(new QuotaViolationException(metric, 0, 10), time.milliseconds()))
    assertEquals(500, throttleTimeMs(new QuotaViolationException(metric, -5, 10), time.milliseconds()))
    assertEquals(1000, throttleTimeMs(new QuotaViolationException(metric, -10, 10), time.milliseconds()))
  }

  @Test
  def testControllerMutationQuotaViolation(): Unit = {
    withQuotaManager { quotaManager =>
      quotaManager.updateQuota(Some(User), Some(ClientId), Some(ClientId),
        Some(Quota.upperBound(10)))
      val queueSizeMetric = metrics.metrics().get(
        metrics.metricName("queue-size", ControllerMutation.toString, ""))

      // Verify that there is no quota violation if we remain under the quota.
      for (_ <- 0 until 10) {
        assertEquals(0, maybeRecord(quotaManager, User, ClientId, 10))
        time.sleep(1000)
      }
      assertEquals(0, queueSizeMetric.metricValue.asInstanceOf[Double].toInt)

      // Create a spike worth of 110 mutations.
      // Current tokens in the bucket = 100
      // As we use the Strict enforcement, the quota is checked before updating the rate. Hence,
      // the spike is accepted and no quota violation error is raised.
      var throttleTime = maybeRecord(quotaManager, User, ClientId, 110)
      assertEquals("Should not be throttled", 0, throttleTime)

      // Create a spike worth of 110 mutations.
      // Current tokens in the bucket = 100 - 110 = -10
      // As the quota is already violated, the spike is rejected immediately without updating the
      // rate. The client must wait:
      // 10 / 10 = 1s
      throttleTime = maybeRecord(quotaManager, User, ClientId, 110)
      assertEquals("Should be throttled", 1000, throttleTime)

      // Throttle
      throttle(quotaManager, User, ClientId, throttleTime, callback)
      assertEquals(1, queueSizeMetric.metricValue.asInstanceOf[Double].toInt)

      // After a request is delayed, the callback cannot be triggered immediately
      quotaManager.throttledChannelReaper.doWork()
      assertEquals(0, numCallbacks)

      // Callback can only be triggered after the delay time passes
      time.sleep(throttleTime)
      quotaManager.throttledChannelReaper.doWork()
      assertEquals(0, queueSizeMetric.metricValue.asInstanceOf[Double].toInt)
      assertEquals(1, numCallbacks)

      // Retry to spike worth of 110 mutations after having waited the required throttle time.
      // Current tokens in the bucket = 0
      throttleTime = maybeRecord(quotaManager, User, ClientId, 110)
      assertEquals("Should be throttled", 0, throttleTime)
    }
  }

  @Test
  def testNewStrictQuotaForReturnsUnboundedQuotaWhenQuotaIsDisabled(): Unit = {
    withQuotaManager { quotaManager =>
      assertEquals(UnboundedControllerMutationQuota,
        quotaManager.newStrictQuotaFor(buildSession(User), ClientId))
    }
  }

  @Test
  def testNewStrictQuotaForReturnsStrictQuotaWhenQuotaIsEnabled(): Unit = {
    withQuotaManager { quotaManager =>
      quotaManager.updateQuota(Some(User), Some(ClientId), Some(ClientId),
        Some(Quota.upperBound(10)))
      val quota = quotaManager.newStrictQuotaFor(buildSession(User), ClientId)
      assertTrue(quota.isInstanceOf[StrictControllerMutationQuota])

    }
  }

  @Test
  def testNewPermissiveQuotaForReturnsUnboundedQuotaWhenQuotaIsDisabled(): Unit = {
    withQuotaManager { quotaManager =>
      assertEquals(UnboundedControllerMutationQuota,
        quotaManager.newPermissiveQuotaFor(buildSession(User), ClientId))
    }
  }

  @Test
  def testNewPermissiveQuotaForReturnsStrictQuotaWhenQuotaIsEnabled(): Unit = {
    withQuotaManager { quotaManager =>
      quotaManager.updateQuota(Some(User), Some(ClientId), Some(ClientId),
        Some(Quota.upperBound(10)))
      val quota = quotaManager.newPermissiveQuotaFor(buildSession(User), ClientId)
      assertTrue(quota.isInstanceOf[PermissiveControllerMutationQuota])
    }
  }
}
