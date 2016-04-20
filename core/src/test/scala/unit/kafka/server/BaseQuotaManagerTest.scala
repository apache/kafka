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

import java.util.Collections

import org.apache.kafka.common.metrics.{MetricConfig, Metrics, Quota}
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}

abstract class BaseQuotaManagerTest {
  private val time = new MockTime

  val quotaType: String
  private var config: ClientQuotaManagerConfig = null

  var numCallbacks: Int = 0
  def callback(delayTimeMs: Int) {
    numCallbacks += 1
  }

  @Before
  def beforeMethod() {
    numCallbacks = 0
    config = ClientQuotaManagerConfig(quotaType = quotaType, quotaBytesPerSecondDefault = 500)
  }

  case class Client(id: String) {
    val clientId = "client-" + id
    val user = "user-" + id
    val quotaId = if (quotaType == ClientQuotaManagerConfig.ClientId) clientId else user
    def sanitizedQuotaId = QuotaId.sanitize(quotaType, quotaId)
  }

  @Test
  def testQuotaParsing() {
    val clientMetrics = new ClientQuotaManager(config, newMetrics, "producer", time)
    val p1 = Client("p1")
    val p2 = Client("p2")

    // Case 1: Update the quota. Assert that the new quota value is returned
    clientMetrics.updateQuota(p1.quotaId, new Quota(2000, true))
    clientMetrics.updateQuota(p2.quotaId, new Quota(4000, true))

    try {
      assertEquals("Default producer quota should be 500", new Quota(500, true), clientMetrics.quota("random-client-id"))
      assertEquals("Should return the overridden value (2000)", new Quota(2000, true), clientMetrics.quota(p1.quotaId))
      assertEquals("Should return the overridden value (4000)", new Quota(4000, true), clientMetrics.quota(p2.quotaId))

      // p1 should be throttled using the overridden quota
      var throttleTimeMs = clientMetrics.recordAndMaybeThrottle(p1.user, p1.clientId, 2500 * config.numQuotaSamples, this.callback)
      assertTrue(s"throttleTimeMs should be > 0. was $throttleTimeMs", throttleTimeMs > 0)

      // Case 2: Change quota again. The quota should be updated within KafkaMetrics as well since the sensor was created.
      // p1 should not longer be throttled after the quota change
      clientMetrics.updateQuota(p1.quotaId, new Quota(3000, true))
      assertEquals("Should return the newly overridden value (3000)", new Quota(3000, true), clientMetrics.quota(p1.quotaId))

      throttleTimeMs = clientMetrics.recordAndMaybeThrottle(p1.user, p1.clientId, 0, this.callback)
      assertEquals(s"throttleTimeMs should be 0. was $throttleTimeMs", 0, throttleTimeMs)

      // Case 3: Change quota back to default. Should be throttled again
      clientMetrics.updateQuota(p1.quotaId, new Quota(500, true))
      assertEquals("Should return the default value (500)", new Quota(500, true), clientMetrics.quota(p1.quotaId))

      throttleTimeMs = clientMetrics.recordAndMaybeThrottle(p1.user, p1.clientId, 0, this.callback)
      assertTrue(s"throttleTimeMs should be > 0. was $throttleTimeMs", throttleTimeMs > 0)
    } finally {
      clientMetrics.shutdown()
    }
  }

  @Test
  def testQuotaViolation() {
    val metrics = newMetrics
    val clientMetrics = new ClientQuotaManager(config, metrics, "producer", time)
    val queueSizeMetric = metrics.metrics().get(metrics.metricName("queue-size", "producer", ""))
    try {
      /* We have 10 second windows. Make sure that there is no quota violation
       * if we produce under the quota
       */
      for (i <- 0 until 10) {
        clientMetrics.recordAndMaybeThrottle("anonymous", "unknown", 400, callback)
        time.sleep(1000)
      }
      assertEquals(10, numCallbacks)
      assertEquals(0, queueSizeMetric.value().toInt)

      // Create a spike.
      // 400*10 + 2000 + 300 = 6300/10.5 = 600 bytes per second.
      // (600 - quota)/quota*window-size = (600-500)/500*10.5 seconds = 2100
      // 10.5 seconds because the last window is half complete
      time.sleep(500)
      val sleepTime = clientMetrics.recordAndMaybeThrottle("anonymous", "unknown", 2300, callback)

      assertEquals("Should be throttled", 2100, sleepTime)
      assertEquals(1, queueSizeMetric.value().toInt)
      // After a request is delayed, the callback cannot be triggered immediately
      clientMetrics.throttledRequestReaper.doWork()
      assertEquals(10, numCallbacks)
      time.sleep(sleepTime)

      // Callback can only be triggered after the delay time passes
      clientMetrics.throttledRequestReaper.doWork()
      assertEquals(0, queueSizeMetric.value().toInt)
      assertEquals(11, numCallbacks)

      // Could continue to see delays until the bursty sample disappears
      for (i <- 0 until 10) {
        clientMetrics.recordAndMaybeThrottle("anonymous", "unknown", 400, callback)
        time.sleep(1000)
      }

      assertEquals("Should be unthrottled since bursty sample has rolled over",
                   0, clientMetrics.recordAndMaybeThrottle("anonymous", "unknown", 0, callback))
    } finally {
      clientMetrics.shutdown()
    }
  }

  @Test
  def testExpireThrottleTimeSensor() {
    val metrics = newMetrics
    val clientMetrics = new ClientQuotaManager(config, metrics, "producer", time)
    val client1 = Client("client1")
    val throttleSensorName = s"producerThrottleTime-$quotaType-${client1.sanitizedQuotaId}"
    try {
      clientMetrics.recordAndMaybeThrottle(client1.user, client1.clientId, 100, callback)
      // remove the throttle time sensor
      metrics.removeSensor(throttleSensorName)
      // should not throw an exception even if the throttle time sensor does not exist.
      val throttleTime = clientMetrics.recordAndMaybeThrottle(client1.user, client1.clientId, 10000, callback)
      assertTrue("Should be throttled", throttleTime > 0)
      // the sensor should get recreated
      val throttleTimeSensor = metrics.getSensor(throttleSensorName)
      assertTrue(s"Throttle time sensor $throttleSensorName should exist", throttleTimeSensor != null)
    } finally {
      clientMetrics.shutdown()
    }
  }

  @Test
  def testExpireQuotaSensors() {
    val metrics = newMetrics
    val clientMetrics = new ClientQuotaManager(config, metrics, "producer", time)
    val client1 = Client("client1")
    val throttleSensorName = s"producerThrottleTime-$quotaType-${client1.sanitizedQuotaId}"
    val rateSensorName = s"producer-$quotaType-${client1.sanitizedQuotaId}"
    try {
      clientMetrics.recordAndMaybeThrottle(client1.user, client1.clientId, 100, callback)
      // remove all the sensors
      metrics.removeSensor(throttleSensorName)
      metrics.removeSensor(rateSensorName)
      // should not throw an exception
      val throttleTime = clientMetrics.recordAndMaybeThrottle(client1.user, client1.clientId, 10000, callback)
      assertTrue("Should be throttled", throttleTime > 0)

      // all the sensors should get recreated
      val throttleTimeSensor = metrics.getSensor(throttleSensorName)
      assertTrue(s"Throttle time sensor $throttleSensorName should exist", throttleTimeSensor != null)

      val byteRateSensor = metrics.getSensor(rateSensorName)
      assertTrue(s"Byte rate sensor $rateSensorName should exist", byteRateSensor != null)
    } finally {
      clientMetrics.shutdown()
    }
  }

  def newMetrics: Metrics = {
    new Metrics(new MetricConfig(), Collections.emptyList(), time)
  }
}
