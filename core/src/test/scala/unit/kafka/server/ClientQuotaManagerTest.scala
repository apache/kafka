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

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.{MetricConfig, Metrics, Quota}
import org.apache.kafka.common.utils.MockTime
import org.junit.{Assert, Before, Test}

class ClientQuotaManagerTest {
  private val time = new MockTime

  private val config = ClientQuotaManagerConfig(quotaBytesPerSecondDefault = 500,
                                                quotaBytesPerSecondOverrides = "p1=2000,p2=4000")

  var numCallbacks: Int = 0
  def callback(delayTimeMs: Int) {
    numCallbacks += 1
  }

  @Before
  def beforeMethod() {
    numCallbacks = 0
  }

  @Test
  def testQuotaParsing() {
    val clientMetrics = new ClientQuotaManager(config, newMetrics, "producer", time)
    try {
      Assert.assertEquals("Default producer quota should be 500",
                          new Quota(500, true), clientMetrics.quota("random-client-id"))
      Assert.assertEquals("Should return the overridden value (2000)",
                          new Quota(2000, true), clientMetrics.quota("p1"))
      Assert.assertEquals("Should return the overridden value (4000)",
                          new Quota(4000, true), clientMetrics.quota("p2"))
    } finally {
      clientMetrics.shutdown()
    }
  }

  @Test
  def testQuotaViolation() {
    val metrics = newMetrics
    val clientMetrics = new ClientQuotaManager(config, metrics, "producer", time)
    val queueSizeMetric = metrics.metrics().get(new MetricName("queue-size", "producer", ""))
    try {
      /* We have 10 second windows. Make sure that there is no quota violation
       * if we produce under the quota
       */
      for (i <- 0 until 10) {
        clientMetrics.recordAndMaybeThrottle("unknown", 400, callback)
        time.sleep(1000)
      }
      Assert.assertEquals(10, numCallbacks)
      Assert.assertEquals(0, queueSizeMetric.value().toInt)

      // Create a spike.
      // 400*10 + 2000 + 300 = 6300/10.5 = 600 bytes per second.
      // (600 - quota)/quota*window-size = (600-500)/500*10.5 seconds = 2100
      // 10.5 seconds because the last window is half complete
      time.sleep(500)
      val sleepTime = clientMetrics.recordAndMaybeThrottle("unknown", 2300, callback)

      Assert.assertEquals("Should be throttled", 2100, sleepTime)
      Assert.assertEquals(1, queueSizeMetric.value().toInt)
      // After a request is delayed, the callback cannot be triggered immediately
      clientMetrics.throttledRequestReaper.doWork()
      Assert.assertEquals(10, numCallbacks)
      time.sleep(sleepTime)

      // Callback can only be triggered after the the delay time passes
      clientMetrics.throttledRequestReaper.doWork()
      Assert.assertEquals(0, queueSizeMetric.value().toInt)
      Assert.assertEquals(11, numCallbacks)

      // Could continue to see delays until the bursty sample disappears
      for (i <- 0 until 10) {
        clientMetrics.recordAndMaybeThrottle("unknown", 400, callback)
        time.sleep(1000)
      }

      Assert.assertEquals("Should be unthrottled since bursty sample has rolled over",
                          0, clientMetrics.recordAndMaybeThrottle("unknown", 0, callback))
    } finally {
      clientMetrics.shutdown()
    }
  }

  @Test
  def testOverrideParse() {
    var testConfig = ClientQuotaManagerConfig()
    var clientMetrics = new ClientQuotaManager(testConfig, newMetrics, "consumer", time)

    try {
      // Case 1 - Default config
      Assert.assertEquals(new Quota(ClientQuotaManagerConfig.QuotaBytesPerSecondDefault, true),
                          clientMetrics.quota("p1"))
    } finally {
      clientMetrics.shutdown()
    }


    // Case 2 - Empty override
    testConfig = ClientQuotaManagerConfig(quotaBytesPerSecondDefault = 500,
                                          quotaBytesPerSecondOverrides = "p1=2000,p2=4000,,")

    clientMetrics = new ClientQuotaManager(testConfig, newMetrics, "consumer", time)
    try {
      Assert.assertEquals(new Quota(2000, true), clientMetrics.quota("p1"))
      Assert.assertEquals(new Quota(4000, true), clientMetrics.quota("p2"))
    } finally {
      clientMetrics.shutdown()
    }

    // Case 3 - NumberFormatException for override
    testConfig = ClientQuotaManagerConfig(quotaBytesPerSecondDefault = 500,
                                          quotaBytesPerSecondOverrides = "p1=2000,p2=4000,p3=p4")
    try {
      clientMetrics = new ClientQuotaManager(testConfig, newMetrics, "consumer", time)
      Assert.fail("Should fail to parse invalid config " + testConfig.quotaBytesPerSecondOverrides)
    }
    catch {
      // Swallow.
      case nfe: NumberFormatException =>
    }

    // Case 4 - IllegalArgumentException for override
    testConfig = ClientQuotaManagerConfig(quotaBytesPerSecondDefault = 500,
                                          quotaBytesPerSecondOverrides = "p1=2000=3000")
    try {
      clientMetrics = new ClientQuotaManager(testConfig, newMetrics, "producer", time)
      Assert.fail("Should fail to parse invalid config " + testConfig.quotaBytesPerSecondOverrides)
    }
    catch {
      // Swallow.
      case nfe: IllegalArgumentException =>
    }

  }

  def newMetrics: Metrics = {
    new Metrics(new MetricConfig(), Collections.emptyList(), time)
  }
}
