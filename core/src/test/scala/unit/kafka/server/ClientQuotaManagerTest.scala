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

import kafka.network.RequestChannel.{EndThrottlingAction, ResponseAction, Session, StartThrottlingAction}
import kafka.server.QuotaType._
import org.apache.kafka.common.metrics.{MetricConfig, Metrics, Quota}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{MockTime, Sanitizer}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}

class ClientQuotaManagerTest {
  private val time = new MockTime

  private val config = ClientQuotaManagerConfig(quotaBytesPerSecondDefault = 500)

  var numCallbacks: Int = 0
  def callback (responseAction: ResponseAction) {
    // Count how many times this callback is called for notifyThrottlingDone().
    responseAction match {
      case StartThrottlingAction =>
      case EndThrottlingAction => numCallbacks += 1
    }
  }

  @Before
  def beforeMethod() {
    numCallbacks = 0
  }

  private def maybeRecord(quotaManager: ClientQuotaManager, user: String, clientId: String, value: Double): Int = {
    val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user)
    quotaManager.maybeRecordAndGetThrottleTimeMs(Session(principal, null),clientId, value, time.milliseconds())
  }

  private def throttle(quotaManager: ClientQuotaManager, user: String, clientId: String, throttleTimeMs: Int,
                       channelThrottlingCallback: (ResponseAction) => Unit) {
    val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user)
    quotaManager.throttle(Session(principal, null),clientId, throttleTimeMs, channelThrottlingCallback)
  }

  private def testQuotaParsing(config: ClientQuotaManagerConfig, client1: UserClient, client2: UserClient, randomClient: UserClient, defaultConfigClient: UserClient) {
    val clientMetrics = new ClientQuotaManager(config, newMetrics, Produce, time, "")

    try {
      // Case 1: Update the quota. Assert that the new quota value is returned
      clientMetrics.updateQuota(client1.configUser, client1.configClientId, client1.sanitizedConfigClientId, Some(new Quota(2000, true)))
      clientMetrics.updateQuota(client2.configUser, client2.configClientId, client2.sanitizedConfigClientId, Some(new Quota(4000, true)))

      assertEquals("Default producer quota should be " + config.quotaBytesPerSecondDefault,
        config.quotaBytesPerSecondDefault, clientMetrics.quota(randomClient.user, randomClient.clientId).bound, 0.0)
      assertEquals("Should return the overridden value (2000)", 2000, clientMetrics.quota(client1.user, client1.clientId).bound, 0.0)
      assertEquals("Should return the overridden value (4000)", 4000, clientMetrics.quota(client2.user, client2.clientId).bound, 0.0)

      // p1 should be throttled using the overridden quota
      var throttleTimeMs = maybeRecord(clientMetrics, client1.user, client1.clientId, 2500 * config.numQuotaSamples)
      assertTrue(s"throttleTimeMs should be > 0. was $throttleTimeMs", throttleTimeMs > 0)

      // Case 2: Change quota again. The quota should be updated within KafkaMetrics as well since the sensor was created.
      // p1 should not longer be throttled after the quota change
      clientMetrics.updateQuota(client1.configUser, client1.configClientId, client1.sanitizedConfigClientId, Some(new Quota(3000, true)))
      assertEquals("Should return the newly overridden value (3000)", 3000, clientMetrics.quota(client1.user, client1.clientId).bound, 0.0)

      throttleTimeMs = maybeRecord(clientMetrics, client1.user, client1.clientId, 0)
      assertEquals(s"throttleTimeMs should be 0. was $throttleTimeMs", 0, throttleTimeMs)

      // Case 3: Change quota back to default. Should be throttled again
      clientMetrics.updateQuota(client1.configUser, client1.configClientId, client1.sanitizedConfigClientId, Some(new Quota(500, true)))
      assertEquals("Should return the default value (500)", 500, clientMetrics.quota(client1.user, client1.clientId).bound, 0.0)

      throttleTimeMs = maybeRecord(clientMetrics, client1.user, client1.clientId, 0)
      assertTrue(s"throttleTimeMs should be > 0. was $throttleTimeMs", throttleTimeMs > 0)

      // Case 4: Set high default quota, remove p1 quota. p1 should no longer be throttled
      clientMetrics.updateQuota(client1.configUser, client1.configClientId, client1.sanitizedConfigClientId, None)
      clientMetrics.updateQuota(defaultConfigClient.configUser, defaultConfigClient.configClientId, defaultConfigClient.sanitizedConfigClientId, Some(new Quota(4000, true)))
      assertEquals("Should return the newly overridden value (4000)", 4000, clientMetrics.quota(client1.user, client1.clientId).bound, 0.0)

      throttleTimeMs = maybeRecord(clientMetrics, client1.user, client1.clientId, 1000 * config.numQuotaSamples)
      assertEquals(s"throttleTimeMs should be 0. was $throttleTimeMs", 0, throttleTimeMs)

    } finally {
      clientMetrics.shutdown()
    }
  }

  /**
   * Tests parsing for <client-id> quotas.
   * Quota overrides persisted in ZooKeeper in /config/clients/<client-id>, default persisted in /config/clients/<default>
   */
  @Test
  def testClientIdQuotaParsing() {
    val client1 = UserClient("ANONYMOUS", "p1", None, Some("p1"))
    val client2 = UserClient("ANONYMOUS", "p2", None, Some("p2"))
    val randomClient = UserClient("ANONYMOUS", "random-client-id", None, None)
    val defaultConfigClient = UserClient("", "", None, Some(ConfigEntityName.Default))
    testQuotaParsing(config, client1, client2, randomClient, defaultConfigClient)
  }

  /**
   * Tests parsing for <user> quotas.
   * Quota overrides persisted in ZooKeeper in /config/users/<user>, default persisted in /config/users/<default>
   */
  @Test
  def testUserQuotaParsing() {
    val client1 = UserClient("User1", "p1", Some("User1"), None)
    val client2 = UserClient("User2", "p2", Some("User2"), None)
    val randomClient = UserClient("RandomUser", "random-client-id", None, None)
    val defaultConfigClient = UserClient("", "", Some(ConfigEntityName.Default), None)
    val config = ClientQuotaManagerConfig(quotaBytesPerSecondDefault = Long.MaxValue)
    testQuotaParsing(config, client1, client2, randomClient, defaultConfigClient)
  }

  /**
   * Tests parsing for <user, client-id> quotas.
   * Quotas persisted in ZooKeeper in /config/users/<user>/clients/<client-id>, default in /config/users/<default>/clients/<default>
   */
  @Test
  def testUserClientIdQuotaParsing() {
    val client1 = UserClient("User1", "p1", Some("User1"), Some("p1"))
    val client2 = UserClient("User2", "p2", Some("User2"), Some("p2"))
    val randomClient = UserClient("RandomUser", "random-client-id", None, None)
    val defaultConfigClient = UserClient("", "", Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
    val config = ClientQuotaManagerConfig(quotaBytesPerSecondDefault = Long.MaxValue)
    testQuotaParsing(config, client1, client2, randomClient, defaultConfigClient)
  }

  /**
   * Tests parsing for <user> quotas when client-id default quota properties are set.
   */
  @Test
  def testUserQuotaParsingWithDefaultClientIdQuota() {
    val client1 = UserClient("User1", "p1", Some("User1"), None)
    val client2 = UserClient("User2", "p2", Some("User2"), None)
    val randomClient = UserClient("RandomUser", "random-client-id", None, None)
    val defaultConfigClient = UserClient("", "", Some(ConfigEntityName.Default), None)
    testQuotaParsing(config, client1, client2, randomClient, defaultConfigClient)
  }

  /**
   * Tests parsing for <user, client-id> quotas when client-id default quota properties are set.
   */
  @Test
  def testUserClientQuotaParsingIdWithDefaultClientIdQuota() {
    val client1 = UserClient("User1", "p1", Some("User1"), Some("p1"))
    val client2 = UserClient("User2", "p2", Some("User2"), Some("p2"))
    val randomClient = UserClient("RandomUser", "random-client-id", None, None)
    val defaultConfigClient = UserClient("", "", Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
    testQuotaParsing(config, client1, client2, randomClient, defaultConfigClient)
  }

  @Test
  def testQuotaConfigPrecedence() {
    val quotaManager = new ClientQuotaManager(ClientQuotaManagerConfig(quotaBytesPerSecondDefault=Long.MaxValue),
        newMetrics, Produce, time, "")

    def checkQuota(user: String, clientId: String, expectedBound: Int, value: Int, expectThrottle: Boolean) {
      assertEquals(expectedBound, quotaManager.quota(user, clientId).bound, 0.0)
      val throttleTimeMs = maybeRecord(quotaManager, user, clientId, value * config.numQuotaSamples)
      if (expectThrottle)
        assertTrue(s"throttleTimeMs should be > 0. was $throttleTimeMs", throttleTimeMs > 0)
      else
        assertEquals(s"throttleTimeMs should be 0. was $throttleTimeMs", 0, throttleTimeMs)
    }

    try {
      quotaManager.updateQuota(Some(ConfigEntityName.Default), None, None, Some(new Quota(1000, true)))
      quotaManager.updateQuota(None, Some(ConfigEntityName.Default), Some(ConfigEntityName.Default), Some(new Quota(2000, true)))
      quotaManager.updateQuota(Some(ConfigEntityName.Default), Some(ConfigEntityName.Default), Some(ConfigEntityName.Default), Some(new Quota(3000, true)))
      quotaManager.updateQuota(Some("userA"), None, None, Some(new Quota(4000, true)))
      quotaManager.updateQuota(Some("userA"), Some("client1"), Some("client1"), Some(new Quota(5000, true)))
      quotaManager.updateQuota(Some("userB"), None, None, Some(new Quota(6000, true)))
      quotaManager.updateQuota(Some("userB"), Some("client1"), Some("client1"), Some(new Quota(7000, true)))
      quotaManager.updateQuota(Some("userB"), Some(ConfigEntityName.Default), Some(ConfigEntityName.Default), Some(new Quota(8000, true)))
      quotaManager.updateQuota(Some("userC"), None, None, Some(new Quota(10000, true)))
      quotaManager.updateQuota(None, Some("client1"), Some("client1"), Some(new Quota(9000, true)))

      checkQuota("userA", "client1", 5000, 4500, false) // <user, client> quota takes precedence over <user>
      checkQuota("userA", "client2", 4000, 4500, true)  // <user> quota takes precedence over <client> and defaults
      checkQuota("userA", "client3", 4000, 0, true)     // <user> quota is shared across clients of user
      checkQuota("userA", "client1", 5000, 0, false)    // <user, client> is exclusive use, unaffected by other clients

      checkQuota("userB", "client1", 7000, 8000, true)
      checkQuota("userB", "client2", 8000, 7000, false) // Default per-client quota for exclusive use of <user, client>
      checkQuota("userB", "client3", 8000, 7000, false)

      checkQuota("userD", "client1", 3000, 3500, true)  // Default <user, client> quota
      checkQuota("userD", "client2", 3000, 2500, false)
      checkQuota("userE", "client1", 3000, 2500, false)

      // Remove default <user, client> quota config, revert to <user> default
      quotaManager.updateQuota(Some(ConfigEntityName.Default), Some(ConfigEntityName.Default), Some(ConfigEntityName.Default), None)
      checkQuota("userD", "client1", 1000, 0, false)    // Metrics tags changed, restart counter
      checkQuota("userE", "client4", 1000, 1500, true)
      checkQuota("userF", "client4", 1000, 800, false)  // Default <user> quota shared across clients of user
      checkQuota("userF", "client5", 1000, 800, true)

      // Remove default <user> quota config, revert to <client-id> default
      quotaManager.updateQuota(Some(ConfigEntityName.Default), None, None, None)
      checkQuota("userF", "client4", 2000, 0, false)  // Default <client-id> quota shared across client-id of all users
      checkQuota("userF", "client5", 2000, 0, false)
      checkQuota("userF", "client5", 2000, 2500, true)
      checkQuota("userG", "client5", 2000, 0, true)

      // Update quotas
      quotaManager.updateQuota(Some("userA"), None, None, Some(new Quota(8000, true)))
      quotaManager.updateQuota(Some("userA"), Some("client1"), Some("client1"), Some(new Quota(10000, true)))
      checkQuota("userA", "client2", 8000, 0, false)
      checkQuota("userA", "client2", 8000, 4500, true) // Throttled due to sum of new and earlier values
      checkQuota("userA", "client1", 10000, 0, false)
      checkQuota("userA", "client1", 10000, 6000, true)
      quotaManager.updateQuota(Some("userA"), Some("client1"), Some("client1"), None)
      checkQuota("userA", "client6", 8000, 0, true)    // Throttled due to shared user quota
      quotaManager.updateQuota(Some("userA"), Some("client6"), Some("client6"), Some(new Quota(11000, true)))
      checkQuota("userA", "client6", 11000, 8500, false)
      quotaManager.updateQuota(Some("userA"), Some(ConfigEntityName.Default), Some(ConfigEntityName.Default), Some(new Quota(12000, true)))
      quotaManager.updateQuota(Some("userA"), Some("client6"), Some("client6"), None)
      checkQuota("userA", "client6", 12000, 4000, true) // Throttled due to sum of new and earlier values

    } finally {
      quotaManager.shutdown()
    }
  }

  @Test
  def testQuotaViolation() {
    val metrics = newMetrics
    val clientMetrics = new ClientQuotaManager(config, metrics, Produce, time, "")
    val queueSizeMetric = metrics.metrics().get(metrics.metricName("queue-size", "Produce", ""))
    try {
      /* We have 10 second windows. Make sure that there is no quota violation
       * if we produce under the quota
       */
      for (_ <- 0 until 10) {
        assertEquals(0, maybeRecord(clientMetrics, "ANONYMOUS", "unknown", 400))
        time.sleep(1000)
      }
      assertEquals(0, queueSizeMetric.value().toInt)

      // Create a spike.
      // 400*10 + 2000 + 300 = 6300/10.5 = 600 bytes per second.
      // (600 - quota)/quota*window-size = (600-500)/500*10.5 seconds = 2100
      // 10.5 seconds because the last window is half complete
      time.sleep(500)
      val sleepTime = maybeRecord(clientMetrics, "ANONYMOUS", "unknown", 2300)

      assertEquals("Should be throttled", 2100, sleepTime)
      throttle(clientMetrics, "ANONYMOYUS", "unknown", sleepTime, callback)
      assertEquals(1, queueSizeMetric.value().toInt)
      // After a request is delayed, the callback cannot be triggered immediately
      clientMetrics.throttledChannelReaper.doWork()
      assertEquals(0, numCallbacks)
      time.sleep(sleepTime)

      // Callback can only be triggered after the delay time passes
      clientMetrics.throttledChannelReaper.doWork()
      assertEquals(0, queueSizeMetric.value().toInt)
      assertEquals(1, numCallbacks)

      // Could continue to see delays until the bursty sample disappears
      for (_ <- 0 until 10) {
        maybeRecord(clientMetrics, "ANONYMOUS", "unknown", 400)
        time.sleep(1000)
      }

      assertEquals("Should be unthrottled since bursty sample has rolled over",
                   0, maybeRecord(clientMetrics, "ANONYMOUS", "unknown", 0))
    } finally {
      clientMetrics.shutdown()
    }
  }

  @Test
  def testRequestPercentageQuotaViolation() {
    val metrics = newMetrics
    val quotaManager = new ClientRequestQuotaManager(config, metrics, time, "", None)
    quotaManager.updateQuota(Some("ANONYMOUS"), Some("test-client"), Some("test-client"), Some(Quota.upperBound(1)))
    val queueSizeMetric = metrics.metrics().get(metrics.metricName("queue-size", "Request", ""))
    def millisToPercent(millis: Double) = millis * 1000 * 1000 * ClientQuotaManagerConfig.NanosToPercentagePerSecond
    try {
      /* We have 10 second windows. Make sure that there is no quota violation
       * if we are under the quota
       */
      for (_ <- 0 until 10) {
        assertEquals(0, maybeRecord(quotaManager, "ANONYMOUS", "test-client", millisToPercent(4)))
        time.sleep(1000)
      }
      assertEquals(0, queueSizeMetric.value().toInt)

      // Create a spike.
      // quota = 1% (10ms per second)
      // 4*10 + 67.1 = 107.1/10.5 = 10.2ms per second.
      // (10.2 - quota)/quota*window-size = (10.2-10)/10*10.5 seconds = 210ms
      // 10.5 seconds interval because the last window is half complete
      time.sleep(500)
      val throttleTime = maybeRecord(quotaManager, "ANONYMOUS", "test-client", millisToPercent(67.1))

      assertEquals("Should be throttled", 210, throttleTime)

      throttle(quotaManager, "ANONYMOYUS", "test-client", throttleTime, callback)
      assertEquals(1, queueSizeMetric.value().toInt)
      // After a request is delayed, the callback cannot be triggered immediately
      quotaManager.throttledChannelReaper.doWork()
      assertEquals(0, numCallbacks)
      time.sleep(throttleTime)

      // Callback can only be triggered after the delay time passes
      quotaManager.throttledChannelReaper.doWork()
      assertEquals(0, queueSizeMetric.value().toInt)
      assertEquals(1, numCallbacks)

      // Could continue to see delays until the bursty sample disappears
      for (_ <- 0 until 11) {
        maybeRecord(quotaManager, "ANONYMOUS", "test-client", millisToPercent(4))
        time.sleep(1000)
      }

      assertEquals("Should be unthrottled since bursty sample has rolled over",
                   0, maybeRecord(quotaManager, "ANONYMOUS", "test-client", 0))

      // Create a very large spike which requires > one quota window to bring within quota
      assertEquals(1000, maybeRecord(quotaManager, "ANONYMOUS", "test-client", millisToPercent(500)))
      for (_ <- 0 until 10) {
        time.sleep(1000)
        assertEquals(1000, maybeRecord(quotaManager, "ANONYMOUS", "test-client", 0))
      }
      time.sleep(1000)
      assertEquals("Should be unthrottled since bursty sample has rolled over",
                   0, maybeRecord(quotaManager, "ANONYMOUS", "test-client", 0))

    } finally {
      quotaManager.shutdown()
    }
  }

  @Test
  def testExpireThrottleTimeSensor() {
    val metrics = newMetrics
    val clientMetrics = new ClientQuotaManager(config, metrics, Produce, time, "")
    try {
      maybeRecord(clientMetrics, "ANONYMOUS", "client1", 100)
      // remove the throttle time sensor
      metrics.removeSensor("ProduceThrottleTime-:client1")
      // should not throw an exception even if the throttle time sensor does not exist.
      val throttleTime = maybeRecord(clientMetrics, "ANONYMOUS", "client1", 10000)
      assertTrue("Should be throttled", throttleTime > 0)
      // the sensor should get recreated
      val throttleTimeSensor = metrics.getSensor("ProduceThrottleTime-:client1")
      assertTrue("Throttle time sensor should exist", throttleTimeSensor != null)
    } finally {
      clientMetrics.shutdown()
    }
  }

  @Test
  def testExpireQuotaSensors() {
    val metrics = newMetrics
    val clientMetrics = new ClientQuotaManager(config, metrics, Produce, time, "")
    try {
      maybeRecord(clientMetrics, "ANONYMOUS", "client1", 100)
      // remove all the sensors
      metrics.removeSensor("ProduceThrottleTime-:client1")
      metrics.removeSensor("Produce-ANONYMOUS:client1")
      // should not throw an exception
      val throttleTime = maybeRecord(clientMetrics, "ANONYMOUS", "client1", 10000)
      assertTrue("Should be throttled", throttleTime > 0)

      // all the sensors should get recreated
      val throttleTimeSensor = metrics.getSensor("ProduceThrottleTime-:client1")
      assertTrue("Throttle time sensor should exist", throttleTimeSensor != null)

      val byteRateSensor = metrics.getSensor("Produce-:client1")
      assertTrue("Byte rate sensor should exist", byteRateSensor != null)
    } finally {
      clientMetrics.shutdown()
    }
  }

  @Test
  def testClientIdNotSanitized() {
    val metrics = newMetrics
    val clientMetrics = new ClientQuotaManager(config, metrics, Produce, time, "")
    val clientId = "client@#$%"
    try {
      maybeRecord(clientMetrics, "ANONYMOUS", clientId, 100)

      // The metrics should use the raw client ID, even if the reporters internally sanitize them
      val throttleTimeSensor = metrics.getSensor("ProduceThrottleTime-:" + clientId)
      assertTrue("Throttle time sensor should exist", throttleTimeSensor != null)

      val byteRateSensor = metrics.getSensor("Produce-:"  + clientId)
      assertTrue("Byte rate sensor should exist", byteRateSensor != null)
    } finally {
      clientMetrics.shutdown()
    }
  }

  def newMetrics: Metrics = {
    new Metrics(new MetricConfig(), Collections.emptyList(), time)
  }

  private case class UserClient(val user: String, val clientId: String, val configUser: Option[String] = None, val configClientId: Option[String] = None) {
    // The class under test expects only sanitized client configs. We pass both the default value (which should not be
    // sanitized to ensure it remains unique) and non-default values, so we need to take care in generating the sanitized
    // client ID
    def sanitizedConfigClientId = configClientId.map(x => if (x == ConfigEntityName.Default) ConfigEntityName.Default else Sanitizer.sanitize(x))
  }
}
