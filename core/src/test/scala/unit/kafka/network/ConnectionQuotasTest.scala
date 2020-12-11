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

package kafka.network

import java.net.InetAddress
import java.util
import java.util.concurrent.{Callable, ExecutorService, Executors, TimeUnit}
import java.util.{Collections, Properties}

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.network.Processor.ListenerMetricTag
import kafka.server.{DynamicConfig, KafkaConfig}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.metrics.internals.MetricsUtils
import org.apache.kafka.common.metrics.{KafkaMetric, MetricConfig, Metrics}
import org.apache.kafka.common.network._
import org.apache.kafka.common.utils.Time
import org.junit.Assert._
import org.junit._
import org.scalatest.Assertions.{assertThrows, intercept}

import scala.jdk.CollectionConverters._
import scala.collection.{Map, mutable}
import scala.concurrent.TimeoutException

class ConnectionQuotasTest {
  private var metrics: Metrics = _
  private var executor: ExecutorService = _
  private var connectionQuotas: ConnectionQuotas = _
  private var time: Time = _

  private val listeners = Map(
    "EXTERNAL" -> ListenerDesc(new ListenerName("EXTERNAL"), InetAddress.getByName("192.168.1.1")),
    "ADMIN" -> ListenerDesc(new ListenerName("ADMIN"), InetAddress.getByName("192.168.1.2")),
    "REPLICATION" -> ListenerDesc(new ListenerName("REPLICATION"), InetAddress.getByName("192.168.1.3")))
  private val blockedPercentMeters = mutable.Map[String, Meter]()
  private val knownHost = InetAddress.getByName("192.168.10.0")
  private val unknownHost = InetAddress.getByName("192.168.2.0")

  private val numQuotaSamples = 2
  private val quotaWindowSizeSeconds = 1
  private val eps = 0.01

  case class ListenerDesc(listenerName: ListenerName, defaultIp: InetAddress) {
    override def toString: String = {
      s"(listener=${listenerName.value}, client=${defaultIp.getHostAddress})"
    }
  }

  def brokerPropsWithDefaultConnectionLimits: Properties = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    props.put(KafkaConfig.ListenersProp, "EXTERNAL://localhost:0,REPLICATION://localhost:1,ADMIN://localhost:2")
    // ConnectionQuotas does not limit inter-broker listener even when broker-wide connection limit is reached
    props.put(KafkaConfig.InterBrokerListenerNameProp, "REPLICATION")
    props.put(KafkaConfig.ListenerSecurityProtocolMapProp, "EXTERNAL:PLAINTEXT,REPLICATION:PLAINTEXT,ADMIN:PLAINTEXT")
    props.put(KafkaConfig.NumQuotaSamplesProp, numQuotaSamples.toString)
    props.put(KafkaConfig.QuotaWindowSizeSecondsProp, quotaWindowSizeSeconds.toString)
    props
  }

  private def setupMockTime(): Unit = {
    // clean up metrics initialized with Time.SYSTEM
    metrics.close()
    time = new MockTime()
    metrics = new Metrics(time)
  }

  @Before
  def setUp(): Unit = {
    // Clean-up any metrics left around by previous tests
    TestUtils.clearYammerMetrics()

    listeners.keys.foreach { name =>
        blockedPercentMeters.put(name, KafkaMetricsGroup.newMeter(
          s"${name}BlockedPercent", "blocked time", TimeUnit.NANOSECONDS, Map(ListenerMetricTag -> name)))
    }
    // use system time, because ConnectionQuota causes the current thread to wait with timeout, which waits based on
    // system time; so using mock time will likely result in test flakiness due to a mixed use of mock and system time
    time = Time.SYSTEM
    metrics = new Metrics(new MetricConfig(), Collections.emptyList(), time)
    executor = Executors.newFixedThreadPool(listeners.size)
  }

  @After
  def tearDown(): Unit = {
    executor.shutdownNow()
    if (connectionQuotas != null) {
      connectionQuotas.close()
    }
    metrics.close()
    TestUtils.clearYammerMetrics()
    blockedPercentMeters.clear()
  }

  @Test
  def testFailWhenNoListeners(): Unit = {
    val config = KafkaConfig.fromProps(brokerPropsWithDefaultConnectionLimits)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)

    // inc() on a separate thread in case it blocks
    val listener = listeners("EXTERNAL")
    executor.submit((() =>
      intercept[RuntimeException](
        connectionQuotas.inc(listener.listenerName, listener.defaultIp, blockedPercentMeters("EXTERNAL"))
      )): Runnable
    ).get(5, TimeUnit.SECONDS)
  }

  @Test
  def testFailDecrementForUnknownIp(): Unit = {
    val config = KafkaConfig.fromProps(brokerPropsWithDefaultConnectionLimits)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)
    addListenersAndVerify(config, connectionQuotas)

    // calling dec() for an IP for which we didn't call inc() should throw an exception
    intercept[IllegalArgumentException](connectionQuotas.dec(listeners("EXTERNAL").listenerName, unknownHost))
  }

  @Test
  def testNoConnectionLimitsByDefault(): Unit = {
    val config = KafkaConfig.fromProps(brokerPropsWithDefaultConnectionLimits)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)
    addListenersAndVerify(config, connectionQuotas)

    // verify there is no limit by accepting 10000 connections as fast as possible
    val numConnections = 10000
    val futures = listeners.values.map { listener =>
      executor.submit((() => acceptConnections(connectionQuotas, listener, numConnections)): Runnable)
    }
    futures.foreach(_.get(10, TimeUnit.SECONDS))
    assertTrue("Expected broker-connection-accept-rate metric to get recorded",
      metricValue(brokerConnRateMetric())> 0)
    listeners.values.foreach { listener =>
      assertEquals(s"Number of connections on $listener:",
        numConnections, connectionQuotas.get(listener.defaultIp))

      assertTrue(s"Expected connection-accept-rate metric to get recorded for listener $listener",
        metricValue(listenerConnRateMetric(listener.listenerName.value)) > 0)

      // verify removing one connection
      connectionQuotas.dec(listener.listenerName, listener.defaultIp)
      assertEquals(s"Number of connections on $listener:",
        numConnections - 1, connectionQuotas.get(listener.defaultIp))
    }
    // the blocked percent should still be 0, because no limits were reached
    verifyNoBlockedPercentRecordedOnAllListeners()
  }

  @Test
  def testMaxConnectionsPerIp(): Unit = {
    val maxConnectionsPerIp = 17
    val props = brokerPropsWithDefaultConnectionLimits
    props.put(KafkaConfig.MaxConnectionsPerIpProp, maxConnectionsPerIp.toString)
    val config = KafkaConfig.fromProps(props)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)

    addListenersAndVerify(config, connectionQuotas)

    val externalListener = listeners("EXTERNAL")
    executor.submit((() =>
      acceptConnections(connectionQuotas, externalListener, maxConnectionsPerIp)): Runnable
    ).get(5, TimeUnit.SECONDS)
    assertEquals(s"Number of connections on $externalListener:",
      maxConnectionsPerIp, connectionQuotas.get(externalListener.defaultIp))

    // all subsequent connections will be added to the counters, but inc() will throw TooManyConnectionsException for each
    executor.submit((() =>
      acceptConnectionsAboveIpLimit(connectionQuotas, externalListener, 2)): Runnable
    ).get(5, TimeUnit.SECONDS)
    assertEquals(s"Number of connections on $externalListener:",
      maxConnectionsPerIp + 2, connectionQuotas.get(externalListener.defaultIp))

    // connections on the same listener but from a different IP should be accepted
    executor.submit((() =>
      acceptConnections(connectionQuotas, externalListener.listenerName, knownHost, maxConnectionsPerIp,
        0, expectIpThrottle = false)): Runnable
    ).get(5, TimeUnit.SECONDS)

    // remove two "rejected" connections and remove 2 more connections to free up the space for another 2 connections
    for (_ <- 0 until 4) connectionQuotas.dec(externalListener.listenerName, externalListener.defaultIp)
    assertEquals(s"Number of connections on $externalListener:",
      maxConnectionsPerIp - 2, connectionQuotas.get(externalListener.defaultIp))

    executor.submit((() =>
      acceptConnections(connectionQuotas, externalListener, 2)): Runnable
    ).get(5, TimeUnit.SECONDS)
    assertEquals(s"Number of connections on $externalListener:",
      maxConnectionsPerIp, connectionQuotas.get(externalListener.defaultIp))
  }

  @Test
  def testMaxBrokerWideConnectionLimit(): Unit = {
    val maxConnections = 800
    val props = brokerPropsWithDefaultConnectionLimits
    props.put(KafkaConfig.MaxConnectionsProp, maxConnections.toString)
    val config = KafkaConfig.fromProps(props)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)

    addListenersAndVerify(config, connectionQuotas)

    // verify that ConnectionQuota can give all connections to one listener
    executor.submit((() =>
      acceptConnections(connectionQuotas, listeners("EXTERNAL"), maxConnections)): Runnable
    ).get(5, TimeUnit.SECONDS)
    assertEquals(s"Number of connections on ${listeners("EXTERNAL")}:",
      maxConnections, connectionQuotas.get(listeners("EXTERNAL").defaultIp))

    // the blocked percent should still be 0, because there should be no wait for a connection slot
    assertEquals(0, blockedPercentMeters("EXTERNAL").count())

    // the number of connections should be above max for maxConnectionsExceeded to return true
    assertFalse("Total number of connections is exactly the maximum.",
      connectionQuotas.maxConnectionsExceeded(listeners("EXTERNAL").listenerName))

    // adding one more connection will block ConnectionQuota.inc()
    val future = executor.submit((() =>
      acceptConnections(connectionQuotas, listeners("EXTERNAL"), 1)): Runnable
    )
    intercept[TimeoutException](future.get(100, TimeUnit.MILLISECONDS))

    // removing one connection should make the waiting connection to succeed
    connectionQuotas.dec(listeners("EXTERNAL").listenerName, listeners("EXTERNAL").defaultIp)
    future.get(1, TimeUnit.SECONDS)
    assertEquals(s"Number of connections on ${listeners("EXTERNAL")}:",
      maxConnections, connectionQuotas.get(listeners("EXTERNAL").defaultIp))
    // metric is recorded in nanoseconds
    assertTrue("Expected BlockedPercentMeter metric to be recorded",
      blockedPercentMeters("EXTERNAL").count() > 0)

    // adding inter-broker connections should succeed even when the total number of connections reached the max
    executor.submit((() =>
      acceptConnections(connectionQuotas, listeners("REPLICATION"), 1)): Runnable
    ).get(5, TimeUnit.SECONDS)
    assertTrue("Expected the number of connections to exceed the maximum.",
      connectionQuotas.maxConnectionsExceeded(listeners("EXTERNAL").listenerName))

    // adding one more connection on another non-inter-broker will block ConnectionQuota.inc()
    val future1 = executor.submit((() =>
      acceptConnections(connectionQuotas, listeners("ADMIN"), 1)): Runnable
    )
    intercept[TimeoutException](future1.get(1, TimeUnit.SECONDS))

    // adding inter-broker connection should still succeed, even though a connection from another listener is waiting
    executor.submit((() =>
      acceptConnections(connectionQuotas, listeners("REPLICATION"), 1)): Runnable
    ).get(5, TimeUnit.SECONDS)

    // at this point, we need to remove 3 connections for the waiting connection to succeed
    // remove 2 first -- should not be enough to accept the waiting connection
    for (_ <- 0 until 2) connectionQuotas.dec(listeners("EXTERNAL").listenerName, listeners("EXTERNAL").defaultIp)
    intercept[TimeoutException](future1.get(100, TimeUnit.MILLISECONDS))
    connectionQuotas.dec(listeners("EXTERNAL").listenerName, listeners("EXTERNAL").defaultIp)
    future1.get(1, TimeUnit.SECONDS)
  }

  @Test
  def testMaxListenerConnectionLimits(): Unit = {
    val maxConnections = 800
    // sum of per-listener connection limits is below total connection limit
    val listenerMaxConnections = 200
    val props = brokerPropsWithDefaultConnectionLimits
    props.put(KafkaConfig.MaxConnectionsProp, maxConnections.toString)
    val config = KafkaConfig.fromProps(props)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)

    addListenersAndVerify(config, connectionQuotas)

    val listenerConfig = Map(KafkaConfig.MaxConnectionsProp -> listenerMaxConnections.toString).asJava
    listeners.values.foreach { listener =>
      connectionQuotas.maxConnectionsPerListener(listener.listenerName).configure(listenerConfig)
    }

    // verify each listener can create up to max connections configured for that listener
    val futures = listeners.values.map { listener =>
      executor.submit((() => acceptConnections(connectionQuotas, listener, listenerMaxConnections)): Runnable)
    }
    futures.foreach(_.get(5, TimeUnit.SECONDS))
    listeners.values.foreach { listener =>
      assertEquals(s"Number of connections on $listener:",
        listenerMaxConnections, connectionQuotas.get(listener.defaultIp))
      assertFalse(s"Total number of connections on $listener should be exactly the maximum.",
        connectionQuotas.maxConnectionsExceeded(listener.listenerName))
    }

    // since every listener has exactly the max number of listener connections,
    // every listener should block on the next connection creation, even the inter-broker listener
    val overLimitFutures = listeners.values.map { listener =>
      executor.submit((() => acceptConnections(connectionQuotas, listener, 1)): Runnable)
    }
    overLimitFutures.foreach { future =>
      intercept[TimeoutException](future.get(1, TimeUnit.SECONDS))
    }
    listeners.values.foreach { listener =>
      // free up one connection slot
      connectionQuotas.dec(listener.listenerName, listener.defaultIp)
    }
    // all connections should get added
    overLimitFutures.foreach(_.get(5, TimeUnit.SECONDS))
    verifyConnectionCountOnEveryListener(connectionQuotas, listenerMaxConnections)
  }

  @Test
  def testBrokerConnectionRateLimitWhenActualRateBelowLimit(): Unit = {
    val brokerRateLimit = 125
    // create connections with the total rate < broker-wide quota, and verify there is no throttling
    val connCreateIntervalMs = 25 // connection creation rate = 40/sec per listener (3 * 40 = 120/sec total)
    val connectionsPerListener = 200 // should take 5 seconds to create 200 connections with rate = 40/sec
    val props = brokerPropsWithDefaultConnectionLimits
    props.put(KafkaConfig.MaxConnectionCreationRateProp, brokerRateLimit.toString)
    val config = KafkaConfig.fromProps(props)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)

    addListenersAndVerify(config, connectionQuotas)

    val futures = listeners.values.map { listener =>
      executor.submit((() => acceptConnections(connectionQuotas, listener, connectionsPerListener, connCreateIntervalMs)): Runnable)
    }
    futures.foreach(_.get(10, TimeUnit.SECONDS))

    // the blocked percent should still be 0, because no limits were reached
    verifyNoBlockedPercentRecordedOnAllListeners()
    verifyConnectionCountOnEveryListener(connectionQuotas, connectionsPerListener)
  }

  @Test
  def testBrokerConnectionRateLimitWhenActualRateAboveLimit(): Unit = {
    val brokerRateLimit = 90
    val props = brokerPropsWithDefaultConnectionLimits
    props.put(KafkaConfig.MaxConnectionCreationRateProp, brokerRateLimit.toString)
    val config = KafkaConfig.fromProps(props)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)

    addListenersAndVerify(config, connectionQuotas)

    // each listener creates connections such that the total connection rate > broker-wide quota
    val connCreateIntervalMs = 10      // connection creation rate = 100
    val connectionsPerListener = 400
    val futures = listeners.values.map { listener =>
      executor.submit((() => acceptConnections(connectionQuotas, listener, connectionsPerListener, connCreateIntervalMs)): Runnable)
    }
    futures.foreach(_.get(20, TimeUnit.SECONDS))

    // verify that connections on non-inter-broker listener are throttled
    verifyOnlyNonInterBrokerListenersBlockedPercentRecorded()

    // expect all connections to be created (no limit on the number of connections)
    verifyConnectionCountOnEveryListener(connectionQuotas, connectionsPerListener)
  }

  @Test
  def testListenerConnectionRateLimitWhenActualRateBelowLimit(): Unit = {
    val brokerRateLimit = 125
    val listenerRateLimit = 50
    val connCreateIntervalMs = 25 // connection creation rate = 40/sec per listener (3 * 40 = 120/sec total)
    val props = brokerPropsWithDefaultConnectionLimits
    props.put(KafkaConfig.MaxConnectionCreationRateProp, brokerRateLimit.toString)
    val config = KafkaConfig.fromProps(props)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)

    val listenerConfig = Map(KafkaConfig.MaxConnectionCreationRateProp -> listenerRateLimit.toString).asJava
    addListenersAndVerify(config, listenerConfig, connectionQuotas)

    // create connections with the rate < listener quota on every listener, and verify there is no throttling
    val connectionsPerListener = 200 // should take 5 seconds to create 200 connections with rate = 40/sec
    val futures = listeners.values.map { listener =>
      executor.submit((() => acceptConnections(connectionQuotas, listener, connectionsPerListener, connCreateIntervalMs)): Runnable)
    }
    futures.foreach(_.get(10, TimeUnit.SECONDS))

    // the blocked percent should still be 0, because no limits were reached
    verifyNoBlockedPercentRecordedOnAllListeners()

    verifyConnectionCountOnEveryListener(connectionQuotas, connectionsPerListener)
  }

  @Test
  def testListenerConnectionRateLimitWhenActualRateAboveLimit(): Unit = {
    val brokerRateLimit = 125
    val listenerRateLimit = 30
    val connCreateIntervalMs = 25 // connection creation rate = 40/sec per listener (3 * 40 = 120/sec total)
    val props = brokerPropsWithDefaultConnectionLimits
    props.put(KafkaConfig.MaxConnectionCreationRateProp, brokerRateLimit.toString)
    val config = KafkaConfig.fromProps(props)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)

    val listenerConfig = Map(KafkaConfig.MaxConnectionCreationRateProp -> listenerRateLimit.toString).asJava
    addListenersAndVerify(config, listenerConfig, connectionQuotas)

    // create connections with the rate > listener quota on every listener
    // run a bit longer (20 seconds) to also verify the throttle rate
    val connectionsPerListener = 600 // should take 20 seconds to create 600 connections with rate = 30/sec
    val futures = listeners.values.map { listener =>
      executor.submit((() =>
        // epsilon is set to account for the worst-case where the measurement is taken just before or after the quota window
        acceptConnectionsAndVerifyRate(connectionQuotas, listener, connectionsPerListener, connCreateIntervalMs, listenerRateLimit, 7)): Runnable)
    }
    futures.foreach(_.get(30, TimeUnit.SECONDS))

    // verify that every listener was throttled
    verifyNonZeroBlockedPercentAndThrottleTimeOnAllListeners()

    // while the connection creation rate was throttled,
    // expect all connections got created (not limit on the number of connections)
    verifyConnectionCountOnEveryListener(connectionQuotas, connectionsPerListener)
  }

  @Test
  def testIpConnectionRateWhenActualRateBelowLimit(): Unit = {
    val ipConnectionRateLimit = 30
    val connCreateIntervalMs = 40 // connection creation rate = 25/sec
    val props = brokerPropsWithDefaultConnectionLimits
    val config = KafkaConfig.fromProps(props)
    // use MockTime for IP connection rate quota tests that don't expect to block
    setupMockTime()
    connectionQuotas = new ConnectionQuotas(config, time, metrics)
    addListenersAndVerify(config, connectionQuotas)
    val externalListener = listeners("EXTERNAL")
    connectionQuotas.updateIpConnectionRateQuota(Some(externalListener.defaultIp), Some(ipConnectionRateLimit))
    val numConnections = 200
    // create connections with the rate < ip quota and verify there is no throttling
    acceptConnectionsAndVerifyRate(connectionQuotas, externalListener, numConnections, connCreateIntervalMs,
      expectedRate = 25, epsilon = 0)
    assertEquals(s"Number of connections on $externalListener:",
      numConnections, connectionQuotas.get(externalListener.defaultIp))

    val adminListener = listeners("ADMIN")
    val unthrottledConnectionCreateInterval = 20 // connection creation rate = 50/s
    // create connections with an IP with no quota and verify there is no throttling
    acceptConnectionsAndVerifyRate(connectionQuotas, adminListener, numConnections, unthrottledConnectionCreateInterval,
      expectedRate = 50, epsilon = 0)

    assertEquals(s"Number of connections on $adminListener:",
      numConnections, connectionQuotas.get(adminListener.defaultIp))

    // acceptor shouldn't block for IP rate throttling
    verifyNoBlockedPercentRecordedOnAllListeners()
    // no IP throttle time should be recorded on any listeners
    listeners.values.map(_.listenerName).foreach(verifyIpThrottleTimeOnListener(_, expectThrottle = false))
  }

  @Test
  def testIpConnectionRateWhenActualRateAboveLimit(): Unit = {
    val ipConnectionRateLimit = 20
    val connCreateIntervalMs = 25 // connection creation rate = 40/sec
    val props = brokerPropsWithDefaultConnectionLimits
    val config = KafkaConfig.fromProps(props)
    // use MockTime for IP connection rate quota tests that don't expect to block
    setupMockTime()
    connectionQuotas = new ConnectionQuotas(config, time, metrics)
    addListenersAndVerify(config, connectionQuotas)
    val externalListener = listeners("EXTERNAL")
    connectionQuotas.updateIpConnectionRateQuota(Some(externalListener.defaultIp), Some(ipConnectionRateLimit))
    // create connections with the rate > ip quota
    val numConnections = 80
    acceptConnectionsAndVerifyRate(connectionQuotas, externalListener, numConnections, connCreateIntervalMs, ipConnectionRateLimit,
      1, expectIpThrottle = true)
    verifyIpThrottleTimeOnListener(externalListener.listenerName, expectThrottle = true)

    // verify that default quota applies to IPs without a quota override
    connectionQuotas.updateIpConnectionRateQuota(None, Some(ipConnectionRateLimit))
    val adminListener = listeners("ADMIN")
    // listener shouldn't have any IP throttle time recorded
    verifyIpThrottleTimeOnListener(adminListener.listenerName, expectThrottle = false)
    acceptConnectionsAndVerifyRate(connectionQuotas, adminListener, numConnections, connCreateIntervalMs, ipConnectionRateLimit,
      1, expectIpThrottle = true)
    verifyIpThrottleTimeOnListener(adminListener.listenerName, expectThrottle = true)

    // acceptor shouldn't block for IP rate throttling
    verifyNoBlockedPercentRecordedOnAllListeners()
    // replication listener shouldn't have any IP throttling recorded
    verifyIpThrottleTimeOnListener(listeners("REPLICATION").listenerName, expectThrottle = false)
  }

  @Test
  def testIpConnectionRateWithListenerConnectionRate(): Unit = {
    val ipConnectionRateLimit = 25
    val listenerRateLimit = 35
    val props = brokerPropsWithDefaultConnectionLimits
    val config = KafkaConfig.fromProps(props)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)
    // with a default per-IP limit of 25 and a listener rate of 30, only one IP should be able to saturate their IP rate
    // limit, the other IP will hit listener rate limits and block
    connectionQuotas.updateIpConnectionRateQuota(None, Some(ipConnectionRateLimit))
    val listenerConfig = Map(KafkaConfig.MaxConnectionCreationRateProp -> listenerRateLimit.toString).asJava
    addListenersAndVerify(config, listenerConfig, connectionQuotas)
    val listener = listeners("EXTERNAL").listenerName
    // use a small number of connections because a longer-running test will have both IPs throttle at different times
    val numConnections = 35
    val futures = List(
      executor.submit((() => acceptConnections(connectionQuotas, listener, knownHost, numConnections,
        0, true)): Callable[Boolean]),
      executor.submit((() => acceptConnections(connectionQuotas, listener, unknownHost, numConnections,
        0, true)): Callable[Boolean])
    )

    val ipsThrottledResults = futures.map(_.get(3, TimeUnit.SECONDS))
    val throttledIps = ipsThrottledResults.filter(identity)
    // at most one IP should get IP throttled before the acceptor blocks on listener quota
    assertTrue("Expected BlockedPercentMeter metric for EXTERNAL listener to be recorded", blockedPercentMeters("EXTERNAL").count() > 0)
    assertTrue("Expect at most one IP to get throttled", throttledIps.size < 2)
  }

  @Test
  def testRejectedIpConnectionUnrecordedFromConnectionRateQuotas(): Unit = {
    val config = KafkaConfig.fromProps(brokerPropsWithDefaultConnectionLimits)
    connectionQuotas = new ConnectionQuotas(config, new MockTime(), metrics)
    addListenersAndVerify(config, connectionQuotas)
    val externalListener = listeners("EXTERNAL")
    val protectedListener = listeners("REPLICATION")
    connectionQuotas.updateIpConnectionRateQuota(Some(externalListener.defaultIp), Some(0))
    connectionQuotas.updateIpConnectionRateQuota(Some(protectedListener.defaultIp), Some(0))

    assertThrows[ConnectionThrottledException] {
      connectionQuotas.inc(externalListener.listenerName, externalListener.defaultIp, blockedPercentMeters("EXTERNAL"))
    }

    val brokerRateMetric = brokerConnRateMetric()
    // rejected connection shouldn't be recorded for any of the connection accepted rate metrics
    assertEquals(0, metricValue(ipConnRateMetric(externalListener.defaultIp.getHostAddress)), eps)
    assertEquals(0, metricValue(listenerConnRateMetric(externalListener.listenerName.value)), eps)
    assertEquals(0, metricValue(brokerRateMetric), eps)

    assertThrows[ConnectionThrottledException] {
      connectionQuotas.inc(protectedListener.listenerName, protectedListener.defaultIp, blockedPercentMeters("REPLICATION"))
    }

    assertEquals(0, metricValue(ipConnRateMetric(protectedListener.defaultIp.getHostAddress)), eps)
    assertEquals(0, metricValue(listenerConnRateMetric(protectedListener.listenerName.value)), eps)
    assertEquals(0, metricValue(brokerRateMetric), eps)
  }

  @Test
  def testMaxListenerConnectionListenerMustBeAboveZero(): Unit = {
    val config = KafkaConfig.fromProps(brokerPropsWithDefaultConnectionLimits)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)

    connectionQuotas.addListener(config, listeners("EXTERNAL").listenerName)

    val maxListenerConnectionRate = 0
    val listenerConfig = Map(KafkaConfig.MaxConnectionCreationRateProp -> maxListenerConnectionRate.toString).asJava
    assertThrows[ConfigException] {
      connectionQuotas.maxConnectionsPerListener(listeners("EXTERNAL").listenerName).validateReconfiguration(listenerConfig)
    }
  }

  @Test
  def testMaxListenerConnectionRateReconfiguration(): Unit = {
    val config = KafkaConfig.fromProps(brokerPropsWithDefaultConnectionLimits)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)
    connectionQuotas.addListener(config, listeners("EXTERNAL").listenerName)

    val listenerRateLimit = 20
    val listenerConfig = Map(KafkaConfig.MaxConnectionCreationRateProp -> listenerRateLimit.toString).asJava
    connectionQuotas.maxConnectionsPerListener(listeners("EXTERNAL").listenerName).configure(listenerConfig)

    // remove connection rate limit
    connectionQuotas.maxConnectionsPerListener(listeners("EXTERNAL").listenerName).reconfigure(Map.empty.asJava)

    // create connections as fast as possible, will timeout if connections get throttled with previous rate
    // (50s to create 1000 connections)
    executor.submit((() =>
      acceptConnections(connectionQuotas, listeners("EXTERNAL"), 1000)): Runnable
    ).get(10, TimeUnit.SECONDS)
    // verify no throttling
    assertEquals(s"BlockedPercentMeter metric for EXTERNAL listener", 0, blockedPercentMeters("EXTERNAL").count())

    // configure 100 connection/second rate limit
    val newMaxListenerConnectionRate = 10
    val newListenerConfig = Map(KafkaConfig.MaxConnectionCreationRateProp -> newMaxListenerConnectionRate.toString).asJava
    connectionQuotas.maxConnectionsPerListener(listeners("EXTERNAL").listenerName).reconfigure(newListenerConfig)

    // verify rate limit
    val connectionsPerListener = 200 // should take 20 seconds to create 200 connections with rate = 10/sec
    executor.submit((() =>
      acceptConnectionsAndVerifyRate(connectionQuotas, listeners("EXTERNAL"), connectionsPerListener, 5, newMaxListenerConnectionRate, 3)): Runnable
    ).get(30, TimeUnit.SECONDS)
    assertTrue("Expected BlockedPercentMeter metric for EXTERNAL listener to be recorded", blockedPercentMeters("EXTERNAL").count() > 0)
  }

  @Test
  def testMaxBrokerConnectionRateReconfiguration(): Unit = {
    val config = KafkaConfig.fromProps(brokerPropsWithDefaultConnectionLimits)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)
    connectionQuotas.addListener(config, listeners("EXTERNAL").listenerName)

    addListenersAndVerify(config, connectionQuotas)

    val maxBrokerConnectionRate = 50
    connectionQuotas.updateBrokerMaxConnectionRate(maxBrokerConnectionRate)

    // create connections with rate = 200 conn/sec (5ms interval), so that connection rate gets throttled
    val totalConnections = 400
    executor.submit((() =>
      // this is a short run, so setting epsilon higher (enough to check that the rate is not unlimited)
      acceptConnectionsAndVerifyRate(connectionQuotas, listeners("EXTERNAL"), totalConnections, 5, maxBrokerConnectionRate, 20)): Runnable
    ).get(10, TimeUnit.SECONDS)
    assertTrue("Expected BlockedPercentMeter metric for EXTERNAL listener to be recorded", blockedPercentMeters("EXTERNAL").count() > 0)
  }

  @Test
  def testIpConnectionRateMetricUpdate(): Unit = {
    val config = KafkaConfig.fromProps(brokerPropsWithDefaultConnectionLimits)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)
    connectionQuotas.addListener(config, listeners("EXTERNAL").listenerName)
    connectionQuotas.addListener(config, listeners("ADMIN").listenerName)
    val defaultIpRate = 50
    val defaultOverrideRate = 20
    val overrideIpRate = 30
    val externalListener = listeners("EXTERNAL")
    val adminListener = listeners("ADMIN")
    // set a non-unlimited default quota so that we create ip rate sensors/metrics
    connectionQuotas.updateIpConnectionRateQuota(None, Some(defaultIpRate))
    connectionQuotas.inc(externalListener.listenerName, externalListener.defaultIp, blockedPercentMeters("EXTERNAL"))
    connectionQuotas.inc(adminListener.listenerName, adminListener.defaultIp, blockedPercentMeters("ADMIN"))

    // both IPs should have the default rate
    verifyIpConnectionQuota(externalListener.defaultIp, defaultIpRate)
    verifyIpConnectionQuota(adminListener.defaultIp, defaultIpRate)

    // external listener should have its in-memory quota and metric config updated
    connectionQuotas.updateIpConnectionRateQuota(Some(externalListener.defaultIp), Some(overrideIpRate))
    verifyIpConnectionQuota(externalListener.defaultIp, overrideIpRate)

    // update default
    connectionQuotas.updateIpConnectionRateQuota(None, Some(defaultOverrideRate))

    // external listener IP should not have its quota updated to the new default
    verifyIpConnectionQuota(externalListener.defaultIp, overrideIpRate)
    // admin listener IP should have its quota updated with to the new default
    verifyIpConnectionQuota(adminListener.defaultIp, defaultOverrideRate)

    // remove default connection rate quota
    connectionQuotas.updateIpConnectionRateQuota(None, None)
    verifyIpConnectionQuota(adminListener.defaultIp, DynamicConfig.Ip.DefaultConnectionCreationRate)
    verifyIpConnectionQuota(externalListener.defaultIp, overrideIpRate)

    // remove override for external listener IP
    connectionQuotas.updateIpConnectionRateQuota(Some(externalListener.defaultIp), None)
    verifyIpConnectionQuota(externalListener.defaultIp, DynamicConfig.Ip.DefaultConnectionCreationRate)
  }

  @Test
  def testEnforcedIpConnectionRateQuotaUpdate(): Unit = {
    val ipConnectionRateLimit = 20
    val props = brokerPropsWithDefaultConnectionLimits
    val config = KafkaConfig.fromProps(props)
    // use MockTime for IP connection rate quota tests that don't expect to block
    setupMockTime()
    connectionQuotas = new ConnectionQuotas(config, time, metrics)
    addListenersAndVerify(config, connectionQuotas)
    val externalListener = listeners("EXTERNAL")
    connectionQuotas.updateIpConnectionRateQuota(Some(externalListener.defaultIp), Some(ipConnectionRateLimit))
    // create connections with the rate > ip quota
    val connectionRate = 40
    assertThrows[ConnectionThrottledException] {
      acceptConnections(connectionQuotas, externalListener, connectionRate)
    }
    assertEquals(s"Number of connections on $externalListener:",
      ipConnectionRateLimit, connectionQuotas.get(externalListener.defaultIp))

    // increase ip quota, we should accept connections up to the new quota limit
    val updatedRateLimit = 30
    connectionQuotas.updateIpConnectionRateQuota(Some(externalListener.defaultIp), Some(updatedRateLimit))
    assertThrows[ConnectionThrottledException] {
      acceptConnections(connectionQuotas, externalListener, connectionRate)
    }
    assertEquals(s"Number of connections on $externalListener:",
      updatedRateLimit, connectionQuotas.get(externalListener.defaultIp))

    // remove IP quota, all connections should get accepted
    connectionQuotas.updateIpConnectionRateQuota(Some(externalListener.defaultIp), None)
    acceptConnections(connectionQuotas, externalListener, connectionRate)
    assertEquals(s"Number of connections on $externalListener:",
      connectionRate + updatedRateLimit, connectionQuotas.get(externalListener.defaultIp))

    // create connections on a different IP,
    val adminListener = listeners("ADMIN")
    acceptConnections(connectionQuotas, adminListener, connectionRate)
    assertEquals(s"Number of connections on $adminListener:",
      connectionRate, connectionQuotas.get(adminListener.defaultIp))

    // set a default IP quota, verify that quota gets propagated
    connectionQuotas.updateIpConnectionRateQuota(None, Some(ipConnectionRateLimit))
    assertThrows[ConnectionThrottledException] {
      acceptConnections(connectionQuotas, adminListener, connectionRate)
    }
    assertEquals(s"Number of connections on $adminListener:",
      connectionRate + ipConnectionRateLimit, connectionQuotas.get(adminListener.defaultIp))

    // acceptor shouldn't block for IP rate throttling
    verifyNoBlockedPercentRecordedOnAllListeners()
  }

  @Test
  def testNonDefaultConnectionCountLimitAndRateLimit(): Unit = {
    val brokerRateLimit = 25
    val maxConnections = 350  // with rate == 25, will run out of connections in 14 seconds
    val props = brokerPropsWithDefaultConnectionLimits
    props.put(KafkaConfig.MaxConnectionsProp, maxConnections.toString)
    props.put(KafkaConfig.MaxConnectionCreationRateProp, brokerRateLimit.toString)
    val config = KafkaConfig.fromProps(props)
    connectionQuotas = new ConnectionQuotas(config, time, metrics)
    connectionQuotas.addListener(config, listeners("EXTERNAL").listenerName)

    addListenersAndVerify(config, connectionQuotas)

    // create connections with rate = 100 conn/sec (10ms interval), so that connection rate gets throttled
    val listener = listeners("EXTERNAL")
    executor.submit((() =>
      acceptConnectionsAndVerifyRate(connectionQuotas, listener, maxConnections, 10, brokerRateLimit, 8)): Runnable
    ).get(20, TimeUnit.SECONDS)
    assertTrue("Expected BlockedPercentMeter metric for EXTERNAL listener to be recorded", blockedPercentMeters("EXTERNAL").count() > 0)
    assertEquals(s"Number of connections on EXTERNAL listener:", maxConnections, connectionQuotas.get(listener.defaultIp))

    // adding one more connection will block ConnectionQuota.inc()
    val future = executor.submit((() =>
      acceptConnections(connectionQuotas, listeners("EXTERNAL"), 1)): Runnable
    )
    intercept[TimeoutException](future.get(100, TimeUnit.MILLISECONDS))

    // removing one connection should make the waiting connection to succeed
    connectionQuotas.dec(listener.listenerName, listener.defaultIp)
    future.get(1, TimeUnit.SECONDS)
    assertEquals(s"Number of connections on EXTERNAL listener:", maxConnections, connectionQuotas.get(listener.defaultIp))
  }

  private def addListenersAndVerify(config: KafkaConfig, connectionQuotas: ConnectionQuotas) : Unit = {
    addListenersAndVerify(config, Map.empty.asJava, connectionQuotas)
  }

  private def addListenersAndVerify(config: KafkaConfig,
                                    listenerConfig: util.Map[String, _],
                                    connectionQuotas: ConnectionQuotas) : Unit = {
    assertNotNull("Expected broker-connection-accept-rate metric to exist", brokerConnRateMetric())

    // add listeners and verify connection limits not exceeded
    listeners.foreach { case (name, listener) =>
      val listenerName = listener.listenerName
      connectionQuotas.addListener(config, listenerName)
      connectionQuotas.maxConnectionsPerListener(listenerName).configure(listenerConfig)
      assertFalse(s"Should not exceed max connection limit on $name listener after initialization",
        connectionQuotas.maxConnectionsExceeded(listenerName))
      assertEquals(s"Number of connections on $listener listener:",
        0, connectionQuotas.get(listener.defaultIp))
      assertNotNull(s"Expected connection-accept-rate metric to exist for listener ${listenerName.value}",
        listenerConnRateMetric(listenerName.value))
      assertEquals(s"Connection acceptance rate metric for listener ${listenerName.value}",
          0, metricValue(listenerConnRateMetric(listenerName.value)), eps)
      assertNotNull(s"Expected connection-accept-throttle-time metric to exist for listener ${listenerName.value}",
        listenerConnThrottleMetric(listenerName.value))
      assertEquals(s"Listener connection throttle metric for listener ${listenerName.value}",
        0, metricValue(listenerConnThrottleMetric(listenerName.value)).toLong)
      assertEquals(s"Ip connection throttle metric for listener ${listenerName.value}",
        0, metricValue(ipConnThrottleMetric(listenerName.value)).toLong)
    }
    verifyNoBlockedPercentRecordedOnAllListeners()
    assertEquals("Broker-wide connection acceptance rate metric", 0, metricValue(brokerConnRateMetric()), eps)
  }

  private def verifyNoBlockedPercentRecordedOnAllListeners(): Unit = {
    blockedPercentMeters.foreach { case (name, meter) =>
      assertEquals(s"BlockedPercentMeter metric for $name listener", 0, meter.count())
    }
  }

  private def verifyNonZeroBlockedPercentAndThrottleTimeOnAllListeners(): Unit = {
    blockedPercentMeters.foreach { case (name, meter) =>
      assertTrue(s"Expected BlockedPercentMeter metric for $name listener to be recorded", meter.count() > 0)
    }
    listeners.values.foreach { listener =>
      assertTrue(s"Connection throttle metric for listener ${listener.listenerName.value}",
        metricValue(listenerConnThrottleMetric(listener.listenerName.value)).toLong > 0)
    }
  }

  private def verifyIpThrottleTimeOnListener(listener: ListenerName, expectThrottle: Boolean): Unit = {
    assertEquals(s"IP connection throttle recorded for listener ${listener.value}", expectThrottle,
      metricValue(ipConnThrottleMetric(listener.value)).toLong > 0)
  }

  private def verifyOnlyNonInterBrokerListenersBlockedPercentRecorded(): Unit = {
    blockedPercentMeters.foreach { case (name, meter) =>
      name match {
        case "REPLICATION" =>
          assertEquals(s"BlockedPercentMeter metric for $name listener", 0, meter.count())
        case _ =>
          assertTrue(s"Expected BlockedPercentMeter metric for $name listener to be recorded", meter.count() > 0)
      }
    }
  }

  private def verifyConnectionCountOnEveryListener(connectionQuotas: ConnectionQuotas, expectedConnectionCount: Int): Unit = {
    listeners.values.foreach { listener =>
      assertEquals(s"Number of connections on $listener:",
        expectedConnectionCount, connectionQuotas.get(listener.defaultIp))
    }
  }

  private def listenerConnThrottleMetric(listener: String) : KafkaMetric = {
    val metricName = metrics.metricName(
      "connection-accept-throttle-time",
      SocketServer.MetricsGroup,
      Collections.singletonMap(Processor.ListenerMetricTag, listener))
    metrics.metric(metricName)
  }

  private def ipConnThrottleMetric(listener: String): KafkaMetric = {
    val metricName = metrics.metricName(
      "ip-connection-accept-throttle-time",
      SocketServer.MetricsGroup,
      Collections.singletonMap(Processor.ListenerMetricTag, listener))
    metrics.metric(metricName)
  }

  private def listenerConnRateMetric(listener: String) : KafkaMetric = {
    val metricName = metrics.metricName(
      "connection-accept-rate",
      SocketServer.MetricsGroup,
      Collections.singletonMap(Processor.ListenerMetricTag, listener))
    metrics.metric(metricName)
  }

  private def brokerConnRateMetric() : KafkaMetric = {
    val metricName = metrics.metricName(
      s"broker-connection-accept-rate",
      SocketServer.MetricsGroup)
    metrics.metric(metricName)
  }

  private def ipConnRateMetric(ip: String): KafkaMetric = {
    val metricName = metrics.metricName(
      s"connection-accept-rate",
      SocketServer.MetricsGroup,
      Collections.singletonMap("ip", ip))
    metrics.metric(metricName)
  }

  private def metricValue(metric: KafkaMetric): Double = {
    metric.metricValue.asInstanceOf[Double]
  }

  private def verifyIpConnectionQuota(ip: InetAddress, quota: Int): Unit = {
    // verify connection quota in-memory rate and metric
    assertEquals(quota, connectionQuotas.connectionRateForIp(ip))
    Option(ipConnRateMetric(ip.getHostAddress)) match {
      case Some(metric) => assertEquals(quota, metric.config.quota.bound, 0.1)
      case None => fail(s"Expected $ip connection rate metric to be defined")
    }
  }

  // this method must be called on a separate thread, because connectionQuotas.inc() may block
  private def acceptConnections(connectionQuotas: ConnectionQuotas,
                                listenerDesc: ListenerDesc,
                                numConnections: Long,
                                timeIntervalMs: Long = 0L,
                                expectIpThrottle: Boolean = false) : Unit = {
    acceptConnections(connectionQuotas, listenerDesc.listenerName, listenerDesc.defaultIp, numConnections,
      timeIntervalMs, expectIpThrottle)
  }

  // this method must be called on a separate thread, because connectionQuotas.inc() may block
  private def acceptConnectionsAndVerifyRate(connectionQuotas: ConnectionQuotas,
                                             listenerDesc: ListenerDesc,
                                             numConnections: Long,
                                             timeIntervalMs: Long,
                                             expectedRate: Int,
                                             epsilon: Int,
                                             expectIpThrottle: Boolean = false) : Unit = {
    val startTimeMs = time.milliseconds
    val startNumConnections = connectionQuotas.get(listenerDesc.defaultIp)
    acceptConnections(connectionQuotas, listenerDesc.listenerName, listenerDesc.defaultIp, numConnections,
      timeIntervalMs, expectIpThrottle)
    val elapsedSeconds = MetricsUtils.convert(time.milliseconds - startTimeMs, TimeUnit.SECONDS)
    val createdConnections = connectionQuotas.get(listenerDesc.defaultIp) - startNumConnections
    val actualRate = createdConnections.toDouble / elapsedSeconds
    assertEquals(s"Expected rate ($expectedRate +- $epsilon), but got $actualRate ($createdConnections connections / $elapsedSeconds sec)",
      expectedRate.toDouble, actualRate, epsilon)
  }

  /**
   * This method will "create" connections every 'timeIntervalMs' which translates to 1000/timeIntervalMs connection rate,
   * as long as the rate is below the connection rate limit. Otherwise, connections will be essentially created as
   * fast as possible, which would result in the maximum connection creation rate.
   *
   * This method must be called on a separate thread, because connectionQuotas.inc() may block
   */
  private def acceptConnections(connectionQuotas: ConnectionQuotas,
                                listenerName: ListenerName,
                                address: InetAddress,
                                numConnections: Long,
                                timeIntervalMs: Long,
                                expectIpThrottle: Boolean): Boolean = {
    var nextSendTime = time.milliseconds + timeIntervalMs
    var ipThrottled = false
    for (_ <- 0L until numConnections) {
      // this method may block if broker-wide or listener limit on the number of connections is reached
      try {
        connectionQuotas.inc(listenerName, address, blockedPercentMeters(listenerName.value))
      } catch {
        case e: ConnectionThrottledException =>
          if (!expectIpThrottle)
            throw e
          ipThrottled = true
      }
      val sleepMs = math.max(nextSendTime - time.milliseconds, 0)
      if (sleepMs > 0)
        time.sleep(sleepMs)

      nextSendTime = nextSendTime + timeIntervalMs
    }
    ipThrottled
  }

  // this method must be called on a separate thread, because connectionQuotas.inc() may block
  private def acceptConnectionsAboveIpLimit(connectionQuotas: ConnectionQuotas,
                                            listenerDesc: ListenerDesc,
                                            numConnections: Long) : Unit = {
    val listenerName = listenerDesc.listenerName
    for (i <- 0L until numConnections) {
      // this method may block if broker-wide or listener limit is reached
      intercept[TooManyConnectionsException](
        connectionQuotas.inc(listenerName, listenerDesc.defaultIp, blockedPercentMeters(listenerName.value))
      )
    }
  }
}
