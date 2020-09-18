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
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.util.{Collections, Properties}

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.network.Processor.ListenerMetricTag
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.metrics.{KafkaMetric, MetricConfig, Metrics}
import org.apache.kafka.common.network._
import org.apache.kafka.common.utils.{Time, Utils}
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

  private val listeners = Map(
    "EXTERNAL" -> ListenerDesc(new ListenerName("EXTERNAL"), InetAddress.getByName("192.168.1.1")),
    "ADMIN" -> ListenerDesc(new ListenerName("ADMIN"), InetAddress.getByName("192.168.1.2")),
    "REPLICATION" -> ListenerDesc(new ListenerName("REPLICATION"), InetAddress.getByName("192.168.1.3")))
  private val blockedPercentMeters = mutable.Map[String, Meter]()
  private val knownHost = InetAddress.getByName("192.168.10.0")
  private val unknownHost = InetAddress.getByName("192.168.2.0")

  private val numQuotaSamples = 2
  private val quotaWindowSizeSeconds = 1

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
    metrics = new Metrics(new MetricConfig(), Collections.emptyList(), Time.SYSTEM)
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
    connectionQuotas = new ConnectionQuotas(config, Time.SYSTEM, metrics)

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
    connectionQuotas = new ConnectionQuotas(config, Time.SYSTEM, metrics)
    addListenersAndVerify(config, connectionQuotas)

    // calling dec() for an IP for which we didn't call inc() should throw an exception
    intercept[IllegalArgumentException](connectionQuotas.dec(listeners("EXTERNAL").listenerName, unknownHost))
  }

  @Test
  def testNoConnectionLimitsByDefault(): Unit = {
    val config = KafkaConfig.fromProps(brokerPropsWithDefaultConnectionLimits)
    connectionQuotas = new ConnectionQuotas(config, Time.SYSTEM, metrics)
    addListenersAndVerify(config, connectionQuotas)

    // verify there is no limit by accepting 10000 connections as fast as possible
    val numConnections = 10000
    val futures = listeners.values.map { listener =>
      executor.submit((() => acceptConnections(connectionQuotas, listener, numConnections)): Runnable)
    }
    futures.foreach(_.get(10, TimeUnit.SECONDS))
    assertTrue("Expected broker-connection-accept-rate metric to get recorded",
      brokerConnRateMetric().metricValue.asInstanceOf[Double].toLong > 0)
    listeners.values.foreach { listener =>
      assertEquals(s"Number of connections on $listener:",
        numConnections, connectionQuotas.get(listener.defaultIp))

      assertTrue(s"Expected connection-accept-rate metric to get recorded for listener $listener",
        listenerConnRateMetric(listener.listenerName.value).metricValue.asInstanceOf[Double].toLong > 0)

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
    connectionQuotas = new ConnectionQuotas(config, Time.SYSTEM, metrics)

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
      acceptConnections(connectionQuotas, externalListener.listenerName, knownHost, maxConnectionsPerIp, 0)): Runnable
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
    connectionQuotas = new ConnectionQuotas(config, Time.SYSTEM, metrics)

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
    connectionQuotas = new ConnectionQuotas(config, Time.SYSTEM, metrics)

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
    connectionQuotas = new ConnectionQuotas(config, Time.SYSTEM, metrics)

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
    connectionQuotas = new ConnectionQuotas(config, Time.SYSTEM, metrics)

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
    connectionQuotas = new ConnectionQuotas(config, Time.SYSTEM, metrics)

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
    connectionQuotas = new ConnectionQuotas(config, Time.SYSTEM, metrics)

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
    verifyNonZeroBlockedPercentRecordedOnAllListeners()

    // while the connection creation rate was throttled,
    // expect all connections got created (not limit on the number of connections)
    verifyConnectionCountOnEveryListener(connectionQuotas, connectionsPerListener)
  }

  @Test
  def testMaxListenerConnectionListenerMustBeAboveZero(): Unit = {
    val config = KafkaConfig.fromProps(brokerPropsWithDefaultConnectionLimits)
    connectionQuotas = new ConnectionQuotas(config, Time.SYSTEM, metrics)

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
    connectionQuotas = new ConnectionQuotas(config, Time.SYSTEM, metrics)
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
    connectionQuotas = new ConnectionQuotas(config, Time.SYSTEM, metrics)
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
  def testNonDefaultConnectionCountLimitAndRateLimit(): Unit = {
    val brokerRateLimit = 25
    val maxConnections = 350  // with rate == 25, will run out of connections in 14 seconds
    val props = brokerPropsWithDefaultConnectionLimits
    props.put(KafkaConfig.MaxConnectionsProp, maxConnections.toString)
    props.put(KafkaConfig.MaxConnectionCreationRateProp, brokerRateLimit.toString)
    val config = KafkaConfig.fromProps(props)
    connectionQuotas = new ConnectionQuotas(config, Time.SYSTEM, metrics)
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
          0, listenerConnRateMetric(listenerName.value).metricValue.asInstanceOf[Double].toLong)
    }
    verifyNoBlockedPercentRecordedOnAllListeners()
    assertEquals("Broker-wide connection acceptance rate metric",
      0, brokerConnRateMetric().metricValue.asInstanceOf[Double].toLong)
  }

  private def verifyNoBlockedPercentRecordedOnAllListeners(): Unit = {
    blockedPercentMeters.foreach { case (name, meter) =>
      assertEquals(s"BlockedPercentMeter metric for $name listener", 0, meter.count())
    }
  }

  private def verifyNonZeroBlockedPercentRecordedOnAllListeners(): Unit = {
    blockedPercentMeters.foreach { case (name, meter) =>
      assertTrue(s"Expected BlockedPercentMeter metric for $name listener to be recorded", meter.count() > 0)
    }
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

  private def listenerConnRateMetric(listener: String) : KafkaMetric = {
    val metricName = metrics.metricName(
      s"connection-accept-rate",
      "socket-server-metrics",
      Collections.singletonMap("listener", listener))
    metrics.metric(metricName)
  }

  private def brokerConnRateMetric() : KafkaMetric = {
    val metricName = metrics.metricName(
      s"broker-connection-accept-rate",
      "socket-server-metrics")
    metrics.metric(metricName)
  }

  // this method must be called on a separate thread, because connectionQuotas.inc() may block
  private def acceptConnections(connectionQuotas: ConnectionQuotas,
                                listenerDesc: ListenerDesc,
                                numConnections: Long,
                                timeIntervalMs: Long = 0L) : Unit = {
    acceptConnections(connectionQuotas, listenerDesc.listenerName, listenerDesc.defaultIp, numConnections, timeIntervalMs)
  }

  // this method must be called on a separate thread, because connectionQuotas.inc() may block
  private def acceptConnectionsAndVerifyRate(connectionQuotas: ConnectionQuotas,
                                             listenerDesc: ListenerDesc,
                                             numConnections: Long,
                                             timeIntervalMs: Long,
                                             expectedRate: Int,
                                             epsilon: Int) : Unit = {
    val startTimeMs = System.currentTimeMillis
    acceptConnections(connectionQuotas, listenerDesc.listenerName, listenerDesc.defaultIp, numConnections, timeIntervalMs)
    val elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis - startTimeMs)
    val actualRate = (numConnections.toDouble / elapsedSeconds).toInt
    assertTrue(s"Expected rate ($expectedRate +- $epsilon), but got $actualRate ($numConnections connections / $elapsedSeconds sec)",
      actualRate <= expectedRate + epsilon && actualRate >= expectedRate - epsilon)
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
                                timeIntervalMs: Long) : Unit = {
    var nextSendTime = System.currentTimeMillis + timeIntervalMs
    for (_ <- 0L until numConnections) {
      // this method may block if broker-wide or listener limit on the number of connections is reached
      connectionQuotas.inc(listenerName, address, blockedPercentMeters(listenerName.value))

      val sleepMs = math.max(nextSendTime - System.currentTimeMillis, 0)
      if (sleepMs > 0)
        Utils.sleep(sleepMs)

      nextSendTime = nextSendTime + timeIntervalMs
    }
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
