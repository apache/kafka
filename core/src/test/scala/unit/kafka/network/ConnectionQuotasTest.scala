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
import java.util.concurrent.{Executors, TimeUnit}
import java.util.Properties

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.network.Processor.ListenerMetricTag
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.network._
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert._
import org.junit._
import org.scalatest.Assertions.intercept

import scala.jdk.CollectionConverters._
import scala.collection.{Map, mutable}
import scala.concurrent.TimeoutException

class ConnectionQuotasTest {
  private val time = new MockTime
  private val listeners = Map(
    "EXTERNAL" -> ListenerDesc(new ListenerName("EXTERNAL"), InetAddress.getByName("192.168.1.1")),
    "ADMIN" -> ListenerDesc(new ListenerName("ADMIN"), InetAddress.getByName("192.168.1.2")),
    "REPLICATION" -> ListenerDesc(new ListenerName("REPLICATION"), InetAddress.getByName("192.168.1.3")))
  private val blockedPercentMeters = mutable.Map[String, Meter]()
  private val knownHost = InetAddress.getByName("192.168.10.0")
  private val unknownHost = InetAddress.getByName("192.168.2.0")

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
  }

  @After
  def tearDown(): Unit = {
    TestUtils.clearYammerMetrics()
    blockedPercentMeters.clear()
  }

  @Test
  def testFailWhenNoListeners(): Unit = {
    val config = KafkaConfig.fromProps(brokerPropsWithDefaultConnectionLimits)
    val connectionQuotas = new ConnectionQuotas(config, time)

    val executor = Executors.newSingleThreadExecutor
    try {
      // inc() on a separate thread in case it blocks
      val listener = listeners("EXTERNAL")
      executor.submit((() =>
        intercept[RuntimeException](
          connectionQuotas.inc(listener.listenerName, listener.defaultIp, blockedPercentMeters("EXTERNAL"))
        )): Runnable
      ).get(5, TimeUnit.SECONDS)
    } finally {
      executor.shutdownNow()
    }
  }

  @Test
  def testFailDecrementForUnknownIp(): Unit = {
    val config = KafkaConfig.fromProps(brokerPropsWithDefaultConnectionLimits)
    val connectionQuotas = new ConnectionQuotas(config, time)
    addListenersAndVerify(config, connectionQuotas)

    // calling dec() for an IP for which we didn't call inc() should throw an exception
    intercept[IllegalArgumentException](connectionQuotas.dec(listeners("EXTERNAL").listenerName, unknownHost))
  }

  @Test
  def testNoConnectionLimitsByDefault(): Unit = {
    val config = KafkaConfig.fromProps(brokerPropsWithDefaultConnectionLimits)
    val connectionQuotas = new ConnectionQuotas(config, time)
    addListenersAndVerify(config, connectionQuotas)

    val executor = Executors.newFixedThreadPool(listeners.size)
    try {
      // verify there is no limit by accepting 10000 connections as fast as possible
      val numConnections = 10000
      val futures = listeners.values.map { listener =>
        executor.submit((() => acceptConnections(connectionQuotas, listener, numConnections)): Runnable)
      }
      futures.foreach(_.get(10, TimeUnit.SECONDS))
      listeners.values.foreach { listener =>
        assertEquals(s"Number of connections on $listener:",
          numConnections, connectionQuotas.get(listener.defaultIp))

        // verify removing one connection
        connectionQuotas.dec(listener.listenerName, listener.defaultIp)
        assertEquals(s"Number of connections on $listener:",
          numConnections - 1, connectionQuotas.get(listener.defaultIp))
      }
    } finally {
      executor.shutdownNow()
    }
  }

  @Test
  def testMaxConnectionsPerIp(): Unit = {
    val maxConnectionsPerIp = 17
    val props = brokerPropsWithDefaultConnectionLimits
    props.put(KafkaConfig.MaxConnectionsPerIpProp, maxConnectionsPerIp.toString)
    val config = KafkaConfig.fromProps(props)
    val connectionQuotas = new ConnectionQuotas(config, time)

    addListenersAndVerify(config, connectionQuotas)

    val executor = Executors.newFixedThreadPool(listeners.size)
    try {
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
    } finally {
      executor.shutdownNow()
    }
  }

  @Test
  def testMaxBrokerWideConnectionLimit(): Unit = {
    val maxConnections = 800
    val props = brokerPropsWithDefaultConnectionLimits
    props.put(KafkaConfig.MaxConnectionsProp, maxConnections.toString)
    val config = KafkaConfig.fromProps(props)
    val connectionQuotas = new ConnectionQuotas(config, time)

    addListenersAndVerify(config, connectionQuotas)

    val executor = Executors.newFixedThreadPool(listeners.size)
    try {
      // the metric should be 0, because we did not advance the time yet
      assertEquals(0, blockedPercentMeters("EXTERNAL").count())

      // verify that ConnectionQuota can give all connections to one listener
      // also add connections with a time interval between connections to make sure that time gets advanced
      executor.submit((() =>
        acceptConnections(connectionQuotas, listeners("EXTERNAL"), maxConnections, 1)): Runnable
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
      // advance time by 3ms so that when the waiting connection gets accepted, the blocked percent is non-zero
      time.sleep(3)

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
    } finally {
      executor.shutdownNow()
    }
  }

  @Test
  def testMaxListenerConnectionLimits(): Unit = {
    val maxConnections = 800
    // sum of per-listener connection limits is below total connection limit
    val listenerMaxConnections = 200
    val props = brokerPropsWithDefaultConnectionLimits
    props.put(KafkaConfig.MaxConnectionsProp, maxConnections.toString)
    val config = KafkaConfig.fromProps(props)
    val connectionQuotas = new ConnectionQuotas(config, time)

    addListenersAndVerify(config, connectionQuotas)

    val listenerConfig = Map(KafkaConfig.MaxConnectionsProp -> listenerMaxConnections.toString).asJava
    listeners.values.foreach { listener =>
      connectionQuotas.maxConnectionsPerListener(listener.listenerName).configure(listenerConfig)
    }

    val executor = Executors.newFixedThreadPool(listeners.size)
    try {
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
      listeners.values.foreach { listener =>
        assertEquals(s"Number of connections on $listener:",
          listenerMaxConnections, connectionQuotas.get(listener.defaultIp))
      }
    } finally {
      executor.shutdownNow()
    }
  }

  private def addListenersAndVerify(config: KafkaConfig, connectionQuotas: ConnectionQuotas) : Unit = {
    // add listeners and verify connection limits not exceeded
    listeners.foreach { case (name, listener) =>
      connectionQuotas.addListener(config, listener.listenerName)
      assertFalse(s"Should not exceed max connection limit on $name listener after initialization",
        connectionQuotas.maxConnectionsExceeded(listener.listenerName))
    }
  }

  // this method must be called on a separate thread, because connectionQuotas.inc() may block
  private def acceptConnections(connectionQuotas: ConnectionQuotas,
                                listenerDesc: ListenerDesc,
                                numConnections: Long,
                                timeIntervalMs: Long = 0L) : Unit = {
    acceptConnections(connectionQuotas, listenerDesc.listenerName, listenerDesc.defaultIp, numConnections, timeIntervalMs)
  }

  // this method must be called on a separate thread, because connectionQuotas.inc() may block
  private def acceptConnections(connectionQuotas: ConnectionQuotas,
                                listenerName: ListenerName,
                                address: InetAddress,
                                numConnections: Long,
                                timeIntervalMs: Long) : Unit = {
    for (_ <- 0L until numConnections) {
      // this method may block if broker-wide or listener limit is reached
      connectionQuotas.inc(listenerName, address, blockedPercentMeters(listenerName.value))
      time.sleep(timeIntervalMs)
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
