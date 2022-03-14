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

package kafka.zk

import javax.security.auth.login.Configuration
import kafka.controller.ControllerEventManager
import kafka.utils.{CoreUtils, Logging, TestUtils}
import org.apache.kafka.clients.admin.AdminClientUnitTestEnv
import org.apache.kafka.clients.consumer.internals.AbstractCoordinator
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.client.ZKClientConfig
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import org.junit.jupiter.api.Assertions.{assertNull, assertTrue}
import org.junit.jupiter.api.{AfterAll, AfterEach, BeforeAll, BeforeEach, Tag}

import scala.collection.JavaConverters._
import scala.collection.Set
import scala.collection.mutable.{ArrayBuffer, Buffer}

@Tag("integration")
abstract class MultiClusterZooKeeperTestHarness extends Logging {
  protected def numClusters: Int = 1 // a.k.a. numZookeepers since each Kafka cluster requires its own ZK (or ZK-chroot)

  val zkConnectionTimeout = 10000
  val zkSessionTimeout = 15000 // Allows us to avoid ZK session expiration due to GC up to 2/3 * 15000ms = 10 secs
  val zkMaxInFlightRequests = Int.MaxValue

  protected def zkAclsEnabled: Option[Boolean] = None

  private val zkClients: Buffer[KafkaZkClient] = new ArrayBuffer(numClusters)
  private val adminZkClients: Buffer[AdminZkClient] = new ArrayBuffer(numClusters)
  private val zookeepers: Buffer[EmbeddedZookeeper] = new ArrayBuffer(numClusters)

  def zkClient(i: Int): KafkaZkClient = {
    zkClients(i)
  }

  def adminZkClient(i: Int): AdminZkClient = {
    adminZkClients(i)
  }

  def zookeeper(i: Int): EmbeddedZookeeper = {
    zookeepers(i)
  }

  def zkConnect(i: Int): String = {
    s"127.0.0.1:${zookeeper(i).port}"
  }

  @BeforeEach
  def setUp(): Unit = {
    (0 until numClusters).map { i =>
      debug(s"creating zookeeper/zkClient/adminZkClient " + (i+1) + " of " + numClusters)
      zookeepers += new EmbeddedZookeeper()
      zkClients += KafkaZkClient(zkConnect(i), zkAclsEnabled.getOrElse(JaasUtils.isZkSaslEnabled), zkSessionTimeout,
        zkConnectionTimeout, zkMaxInFlightRequests, Time.SYSTEM, name = MultiClusterZooKeeperTestHarness.getClass.getName, new ZKClientConfig)
      adminZkClients += new AdminZkClient(zkClients(i))
    }
  }

  @AfterEach
  def tearDown(): Unit = {
    for (zkc <- zkClients)
      zkc.close()
    for (zk <- zookeepers)
      CoreUtils.swallow(zk.shutdown(), this)
    Configuration.setConfiguration(null)
  }

  // Trigger session expiry by reusing the session id in another client
  def createZooKeeperClientToTriggerSessionExpiry(zooKeeper: ZooKeeper, i: Int): ZooKeeper = {
    val dummyWatcher = new Watcher {
      override def process(event: WatchedEvent): Unit = {}
    }
    val anotherZkClient = new ZooKeeper(zkConnect(i), 1000, dummyWatcher,
      zooKeeper.getSessionId,
      zooKeeper.getSessionPasswd)
    assertNull(anotherZkClient.exists("/nonexistent", false)) // Make sure new client works
    anotherZkClient
  }
}

object MultiClusterZooKeeperTestHarness {
  val ZkClientEventThreadSuffix = "-EventThread"

  // Threads which may cause transient failures in subsequent tests if not shutdown.
  // These include threads which make connections to brokers and may cause issues
  // when broker ports are reused (e.g. auto-create topics) as well as threads
  // which reset static JAAS configuration.
  val unexpectedThreadNames = Set(ControllerEventManager.ControllerEventThreadName,
                                  KafkaProducer.NETWORK_THREAD_PREFIX,
                                  AdminClientUnitTestEnv.kafkaAdminClientNetworkThreadPrefix(),
                                  AbstractCoordinator.HEARTBEAT_THREAD_PREFIX,
                                  ZkClientEventThreadSuffix)

  /**
   * Verify that a previous test that doesn't use MultiClusterZooKeeperTestHarness hasn't left behind an unexpected
   * thread. This assumes that brokers, ZooKeeper clients, producers and consumers are not created in another
   * {code @BeforeAll}, which is true for core tests where this harness is used.
   */
  @BeforeAll
  def setUpClass(): Unit = {
    verifyNoUnexpectedThreads("@BeforeClass")
  }

  /**
   * Verify that tests from the current test class using MultiClusterZooKeeperTestHarness haven't left behind an
   * unexpected thread.
   */
  @AfterAll
  def tearDownClass(): Unit = {
    verifyNoUnexpectedThreads("@AfterClass")
  }

  /**
   * Verifies that threads which are known to cause transient failures in subsequent tests
   * have been shutdown.
   */
  def verifyNoUnexpectedThreads(context: String): Unit = {
    def allThreads = Thread.getAllStackTraces.keySet.asScala.map(thread => thread.getName)
    val (threads, noUnexpected) = TestUtils.computeUntilTrue(allThreads) { threads =>
      threads.forall(t => unexpectedThreadNames.forall(s => !t.contains(s)))
    }
    assertTrue(
      noUnexpected,
      s"Found unexpected threads during $context, allThreads=$threads, " +
        s"unexpected=${threads.filterNot(t => unexpectedThreadNames.forall(s => !t.contains(s)))}"
    )
  }
}
