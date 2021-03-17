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
import kafka.utils.{CoreUtils, Logging, TestUtils}
import org.junit.jupiter.api.{AfterEach, AfterAll, BeforeEach, BeforeAll, Tag}
import org.junit.jupiter.api.Assertions._
import org.apache.kafka.common.security.JaasUtils

import scala.collection.Set
import scala.jdk.CollectionConverters._
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.consumer.internals.AbstractCoordinator
import kafka.controller.ControllerEventManager
import org.apache.kafka.clients.admin.AdminClientUnitTestEnv
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

@Tag("integration")
abstract class ZooKeeperTestHarness extends Logging {

  val zkConnectionTimeout = 10000
  val zkSessionTimeout = 15000 // Allows us to avoid ZK session expiration due to GC up to 2/3 * 15000ms = 10 secs
  val zkMaxInFlightRequests = Int.MaxValue

  protected def zkAclsEnabled: Option[Boolean] = None

  var zkClient: KafkaZkClient = null
  var adminZkClient: AdminZkClient = null

  var zookeeper: EmbeddedZookeeper = null

  def zkPort: Int = zookeeper.port
  def zkConnect: String = s"127.0.0.1:$zkPort"
  
  @BeforeEach
  def setUp(): Unit = {
    zookeeper = new EmbeddedZookeeper()
    zkClient = KafkaZkClient(zkConnect, zkAclsEnabled.getOrElse(JaasUtils.isZkSaslEnabled), zkSessionTimeout,
      zkConnectionTimeout, zkMaxInFlightRequests, Time.SYSTEM)
    adminZkClient = new AdminZkClient(zkClient)
  }

  @AfterEach
  def tearDown(): Unit = {
    if (zkClient != null)
     zkClient.close()
    if (zookeeper != null)
      CoreUtils.swallow(zookeeper.shutdown(), this)
    Configuration.setConfiguration(null)
  }

  // Trigger session expiry by reusing the session id in another client
  def createZooKeeperClientToTriggerSessionExpiry(zooKeeper: ZooKeeper): ZooKeeper = {
    val dummyWatcher = new Watcher {
      override def process(event: WatchedEvent): Unit = {}
    }
    val anotherZkClient = new ZooKeeper(zkConnect, 1000, dummyWatcher,
      zooKeeper.getSessionId,
      zooKeeper.getSessionPasswd)
    assertNull(anotherZkClient.exists("/nonexistent", false)) // Make sure new client works
    anotherZkClient
  }
}

object ZooKeeperTestHarness {
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
   * Verify that a previous test that doesn't use ZooKeeperTestHarness hasn't left behind an unexpected thread.
   * This assumes that brokers, ZooKeeper clients, producers and consumers are not created in another @BeforeClass,
   * which is true for core tests where this harness is used.
   */
  @BeforeAll
  def setUpClass(): Unit = {
    verifyNoUnexpectedThreads("@BeforeClass")
  }

  /**
   * Verify that tests from the current test class using ZooKeeperTestHarness haven't left behind an unexpected thread
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
    assertTrue(noUnexpected, s"Found unexpected threads during $context, allThreads=$threads, " +
      s"unexpected=${threads.filterNot(t => unexpectedThreadNames.forall(s => !t.contains(s)))}")
  }
}
