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

import kafka.utils.{CoreUtils, Logging, TestUtils, ZkUtils}
import org.junit.{After, AfterClass, Before, BeforeClass}
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.test.IntegrationTest
import org.junit.experimental.categories.Category

import scala.collection.Set
import scala.collection.JavaConverters._
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.consumer.internals.AbstractCoordinator
import kafka.controller.ControllerEventManager

@Category(Array(classOf[IntegrationTest]))
abstract class ZooKeeperTestHarness extends JUnitSuite with Logging {

  val zkConnectionTimeout = 10000
  val zkSessionTimeout = 6000
  protected val zkAclsEnabled: Option[Boolean] = None

  var zkUtils: ZkUtils = null
  var zookeeper: EmbeddedZookeeper = null

  def zkPort: Int = zookeeper.port
  def zkConnect: String = s"127.0.0.1:$zkPort"
  
  @Before
  def setUp() {
    zookeeper = new EmbeddedZookeeper()
    zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkAclsEnabled.getOrElse(JaasUtils.isZkSecurityEnabled()))
  }

  @After
  def tearDown() {
    if (zkUtils != null)
     CoreUtils.swallow(zkUtils.close())
    if (zookeeper != null)
      CoreUtils.swallow(zookeeper.shutdown())
    Configuration.setConfiguration(null)
  }
}

object ZooKeeperTestHarness {
  val ZkClientEventThreadPrefix = "ZkClient-EventThread"

  // Threads which may cause transient failures in subsequent tests if not shutdown.
  // These include threads which make connections to brokers and may cause issues
  // when broker ports are reused (e.g. auto-create topics) as well as threads
  // which reset static JAAS configuration.
  val unexpectedThreadNames = Set(ControllerEventManager.ControllerEventThreadName,
                                  KafkaProducer.NETWORK_THREAD_PREFIX,
                                  AbstractCoordinator.HEARTBEAT_THREAD_PREFIX,
                                  ZkClientEventThreadPrefix)

  /**
   * Verify that a previous test that doesn't use ZooKeeperTestHarness hasn't left behind an unexpected thread.
   * This assumes that brokers, ZooKeeper clients, producers and consumers are not created in another @BeforeClass,
   * which is true for core tests where this harness is used.
   */
  @BeforeClass
  def setUpClass() {
    verifyNoUnexpectedThreads()
  }

  /**
   * Verify that tests from the current test class using ZooKeeperTestHarness haven't left behind an unexpected thread
   */
  @AfterClass
  def tearDownClass() {
    verifyNoUnexpectedThreads()
  }

  /**
   * Verifies that threads which are known to cause transient failures in subsequent tests
   * have been shutdown.
   */
  def verifyNoUnexpectedThreads() {
    def allThreads = Thread.getAllStackTraces.keySet.asScala.map(thread => thread.getName)
    val (threads, noUnexpected) = TestUtils.computeUntilTrue(allThreads) { threads =>
      threads.forall(t => unexpectedThreadNames.forall(s => !t.contains(s)))
    }
    assertTrue(s"Found unexpected threads, allThreads=$threads", noUnexpected)
  }
}
