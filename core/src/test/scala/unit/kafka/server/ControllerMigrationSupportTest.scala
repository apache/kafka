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

package kafka.server

import kafka.common.{NotificationHandler, ZkNodeChangeNotificationListener}
import kafka.utils.TestUtils
import kafka.zk.{ConfigEntityChangeNotificationZNode, ConfigEntityZNode, ZkAclChangeStore}
import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType.TOPIC
import org.apache.kafka.server.config.ConfigType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.Seq

class ControllerMigrationSupportTest extends QuorumTestHarness {

  private var notificationHandler: TestNotificationHandler = _
  private var cleaners: Seq[ZkNodeChangeNotificationListener] = Seq.empty

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    zkClient.createAclPaths()
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)
    notificationHandler = new TestNotificationHandler()
  }

  @AfterEach
  override def tearDown(): Unit = {
    cleaners.foreach(_.close())
    super.tearDown()
  }

  @Test
  def testNotificationCleanersProcessConfigAndAclChanges(): Unit = {
    cleaners = ControllerMigrationSupport.notificationCleaners(zkClient, notificationHandler)

    // 1 + the number of ACL change stores, since there's one for config changes too
    assertEquals(1 + ZkAclChangeStore.stores.size, cleaners.size)

    cleaners.foreach(_.init())
    zkClient.createConfigChangeNotification(ConfigEntityZNode.path(ConfigType.TOPIC, "test-topic"))
    zkClient.createAclChangeNotification(new ResourcePattern(TOPIC, "test-topic", LITERAL))
    zkClient.createAclChangeNotification(new ResourcePattern(TOPIC, "test-topic", PREFIXED))

    TestUtils.waitUntilTrue(() => notificationHandler.receivedCount == 3,
      s"Expected 3 invocations of processNotification, but there were ${notificationHandler.receivedCount}")
  }

  private class TestNotificationHandler extends NotificationHandler {
    private val count = new AtomicInteger

    override def processNotification(notificationMessage: Array[Byte]): Unit =
      count.incrementAndGet()

    def receivedCount: Int = count.get()
  }
}
