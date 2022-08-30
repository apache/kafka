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
package kafka.common

import kafka.utils.TestUtils
import kafka.zk.{LiteralAclChangeStore, LiteralAclStore, ZkAclChangeStore}
import kafka.server.QuorumTestHarness
import org.apache.kafka.common.resource.PatternType.LITERAL
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType.GROUP
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

import scala.collection.mutable.ArrayBuffer
import scala.collection.Seq

class ZkNodeChangeNotificationListenerTest extends QuorumTestHarness {

  private val changeExpirationMs = 1000
  private var notificationListener: ZkNodeChangeNotificationListener = _
  private var notificationHandler: TestNotificationHandler = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    zkClient.createAclPaths()
    notificationHandler = new TestNotificationHandler()
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (notificationListener != null) {
      notificationListener.close()
    }
    super.tearDown()
  }

  @Test
  def testProcessNotification(): Unit = {
    val notificationMessage1 = new ResourcePattern(GROUP, "messageA", LITERAL)
    val notificationMessage2 = new ResourcePattern(GROUP, "messageB", LITERAL)

    notificationListener = new ZkNodeChangeNotificationListener(zkClient, LiteralAclChangeStore.aclChangePath,
      ZkAclChangeStore.SequenceNumberPrefix, notificationHandler, changeExpirationMs)
    notificationListener.init()

    zkClient.createAclChangeNotification(notificationMessage1)
    TestUtils.waitUntilTrue(() => notificationHandler.received().size == 1 && notificationHandler.received().last == notificationMessage1,
      "Failed to send/process notification message in the timeout period.")

    /*
     * There is no easy way to test purging. Even if we mock kafka time with MockTime, the purging compares kafka time
     * with the time stored in ZooKeeper stat and the embedded ZooKeeper server does not provide a way to mock time.
     * So to test purging we would have to use Time.SYSTEM.sleep(changeExpirationMs + 1) issue a write and check
     * Assert.assertEquals(1, KafkaZkClient.getChildren(seqNodeRoot).size). However even that the assertion
     * can fail as the second node can be deleted depending on how threads get scheduled.
     */

    zkClient.createAclChangeNotification(notificationMessage2)
    TestUtils.waitUntilTrue(() => notificationHandler.received().size == 2 && notificationHandler.received().last == notificationMessage2,
      "Failed to send/process notification message in the timeout period.")

    (3 to 10).foreach(i => zkClient.createAclChangeNotification(new ResourcePattern(GROUP, "message" + i, LITERAL)))

    TestUtils.waitUntilTrue(() => notificationHandler.received().size == 10,
      s"Expected 10 invocations of processNotifications, but there were ${notificationHandler.received()}")
  }

  @Test
  def testSwallowsProcessorException(): Unit = {
    notificationHandler.setThrowSize(2)
    notificationListener = new ZkNodeChangeNotificationListener(zkClient, LiteralAclChangeStore.aclChangePath,
      ZkAclChangeStore.SequenceNumberPrefix, notificationHandler, changeExpirationMs)
    notificationListener.init()

    zkClient.createAclChangeNotification(new ResourcePattern(GROUP, "messageA", LITERAL))
    zkClient.createAclChangeNotification(new ResourcePattern(GROUP, "messageB", LITERAL))
    zkClient.createAclChangeNotification(new ResourcePattern(GROUP, "messageC", LITERAL))

    TestUtils.waitUntilTrue(() => notificationHandler.received().size == 3,
      s"Expected 2 invocations of processNotifications, but there were ${notificationHandler.received()}")
  }

  private class TestNotificationHandler extends NotificationHandler {
    private val messages = ArrayBuffer.empty[ResourcePattern]
    @volatile private var throwSize = Option.empty[Int]

    override def processNotification(notificationMessage: Array[Byte]): Unit = {
      messages += LiteralAclStore.changeStore.decode(notificationMessage)

      if (throwSize.contains(messages.size))
        throw new RuntimeException("Oh no, my processing failed!")
    }

    def received(): Seq[ResourcePattern] = messages

    def setThrowSize(index: Int): Unit = throwSize = Option(index)
  }
}
