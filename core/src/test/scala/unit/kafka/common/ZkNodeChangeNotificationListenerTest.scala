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

import kafka.security.auth.{Group, Resource}
import kafka.utils.TestUtils
import kafka.zk.{LiteralAclChangeStore, LiteralAclStore, ZkAclChangeStore, ZooKeeperTestHarness}
import org.apache.kafka.common.resource.ResourceNameType.LITERAL
import org.junit.{After, Before, Test}

import scala.collection.mutable.ArrayBuffer

class ZkNodeChangeNotificationListenerTest extends ZooKeeperTestHarness {

  private val changeExpirationMs = 1000
  private var notificationListener: ZkNodeChangeNotificationListener = _
  private var notificationHandler: TestNotificationHandler = _

  @Before
  override def setUp(): Unit = {
    super.setUp()
    zkClient.createAclPaths()
    notificationHandler = new TestNotificationHandler()
  }

  @After
  override def tearDown(): Unit = {
    if (notificationListener != null) {
      notificationListener.close()
    }
    super.tearDown()
  }

  @Test
  def testProcessNotification() {
    val notificationMessage1 = Resource(Group, "messageA", LITERAL)
    val notificationMessage2 = Resource(Group, "messageB", LITERAL)

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
     * Assert.assertEquals(1, ZkUtils.getChildren(zkClient, seqNodeRoot).size). However even that the assertion
     * can fail as the second node can be deleted depending on how threads get scheduled.
     */

    zkClient.createAclChangeNotification(notificationMessage2)
    TestUtils.waitUntilTrue(() => notificationHandler.received().size == 2 && notificationHandler.received().last == notificationMessage2,
      "Failed to send/process notification message in the timeout period.")

    (3 to 10).foreach(i => zkClient.createAclChangeNotification(Resource(Group, "message" + i, LITERAL)))

    TestUtils.waitUntilTrue(() => notificationHandler.received().size == 10,
      s"Expected 10 invocations of processNotifications, but there were ${notificationHandler.received()}")
  }

  @Test
  def testSwallowsProcessorException() : Unit = {
    notificationHandler.setThrowSize(2)
    notificationListener = new ZkNodeChangeNotificationListener(zkClient, LiteralAclChangeStore.aclChangePath,
      ZkAclChangeStore.SequenceNumberPrefix, notificationHandler, changeExpirationMs)
    notificationListener.init()

    zkClient.createAclChangeNotification(Resource(Group, "messageA", LITERAL))
    zkClient.createAclChangeNotification(Resource(Group, "messageB", LITERAL))
    zkClient.createAclChangeNotification(Resource(Group, "messageC", LITERAL))

    TestUtils.waitUntilTrue(() => notificationHandler.received().size == 3,
      s"Expected 2 invocations of processNotifications, but there were ${notificationHandler.received()}")
  }

  private class TestNotificationHandler extends NotificationHandler {
    private val messages = ArrayBuffer.empty[Resource]
    @volatile private var throwSize = Option.empty[Int]

    override def processNotification(notificationMessage: Array[Byte]): Unit = {
      messages += LiteralAclStore.changeStore.decode(notificationMessage)

      if (throwSize.contains(messages.size))
        throw new RuntimeException("Oh no, my processing failed!")
    }

    def received(): Seq[Resource] = messages

    def setThrowSize(index: Int): Unit = throwSize = Option(index)
  }
}