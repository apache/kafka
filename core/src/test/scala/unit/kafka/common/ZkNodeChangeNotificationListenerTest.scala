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

import java.nio.charset.StandardCharsets

import kafka.utils.TestUtils
import kafka.zk.{AclChangeNotificationSequenceZNode, AclChangeNotificationZNode, ZooKeeperTestHarness}
import org.junit.Test

class ZkNodeChangeNotificationListenerTest extends ZooKeeperTestHarness {

  @Test
  def testProcessNotification(): Unit = {
    @volatile var notification: String = null
    @volatile var invocationCount = 0
    val notificationHandler = new NotificationHandler {
      override def processNotification(notificationMessage: Array[Byte]): Unit = {
        notification = new String(notificationMessage, StandardCharsets.UTF_8)
        invocationCount += 1
      }
    }

    zkClient.createAclPaths()
    val notificationMessage1 = "message1"
    val notificationMessage2 = "message2"
    val changeExpirationMs = 1000

    val notificationListener = new ZkNodeChangeNotificationListener(zkClient,  AclChangeNotificationZNode.path,
      AclChangeNotificationSequenceZNode.SequenceNumberPrefix, notificationHandler, changeExpirationMs)
    notificationListener.init()

    zkClient.createAclChangeNotification(notificationMessage1)
    TestUtils.waitUntilTrue(() => invocationCount == 1 && notification == notificationMessage1,
      "Failed to send/process notification message in the timeout period.")

    /*
     * There is no easy way to test purging. Even if we mock kafka time with MockTime, the purging compares kafka time
     * with the time stored in ZooKeeper stat and the embedded ZooKeeper server does not provide a way to mock time.
     * So to test purging we would have to use Time.SYSTEM.sleep(changeExpirationMs + 1) issue a write and check
     * Assert.assertEquals(1, ZkUtils.getChildren(zkClient, seqNodeRoot).size). However even that the assertion
     * can fail as the second node can be deleted depending on how threads get scheduled.
     */

    zkClient.createAclChangeNotification(notificationMessage2)
    TestUtils.waitUntilTrue(() => invocationCount == 2 && notification == notificationMessage2,
      "Failed to send/process notification message in the timeout period.")

    (3 to 10).foreach(i => zkClient.createAclChangeNotification("message" + i))

    TestUtils.waitUntilTrue(() => invocationCount == 10 ,
      s"Expected 10 invocations of processNotifications, but there were $invocationCount")
  }
}
