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

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{TestUtils, ZkUtils}
import org.junit.Test

class ZkNodeChangeNotificationListenerTest extends KafkaServerTestHarness {

  override def generateConfigs() = List(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))

  @Test
  def testProcessNotification() {
    @volatile var notification: String = null
    @volatile var invocationCount = 0
    val notificationHandler = new NotificationHandler {
      override def processNotification(notificationMessage: String): Unit = {
        notification = notificationMessage
        invocationCount += 1
      }
    }

    val seqNodeRoot = "/root"
    val seqNodePrefix = "prefix"
    val seqNodePath = seqNodeRoot + "/" + seqNodePrefix
    val notificationMessage1 = "message1"
    val notificationMessage2 = "message2"
    val changeExpirationMs = 100

    val notificationListener = new ZkNodeChangeNotificationListener(zkUtils, seqNodeRoot, seqNodePrefix, notificationHandler, changeExpirationMs)
    notificationListener.init()

    zkUtils.createSequentialPersistentPath(seqNodePath, notificationMessage1)

    TestUtils.waitUntilTrue(() => invocationCount == 1 && notification == notificationMessage1, "failed to send/process notification message in the timeout period.")

    /*There is no easy way to test that purging. Even if we mock kafka time with MockTime, the purging compares kafka time with the time stored in zookeeper stat and the
    embeded zookeeper server does not provide a way to mock time. so to test purging we will have to use SystemTime.sleep(changeExpirationMs + 1) issue a write and check
    Assert.assertEquals(1, ZkUtils.getChildren(zkClient, seqNodeRoot).size) however even after that the assertion can fail as the second node it self can be deleted
    depending on how threads get scheduled.*/

    zkUtils.createSequentialPersistentPath(seqNodePath, notificationMessage2)
    TestUtils.waitUntilTrue(() => invocationCount == 2 && notification == notificationMessage2, "failed to send/process notification message in the timeout period.")
  }
}
