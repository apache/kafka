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

import kafka.consumer.ConsumerConfig
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{ZkUtils, ZKStringSerializer}
import kafka.utils.TestUtils
import org.junit.Assert
import org.scalatest.junit.JUnit3Suite

class ZKEphemeralTest extends JUnit3Suite with ZooKeeperTestHarness {
  var zkSessionTimeoutMs = 1000

  def testEphemeralNodeCleanup = {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"))
    var zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
                                ZKStringSerializer)

    try {
      ZkUtils.createEphemeralPathExpectConflict(zkClient, "/tmp/zktest", "node created")
    } catch {                       
      case e: Exception =>
    }

    var testData: String = null
    testData = ZkUtils.readData(zkClient, "/tmp/zktest")._1
    Assert.assertNotNull(testData)
    zkClient.close
    zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
                                ZKStringSerializer)
    val nodeExists = ZkUtils.pathExists(zkClient, "/tmp/zktest")
    Assert.assertFalse(nodeExists)
  }
}
