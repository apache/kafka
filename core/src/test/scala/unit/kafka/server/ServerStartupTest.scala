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

import org.scalatest.junit.JUnit3Suite
import kafka.utils.ZkUtils
import kafka.utils.Utils
import kafka.utils.TestUtils

import kafka.zk.ZooKeeperTestHarness
import junit.framework.Assert._

class ServerStartupTest extends JUnit3Suite with ZooKeeperTestHarness {

  def testBrokerCreatesZKChroot {
    val brokerId = 0
    val zookeeperChroot = "/kafka-chroot-for-unittest"
    val props = TestUtils.createBrokerConfig(brokerId, TestUtils.choosePort())
    val zooKeeperConnect = props.get("zookeeper.connect")
    props.put("zookeeper.connect", zooKeeperConnect + zookeeperChroot)
    val server = TestUtils.createServer(new KafkaConfig(props))

    val pathExists = ZkUtils.pathExists(zkClient, zookeeperChroot)
    assertTrue(pathExists)

    server.shutdown()
    Utils.rm(server.config.logDirs)
  }

  def testConflictBrokerRegistration {
    // Try starting a broker with the a conflicting broker id.
    // This shouldn't affect the existing broker registration.

    val brokerId = 0
    val props1 = TestUtils.createBrokerConfig(brokerId)
    val server1 = TestUtils.createServer(new KafkaConfig(props1))
    val brokerRegistration = ZkUtils.readData(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId)._1

    val props2 = TestUtils.createBrokerConfig(brokerId)
    try {
      TestUtils.createServer(new KafkaConfig(props2))
      fail("Registering a broker with a conflicting id should fail")
    } catch {
      case e : RuntimeException =>
      // this is expected
    }

    // broker registration shouldn't change
    assertEquals(brokerRegistration, ZkUtils.readData(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId)._1)

    server1.shutdown()
    Utils.rm(server1.config.logDirs)
  }
}