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

import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{CoreUtils, TestUtils, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import org.junit.Assert._
import org.junit.{Before, Test}

class ServerGenerateClusterIdTest extends ZooKeeperTestHarness {
  var props1: Properties = null
  var config1: KafkaConfig = null
  var props2: Properties = null
  var config2: KafkaConfig = null
  var props3: Properties = null
  var config3: KafkaConfig = null

  @Before
  override def setUp() {
    super.setUp()
    props1 = TestUtils.createBrokerConfig(1, zkConnect)
    config1 = KafkaConfig.fromProps(props1)
    props2 = TestUtils.createBrokerConfig(2, zkConnect)
    config2 = KafkaConfig.fromProps(props2)
    props3 = TestUtils.createBrokerConfig(3, zkConnect)
    config3 = KafkaConfig.fromProps(props3)
  }

  @Test
  def testAutoGenerateClusterId() {
    //Make sure that the cluster id doesn't exist yet.
    assertFalse(zkUtils.pathExists(ZkUtils.ClusterIdPath))
    var server1 = new KafkaServer(config1, threadNamePrefix = Option(this.getClass.getName))
    server1.startup()

    // Make sure the cluster id is 48 characters long (base64 of UUID.randomUUID() )
    val clusterIdOnFirstBoot = server1.clusterId
    assertEquals(clusterIdOnFirstBoot.length, 48)

    server1.shutdown()

    //Make sure that the cluster id is persistent.
    assertTrue(zkUtils.pathExists(ZkUtils.ClusterIdPath))
    assertEquals(zkUtils.getClusterId().get, clusterIdOnFirstBoot)

    // restart the server check to confirm that it uses the clusterId generated previously
    server1 = new KafkaServer(config1)
    server1.startup()

    val clusterIdOnSecondBoot = server1.clusterId
    assertEquals(clusterIdOnFirstBoot, clusterIdOnSecondBoot)

    server1.shutdown()

    //Make sure that the cluster id is persistent after multiple reboots.
    assertTrue(zkUtils.pathExists(ZkUtils.ClusterIdPath))
    assertEquals(zkUtils.getClusterId().get, clusterIdOnFirstBoot)

    CoreUtils.delete(server1.config.logDirs)
    TestUtils.verifyNonDaemonThreadsStatus(this.getClass.getName)
  }

  @Test
  def testAutoGenerateClusterIdForKafkaCluster() {
    val server1 = new KafkaServer(config1, threadNamePrefix = Option(this.getClass.getName))
    val server2 = new KafkaServer(config2, threadNamePrefix = Option(this.getClass.getName))
    val server3 = new KafkaServer(config3, threadNamePrefix = Option(this.getClass.getName))
    server1.startup()
    val clusterIdFromServer1 = server1.clusterId
    server2.startup()
    val clusterIdFromServer2 = server2.clusterId
    server3.startup()
    val clusterIdFromServer3 = server3.clusterId
    server1.shutdown()
    server2.shutdown()
    server3.shutdown()
    assertEquals(clusterIdFromServer1, clusterIdFromServer2, clusterIdFromServer3)

    // Check again after reboot
    server1.startup()
    assertEquals(clusterIdFromServer1, server1.clusterId)
    server2.startup()
    assertEquals(clusterIdFromServer2, server2.clusterId)
    server3.startup()
    assertEquals(clusterIdFromServer3, server3.clusterId)
    server1.shutdown()
    server2.shutdown()
    server3.shutdown()

    CoreUtils.delete(server1.config.logDirs)
    CoreUtils.delete(server2.config.logDirs)
    CoreUtils.delete(server3.config.logDirs)
    TestUtils.verifyNonDaemonThreadsStatus(this.getClass.getName)
  }

}
