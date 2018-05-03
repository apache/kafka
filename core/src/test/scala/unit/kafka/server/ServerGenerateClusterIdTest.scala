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

import scala.concurrent._
import ExecutionContext.Implicits._
import scala.concurrent.duration._
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.junit.Assert._
import org.junit.{Before, After, Test}
import org.apache.kafka.test.TestUtils.isValidClusterId

class ServerGenerateClusterIdTest extends ZooKeeperTestHarness {
  var config1: KafkaConfig = null
  var config2: KafkaConfig = null
  var config3: KafkaConfig = null
  var servers: Seq[KafkaServer] = Seq()

  @Before
  override def setUp(): Unit = {
    super.setUp()
    config1 = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, zkConnect))
    config2 = KafkaConfig.fromProps(TestUtils.createBrokerConfig(2, zkConnect))
    config3 = KafkaConfig.fromProps(TestUtils.createBrokerConfig(3, zkConnect))
  }

  @After
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }


  @Test
  def testAutoGenerateClusterId(): Unit = {
    // Make sure that the cluster id doesn't exist yet.
    assertFalse(zkClient.getClusterId.isDefined)

    var server1 = TestUtils.createServer(config1)
    servers = Seq(server1)

    // Validate the cluster id
    val clusterIdOnFirstBoot = server1.clusterId
    isValidClusterId(clusterIdOnFirstBoot)

    server1.shutdown()

    // Make sure that the cluster id is persistent.
    assertTrue(zkClient.getClusterId.isDefined)
    assertEquals(zkClient.getClusterId, Some(clusterIdOnFirstBoot))

    // Restart the server check to confirm that it uses the clusterId generated previously
    server1 = TestUtils.createServer(config1)
    servers = Seq(server1)

    val clusterIdOnSecondBoot = server1.clusterId
    assertEquals(clusterIdOnFirstBoot, clusterIdOnSecondBoot)

    server1.shutdown()

    // Make sure that the cluster id is persistent after multiple reboots.
    assertTrue(zkClient.getClusterId.isDefined)
    assertEquals(zkClient.getClusterId, Some(clusterIdOnFirstBoot))

    TestUtils.verifyNonDaemonThreadsStatus(this.getClass.getName)
  }

  @Test
  def testAutoGenerateClusterIdForKafkaClusterSequential(): Unit = {
    val server1 = TestUtils.createServer(config1)
    val clusterIdFromServer1 = server1.clusterId

    val server2 = TestUtils.createServer(config2)
    val clusterIdFromServer2 = server2.clusterId

    val server3 = TestUtils.createServer(config3)
    val clusterIdFromServer3 = server3.clusterId
    servers = Seq(server1, server2, server3)

    servers.foreach(_.shutdown())

    isValidClusterId(clusterIdFromServer1)
    assertEquals(clusterIdFromServer1, clusterIdFromServer2, clusterIdFromServer3)

    // Check again after reboot
    server1.startup()
    assertEquals(clusterIdFromServer1, server1.clusterId)
    server2.startup()
    assertEquals(clusterIdFromServer2, server2.clusterId)
    server3.startup()
    assertEquals(clusterIdFromServer3, server3.clusterId)

    servers.foreach(_.shutdown())

    TestUtils.verifyNonDaemonThreadsStatus(this.getClass.getName)
  }

  @Test
  def testAutoGenerateClusterIdForKafkaClusterParallel(): Unit = {
    val firstBoot = Future.traverse(Seq(config1, config2, config3))(config => Future(TestUtils.createServer(config)))
    servers = Await.result(firstBoot, 100 second)
    val Seq(server1, server2, server3) = servers

    val clusterIdFromServer1 = server1.clusterId
    val clusterIdFromServer2 = server2.clusterId
    val clusterIdFromServer3 = server3.clusterId

    servers.foreach(_.shutdown())
    isValidClusterId(clusterIdFromServer1)
    assertEquals(clusterIdFromServer1, clusterIdFromServer2, clusterIdFromServer3)

    // Check again after reboot
    val secondBoot = Future.traverse(Seq(server1, server2, server3))(server => Future {
      server.startup()
      server
    })
    servers = Await.result(secondBoot, 100 second)
    servers.foreach(server => assertEquals(clusterIdFromServer1, server.clusterId))

    servers.foreach(_.shutdown())

    TestUtils.verifyNonDaemonThreadsStatus(this.getClass.getName)
  }

}
