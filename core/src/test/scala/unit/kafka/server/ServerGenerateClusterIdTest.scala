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

import java.io.File


import scala.collection.Seq
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits._

import kafka.common.{InconsistentBrokerMetadataException, InconsistentClusterIdException}
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness

import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.Assertions.assertThrows
import org.apache.kafka.test.TestUtils.isValidClusterId


class ServerGenerateClusterIdTest extends ZooKeeperTestHarness {
  var config1: KafkaConfig = null
  var config2: KafkaConfig = null
  var config3: KafkaConfig = null
  var servers: Seq[KafkaServer] = Seq()
  val brokerMetaPropsFile = "meta.properties"

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

    var server1 = TestUtils.createServer(config1, threadNamePrefix = Option(this.getClass.getName))
    servers = Seq(server1)

    // Validate the cluster id
    val clusterIdOnFirstBoot = server1.clusterId
    isValidClusterId(clusterIdOnFirstBoot)

    server1.shutdown()

    // Make sure that the cluster id is persistent.
    assertTrue(zkClient.getClusterId.isDefined)
    assertEquals(zkClient.getClusterId, Some(clusterIdOnFirstBoot))

    // Restart the server check to confirm that it uses the clusterId generated previously
    server1 = TestUtils.createServer(config1, threadNamePrefix = Option(this.getClass.getName))
    servers = Seq(server1)

    val clusterIdOnSecondBoot = server1.clusterId
    assertEquals(clusterIdOnFirstBoot, clusterIdOnSecondBoot)

    server1.shutdown()

    // Make sure that the cluster id is persistent after multiple reboots.
    assertTrue(zkClient.getClusterId.isDefined)
    assertEquals(zkClient.getClusterId, Some(clusterIdOnFirstBoot))

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testAutoGenerateClusterIdForKafkaClusterSequential(): Unit = {
    val server1 = TestUtils.createServer(config1, threadNamePrefix = Option(this.getClass.getName))
    val clusterIdFromServer1 = server1.clusterId

    val server2 = TestUtils.createServer(config2, threadNamePrefix = Option(this.getClass.getName))
    val clusterIdFromServer2 = server2.clusterId

    val server3 = TestUtils.createServer(config3, threadNamePrefix = Option(this.getClass.getName))
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

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testAutoGenerateClusterIdForKafkaClusterParallel(): Unit = {
    val firstBoot = Future.traverse(Seq(config1, config2, config3))(config => Future(TestUtils.createServer(config, threadNamePrefix = Option(this.getClass.getName))))
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

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testConsistentClusterIdFromZookeeperAndFromMetaProps() = {
    // Check at the first boot
    val server = TestUtils.createServer(config1, threadNamePrefix = Option(this.getClass.getName))
    val clusterId = server.clusterId

    assertTrue(verifyBrokerMetadata(server.config.logDirs, clusterId))

    server.shutdown()

    // Check again after reboot
    server.startup()

    assertEquals(clusterId, server.clusterId)
    assertTrue(verifyBrokerMetadata(server.config.logDirs, server.clusterId))

    server.shutdown()

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testInconsistentClusterIdFromZookeeperAndFromMetaProps() = {
    forgeBrokerMetadata(config1.logDirs, config1.brokerId, "aclusterid")

    val server = new KafkaServer(config1, threadNamePrefix = Option(this.getClass.getName))

    // Startup fails
    assertThrows[InconsistentClusterIdException] {
      server.startup()
    }

    server.shutdown()

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testInconsistentBrokerMetadataBetweenMultipleLogDirs(): Unit = {
    // Add multiple logDirs with different BrokerMetadata
    val logDir1 = TestUtils.tempDir().getAbsolutePath
    val logDir2 = TestUtils.tempDir().getAbsolutePath
    val logDirs = logDir1 + "," + logDir2

    forgeBrokerMetadata(logDir1, 1, "ebwOKU-zSieInaFQh_qP4g")
    forgeBrokerMetadata(logDir2, 1, "blaOKU-zSieInaFQh_qP4g")

    val props = TestUtils.createBrokerConfig(1, zkConnect)
    props.setProperty("log.dir", logDirs)
    val config = KafkaConfig.fromProps(props)

    val server = new KafkaServer(config, threadNamePrefix = Option(this.getClass.getName))

    // Startup fails
    assertThrows[InconsistentBrokerMetadataException] {
      server.startup()
    }

    server.shutdown()

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  def forgeBrokerMetadata(logDirs: Seq[String], brokerId: Int, clusterId: String): Unit = {
    for (logDir <- logDirs) {
      forgeBrokerMetadata(logDir, brokerId, clusterId)
    }
  }

  def forgeBrokerMetadata(logDir: String, brokerId: Int, clusterId: String): Unit = {
    val checkpoint = new BrokerMetadataCheckpoint(
      new File(logDir + File.separator + brokerMetaPropsFile))
    checkpoint.write(BrokerMetadata(brokerId, Option(clusterId)))
  }

  def verifyBrokerMetadata(logDirs: Seq[String], clusterId: String): Boolean = {
    for (logDir <- logDirs) {
      val brokerMetadataOpt = new BrokerMetadataCheckpoint(
        new File(logDir + File.separator + brokerMetaPropsFile)).read()
      brokerMetadataOpt match {
        case Some(brokerMetadata) =>
          if (brokerMetadata.clusterId.isDefined && brokerMetadata.clusterId.get != clusterId) return false
        case _ => return false
      }
    }
    true
  }
}
