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

import java.util.{OptionalInt, Properties}
import scala.collection.Seq
import kafka.utils.TestUtils
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble, PropertiesUtils}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._

import java.io.File
import org.apache.zookeeper.KeeperException.NodeExistsException

class ServerGenerateBrokerIdTest extends QuorumTestHarness {
  var props1: Properties = _
  var config1: KafkaConfig = _
  var props2: Properties = _
  var config2: KafkaConfig = _
  var servers: Seq[KafkaServer] = Seq()

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    props1 = TestUtils.createBrokerConfig(-1, zkConnect)
    config1 = KafkaConfig.fromProps(props1)
    props2 = TestUtils.createBrokerConfig(0, zkConnect)
    config2 = KafkaConfig.fromProps(props2)
  }

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testAutoGenerateBrokerId(): Unit = {
    var server1 = new KafkaServer(config1, threadNamePrefix = Option(this.getClass.getName))
    server1.startup()
    server1.shutdown()
    assertTrue(verifyBrokerMetadata(config1.logDirs, 1001))
    // restart the server check to see if it uses the brokerId generated previously
    server1 = TestUtils.createServer(config1, threadNamePrefix = Option(this.getClass.getName))
    servers = Seq(server1)
    assertEquals(server1.config.brokerId, 1001)
    server1.shutdown()
    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testUserConfigAndGeneratedBrokerId(): Unit = {
    // start the server with broker.id as part of config
    val server1 = new KafkaServer(config1, threadNamePrefix = Option(this.getClass.getName))
    val server2 = new KafkaServer(config2, threadNamePrefix = Option(this.getClass.getName))
    val props3 = TestUtils.createBrokerConfig(-1, zkConnect)
    val server3 = new KafkaServer(KafkaConfig.fromProps(props3), threadNamePrefix = Option(this.getClass.getName))
    server1.startup()
    assertEquals(server1.config.brokerId, 1001)
    server2.startup()
    assertEquals(server2.config.brokerId, 0)
    server3.startup()
    assertEquals(server3.config.brokerId, 1002)
    servers = Seq(server1, server2, server3)
    servers.foreach(_.shutdown())
    assertTrue(verifyBrokerMetadata(server1.config.logDirs, 1001))
    assertTrue(verifyBrokerMetadata(server2.config.logDirs, 0))
    assertTrue(verifyBrokerMetadata(server3.config.logDirs, 1002))
    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testDisableGeneratedBrokerId(): Unit = {
    val props3 = TestUtils.createBrokerConfig(3, zkConnect)
    props3.put(KafkaConfig.BrokerIdGenerationEnableProp, "false")
    // Set reserve broker ids to cause collision and ensure disabling broker id generation ignores the setting
    props3.put(KafkaConfig.MaxReservedBrokerIdProp, "0")
    val config3 = KafkaConfig.fromProps(props3)
    val server3 = TestUtils.createServer(config3, threadNamePrefix = Option(this.getClass.getName))
    servers = Seq(server3)
    assertEquals(server3.config.brokerId, 3)
    server3.shutdown()
    assertTrue(verifyBrokerMetadata(server3.config.logDirs, 3))
    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testMultipleLogDirsMetaProps(): Unit = {
    // add multiple logDirs and check if the generate brokerId is stored in all of them
    val logDirs = props1.getProperty("log.dir")+ "," + TestUtils.tempDir().getAbsolutePath +
    "," + TestUtils.tempDir().getAbsolutePath
    props1.setProperty("log.dir", logDirs)
    config1 = KafkaConfig.fromProps(props1)
    var server1 = new KafkaServer(config1, threadNamePrefix = Option(this.getClass.getName))
    server1.startup()
    servers = Seq(server1)
    server1.shutdown()
    assertTrue(verifyBrokerMetadata(config1.logDirs, 1001))
    // addition to log.dirs after generation of a broker.id from zk should be copied over
    val newLogDirs = props1.getProperty("log.dir") + "," + TestUtils.tempDir().getAbsolutePath
    props1.setProperty("log.dir", newLogDirs)
    config1 = KafkaConfig.fromProps(props1)
    server1 = new KafkaServer(config1, threadNamePrefix = Option(this.getClass.getName))
    server1.startup()
    servers = Seq(server1)
    server1.shutdown()
    assertTrue(verifyBrokerMetadata(config1.logDirs, 1001))
    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testConsistentBrokerIdFromUserConfigAndMetaProps(): Unit = {
    // check if configured brokerId and stored brokerId are equal or throw InconsistentBrokerException
    var server1 = new KafkaServer(config1, threadNamePrefix = Option(this.getClass.getName)) //auto generate broker Id
    server1.startup()
    servers = Seq(server1)
    server1.shutdown()
    server1 = new KafkaServer(config2, threadNamePrefix = Option(this.getClass.getName)) // user specified broker id
    server1.startup()
    server1.shutdown()
    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testBrokerMetadataOnIdCollision(): Unit = {
    // Start a good server
    val propsA = TestUtils.createBrokerConfig(1, zkConnect)
    val configA = KafkaConfig.fromProps(propsA)
    val serverA = TestUtils.createServer(configA, threadNamePrefix = Option(this.getClass.getName))

    // Start a server that collides on the broker id
    val propsB = TestUtils.createBrokerConfig(1, zkConnect)
    val configB = KafkaConfig.fromProps(propsB)
    val serverB = new KafkaServer(configB, threadNamePrefix = Option(this.getClass.getName))
    assertThrows(classOf[NodeExistsException], () => serverB.startup())
    servers = Seq(serverA)

    // adjust the broker config and start again
    propsB.setProperty(KafkaConfig.BrokerIdProp, "2")
    val serverB2 = new KafkaServer(KafkaConfig.fromProps(propsB),
      threadNamePrefix = Option(this.getClass.getName))
    val startupException = assertThrows(classOf[RuntimeException], () => serverB2.startup())
    assertTrue(startupException.getMessage.startsWith("Stored node id 1 doesn't match previous node id 2"),
      "Unexpected exception message " + startupException.getMessage())
    serverB2.config.logDirs.foreach(logDir => Utils.delete(new File(logDir)))
    propsB.setProperty(KafkaConfig.BrokerIdProp, "3")
    val serverB3 = new KafkaServer(KafkaConfig.fromProps(propsB),
      threadNamePrefix = Option(this.getClass.getName))
    serverB3.startup()
    servers = Seq(serverA, serverB3)

    serverA.shutdown()
    serverB3.shutdown()

    // verify correct broker metadata was written
    assertTrue(verifyBrokerMetadata(serverA.config.logDirs, 1))
    assertTrue(verifyBrokerMetadata(serverB3.config.logDirs, 3))
    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  def verifyBrokerMetadata(logDirs: Seq[String], brokerId: Int): Boolean = {
    for (logDir <- logDirs) {
      val properties = PropertiesUtils.readPropertiesFile(
        new File(logDir, MetaPropertiesEnsemble.META_PROPERTIES_NAME).getAbsolutePath)
      val metaProps = new MetaProperties.Builder(properties).build()
      if (!metaProps.nodeId().equals(OptionalInt.of(brokerId))) {
        return false
      }
    }
    true
  }
}
