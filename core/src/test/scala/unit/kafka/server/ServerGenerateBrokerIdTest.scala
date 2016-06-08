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

import kafka.zk.ZooKeeperTestHarness
import kafka.utils.{TestUtils, CoreUtils}
import org.junit.{Before, Test}
import org.junit.Assert._
import java.io.File

class ServerGenerateBrokerIdTest extends ZooKeeperTestHarness {
  var props1: Properties = null
  var config1: KafkaConfig = null
  var props2: Properties = null
  var config2: KafkaConfig = null
  val brokerMetaPropsFile = "meta.properties"

  @Before
  override def setUp() {
    super.setUp()
    props1 = TestUtils.createBrokerConfig(-1, zkConnect)
    config1 = KafkaConfig.fromProps(props1)
    props2 = TestUtils.createBrokerConfig(0, zkConnect)
    config2 = KafkaConfig.fromProps(props2)
  }

  @Test
  def testAutoGenerateBrokerId() {
    var server1 = new KafkaServer(config1, threadNamePrefix = Option(this.getClass.getName))
    server1.startup()
    server1.shutdown()
    assertTrue(verifyBrokerMetadata(config1.logDirs, 1001))
    // restart the server check to see if it uses the brokerId generated previously
    server1 = new KafkaServer(config1)
    server1.startup()
    assertEquals(server1.config.brokerId, 1001)
    server1.shutdown()
    CoreUtils.delete(server1.config.logDirs)
    TestUtils.verifyNonDaemonThreadsStatus(this.getClass.getName)
  }

  @Test
  def testUserConfigAndGeneratedBrokerId() {
    // start the server with broker.id as part of config
    val server1 = new KafkaServer(config1, threadNamePrefix = Option(this.getClass.getName))
    val server2 = new KafkaServer(config2, threadNamePrefix = Option(this.getClass.getName))
    val props3 = TestUtils.createBrokerConfig(-1, zkConnect)
    val config3 = KafkaConfig.fromProps(props3)
    val server3 = new KafkaServer(config3)
    server1.startup()
    assertEquals(server1.config.brokerId,1001)
    server2.startup()
    assertEquals(server2.config.brokerId,0)
    server3.startup()
    assertEquals(server3.config.brokerId,1002)
    server1.shutdown()
    server2.shutdown()
    server3.shutdown()
    assertTrue(verifyBrokerMetadata(server1.config.logDirs,1001))
    assertTrue(verifyBrokerMetadata(server2.config.logDirs,0))
    assertTrue(verifyBrokerMetadata(server3.config.logDirs,1002))
    CoreUtils.delete(server1.config.logDirs)
    CoreUtils.delete(server2.config.logDirs)
    CoreUtils.delete(server3.config.logDirs)
    TestUtils.verifyNonDaemonThreadsStatus(this.getClass.getName)
  }

  @Test
  def testDisableGeneratedBrokerId() {
    val props3 = TestUtils.createBrokerConfig(3, zkConnect)
    props3.put(KafkaConfig.BrokerIdGenerationEnableProp, "false")
    // Set reserve broker ids to cause collision and ensure disabling broker id generation ignores the setting
    props3.put(KafkaConfig.MaxReservedBrokerIdProp, "0")
    val config3 = KafkaConfig.fromProps(props3)
    val server3 = new KafkaServer(config3)
    server3.startup()
    assertEquals(server3.config.brokerId,3)
    server3.shutdown()
    assertTrue(verifyBrokerMetadata(server3.config.logDirs,3))
    CoreUtils.delete(server3.config.logDirs)
    TestUtils.verifyNonDaemonThreadsStatus(this.getClass.getName)
  }

  @Test
  def testMultipleLogDirsMetaProps() {
    // add multiple logDirs and check if the generate brokerId is stored in all of them
    val logDirs = props1.getProperty("log.dir")+ "," + TestUtils.tempDir().getAbsolutePath +
    "," + TestUtils.tempDir().getAbsolutePath
    props1.setProperty("log.dir",logDirs)
    config1 = KafkaConfig.fromProps(props1)
    var server1 = new KafkaServer(config1, threadNamePrefix = Option(this.getClass.getName))
    server1.startup()
    server1.shutdown()
    assertTrue(verifyBrokerMetadata(config1.logDirs, 1001))
    // addition to log.dirs after generation of a broker.id from zk should be copied over
    val newLogDirs = props1.getProperty("log.dir") + "," + TestUtils.tempDir().getAbsolutePath
    props1.setProperty("log.dir",newLogDirs)
    config1 = KafkaConfig.fromProps(props1)
    server1 = new KafkaServer(config1, threadNamePrefix = Option(this.getClass.getName))
    server1.startup()
    server1.shutdown()
    assertTrue(verifyBrokerMetadata(config1.logDirs, 1001))
    CoreUtils.delete(server1.config.logDirs)
    TestUtils.verifyNonDaemonThreadsStatus(this.getClass.getName)
  }

  @Test
  def testConsistentBrokerIdFromUserConfigAndMetaProps() {
    // check if configured brokerId and stored brokerId are equal or throw InconsistentBrokerException
    var server1 = new KafkaServer(config1, threadNamePrefix = Option(this.getClass.getName)) //auto generate broker Id
    server1.startup()
    server1.shutdown()
    server1 = new KafkaServer(config2, threadNamePrefix = Option(this.getClass.getName)) // user specified broker id
    try {
      server1.startup()
    } catch {
      case e: kafka.common.InconsistentBrokerIdException => //success
    }
    server1.shutdown()
    CoreUtils.delete(server1.config.logDirs)
    TestUtils.verifyNonDaemonThreadsStatus(this.getClass.getName)
  }

  @Test
  def testBrokerMetadataOnIdCollision() {
    // Start a good server
    val propsA = TestUtils.createBrokerConfig(1, zkConnect)
    val configA = KafkaConfig.fromProps(propsA)
    val serverA = new KafkaServer(configA)
    serverA.startup()

    // Start a server that collides on the broker id
    val propsB = TestUtils.createBrokerConfig(1, zkConnect)
    val configB = KafkaConfig.fromProps(propsB)
    val serverB = new KafkaServer(configB)
    intercept[RuntimeException] {
      serverB.startup()
    }

    // verify no broker metadata was written
    serverB.config.logDirs.foreach { logDir =>
      val brokerMetaFile = new File(logDir + File.separator + brokerMetaPropsFile)
      assertFalse(brokerMetaFile.exists())
    }

    // adjust the broker config and start again
    propsB.setProperty(KafkaConfig.BrokerIdProp, "2")
    val newConfigB = KafkaConfig.fromProps(propsB)
    val newServerB = new KafkaServer(newConfigB)
    newServerB.startup()

    serverA.shutdown()
    newServerB.shutdown()
    // verify correct broker metadata was written
    assertTrue(verifyBrokerMetadata(serverA.config.logDirs,1))
    assertTrue(verifyBrokerMetadata(newServerB.config.logDirs,2))
    CoreUtils.delete(serverA.config.logDirs)
    CoreUtils.delete(newServerB.config.logDirs)
    TestUtils.verifyNonDaemonThreadsStatus(this.getClass.getName)
  }

  def verifyBrokerMetadata(logDirs: Seq[String], brokerId: Int): Boolean = {
    for(logDir <- logDirs) {
      val brokerMetadataOpt = new BrokerMetadataCheckpoint(
        new File(logDir + File.separator + brokerMetaPropsFile)).read()
      brokerMetadataOpt match {
        case Some(brokerMetadata: BrokerMetadata) =>
          if (brokerMetadata.brokerId != brokerId)  return false
        case _ => return false
      }
    }
    true
  }

  @Test
  def testGetSequenceIdMethod() {
    val path = "/test/seqid"
    (1 to 10).foreach { seqid =>
      assertEquals(seqid, zkUtils.getSequenceId(path))
    }
  }
}
