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

package kafka.security.auth

import java.nio.charset.StandardCharsets

import kafka.admin.ZkSecurityMigrator
import kafka.server.QuorumTestHarness
import kafka.utils.{Logging, TestUtils}
import kafka.zk._
import org.apache.kafka.common.{KafkaException, TopicPartition, Uuid}
import org.apache.kafka.common.security.JaasUtils
import org.apache.zookeeper.data.{ACL, Stat}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

import scala.util.{Failure, Success, Try}
import javax.security.auth.login.Configuration
import kafka.cluster.{Broker, EndPoint}
import kafka.controller.ReplicaAssignment
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.common.MetadataVersion
import org.apache.zookeeper.client.ZKClientConfig

import scala.jdk.CollectionConverters._
import scala.collection.Seq

class ZkAuthorizationTest extends QuorumTestHarness with Logging {
  val jaasFile = kafka.utils.JaasTestUtils.writeJaasContextsToFile(kafka.utils.JaasTestUtils.zkSections)
  val authProvider = "zookeeper.authProvider.1"

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasFile.getAbsolutePath)
    Configuration.setConfiguration(null)
    System.setProperty(authProvider, "org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
    super.setUp(testInfo)
  }

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    System.clearProperty(authProvider)
    Configuration.setConfiguration(null)
  }

  /**
   * Tests the method in JaasUtils that checks whether to use
   * secure ACLs and authentication with ZooKeeper.
   */
  @Test
  def testIsZkSecurityEnabled(): Unit = {
    assertTrue(JaasUtils.isZkSaslEnabled())
    Configuration.setConfiguration(null)
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    assertFalse(JaasUtils.isZkSaslEnabled())
    Configuration.setConfiguration(null)
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, "no-such-file-exists.conf")
    assertThrows(classOf[KafkaException], () => JaasUtils.isZkSaslEnabled())
  }

  /**
   * Exercises the code in KafkaZkClient. The goal is mainly
   * to verify that the behavior of KafkaZkClient is correct
   * when isSecure is set to true.
   */
  @Test
  def testKafkaZkClient(): Unit = {
    assertTrue(zkClient.secure)
    for (path <- ZkData.PersistentZkPaths) {
      zkClient.makeSurePersistentPathExists(path)
      if (ZkData.sensitivePath(path)) {
        val aclList = zkClient.getAcl(path)
        assertEquals(1, aclList.size, s"Unexpected acl list size for $path")
        for (acl <- aclList)
          assertTrue(TestUtils.isAclSecure(acl, sensitive = true))
      } else if (!path.equals(ConsumerPathZNode.path)) {
        val aclList = zkClient.getAcl(path)
        assertEquals(2, aclList.size, s"Unexpected acl list size for $path")
        for (acl <- aclList)
          assertTrue(TestUtils.isAclSecure(acl, sensitive = false))
      }
    }

    // Test that creates Ephemeral node
    val brokerInfo = createBrokerInfo(1, "test.host", 9999, SecurityProtocol.PLAINTEXT)
    zkClient.registerBroker(brokerInfo)
    verify(brokerInfo.path)

    // Test that creates persistent nodes
    val topic1 = "topic1"
    val topicId = Some(Uuid.randomUuid())
    val assignment = Map(
      new TopicPartition(topic1, 0) -> Seq(0, 1),
      new TopicPartition(topic1, 1) -> Seq(0, 1),
      new TopicPartition(topic1, 2) -> Seq(1, 2, 3)
    )

    // create a topic assignment
    zkClient.createTopicAssignment(topic1, topicId, assignment)
    verify(TopicZNode.path(topic1))

    // Test that can create: createSequentialPersistentPath
    val seqPath = zkClient.createSequentialPersistentPath("/c", "".getBytes(StandardCharsets.UTF_8))
    verify(seqPath)

    // Test that can update Ephemeral node
    val updatedBrokerInfo = createBrokerInfo(1, "test.host2", 9995, SecurityProtocol.SSL)
    zkClient.updateBrokerInfo(updatedBrokerInfo)
    assertEquals(Some(updatedBrokerInfo.broker), zkClient.getBroker(1))

    // Test that can update persistent nodes
    val updatedAssignment = assignment - new TopicPartition(topic1, 2)
    zkClient.setTopicAssignment(topic1, topicId,
      updatedAssignment.map { case (k, v) => k -> ReplicaAssignment(v, List(), List()) })
    assertEquals(updatedAssignment.size, zkClient.getTopicPartitionCount(topic1).get)
  }

  private def createBrokerInfo(id: Int, host: String, port: Int, securityProtocol: SecurityProtocol,
                               rack: Option[String] = None): BrokerInfo =
    BrokerInfo(Broker(id, Seq(new EndPoint(host, port, ListenerName.forSecurityProtocol
    (securityProtocol), securityProtocol)), rack = rack), MetadataVersion.latestTesting, jmxPort = port + 10)

  private def newKafkaZkClient(connectionString: String, isSecure: Boolean) =
    KafkaZkClient(connectionString, isSecure, 6000, 6000, Int.MaxValue, Time.SYSTEM, "ZkAuthorizationTest",
      new ZKClientConfig)

  /**
   * Tests the migration tool when making an unsecure
   * cluster secure.
   */
  @Test
  def testZkMigration(): Unit = {
    val unsecureZkClient = newKafkaZkClient(zkConnect, isSecure = false)
    try {
      testMigration(zkConnect, unsecureZkClient, zkClient)
    } finally {
      unsecureZkClient.close()
    }
  }

  /**
   * Tests the migration tool when making a secure
   * cluster unsecure.
   */
  @Test
  def testZkAntiMigration(): Unit = {
    val unsecureZkClient = newKafkaZkClient(zkConnect, isSecure = false)
    try {
      testMigration(zkConnect, zkClient, unsecureZkClient)
    } finally {
      unsecureZkClient.close()
    }
  }

  /**
   * Tests that the persistent paths cannot be deleted.
   */
  @Test
  def testDelete(): Unit = {
    info(s"zkConnect string: $zkConnect")
    ZkSecurityMigrator.run(Array("--zookeeper.acl=secure", s"--zookeeper.connect=$zkConnect"))
    deleteAllUnsecure()
  }

  /**
   * Tests that znodes cannot be deleted when the
   * persistent paths have children.
   */
  @Test
  def testDeleteRecursive(): Unit = {
    info(s"zkConnect string: $zkConnect")
    for (path <- ZkData.SecureRootPaths) {
      info(s"Creating $path")
      zkClient.makeSurePersistentPathExists(path)
      zkClient.createRecursive(s"$path/fpjwashere", "".getBytes(StandardCharsets.UTF_8))
    }
    zkClient.setAcl("/", zkClient.defaultAcls("/"))
    deleteAllUnsecure()
  }

  /**
   * Tests the migration tool when chroot is being used.
   */
  @Test
  def testChroot(): Unit = {
    val zkUrl = zkConnect + "/kafka"
    zkClient.createRecursive("/kafka")
    val unsecureZkClient = newKafkaZkClient(zkUrl, isSecure = false)
    val secureZkClient = newKafkaZkClient(zkUrl, isSecure = true)
    try {
      testMigration(zkUrl, unsecureZkClient, secureZkClient)
    } finally {
      unsecureZkClient.close()
      secureZkClient.close()
    }
  }

  /**
   * Exercises the migration tool. It is used in these test cases:
   * testZkMigration, testZkAntiMigration, testChroot.
   */
  private def testMigration(zkUrl: String, firstZk: KafkaZkClient, secondZk: KafkaZkClient): Unit = {
    info(s"zkConnect string: $zkUrl")
    for (path <- ZkData.SecureRootPaths ++ ZkData.SensitiveRootPaths) {
      info(s"Creating $path")
      firstZk.makeSurePersistentPathExists(path)
      // Create a child for each znode to exercise the recurrent
      // traversal of the data tree
      firstZk.createRecursive(s"$path/fpjwashere", "".getBytes(StandardCharsets.UTF_8))
    }
    // Getting security option to determine how to verify ACLs.
    // Additionally, we create the consumers znode (not in
    // securePersistentZkPaths) to make sure that we don't
    // add ACLs to it.
    val secureOpt: String =
      if (secondZk.secure) {
        firstZk.createRecursive(ConsumerPathZNode.path)
        "secure"
      } else {
        secondZk.createRecursive(ConsumerPathZNode.path)
        "unsecure"
      }
    ZkSecurityMigrator.run(Array(s"--zookeeper.acl=$secureOpt", s"--zookeeper.connect=$zkUrl"))
    info("Done with migration")
    for (path <- ZkData.SecureRootPaths ++ ZkData.SensitiveRootPaths) {
      val sensitive = ZkData.sensitivePath(path)
      val listParent = secondZk.getAcl(path)
      assertTrue(isAclCorrect(listParent, secondZk.secure, sensitive), path)

      val childPath = path + "/fpjwashere"
      val listChild = secondZk.getAcl(childPath)
      assertTrue(isAclCorrect(listChild, secondZk.secure, sensitive), childPath)
    }
    // Check consumers path.
    val consumersAcl = firstZk.getAcl(ConsumerPathZNode.path)
    assertTrue(isAclCorrect(consumersAcl, false, false), ConsumerPathZNode.path)
    assertTrue(isAclCorrect(firstZk.getAcl("/kafka-acl-extended"), secondZk.secure,
      ZkData.sensitivePath(ExtendedAclZNode.path)), "/kafka-acl-extended")
    assertTrue(isAclCorrect(firstZk.getAcl("/feature"), secondZk.secure,
      ZkData.sensitivePath(FeatureZNode.path)), "ACL mismatch for /feature path")
  }

  /**
   * Verifies that the path has the appropriate secure ACL.
   */
  private def verify(path: String): Unit = {
    val sensitive = ZkData.sensitivePath(path)
    val list = zkClient.getAcl(path)
    assertTrue(list.forall(TestUtils.isAclSecure(_, sensitive)))
  }

  /**
   * Verifies ACL.
   */
  private def isAclCorrect(list: Seq[ACL], secure: Boolean, sensitive: Boolean): Boolean = {
    val isListSizeCorrect =
      if (secure && !sensitive)
        list.size == 2
      else
        list.size == 1
    isListSizeCorrect && list.forall(
      if (secure)
        TestUtils.isAclSecure(_, sensitive)
      else
        TestUtils.isAclUnsecure
    )
  }
  
  /**
   * Sets up and starts the recursive execution of deletes.
   * This is used in the testDelete and testDeleteRecursive
   * test cases.
   */
  private def deleteAllUnsecure(): Unit = {
    System.setProperty(JaasUtils.ZK_SASL_CLIENT, "false")
    val unsecureZkClient = newKafkaZkClient(zkConnect, isSecure = false)
    val result: Try[Boolean] = {
      deleteRecursive(unsecureZkClient, "/")
    }
    // Clean up before leaving the test case
    unsecureZkClient.close()
    System.clearProperty(JaasUtils.ZK_SASL_CLIENT)
    
    // Fail the test if able to delete
    result match {
      case Success(_) => // All done
      case Failure(e) => fail(e.getMessage)
    }
  }
  
  /**
   * Tries to delete znodes recursively
   */
  private def deleteRecursive(zkClient: KafkaZkClient, path: String): Try[Boolean] = {
    info(s"Deleting $path")
    var result: Try[Boolean] = Success(true)
    for (child <- zkClient.getChildren(path))
      result = (path match {
        case "/" => deleteRecursive(zkClient, s"/$child")
        case path => deleteRecursive(zkClient, s"$path/$child")
      }) match {
        case Success(_) => result
        case Failure(e) => Failure(e)
      }
    path match {
      // Do not try to delete the root
      case "/" => result
      // For all other paths, try to delete it
      case path =>
        try {
          zkClient.deletePath(path, recursiveDelete = false)
          Failure(new Exception(s"Have been able to delete $path"))
        } catch {
          case _: Exception => result
        }
    }
  }

  @Test
  def testConsumerOffsetPathAcls(): Unit = {
    zkClient.makeSurePersistentPathExists(ConsumerPathZNode.path)

    val consumerPathAcls = zkClient.currentZooKeeper.getACL(ConsumerPathZNode.path, new Stat())
    assertTrue(consumerPathAcls.asScala.forall(TestUtils.isAclUnsecure), "old consumer znode path acls are not open")
  }
}
