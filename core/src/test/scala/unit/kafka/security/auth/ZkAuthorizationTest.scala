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

import kafka.admin.ZkSecurityMigrator
import kafka.utils.{Logging, TestUtils, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.security.JaasUtils
import org.apache.zookeeper.data.{ACL}
import org.junit.Assert._
import org.junit.{After, Before, Test}
import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}
import javax.security.auth.login.Configuration

class ZkAuthorizationTest extends ZooKeeperTestHarness with Logging {
  val jaasFile = kafka.utils.JaasTestUtils.writeJaasContextsToFile(kafka.utils.JaasTestUtils.zkSections)
  val authProvider = "zookeeper.authProvider.1"

  @Before
  override def setUp() {
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasFile.getAbsolutePath)
    Configuration.setConfiguration(null)
    System.setProperty(authProvider, "org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
    super.setUp()
  }

  @After
  override def tearDown() {
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
  def testIsZkSecurityEnabled() {
    assertTrue(JaasUtils.isZkSecurityEnabled())
    Configuration.setConfiguration(null)
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    assertFalse(JaasUtils.isZkSecurityEnabled())
    try {
      Configuration.setConfiguration(null)
      System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, "no-such-file-exists.conf")
      JaasUtils.isZkSecurityEnabled()
      fail("Should have thrown an exception")
    } catch {
      case _: KafkaException => // Expected
    }
  }

  /**
   * Exercises the code in ZkUtils. The goal is mainly
   * to verify that the behavior of ZkUtils is correct
   * when isSecure is set to true.
   */
  @Test
  def testZkUtils() {
    assertTrue(zkUtils.isSecure)
    for (path <- zkUtils.persistentZkPaths) {
      zkUtils.makeSurePersistentPathExists(path)
      if(!path.equals(ZkUtils.ConsumersPath)) {
        val aclList = zkUtils.zkConnection.getAcl(path).getKey
        assertTrue(aclList.size == 2)
        for (acl: ACL <- aclList.asScala) {
          assertTrue(TestUtils.isAclSecure(acl, false))
        }
      }
    }
    // Test that can create: createEphemeralPathExpectConflict
    zkUtils.createEphemeralPathExpectConflict("/a", "")
    verify("/a")
    // Test that can create: createPersistentPath
    zkUtils.createPersistentPath("/b")
    verify("/b")
    // Test that can create: createSequentialPersistentPath
    val seqPath = zkUtils.createSequentialPersistentPath("/c", "")
    verify(seqPath)
    // Test that can update: updateEphemeralPath
    zkUtils.updateEphemeralPath("/a", "updated")
    val valueA: String = zkUtils.zkClient.readData("/a")
    assertTrue(valueA.equals("updated"))
    // Test that can update: updatePersistentPath
    zkUtils.updatePersistentPath("/b", "updated")
    val valueB: String = zkUtils.zkClient.readData("/b")
    assertTrue(valueB.equals("updated"))

    info("Leaving testZkUtils")
  }

  /**
   * Tests the migration tool when making an unsecure
   * cluster secure.
   */
  @Test
  def testZkMigration() {
    val unsecureZkUtils = ZkUtils(zkConnect, 6000, 6000, false) 
    try {
      testMigration(zkConnect, unsecureZkUtils, zkUtils)
    } finally {
      unsecureZkUtils.close()
    }
  }

  /**
   * Tests the migration tool when making a secure
   * cluster unsecure.
   */
  @Test
  def testZkAntiMigration() {
    val unsecureZkUtils = ZkUtils(zkConnect, 6000, 6000, false)
    try {
      testMigration(zkConnect, zkUtils, unsecureZkUtils)
    } finally {
      unsecureZkUtils.close()
    }
  }

  /**
   * Tests that the persistent paths cannot be deleted.
   */
  @Test
  def testDelete() {
    info(s"zkConnect string: $zkConnect")
    ZkSecurityMigrator.run(Array("--zookeeper.acl=secure", s"--zookeeper.connect=$zkConnect"))
    deleteAllUnsecure()
  }

  /**
   * Tests that znodes cannot be deleted when the 
   * persistent paths have children.
   */
  @Test
  def testDeleteRecursive() {
    info(s"zkConnect string: $zkConnect")
    for (path <- ZkUtils.SecureZkRootPaths) {
      info(s"Creating $path")
      zkUtils.makeSurePersistentPathExists(path)
      zkUtils.createPersistentPath(s"$path/fpjwashere", "")
    }
    zkUtils.zkConnection.setAcl("/", zkUtils.defaultAcls("/"), -1)
    deleteAllUnsecure()
  }
  
  /**
   * Tests the migration tool when chroot is being used.
   */
  @Test
  def testChroot(): Unit = {
    val zkUrl = zkConnect + "/kafka"
    zkUtils.createPersistentPath("/kafka")
    val unsecureZkUtils = ZkUtils(zkUrl, 6000, 6000, false)
    val secureZkUtils = ZkUtils(zkUrl, 6000, 6000, true)
    try {
      testMigration(zkUrl, unsecureZkUtils, secureZkUtils)
    } finally {
      unsecureZkUtils.close()
      secureZkUtils.close()
    }
  }

  /**
   * Exercises the migration tool. It is used in these test cases:
   * testZkMigration, testZkAntiMigration, testChroot.
   */
  private def testMigration(zkUrl: String, firstZk: ZkUtils, secondZk: ZkUtils) {
    info(s"zkConnect string: $zkUrl")
    for (path <- ZkUtils.SecureZkRootPaths ++ ZkUtils.SensitiveZkRootPaths) {
      info(s"Creating $path")
      firstZk.makeSurePersistentPathExists(path)
      // Create a child for each znode to exercise the recurrent
      // traversal of the data tree
      firstZk.createPersistentPath(s"$path/fpjwashere", "")
    }
    // Getting security option to determine how to verify ACLs.
    // Additionally, we create the consumers znode (not in
    // securePersistentZkPaths) to make sure that we don't
    // add ACLs to it.
    val secureOpt: String =
      if (secondZk.isSecure) {
        firstZk.createPersistentPath(ZkUtils.ConsumersPath)
        "secure"
      } else {
        secondZk.createPersistentPath(ZkUtils.ConsumersPath)
        "unsecure"
      }
    ZkSecurityMigrator.run(Array(s"--zookeeper.acl=$secureOpt", s"--zookeeper.connect=$zkUrl"))
    info("Done with migration")
    for (path <- ZkUtils.SecureZkRootPaths ++ ZkUtils.SensitiveZkRootPaths) {
      val sensitive = ZkUtils.sensitivePath(path)
      val listParent = secondZk.zkConnection.getAcl(path).getKey
      assertTrue(path, isAclCorrect(listParent, secondZk.isSecure, sensitive))

      val childPath = path + "/fpjwashere"
      val listChild = secondZk.zkConnection.getAcl(childPath).getKey
      assertTrue(childPath, isAclCorrect(listChild, secondZk.isSecure, sensitive))
    }
    // Check consumers path.
    val consumersAcl = firstZk.zkConnection.getAcl(ZkUtils.ConsumersPath).getKey
    assertTrue(ZkUtils.ConsumersPath, isAclCorrect(consumersAcl, false, false))
  }

  /**
   * Verifies that the path has the appropriate secure ACL.
   */
  private def verify(path: String): Boolean = {
    val sensitive = ZkUtils.sensitivePath(path)
    val list = zkUtils.zkConnection.getAcl(path).getKey
    list.asScala.forall(TestUtils.isAclSecure(_, sensitive))
  }

  /**
   * Verifies ACL.
   */
  private def isAclCorrect(list: java.util.List[ACL], secure: Boolean, sensitive: Boolean): Boolean = {
    val isListSizeCorrect =
      if (secure && !sensitive)
        list.size == 2
      else
        list.size == 1
    isListSizeCorrect && list.asScala.forall(
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
  private def deleteAllUnsecure() {
    System.setProperty(JaasUtils.ZK_SASL_CLIENT, "false")
    val unsecureZkUtils = ZkUtils(zkConnect, 6000, 6000, false)
    val result: Try[Boolean] = {
      deleteRecursive(unsecureZkUtils, "/")
    }
    // Clean up before leaving the test case
    unsecureZkUtils.close()
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
  private def deleteRecursive(zkUtils: ZkUtils, path: String): Try[Boolean] = {
    info(s"Deleting $path")
    var result: Try[Boolean] = Success(true)
    for (child <- zkUtils.getChildren(path))
      result = (path match {
        case "/" => deleteRecursive(zkUtils, s"/$child")
        case path => deleteRecursive(zkUtils, s"$path/$child")
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
          zkUtils.deletePath(path)
          Failure(new Exception(s"Have been able to delete $path"))
        } catch {
          case _: Exception => result
        }
    }
  }
}
