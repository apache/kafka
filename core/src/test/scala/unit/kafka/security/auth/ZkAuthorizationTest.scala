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
import kafka.utils.{Logging, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.JaasUtils
import org.apache.zookeeper.data.{ACL, Stat}
import org.junit.Assert._
import org.junit.{After, Before, BeforeClass, Test}
import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}


class ZkAuthorizationTest extends ZooKeeperTestHarness with Logging{
  val jaasFile: String = "zk-digest-jaas.conf"
  val authProvider: String = "zookeeper.authProvider.1"
  @Before
  override def setUp() {
    val classLoader = getClass.getClassLoader
    val filePath = classLoader.getResource(jaasFile).getPath
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, filePath)
    System.setProperty(authProvider, "org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
    super.setUp()
  }

  @After
  override def tearDown() {
    super.tearDown()
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    System.clearProperty(authProvider)
  }

  /**
   * Tests the method in JaasUtils that checks whether to use
   * secure ACLs and authentication with ZooKeeper.
   */
  @Test
  def testIsZkSecurityEnabled() {
    assertTrue(JaasUtils.isZkSecurityEnabled(System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)))
    assertFalse(JaasUtils.isZkSecurityEnabled(""))
    try {
      JaasUtils.isZkSecurityEnabled("no-such-file-exists.conf")
      fail("Should have thrown an exception")
    } catch {
      case e: KafkaException => {
        // Expected
      }
      case e: Exception => {
        fail(e.toString)
      }
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
        val aclList = (zkUtils.zkConnection.getAcl(path)).getKey
        assertTrue(aclList.size == 2)
        for (acl: ACL <- aclList.asScala) {
          assertTrue(isAclSecure(acl))
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
      testMigration(unsecureZkUtils, zkUtils)
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
      testMigration(zkUtils, unsecureZkUtils)
    } finally {
      unsecureZkUtils.close()
    }
  }

  /**
   * Tests that the persistent paths cannot be deleted.
   */
  @Test
  def testDelete() {
    info("zkConnect string: %s".format(zkConnect))
    ZkSecurityMigrator.run(Array("--zookeeper.acl=secure", "--zookeeper.connect=%s".format(zkConnect)))
    deleteAllUnsecure()
  }

  /**
   * Tests that znodes cannot be deleted when the 
   * persistent paths have children.
   */
  @Test
  def testDeleteRecursive() {
    info("zkConnect string: %s".format(zkConnect))
    for (path <- zkUtils.securePersistentZkPaths) {
      info("Creating " + path)
      zkUtils.makeSurePersistentPathExists(path)
      zkUtils.createPersistentPath(path + "/fpjwashere", "")
    }
    zkUtils.zkConnection.setAcl("/", zkUtils.DefaultAcls, -1)
    deleteAllUnsecure()
  }
  
  /**
   * Tests the migration tool when chroot is being used.
   */
  @Test
  def testChroot {
    zkUtils.createPersistentPath("/kafka")
    val unsecureZkUtils = ZkUtils(zkConnect + "/kafka", 6000, 6000, false)
    val secureZkUtils = ZkUtils(zkConnect + "/kafka", 6000, 6000, true)
    try {
      testMigration(unsecureZkUtils, secureZkUtils)
    } finally {
      unsecureZkUtils.close()
      secureZkUtils.close()
    }
  }

  /**
   * Exercises the migration tool. It is used by two test cases:
   * testZkMigration and testZkAntiMigration.
   */
  private def testMigration(firstZk: ZkUtils, secondZk: ZkUtils) {
    info("zkConnect string: %s".format(zkConnect))
    for (path <- firstZk.securePersistentZkPaths) {
      info("Creating " + path)
      firstZk.makeSurePersistentPathExists(path)
      // Create a child for each znode to exercise the recurrent
      // traversal of the data tree
      firstZk.createPersistentPath(path + "/fpjwashere", "")
    }
    val secureOpt: String  = secondZk.isSecure match {
      case true => "secure"
      case false => "unsecure"
    }
    ZkSecurityMigrator.run(Array("--zookeeper.acl=%s".format(secureOpt), "--zookeeper.connect=%s".format(zkConnect)))
    info("Done with migration")
    for (path <- secondZk.securePersistentZkPaths) {
      val listParent = (secondZk.zkConnection.getAcl(path)).getKey
      assertTrue(path, isAclCorrect(listParent, secondZk.isSecure))

      val childPath = path + "/fpjwashere"
      val listChild = (secondZk.zkConnection.getAcl(childPath)).getKey
      assertTrue(childPath, isAclCorrect(listChild, secondZk.isSecure))
    }
  }

  /**
   * Verifies that the path has the appropriate secure ACL.
   */
  private def verify(path: String): Boolean = {
    val list = (zkUtils.zkConnection.getAcl(path)).getKey
    list.asScala.forall(isAclSecure)
  }

  /**
   * Verifies ACL.
   */
  private def isAclCorrect(list: java.util.List[ACL], secure: Boolean): Boolean = {
    val isListSizeCorrect = secure match {
      case true => list.size == 2
      case false => list.size == 1
    } 
    isListSizeCorrect && list.asScala.forall(
        secure match {
          case true => isAclSecure
          case false => isAclUnsecure
        })
  }
  
  /**
   * Verifies that this ACL is the secure one. The
   * values are based on the constants used in the 
   * ZooKeeper code base.
   */
  private def isAclSecure(acl: ACL): Boolean = {
    info("Perms " + acl.getPerms + " and id" + acl.getId)
    acl.getPerms match {
      case 1 => {
        acl.getId.getScheme.equals("world")
      }
      case 31 => {
        acl.getId.getScheme.equals("sasl")
      }
      case _: Int => {
        false
      }
    }
  }
  
  /**
   * Verifies that the ACL corresponds to the unsecure one.
   */
  private def isAclUnsecure(acl: ACL): Boolean = {
    info("Perms " + acl.getPerms + " and id" + acl.getId)
    acl.getPerms match {
      case 31 => {
        acl.getId.getScheme.equals("world")
      }
      case _: Int => {
        false
      }
    }
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
      case Success(v) => // All done
      case Failure(e) => fail(e.getMessage)
    }
  }
  
  /**
   * Tries to delete znodes recursively
   */
  private def deleteRecursive(zkUtils: ZkUtils, path: String): Try[Boolean] = {
    info("Deleting " + path)
    var result: Try[Boolean] = Success(true)
    for (child <- zkUtils.getChildren(path))
      result = (path match {
        case "/" => deleteRecursive(zkUtils, s"/$child")
        case path => deleteRecursive(zkUtils, s"$path/$child")
      }) match {
        case Success(v) => result
        case Failure(e) => Failure(e)
      }
    path match {
      // Do not try to delete the root
      case "/" => result
      // For all other paths, try to delete it
      case path =>
        try{
          zkUtils.deletePath(path)
          Failure(new Exception(s"Have been able to delete $path"))
        } catch {
          case e: Exception => result
        }
    }
  }
}