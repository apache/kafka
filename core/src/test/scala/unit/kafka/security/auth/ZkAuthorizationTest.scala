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
   * Tests ZkUtils. The goal is mainly to verify that the behavior of ZkUtils is
   * correct when isSecure is set to true.
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

  @Test
  def testZkMigration() {
    val unsecureZkUtils = ZkUtils(zkConnect, 6000, 6000, false) 
    try {
      testMigration(unsecureZkUtils, zkUtils)
    } finally {
      unsecureZkUtils.close()
    }
  }

  @Test
  def testZkAntiMigration() {
    val unsecureZkUtils = ZkUtils(zkConnect, 6000, 6000, false)
    try {
      testMigration(zkUtils, unsecureZkUtils)
    } finally {
      unsecureZkUtils.close()
    }
  }
  
  @Test
  def testDelete() {
    info("zkConnect string: %s".format(zkConnect))
    for (path <- zkUtils.securePersistentZkPaths) {
      info("Creating " + path)
      zkUtils.makeSurePersistentPathExists(path)
    }
    System.setProperty(JaasUtils.ZK_SASL_CLIENT, "false")
    val unsecureZkUtils = ZkUtils(zkConnect, 6000, 6000, false)
    var failed = false
    for (path <- unsecureZkUtils.securePersistentZkPaths) {
      info("Deleting " + path)
      try {
        unsecureZkUtils.deletePath(path)
        info("Failed for path %s".format(path))
        failed = true
      } catch {
        case e: Exception => // Expected
      }
    }
    unsecureZkUtils.close()
    System.clearProperty(JaasUtils.ZK_SASL_CLIENT)
    if (failed)
      fail("Was able to delete at least one znode")
  }

  @Test
  def testDeleteRecursive() {
    info("zkConnect string: %s".format(zkConnect))
    for (path <- zkUtils.securePersistentZkPaths) {
      info("Creating " + path)
      zkUtils.makeSurePersistentPathExists(path)
      zkUtils.createPersistentPath(path + "/fpjwashere", "")
    }
    System.setProperty(JaasUtils.ZK_SASL_CLIENT, "false")
    val unsecureZkUtils = ZkUtils(zkConnect, 6000, 6000, false)
    for (path <- unsecureZkUtils.securePersistentZkPaths) {
      info("Deleting " + path)
      try {
        unsecureZkUtils.deletePath(path)
        fail("Should have thrown an exception: %s".format(path))
      } catch {
        case e: Exception => // Expected
      }
      try {
        unsecureZkUtils.deletePath(path + "/fpjwashere")
        fail("Should have thrown an exception: %s".format(path))
      } catch {
        case e: Exception => // Expected
      }
    }
    unsecureZkUtils.close()
    System.clearProperty(JaasUtils.ZK_SASL_CLIENT)
  }

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
      secondZk.makeSurePersistentPathExists(path)
      val listParent = (secondZk.zkConnection.getAcl(path)).getKey
      assertTrue(path, isAclCorrect(listParent, secondZk.isSecure))

      val childPath = path + "/fpjwashere"
      val listChild = (secondZk.zkConnection.getAcl(childPath)).getKey
      assertTrue(childPath, isAclCorrect(listChild, secondZk.isSecure))
    }
  }

  private def verify(path: String): Boolean = {
    val list = (zkUtils.zkConnection.getAcl(path)).getKey
    var result: Boolean = true
    for (acl: ACL <- list.asScala) {
           result = result && isAclSecure(acl)
    }
    result
  }

  private def isAclCorrect(list: java.util.List[ACL], secure: Boolean): Boolean = {
    var flag = true
    // Check length
    secure match {
      case true => flag = flag && list.size == 2
      case false => flag = flag && list.size == 1
    }
    
    // Check ACL
    for (acl: ACL <- list.asScala) {
      secure match {
        case true => flag = flag && isAclSecure(acl)
        case false => flag = flag && isAclUnsecure(acl)
      }
    }
    flag
  }
  
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
}