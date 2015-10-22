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
        var stat: Stat = new Stat;
        val aclList = zkUtils.zkConnection.getZookeeper.getACL(path, stat)
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
    assertTrue(zkUtils.isSecure)
    migration(true)
  }
  
  @Test
  def testZkAntiMigration() {
    assertTrue(!zkUtils.isSecure)
    migration(false)
  }
  
  private def migration(secure: Boolean) {
    info("zkConnect string: %s".format(zkConnect))
    val otherZkUtils = ZkUtils(zkConnect, 30000, 30000, !secure)
    for (path <- zkUtils.securePersistentZkPaths) {
      otherZkUtils.makeSurePersistentPathExists(path)
      // Create a child for each znode to exercise the recurrent
      // traversal of the data tree
      otherZkUtils.createPersistentPath(path + "/fpjwashere", "")
    }
    val goOpt: String = secure match {
      case true =>
        "secure"
      case false =>
        "unsecure"
    }
    ZkSecurityMigrator.run(Array("--kafka.go=%s --zookeeper.connect=%s".format(goOpt, zkConnect))) 
    for (path <- zkUtils.securePersistentZkPaths) {
      zkUtils.makeSurePersistentPathExists(path)
      if(!path.equals(ZkUtils.ConsumersPath)) {
        var stat: Stat = new Stat
        val listParent = zkUtils.zkConnection.getZookeeper.getACL(path, stat)
        assertTrue(listParent.size == 2)
        for (acl: ACL <- listParent.asScala) {
          if(secure)
            assertTrue(path, isAclSecure(acl))
          else
            assertTrue(path, isAclUnsecure(acl))
        }

        val childPath = path + "/fpjwashere"
        val listChild = zkUtils.zkConnection.getZookeeper.getACL(childPath, stat)
        assertTrue(listChild.size == 2)
        for (acl: ACL <- listChild.asScala) {
          if(secure)
            assertTrue(childPath, isAclSecure(acl))
          else
            assertTrue(childPath, isAclUnsecure(acl))
        }
      }
    }
  }

  def verify(path: String): Boolean = {
    var stat: Stat = new Stat;
    val list = zkUtils.zkConnection.getZookeeper.getACL(path, stat)
    var result: Boolean = true
    for (acl: ACL <- list.asScala) {
           result = result && isAclSecure(acl)
    }
    result
  }

  def isAclSecure(acl: ACL): Boolean = {
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
  
  def isAclUnsecure(acl: ACL): Boolean = {
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