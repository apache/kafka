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

package kafka.zk

import java.util.ArrayList
import java.util.Collection
import javax.security.auth.login.Configuration

import kafka.consumer.ConsumerConfig
import kafka.utils.ZkUtils
import kafka.utils.ZKCheckedEphemeral
import kafka.utils.TestUtils
import org.apache.kafka.common.security.JaasUtils
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooDefs.Ids
import org.I0Itec.zkclient.exception.{ZkException,ZkNodeExistsException}
import org.junit.{After, Before, Test, Assert}
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;

object ZKEphemeralTest {
  @Parameters
  def enableSecurityOptions: Collection[Array[java.lang.Boolean]] = {
    val list = new ArrayList[Array[java.lang.Boolean]]()
    list.add(Array(true))
    list.add(Array(false))
    list
  }
}

@RunWith(value = classOf[Parameterized])
class ZKEphemeralTest(val secure: Boolean) extends ZooKeeperTestHarness {
  val jaasFile: String = kafka.utils.JaasTestUtils.genZkFile
  val authProvider: String = "zookeeper.authProvider.1"
  var zkSessionTimeoutMs = 1000
  
  @Before
  override def setUp() {
    if(secure) {
      Configuration.setConfiguration(null)
      System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasFile)
      System.setProperty(authProvider, "org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
      if(!JaasUtils.isZkSecurityEnabled()) {
        fail("Secure access not enabled")
     }
    }
    super.setUp
  }
  
  @After
  override def tearDown() {
    super.tearDown
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    System.clearProperty(authProvider)
    Configuration.setConfiguration(null)
  }
  
  @Test
  def testEphemeralNodeCleanup = {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"))
    var zkUtils = ZkUtils(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs, JaasUtils.isZkSecurityEnabled())

    try {
      zkUtils.createEphemeralPathExpectConflict("/tmp/zktest", "node created")
    } catch {                       
      case e: Exception =>
    }

    var testData: String = null
    testData = zkUtils.readData("/tmp/zktest")._1
    Assert.assertNotNull(testData)
    zkUtils.close
    zkUtils = ZkUtils(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs, JaasUtils.isZkSecurityEnabled())
    val nodeExists = zkUtils.pathExists("/tmp/zktest")
    Assert.assertFalse(nodeExists)
  }

  /*****
   ***** Tests for ZkWatchedEphemeral
   *****/
  
  /**
   * Tests basic creation
   */
  @Test
  def testZkWatchedEphemeral = {
    var path = "/zwe-test"
    testCreation(path)
    path = "/zwe-test-parent/zwe-test"
    testCreation(path)
  }
 
  private def testCreation(path: String) {
    val zk = zkUtils.zkConnection.getZookeeper
    val zwe = new ZKCheckedEphemeral(path, "", zk, JaasUtils.isZkSecurityEnabled())
    var created = false
    var counter = 10

    zk.exists(path, new Watcher() {
      def process(event: WatchedEvent) {
        if(event.getType == Watcher.Event.EventType.NodeCreated) {
          created = true
        }
      }
    })
    zwe.create()
    // Waits until the znode is created
    TestUtils.waitUntilTrue(() => zkUtils.pathExists(path),
                            s"Znode $path wasn't created")
  }

  /**
   * Tests that it fails in the presence of an overlapping
   * session.
   */
  @Test
  def testOverlappingSessions = {
    val path = "/zwe-test"
    val zk1 = zkUtils.zkConnection.getZookeeper

    //Creates a second session
    val (_, zkConnection2) = ZkUtils.createZkClientAndConnection(zkConnect, zkSessionTimeoutMs, zkConnectionTimeout)
    val zk2 = zkConnection2.getZookeeper
    var zwe = new ZKCheckedEphemeral(path, "", zk2, JaasUtils.isZkSecurityEnabled())

    // Creates znode for path in the first session
    zk1.create(path, Array[Byte](), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    
    //Bootstraps the ZKWatchedEphemeral object
    var gotException = false;
    try {
      zwe.create()
    } catch {
      case e: ZkNodeExistsException =>
        gotException = true
    }
    Assert.assertTrue(gotException)
  }
  
  /**
   * Tests if succeeds with znode from the same session
   * 
   */
  @Test
  def testSameSession = {
    val path = "/zwe-test"
    val zk = zkUtils.zkConnection.getZookeeper
    // Creates znode for path in the first session
    zk.create(path, Array[Byte](), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    
    var zwe = new ZKCheckedEphemeral(path, "", zk, JaasUtils.isZkSecurityEnabled())
    //Bootstraps the ZKWatchedEphemeral object
    var gotException = false;
    try {
      zwe.create()
    } catch {
      case e: ZkNodeExistsException =>
        gotException = true
    }
    Assert.assertFalse(gotException)
  }
}
