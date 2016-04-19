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

import java.lang.Iterable
import javax.security.auth.login.Configuration

import scala.collection.JavaConverters._

import kafka.consumer.ConsumerConfig
import kafka.utils.ZkUtils
import kafka.utils.ZKCheckedEphemeral
import kafka.utils.TestUtils
import org.apache.kafka.common.security.JaasUtils
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooDefs.Ids
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.junit.{After, Before, Test, Assert}
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.junit.runner.RunWith

object ZKEphemeralTest {

  @Parameters
  def enableSecurityOptions: Iterable[Array[java.lang.Boolean]] =
    Seq[Array[java.lang.Boolean]](Array(true), Array(false)).asJava

}

@RunWith(value = classOf[Parameterized])
class ZKEphemeralTest(val secure: Boolean) extends ZooKeeperTestHarness {
  val jaasFile = kafka.utils.JaasTestUtils.writeZkFile()
  val authProvider = "zookeeper.authProvider.1"
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
    zkUtils.close()
  }

  /*****
   ***** Tests for ZkWatchedEphemeral
   *****/
  
  /**
   * Tests basic creation
   */
  @Test
  def testZkWatchedEphemeral = {
    testCreation("/zwe-test")
    testCreation("/zwe-test-parent/zwe-test")
  }
 
  private def testCreation(path: String) {
    val zk = zkUtils.zkConnection.getZookeeper
    val zwe = new ZKCheckedEphemeral(path, "", zk, JaasUtils.isZkSecurityEnabled())
    var created = false

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
    val (zkClient2, zkConnection2) = ZkUtils.createZkClientAndConnection(zkConnect, zkSessionTimeoutMs, zkConnectionTimeout)
    val zk2 = zkConnection2.getZookeeper
    val zwe = new ZKCheckedEphemeral(path, "", zk2, JaasUtils.isZkSecurityEnabled())

    // Creates znode for path in the first session
    zk1.create(path, Array[Byte](), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    
    //Bootstraps the ZKWatchedEphemeral object
    val gotException =
      try {
        zwe.create()
        false
      } catch {
        case e: ZkNodeExistsException => true
      }
    Assert.assertTrue(gotException)
    zkClient2.close()
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
    
    val zwe = new ZKCheckedEphemeral(path, "", zk, JaasUtils.isZkSecurityEnabled())
    //Bootstraps the ZKWatchedEphemeral object
    val gotException =
      try {
        zwe.create()
        false
      } catch {
        case e: ZkNodeExistsException => true
      }
    Assert.assertFalse(gotException)
  }
}
