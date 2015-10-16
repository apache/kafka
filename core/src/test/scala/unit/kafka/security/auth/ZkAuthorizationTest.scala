package unit.kafka.security.auth

import kafka.utils.{Logging, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.JaasUtils
import org.junit.Assert._
import org.junit.{After, Before, BeforeClass, Test}


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
    info("Finished setup")
  }
  
  @After
  override def tearDown() {
    super.tearDown()
    Thread.sleep(2)
  }
  
  /**
   * Tests the method in JaasUtils that checks whether to use 
   * secure ACLs and authentication with ZooKeeper.
   */
  @Test
  def testIsZkSecurityEnabled() {
    info("testIsZkSecurityEnabled")
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
        throw e
      }
    }
    info("testIsZkSecurityEnabled")
  }
  
  /**
   * Tests ZkUtils. The goal is mainly to verify that the behavior of ZkUtils is
   * correct when isSecure is set to true.
   */
  @Test
  def testZkUtils() {
    info("Verifying that the initialization has worked.")
    assertTrue(zkUtils.isSecure)
    info("Creating paths with secure acls")
    for (path <- zkUtils.persistentZkPaths) {
      info("Processing path: " + path)
      zkUtils.makeSurePersistentPathExists(path)
    }
    
    info("Check that non-authenticated clients can't modify the znodes")
    System.setProperty(JaasUtils.ZK_SASL_CLIENT, "false")
    val otherZkUtils = ZkUtils.apply(zkConnect, 30000, 30000, false)
    for (path <- otherZkUtils.persistentZkPaths) {
      try {
        otherZkUtils.zkClient.writeData(path, "")
        fail("Expected an exception")
      } catch {
        case e: Exception => {
          // Expected
        }
      }
    }
  }
}