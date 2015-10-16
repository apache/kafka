package unit.kafka.security.auth

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
        throw e
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
        val aclListEntry = zkUtils.zkConnection.getZookeeper.getACL(path, stat)
        assertTrue(aclListEntry.size == 2)
        for (acl: ACL <- aclListEntry.asScala) {
          info("Perms " + acl.getPerms + " and id" + acl.getId)
          acl.getPerms match {
            case 1 => {
              assertTrue(acl.getId.getScheme.equals("world"))
            }
            case 31 => {
              assertTrue(acl.getId.getScheme.equals("sasl"))
            }
            case _: Int => {
             fail("Unrecognized ID scheme %d".format(acl.getPerms)) 
            }
          }
        }
      }
    }
  }
}