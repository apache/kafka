package unit.kafka.security.auth

import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.JaasUtils
import org.junit.Assert._
import org.junit.{Before, Test}

class ZkAuthorizationTest extends ZooKeeperTestHarness {
  val jaasFile: String = "zk-digest-jaas.conf"
  
  @Before
  override def setUp() {
    val classLoader = getClass.getClassLoader
    val filePath = classLoader.getResource(jaasFile).getPath
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, filePath)
  }
  
  @Test
  def testIsZkSecurityEnabled() {
    assertTrue(JaasUtils.isZkSecurityEnabled(System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)))
    assertFalse(JaasUtils.isZkSecurityEnabled(""))
    try {
      assertFalse(JaasUtils.isZkSecurityEnabled("no-such-file-exists.conf"))
    } catch {
      case e: KafkaException => {
        // Expected
      }
      case e: Exception => {
        throw e
      }
    }
  }
}