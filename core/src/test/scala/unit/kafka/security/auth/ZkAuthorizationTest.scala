package unit.kafka.security.auth

import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.security.JaasUtils
import org.junit.Assert._
import org.junit.{Before, Test}

class ZkAuthorizationTest extends ZooKeeperTestHarness {
  val jaasFile: String = "./resources/zk-digest-jaas.conf"
  
  @Test
  def testIsZkSecurityEnabled() {
    assertTrue(JaasUtils.isZkSecurityEnabled(jaasFile))
    assertFalse(JaasUtils.isZkSecurityEnabled(jaasFile))
  }
}