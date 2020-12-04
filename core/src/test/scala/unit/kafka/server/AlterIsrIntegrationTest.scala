package unit.kafka.server

import kafka.server.{DefaultAlterIsrManager, KafkaConfig, KafkaServer, ZkIsrManager}
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.junit.Assert.assertEquals
import org.junit.{After, Test}

class AlterIsrIntegrationTest extends ZooKeeperTestHarness {
  private var server: KafkaServer = null

  @After
  override def tearDown(): Unit = {
    if (server != null)
      TestUtils.shutdownServers(Seq(server))
    super.tearDown()
  }

  @Test
  def testIsrManager2_7_IV1(): Unit = {
    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.setProperty("inter.broker.protocol.version", "2.7-IV1")
    server = TestUtils.createServer(KafkaConfig.fromProps(props))
    assertEquals(classOf[ZkIsrManager], server.replicaManager.alterIsrManager.getClass)
  }

  @Test
  def testIsrManager2_7_IV2(): Unit = {
    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.setProperty("inter.broker.protocol.version", "2.7-IV2")
    server = TestUtils.createServer(KafkaConfig.fromProps(props))
    assertEquals(classOf[DefaultAlterIsrManager], server.replicaManager.alterIsrManager.getClass)
  }
}
