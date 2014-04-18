package kafka.server

import junit.framework.Assert._
import java.util.Properties
import java.io.File
import org.junit.{After, Before, Test}
import kafka.integration.KafkaServerTestHarness
import kafka.utils._
import kafka.common._
import kafka.log.LogConfig
import kafka.admin.AdminUtils
import org.scalatest.junit.JUnit3Suite

class DynamicConfigChangeTest extends JUnit3Suite with KafkaServerTestHarness {
  
  override val configs = List(new KafkaConfig(TestUtils.createBrokerConfig(0, TestUtils.choosePort)))

  @Test
  def testConfigChange() {
    val oldVal = 100000
    val newVal = 200000
    val tp = TopicAndPartition("test", 0)
    AdminUtils.createTopic(zkClient, tp.topic, 1, 1, LogConfig(flushInterval = oldVal).toProps)
    TestUtils.retry(10000) {
      val logOpt = this.servers(0).logManager.getLog(tp)
      assertTrue(logOpt.isDefined)
      assertEquals(oldVal, logOpt.get.config.flushInterval)
    }
    AdminUtils.changeTopicConfig(zkClient, tp.topic, LogConfig(flushInterval = newVal).toProps)
    TestUtils.retry(10000) {
      assertEquals(newVal, this.servers(0).logManager.getLog(tp).get.config.flushInterval)
    }
  }

}