package kafka.tools

import java.util.Properties

import kafka.consumer.{OldConsumer, Whitelist}
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{TestUtils, ZkUtils}
import org.junit.Assert.assertTrue
import org.junit.Test


class ConsoleConsumerIntegrationTest extends KafkaServerTestHarness {
  override def generateConfigs(): Seq[KafkaConfig] = TestUtils.createBrokerConfigs(1, zkConnect)
    .map(KafkaConfig.fromProps(_, new Properties()))

  @Test
  def testZkNodeDeleteForOldConsumerWithUnspecifiedGroupID() {
    val topic = "test-topic"
    val consumerArgs: Array[String] = Array(
      "--zookeeper", this.zkConnect,
      "--topic", topic,
      "--from-beginning")
    val conf = new ConsoleConsumer.ConsumerConfig(consumerArgs)
    val consumer = new OldConsumer(Whitelist(topic), ConsoleConsumer.getOldConsumerProps(conf))
    val groupID = conf.consumerProps.get("group.id")
    try {
      assertTrue("Consumer group should be created.", zkUtils.getChildren(ZkUtils.ConsumersPath).head == groupID)
    } finally {
      consumer.stop()
      ConsoleConsumer.deleteZkPathForConsumerGroup(conf.options.valueOf(conf.zkConnectOpt), conf.consumerProps.getProperty("group.id"))
    }
    assertTrue("The zk node for this group should be deleted.", zkUtils.getChildren(ZkUtils.ConsumersPath).isEmpty)
  }
}
