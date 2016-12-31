package kafka.tools

import java.util.Properties

import kafka.consumer.ConsumerTimeoutException
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.tools.MirrorMaker.{MirrorMakerNewConsumer, MirrorMakerProducer}
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.junit.Test

class MirrorMakerIntegrationTest extends KafkaServerTestHarness {

  override def generateConfigs(): Seq[KafkaConfig] = TestUtils.createBrokerConfigs(1, zkConnect)
    .map(KafkaConfig.fromProps(_, new Properties()))

  @Test
  def testRegularExpressionTopic() {
    val topic = "new-topic"
    val msg = "a test message"
    val brokerList = TestUtils.getBrokerListStrFromServers(servers)

    // Create a test producer to delivery a message
    val producerProps = new Properties()
    producerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put("producer.type", "sync")
    producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    val producer = new MirrorMakerProducer(producerProps)
    MirrorMaker.producer = producer
    MirrorMaker.producer.send(new ProducerRecord(topic, msg.getBytes()))
    MirrorMaker.producer.close()

    // Create a MirrorMaker consumer
    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val consumer = new KafkaConsumer(consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer())

    val whitelist = Some("new.*,another_topic,foo")
    val mirrorMakerConsumer = new MirrorMakerNewConsumer(consumer, None, whitelist)
    mirrorMakerConsumer.init()
    try {
      TestUtils.waitUntilTrue(() => {
        try {
          val data = mirrorMakerConsumer.receive()
          data.topic == topic && new String(data.value) == msg
        } catch {
          case _: ConsumerTimeoutException => false
        }
      }, "MirrorMaker consumer should be able to subscribe the correct topic and read the correct message.")
    } finally {
      consumer.close()
    }
  }

}