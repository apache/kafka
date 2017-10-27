package integration.kafka.tools

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util.Properties

import kafka.integration.KafkaServerTestHarness
import kafka.log.Log
import kafka.server.KafkaConfig
import kafka.tools.DumpLogSegments
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.junit.Assert.assertTrue
import org.junit.Test

class DumpLogSegmentsIntegrationTest extends KafkaServerTestHarness {

  override def generateConfigs: Seq[KafkaConfig] = TestUtils.createBrokerConfigs(1, zkConnect)
    .map(KafkaConfig.fromProps(_, new Properties()))

  @Test
  def testOutputForJustSent3MessagesLogDump(): Unit = {
    val topic = "new-topic"
    val msg = "a test message"
    val producerProps = new Properties
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestUtils.getBrokerListStrFromServers(servers))
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

    // send 3 messages
    producer.send(new ProducerRecord(topic, msg.getBytes()))
    producer.send(new ProducerRecord(topic, msg.getBytes()))
    producer.send(new ProducerRecord(topic, msg.getBytes()))
    producer.close()

    val log: Log = servers.head.logManager.logsByTopic(topic).head

    val baos = new ByteArrayOutputStream()
    val out = new PrintStream(baos)
    val old = Console.out
    val err = Console.err
    try {
      old.flush()
      err.flush()
      // redirect output from console
      Console.setOut(out)
      Console.setErr(out)
      DumpLogSegments.main(
        Seq(
          "--files", log.dir.listFiles.filter(_.getName.endsWith(".log")).head.getCanonicalPath,
          "--deep-iteration"
        ).toArray
      )
      out.flush()
    } finally {
      Console.setOut(old)
      Console.setErr(err)
    }

    assertTrue(baos.toString.contains("Starting offset: 0"))
    assertTrue(baos.toString.contains("offset: 0"))
    assertTrue(baos.toString.contains("offset: 1"))
    assertTrue(baos.toString.contains("offset: 2"))
  }
}
