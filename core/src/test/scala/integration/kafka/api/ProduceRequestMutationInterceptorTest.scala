package integration.kafka.api

import kafka.server.{KafkaConfig, ProduceRequestInterceptor}
import kafka.utils.TestInfoUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.nio.charset.StandardCharsets
import java.util.Properties

class MutationProduceRequestInterceptor extends ProduceRequestInterceptor {
  override def processKey(key: Array[Byte]): Array[Byte] = {
    if (key == null) null
    else key
  }

  override def processValue(value: Array[Byte]): Array[Byte] = {
    if (value == null) null
    else {
      val s = new String(value, StandardCharsets.UTF_8)
      s"mutated-$s".getBytes(StandardCharsets.UTF_8)
    }
  }

  override def configure(): Unit = ()
}

class ProduceRequestMutationInterceptorTest extends ProducerSendTestHelpers {

  override def generateConfigs: collection.Seq[KafkaConfig] = {
    val overridingProps = new Properties()
    overridingProps.put(KafkaConfig.ProduceRequestInterceptorsProp, "integration.kafka.api.MutationProduceRequestInterceptor")
    baseProps.map(KafkaConfig.fromProps(_, overridingProps))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testSendToPartition(quorum: String): Unit = {
    val recordAssertion: (ConsumerRecord[Array[Byte], Array[Byte]], Int, Long, String, Int) => Unit = (record, i, now, topic, partition) => {
      assertEquals(topic, record.topic)
      assertEquals(partition, record.partition)
      assertEquals(i.toLong, record.offset)
      assertNull(record.key)
      assertEquals(s"mutated-value${i + 1}", new String(record.value))
      assertEquals(now, record.timestamp)
    }
    sendToPartition(quorum, numRecords, recordAssertion)
  }

}
