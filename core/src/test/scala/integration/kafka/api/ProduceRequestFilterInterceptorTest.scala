package integration.kafka.api

import kafka.server.KafkaConfig
import kafka.utils.TestInfoUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.errors.ProduceRequestInterceptorSkipRecordException
import org.apache.kafka.common.header.Header
import org.apache.kafka.server.interceptors.{ProduceRequestInterceptor, ProduceRequestInterceptorResult}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.nio.charset.StandardCharsets
import java.util.Properties

class EvenNumberFilterProduceRequestInterceptor extends ProduceRequestInterceptor {

  override def processRecord(key: Array[Byte], value: Array[Byte], topic: String, partition: Int, headers: Array[Header]): ProduceRequestInterceptorResult = {
    val newValue = {
      if (value == null) null
      else {
        // Only keep even numbered records, and discard odds
        val s = new String(value, StandardCharsets.UTF_8)
        if (s.drop("value".length).toInt % 2 == 0) value
        else throw new ProduceRequestInterceptorSkipRecordException("Filtering out odd numbered values")
      }
    }
    new ProduceRequestInterceptorResult(key, newValue)
  }

  override def configure(): Unit = ()
}

class ProduceRequestFilterInterceptorTest extends ProducerSendTestHelpers {

  override def generateConfigs: collection.Seq[KafkaConfig] = {
    val overridingProps = new Properties()
    overridingProps.put(KafkaConfig.ProduceRequestInterceptorsProp, "integration.kafka.api.EvenNumberFilterProduceRequestInterceptor")
    baseProps.map(KafkaConfig.fromProps(_, overridingProps))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testInterceptorFiltersMessages(quorum: String): Unit = {
    val recordAssertion: (ConsumerRecord[Array[Byte], Array[Byte]], Int, Long, String, Int) => Unit = (record, i, now, topic, partition) => {
      assertEquals(topic, record.topic)
      assertEquals(partition, record.partition)
      assertEquals(i.toLong, record.offset)
      assertNull(record.key)
      assertEquals(s"value${(i + 1) * 2}", new String(record.value))
      assertEquals(now, record.timestamp)
    }
    sendToPartition(quorum, 50, recordAssertion)
  }

}
