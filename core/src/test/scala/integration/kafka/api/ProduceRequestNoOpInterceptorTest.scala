package integration.kafka.api

import kafka.utils.TestInfoUtils
import kafka.server.{KafkaConfig, ProduceRequestInterceptor, ProduceRequestInterceptorResult}
import org.apache.kafka.common.header.Header
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Properties

class NoOpProduceRequestInterceptor extends ProduceRequestInterceptor {

  override def processRecord(key: Array[Byte], value: Array[Byte], topic: String, partition: Int, headers: Array[Header]): ProduceRequestInterceptorResult = {
    val newValue = {
      if (value == null) null
      else value
    }
    ProduceRequestInterceptorResult(key, newValue)
  }

  override def configure(): Unit = ()
}

class ProduceRequestNoOpInterceptorTest extends ProducerSendTestHelpers {

  override def generateConfigs: collection.Seq[KafkaConfig] = {
    val overridingProps = new Properties()
    overridingProps.put(KafkaConfig.ProduceRequestInterceptorsProp, "integration.kafka.api.NoOpProduceRequestInterceptor")
    baseProps.map(KafkaConfig.fromProps(_, overridingProps))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testInterceptorDoesNothing(quorum: String): Unit = {
    sendToPartition(quorum)
  }


}
