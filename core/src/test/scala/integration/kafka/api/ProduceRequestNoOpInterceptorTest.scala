package integration.kafka.api

import kafka.utils.TestInfoUtils
import kafka.server.{KafkaConfig, ProduceRequestInterceptor}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Properties

class NoOpProduceRequestInterceptor extends ProduceRequestInterceptor {
  override def processKey(key: Array[Byte]): Array[Byte] = {
    if (key == null) null
    else key
  }

  override def processValue(value: Array[Byte]): Array[Byte] = {
    if (value == null) null
    else value
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
  def testSendToPartition(quorum: String): Unit = {
    sendToPartition(quorum)
  }


}
