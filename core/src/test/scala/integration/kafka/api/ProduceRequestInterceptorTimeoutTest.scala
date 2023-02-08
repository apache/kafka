package integration.kafka.api

import kafka.utils.TestInfoUtils
import kafka.server.{KafkaConfig, ProduceRequestInterceptor}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.nio.charset.StandardCharsets
import java.util.Properties

class TimeoutProduceRequestInterceptor extends ProduceRequestInterceptor {
  override def processKey(key: Array[Byte]): Array[Byte] = {
    if (key == null) null
    else key
  }

  override def processValue(value: Array[Byte]): Array[Byte] = {
    if (value == null) null
    else {
      // Even though we mutate every record with this interceptor, the final result should be the same as the
      // original input, given that the interceptor is slower than the maximum allowed timeout ms for 50% of the values
      val s = new String(value, StandardCharsets.UTF_8)
      if (s.drop("value".length).toInt % 2 == 0) Thread.sleep(8)
      s"mutated-$s".getBytes(StandardCharsets.UTF_8)
    }
  }

  override def configure(): Unit = ()
}

class ProduceRequestInterceptorTimeoutTest extends ProducerSendTestHelpers {

  override def generateConfigs: collection.Seq[KafkaConfig] = {
    val overridingProps = new Properties()
    overridingProps.put(KafkaConfig.ProduceRequestInterceptorsProp, "integration.kafka.api.TimeoutProduceRequestInterceptor")
    overridingProps.put(KafkaConfig.ProduceRequestInterceptorTimeoutMsProp, 5.toString)
    baseProps.map(KafkaConfig.fromProps(_, overridingProps))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testSendToPartition(quorum: String): Unit = {
    sendToPartition(quorum)
  }

}
