package integration.kafka.api

import kafka.server.{BaseRequestTest, KafkaConfig, ProduceRequestInterceptor, ProduceRequestInterceptorResult}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.message.ProduceRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._

class ErrorProduceRequestInterceptor extends ProduceRequestInterceptor {

  override def processRecord(key: Array[Byte], value: Array[Byte], topic: String, partition: Int, headers: Array[Header]): ProduceRequestInterceptorResult = {
    val newValue = {
      if (value == null) null
      else {
        throw new RuntimeException("err")
      }
    }
    ProduceRequestInterceptorResult(key, newValue)
  }

  override def configure(): Unit = ()
}

class ProduceRequestInterceptorErrorTest extends BaseRequestTest {

  override def modifyConfigs(props: collection.Seq[Properties]): Unit = {
    props.foreach { p =>
      p.put(KafkaConfig.ProduceRequestInterceptorsProp, "integration.kafka.api.ErrorProduceRequestInterceptor")
    }
    super.modifyConfigs(props)
  }

  @Test
  def testInterceptorFailsRequestOnError(): Unit = {
    val topic = "topic"
    val (partition, leader) = createTopicAndFindPartitionWithLeader(topic)

    val records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("key".getBytes(), "value".getBytes()))
    val topicPartition = new TopicPartition(topic, partition)
    val produceRequest = ProduceRequest.forCurrentMagic(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
        new ProduceRequestData.TopicProduceData()
          .setName(topicPartition.topic())
          .setPartitionData(Collections.singletonList(new ProduceRequestData.PartitionProduceData()
            .setIndex(topicPartition.partition())
            .setRecords(records)))).iterator))
      .setAcks((-1).toShort)
      .setTimeoutMs(3000)
      .setTransactionalId(null)).build()

    val response = connectAndReceive[ProduceResponse](produceRequest, destination = brokerSocketServer(leader))
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, Errors.forCode(response.data().responses().asScala.head.partitionResponses().asScala.head.errorCode()))
  }

}
