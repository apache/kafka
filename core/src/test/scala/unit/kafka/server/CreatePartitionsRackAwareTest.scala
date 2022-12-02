package kafka.server

import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.common.requests.{MetadataRequest, MetadataResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Collections
import scala.jdk.CollectionConverters._

class CreatePartitionsRackAwareTest extends BaseRequestTest {

  val brokerIdToRack = Map(
    0 -> "rack1",
    1 -> "rack1",
    2 -> "rack1",
    3 -> "rack1",
    4 -> "rack2",
    5 -> "rack2",
    6 -> "rack2",
    7 -> "rack2",
    8 -> "rack3",
    9 -> "rack3",
    10 -> "rack3",
    11 -> "rack3"
  )

  override def generateConfigs: collection.Seq[KafkaConfig] = {
    brokerIdToRack.map { case (id, rack) =>
      KafkaConfig.fromProps(TestUtils.createBrokerConfig(id, zkConnectOrNull, rack = Some(rack)))
    }.toSeq
  }

  // This test does not pass reliably for ZK - see https://issues.apache.org/jira/browse/KAFKA-14456.
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testRackAwarePartitionAssignment(quorum: String): Unit = {
    val topicName = "topic-1"
    createTopic(topicName, 1, 1)
    TestUtils.waitForPartitionMetadata(brokers, topicName, 0)

    val admin = createAdminClient()
    admin.createPartitions(Collections.singletonMap(topicName, NewPartitions.increaseTo(2)))
    TestUtils.waitForPartitionMetadata(brokers, topicName, 1)

    val response = connectAndReceive[MetadataResponse](
      new MetadataRequest.Builder(Seq(topicName).asJava, false).build)
    assertEquals(1, response.topicMetadata.size)
    val partitions = response.topicMetadata.asScala.head.partitionMetadata.asScala.sortBy(_.partition)
    assertEquals(partitions.size, 2)
    assertEquals(0, partitions(0).partition)
    assertEquals(1, partitions(1).partition)

    val p0Assignment = partitions(0)
    val p0LeaderRack = brokerIdToRack(p0Assignment.leaderId.get())
    val p1Assignment = partitions(1)
    val p1LeaderRack = brokerIdToRack(p1Assignment.leaderId.get())
    // Just make sure the new partition leader is put on a different rack. We don't need to do deep validation here,
    // as that should be done by testing of the assignment implementation components e.g. AdminUtils or
    // StripedReplicaPlacer.
    assertNotEquals(p0LeaderRack, p1LeaderRack)
  }

  override def brokerCount: Int = brokerIdToRack.keys.size
}
