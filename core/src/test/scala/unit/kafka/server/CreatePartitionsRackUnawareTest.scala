package kafka.server

import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.common.requests.{MetadataRequest, MetadataResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Collections
import scala.jdk.CollectionConverters._

class CreatePartitionsRackUnawareTest extends BaseRequestTest {

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testRackUnawarePartitionAssignment(quorum: String): Unit = {
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

    val p0Assignment = partitions(0)
    val p1Assignment = partitions(1)
    // Just make sure the new partition leader is put on a different broker. We don't need to do deep validation here,
    // as that should be done by testing of the assignment implementation components e.g. AdminUtils or
    // StripedReplicaPlacer.
    assertNotEquals(p0Assignment.leaderId.get(), p1Assignment.leaderId.get())
  }

  override def brokerCount: Int = 2
}
