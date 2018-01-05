package unit.kafka.zk

import com.fasterxml.jackson.core.JsonProcessingException
import kafka.zk.ReassignPartitionsZNode
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.Test

class ReassignPartitionsZNodeTest {

  val topic = "foo"
  val partition1 = 0
  val replica1 = 1
  val replica2 = 2

  private val reassignPartitionData = Map(
    new TopicPartition(topic, partition1) -> Seq(replica1, replica2))
  private val reassignmentJson = "{\"version\":1,\"partitions\":[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[1,2]}]}"

  @Test
  def testEncode() {
    val encodedJsonString = ReassignPartitionsZNode.encode(reassignPartitionData)
      .map(_.toChar)
      .mkString

    assertEquals(reassignmentJson, encodedJsonString)
  }

  @Test
  def testDecodeInvalidJson() {
    val result = ReassignPartitionsZNode.decode("invalid json".getBytes)

    assertTrue(result.isLeft)
    assertTrue(result.left.get.isInstanceOf[JsonProcessingException])
  }

  @Test
  def testDecodeValidJson() {
    val result = ReassignPartitionsZNode.decode(reassignmentJson.getBytes)

    assertTrue(result.isRight)
    val assignmentMap = result.right.get
    assertEquals(Seq(replica1, replica2), assignmentMap(new TopicPartition(topic, partition1)))
  }
}
