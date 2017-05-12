package integration.kafka.tools

import kafka.api.IntegrationTestHarness
import kafka.tools.GetOffsetShell
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{After, Before, Test}

class GetOffsetShellReplIntegrationTest extends IntegrationTestHarness {

  val producerCount = 1
  val consumerCount = 0
  val serverCount = 2

  val topic1 = "topic1"

  @Before
  override def setUp: Unit = {
    super.setUp
    val leaders = TestUtils.createTopic(zkUtils = this.zkUtils,
      topic = topic1,
      numPartitions = 2,
      replicationFactor = 2,
      servers = this.servers)
    assertEquals("Size of leaders map", 2, leaders.size)
    val p0Leader = leaders(0)
    val p1Leader = leaders(1)
    assertNotEquals("Partitions 0 and 1 are supposed to have different leaders", p0Leader, p1Leader)
  }

  @After
  override def tearDown: Unit = {
    super.tearDown
  }

  @Test
  def twoReplicatedPartitions: Unit = {
    val producerP0Offset = sendRecordsLastOffsets(topic1, 0, 10)
    val producerP1Offset = sendRecordsLastOffsets(topic1, 1, 20)
    val offsets = GetOffsetShell.getLastOffsets(brokerList,
      Set(
        new TopicPartition(topic1, 0),
        new TopicPartition(topic1, 1)
      ))
    assertEquals(s"Must have 2 offset entries: $offsets", 2, offsets.size)
    val actualP0Offset = offsets(new TopicPartition(topic1, 0)).right.get
    assertEquals("Actual offset for partition 0 must be equal to producer offset plus 1", producerP0Offset + 1, actualP0Offset)
    val actualP1Offset = offsets(new TopicPartition(topic1, 1)).right.get
    assertEquals("Actual offset for partition 1 must be equal to producer offset plus 1", producerP1Offset + 1, actualP1Offset)
  }

  private def sendRecordsLastOffsets(topic: String, partition: Int, number: Int): Long = {
    sendRecords(topic, partition, number).last.offset()
  }

  private def sendRecords(topic: String, partition: Int, number: Int): Seq[RecordMetadata] = {
    val futures = (0 until number) map { i =>
      val record = new ProducerRecord(topic, partition, i.toString.getBytes, i.toString.getBytes)
      producers.head.send(record)
    }
    futures.map(_.get)
  }
}
