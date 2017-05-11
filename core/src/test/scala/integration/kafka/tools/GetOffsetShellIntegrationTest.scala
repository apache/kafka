package integration.kafka.tools

import kafka.api.IntegrationTestHarness
import kafka.tools.GetOffsetShell
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{After, Before, Test}

class GetOffsetShellIntegrationTest extends IntegrationTestHarness {

  val producerCount = 1
  val consumerCount = 0
  val serverCount = 1

  val topic1 = "topic1"
  val topic2 = "topic2"

  @Before
  override def setUp: Unit = {
    super.setUp
    TestUtils.createTopic(zkUtils = this.zkUtils,
      topic = topic1,
      numPartitions = 3,
      servers = this.servers)
    TestUtils.createTopic(zkUtils = this.zkUtils,
      topic = topic2,
      numPartitions = 3,
      servers = this.servers)
  }

  @After
  override def tearDown: Unit = {
    super.tearDown
  }

  @Test
  def oneTopicOnePartitionOneMessage: Unit = {
    val producerOffset = sendRecords(topic1, 0, 1).last.offset
    val offsets = GetOffsetShell.getOffsetsNew(brokerList,
      Set(new TopicPartition(topic1, 0)))
    assertEquals(s"Must have 1 offset entry: $offsets", 1, offsets.size)
    val actualOffset = offsets(new TopicPartition(topic1, 0)).right.get
    assertEquals("Actual offset must be equal to producer offset plus 1", producerOffset + 1, actualOffset)
  }

  @Test
  def oneTopicOnePartitionManyMessages: Unit = {
    val producerOffset = sendRecords(topic1, 0, 10).last.offset
    val offsets = GetOffsetShell.getOffsetsNew(brokerList,
      Set(new TopicPartition(topic1, 0)))
    assertEquals(s"Must have 1 offset entry: $offsets", 1, offsets.size)
    val actualOffset = offsets(new TopicPartition(topic1, 0)).right.get
    assertEquals("Actual offset must be equal to producer offset plus 1", producerOffset + 1, actualOffset)
  }

  @Test
  def oneTopicManyPartitions: Unit = {
    val producerP0Offset = sendRecords(topic1, 0, 10).last.offset
    val producerP1Offset = sendRecords(topic1, 1, 20).last.offset
    val producerP2Offset = sendRecords(topic1, 2, 30).last.offset
    val offsets = GetOffsetShell.getOffsetsNew(brokerList,
      Set(
        new TopicPartition(topic1, 0),
        new TopicPartition(topic1, 1),
        new TopicPartition(topic1, 2)
      ))
    assertEquals(s"Must have 3 offset entries: $offsets", 3, offsets.size)
    val actualP0Offset = offsets(new TopicPartition(topic1, 0)).right.get
    assertEquals("Actual offset for partition 0 must be equal to producer offset plus 1", producerP0Offset + 1, actualP0Offset)
    val actualP1Offset = offsets(new TopicPartition(topic1, 1)).right.get
    assertEquals("Actual offset for partition 1 must be equal to producer offset plus 1", producerP1Offset + 1, actualP1Offset)
    val actualP2Offset = offsets(new TopicPartition(topic1, 2)).right.get
    assertEquals("Actual offset for partition 2 must be equal to producer offset plus 1", producerP2Offset + 1, actualP2Offset)
  }

  @Test
  def manyTopicsManyPartitions: Unit = {
    val producerT1P0Offset = sendRecords(topic1, 0, 10).last.offset
    val producerT1P1Offset = sendRecords(topic1, 1, 20).last.offset
    val producerT2P0Offset = sendRecords(topic2, 0, 30).last.offset
    val producerT2P1Offset = sendRecords(topic2, 1, 40).last.offset
    val offsets = GetOffsetShell.getOffsetsNew(brokerList,
      Set(
        new TopicPartition(topic1, 0),
        new TopicPartition(topic1, 1),
        new TopicPartition(topic2, 0),
        new TopicPartition(topic2, 1)
      ))
    assertEquals(s"Must have 4 offset entries: $offsets", 4, offsets.size)
    val actualT1P0Offset = offsets(new TopicPartition(topic1, 0)).right.get
    assertEquals("Actual offset for topic1 partition 0 must be equal to producer offset plus 1", producerT1P0Offset + 1, actualT1P0Offset)
    val actualT1P1Offset = offsets(new TopicPartition(topic1, 1)).right.get
    assertEquals("Actual offset for topic1 partition 1 must be equal to producer offset plus 1", producerT1P1Offset + 1, actualT1P1Offset)
    val actualT2P0Offset = offsets(new TopicPartition(topic2, 0)).right.get
    assertEquals("Actual offset for topic2 partition 0 must be equal to producer offset plus 1", producerT2P0Offset + 1, actualT2P0Offset)
    val actualT2P1Offset = offsets(new TopicPartition(topic2, 1)).right.get
    assertEquals("Actual offset for topic2 partition 1 must be equal to producer offset plus 1", producerT2P1Offset + 1, actualT2P1Offset)
  }

  @Test
  def nonExistingTopic: Unit = {
    sendRecords(topic1, 0, 1)
    val offsets = GetOffsetShell.getOffsetsNew(brokerList,
      Set(new TopicPartition("topic999", 0)))
    assertEquals(s"Must have 1 offset entry: $offsets", 1, offsets.size)
    val error = offsets(new TopicPartition("topic999", 0)).left.get
    assertTrue("Must return an error about non-existing topic",
      error.toLowerCase.contains("topic not found")
        && error.contains("topic999"))
  }

  @Test
  def nonExistingPartition: Unit = {
    sendRecords(topic1, 0, 1)
    val offsets = GetOffsetShell.getOffsetsNew(brokerList,
      Set(new TopicPartition(topic1, 9999)))
    assertEquals(s"Must have 1 offset entry: $offsets", 1, offsets.size)
    val error = offsets(new TopicPartition(topic1, 9999)).left.get
    assertTrue("Must return an error about non-existing partition",
      error.toLowerCase.contains("partition for topic not found")
        && error.contains(topic1)
        && error.contains("9999"))
  }

  private def sendRecords(topic: String, partition: Int, number: Int): Seq[RecordMetadata] = {
    val futures = (0 until number) map { i =>
      val record = new ProducerRecord(topic, partition, i.toString.getBytes, i.toString.getBytes)
      producers.head.send(record)
    }
    futures.map(_.get)
  }
}
