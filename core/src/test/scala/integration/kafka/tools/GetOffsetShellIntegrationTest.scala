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
    val producerOffset = sendRecordsLastOffsets(topic1, 0, 1)
    val offsets = GetOffsetShell.getLastOffsets(brokerList,
      Set(new TopicPartition(topic1, 0)))
    assertEquals(s"Must have 1 offset entry: $offsets", 1, offsets.size)
    val actualOffset = offsets(new TopicPartition(topic1, 0)).right.get
    assertEquals("Actual offset must be equal to producer offset plus 1", producerOffset + 1, actualOffset)
  }

  @Test
  def oneTopicOnePartitionManyMessages: Unit = {
    val producerOffset = sendRecordsLastOffsets(topic1, 0, 10)
    val offsets = GetOffsetShell.getLastOffsets(brokerList,
      Set(new TopicPartition(topic1, 0)))
    assertEquals(s"Must have 1 offset entry: $offsets", 1, offsets.size)
    val actualOffset = offsets(new TopicPartition(topic1, 0)).right.get
    assertEquals("Actual offset must be equal to producer offset plus 1", producerOffset + 1, actualOffset)
  }

  @Test
  def oneTopicManyPartitions: Unit = {
    val producerOffsets = (0 to 2).map(p => sendRecordsLastOffsets(topic1, p, 10 + 10 * p))
    val offsets = GetOffsetShell.getLastOffsets(brokerList,
      Set(0, 1, 2).map(new TopicPartition(topic1, _)))
    assertEquals(s"Must have all offset entries: $offsets", 3, offsets.size)

    for (p <- 0 to 2) {
      val actualOffset = offsets(new TopicPartition(topic1, p)).right.get
      assertEquals(s"Actual offset for partition $topic1:$p must be equal to producer offset plus 1", producerOffsets(p) + 1, actualOffset)
    }
  }

  @Test
  def manyTopicsManyPartitions: Unit = {
    val producerT1P0Offset = sendRecordsLastOffsets(topic1, 0, 10)
    val producerT1P1Offset = sendRecordsLastOffsets(topic1, 1, 20)
    val producerT2P0Offset = sendRecordsLastOffsets(topic2, 0, 30)
    val producerT2P1Offset = sendRecordsLastOffsets(topic2, 1, 40)
    val offsets = GetOffsetShell.getLastOffsets(brokerList,
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
    val offsets = GetOffsetShell.getLastOffsets(brokerList,
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
    val offsets = GetOffsetShell.getLastOffsets(brokerList,
      Set(new TopicPartition(topic1, 9999)))
    assertEquals(s"Must have 1 offset entry: $offsets", 1, offsets.size)
    val error = offsets(new TopicPartition(topic1, 9999)).left.get
    assertTrue("Must return an error about non-existing partition",
      error.toLowerCase.contains("partition for topic not found")
        && error.contains(topic1)
        && error.contains("9999"))
  }

  @Test
  def manyTopicsSomeNonExisting: Unit = {
    val topics = Set(topic1, "topic999")
    val partitions = Set(0, 1, 2)
    val producerOffsets = (0 to 2).map(p => sendRecordsLastOffsets(topic1, p, 10 + 10 * p))
    val topicPartitions = for {
      topic <- topics
      partition <- partitions
    } yield (new TopicPartition(topic, partition))
    val offsets = GetOffsetShell.getLastOffsets(brokerList, topicPartitions)

    assertEquals(s"Must have all offset entries: $offsets", topics.size * partitions.size, offsets.size)

    for (p <- 0 to 2) {
      val actualOffset = offsets(new TopicPartition(topic1, p)).right.get
      assertEquals(s"Actual offset for partition $topic1:$p must be equal to producer offset plus 1", producerOffsets(p) + 1, actualOffset)
    }

    for (p <- 0 to 2) {
      val error = offsets(new TopicPartition("topic999", p)).left.get
      assertTrue(s"Must return an error about non-existing partition: topic999:$p",
        error.toLowerCase.contains("topic not found")
          && error.contains("topic999"))
    }
  }

  @Test
  def oneTopicSomePartitionsExistingSomeNonExisting: Unit = {
    val partitions = Set(10, 0, 11, 1, 12, 2, 13)
    val producerOffsets = (0 to 2).map(p => sendRecordsLastOffsets(topic1, p, 10 + 10 * p))
    val offsets = GetOffsetShell.getLastOffsets(brokerList,
      partitions.map(new TopicPartition(topic1, _)))
    assertEquals(s"Must have all offset entries: $offsets", partitions.size, offsets.size)

    for (p <- 0 to 2) {
      val actualOffset = offsets(new TopicPartition(topic1, p)).right.get
      assertEquals(s"Actual offset for partition $topic1:$p must be equal to producer offset plus 1", producerOffsets(p) + 1, actualOffset)
    }

    for (p <- 10 to 13) {
      val error = offsets(new TopicPartition(topic1, p)).left.get
      assertTrue(s"Must return an error about non-existing partition: $topic1:$p",
        error.toLowerCase.contains("partition for topic not found")
          && error.contains(topic1)
          && error.contains(p.toString))
    }
  }

  @Test
  def manyTopicsSomePartitionsExistingSomeNonExisting: Unit = {
    val topics = Set(topic1, topic2)
    val partitions = Set(10, 0, 11, 1, 12, 2, 13)
    val producerOffsets: Map[String, IndexedSeq[Long]] =
      topics.map(topic => topic -> (0 to 2).map(p => sendRecordsLastOffsets(topic, p, 10 + 10 * p))).toMap
    val topicPartitions = for {
      topic <- topics
      partition <- partitions
    } yield (new TopicPartition(topic, partition))
    val offsets = GetOffsetShell.getLastOffsets(brokerList, topicPartitions)

    assertEquals(s"Must have all offset entries: $offsets", topics.size * partitions.size, offsets.size)

    for (topic <- topics) {
      for (p <- 0 to 2) {
        val actualOffset = offsets(new TopicPartition(topic, p)).right.get
        assertEquals(s"Actual offset for partition $topic:$p must be equal to producer offset plus 1", producerOffsets(topic)(p) + 1, actualOffset)
      }

      for (p <- 10 to 13) {
        val error = offsets(new TopicPartition(topic, p)).left.get
        assertTrue(s"Must return an error about non-existing partition: $topic:$p",
          error.toLowerCase.contains("partition for topic not found")
            && error.contains(topic)
            && error.contains(p.toString))
      }
    }
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
