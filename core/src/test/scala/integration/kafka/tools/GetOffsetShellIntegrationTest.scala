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
    val offsets = GetOffsetShell.getOffsets(brokerList,
      Set(topic1),
      Set(0),
      -1,
      false)

    assertEquals(s"Must have 1 offset entry: $offsets", 1, offsets.size)

    val actualOffset = offsets(new TopicPartition(topic1, 0)).right.get
    assertEquals("Actual offset must be equal to producer offset plus 1", producerOffset + 1, actualOffset)
  }

  @Test
  def oneTopicOnePartitionManyMessages: Unit = {
    val producerOffset = sendRecordsLastOffsets(topic1, 0, 10)
    val offsets = GetOffsetShell.getOffsets(brokerList,
      Set(topic1),
      Set(0),
      -1,
      false)

    assertEquals(s"Must have 1 offset entry: $offsets", 1, offsets.size)

    val actualOffset = offsets(new TopicPartition(topic1, 0)).right.get
    assertEquals("Actual offset must be equal to producer offset plus 1", producerOffset + 1, actualOffset)
  }

  @Test
  def oneTopicManyPartitions: Unit = {
    val partitions = 0 to 2
    val producerOffsets = partitions.map(p => sendRecordsLastOffsets(topic1, p, 10 + 10 * p))
    val offsets = GetOffsetShell.getOffsets(brokerList,
      Set(topic1),
      partitions.toSet,
      -1,
      false)

    assertEquals(s"Must have all offset entries: $offsets", partitions.size, offsets.size)

    for (p <- partitions) {
      val actualOffset = offsets(new TopicPartition(topic1, p)).right.get
      assertEquals(s"Actual offset for partition $topic1:$p must be equal to producer offset plus 1", producerOffsets(p) + 1, actualOffset)
    }
  }

  @Test
  def manyTopicsManyPartitions: Unit = {
    val topics = Set(topic1, topic2)
    val partitions = 0 to 2
    val producerOffsets: Map[String, IndexedSeq[Long]] =
      topics.map(topic => topic -> partitions.map(p => sendRecordsLastOffsets(topic, p, 10 + 10 * p))).toMap
    val offsets = GetOffsetShell.getOffsets(brokerList,
      topics,
      partitions.toSet,
      -1,
      false)

    assertEquals(s"Must have all offset entries: $offsets", topics.size * partitions.size, offsets.size)

    for (topic <- topics) {
      for (p <- partitions) {
        val actualOffset = offsets(new TopicPartition(topic, p)).right.get
        assertEquals(s"Actual offset for partition $topic:$p must be equal to producer offset plus 1", producerOffsets(topic)(p) + 1, actualOffset)
      }
    }
  }

  @Test
  def nonExistingTopic: Unit = {
    sendRecords(topic1, 0, 1)
    val offsets = GetOffsetShell.getOffsets(brokerList,
      Set("topic999"),
      Set(0),
      -1,
      false)

    assertEquals(s"Must have 1 offset entry: $offsets", 1, offsets.size)

    val error = offsets(new TopicPartition("topic999", 0)).left.get
    assertTrue("Must return an error about non-existing topic",
      error.toLowerCase.contains("topic not found")
        && error.contains("topic999"))
  }

  @Test
  def nonExistingPartition: Unit = {
    sendRecords(topic1, 0, 1)
    val offsets = GetOffsetShell.getOffsets(brokerList,
      Set(topic1),
      Set(9999),
      -1,
      false)

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
    val partitions = 0 to 2
    val producerOffsets = partitions.map(p => sendRecordsLastOffsets(topic1, p, 10 + 10 * p))
    val offsets = GetOffsetShell.getOffsets(brokerList,
      topics,
      partitions.toSet,
      -1,
      false)

    assertEquals(s"Must have all offset entries: $offsets", topics.size * partitions.size, offsets.size)

    for (p <- partitions) {
      val actualOffset = offsets(new TopicPartition(topic1, p)).right.get
      assertEquals(s"Actual offset for partition $topic1:$p must be equal to producer offset plus 1", producerOffsets(p) + 1, actualOffset)
    }

    for (p <- partitions) {
      val error = offsets(new TopicPartition("topic999", p)).left.get
      assertTrue(s"Must return an error about non-existing partition: topic999:$p",
        error.toLowerCase.contains("topic not found")
          && error.contains("topic999"))
    }
  }

  @Test
  def oneTopicSomePartitionsExistingSomeNonExisting: Unit = {
    val existingPartitions = 0 to 2
    val nonExistingPartitions = 10 to 13
    val partitions = existingPartitions.toSet ++ nonExistingPartitions
    val producerOffsets = existingPartitions.map(p => sendRecordsLastOffsets(topic1, p, 10 + 10 * p))
    val offsets = GetOffsetShell.getOffsets(brokerList,
      Set(topic1),
      partitions,
      -1,
      false)

    assertEquals(s"Must have all offset entries: $offsets", partitions.size, offsets.size)

    for (p <- existingPartitions) {
      val actualOffset = offsets(new TopicPartition(topic1, p)).right.get
      assertEquals(s"Actual offset for partition $topic1:$p must be equal to producer offset plus 1", producerOffsets(p) + 1, actualOffset)
    }

    for (p <- nonExistingPartitions) {
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
    val existingPartitions = 0 to 2
    val nonExistingPartitions = 10 to 13
    val partitions = existingPartitions.toSet ++ nonExistingPartitions
    val producerOffsets: Map[String, IndexedSeq[Long]] =
      topics.map(topic => topic -> existingPartitions.map(p => sendRecordsLastOffsets(topic, p, 10 + 10 * p))).toMap
    val offsets = GetOffsetShell.getOffsets(brokerList,
      topics,
      partitions,
      -1,
      false)

    assertEquals(s"Must have all offset entries: $offsets", topics.size * partitions.size, offsets.size)

    for (topic <- topics) {
      for (p <- existingPartitions) {
        val actualOffset = offsets(new TopicPartition(topic, p)).right.get
        assertEquals(s"Actual offset for partition $topic:$p must be equal to producer offset plus 1", producerOffsets(topic)(p) + 1, actualOffset)
      }

      for (p <- nonExistingPartitions) {
        val error = offsets(new TopicPartition(topic, p)).left.get
        assertTrue(s"Must return an error about non-existing partition: $topic:$p",
          error.toLowerCase.contains("partition for topic not found")
            && error.contains(topic)
            && error.contains(p.toString))
      }
    }
  }

  @Test
  def noTopicsNoPartitions: Unit = {
    val topics = Set(topic1, topic2)
    val partitions = 0 to 2
    val producerOffsets: Map[String, IndexedSeq[Long]] =
      topics.map(topic => topic -> partitions.map(p => sendRecordsLastOffsets(topic, p, 10 + 10 * p))).toMap
    val offsets = GetOffsetShell.getOffsets(brokerList,
      Set(),
      Set(),
      -1,
      false)

    assertEquals(s"Must have all offset entries: $offsets", topics.size * partitions.size, offsets.size)

    for (topic <- topics) {
      for (p <- partitions) {
        val actualOffset = offsets(new TopicPartition(topic, p)).right.get
        assertEquals(s"Actual offset for partition $topic:$p must be equal to producer offset plus 1", producerOffsets(topic)(p) + 1, actualOffset)
      }
    }
  }

  @Test
  def noTopicsWithPartitions: Unit = {
    val topics = Set(topic1, topic2)
    val partitions = 0 to 1
    val producerOffsets: Map[String, IndexedSeq[Long]] =
      topics.map(topic => topic -> partitions.map(p => sendRecordsLastOffsets(topic, p, 10 + 10 * p))).toMap
    val offsets = GetOffsetShell.getOffsets(brokerList,
      Set(),
      partitions.toSet,
      -1,
      false)

    assertEquals(s"Must have all offset entries: $offsets", topics.size * partitions.size, offsets.size)

    for (topic <- topics) {
      for (p <- partitions) {
        val actualOffset = offsets(new TopicPartition(topic, p)).right.get
        assertEquals(s"Actual offset for partition $topic:$p must be equal to producer offset plus 1", producerOffsets(topic)(p) + 1, actualOffset)
      }
    }
  }

  @Test
  def noTopicsWithSomeNonExistingPartitions: Unit = {
    val topics = Set(topic1, topic2)
    val existingPartitions = 0 to 2
    val nonExistingPartitions = 10 to 13
    val partitions = existingPartitions.toSet ++ nonExistingPartitions
    val producerOffsets: Map[String, IndexedSeq[Long]] =
      topics.map(topic => topic -> existingPartitions.map(p => sendRecordsLastOffsets(topic, p, 10 + 10 * p))).toMap
    val offsets = GetOffsetShell.getOffsets(brokerList,
      Set(),
      partitions,
      -1,
      false)

    assertEquals(s"Must have all offset entries: $offsets", topics.size * partitions.size, offsets.size)

    for (topic <- topics) {
      for (p <- existingPartitions) {
        val actualOffset = offsets(new TopicPartition(topic, p)).right.get
        assertEquals(s"Actual offset for partition $topic:$p must be equal to producer offset plus 1", producerOffsets(topic)(p) + 1, actualOffset)
      }

      for (p <- nonExistingPartitions) {
        val error = offsets(new TopicPartition(topic, p)).left.get
        assertTrue(s"Must return an error about non-existing partition: $topic:$p",
          error.toLowerCase.contains("partition for topic not found")
            && error.contains(topic)
            && error.contains(p.toString))
      }
    }
  }

  @Test
  def noTopicsNoPartitionsIncludeInternalTopics: Unit = {
    val topics = Set(topic1, topic2)
    val partitions = 0 to 2
    val producerOffsets: Map[String, IndexedSeq[Long]] =
      topics.map(topic => topic -> partitions.map(p => sendRecordsLastOffsets(topic, p, 10 + 10 * p))).toMap
    val offsets = GetOffsetShell.getOffsets(brokerList,
      Set(),
      Set(),
      -1,
      true)

    assertTrue(s"Must have offset entries for user topics plus internal topics: $offsets", offsets.size > topics.size * partitions.size)

    for (topic <- topics) {
      for (p <- partitions) {
        val actualOffset = offsets(new TopicPartition(topic, p)).right.get
        assertEquals(s"Actual offset for partition $topic:$p must be equal to producer offset plus 1", producerOffsets(topic)(p) + 1, actualOffset)
      }
    }

    assertTrue("Must contain an entry for consumer offsets topic partition 0", offsets.contains(new TopicPartition("__consumer_offsets", 0)))
  }

  @Test
  def noTopicsWithPartitionsIncludeInternalTopics: Unit = {
    val topics = Set(topic1, topic2)
    val partitions = 0 to 1
    val producerOffsets: Map[String, IndexedSeq[Long]] =
      topics.map(topic => topic -> partitions.map(p => sendRecordsLastOffsets(topic, p, 10 + 10 * p))).toMap
    val offsets = GetOffsetShell.getOffsets(brokerList,
      Set(),
      partitions.toSet,
      -1,
      true)

    assertTrue(s"Must have offset entries for user topics plus internal topics: $offsets", offsets.size > topics.size * partitions.size)

    for (topic <- topics) {
      for (p <- partitions) {
        val actualOffset = offsets(new TopicPartition(topic, p)).right.get
        assertEquals(s"Actual offset for partition $topic:$p must be equal to producer offset plus 1", producerOffsets(topic)(p) + 1, actualOffset)
      }
    }

    assertTrue("Must contain an entry for consumer offsets topic partition 0", offsets.contains(new TopicPartition("__consumer_offsets", 0)))
  }

  @Test
  def withTopicsNoPartitions: Unit = {
    val topics = Set(topic1)
    val partitions = 0 to 2
    val producerOffsets: Map[String, IndexedSeq[Long]] =
      topics.map(topic => topic -> partitions.map(p => sendRecordsLastOffsets(topic, p, 10 + 10 * p))).toMap
    val offsets = GetOffsetShell.getOffsets(brokerList,
      topics,
      Set(),
      -1,
      false)

    assertEquals(s"Must have all offset entries: $offsets", topics.size * partitions.size, offsets.size)

    for (topic <- topics) {
      for (p <- partitions) {
        val actualOffset = offsets(new TopicPartition(topic, p)).right.get
        assertEquals(s"Actual offset for partition $topic:$p must be equal to producer offset plus 1", producerOffsets(topic)(p) + 1, actualOffset)
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
