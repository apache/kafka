package integration.kafka.tools

import kafka.api.IntegrationTestHarness
import kafka.tools.GetOffsetShell.getOffsets
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{After, Before, Test}

class GetOffsetShellTimestampTest extends IntegrationTestHarness {

  val producerCount = 1
  val consumerCount = 0
  val serverCount = 1

  val topic1 = "topic1"

  @Before
  override def setUp: Unit = {
    super.setUp
    TestUtils.createTopic(zkUtils = this.zkUtils,
      topic = topic1,
      numPartitions = 1,
      servers = this.servers)
  }

  @After
  override def tearDown: Unit = {
    super.tearDown
  }

  @Test
  def nonExistingTimestamp: Unit = {
    val recordsNumber = 10
    val requestedTimestamp = recordsNumber + 1
    sendRecords(topic1, 0, recordsNumber)
    val offsets = getOffsets(brokerList,
      Set(topic1),
      Set(0),
      requestedTimestamp,
      includeInternalTopics = false)

    assertEquals(s"Must not have an offset entry for non-existing timestamp: $offsets", 0, offsets.size)
  }

  @Test
  def existingTimestamp: Unit = {
    val recordsNumber = 10
    val requestedTimestamp = 5
    sendRecords(topic1, 0, recordsNumber)
    val offsets = getOffsets(brokerList,
      Set(topic1),
      Set(0),
      requestedTimestamp,
      includeInternalTopics = false)

    assertEquals(s"Must have 1 offset entry: $offsets", 1, offsets.size)

    val actualOffset = offsets(new TopicPartition(topic1, 0)).right.get
    assertEquals("Actual offset must be equal to the requested timestamp", requestedTimestamp, actualOffset)

  }

  private def sendRecords(topic: String, partition: Int, number: Int): Seq[RecordMetadata] = {
    val futures = (0 until number) map { i =>
      val record = new ProducerRecord(topic, partition, i.toLong, i.toString.getBytes, i.toString.getBytes)
      producers.head.send(record)
    }
    futures.map(_.get)
  }
}
