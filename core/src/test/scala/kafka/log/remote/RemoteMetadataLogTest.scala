package kafka.log.remote

import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage.{RemoteLogSegmentId, RemoteLogSegmentMetadata}
import org.apache.kafka.common.record.SimpleRecord
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.util.{Collections, UUID}
import scala.jdk.CollectionConverters._

class RemoteMetadataLogTest {

  val time = new MockTime()

  @Test
  def testFormatRecordKeyValue(): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val timestamp = time.hiResClockMs()

    val metadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicPartition, UUID.randomUUID()), 5,
      10, timestamp, 2, timestamp, 100,
      RemoteLogSegmentMetadata.State.COPY_STARTED, Collections.emptyMap())

    val keyBytes = RemoteMetadataLog.keyToBytes(topicPartition.toString)
    val valueBytes = RemoteMetadataLog.valueToBytes(metadata)
    val remoteLogMetadataRecord = TestUtils.records(Seq(
      new SimpleRecord(keyBytes, valueBytes)
    )).records.asScala.head

    val actual = RemoteMetadataLog.formatRecordKeyAndValue(remoteLogMetadataRecord)
    assertEquals(topicPartition.toString, actual._1.get)
    assertEquals(metadata.toString, actual._2.get)
  }

  @Test
  def testFormatEmptyRecordKeyValue(): Unit = {
    val emptyBytes = Array[Byte]()
    val remoteLogMetadataRecord = TestUtils.records(Seq(
      new SimpleRecord(emptyBytes, null)
    )).records.asScala.head
    val actual = RemoteMetadataLog.formatRecordKeyAndValue(remoteLogMetadataRecord)
    assertTrue(actual._1.get.isEmpty)
    assertEquals("<EMPTY>", actual._2.get)
  }
}