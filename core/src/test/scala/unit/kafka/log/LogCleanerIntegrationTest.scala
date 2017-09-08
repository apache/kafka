/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import java.io.File
import java.util.Properties

import kafka.api.KAFKA_0_11_0_IV0
import kafka.api.{KAFKA_0_10_0_IV1, KAFKA_0_9_0}
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record._
import org.junit.Assert._
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.Seq
import scala.collection._
import scala.util.Random

/**
 * This is an integration test that tests the fully integrated log cleaner
 */
@RunWith(value = classOf[Parameterized])
class LogCleanerIntegrationTest(compressionCodec: String) extends AbstractLogCleanerIntegrationTest {

  val codec = CompressionType.forName(compressionCodec)
  val time = new MockTime()
  var counter = 0
  val topicPartitions = Array(new TopicPartition("log", 0), new TopicPartition("log", 1), new TopicPartition("log", 2))

  @Test
  def cleanerTest() {
    val largeMessageKey = 20
    val (largeMessageValue, largeMessageSet) = createLargeSingleMessageSet(largeMessageKey, RecordBatch.CURRENT_MAGIC_VALUE)
    val maxMessageSize = largeMessageSet.sizeInBytes

    cleaner = makeCleaner(partitions = topicPartitions, maxMessageSize = maxMessageSize)
    val log = cleaner.logs.get(topicPartitions(0))

    val appends = writeDups(numKeys = 100, numDups = 3, log = log, codec = codec)
    val startSize = log.size
    cleaner.startup()

    val firstDirty = log.activeSegment.baseOffset
    checkLastCleaned("log", 0, firstDirty)
    val compactedSize = log.logSegments.map(_.size).sum
    assertTrue(s"log should have been compacted: startSize=$startSize compactedSize=$compactedSize", startSize > compactedSize)

    checkLogAfterAppendingDups(log, startSize, appends)

    val appendInfo = log.appendAsLeader(largeMessageSet, leaderEpoch = 0)
    val largeMessageOffset = appendInfo.firstOffset

    val dups = writeDups(startKey = largeMessageKey + 1, numKeys = 100, numDups = 3, log = log, codec = codec)
    val appends2 = appends ++ Seq((largeMessageKey, largeMessageValue, largeMessageOffset)) ++ dups
    val firstDirty2 = log.activeSegment.baseOffset
    checkLastCleaned("log", 0, firstDirty2)

    checkLogAfterAppendingDups(log, startSize, appends2)

    // simulate deleting a partition, by removing it from logs
    // force a checkpoint
    // and make sure its gone from checkpoint file
    cleaner.logs.remove(topicPartitions(0))
    cleaner.updateCheckpoints(logDir)
    val checkpoints = new OffsetCheckpointFile(new File(logDir, cleaner.cleanerManager.offsetCheckpointFile)).read()
    // we expect partition 0 to be gone
    assertFalse(checkpoints.contains(topicPartitions(0)))
  }

  @Test
  def testCleansCombinedCompactAndDeleteTopic(): Unit = {
    val logProps  = new Properties()
    val retentionMs: Integer = 100000
    logProps.put(LogConfig.RetentionMsProp, retentionMs: Integer)
    logProps.put(LogConfig.CleanupPolicyProp, "compact,delete")

    def runCleanerAndCheckCompacted(numKeys: Int): (Log, Seq[(Int, String, Long)]) = {
      cleaner = makeCleaner(partitions = topicPartitions.take(1), propertyOverrides = logProps, backOffMs = 100L)
      val log = cleaner.logs.get(topicPartitions(0))

      val messages = writeDups(numKeys = numKeys, numDups = 3, log = log, codec = codec)
      val startSize = log.size

      log.onHighWatermarkIncremented(log.logEndOffset)

      val firstDirty = log.activeSegment.baseOffset
      cleaner.startup()

      // should compact the log
      checkLastCleaned("log", 0, firstDirty)
      val compactedSize = log.logSegments.map(_.size).sum
      assertTrue(s"log should have been compacted: startSize=$startSize compactedSize=$compactedSize", startSize > compactedSize)
      (log, messages)
    }

    val (log, _) = runCleanerAndCheckCompacted(100)
    // should delete old segments
    log.logSegments.foreach(_.lastModified = time.milliseconds - (2 * retentionMs))

    TestUtils.waitUntilTrue(() => log.numberOfSegments == 1, "There should only be 1 segment remaining", 10000L)
    assertEquals(1, log.numberOfSegments)

    cleaner.shutdown()

    // run the cleaner again to make sure if there are no issues post deletion
    val (log2, messages) = runCleanerAndCheckCompacted(20)
    val read = readFromLog(log2)
    assertEquals("Contents of the map shouldn't change", toMap(messages), toMap(read))
  }

  // returns (value, ByteBufferMessageSet)
  private def createLargeSingleMessageSet(key: Int, messageFormatVersion: Byte): (String, MemoryRecords) = {
    def messageValue(length: Int): String = {
      val random = new Random(0)
      new String(random.alphanumeric.take(length).toArray)
    }
    val value = messageValue(128)
    val messageSet = TestUtils.singletonRecords(value = value.getBytes, codec = codec, key = key.toString.getBytes,
      magicValue = messageFormatVersion)
    (value, messageSet)
  }

  @Test
  def testCleanerWithMessageFormatV0(): Unit = {
    val largeMessageKey = 20
    val (largeMessageValue, largeMessageSet) = createLargeSingleMessageSet(largeMessageKey, RecordBatch.MAGIC_VALUE_V0)
    val maxMessageSize = codec match {
      case CompressionType.NONE => largeMessageSet.sizeInBytes
      case _ =>
        // the broker assigns absolute offsets for message format 0 which potentially causes the compressed size to
        // increase because the broker offsets are larger than the ones assigned by the client
        // adding `5` to the message set size is good enough for this test: it covers the increased message size while
        // still being less than the overhead introduced by the conversion from message format version 0 to 1
        largeMessageSet.sizeInBytes + 5
    }

    cleaner = makeCleaner(partitions = topicPartitions, maxMessageSize = maxMessageSize)

    val log = cleaner.logs.get(topicPartitions(0))
    val props = logConfigProperties(maxMessageSize = maxMessageSize)
    props.put(LogConfig.MessageFormatVersionProp, KAFKA_0_9_0.version)
    log.config = new LogConfig(props)

    val appends = writeDups(numKeys = 100, numDups = 3, log = log, codec = codec, magicValue = RecordBatch.MAGIC_VALUE_V0)
    val startSize = log.size
    cleaner.startup()

    val firstDirty = log.activeSegment.baseOffset
    checkLastCleaned("log", 0, firstDirty)
    val compactedSize = log.logSegments.map(_.size).sum
    assertTrue(s"log should have been compacted: startSize=$startSize compactedSize=$compactedSize", startSize > compactedSize)

    checkLogAfterAppendingDups(log, startSize, appends)

    val appends2: Seq[(Int, String, Long)] = {
      val dupsV0 = writeDups(numKeys = 40, numDups = 3, log = log, codec = codec, magicValue = RecordBatch.MAGIC_VALUE_V0)
      val appendInfo = log.appendAsLeader(largeMessageSet, leaderEpoch = 0)
      val largeMessageOffset = appendInfo.firstOffset

      // also add some messages with version 1 and version 2 to check that we handle mixed format versions correctly
      props.put(LogConfig.MessageFormatVersionProp, KAFKA_0_11_0_IV0.version)
      log.config = new LogConfig(props)
      val dupsV1 = writeDups(startKey = 30, numKeys = 40, numDups = 3, log = log, codec = codec, magicValue = RecordBatch.MAGIC_VALUE_V1)
      val dupsV2 = writeDups(startKey = 15, numKeys = 5, numDups = 3, log = log, codec = codec, magicValue = RecordBatch.MAGIC_VALUE_V2)
      appends ++ dupsV0 ++ Seq((largeMessageKey, largeMessageValue, largeMessageOffset)) ++ dupsV1 ++ dupsV2
    }
    val firstDirty2 = log.activeSegment.baseOffset
    checkLastCleaned("log", 0, firstDirty2)

    checkLogAfterAppendingDups(log, startSize, appends2)
  }

  @Test
  def testCleaningNestedMessagesWithMultipleVersions(): Unit = {
    val maxMessageSize = 192
    cleaner = makeCleaner(partitions = topicPartitions, maxMessageSize = maxMessageSize)

    val log = cleaner.logs.get(topicPartitions(0))
    val props = logConfigProperties(maxMessageSize = maxMessageSize)
    props.put(LogConfig.MessageFormatVersionProp, KAFKA_0_9_0.version)
    log.config = new LogConfig(props)

    // with compression enabled, these messages will be written as a single message containing
    // all of the individual messages
    var appendsV0 = writeDupsSingleMessageSet(numKeys = 2, numDups = 3, log = log, codec = codec, magicValue = RecordBatch.MAGIC_VALUE_V0)
    appendsV0 ++= writeDupsSingleMessageSet(numKeys = 2, startKey = 3, numDups = 2, log = log, codec = codec, magicValue = RecordBatch.MAGIC_VALUE_V0)

    props.put(LogConfig.MessageFormatVersionProp, KAFKA_0_10_0_IV1.version)
    log.config = new LogConfig(props)

    var appendsV1 = writeDupsSingleMessageSet(startKey = 4, numKeys = 2, numDups = 2, log = log, codec = codec, magicValue = RecordBatch.MAGIC_VALUE_V1)
    appendsV1 ++= writeDupsSingleMessageSet(startKey = 4, numKeys = 2, numDups = 2, log = log, codec = codec, magicValue = RecordBatch.MAGIC_VALUE_V1)
    appendsV1 ++= writeDupsSingleMessageSet(startKey = 6, numKeys = 2, numDups = 2, log = log, codec = codec, magicValue = RecordBatch.MAGIC_VALUE_V1)

    val appends = appendsV0 ++ appendsV1

    val startSize = log.size
    cleaner.startup()

    val firstDirty = log.activeSegment.baseOffset
    assertTrue(firstDirty > appendsV0.size) // ensure we clean data from V0 and V1

    checkLastCleaned("log", 0, firstDirty)
    val compactedSize = log.logSegments.map(_.size).sum
    assertTrue(s"log should have been compacted: startSize=$startSize compactedSize=$compactedSize", startSize > compactedSize)

    checkLogAfterAppendingDups(log, startSize, appends)
  }

  private def checkLastCleaned(topic: String, partitionId: Int, firstDirty: Long) {
    // wait until cleaning up to base_offset, note that cleaning happens only when "log dirty ratio" is higher than
    // LogConfig.MinCleanableDirtyRatioProp
    val topicPartition = new TopicPartition(topic, partitionId)
    cleaner.awaitCleaned(topicPartition, firstDirty)
    val lastCleaned = cleaner.cleanerManager.allCleanerCheckpoints(topicPartition)
    assertTrue(s"log cleaner should have processed up to offset $firstDirty, but lastCleaned=$lastCleaned",
      lastCleaned >= firstDirty)
  }

  private def checkLogAfterAppendingDups(log: Log, startSize: Long, appends: Seq[(Int, String, Long)]) {
    val read = readFromLog(log)
    assertEquals("Contents of the map shouldn't change", toMap(appends), toMap(read))
    assertTrue(startSize > log.size)
  }

  private def toMap(messages: Iterable[(Int, String, Long)]): Map[Int, (String, Long)] = {
    messages.map { case (key, value, offset) => key -> (value, offset) }.toMap
  }

  private def readFromLog(log: Log): Iterable[(Int, String, Long)] = {
    import JavaConverters._
    for (segment <- log.logSegments; deepLogEntry <- segment.log.records.asScala) yield {
      val key = TestUtils.readString(deepLogEntry.key).toInt
      val value = TestUtils.readString(deepLogEntry.value)
      (key, value, deepLogEntry.offset)
    }
  }

  private def writeDups(numKeys: Int, numDups: Int, log: Log, codec: CompressionType,
                        startKey: Int = 0, magicValue: Byte = RecordBatch.CURRENT_MAGIC_VALUE): Seq[(Int, String, Long)] = {
    for(_ <- 0 until numDups; key <- startKey until (startKey + numKeys)) yield {
      val value = counter.toString
      val appendInfo = log.appendAsLeader(TestUtils.singletonRecords(value = value.toString.getBytes, codec = codec,
              key = key.toString.getBytes, magicValue = magicValue), leaderEpoch = 0)
      counter += 1
      (key, value, appendInfo.firstOffset)
    }
  }

  private def writeDupsSingleMessageSet(numKeys: Int, numDups: Int, log: Log, codec: CompressionType,
                                        startKey: Int = 0, magicValue: Byte): Seq[(Int, String, Long)] = {
    val kvs = for (_ <- 0 until numDups; key <- startKey until (startKey + numKeys)) yield {
      val payload = counter.toString
      counter += 1
      (key, payload)
    }

    val records = kvs.map { case (key, payload) =>
      new SimpleRecord(key.toString.getBytes, payload.toString.getBytes)
    }

    val appendInfo = log.appendAsLeader(MemoryRecords.withRecords(magicValue, codec, records: _*), leaderEpoch = 0)
    val offsets = appendInfo.firstOffset to appendInfo.lastOffset

    kvs.zip(offsets).map { case (kv, offset) => (kv._1, kv._2, offset) }
  }

}

object LogCleanerIntegrationTest {
  @Parameters
  def parameters: java.util.Collection[Array[String]] = {
    val list = new java.util.ArrayList[Array[String]]()
    for (codec <- CompressionType.values)
      list.add(Array(codec.name))
    list
  }
}
