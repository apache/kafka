/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.io.{ByteArrayOutputStream, File, PrintWriter}
import java.nio.ByteBuffer
import java.util
import java.util.Collections
import java.util.Optional
import java.util.Properties
import java.util.stream.IntStream
import kafka.log.{LogTestUtils, UnifiedLog}
import kafka.raft.{KafkaMetadataLog, MetadataLogConfig}
import kafka.server.KafkaRaftServer
import kafka.tools.DumpLogSegments.{OffsetsMessageParser, ShareGroupStateMessageParser, TimeIndexDumpErrors}
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.{Assignment, Subscription}
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.metadata.{PartitionChangeRecord, RegisterBrokerRecord, TopicRecord}
import org.apache.kafka.common.protocol.{ByteBufferAccessor, ObjectSerializationCache}
import org.apache.kafka.common.record.{ControlRecordType, EndTransactionMarker, MemoryRecords, Record, RecordBatch, RecordVersion, SimpleRecord}
import org.apache.kafka.common.utils.{Exit, Utils}
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord
import org.apache.kafka.coordinator.group.GroupCoordinatorRecordSerde
import org.apache.kafka.coordinator.group.generated.{ConsumerGroupMemberMetadataValue, ConsumerGroupMetadataKey, ConsumerGroupMetadataValue, GroupMetadataKey, GroupMetadataValue}
import org.apache.kafka.coordinator.share.generated.{ShareSnapshotKey, ShareSnapshotValue, ShareUpdateKey, ShareUpdateValue}
import org.apache.kafka.coordinator.share.{ShareCoordinator, ShareCoordinatorRecordSerde}
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.metadata.MetadataRecordSerde
import org.apache.kafka.raft.{KafkaRaftClient, OffsetAndEpoch, VoterSetTest}
import org.apache.kafka.server.common.{ApiMessageAndVersion, KRaftVersion}
import org.apache.kafka.server.config.ServerLogConfigs
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde
import org.apache.kafka.server.log.remote.storage.{RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, RemotePartitionDeleteMetadata, RemotePartitionDeleteState}
import org.apache.kafka.server.storage.log.FetchIsolation
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.snapshot.RecordsSnapshotWriter
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogConfig, LogDirFailureChannel, ProducerStateManagerConfig}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}

import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{AccessDeniedException, Files, NoSuchFileException, Paths}
import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Using
import scala.util.matching.Regex

case class BatchInfo(records: Seq[SimpleRecord], hasKeys: Boolean, hasValues: Boolean)

class DumpLogSegmentsTest {
  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val segmentName = "00000000000000000000"
  val logFilePath = s"$logDir/$segmentName.log"
  val snapshotPath = s"$logDir/00000000000000000000-0000000000.checkpoint"
  val indexFilePath = s"$logDir/$segmentName.index"
  val timeIndexFilePath = s"$logDir/$segmentName.timeindex"
  val time = new MockTime(0, 0)
  var log: UnifiedLog = _

  @AfterEach
  def afterEach(): Unit = {
    Option(log).foreach(log => Utils.closeQuietly(log, "UnifiedLog"))
  }

  private def createTestLog = {
    val props = new Properties
    props.setProperty(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, "128")
    log = UnifiedLog(
      dir = logDir,
      config = new LogConfig(props),
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      time = time,
      brokerTopicStats = new BrokerTopicStats,
      maxTransactionTimeoutMs = 5 * 60 * 1000,
      producerStateManagerConfig = new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false),
      producerIdExpirationCheckIntervalMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
      logDirFailureChannel = new LogDirFailureChannel(10),
      topicId = None,
      keepPartitionMetadataFile = true
    )
    log
  }

  private def addSimpleRecords(log: UnifiedLog, batches: ArrayBuffer[BatchInfo]): Unit = {
    val now = System.currentTimeMillis()
    val firstBatchRecords = (0 until 10).map { i => new SimpleRecord(now + i * 2, s"message key $i".getBytes, s"message value $i".getBytes)}
    batches += BatchInfo(firstBatchRecords, hasKeys = true, hasValues = true)
    val secondBatchRecords = (10 until 30).map { i => new SimpleRecord(now + i * 3, s"message key $i".getBytes, null)}
    batches += BatchInfo(secondBatchRecords, hasKeys = true, hasValues = false)
    val thirdBatchRecords = (30 until 50).map { i => new SimpleRecord(now + i * 5, null, s"message value $i".getBytes)}
    batches += BatchInfo(thirdBatchRecords, hasKeys = false, hasValues = true)
    val fourthBatchRecords = (50 until 60).map { i => new SimpleRecord(now + i * 7, null)}
    batches += BatchInfo(fourthBatchRecords, hasKeys = false, hasValues = false)

    batches.foreach { batchInfo =>
      log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, 0, batchInfo.records: _*),
        leaderEpoch = 0)
    }
    // Flush, but don't close so that the indexes are not trimmed and contain some zero entries
    log.flush(false)
  }

  @Test
  def testBatchAndRecordMetadataOutput(): Unit = {
    log = createTestLog

    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, 0,
      new SimpleRecord("a".getBytes),
      new SimpleRecord("b".getBytes)
    ), leaderEpoch = 0)

    log.appendAsLeader(MemoryRecords.withRecords(Compression.gzip().build(), 0,
      new SimpleRecord(time.milliseconds(), "c".getBytes, "1".getBytes),
      new SimpleRecord("d".getBytes)
    ), leaderEpoch = 3)

    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, 0,
      new SimpleRecord("e".getBytes, null),
      new SimpleRecord(null, "f".getBytes),
      new SimpleRecord("g".getBytes)
    ), leaderEpoch = 3)

    log.appendAsLeader(MemoryRecords.withIdempotentRecords(Compression.NONE, 29342342L, 15.toShort, 234123,
      new SimpleRecord("h".getBytes)
    ), leaderEpoch = 3)

    log.appendAsLeader(MemoryRecords.withTransactionalRecords(Compression.gzip().build(), 98323L, 99.toShort, 266,
      new SimpleRecord("i".getBytes),
      new SimpleRecord("j".getBytes)
    ), leaderEpoch = 5)

    log.appendAsLeader(MemoryRecords.withEndTransactionMarker(98323L, 99.toShort,
      new EndTransactionMarker(ControlRecordType.COMMIT, 100)
    ), origin = AppendOrigin.COORDINATOR, leaderEpoch = 7)

    assertDumpLogRecordMetadata(log)
  }

  @Test
  def testPrintDataLog(): Unit = {
    log = createTestLog
    val batches = new ArrayBuffer[BatchInfo]
    addSimpleRecords(log, batches)
    
    def verifyRecordsInOutput(checkKeysAndValues: Boolean, args: Array[String]): Unit = {
      def isBatch(index: Int): Boolean = {
        var i = 0
        batches.zipWithIndex.foreach { case (batch, _) =>
          if (i == index)
            return true

          i += 1

          batch.records.indices.foreach { _ =>
            if (i == index)
              return false
            i += 1
          }
        }
        throw new AssertionError(s"No match for index $index")
      }

      val output = runDumpLogSegments(args)
      val lines = output.split("\n")
      assertTrue(lines.length > 2, s"Data not printed: $output")
      val totalRecords = batches.map(_.records.size).sum
      var offset = 0
      val batchIterator = batches.iterator
      var batch : BatchInfo = null
      (0 until totalRecords + batches.size).foreach { index =>
        val line = lines(lines.length - totalRecords - batches.size + index)
        // The base offset of the batch is the offset of the first record in the batch, so we
        // only increment the offset if it's not a batch
        if (isBatch(index)) {
          assertTrue(line.startsWith(s"baseOffset: $offset lastOffset: "), s"Not a valid batch-level message record: $line")
          batch = batchIterator.next()
        } else {
          assertTrue(line.startsWith(s"${DumpLogSegments.RecordIndent} offset: $offset"), s"Not a valid message record: $line")
          if (checkKeysAndValues) {
            var suffix = "headerKeys: []"
            if (batch.hasKeys)
              suffix += s" key: message key $offset"
            if (batch.hasValues)
              suffix += s" payload: message value $offset"
            assertTrue(line.endsWith(suffix), s"Message record missing key or value: $line")
          }
          offset += 1
        }
      }
    }

    def verifyNoRecordsInOutput(args: Array[String]): Unit = {
      val output = runDumpLogSegments(args)
      assertFalse(output.matches("(?s).*offset: [0-9]* isvalid.*"), s"Data should not have been printed: $output")
    }

    // Verify that records are printed with --print-data-log even if --deep-iteration is not specified
    verifyRecordsInOutput(checkKeysAndValues = true, Array("--print-data-log", "--files", logFilePath))
    // Verify that records are printed with --print-data-log if --deep-iteration is also specified
    verifyRecordsInOutput(checkKeysAndValues = true, Array("--print-data-log", "--deep-iteration", "--files", logFilePath))
    // Verify that records are printed with --value-decoder even if --print-data-log is not specified
    verifyRecordsInOutput(checkKeysAndValues = true, Array("--value-decoder-class", "org.apache.kafka.tools.api.StringDecoder", "--files", logFilePath))
    // Verify that records are printed with --key-decoder even if --print-data-log is not specified
    verifyRecordsInOutput(checkKeysAndValues = true, Array("--key-decoder-class", "org.apache.kafka.tools.api.StringDecoder", "--files", logFilePath))
    // Verify that records are printed with --deep-iteration even if --print-data-log is not specified
    verifyRecordsInOutput(checkKeysAndValues = false, Array("--deep-iteration", "--files", logFilePath))

    // Verify that records are not printed by default
    verifyNoRecordsInOutput(Array("--files", logFilePath))
  }

  @Test
  def testDumpIndexMismatches(): Unit = {
    log = createTestLog
    val batches = new ArrayBuffer[BatchInfo]
    addSimpleRecords(log, batches)
    
    val offsetMismatches = mutable.Map[String, List[(Long, Long)]]()
    DumpLogSegments.dumpIndex(new File(indexFilePath), indexSanityOnly = false, verifyOnly = true, offsetMismatches,
      Int.MaxValue)
    assertEquals(Map.empty, offsetMismatches)
  }

  @Test
  def testDumpTimeIndexErrors(): Unit = {
    log = createTestLog
    val batches = new ArrayBuffer[BatchInfo]
    addSimpleRecords(log, batches)
    
    val errors = new TimeIndexDumpErrors
    DumpLogSegments.dumpTimeIndex(new File(timeIndexFilePath), indexSanityOnly = false, verifyOnly = true, errors)
    assertEquals(Map.empty, errors.misMatchesForTimeIndexFilesMap)
    assertEquals(Map.empty, errors.outOfOrderTimestamp)
    assertEquals(Map.empty, errors.shallowOffsetNotFound)
  }

  def countSubstring(str: String, sub: String): Int =
    str.sliding(sub.length).count(_ == sub)
  
  // the number of batches in the log dump is equal to 
  // the number of occurrences of the "baseOffset:" substring
  def batchCount(str: String): Int =
    countSubstring(str, "baseOffset:")

  // the number of records in the log dump is equal to 
  // the number of occurrences of the "payload:" substring
  def recordCount(str: String): Int =
    countSubstring(str, "payload:")

  @Test
  def testDumpRemoteLogMetadataEmpty(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    log = LogTestUtils.createLog(logDir, logConfig, new BrokerTopicStats, time.scheduler, time)

    val output = runDumpLogSegments(Array("--remote-log-metadata-decoder", "--files", logFilePath))
    assertTrue(batchCount(output) == 0)
    assertTrue(recordCount(output) == 0)
    assertTrue(output.contains("Log starting offset: 0"))
  }

  @Test
  def testDumpRemoteLogMetadataOneRecordOneBatch(): Unit = {
    val topicId = Uuid.randomUuid
    val topicName = "foo"
    
    val metadata = Seq(new RemotePartitionDeleteMetadata(new TopicIdPartition(topicId, new TopicPartition(topicName, 0)), 
        RemotePartitionDeleteState.DELETE_PARTITION_MARKED, time.milliseconds, 0))

    val records: Array[SimpleRecord] = metadata.map(message => {
      new SimpleRecord(null, new RemoteLogMetadataSerde().serialize(message))
    }).toArray

    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    log = LogTestUtils.createLog(logDir, logConfig, new BrokerTopicStats, time.scheduler, time)
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, records:_*), leaderEpoch = 0)
    log.flush(false)

    val expectedDeletePayload = String.format("RemotePartitionDeleteMetadata{topicPartition=%s:%s-0, " +
      "state=DELETE_PARTITION_MARKED, eventTimestampMs=0, brokerId=0}", topicId, topicName)

    val output = runDumpLogSegments(Array("--remote-log-metadata-decoder", "--files", logFilePath))
    assertTrue(batchCount(output) == 1)
    assertTrue(recordCount(output) == 1)
    assertTrue(output.contains("Log starting offset: 0"))
    assertTrue(output.contains(expectedDeletePayload))
  }

  @Test
  def testDumpRemoteLogMetadataMultipleRecordsOneBatch(): Unit = {
    val topicId = Uuid.randomUuid
    val topicName = "foo"
    val remoteSegmentId = Uuid.randomUuid

    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(topicName, 0))
    val remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, remoteSegmentId)

    val metadata = Seq(new RemoteLogSegmentMetadataUpdate(remoteLogSegmentId, time.milliseconds,
        Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(Array[Byte](0, 1, 2, 3))), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 0),
      new RemotePartitionDeleteMetadata(topicIdPartition, RemotePartitionDeleteState.DELETE_PARTITION_MARKED, time.milliseconds, 0))

    val metadataRecords: Array[SimpleRecord] = metadata.map(message => {
      new SimpleRecord(null, new RemoteLogMetadataSerde().serialize(message))
    }).toArray

    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    log = LogTestUtils.createLog(logDir, logConfig, new BrokerTopicStats, time.scheduler, time)
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, metadataRecords:_*), leaderEpoch = 0)
    log.flush(false)

    val expectedUpdatePayload = String.format("RemoteLogSegmentMetadataUpdate{remoteLogSegmentId=" +
      "RemoteLogSegmentId{topicIdPartition=%s:%s-0, id=%s}, customMetadata=Optional[" +
      "CustomMetadata{4 bytes}], state=COPY_SEGMENT_FINISHED, eventTimestampMs=0, brokerId=0}", topicId, topicName, remoteSegmentId)
    val expectedDeletePayload = String.format("RemotePartitionDeleteMetadata{topicPartition=%s:%s-0, " +
      "state=DELETE_PARTITION_MARKED, eventTimestampMs=0, brokerId=0}", topicId, topicName)
    
    val output = runDumpLogSegments(Array("--remote-log-metadata-decoder", "--files", logFilePath))
    assertTrue(batchCount(output) == 1)
    assertTrue(recordCount(output) == 2)
    assertTrue(output.contains("Log starting offset: 0"))
    assertTrue(output.contains(expectedUpdatePayload))
    assertTrue(output.contains(expectedDeletePayload))
  }
  
  @Test
  def testDumpRemoteLogMetadataMultipleRecordsMultipleBatches(): Unit = {
    val topicId = Uuid.randomUuid
    val topicName = "foo"
    val remoteSegmentId = Uuid.randomUuid
    
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(topicName, 0))
    val remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, remoteSegmentId)

    val metadata = Seq(
      new RemoteLogSegmentMetadataUpdate(remoteLogSegmentId, time.milliseconds,
        Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(Array[Byte](0, 1, 2, 3))), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 0),
      new RemotePartitionDeleteMetadata(topicIdPartition, RemotePartitionDeleteState.DELETE_PARTITION_MARKED, time.milliseconds, 0)
    )

    val records: Array[SimpleRecord] = metadata.map(message => {
      new SimpleRecord(null, new RemoteLogMetadataSerde().serialize(message))
    }).toArray

    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    log = LogTestUtils.createLog(logDir, logConfig, new BrokerTopicStats, time.scheduler, time)
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, records:_*), leaderEpoch = 0)
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, records:_*), leaderEpoch = 0)
    log.flush(false)

    val expectedUpdatePayload = String.format("RemoteLogSegmentMetadataUpdate{remoteLogSegmentId=" +
      "RemoteLogSegmentId{topicIdPartition=%s:%s-0, id=%s}, customMetadata=Optional[" +
      "CustomMetadata{4 bytes}], state=COPY_SEGMENT_FINISHED, eventTimestampMs=0, brokerId=0}", topicId, topicName, remoteSegmentId)
    val expectedDeletePayload = String.format("RemotePartitionDeleteMetadata{topicPartition=%s:%s-0, " +
      "state=DELETE_PARTITION_MARKED, eventTimestampMs=0, brokerId=0}", topicId, topicName)

    val output = runDumpLogSegments(Array("--remote-log-metadata-decoder", "--files", logFilePath))
    assertTrue(batchCount(output) == 2)
    assertTrue(recordCount(output) == 4)
    assertTrue(output.contains("Log starting offset: 0"))
    assertTrue(countSubstring(output, expectedUpdatePayload) == 2)
    assertTrue(countSubstring(output, expectedDeletePayload) == 2)
  }

  @Test
  def testDumpRemoteLogMetadataNonZeroStartingOffset(): Unit = {
    val topicId = Uuid.randomUuid
    val topicName = "foo"
    
    val metadata = Seq(new RemotePartitionDeleteMetadata(new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
      RemotePartitionDeleteState.DELETE_PARTITION_MARKED, time.milliseconds, 0))

    val metadataRecords: Array[SimpleRecord] = metadata.map(message => {
      new SimpleRecord(null, new RemoteLogMetadataSerde().serialize(message))
    }).toArray
    
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    log = LogTestUtils.createLog(logDir, logConfig, new BrokerTopicStats, time.scheduler, time)
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, metadataRecords:_*), leaderEpoch = 0)
    val secondSegment = log.roll()
    secondSegment.append(1L, RecordBatch.NO_TIMESTAMP, 1L, MemoryRecords.withRecords(Compression.NONE, metadataRecords:_*))
    secondSegment.flush()
    log.flush(true)
    
    val expectedDeletePayload = String.format("RemotePartitionDeleteMetadata{topicPartition=%s:%s-0, " +
      "state=DELETE_PARTITION_MARKED, eventTimestampMs=0, brokerId=0}", topicId, topicName)

    val output = runDumpLogSegments(Array("--remote-log-metadata-decoder", "--files", secondSegment.log().file().getAbsolutePath))
    assertTrue(batchCount(output) == 1)
    assertTrue(recordCount(output) == 1)
    assertTrue(output.contains("Log starting offset: 1"))
    assertTrue(output.contains(expectedDeletePayload))
  }

  @Test
  def testDumpRemoteLogMetadataWithCorruption(): Unit = {
    val metadataRecords = Array(new SimpleRecord(null, "corrupted".getBytes()))

    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    log = LogTestUtils.createLog(logDir, logConfig, new BrokerTopicStats, time.scheduler, time)
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, metadataRecords:_*), leaderEpoch = 0)
    log.flush(false)

    val output = runDumpLogSegments(Array("--remote-log-metadata-decoder", "--files", logFilePath))
    assertTrue(batchCount(output) == 1)
    assertTrue(recordCount(output) == 1)
    assertTrue(output.contains("Log starting offset: 0"))
    assertTrue(output.contains("Could not deserialize metadata record"))
  }

  @Test
  def testDumpRemoteLogMetadataIoException(): Unit = {
    val topicId = Uuid.randomUuid
    val topicName = "foo"

    val metadata = Seq(new RemotePartitionDeleteMetadata(new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
      RemotePartitionDeleteState.DELETE_PARTITION_MARKED, time.milliseconds, 0))

    val metadataRecords: Array[SimpleRecord] = metadata.map(message => {
      new SimpleRecord(null, new RemoteLogMetadataSerde().serialize(message))
    }).toArray
    
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    log = LogTestUtils.createLog(logDir, logConfig, new BrokerTopicStats, time.scheduler, time)
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, metadataRecords:_*), leaderEpoch = 0)
    log.flush(false)
    
    Files.setPosixFilePermissions(Paths.get(logFilePath), PosixFilePermissions.fromString("-w-------"))
    
    assertThrows(classOf[AccessDeniedException],
      () => runDumpLogSegments(Array("--remote-log-metadata-decoder", "--files", logFilePath)))
  }

  @Test
  def testDumpRemoteLogMetadataNoFilesFlag(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message))
    try {
      val thrown = assertThrows(classOf[IllegalArgumentException], () => runDumpLogSegments(Array("--remote-log-metadata-decoder")))
      assertTrue(thrown.getMessage.equals("Missing required argument \"[files]\""))
    } finally {
      Exit.resetExitProcedure()
    }
  }

  @Test
  def testDumpRemoteLogMetadataNoSuchFileException(): Unit = {
    val noSuchFileLogPath = "/tmp/nosuchfile/00000000000000000000.log"
    assertThrows(classOf[NoSuchFileException], 
      () => runDumpLogSegments(Array("--remote-log-metadata-decoder", "--files", noSuchFileLogPath)))
  }
  
  @Test
  def testDumpMetadataRecords(): Unit = {
    val mockTime = new MockTime
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    log = LogTestUtils.createLog(logDir, logConfig, new BrokerTopicStats, mockTime.scheduler, mockTime)

    val metadataRecords = Seq(
      new ApiMessageAndVersion(
        new RegisterBrokerRecord().setBrokerId(0).setBrokerEpoch(10), 0.toShort),
      new ApiMessageAndVersion(
        new RegisterBrokerRecord().setBrokerId(1).setBrokerEpoch(20), 0.toShort),
      new ApiMessageAndVersion(
        new TopicRecord().setName("test-topic").setTopicId(Uuid.randomUuid()), 0.toShort),
      new ApiMessageAndVersion(
        new PartitionChangeRecord().setTopicId(Uuid.randomUuid()).setLeader(1).
          setPartitionId(0).setIsr(util.Arrays.asList(0, 1, 2)), 0.toShort)
    )

    val records: Array[SimpleRecord] = metadataRecords.map(message => {
      val serde = MetadataRecordSerde.INSTANCE
      val cache = new ObjectSerializationCache
      val size = serde.recordSize(message, cache)
      val buf = ByteBuffer.allocate(size)
      val writer = new ByteBufferAccessor(buf)
      serde.write(message, cache, writer)
      buf.flip()
      new SimpleRecord(null, buf.array)
    }).toArray
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, records:_*), leaderEpoch = 1)
    log.flush(false)

    var output = runDumpLogSegments(Array("--cluster-metadata-decoder", "--files", logFilePath))
    assertTrue(output.contains("Log starting offset: 0"))
    assertTrue(output.contains("TOPIC_RECORD"))
    assertTrue(output.contains("BROKER_RECORD"))

    output = runDumpLogSegments(Array("--cluster-metadata-decoder", "--skip-record-metadata", "--files", logFilePath))
    assertTrue(output.contains("TOPIC_RECORD"))
    assertTrue(output.contains("BROKER_RECORD"))

    // Bogus metadata record
    val buf = ByteBuffer.allocate(4)
    val writer = new ByteBufferAccessor(buf)
    writer.writeUnsignedVarint(10000)
    writer.writeUnsignedVarint(10000)
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(null, buf.array)), leaderEpoch = 2)
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, records:_*), leaderEpoch = 2)

    output = runDumpLogSegments(Array("--cluster-metadata-decoder", "--skip-record-metadata", "--files", logFilePath))
    assertTrue(output.contains("TOPIC_RECORD"))
    assertTrue(output.contains("BROKER_RECORD"))
    assertTrue(output.contains("skipping"))
  }

  @Test
  def testDumpMetadataSnapshot(): Unit = {
    val metadataRecords = Seq(
      new ApiMessageAndVersion(
        new RegisterBrokerRecord().setBrokerId(0).setBrokerEpoch(10), 0.toShort),
      new ApiMessageAndVersion(
        new RegisterBrokerRecord().setBrokerId(1).setBrokerEpoch(20), 0.toShort),
      new ApiMessageAndVersion(
        new TopicRecord().setName("test-topic").setTopicId(Uuid.randomUuid()), 0.toShort),
      new ApiMessageAndVersion(
        new PartitionChangeRecord().setTopicId(Uuid.randomUuid()).setLeader(1).
          setPartitionId(0).setIsr(util.Arrays.asList(0, 1, 2)), 0.toShort)
    )

    val metadataLog = KafkaMetadataLog(
      KafkaRaftServer.MetadataPartition,
      KafkaRaftServer.MetadataTopicId,
      logDir,
      time,
      time.scheduler,
      MetadataLogConfig(
        logSegmentBytes = 100 * 1024,
        logSegmentMinBytes = 100 * 1024,
        logSegmentMillis = 10 * 1000,
        retentionMaxBytes = 100 * 1024,
        retentionMillis = 60 * 1000,
        maxBatchSizeInBytes = KafkaRaftClient.MAX_BATCH_SIZE_BYTES,
        maxFetchSizeInBytes = KafkaRaftClient.MAX_FETCH_SIZE_BYTES,
        fileDeleteDelayMs = ServerLogConfigs.LOG_DELETE_DELAY_MS_DEFAULT,
        nodeId = 1
      )
    )

    val lastContainedLogTimestamp = 10000

    Using.resource(
      new RecordsSnapshotWriter.Builder()
        .setTime(new MockTime)
        .setLastContainedLogTimestamp(lastContainedLogTimestamp)
        .setRawSnapshotWriter(metadataLog.createNewSnapshot(new OffsetAndEpoch(0, 0)).get)
        .setKraftVersion(KRaftVersion.KRAFT_VERSION_1)
        .setVoterSet(Optional.of(VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(1, 2, 3), true))))
        .build(MetadataRecordSerde.INSTANCE)
    ) { snapshotWriter =>
      snapshotWriter.append(metadataRecords.asJava)
      snapshotWriter.freeze()
    }

    var output = runDumpLogSegments(Array("--cluster-metadata-decoder", "--files", snapshotPath))
    assertTrue(output.contains("Snapshot end offset: 0, epoch: 0"), output)
    assertTrue(output.contains("TOPIC_RECORD"), output)
    assertTrue(output.contains("BROKER_RECORD"), output)
    assertTrue(output.contains("SnapshotHeader"), output)
    assertTrue(output.contains("SnapshotFooter"), output)
    assertTrue(output.contains("KRaftVersion"), output)
    assertTrue(output.contains("KRaftVoters"), output)
    assertTrue(output.contains(s""""lastContainedLogTimestamp":$lastContainedLogTimestamp"""), output)

    output = runDumpLogSegments(Array("--cluster-metadata-decoder", "--skip-record-metadata", "--files", snapshotPath))
    assertTrue(output.contains("Snapshot end offset: 0, epoch: 0"), output)
    assertTrue(output.contains("TOPIC_RECORD"), output)
    assertTrue(output.contains("BROKER_RECORD"), output)
    assertFalse(output.contains("SnapshotHeader"), output)
    assertFalse(output.contains("SnapshotFooter"), output)
    assertFalse(output.contains("KRaftVersion"), output)
    assertFalse(output.contains("KRaftVoters"), output)
    assertFalse(output.contains(s""""lastContainedLogTimestamp": $lastContainedLogTimestamp"""), output)
  }

  @Test
  def testDumpEmptyIndex(): Unit = {
    log = createTestLog
    
    val indexFile = new File(indexFilePath)
    new PrintWriter(indexFile).close()
    val expectOutput = s"$indexFile is empty.\n"
    val outContent = new ByteArrayOutputStream()
    Console.withOut(outContent) {
      DumpLogSegments.dumpIndex(indexFile, indexSanityOnly = false, verifyOnly = true,
        misMatchesForIndexFilesMap = mutable.Map[String, List[(Long, Long)]](), Int.MaxValue)
    }
    assertEquals(expectOutput, outContent.toString)
  }

  private def runDumpLogSegments(args: Array[String]): String = {
    val outContent = new ByteArrayOutputStream
    Console.withOut(outContent) {
      DumpLogSegments.main(args)
    }
    outContent.toString
  }

  @Test
  def testPrintDataLogPartialBatches(): Unit = {
    log = createTestLog
    val batches = new ArrayBuffer[BatchInfo]
    addSimpleRecords(log, batches)
    
    val totalBatches = batches.size
    val partialBatches = totalBatches / 2

    // Get all the batches
    val output = runDumpLogSegments(Array("--files", logFilePath))
    val lines = util.Arrays.asList(output.split("\n"): _*).listIterator()

    // Get total bytes of the partial batches
    val partialBatchesBytes = readPartialBatchesBytes(lines, partialBatches)

    // Request only the partial batches by bytes
    val partialOutput = runDumpLogSegments(Array("--max-bytes", partialBatchesBytes.toString, "--files", logFilePath))
    val partialLines = util.Arrays.asList(partialOutput.split("\n"): _*).listIterator()

    // Count the total of partial batches limited by bytes
    val partialBatchesCount = countBatches(partialLines)

    assertEquals(partialBatches, partialBatchesCount)
  }

  @Test
  def testOffsetsMessageParser(): Unit = {
    val serde = new GroupCoordinatorRecordSerde()
    val parser = new OffsetsMessageParser()

    def serializedRecord(key: ApiMessageAndVersion, value: ApiMessageAndVersion): Record = {
      val record = new CoordinatorRecord(key, value)
      TestUtils.singletonRecords(
        key = serde.serializeKey(record),
        value = serde.serializeValue(record)
      ).records.iterator.next
    }

    // The key is mandatory.
    assertEquals(
      "Failed to decode message at offset 0 using offset topic decoder (message had a missing key)",
      assertThrows(
        classOf[RuntimeException],
        () => parser.parse(TestUtils.singletonRecords(key = null, value = null).records.iterator.next)
      ).getMessage
    )

    // A valid key and value should work.
    assertEquals(
      (
        Some("{\"type\":\"3\",\"data\":{\"groupId\":\"group\"}}"),
        Some("{\"version\":\"0\",\"data\":{\"epoch\":10}}")
      ),
      parser.parse(serializedRecord(
        new ApiMessageAndVersion(
          new ConsumerGroupMetadataKey()
            .setGroupId("group"),
          3.toShort
        ),
        new ApiMessageAndVersion(
          new ConsumerGroupMetadataValue()
            .setEpoch(10),
          0.toShort
        )
      ))
    )

    // Consumer embedded protocol is parsed if possible.
    assertEquals(
      (
        Some("{\"type\":\"2\",\"data\":{\"group\":\"group\"}}"),
        Some("{\"version\":\"4\",\"data\":{\"protocolType\":\"consumer\",\"generation\":10,\"protocol\":\"range\"," +
             "\"leader\":\"member\",\"currentStateTimestamp\":-1,\"members\":[{\"memberId\":\"member\"," +
             "\"groupInstanceId\":\"instance\",\"clientId\":\"client\",\"clientHost\":\"host\"," +
             "\"rebalanceTimeout\":1000,\"sessionTimeout\":100,\"subscription\":{\"topics\":[\"foo\"]," +
             "\"userData\":null,\"ownedPartitions\":[{\"topic\":\"foo\",\"partitions\":[0]}]," +
             "\"generationId\":0,\"rackId\":\"rack\"},\"assignment\":{\"assignedPartitions\":" +
             "[{\"topic\":\"foo\",\"partitions\":[0]}],\"userData\":null}}]}}")
      ),
      parser.parse(serializedRecord(
        new ApiMessageAndVersion(
          new GroupMetadataKey()
            .setGroup("group"),
          2.toShort
        ),
        new ApiMessageAndVersion(
          new GroupMetadataValue()
            .setProtocolType("consumer")
            .setProtocol("range")
            .setLeader("member")
            .setGeneration(10)
            .setMembers(Collections.singletonList(
              new GroupMetadataValue.MemberMetadata()
                .setMemberId("member")
                .setClientId("client")
                .setClientHost("host")
                .setGroupInstanceId("instance")
                .setSessionTimeout(100)
                .setRebalanceTimeout(1000)
                .setSubscription(Utils.toArray(ConsumerProtocol.serializeSubscription(
                  new Subscription(
                    Collections.singletonList("foo"),
                    null,
                    Collections.singletonList(new TopicPartition("foo", 0)),
                    0,
                    Optional.of("rack")))))
                .setAssignment(Utils.toArray(ConsumerProtocol.serializeAssignment(
                  new Assignment(Collections.singletonList(new TopicPartition("foo", 0))))))
            )),
          GroupMetadataValue.HIGHEST_SUPPORTED_VERSION
        )
      ))
    )

    // Consumer embedded protocol is not parsed if malformed.
    assertEquals(
      (
        Some("{\"type\":\"2\",\"data\":{\"group\":\"group\"}}"),
        Some("{\"version\":\"4\",\"data\":{\"protocolType\":\"consumer\",\"generation\":10,\"protocol\":\"range\"," +
             "\"leader\":\"member\",\"currentStateTimestamp\":-1,\"members\":[{\"memberId\":\"member\"," +
             "\"groupInstanceId\":\"instance\",\"clientId\":\"client\",\"clientHost\":\"host\"," +
             "\"rebalanceTimeout\":1000,\"sessionTimeout\":100,\"subscription\":\"U3Vic2NyaXB0aW9u\"," +
             "\"assignment\":\"QXNzaWdubWVudA==\"}]}}")
      ),
      parser.parse(serializedRecord(
        new ApiMessageAndVersion(
          new GroupMetadataKey()
            .setGroup("group"),
          2.toShort
        ),
        new ApiMessageAndVersion(
          new GroupMetadataValue()
            .setProtocolType("consumer")
            .setProtocol("range")
            .setLeader("member")
            .setGeneration(10)
            .setMembers(Collections.singletonList(
              new GroupMetadataValue.MemberMetadata()
                .setMemberId("member")
                .setClientId("client")
                .setClientHost("host")
                .setGroupInstanceId("instance")
                .setSessionTimeout(100)
                .setRebalanceTimeout(1000)
                .setSubscription("Subscription".getBytes)
                .setAssignment("Assignment".getBytes)
            )),
          GroupMetadataValue.HIGHEST_SUPPORTED_VERSION
        )
      ))
    )

    // A valid key with a tombstone should work.
    assertEquals(
      (
        Some("{\"type\":\"3\",\"data\":{\"groupId\":\"group\"}}"),
        Some("<DELETE>")
      ),
      parser.parse(serializedRecord(
        new ApiMessageAndVersion(
          new ConsumerGroupMetadataKey()
            .setGroupId("group"),
          3.toShort
        ),
        null
      ))
    )

    // An unknown record type should be handled and reported as such.
    assertEquals(
      (
        Some(
          "Unknown record type 32767 at offset 0, skipping."
        ),
        None
      ),
      parser.parse(serializedRecord(
        new ApiMessageAndVersion(
          new ConsumerGroupMetadataKey()
            .setGroupId("group"),
          Short.MaxValue // Invalid record id.
        ),
        new ApiMessageAndVersion(
          new ConsumerGroupMetadataValue()
            .setEpoch(10),
          0.toShort
        )
      ))
    )

    // Any parsing error is swallowed and reported.
    assertEquals(
      (
        Some(
          "Error at offset 0, skipping. Could not read record with version 0 from value's buffer due to: " +
            "Error reading byte array of 536870911 byte(s): only 1 byte(s) available."
        ),
        None
      ),
      parser.parse(serializedRecord(
        new ApiMessageAndVersion(
          new ConsumerGroupMetadataKey()
            .setGroupId("group"),
          3.toShort
        ),
        new ApiMessageAndVersion(
          new ConsumerGroupMemberMetadataValue(), // The value does correspond to the record id.
          0.toShort
        )
      ))
    )
  }

  private def readBatchMetadata(lines: util.ListIterator[String]): Option[String] = {
    while (lines.hasNext) {
      val line = lines.next()
      if (line.startsWith("|")) {
        throw new IllegalStateException("Read unexpected record entry")
      } else if (line.startsWith("baseOffset")) {
        return Some(line)
      }
    }
    None
  }

  // Returns the total bytes of the batches specified
  private def readPartialBatchesBytes(lines: util.ListIterator[String], limit: Int): Int = {
    val sizePattern: Regex = raw".+?size:\s(\d+).+".r
    var batchesBytes = 0
    var batchesCounter = 0
    while (lines.hasNext) {
      if (batchesCounter >= limit) {
        return batchesBytes
      }
      val line = lines.next()
      if (line.startsWith("baseOffset")) {
        line match {
          case sizePattern(size) => batchesBytes += size.toInt
          case _ => throw new IllegalStateException(s"Failed to parse and find size value for batch line: $line")
        }
        batchesCounter += 1
      }
    }
    batchesBytes
  }

  private def countBatches(lines: util.ListIterator[String]): Int = {
    var countBatches = 0
    while (lines.hasNext) {
      val line = lines.next()
      if (line.startsWith("baseOffset")) {
        countBatches += 1
      }
    }
    countBatches
  }

  private def readBatchRecords(lines: util.ListIterator[String]): Seq[String] = {
    val records = mutable.ArrayBuffer.empty[String]
    while (lines.hasNext) {
      val line = lines.next()
      if (line.startsWith("|")) {
        records += line.substring(1)
      } else {
        lines.previous()
        return records.toSeq
      }
    }
    records.toSeq
  }

  private def parseMetadataFields(line: String): Map[String, String] = {
    val fields = mutable.Map.empty[String, String]
    val tokens = line.split("\\s+").map(_.trim()).filter(_.nonEmpty).iterator

    while (tokens.hasNext) {
      val token = tokens.next()
      if (!token.endsWith(":")) {
        throw new IllegalStateException(s"Unexpected non-field token $token")
      }

      val field = token.substring(0, token.length - 1)
      if (!tokens.hasNext) {
        throw new IllegalStateException(s"Failed to parse value for $field")
      }

      val value = tokens.next()
      fields += field -> value
    }

    fields.toMap
  }

  private def assertDumpLogRecordMetadata(log: UnifiedLog): Unit = {
    val logReadInfo = log.read(
      startOffset = 0,
      maxLength = Int.MaxValue,
      isolation = FetchIsolation.LOG_END,
      minOneMessage = true
    )

    val output = runDumpLogSegments(Array("--deep-iteration", "--files", logFilePath))
    val lines = util.Arrays.asList(output.split("\n"): _*).listIterator()

    for (batch <- logReadInfo.records.batches.asScala) {
      val parsedBatchOpt = readBatchMetadata(lines)
      assertTrue(parsedBatchOpt.isDefined)

      val parsedBatch = parseMetadataFields(parsedBatchOpt.get)
      assertEquals(Some(batch.baseOffset), parsedBatch.get("baseOffset").map(_.toLong))
      assertEquals(Some(batch.lastOffset), parsedBatch.get("lastOffset").map(_.toLong))
      assertEquals(Option(batch.countOrNull), parsedBatch.get("count").map(_.toLong))
      assertEquals(Some(batch.partitionLeaderEpoch), parsedBatch.get("partitionLeaderEpoch").map(_.toInt))
      assertEquals(Some(batch.isTransactional), parsedBatch.get("isTransactional").map(_.toBoolean))
      assertEquals(Some(batch.isControlBatch), parsedBatch.get("isControl").map(_.toBoolean))
      assertEquals(Some(batch.producerId), parsedBatch.get("producerId").map(_.toLong))
      assertEquals(Some(batch.producerEpoch), parsedBatch.get("producerEpoch").map(_.toShort))
      assertEquals(Some(batch.baseSequence), parsedBatch.get("baseSequence").map(_.toInt))
      assertEquals(Some(batch.compressionType.name), parsedBatch.get("compresscodec"))

      val parsedRecordIter = readBatchRecords(lines).iterator
      for (record <- batch.asScala) {
        assertTrue(parsedRecordIter.hasNext)
        val parsedRecord = parseMetadataFields(parsedRecordIter.next())
        assertEquals(Some(record.offset), parsedRecord.get("offset").map(_.toLong))
        assertEquals(Some(record.keySize), parsedRecord.get("keySize").map(_.toInt))
        assertEquals(Some(record.valueSize), parsedRecord.get("valueSize").map(_.toInt))
        assertEquals(Some(record.timestamp), parsedRecord.get(batch.timestampType.name).map(_.toLong))

        if (batch.magic >= RecordVersion.V2.value) {
          assertEquals(Some(record.sequence), parsedRecord.get("sequence").map(_.toInt))
        }

        // Batch fields should not be present in the record output
        assertEquals(None, parsedRecord.get("baseOffset"))
        assertEquals(None, parsedRecord.get("lastOffset"))
        assertEquals(None, parsedRecord.get("partitionLeaderEpoch"))
        assertEquals(None, parsedRecord.get("producerId"))
        assertEquals(None, parsedRecord.get("producerEpoch"))
        assertEquals(None, parsedRecord.get("baseSequence"))
        assertEquals(None, parsedRecord.get("isTransactional"))
        assertEquals(None, parsedRecord.get("isControl"))
        assertEquals(None, parsedRecord.get("compresscodec"))
      }
    }
  }

  @Test
  def testShareGroupStateMessageParser(): Unit = {
    val serde = new ShareCoordinatorRecordSerde()
    val parser = new ShareGroupStateMessageParser()

    def serializedRecord(key: ApiMessageAndVersion, value: ApiMessageAndVersion): Record = {
      val record = new CoordinatorRecord(key, value)
      TestUtils.singletonRecords(
        key = serde.serializeKey(record),
        value = serde.serializeValue(record)
      ).records.iterator.next
    }

    // The key is mandatory.
    assertEquals(
      "Failed to decode message at offset 0 using share group state topic decoder (message had a missing key)",
      assertThrows(
        classOf[RuntimeException],
        () => parser.parse(TestUtils.singletonRecords(key = null, value = null).records.iterator.next)
      ).getMessage
    )

    // A valid key and value should work (ShareSnapshot).
    assertEquals(
      (
        Some("{\"type\":\"0\",\"data\":{\"groupId\":\"gs1\",\"topicId\":\"Uj5wn_FqTXirEASvVZRY1w\",\"partition\":0}}"),
        Some("{\"type\":\"0\",\"data\":{\"snapshotEpoch\":0,\"stateEpoch\":0,\"leaderEpoch\":0,\"startOffset\":0,\"stateBatches\":[{\"firstOffset\":0,\"lastOffset\":4,\"deliveryState\":2,\"deliveryCount\":1}]}}")
      ),
      parser.parse(serializedRecord(
        new ApiMessageAndVersion(new ShareSnapshotKey()
          .setGroupId("gs1")
          .setTopicId(Uuid.fromString("Uj5wn_FqTXirEASvVZRY1w"))
          .setPartition(0),
          ShareCoordinator.SHARE_SNAPSHOT_RECORD_KEY_VERSION),
        new ApiMessageAndVersion(new ShareSnapshotValue()
          .setSnapshotEpoch(0)
          .setStateEpoch(0)
          .setLeaderEpoch(0)
          .setStartOffset(0)
          .setStateBatches(List[ShareSnapshotValue.StateBatch](
            new ShareSnapshotValue.StateBatch()
              .setFirstOffset(0)
              .setLastOffset(4)
              .setDeliveryState(2)
              .setDeliveryCount(1)
          ).asJava),
          ShareCoordinator.SHARE_SNAPSHOT_RECORD_VALUE_VERSION)
      ))
    )

    // A valid key and value should work (ShareUpdate).
    assertEquals(
      (
        Some("{\"type\":\"1\",\"data\":{\"groupId\":\"gs1\",\"topicId\":\"Uj5wn_FqTXirEASvVZRY1w\",\"partition\":0}}"),
        Some("{\"type\":\"0\",\"data\":{\"snapshotEpoch\":0,\"leaderEpoch\":0,\"startOffset\":0,\"stateBatches\":[{\"firstOffset\":0,\"lastOffset\":4,\"deliveryState\":2,\"deliveryCount\":1}]}}")
      ),
      parser.parse(serializedRecord(
        new ApiMessageAndVersion(new ShareUpdateKey()
          .setGroupId("gs1")
          .setTopicId(Uuid.fromString("Uj5wn_FqTXirEASvVZRY1w"))
          .setPartition(0),
          ShareCoordinator.SHARE_UPDATE_RECORD_KEY_VERSION),
        new ApiMessageAndVersion(new ShareUpdateValue()
          .setSnapshotEpoch(0)
          .setLeaderEpoch(0)
          .setStartOffset(0)
          .setStateBatches(List[ShareUpdateValue.StateBatch](
            new ShareUpdateValue.StateBatch()
              .setFirstOffset(0)
              .setLastOffset(4)
              .setDeliveryState(2)
              .setDeliveryCount(1)
          ).asJava),
          0.toShort)
      ))
    )

    // A valid key with a tombstone should work.
    assertEquals(
      (
        Some("{\"type\":\"0\",\"data\":{\"groupId\":\"gs1\",\"topicId\":\"Uj5wn_FqTXirEASvVZRY1w\",\"partition\":0}}"),
        Some("<DELETE>")
      ),
      parser.parse(serializedRecord(
        new ApiMessageAndVersion(
          new ShareSnapshotKey()
            .setGroupId("gs1")
            .setTopicId(Uuid.fromString("Uj5wn_FqTXirEASvVZRY1w"))
            .setPartition(0),
          0.toShort
        ),
        null
      ))
    )

    // An unknown record type should be handled and reported as such.
    assertEquals(
      (
        Some(
          "Unknown record type 32767 at offset 0, skipping."
        ),
        None
      ),
      parser.parse(serializedRecord(
        new ApiMessageAndVersion(
          new ShareSnapshotKey()
            .setGroupId("group")
            .setTopicId(Uuid.fromString("Uj5wn_FqTXirEASvVZRY1w"))
            .setPartition(0),
          Short.MaxValue // Invalid record id.
        ),
        new ApiMessageAndVersion(
          new ShareSnapshotValue()
            .setSnapshotEpoch(0),
          0.toShort
        )
      ))
    )

    // Any parsing error is swallowed and reported.
    assertEquals(
      (
        Some(
          "Error at offset 0, skipping. Could not read record with version 0 from value's buffer due to: " +
          "non-nullable field stateBatches was serialized as null."
        ),
        None
      ),
      parser.parse(serializedRecord(
        new ApiMessageAndVersion(
          new ShareUpdateKey()
            .setGroupId("group")
            .setTopicId(Uuid.fromString("Uj5wn_FqTXirEASvVZRY1w"))
            .setPartition(0),
          1.toShort
        ),
        new ApiMessageAndVersion(
          new ShareSnapshotValue(), // incorrect class to deserialize the snapshot update value
          0.toShort
        )
      ))
    )
  }
}
