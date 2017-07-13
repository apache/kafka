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

import java.io._
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.Properties

import org.apache.kafka.common.errors._
import kafka.api.ApiVersion
import kafka.common.KafkaException
import org.junit.Assert._
import org.junit.{After, Before, Test}
import kafka.utils._
import kafka.server.{BrokerTopicStats, KafkaConfig}
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords.RecordFilter
import org.apache.kafka.common.record.MemoryRecords.RecordFilter.BatchRetention
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.utils.Utils
import org.easymock.EasyMock

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class LogTest {

  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val time = new MockTime()
  var config: KafkaConfig = null
  val logConfig = LogConfig()
  val brokerTopicStats = new BrokerTopicStats

  @Before
  def setUp() {
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
    config = KafkaConfig.fromProps(props)
  }

  @After
  def tearDown() {
    brokerTopicStats.close()
    Utils.delete(tmpDir)
  }

  def createEmptyLogs(dir: File, offsets: Int*) {
    for(offset <- offsets) {
      Log.logFile(dir, offset).createNewFile()
      Log.offsetIndexFile(dir, offset).createNewFile()
    }
  }

  @Test
  def testOffsetFromFilename() {
    val offset = 23423423L

    val logFile = Log.logFile(tmpDir, offset)
    assertEquals(offset, Log.offsetFromFilename(logFile.getName))

    val offsetIndexFile = Log.offsetIndexFile(tmpDir, offset)
    assertEquals(offset, Log.offsetFromFilename(offsetIndexFile.getName))

    val timeIndexFile = Log.timeIndexFile(tmpDir, offset)
    assertEquals(offset, Log.offsetFromFilename(timeIndexFile.getName))

    val snapshotFile = Log.producerSnapshotFile(tmpDir, offset)
    assertEquals(offset, Log.offsetFromFilename(snapshotFile.getName))
  }

  /**
   * Tests for time based log roll. This test appends messages then changes the time
   * using the mock clock to force the log to roll and checks the number of segments.
   */
  @Test
  def testTimeBasedLogRoll() {
    def createRecords = TestUtils.singletonRecords("test".getBytes)

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentMsProp, 1 * 60 * 60L: java.lang.Long)
    logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp, Long.MaxValue.toString)

    // create a log
    val log = Log(logDir,
      LogConfig(logProps),
      logStartOffset = 0L,
      recoveryPoint = 0L,
      maxProducerIdExpirationMs = 24 * 60,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)
    assertEquals("Log begins with a single empty segment.", 1, log.numberOfSegments)
    // Test the segment rolling behavior when messages do not have a timestamp.
    time.sleep(log.config.segmentMs + 1)
    log.appendAsLeader(createRecords, leaderEpoch = 0)
    assertEquals("Log doesn't roll if doing so creates an empty segment.", 1, log.numberOfSegments)

    log.appendAsLeader(createRecords, leaderEpoch = 0)
    assertEquals("Log rolls on this append since time has expired.", 2, log.numberOfSegments)

    for (numSegments <- 3 until 5) {
      time.sleep(log.config.segmentMs + 1)
      log.appendAsLeader(createRecords, leaderEpoch = 0)
      assertEquals("Changing time beyond rollMs and appending should create a new segment.", numSegments, log.numberOfSegments)
    }

    // Append a message with timestamp to a segment whose first message do not have a timestamp.
    val timestamp = time.milliseconds + log.config.segmentMs + 1
    def createRecordsWithTimestamp = TestUtils.singletonRecords(value = "test".getBytes, timestamp = timestamp)
    log.appendAsLeader(createRecordsWithTimestamp, leaderEpoch = 0)
    assertEquals("Segment should not have been rolled out because the log rolling should be based on wall clock.", 4, log.numberOfSegments)

    // Test the segment rolling behavior when messages have timestamps.
    time.sleep(log.config.segmentMs + 1)
    log.appendAsLeader(createRecordsWithTimestamp, leaderEpoch = 0)
    assertEquals("A new segment should have been rolled out", 5, log.numberOfSegments)

    // move the wall clock beyond log rolling time
    time.sleep(log.config.segmentMs + 1)
    log.appendAsLeader(createRecordsWithTimestamp, leaderEpoch = 0)
    assertEquals("Log should not roll because the roll should depend on timestamp of the first message.", 5, log.numberOfSegments)

    val recordWithExpiredTimestamp = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds)
    log.appendAsLeader(recordWithExpiredTimestamp, leaderEpoch = 0)
    assertEquals("Log should roll because the timestamp in the message should make the log segment expire.", 6, log.numberOfSegments)

    val numSegments = log.numberOfSegments
    time.sleep(log.config.segmentMs + 1)
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE), leaderEpoch = 0)
    assertEquals("Appending an empty message set should not roll log even if sufficient time has passed.", numSegments, log.numberOfSegments)

    log.close()
  }

  @Test(expected = classOf[OutOfOrderSequenceException])
  def testNonSequentialAppend(): Unit = {
    val logProps = new Properties()

    // create a log
    val log = Log(logDir,
      LogConfig(logProps),
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      time = time,
      brokerTopicStats = new BrokerTopicStats)

    val pid = 1L
    val epoch: Short = 0

    val records = TestUtils.records(List(new SimpleRecord(time.milliseconds, "key".getBytes, "value".getBytes)), producerId = pid, producerEpoch = epoch, sequence = 0)
    log.appendAsLeader(records, leaderEpoch = 0)

    val nextRecords = TestUtils.records(List(new SimpleRecord(time.milliseconds, "key".getBytes, "value".getBytes)), producerId = pid, producerEpoch = epoch, sequence = 2)
    try {
      log.appendAsLeader(nextRecords, leaderEpoch = 0)
    } finally {
      log.close()
    }
  }

  @Test
  def testInitializationOfProducerSnapshotsUpgradePath(): Unit = {
    // simulate the upgrade path by creating a new log with several segments, deleting the
    // snapshot files, and then reloading the log

    val log = createLog(64, messagesPerSegment = 10)
    assertEquals(None, log.oldestProducerSnapshotOffset)

    for (i <- 0 to 100) {
      val record = new SimpleRecord(time.milliseconds, i.toString.getBytes)
      log.appendAsLeader(TestUtils.records(List(record)), leaderEpoch = 0)
    }

    assertTrue(log.logSegments.size >= 2)
    log.close()

    logDir.listFiles.filter(f => f.isFile && f.getName.endsWith(Log.PidSnapshotFileSuffix)).foreach { file =>
      Files.delete(file.toPath)
    }

    val reloadedLog = createLog(64, messagesPerSegment = 10)
    val expectedSnapshotsOffsets = log.logSegments.toSeq.reverse.take(2).map(_.baseOffset) ++ Seq(reloadedLog.logEndOffset)
    expectedSnapshotsOffsets.foreach { offset =>
      assertTrue(Log.producerSnapshotFile(logDir, offset).exists)
    }
    reloadedLog.close()
  }

  @Test
  def testSizeForLargeLogs(): Unit = {
    val largeSize = Int.MaxValue.toLong * 2
    val logSegment = EasyMock.createMock(classOf[LogSegment])

    EasyMock.expect(logSegment.size).andReturn(Int.MaxValue).anyTimes
    EasyMock.replay(logSegment)

    assertEquals(Int.MaxValue, Log.sizeInBytes(Seq(logSegment)))
    assertEquals(largeSize, Log.sizeInBytes(Seq(logSegment, logSegment)))
    assertTrue(Log.sizeInBytes(Seq(logSegment, logSegment)) > Int.MaxValue)
  }

  @Test
  def testPidMapOffsetUpdatedForNonIdempotentData() {
    val log = createLog(2048)
    val records = TestUtils.records(List(new SimpleRecord(time.milliseconds, "key".getBytes, "value".getBytes)))
    log.appendAsLeader(records, leaderEpoch = 0)
    log.takeProducerSnapshot()
    assertEquals(Some(1), log.latestProducerSnapshotOffset)
    log.close()
  }

  @Test
  def testSkipLoadingIfEmptyProducerStateBeforeTruncation(): Unit = {
    val stateManager = EasyMock.mock(classOf[ProducerStateManager])

    // Load the log
    EasyMock.expect(stateManager.latestSnapshotOffset).andReturn(None)

    stateManager.updateMapEndOffset(0L)
    EasyMock.expectLastCall().anyTimes()

    EasyMock.expect(stateManager.mapEndOffset).andStubReturn(0L)
    EasyMock.expect(stateManager.isEmpty).andStubReturn(true)

    stateManager.takeSnapshot()
    EasyMock.expectLastCall().anyTimes()

    stateManager.truncateAndReload(EasyMock.eq(0L), EasyMock.eq(0L), EasyMock.anyLong)
    EasyMock.expectLastCall()

    EasyMock.expect(stateManager.firstUnstableOffset).andStubReturn(None)

    EasyMock.replay(stateManager)

    val config = LogConfig(new Properties())
    val log = new Log(logDir,
      config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = 300000,
      producerIdExpirationCheckIntervalMs = 30000,
      topicPartition = Log.parseTopicPartitionName(logDir),
      stateManager)

    EasyMock.verify(stateManager)

    // Append some messages
    EasyMock.reset(stateManager)
    EasyMock.expect(stateManager.firstUnstableOffset).andStubReturn(None)

    stateManager.updateMapEndOffset(1L)
    EasyMock.expectLastCall()
    stateManager.updateMapEndOffset(2L)
    EasyMock.expectLastCall()

    EasyMock.replay(stateManager)

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes))), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes))), leaderEpoch = 0)

    EasyMock.verify(stateManager)

    // Now truncate
    EasyMock.reset(stateManager)
    EasyMock.expect(stateManager.firstUnstableOffset).andStubReturn(None)
    EasyMock.expect(stateManager.latestSnapshotOffset).andReturn(None)
    EasyMock.expect(stateManager.isEmpty).andStubReturn(true)
    EasyMock.expect(stateManager.mapEndOffset).andReturn(2L)
    stateManager.truncateAndReload(EasyMock.eq(0L), EasyMock.eq(1L), EasyMock.anyLong)
    EasyMock.expectLastCall()
    // Truncation causes the map end offset to reset to 0
    EasyMock.expect(stateManager.mapEndOffset).andReturn(0L)
    // We skip directly to updating the map end offset
    stateManager.updateMapEndOffset(1L)
    EasyMock.expectLastCall()

    EasyMock.replay(stateManager)

    log.truncateTo(1L)

    EasyMock.verify(stateManager)

    // Close out the log so that tearDown can delete files (necessary on Windows).
    EasyMock.reset(stateManager)
    stateManager.takeSnapshot()
    EasyMock.expectLastCall().anyTimes()
    EasyMock.replay(stateManager)
    log.close()
  }

  @Test
  def testSkipTruncateAndReloadIfOldMessageFormatAndNoCleanShutdown(): Unit = {
    val stateManager = EasyMock.mock(classOf[ProducerStateManager])

    EasyMock.expect(stateManager.latestSnapshotOffset).andReturn(None)

    stateManager.updateMapEndOffset(0L)
    EasyMock.expectLastCall().anyTimes()

    stateManager.takeSnapshot()
    EasyMock.expectLastCall().anyTimes()

    EasyMock.replay(stateManager)

    val logProps = new Properties()
    logProps.put(LogConfig.MessageFormatVersionProp, "0.10.2")
    val config = LogConfig(logProps)
    val log = new Log(logDir,
      config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = 300000,
      producerIdExpirationCheckIntervalMs = 30000,
      topicPartition = Log.parseTopicPartitionName(logDir),
      stateManager)
    log.close()

    EasyMock.verify(stateManager)
  }

  @Test
  def testSkipTruncateAndReloadIfOldMessageFormatAndCleanShutdown(): Unit = {
    val stateManager = EasyMock.mock(classOf[ProducerStateManager])

    EasyMock.expect(stateManager.latestSnapshotOffset).andReturn(None)

    stateManager.updateMapEndOffset(0L)
    EasyMock.expectLastCall().anyTimes()

    stateManager.takeSnapshot()
    EasyMock.expectLastCall().anyTimes()

    EasyMock.replay(stateManager)

    val cleanShutdownFile = createCleanShutdownFile()

    val logProps = new Properties()
    logProps.put(LogConfig.MessageFormatVersionProp, "0.10.2")
    val config = LogConfig(logProps)
    val log = new Log(logDir,
      config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = 300000,
      producerIdExpirationCheckIntervalMs = 30000,
      topicPartition = Log.parseTopicPartitionName(logDir),
      stateManager)

    EasyMock.verify(stateManager)
    cleanShutdownFile.delete()
    log.close()
  }

  @Test
  def testSkipTruncateAndReloadIfNewMessageFormatAndCleanShutdown(): Unit = {
    val stateManager = EasyMock.mock(classOf[ProducerStateManager])

    EasyMock.expect(stateManager.latestSnapshotOffset).andReturn(None)

    stateManager.updateMapEndOffset(0L)
    EasyMock.expectLastCall().anyTimes()

    stateManager.takeSnapshot()
    EasyMock.expectLastCall().anyTimes()

    EasyMock.replay(stateManager)

    val cleanShutdownFile = createCleanShutdownFile()

    val logProps = new Properties()
    logProps.put(LogConfig.MessageFormatVersionProp, "0.11.0")
    val config = LogConfig(logProps)
    val log = new Log(logDir,
      config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = 300000,
      producerIdExpirationCheckIntervalMs = 30000,
      topicPartition = Log.parseTopicPartitionName(logDir),
      stateManager)

    EasyMock.verify(stateManager)
    cleanShutdownFile.delete()
    log.close()
  }

  @Test
  def testRebuildPidMapWithCompactedData() {
    val log = createLog(2048)
    val pid = 1L
    val epoch = 0.toShort
    val seq = 0
    val baseOffset = 23L

    // create a batch with a couple gaps to simulate compaction
    val records = TestUtils.records(producerId = pid, producerEpoch = epoch, sequence = seq, baseOffset = baseOffset, records = List(
      new SimpleRecord(System.currentTimeMillis(), "a".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "b".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "c".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "d".getBytes)))
    records.batches.asScala.foreach(_.setPartitionLeaderEpoch(0))

    val filtered = ByteBuffer.allocate(2048)
    records.filterTo(new TopicPartition("foo", 0), new RecordFilter {
      override def checkBatchRetention(batch: RecordBatch): BatchRetention = RecordFilter.BatchRetention.DELETE_EMPTY
      override def shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = !record.hasKey
    }, filtered, Int.MaxValue, BufferSupplier.NO_CACHING)
    filtered.flip()
    val filteredRecords = MemoryRecords.readableRecords(filtered)

    log.appendAsFollower(filteredRecords)

    // append some more data and then truncate to force rebuilding of the PID map
    val moreRecords = TestUtils.records(baseOffset = baseOffset + 4, records = List(
      new SimpleRecord(System.currentTimeMillis(), "e".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "f".getBytes)))
    moreRecords.batches.asScala.foreach(_.setPartitionLeaderEpoch(0))
    log.appendAsFollower(moreRecords)

    log.truncateTo(baseOffset + 4)

    val activeProducers = log.activeProducers
    assertTrue(activeProducers.contains(pid))

    val entry = activeProducers(pid)
    assertEquals(0, entry.firstSeq)
    assertEquals(baseOffset, entry.firstOffset)
    assertEquals(3, entry.lastSeq)
    assertEquals(baseOffset + 3, entry.lastOffset)

    log.close()
  }

  @Test
  def testRebuildProducerStateWithEmptyCompactedBatch() {
    val log = createLog(2048)
    val pid = 1L
    val epoch = 0.toShort
    val seq = 0
    val baseOffset = 23L

    // create an empty batch
    val records = TestUtils.records(producerId = pid, producerEpoch = epoch, sequence = seq, baseOffset = baseOffset, records = List(
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "a".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "b".getBytes)))
    records.batches.asScala.foreach(_.setPartitionLeaderEpoch(0))

    val filtered = ByteBuffer.allocate(2048)
    records.filterTo(new TopicPartition("foo", 0), new RecordFilter {
      override def checkBatchRetention(batch: RecordBatch): BatchRetention = RecordFilter.BatchRetention.RETAIN_EMPTY
      override def shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = false
    }, filtered, Int.MaxValue, BufferSupplier.NO_CACHING)
    filtered.flip()
    val filteredRecords = MemoryRecords.readableRecords(filtered)

    log.appendAsFollower(filteredRecords)

    // append some more data and then truncate to force rebuilding of the PID map
    val moreRecords = TestUtils.records(baseOffset = baseOffset + 2, records = List(
      new SimpleRecord(System.currentTimeMillis(), "e".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "f".getBytes)))
    moreRecords.batches.asScala.foreach(_.setPartitionLeaderEpoch(0))
    log.appendAsFollower(moreRecords)

    log.truncateTo(baseOffset + 2)

    val activeProducers = log.activeProducers
    assertTrue(activeProducers.contains(pid))

    val entry = activeProducers(pid)
    assertEquals(0, entry.firstSeq)
    assertEquals(baseOffset, entry.firstOffset)
    assertEquals(1, entry.lastSeq)
    assertEquals(baseOffset + 1, entry.lastOffset)

    log.close()
  }

  @Test
  def testUpdatePidMapWithCompactedData() {
    val log = createLog(2048)
    val pid = 1L
    val epoch = 0.toShort
    val seq = 0
    val baseOffset = 23L

    // create a batch with a couple gaps to simulate compaction
    val records = TestUtils.records(producerId = pid, producerEpoch = epoch, sequence = seq, baseOffset = baseOffset, records = List(
      new SimpleRecord(System.currentTimeMillis(), "a".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "b".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "c".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "d".getBytes)))
    records.batches.asScala.foreach(_.setPartitionLeaderEpoch(0))

    val filtered = ByteBuffer.allocate(2048)
    records.filterTo(new TopicPartition("foo", 0), new RecordFilter {
      override def checkBatchRetention(batch: RecordBatch): BatchRetention = RecordFilter.BatchRetention.DELETE_EMPTY
      override def shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = !record.hasKey
    }, filtered, Int.MaxValue, BufferSupplier.NO_CACHING)
    filtered.flip()
    val filteredRecords = MemoryRecords.readableRecords(filtered)

    log.appendAsFollower(filteredRecords)
    val activeProducers = log.activeProducers
    assertTrue(activeProducers.contains(pid))

    val entry = activeProducers(pid)
    assertEquals(0, entry.firstSeq)
    assertEquals(baseOffset, entry.firstOffset)
    assertEquals(3, entry.lastSeq)
    assertEquals(baseOffset + 3, entry.lastOffset)

    log.close()
  }

  @Test
  def testPidMapTruncateTo() {
    val log = createLog(2048)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes))), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes))), leaderEpoch = 0)
    log.takeProducerSnapshot()

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("c".getBytes))), leaderEpoch = 0)
    log.takeProducerSnapshot()

    log.truncateTo(2)
    assertEquals(Some(2), log.latestProducerSnapshotOffset)
    assertEquals(2, log.latestProducerStateEndOffset)

    log.truncateTo(1)
    assertEquals(None, log.latestProducerSnapshotOffset)
    assertEquals(1, log.latestProducerStateEndOffset)

    log.close()
  }

  @Test
  def testPidMapTruncateToWithNoSnapshots() {
    // This ensures that the upgrade optimization path cannot be hit after initial loading

    val log = createLog(2048)
    val pid = 1L
    val epoch = 0.toShort

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes)), producerId = pid,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes)), producerId = pid,
      producerEpoch = epoch, sequence = 1), leaderEpoch = 0)

    // Delete all snapshots prior to truncating
    logDir.listFiles.filter(f => f.isFile && f.getName.endsWith(Log.PidSnapshotFileSuffix)).foreach { file =>
      Files.delete(file.toPath)
    }

    log.truncateTo(1L)
    assertEquals(1, log.activeProducers.size)

    val pidEntryOpt = log.activeProducers.get(pid)
    assertTrue(pidEntryOpt.isDefined)

    val pidEntry = pidEntryOpt.get
    assertEquals(0, pidEntry.lastSeq)

    log.close()
  }

  @Test
  def testLoadProducersAfterDeleteRecordsMidSegment(): Unit = {
    val log = createLog(2048)
    val pid1 = 1L
    val pid2 = 2L
    val epoch = 0.toShort

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(time.milliseconds(), "a".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(time.milliseconds(), "b".getBytes)), producerId = pid2,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    assertEquals(2, log.activeProducers.size)

    log.maybeIncrementLogStartOffset(1L)

    assertEquals(1, log.activeProducers.size)
    val retainedEntryOpt = log.activeProducers.get(pid2)
    assertTrue(retainedEntryOpt.isDefined)
    assertEquals(0, retainedEntryOpt.get.lastSeq)

    log.close()

    val reloadedLog = createLog(2048, logStartOffset = 1L)
    assertEquals(1, reloadedLog.activeProducers.size)
    val reloadedEntryOpt = log.activeProducers.get(pid2)
    assertEquals(retainedEntryOpt, reloadedEntryOpt)
    reloadedLog.close()
  }

  @Test
  def testLoadProducersAfterDeleteRecordsOnSegment(): Unit = {
    val log = createLog(2048)
    val pid1 = 1L
    val pid2 = 2L
    val epoch = 0.toShort

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(time.milliseconds(), "a".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(time.milliseconds(), "b".getBytes)), producerId = pid2,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)

    assertEquals(2, log.logSegments.size)
    assertEquals(2, log.activeProducers.size)

    log.maybeIncrementLogStartOffset(1L)
    log.deleteOldSegments()

    assertEquals(1, log.logSegments.size)
    assertEquals(1, log.activeProducers.size)
    val retainedEntryOpt = log.activeProducers.get(pid2)
    assertTrue(retainedEntryOpt.isDefined)
    assertEquals(0, retainedEntryOpt.get.lastSeq)

    time.sleep(log.config.fileDeleteDelayMs + 1)
    log.close()

    val reloadedLog = createLog(2048, logStartOffset = 1L)
    assertEquals(1, reloadedLog.activeProducers.size)
    val reloadedEntryOpt = log.activeProducers.get(pid2)
    assertEquals(retainedEntryOpt, reloadedEntryOpt)
    reloadedLog.close()
  }

  @Test
  def testPidMapTruncateFullyAndStartAt() {
    val records = TestUtils.singletonRecords("foo".getBytes)
    val log = createLog(records.sizeInBytes, messagesPerSegment = 1, retentionBytes = records.sizeInBytes * 2)
    log.appendAsLeader(records, leaderEpoch = 0)
    log.takeProducerSnapshot()

    log.appendAsLeader(TestUtils.singletonRecords("bar".getBytes), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.singletonRecords("baz".getBytes), leaderEpoch = 0)
    log.takeProducerSnapshot()

    assertEquals(3, log.logSegments.size)
    assertEquals(3, log.latestProducerStateEndOffset)
    assertEquals(Some(3), log.latestProducerSnapshotOffset)

    log.truncateFullyAndStartAt(29)
    assertEquals(1, log.logSegments.size)
    assertEquals(None, log.latestProducerSnapshotOffset)
    assertEquals(29, log.latestProducerStateEndOffset)

    time.sleep(log.config.fileDeleteDelayMs + 1)
    log.close()
  }

  @Test
  def testPidExpirationOnSegmentDeletion() {
    val pid1 = 1L
    val records = TestUtils.records(Seq(new SimpleRecord("foo".getBytes)), producerId = pid1, producerEpoch = 0, sequence = 0)
    val log = createLog(records.sizeInBytes, messagesPerSegment = 1, retentionBytes = records.sizeInBytes * 2)
    log.appendAsLeader(records, leaderEpoch = 0)
    log.takeProducerSnapshot()

    val pid2 = 2L
    log.appendAsLeader(TestUtils.records(Seq(new SimpleRecord("bar".getBytes)), producerId = pid2, producerEpoch = 0, sequence = 0),
      leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(Seq(new SimpleRecord("baz".getBytes)), producerId = pid2, producerEpoch = 0, sequence = 1),
      leaderEpoch = 0)
    log.takeProducerSnapshot()

    assertEquals(3, log.logSegments.size)
    assertEquals(Set(pid1, pid2), log.activeProducers.keySet)

    log.deleteOldSegments()

    assertEquals(2, log.logSegments.size)
    assertEquals(Set(pid2), log.activeProducers.keySet)

    time.sleep(log.config.fileDeleteDelayMs + 1)
    log.close()
  }

  @Test
  def testTakeSnapshotOnRollAndDeleteSnapshotOnFlush() {
    val log = createLog(2048)
    log.appendAsLeader(TestUtils.singletonRecords("a".getBytes), leaderEpoch = 0)
    log.roll(1L)
    assertEquals(Some(1L), log.latestProducerSnapshotOffset)
    assertEquals(Some(1L), log.oldestProducerSnapshotOffset)

    log.appendAsLeader(TestUtils.singletonRecords("b".getBytes), leaderEpoch = 0)
    log.roll(2L)
    assertEquals(Some(2L), log.latestProducerSnapshotOffset)
    assertEquals(Some(1L), log.oldestProducerSnapshotOffset)

    log.appendAsLeader(TestUtils.singletonRecords("c".getBytes), leaderEpoch = 0)
    log.roll(3L)
    assertEquals(Some(3L), log.latestProducerSnapshotOffset)

    // roll triggers a flush at the starting offset of the new segment. we should
    // retain the snapshots from the active segment and the previous segment, but
    // the oldest one should be gone
    assertEquals(Some(2L), log.oldestProducerSnapshotOffset)

    // even if we flush within the active segment, the snapshot should remain
    log.appendAsLeader(TestUtils.singletonRecords("baz".getBytes), leaderEpoch = 0)
    log.flush(4L)
    assertEquals(Some(3L), log.latestProducerSnapshotOffset)
    assertEquals(Some(2L), log.oldestProducerSnapshotOffset)

    log.close()
  }

  @Test
  def testRebuildTransactionalState(): Unit = {
    val log = createLog(1024 * 1024)

    val pid = 137L
    val epoch = 5.toShort
    val seq = 0

    // add some transactional records
    val records = MemoryRecords.withTransactionalRecords(CompressionType.NONE, pid, epoch, seq,
      new SimpleRecord("foo".getBytes),
      new SimpleRecord("bar".getBytes),
      new SimpleRecord("baz".getBytes))
    log.appendAsLeader(records, leaderEpoch = 0)
    val commitAppendInfo = log.appendAsLeader(endTxnRecords(ControlRecordType.ABORT, pid, epoch),
      isFromClient = false, leaderEpoch = 0)
    log.onHighWatermarkIncremented(commitAppendInfo.lastOffset + 1)

    // now there should be no first unstable offset
    assertEquals(None, log.firstUnstableOffset)

    log.close()

    val reopenedLog = createLog(1024 * 1024)
    reopenedLog.onHighWatermarkIncremented(commitAppendInfo.lastOffset + 1)
    assertEquals(None, reopenedLog.firstUnstableOffset)
    reopenedLog.close()
  }

  private def endTxnRecords(controlRecordType: ControlRecordType,
                            producerId: Long,
                            epoch: Short,
                            offset: Long = 0L,
                            coordinatorEpoch: Int = 0,
                            partitionLeaderEpoch: Int = 0): MemoryRecords = {
    val marker = new EndTransactionMarker(controlRecordType, coordinatorEpoch)
    MemoryRecords.withEndTransactionMarker(offset, time.milliseconds(), partitionLeaderEpoch, producerId, epoch, marker)
  }

  @Test
  def testPeriodicPidExpiration() {
    val maxPidExpirationMs = 200
    val expirationCheckInterval = 100

    val pid = 23L
    val log = createLog(2048, maxPidExpirationMs = maxPidExpirationMs,
      pidExpirationCheckIntervalMs = expirationCheckInterval)
    val records = Seq(new SimpleRecord(time.milliseconds(), "foo".getBytes))
    log.appendAsLeader(TestUtils.records(records, producerId = pid, producerEpoch = 0, sequence = 0), leaderEpoch = 0)

    assertEquals(Set(pid), log.activeProducers.keySet)

    time.sleep(expirationCheckInterval)
    assertEquals(Set(pid), log.activeProducers.keySet)

    time.sleep(expirationCheckInterval)
    assertEquals(Set(), log.activeProducers.keySet)

	log.close()
  }

  @Test
  def testDuplicateAppends(): Unit = {
    val logProps = new Properties()

    // create a log
    val log = Log(logDir,
      LogConfig(logProps),
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)

    val pid = 1L
    val epoch: Short = 0

    var seq = 0
    // Pad the beginning of the log.
    for (_ <- 0 to 5) {
      val record = TestUtils.records(List(new SimpleRecord(time.milliseconds, "key".getBytes, "value".getBytes)),
        producerId = pid, producerEpoch = epoch, sequence = seq)
      log.appendAsLeader(record, leaderEpoch = 0)
      seq = seq + 1
    }
    // Append an entry with multiple log records.
    def createRecords = TestUtils.records(List(
      new SimpleRecord(time.milliseconds, s"key-$seq".getBytes, s"value-$seq".getBytes),
      new SimpleRecord(time.milliseconds, s"key-$seq".getBytes, s"value-$seq".getBytes),
      new SimpleRecord(time.milliseconds, s"key-$seq".getBytes, s"value-$seq".getBytes)
    ), producerId = pid, producerEpoch = epoch, sequence = seq)
    val multiEntryAppendInfo = log.appendAsLeader(createRecords, leaderEpoch = 0)
    assertEquals("should have appended 3 entries", multiEntryAppendInfo.lastOffset - multiEntryAppendInfo.firstOffset + 1, 3)

    // Append a Duplicate of the tail, when the entry at the tail has multiple records.
    val dupMultiEntryAppendInfo = log.appendAsLeader(createRecords, leaderEpoch = 0)
    assertEquals("Somehow appended a duplicate entry with multiple log records to the tail",
      multiEntryAppendInfo.firstOffset, dupMultiEntryAppendInfo.firstOffset)
    assertEquals("Somehow appended a duplicate entry with multiple log records to the tail",
      multiEntryAppendInfo.lastOffset, dupMultiEntryAppendInfo.lastOffset)

    seq = seq + 3

    // Append a partial duplicate of the tail. This is not allowed.
    try {
      val records = TestUtils.records(
        List(
          new SimpleRecord(time.milliseconds, s"key-$seq".getBytes, s"value-$seq".getBytes),
          new SimpleRecord(time.milliseconds, s"key-$seq".getBytes, s"value-$seq".getBytes)),
        producerId = pid, producerEpoch = epoch, sequence = seq - 2)
      log.appendAsLeader(records, leaderEpoch = 0)
      fail ("Should have received an OutOfOrderSequenceException since we attempted to append a duplicate of a records " +
        "in the middle of the log.")
    } catch {
      case _: OutOfOrderSequenceException => // Good!
    }

    // Append a Duplicate of an entry in the middle of the log. This is not allowed.
     try {
      val records = TestUtils.records(
        List(new SimpleRecord(time.milliseconds, s"key-1".getBytes, s"value-1".getBytes)),
        producerId = pid, producerEpoch = epoch, sequence = 1)
      log.appendAsLeader(records, leaderEpoch = 0)
      fail ("Should have received an OutOfOrderSequenceException since we attempted to append a duplicate of a records " +
        "in the middle of the log.")
    } catch {
      case _: OutOfOrderSequenceException => // Good!
    }

    // Append a duplicate entry with a single records at the tail of the log. This should return the appendInfo of the original entry.
    def createRecordsWithDuplicate = TestUtils.records(List(new SimpleRecord(time.milliseconds, "key".getBytes, "value".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = seq)
    val origAppendInfo = log.appendAsLeader(createRecordsWithDuplicate, leaderEpoch = 0)
    val newAppendInfo = log.appendAsLeader(createRecordsWithDuplicate, leaderEpoch = 0)
    assertEquals("Inserted a duplicate records into the log", origAppendInfo.firstOffset, newAppendInfo.firstOffset)
    assertEquals("Inserted a duplicate records into the log", origAppendInfo.lastOffset, newAppendInfo.lastOffset)

    log.close()
  }

  @Test
  def testMultiplePidsPerMemoryRecord() : Unit = {
    val logProps = new Properties()

    // create a log
    val log = Log(logDir,
      LogConfig(logProps),
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)

    val epoch: Short = 0
    val buffer = ByteBuffer.allocate(512)

    var builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE,
      TimestampType.LOG_APPEND_TIME, 0L, time.milliseconds(), 1L, epoch, 0, false, 0)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE,
      TimestampType.LOG_APPEND_TIME, 1L, time.milliseconds(), 2L, epoch, 0, false, 0)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE,
      TimestampType.LOG_APPEND_TIME, 2L, time.milliseconds(), 3L, epoch, 0, false, 0)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE,
      TimestampType.LOG_APPEND_TIME, 3L, time.milliseconds(), 4L, epoch, 0, false, 0)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    buffer.flip()
    val memoryRecords = MemoryRecords.readableRecords(buffer)

    log.appendAsFollower(memoryRecords)
    log.flush()

    val fetchedData = log.readUncommitted(0, Int.MaxValue)

    val origIterator = memoryRecords.batches.iterator()
    for (batch <- fetchedData.records.batches.asScala) {
      assertTrue(origIterator.hasNext)
      val origEntry = origIterator.next()
      assertEquals(origEntry.producerId, batch.producerId)
      assertEquals(origEntry.baseOffset, batch.baseOffset)
      assertEquals(origEntry.baseSequence, batch.baseSequence)
    }

    log.close()
  }

  @Test(expected = classOf[DuplicateSequenceNumberException])
  def testDuplicateAppendToFollower() : Unit = {
    val log = createLog(1024*1024)
    val epoch: Short = 0
    val pid = 1L
    val baseSequence = 0
    val partitionLeaderEpoch = 0
    // this is a bit contrived. to trigger the duplicate case for a follower append, we have to append
    // a batch with matching sequence numbers, but valid increasing offsets
    log.appendAsFollower(MemoryRecords.withIdempotentRecords(0L, CompressionType.NONE, pid, epoch, baseSequence,
      partitionLeaderEpoch, new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

    try {
      log.appendAsFollower(MemoryRecords.withIdempotentRecords(2L, CompressionType.NONE, pid, epoch, baseSequence,
        partitionLeaderEpoch, new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))
    } finally {
      log.close()
    }
  }

  @Test(expected = classOf[DuplicateSequenceNumberException])
  def testMultipleProducersWithDuplicatesInSingleAppend() : Unit = {
    val log = createLog(1024*1024)

    val pid1 = 1L
    val pid2 = 2L
    val epoch: Short = 0

    val buffer = ByteBuffer.allocate(512)

    // pid1 seq = 0
    var builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE,
      TimestampType.LOG_APPEND_TIME, 0L, time.milliseconds(), pid1, epoch, 0)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    // pid2 seq = 0
    builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE,
      TimestampType.LOG_APPEND_TIME, 1L, time.milliseconds(), pid2, epoch, 0)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    // pid1 seq = 1
    builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE,
      TimestampType.LOG_APPEND_TIME, 2L, time.milliseconds(), pid1, epoch, 1)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    // pid2 seq = 1
    builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE,
      TimestampType.LOG_APPEND_TIME, 3L, time.milliseconds(), pid2, epoch, 1)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    // // pid1 seq = 1 (duplicate)
    builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE,
      TimestampType.LOG_APPEND_TIME, 4L, time.milliseconds(), pid1, epoch, 1)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    records.batches.asScala.foreach(_.setPartitionLeaderEpoch(0))
    try {
      log.appendAsFollower(records)
    } finally {
      log.close()
    }
    // Should throw a duplicate sequence exception here.
    fail("should have thrown a DuplicateSequenceNumberException.")
  }

  @Test(expected = classOf[ProducerFencedException])
  def testOldProducerEpoch(): Unit = {
    val logProps = new Properties()

    // create a log
    val log = Log(logDir,
      LogConfig(logProps),
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)

    val pid = 1L
    val newEpoch: Short = 1
    val oldEpoch: Short = 0

    val records = TestUtils.records(List(new SimpleRecord(time.milliseconds, "key".getBytes, "value".getBytes)), producerId = pid, producerEpoch = newEpoch, sequence = 0)
    log.appendAsLeader(records, leaderEpoch = 0)

    val nextRecords = TestUtils.records(List(new SimpleRecord(time.milliseconds, "key".getBytes, "value".getBytes)), producerId = pid, producerEpoch = oldEpoch, sequence = 0)
    try {
      log.appendAsLeader(nextRecords, leaderEpoch = 0)
    } finally {
      log.close()
    }
  }

  /**
   * Test for jitter s for time based log roll. This test appends messages then changes the time
   * using the mock clock to force the log to roll and checks the number of segments.
   */
  @Test
  def testTimeBasedLogRollJitter() {
    var set = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds)
    val maxJitter = 20 * 60L

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentMsProp, 1 * 60 * 60L: java.lang.Long)
    logProps.put(LogConfig.SegmentJitterMsProp, maxJitter: java.lang.Long)
    // create a log
    val log = Log(logDir,
      LogConfig(logProps),
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)
    assertEquals("Log begins with a single empty segment.", 1, log.numberOfSegments)
    log.appendAsLeader(set, leaderEpoch = 0)

    time.sleep(log.config.segmentMs - maxJitter)
    set = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds)
    log.appendAsLeader(set, leaderEpoch = 0)
    assertEquals("Log does not roll on this append because it occurs earlier than max jitter", 1, log.numberOfSegments)
    time.sleep(maxJitter - log.activeSegment.rollJitterMs + 1)
    set = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds)
    log.appendAsLeader(set, leaderEpoch = 0)
    assertEquals("Log should roll after segmentMs adjusted by random jitter", 2, log.numberOfSegments)

    log.close()
  }

  /**
   * Test that appending more than the maximum segment size rolls the log
   */
  @Test
  def testSizeBasedLogRoll() {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds)
    val setSize = createRecords.sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * (setSize - 1) // each segment will be 10 messages

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
    // We use need to use magic value 1 here because the test is message size sensitive.
    logProps.put(LogConfig.MessageFormatVersionProp, ApiVersion.latestVersion.toString)
    // create a log
    val log = Log(logDir, LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)

    // segments expire in size
    for (_ <- 1 to (msgPerSeg + 1))
      log.appendAsLeader(createRecords, leaderEpoch = 0)
    assertEquals("There should be exactly 2 segments.", 2, log.numberOfSegments)

    log.close()
  }

  /**
   * Test that we can open and append to an empty log
   */
  @Test
  def testLoadEmptyLog() {
    createEmptyLogs(logDir, 0)
    val log = Log(logDir, logConfig, logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    log.appendAsLeader(TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds), leaderEpoch = 0)
    log.close()
  }

  /**
   * This test case appends a bunch of messages and checks that we can read them all back using sequential offsets.
   */
  @Test
  def testAppendAndReadWithSequentialOffsets() {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 71: java.lang.Integer)
    // We use need to use magic value 1 here because the test is message size sensitive.
    logProps.put(LogConfig.MessageFormatVersionProp, ApiVersion.latestVersion.toString)
    val log = Log(logDir, LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    val values = (0 until 100 by 2).map(id => id.toString.getBytes).toArray

    for(value <- values)
      log.appendAsLeader(TestUtils.singletonRecords(value = value), leaderEpoch = 0)

    for(i <- values.indices) {
      val read = log.readUncommitted(i, 100, Some(i+1)).records.batches.iterator.next()
      assertEquals("Offset read should match order appended.", i, read.lastOffset)
      val actual = read.iterator.next()
      assertNull("Key should be null", actual.key)
      assertEquals("Values not equal", ByteBuffer.wrap(values(i)), actual.value)
    }
    assertEquals("Reading beyond the last message returns nothing.", 0,
      log.readUncommitted(values.length, 100, None).records.batches.asScala.size)
    log.close()
  }

  /**
   * This test appends a bunch of messages with non-sequential offsets and checks that we can read the correct message
   * from any offset less than the logEndOffset including offsets not appended.
   */
  @Test
  def testAppendAndReadWithNonSequentialOffsets() {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 72: java.lang.Integer)
    val log = Log(logDir,  LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    val messageIds = ((0 until 50) ++ (50 until 200 by 7)).toArray
    val records = messageIds.map(id => new SimpleRecord(id.toString.getBytes))

    // now test the case that we give the offsets and use non-sequential offsets
    for(i <- records.indices)
      log.appendAsFollower(MemoryRecords.withRecords(messageIds(i), CompressionType.NONE, 0, records(i)))
    for(i <- 50 until messageIds.max) {
      val idx = messageIds.indexWhere(_ >= i)
      val read = log.readUncommitted(i, 100, None).records.records.iterator.next()
      assertEquals("Offset read should match message id.", messageIds(idx), read.offset)
      assertEquals("Message should match appended.", records(idx), new SimpleRecord(read))
    }

    log.close()
  }

  /**
   * This test covers an odd case where we have a gap in the offsets that falls at the end of a log segment.
   * Specifically we create a log where the last message in the first segment has offset 0. If we
   * then read offset 1, we should expect this read to come from the second segment, even though the
   * first segment has the greatest lower bound on the offset.
   */
  @Test
  def testReadAtLogGap() {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 300: java.lang.Integer)
    val log = Log(logDir, LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)

    // keep appending until we have two segments with only a single message in the second segment
    while(log.numberOfSegments == 1)
      log.appendAsLeader(TestUtils.singletonRecords(value = "42".getBytes), leaderEpoch = 0)

    // now manually truncate off all but one message from the first segment to create a gap in the messages
    log.logSegments.head.truncateTo(1)

    assertEquals("A read should now return the last message in the log", log.logEndOffset - 1,
      log.readUncommitted(1, 200, None).records.batches.iterator.next().lastOffset)

    log.close()
  }

  @Test
  def testReadWithMinMessage() {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 72: java.lang.Integer)
    val log = Log(logDir,  LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    val messageIds = ((0 until 50) ++ (50 until 200 by 7)).toArray
    val records = messageIds.map(id => new SimpleRecord(id.toString.getBytes))

    // now test the case that we give the offsets and use non-sequential offsets
    for (i <- records.indices)
      log.appendAsFollower(MemoryRecords.withRecords(messageIds(i), CompressionType.NONE, 0, records(i)))

    for (i <- 50 until messageIds.max) {
      val idx = messageIds.indexWhere(_ >= i)
      val reads = Seq(
        log.readUncommitted(i, 1, minOneMessage = true),
        log.readUncommitted(i, 100, minOneMessage = true),
        log.readUncommitted(i, 100, Some(10000), minOneMessage = true)
      ).map(_.records.records.iterator.next())
      reads.foreach { read =>
        assertEquals("Offset read should match message id.", messageIds(idx), read.offset)
        assertEquals("Message should match appended.", records(idx), new SimpleRecord(read))
      }

      assertEquals(Seq.empty, log.readUncommitted(i, 1, Some(1), minOneMessage = true).records.batches.asScala.toIndexedSeq)
    }

    log.close()
  }

  @Test
  def testReadWithTooSmallMaxLength() {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 72: java.lang.Integer)
    val log = Log(logDir,  LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    val messageIds = ((0 until 50) ++ (50 until 200 by 7)).toArray
    val records = messageIds.map(id => new SimpleRecord(id.toString.getBytes))

    // now test the case that we give the offsets and use non-sequential offsets
    for (i <- records.indices)
      log.appendAsFollower(MemoryRecords.withRecords(messageIds(i), CompressionType.NONE, 0, records(i)))

    for (i <- 50 until messageIds.max) {
      assertEquals(MemoryRecords.EMPTY, log.readUncommitted(i, 0).records)

      // we return an incomplete message instead of an empty one for the case below
      // we use this mechanism to tell consumers of the fetch request version 2 and below that the message size is
      // larger than the fetch size
      // in fetch request version 3, we no longer need this as we return oversized messages from the first non-empty
      // partition
      val fetchInfo = log.readUncommitted(i, 1)
      assertTrue(fetchInfo.firstEntryIncomplete)
      assertTrue(fetchInfo.records.isInstanceOf[FileRecords])
      assertEquals(1, fetchInfo.records.sizeInBytes)
    }

    log.close()
  }

  /**
   * Test reading at the boundary of the log, specifically
   * - reading from the logEndOffset should give an empty message set
   * - reading from the maxOffset should give an empty message set
   * - reading beyond the log end offset should throw an OffsetOutOfRangeException
   */
  @Test
  def testReadOutOfRange() {
    createEmptyLogs(logDir, 1024)
    val logProps = new Properties()

    // set up replica log starting with offset 1024 and with one message (at offset 1024)
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    val log = Log(logDir, LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    log.appendAsLeader(TestUtils.singletonRecords(value = "42".getBytes), leaderEpoch = 0)

    assertEquals("Reading at the log end offset should produce 0 byte read.", 0,
      log.readUncommitted(1025, 1000).records.sizeInBytes)

    try {
      log.readUncommitted(0, 1000)
      fail("Reading below the log start offset should throw OffsetOutOfRangeException")
    } catch {
      case _: OffsetOutOfRangeException => // This is good.
    }

    try {
      log.readUncommitted(1026, 1000)
      fail("Reading at beyond the log end offset should throw OffsetOutOfRangeException")
    } catch {
      case _: OffsetOutOfRangeException => // This is good.
    }

    assertEquals("Reading from below the specified maxOffset should produce 0 byte read.", 0,
      log.readUncommitted(1025, 1000, Some(1024)).records.sizeInBytes)

    log.close()
  }

  /**
   * Test that covers reads and writes on a multisegment log. This test appends a bunch of messages
   * and then reads them all back and checks that the message read and offset matches what was appended.
   */
  @Test
  def testLogRolls() {
    /* create a multipart log with 100 messages */
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 100: java.lang.Integer)
    val log = Log(logDir, LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    val numMessages = 100
    val messageSets = (0 until numMessages).map(i => TestUtils.singletonRecords(value = i.toString.getBytes,
                                                                                timestamp = time.milliseconds))
    messageSets.foreach(log.appendAsLeader(_, leaderEpoch = 0))
    log.flush()

    /* do successive reads to ensure all our messages are there */
    var offset = 0L
    for(i <- 0 until numMessages) {
      val messages = log.readUncommitted(offset, 1024*1024).records.batches
      val head = messages.iterator.next()
      assertEquals("Offsets not equal", offset, head.lastOffset)

      val expected = messageSets(i).records.iterator.next()
      val actual = head.iterator.next()
      assertEquals(s"Keys not equal at offset $offset", expected.key, actual.key)
      assertEquals(s"Values not equal at offset $offset", expected.value, actual.value)
      assertEquals(s"Timestamps not equal at offset $offset", expected.timestamp, actual.timestamp)
      offset = head.lastOffset + 1
    }
    val lastRead = log.readUncommitted(startOffset = numMessages, maxLength = 1024*1024,
      maxOffset = Some(numMessages + 1)).records
    assertEquals("Should be no more messages", 0, lastRead.records.asScala.size)

    // check that rolling the log forced a flushed the log--the flush is asyn so retry in case of failure
    TestUtils.retry(1000L){
      assertTrue("Log role should have forced flush", log.recoveryPoint >= log.activeSegment.baseOffset)
    }

    log.close()
  }

  /**
   * Test reads at offsets that fall within compressed message set boundaries.
   */
  @Test
  def testCompressedMessages() {
    /* this log should roll after every messageset */
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 110: java.lang.Integer)
    val log = Log(logDir, LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)

    /* append 2 compressed message sets, each with two messages giving offsets 0, 1, 2, 3 */
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.GZIP, new SimpleRecord("hello".getBytes), new SimpleRecord("there".getBytes)), leaderEpoch = 0)
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.GZIP, new SimpleRecord("alpha".getBytes), new SimpleRecord("beta".getBytes)), leaderEpoch = 0)

    def read(offset: Int) = log.readUncommitted(offset, 4096).records.records

    /* we should always get the first message in the compressed set when reading any offset in the set */
    assertEquals("Read at offset 0 should produce 0", 0, read(0).iterator.next().offset)
    assertEquals("Read at offset 1 should produce 0", 0, read(1).iterator.next().offset)
    assertEquals("Read at offset 2 should produce 2", 2, read(2).iterator.next().offset)
    assertEquals("Read at offset 3 should produce 2", 2, read(3).iterator.next().offset)

    log.close()
  }

  /**
   * Test garbage collecting old segments
   */
  @Test
  def testThatGarbageCollectingSegmentsDoesntChangeOffset() {
    for(messagesToAppend <- List(0, 1, 25)) {
      logDir.mkdirs()
      // first test a log segment starting at 0
      val logProps = new Properties()
      logProps.put(LogConfig.SegmentBytesProp, 100: java.lang.Integer)
      logProps.put(LogConfig.RetentionMsProp, 0: java.lang.Integer)
      val log = Log(logDir, LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
        brokerTopicStats = brokerTopicStats, time = time)
      for(i <- 0 until messagesToAppend)
        log.appendAsLeader(TestUtils.singletonRecords(value = i.toString.getBytes, timestamp = time.milliseconds - 10), leaderEpoch = 0)

      val currOffset = log.logEndOffset
      assertEquals(currOffset, messagesToAppend)

      // time goes by; the log file is deleted
      log.deleteOldSegments()
      time.sleep(log.config.fileDeleteDelayMs + 1)

      assertEquals("Deleting segments shouldn't have changed the logEndOffset", currOffset, log.logEndOffset)
      assertEquals("We should still have one segment left", 1, log.numberOfSegments)
      assertEquals("Further collection shouldn't delete anything", 0, log.deleteOldSegments())
      assertEquals("Still no change in the logEndOffset", currOffset, log.logEndOffset)
      assertEquals("Should still be able to append and should get the logEndOffset assigned to the new append",
                   currOffset,
                   log.appendAsLeader(TestUtils.singletonRecords(value = "hello".getBytes, timestamp = time.milliseconds), leaderEpoch = 0).firstOffset)

      // cleanup the log
      log.delete()
    }
  }

  /**
   *  MessageSet size shouldn't exceed the config.segmentSize, check that it is properly enforced by
   * appending a message set larger than the config.segmentSize setting and checking that an exception is thrown.
   */
  @Test
  def testMessageSetSizeCheck() {
    val messageSet = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("You".getBytes), new SimpleRecord("bethe".getBytes))
    // append messages to log
    val configSegmentSize = messageSet.sizeInBytes - 1
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, configSegmentSize: java.lang.Integer)
    // We use need to use magic value 1 here because the test is message size sensitive.
    logProps.put(LogConfig.MessageFormatVersionProp, ApiVersion.latestVersion.toString)
    val log = Log(logDir, LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)

    try {
      log.appendAsLeader(messageSet, leaderEpoch = 0)
      fail("message set should throw RecordBatchTooLargeException.")
    } catch {
      case _: RecordBatchTooLargeException => // this is good
    } finally {
      log.close()
    }
  }

  @Test
  def testCompactedTopicConstraints() {
    val keyedMessage = new SimpleRecord("and here it is".getBytes, "this message has a key".getBytes)
    val anotherKeyedMessage = new SimpleRecord("another key".getBytes, "this message also has a key".getBytes)
    val unkeyedMessage = new SimpleRecord("this message does not have a key".getBytes)

    val messageSetWithUnkeyedMessage = MemoryRecords.withRecords(CompressionType.NONE, unkeyedMessage, keyedMessage)
    val messageSetWithOneUnkeyedMessage = MemoryRecords.withRecords(CompressionType.NONE, unkeyedMessage)
    val messageSetWithCompressedKeyedMessage = MemoryRecords.withRecords(CompressionType.GZIP, keyedMessage)
    val messageSetWithCompressedUnkeyedMessage = MemoryRecords.withRecords(CompressionType.GZIP, keyedMessage, unkeyedMessage)

    val messageSetWithKeyedMessage = MemoryRecords.withRecords(CompressionType.NONE, keyedMessage)
    val messageSetWithKeyedMessages = MemoryRecords.withRecords(CompressionType.NONE, keyedMessage, anotherKeyedMessage)

    val logProps = new Properties()
    logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)

    val log = Log(logDir, LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)

    try {
      log.appendAsLeader(messageSetWithUnkeyedMessage, leaderEpoch = 0)
      fail("Compacted topics cannot accept a message without a key.")
    } catch {
      case _: CorruptRecordException => // this is good
    }
    try {
      log.appendAsLeader(messageSetWithOneUnkeyedMessage, leaderEpoch = 0)
      fail("Compacted topics cannot accept a message without a key.")
    } catch {
      case _: CorruptRecordException => // this is good
    }
    try {
      log.appendAsLeader(messageSetWithCompressedUnkeyedMessage, leaderEpoch = 0)
      fail("Compacted topics cannot accept a message without a key.")
    } catch {
      case _: CorruptRecordException => // this is good
    }

    // the following should succeed without any InvalidMessageException
    log.appendAsLeader(messageSetWithKeyedMessage, leaderEpoch = 0)
    log.appendAsLeader(messageSetWithKeyedMessages, leaderEpoch = 0)
    log.appendAsLeader(messageSetWithCompressedKeyedMessage, leaderEpoch = 0)

    log.close()
  }

  /**
   * We have a max size limit on message appends, check that it is properly enforced by appending a message larger than the
   * setting and checking that an exception is thrown.
   */
  @Test
  def testMessageSizeCheck() {
    val first = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("You".getBytes), new SimpleRecord("bethe".getBytes))
    val second = MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord("change (I need more bytes)... blah blah blah.".getBytes),
      new SimpleRecord("More padding boo hoo".getBytes))

    // append messages to log
    val maxMessageSize = second.sizeInBytes - 1
    val logProps = new Properties()
    logProps.put(LogConfig.MaxMessageBytesProp, maxMessageSize: java.lang.Integer)
    val log = Log(logDir, LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)

    // should be able to append the small message
    log.appendAsLeader(first, leaderEpoch = 0)

    try {
      log.appendAsLeader(second, leaderEpoch = 0)
      fail("Second message set should throw MessageSizeTooLargeException.")
    } catch {
      case _: RecordTooLargeException => // this is good
    }

    log.close()
  }
  /**
   * Append a bunch of messages to a log and then re-open it both with and without recovery and check that the log re-initializes correctly.
   */
  @Test
  def testLogRecoversToCorrectOffset() {
    val numMessages = 100
    val messageSize = 100
    val segmentSize = 7 * messageSize
    val indexInterval = 3 * messageSize
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, indexInterval: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 4096: java.lang.Integer)
    val config = LogConfig(logProps)
    var log = Log(logDir, config, logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    for(i <- 0 until numMessages)
      log.appendAsLeader(TestUtils.singletonRecords(value = TestUtils.randomBytes(messageSize),
        timestamp = time.milliseconds + i * 10), leaderEpoch = 0)
    assertEquals("After appending %d messages to an empty log, the log end offset should be %d".format(numMessages, numMessages), numMessages, log.logEndOffset)
    val lastIndexOffset = log.activeSegment.index.lastOffset
    val numIndexEntries = log.activeSegment.index.entries
    val lastOffset = log.logEndOffset
    // After segment is closed, the last entry in the time index should be (largest timestamp -> last offset).
    val lastTimeIndexOffset = log.logEndOffset - 1
    val lastTimeIndexTimestamp  = log.activeSegment.largestTimestamp
    // Depending on when the last time index entry is inserted, an entry may or may not be inserted into the time index.
    val numTimeIndexEntries = log.activeSegment.timeIndex.entries + {
      if (log.activeSegment.timeIndex.lastEntry.offset == log.logEndOffset - 1) 0 else 1
    }
    log.close()

    def verifyRecoveredLog(log: Log) {
      assertEquals(s"Should have $numMessages messages when log is reopened w/o recovery", numMessages, log.logEndOffset)
      assertEquals("Should have same last index offset as before.", lastIndexOffset, log.activeSegment.index.lastOffset)
      assertEquals("Should have same number of index entries as before.", numIndexEntries, log.activeSegment.index.entries)
      assertEquals("Should have same last time index timestamp", lastTimeIndexTimestamp, log.activeSegment.timeIndex.lastEntry.timestamp)
      assertEquals("Should have same last time index offset", lastTimeIndexOffset, log.activeSegment.timeIndex.lastEntry.offset)
      assertEquals("Should have same number of time index entries as before.", numTimeIndexEntries, log.activeSegment.timeIndex.entries)
    }

    log = Log(logDir, config, logStartOffset = 0L, recoveryPoint = lastOffset, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    verifyRecoveredLog(log)
    log.close()

    // test recovery case
    log = Log(logDir, config, logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    verifyRecoveredLog(log)
    log.close()
  }

  /**
   * Test building the time index on the follower by setting assignOffsets to false.
   */
  @Test
  def testBuildTimeIndexWhenNotAssigningOffsets() {
    val numMessages = 100
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 10000: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)

    val config = LogConfig(logProps)
    val log = Log(logDir, config, logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)

    val messages = (0 until numMessages).map { i =>
      MemoryRecords.withRecords(100 + i, CompressionType.NONE, 0, new SimpleRecord(time.milliseconds + i, i.toString.getBytes()))
    }
    messages.foreach(log.appendAsFollower)
    val timeIndexEntries = log.logSegments.foldLeft(0) { (entries, segment) => entries + segment.timeIndex.entries }
    assertEquals(s"There should be ${numMessages - 1} time index entries", numMessages - 1, timeIndexEntries)
    assertEquals(s"The last time index entry should have timestamp ${time.milliseconds + numMessages - 1}",
      time.milliseconds + numMessages - 1, log.activeSegment.timeIndex.lastEntry.timestamp)

    log.close()
  }

  /**
   * Test that if we manually delete an index segment it is rebuilt when the log is re-opened
   */
  @Test
  def testIndexRebuild() {
    // publish the messages and close the log
    val numMessages = 200
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 200: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)

    val config = LogConfig(logProps)
    var log = Log(logDir, config, logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    for(i <- 0 until numMessages)
      log.appendAsLeader(TestUtils.singletonRecords(value = TestUtils.randomBytes(10), timestamp = time.milliseconds + i * 10), leaderEpoch = 0)
    val indexFiles = log.logSegments.map(_.index.file)
    val timeIndexFiles = log.logSegments.map(_.timeIndex.file)
    log.close()

    // delete all the index files
    indexFiles.foreach(_.delete())
    timeIndexFiles.foreach(_.delete())

    // reopen the log
    log = Log(logDir, config, logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    assertEquals("Should have %d messages when log is reopened".format(numMessages), numMessages, log.logEndOffset)
    assertTrue("The index should have been rebuilt", log.logSegments.head.index.entries > 0)
    assertTrue("The time index should have been rebuilt", log.logSegments.head.timeIndex.entries > 0)
    for(i <- 0 until numMessages) {
      assertEquals(i, log.readUncommitted(i, 100, None).records.batches.iterator.next().lastOffset)
      if (i == 0)
        assertEquals(log.logSegments.head.baseOffset, log.fetchOffsetsByTimestamp(time.milliseconds + i * 10).get.offset)
      else
        assertEquals(i, log.fetchOffsetsByTimestamp(time.milliseconds + i * 10).get.offset)
    }
    log.close()
  }

  /**
   * Test that if messages format version of the messages in a segment is before 0.10.0, the time index should be empty.
   */
  @Test
  def testRebuildTimeIndexForOldMessages() {
    val numMessages = 200
    val segmentSize = 200
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    logProps.put(LogConfig.MessageFormatVersionProp, "0.9.0")

    val config = LogConfig(logProps)
    var log = Log(logDir, config, logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    for(i <- 0 until numMessages)
      log.appendAsLeader(TestUtils.singletonRecords(value = TestUtils.randomBytes(10),
        timestamp = time.milliseconds + i * 10, magicValue = RecordBatch.MAGIC_VALUE_V1), leaderEpoch = 0)
    val timeIndexFiles = log.logSegments.map(_.timeIndex.file)
    log.close()

    // Delete the time index.
    timeIndexFiles.foreach(_.delete())

    // The rebuilt time index should be empty
    log = Log(logDir, config, logStartOffset = 0L, recoveryPoint = numMessages + 1, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    val segArray = log.logSegments.toArray
    for (i <- segArray.indices.init) {
      assertEquals("The time index should be empty", 0, segArray(i).timeIndex.entries)
      assertEquals("The time index file size should be 0", 0, segArray(i).timeIndex.file.length)
    }

    log.close()
  }

  /**
   * Test that if we have corrupted an index segment it is rebuilt when the log is re-opened
   */
  @Test
  def testCorruptIndexRebuild() {
    // publish the messages and close the log
    val numMessages = 200
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 200: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)

    val config = LogConfig(logProps)
    var log = Log(logDir, config, logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    for(i <- 0 until numMessages)
      log.appendAsLeader(TestUtils.singletonRecords(value = TestUtils.randomBytes(10), timestamp = time.milliseconds + i * 10), leaderEpoch = 0)
    val indexFiles = log.logSegments.map(_.index.file)
    val timeIndexFiles = log.logSegments.map(_.timeIndex.file)
    log.close()

    // corrupt all the index files
    for( file <- indexFiles) {
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("  ")
      bw.close()
    }

    // corrupt all the index files
    for( file <- timeIndexFiles) {
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("  ")
      bw.close()
    }

    // reopen the log
    log = Log(logDir, config, logStartOffset = 0L, recoveryPoint = 200L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    assertEquals("Should have %d messages when log is reopened".format(numMessages), numMessages, log.logEndOffset)
    for(i <- 0 until numMessages) {
      assertEquals(i, log.readUncommitted(i, 100, None).records.batches.iterator.next().lastOffset)
      if (i == 0)
        assertEquals(log.logSegments.head.baseOffset, log.fetchOffsetsByTimestamp(time.milliseconds + i * 10).get.offset)
      else
        assertEquals(i, log.fetchOffsetsByTimestamp(time.milliseconds + i * 10).get.offset)
    }
    log.close()
  }

  /**
   * Test the Log truncate operations
   */
  @Test
  def testTruncateTo() {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds)
    val setSize = createRecords.sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * setSize  // each segment will be 10 messages

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)

    // create a log
    val log = Log(logDir, LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)

    for (_ <- 1 to msgPerSeg)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    assertEquals("There should be exactly 1 segments.", 1, log.numberOfSegments)
    assertEquals("Log end offset should be equal to number of messages", msgPerSeg, log.logEndOffset)

    val lastOffset = log.logEndOffset
    val size = log.size
    log.truncateTo(log.logEndOffset) // keep the entire log
    assertEquals("Should not change offset", lastOffset, log.logEndOffset)
    assertEquals("Should not change log size", size, log.size)
    log.truncateTo(log.logEndOffset + 1) // try to truncate beyond lastOffset
    assertEquals("Should not change offset but should log error", lastOffset, log.logEndOffset)
    assertEquals("Should not change log size", size, log.size)
    log.truncateTo(msgPerSeg/2) // truncate somewhere in between
    assertEquals("Should change offset", log.logEndOffset, msgPerSeg/2)
    assertTrue("Should change log size", log.size < size)
    log.truncateTo(0) // truncate the entire log
    assertEquals("Should change offset", 0, log.logEndOffset)
    assertEquals("Should change log size", 0, log.size)

    for (_ <- 1 to msgPerSeg)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    assertEquals("Should be back to original offset", log.logEndOffset, lastOffset)
    assertEquals("Should be back to original size", log.size, size)
    log.truncateFullyAndStartAt(log.logEndOffset - (msgPerSeg - 1))
    assertEquals("Should change offset", log.logEndOffset, lastOffset - (msgPerSeg - 1))
    assertEquals("Should change log size", log.size, 0)

    for (_ <- 1 to msgPerSeg)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    assertTrue("Should be ahead of to original offset", log.logEndOffset > msgPerSeg)
    assertEquals("log size should be same as before", size, log.size)
    log.truncateTo(0) // truncate before first start offset in the log
    assertEquals("Should change offset", 0, log.logEndOffset)
    assertEquals("Should change log size", log.size, 0)

    time.sleep(log.config.fileDeleteDelayMs + 1)
    log.close()
  }

  /**
   * Verify that when we truncate a log the index of the last segment is resized to the max index size to allow more appends
   */
  @Test
  def testIndexResizingAtTruncation() {
    val setSize = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds).sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * setSize  // each segment will be 10 messages
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, setSize - 1: java.lang.Integer)
    val config = LogConfig(logProps)
    val log = Log(logDir, config, logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)

    for (i<- 1 to msgPerSeg)
      log.appendAsLeader(TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds + i), leaderEpoch = 0)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)

    time.sleep(msgPerSeg)
    for (i<- 1 to msgPerSeg)
      log.appendAsLeader(TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds + i), leaderEpoch = 0)
    assertEquals("There should be exactly 2 segment.", 2, log.numberOfSegments)
    val expectedEntries = msgPerSeg - 1

    assertEquals(s"The index of the first segment should have $expectedEntries entries", expectedEntries, log.logSegments.toList.head.index.maxEntries)
    assertEquals(s"The time index of the first segment should have $expectedEntries entries", expectedEntries, log.logSegments.toList.head.timeIndex.maxEntries)

    log.truncateTo(0)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)
    assertEquals("The index of segment 1 should be resized to maxIndexSize", log.config.maxIndexSize/8, log.logSegments.toList.head.index.maxEntries)
    assertEquals("The time index of segment 1 should be resized to maxIndexSize", log.config.maxIndexSize/12, log.logSegments.toList.head.timeIndex.maxEntries)

    time.sleep(msgPerSeg)
    for (i<- 1 to msgPerSeg)
      log.appendAsLeader(TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds + i), leaderEpoch = 0)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)

    time.sleep(log.config.fileDeleteDelayMs + 1)
    log.close()
  }

  /**
   * When we open a log any index segments without an associated log segment should be deleted.
   */
  @Test
  def testBogusIndexSegmentsAreRemoved() {
    val bogusIndex1 = Log.offsetIndexFile(logDir, 0)
    val bogusTimeIndex1 = Log.timeIndexFile(logDir, 0)
    val bogusIndex2 = Log.offsetIndexFile(logDir, 5)
    val bogusTimeIndex2 = Log.timeIndexFile(logDir, 5)

    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, createRecords.sizeInBytes * 5: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    val log = Log(logDir,
      LogConfig(logProps),
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)

    assertTrue("The first index file should have been replaced with a larger file", bogusIndex1.length > 0)
    assertTrue("The first time index file should have been replaced with a larger file", bogusTimeIndex1.length > 0)
    assertFalse("The second index file should have been deleted.", bogusIndex2.exists)
    assertFalse("The second time index file should have been deleted.", bogusTimeIndex2.exists)

    // check that we can append to the log
    for (_ <- 0 until 10)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    log.delete()
  }

  /**
   * Verify that truncation works correctly after re-opening the log
   */
  @Test
  def testReopenThenTruncate() {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, createRecords.sizeInBytes * 5: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 10000: java.lang.Integer)
    val config = LogConfig(logProps)

    // create a log
    var log = Log(logDir,
      config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)

    // add enough messages to roll over several segments then close and re-open and attempt to truncate
    for (_ <- 0 until 100)
      log.appendAsLeader(createRecords, leaderEpoch = 0)
    log.close()
    log = Log(logDir,
      config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)
    log.truncateTo(3)
    assertEquals("All but one segment should be deleted.", 1, log.numberOfSegments)
    assertEquals("Log end offset should be 3.", 3, log.logEndOffset)

    time.sleep(log.config.fileDeleteDelayMs + 1)
    log.close()
  }

  /**
   * Test that deleted files are deleted after the appropriate time.
   */
  @Test
  def testAsyncDelete() {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds - 1000L)
    val asyncDeleteMs = 1000
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, createRecords.sizeInBytes * 5: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 10000: java.lang.Integer)
    logProps.put(LogConfig.FileDeleteDelayMsProp, asyncDeleteMs: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 999: java.lang.Integer)
    val config = LogConfig(logProps)

    val log = Log(logDir,
      config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)

    // append some messages to create some segments
    for (_ <- 0 until 100)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    // files should be renamed
    val segments = log.logSegments.toArray
    val oldFiles = segments.map(_.log.file) ++ segments.map(_.index.file)

    log.deleteOldSegments()

    assertEquals("Only one segment should remain.", 1, log.numberOfSegments)
    assertTrue("All log and index files should end in .deleted", segments.forall(_.log.file.getName.endsWith(Log.DeletedFileSuffix)) &&
                                                                 segments.forall(_.index.file.getName.endsWith(Log.DeletedFileSuffix)))
    assertTrue("The .deleted files should still be there.", segments.forall(_.log.file.exists) &&
                                                            segments.forall(_.index.file.exists))
    assertTrue("The original file should be gone.", oldFiles.forall(!_.exists))

    // when enough time passes the files should be deleted
    val deletedFiles = segments.map(_.log.file) ++ segments.map(_.index.file)
    time.sleep(asyncDeleteMs + 1)
    assertTrue("Files should all be gone.", deletedFiles.forall(!_.exists))

    log.close()
  }

  /**
   * Any files ending in .deleted should be removed when the log is re-opened.
   */
  @Test
  def testOpenDeletesObsoleteFiles() {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds - 1000)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, createRecords.sizeInBytes * 5: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 999: java.lang.Integer)
    val config = LogConfig(logProps)
    val log = Log(logDir,
      config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)

    // append some messages to create some segments
    for (_ <- 0 until 100)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    // expire all segments
    log.deleteOldSegments()

    // The segment files scheduled for physical deletion remain open (until fileDeleteDelayMs has elapsed), causing
    // errors to arise on Windows if a new Log instance, pointed at those files, attempts to physically delete the
    // files itself during its startup.  Our solution here is to simply copy the log's files to a fresh directory
    // and point a new Log instance at those.
    val log2Dir = TestUtils.randomPartitionLogDir(tmpDir)
    TestUtils.copyDir(logDir.toPath, log2Dir.toPath)
    val log2 = Log(log2Dir,
      config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)
    assertEquals("The deleted segments should be gone.", 1, log2.numberOfSegments)

    time.sleep(log.config.fileDeleteDelayMs + 1)
    log.close()
    log2.close()
  }

  @Test
  def testAppendMessageWithNullPayload() {
    val log = Log(logDir,
      LogConfig(),
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)
    log.appendAsLeader(TestUtils.singletonRecords(value = null), leaderEpoch = 0)
    val head = log.readUncommitted(0, 4096, None).records.records.iterator.next()
    assertEquals(0, head.offset)
    assertTrue("Message payload should be null.", !head.hasValue)
    log.close()
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testAppendWithOutOfOrderOffsetsThrowsException() {
    val log = Log(logDir,
      LogConfig(),
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)
    val records = (0 until 2).map(id => new SimpleRecord(id.toString.getBytes)).toArray
    records.foreach(record => log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE, record), leaderEpoch = 0))
    val invalidRecord = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord(1.toString.getBytes))
    try {
      log.appendAsFollower(invalidRecord)
    } finally {
      log.close()
    }
  }

  @Test
  def testAppendWithNoTimestamp(): Unit = {
    val log = Log(logDir,
      LogConfig(),
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord(RecordBatch.NO_TIMESTAMP, "key".getBytes, "value".getBytes)), leaderEpoch = 0)
    log.close()
  }

  @Test
  def testCorruptLog() {
    // append some messages to create some segments
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    logProps.put(LogConfig.MaxMessageBytesProp, 64*1024: java.lang.Integer)
    val config = LogConfig(logProps)
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds)
    val recoveryPoint = 50L
    for (_ <- 0 until 10) {
      // create a log and write some messages to it
      logDir.mkdirs()
      var log = Log(logDir,
        config,
        logStartOffset = 0L,
        recoveryPoint = 0L,
        scheduler = time.scheduler,
        brokerTopicStats = brokerTopicStats,
        time = time)
      val numMessages = 50 + TestUtils.random.nextInt(50)
      for (_ <- 0 until numMessages)
        log.appendAsLeader(createRecords, leaderEpoch = 0)
      val records = log.logSegments.flatMap(_.log.records.asScala.toList).toList
      log.close()

      // corrupt index and log by appending random bytes
      TestUtils.appendNonsenseToFile(log.activeSegment.index.file, TestUtils.random.nextInt(1024) + 1)
      TestUtils.appendNonsenseToFile(log.activeSegment.log.file, TestUtils.random.nextInt(1024) + 1)

      // attempt recovery
      log = Log(logDir, config, 0L, recoveryPoint, time.scheduler, brokerTopicStats, time)
      assertEquals(numMessages, log.logEndOffset)

      val recovered = log.logSegments.flatMap(_.log.records.asScala.toList).toList
      assertEquals(records.size, recovered.size)

      for (i <- records.indices) {
        val expected = records(i)
        val actual = recovered(i)
        assertEquals(s"Keys not equal", expected.key, actual.key)
        assertEquals(s"Values not equal", expected.value, actual.value)
        assertEquals(s"Timestamps not equal", expected.timestamp, actual.timestamp)
      }

      log.close()
      Utils.delete(logDir)
    }
  }

  @Test
  def testOverCompactedLogRecovery(): Unit = {
    // append some messages to create some segments
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.MaxMessageBytesProp, 64*1024: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    val config = LogConfig(logProps)
    val log = Log(logDir,
      config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)
    val set1 = MemoryRecords.withRecords(0, CompressionType.NONE, 0, new SimpleRecord("v1".getBytes(), "k1".getBytes()))
    val set2 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 2, CompressionType.NONE, 0, new SimpleRecord("v3".getBytes(), "k3".getBytes()))
    val set3 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 3, CompressionType.NONE, 0, new SimpleRecord("v4".getBytes(), "k4".getBytes()))
    val set4 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 4, CompressionType.NONE, 0, new SimpleRecord("v5".getBytes(), "k5".getBytes()))
    //Writes into an empty log with baseOffset 0
    log.appendAsFollower(set1)
    assertEquals(0L, log.activeSegment.baseOffset)
    //This write will roll the segment, yielding a new segment with base offset = max(2, 1) = 2
    log.appendAsFollower(set2)
    assertEquals(2L, log.activeSegment.baseOffset)
    assertTrue(Log.producerSnapshotFile(logDir, 2L).exists)
    //This will also roll the segment, yielding a new segment with base offset = max(3, Integer.MAX_VALUE+3) = Integer.MAX_VALUE+3
    log.appendAsFollower(set3)
    assertEquals(Integer.MAX_VALUE.toLong + 3, log.activeSegment.baseOffset)
    assertTrue(Log.producerSnapshotFile(logDir, Integer.MAX_VALUE.toLong + 3).exists)
    //This will go into the existing log
    log.appendAsFollower(set4)
    assertEquals(Integer.MAX_VALUE.toLong + 3, log.activeSegment.baseOffset)
    log.close()
    val indexFiles = logDir.listFiles.filter(file => file.getName.contains(".index"))
    assertEquals(3, indexFiles.length)
    for (file <- indexFiles) {
      val offsetIndex = new OffsetIndex(file, file.getName.replace(".index","").toLong)
      assertTrue(offsetIndex.lastOffset >= 0)
      offsetIndex.close()
    }
    Utils.delete(logDir)
  }

  @Test
  def testCleanShutdownFile() {
    // append some messages to create some segments
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.MaxMessageBytesProp, 64*1024: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    val config = LogConfig(logProps)
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds)

    val cleanShutdownFile = createCleanShutdownFile()
    assertTrue(".kafka_cleanshutdown must exist", cleanShutdownFile.exists())
    var recoveryPoint = 0L
    // create a log and write some messages to it
    var log = Log(logDir,
      config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)
    for (_ <- 0 until 100)
      log.appendAsLeader(createRecords, leaderEpoch = 0)
    log.close()

    // check if recovery was attempted. Even if the recovery point is 0L, recovery should not be attempted as the
    // clean shutdown file exists.
    recoveryPoint = log.logEndOffset
    log = Log(logDir, config, 0L, 0L, time.scheduler, brokerTopicStats, time)
    assertEquals(recoveryPoint, log.logEndOffset)
    cleanShutdownFile.delete()
    log.close()
  }

  @Test
  def testParseTopicPartitionName() {
    val topic = "test_topic"
    val partition = "143"
    val dir = new File(logDir, topicPartitionName(topic, partition))
    val topicPartition = Log.parseTopicPartitionName(dir)
    assertEquals(topic, topicPartition.topic)
    assertEquals(partition.toInt, topicPartition.partition)
  }

  /**
   * Tests that log directories with a period in their name that have been marked for deletion
   * are parsed correctly by `Log.parseTopicPartitionName` (see KAFKA-5232 for details).
   */
  @Test
  def testParseTopicPartitionNameWithPeriodForDeletedTopic() {
    val topic = "foo.bar-testtopic"
    val partition = "42"
    val dir = new File(logDir, Log.logDeleteDirName(topicPartitionName(topic, partition)))
    val topicPartition = Log.parseTopicPartitionName(dir)
    assertEquals("Unexpected topic name parsed", topic, topicPartition.topic)
    assertEquals("Unexpected partition number parsed", partition.toInt, topicPartition.partition)
  }

  @Test
  def testParseTopicPartitionNameForEmptyName() {
    try {
      val dir = new File("")
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
    } catch {
      case _: KafkaException => // its GOOD!
    }
  }

  @Test
  def testParseTopicPartitionNameForNull() {
    try {
      val dir: File = null
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir)
    } catch {
      case _: KafkaException => // its GOOD!
    }
  }

  @Test
  def testParseTopicPartitionNameForMissingSeparator() {
    val topic = "test_topic"
    val partition = "1999"
    val dir = new File(logDir, topic + partition)
    try {
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
    } catch {
      case _: KafkaException => // expected
    }
    // also test the "-delete" marker case
    val deleteMarkerDir = new File(logDir, Log.logDeleteDirName(topic + partition))
    try {
      Log.parseTopicPartitionName(deleteMarkerDir)
      fail("KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath)
    } catch {
      case _: KafkaException => // expected
    }
  }

  @Test
  def testParseTopicPartitionNameForMissingTopic() {
    val topic = ""
    val partition = "1999"
    val dir = new File(logDir, topicPartitionName(topic, partition))
    try {
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
    } catch {
      case _: KafkaException => // expected
    }
    // also test the "-delete" marker case
    val deleteMarkerDir = new File(logDir, Log.logDeleteDirName(topicPartitionName(topic, partition)))
    try {
      Log.parseTopicPartitionName(deleteMarkerDir)
      fail("KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath)
    } catch {
      case _: KafkaException => // expected
    }

  }

  @Test
  def testParseTopicPartitionNameForMissingPartition() {
    val topic = "test_topic"
    val partition = ""
    val dir = new File(logDir + topicPartitionName(topic, partition))
    try {
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
    } catch {
      case _: KafkaException => // expected
    }
    // also test the "-delete" marker case
    val deleteMarkerDir = new File(logDir, Log.logDeleteDirName(topicPartitionName(topic, partition)))
    try {
      Log.parseTopicPartitionName(deleteMarkerDir)
      fail("KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath)
    } catch {
      case _: KafkaException => // expected
    }
  }

  @Test
  def testParseTopicPartitionNameForInvalidPartition() {
    val topic = "test_topic"
    val partition = "1999a"
    val dir = new File(logDir, topicPartitionName(topic, partition))
    try {
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
    } catch {
      case _: KafkaException => // expected
    }
    // also test the "-delete" marker case
    val deleteMarkerDir = new File(logDir, Log.logDeleteDirName(topic + partition))
    try {
      Log.parseTopicPartitionName(deleteMarkerDir)
      fail("KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath)
    } catch {
      case _: KafkaException => // expected
    }
  }

  @Test
  def testParseTopicPartitionNameForExistingInvalidDir() {
    val dir1 = new File(logDir + "/non_kafka_dir")
    try {
      Log.parseTopicPartitionName(dir1)
      fail("KafkaException should have been thrown for dir: " + dir1.getCanonicalPath)
    } catch {
      case _: KafkaException => // should only throw KafkaException
    }
    val dir2 = new File(logDir + "/non_kafka_dir-delete")
    try {
      Log.parseTopicPartitionName(dir2)
      fail("KafkaException should have been thrown for dir: " + dir2.getCanonicalPath)
    } catch {
      case _: KafkaException => // should only throw KafkaException
    }
  }

  def topicPartitionName(topic: String, partition: String): String =
    topic + "-" + partition

  @Test
  def testDeleteOldSegmentsMethod() {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds - 1000)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, createRecords.sizeInBytes * 5: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 999: java.lang.Integer)
    val config = LogConfig(logProps)
    val log = Log(logDir,
      config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time)

    // append some messages to create some segments
    for (_ <- 0 until 100)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    log.leaderEpochCache.assign(0, 40)
    log.leaderEpochCache.assign(1, 90)

    // expire all segments
    log.deleteOldSegments()
    assertEquals("The deleted segments should be gone.", 1, log.numberOfSegments)
    assertEquals("Epoch entries should have gone.", 1, epochCache(log).epochEntries().size)
    assertEquals("Epoch entry should be the latest epoch and the leo.", new EpochEntry(1, 100), epochCache(log).epochEntries().head)

    time.sleep(log.config.fileDeleteDelayMs + 1)

    // append some messages to create some segments
    for (_ <- 0 until 100)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    log.close()
    log.delete()
    assertEquals("The number of segments should be 0", 0, log.numberOfSegments)
    assertEquals("The number of deleted segments should be zero.", 0, log.deleteOldSegments())
    assertEquals("Epoch entries should have gone.", 0, epochCache(log).epochEntries().size)
  }

  @Test
  def testLogDeletionAfterDeleteRecords() {
    def createRecords = TestUtils.singletonRecords("test".getBytes)
    val log = createLog(createRecords.sizeInBytes)

    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, leaderEpoch = 0)
    assertEquals("should have 3 segments", 3, log.numberOfSegments)
    assertEquals(log.logStartOffset, 0)

    log.maybeIncrementLogStartOffset(1)
    log.deleteOldSegments()
    assertEquals("should have 3 segments", 3, log.numberOfSegments)
    assertEquals(log.logStartOffset, 1)

    log.maybeIncrementLogStartOffset(6)
    log.deleteOldSegments()
    assertEquals("should have 2 segments", 2, log.numberOfSegments)
    assertEquals(log.logStartOffset, 6)

    log.maybeIncrementLogStartOffset(15)
    log.deleteOldSegments()
    assertEquals("should have 1 segments", 1, log.numberOfSegments)
    assertEquals(log.logStartOffset, 15)

    time.sleep(log.config.fileDeleteDelayMs + 1)
    log.close()
  }

  def epochCache(log: Log): LeaderEpochFileCache = {
    log.leaderEpochCache.asInstanceOf[LeaderEpochFileCache]
  }

  @Test
  def shouldDeleteSizeBasedSegments() {
    def createRecords = TestUtils.singletonRecords("test".getBytes)
    val log = createLog(createRecords.sizeInBytes, retentionBytes = createRecords.sizeInBytes * 10)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    log.deleteOldSegments
    assertEquals("should have 2 segments", 2,log.numberOfSegments)

    time.sleep(log.config.fileDeleteDelayMs + 1)
    log.close()
  }

  @Test
  def shouldNotDeleteSizeBasedSegmentsWhenUnderRetentionSize() {
    def createRecords = TestUtils.singletonRecords("test".getBytes)
    val log = createLog(createRecords.sizeInBytes, retentionBytes = createRecords.sizeInBytes * 15)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    log.deleteOldSegments
    assertEquals("should have 3 segments", 3,log.numberOfSegments)

    log.close()
  }

  @Test
  def shouldDeleteTimeBasedSegmentsReadyToBeDeleted() {
    def createRecords = TestUtils.singletonRecords("test".getBytes, timestamp = 10)
    val log = createLog(createRecords.sizeInBytes, retentionMs = 10000)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    log.deleteOldSegments()
    assertEquals("There should be 1 segment remaining", 1, log.numberOfSegments)

    time.sleep(log.config.fileDeleteDelayMs + 1)
    log.close()
  }

  @Test
  def shouldNotDeleteTimeBasedSegmentsWhenNoneReadyToBeDeleted() {
    def createRecords = TestUtils.singletonRecords("test".getBytes, timestamp = time.milliseconds)
    val log = createLog(createRecords.sizeInBytes, retentionMs = 10000000)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    log.deleteOldSegments()
    assertEquals("There should be 3 segments remaining", 3, log.numberOfSegments)

    log.close()
  }

  @Test
  def shouldNotDeleteSegmentsWhenPolicyDoesNotIncludeDelete() {
    def createRecords = TestUtils.singletonRecords("test".getBytes, key = "test".getBytes(), timestamp = 10L)
    val log = createLog(createRecords.sizeInBytes,
      retentionMs = 10000,
      cleanupPolicy = "compact")

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    // mark oldest segment as older the retention.ms
    log.logSegments.head.lastModified = time.milliseconds - 20000

    val segments = log.numberOfSegments
    log.deleteOldSegments()
    assertEquals("There should be 3 segments remaining", segments, log.numberOfSegments)

    log.close()
  }

  @Test
  def shouldDeleteSegmentsReadyToBeDeletedWhenCleanupPolicyIsCompactAndDelete() {
    def createRecords = TestUtils.singletonRecords("test".getBytes, key = "test".getBytes, timestamp = 10L)
    val log = createLog(createRecords.sizeInBytes,
      retentionMs = 10000,
      cleanupPolicy = "compact,delete")

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    log.deleteOldSegments()
    assertEquals("There should be 1 segment remaining", 1, log.numberOfSegments)

    time.sleep(log.config.fileDeleteDelayMs + 1)
    log.close()
  }

  @Test
  def shouldApplyEpochToMessageOnAppendIfLeader() {
    val records = (0 until 50).toArray.map(id => new SimpleRecord(id.toString.getBytes))

    //Given this partition is on leader epoch 72
    val epoch = 72
    val log = Log(logDir, LogConfig(), recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    log.leaderEpochCache.assign(epoch, records.size)

    //When appending messages as a leader (i.e. assignOffsets = true)
    for (record <- records)
      log.appendAsLeader(
        MemoryRecords.withRecords(CompressionType.NONE, record),
        leaderEpoch = epoch
      )

    //Then leader epoch should be set on messages
    for (i <- records.indices) {
      val read = log.readUncommitted(i, 100, Some(i+1)).records.batches.iterator.next()
      assertEquals("Should have set leader epoch", 72, read.partitionLeaderEpoch)
    }

    log.close()
  }

  @Test
  def followerShouldSaveEpochInformationFromReplicatedMessagesToTheEpochCache() {
    val messageIds = (0 until 50).toArray
    val records = messageIds.map(id => new SimpleRecord(id.toString.getBytes))

    //Given each message has an offset & epoch, as msgs from leader would
    def recordsForEpoch(i: Int): MemoryRecords = {
      val recs = MemoryRecords.withRecords(messageIds(i), CompressionType.NONE, records(i))
      recs.batches.asScala.foreach{record =>
        record.setPartitionLeaderEpoch(42)
        record.setLastOffset(i)
      }
      recs
    }

    val log = Log(logDir, LogConfig(), recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)

    //When appending as follower (assignOffsets = false)
    for (i <- records.indices)
      log.appendAsFollower(recordsForEpoch(i))

    assertEquals(42, log.leaderEpochCache.asInstanceOf[LeaderEpochFileCache].latestEpoch())

    log.close()
  }

  @Test
  def shouldTruncateLeaderEpochsWhenDeletingSegments() {
    def createRecords = TestUtils.singletonRecords("test".getBytes)
    val log = createLog(createRecords.sizeInBytes, retentionBytes = createRecords.sizeInBytes * 10)
    val cache = epochCache(log)

    // Given three segments of 5 messages each
    for (e <- 0 until 15) {
      log.appendAsLeader(createRecords, leaderEpoch = 0)
    }

    //Given epochs
    cache.assign(0, 0)
    cache.assign(1, 5)
    cache.assign(2, 10)

    //When first segment is removed
    log.deleteOldSegments

    //The oldest epoch entry should have been removed
    assertEquals(ListBuffer(EpochEntry(1, 5), EpochEntry(2, 10)), cache.epochEntries)

    time.sleep(log.config.fileDeleteDelayMs + 1)
    log.close()
  }

  @Test
  def shouldUpdateOffsetForLeaderEpochsWhenDeletingSegments() {
    def createRecords = TestUtils.singletonRecords("test".getBytes)
    val log = createLog(createRecords.sizeInBytes, retentionBytes = createRecords.sizeInBytes * 10)
    val cache = epochCache(log)

    // Given three segments of 5 messages each
    for (e <- 0 until 15) {
      log.appendAsLeader(createRecords, leaderEpoch = 0)
    }

    //Given epochs
    cache.assign(0, 0)
    cache.assign(1, 7)
    cache.assign(2, 10)

    //When first segment removed (up to offset 5)
    log.deleteOldSegments

    //The the first entry should have gone from (0,0) => (0,5)
    assertEquals(ListBuffer(EpochEntry(0, 5), EpochEntry(1, 7), EpochEntry(2, 10)), cache.epochEntries)

    time.sleep(log.config.fileDeleteDelayMs + 1)
    log.close()
  }

  @Test
  def shouldTruncateLeaderEpochFileWhenTruncatingLog() {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds)
    val logProps = CoreUtils.propsWith(LogConfig.SegmentBytesProp, (10 * createRecords.sizeInBytes).toString)
    val log = Log(logDir, LogConfig( logProps), recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    val cache = epochCache(log)

    //Given 2 segments, 10 messages per segment
    for (epoch <- 1 to 20)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    //Simulate some leader changes at specific offsets
    cache.assign(0, 0)
    cache.assign(1, 10)
    cache.assign(2, 16)

    assertEquals(2, log.numberOfSegments)
    assertEquals(20, log.logEndOffset)

    //When truncate to LEO (no op)
    log.truncateTo(log.logEndOffset)

    //Then no change
    assertEquals(3, cache.epochEntries.size)

    //When truncate
    log.truncateTo(11)

    //Then no change
    assertEquals(2, cache.epochEntries.size)

    //When truncate
    log.truncateTo(10)

    //Then
    assertEquals(1, cache.epochEntries.size)

    //When truncate all
    log.truncateTo(0)

    //Then
    assertEquals(0, cache.epochEntries.size)

    time.sleep(log.config.fileDeleteDelayMs + 1)
    log.close()
  }

  /**
    * Append a bunch of messages to a log and then re-open it with recovery and check that the leader epochs are recovered properly.
    */
  @Test
  def testLogRecoversForLeaderEpoch() {
    val log = Log(logDir, LogConfig(new Properties()), recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    val leaderEpochCache = epochCache(log)
    val firstBatch = singletonRecordsWithLeaderEpoch(value = "random".getBytes, leaderEpoch = 1, offset = 0)
    log.appendAsFollower(records = firstBatch)

    val secondBatch = singletonRecordsWithLeaderEpoch(value = "random".getBytes, leaderEpoch = 2, offset = 1)
    log.appendAsFollower(records = secondBatch)

    val thirdBatch = singletonRecordsWithLeaderEpoch(value = "random".getBytes, leaderEpoch = 2, offset = 2)
    log.appendAsFollower(records = thirdBatch)

    val fourthBatch = singletonRecordsWithLeaderEpoch(value = "random".getBytes, leaderEpoch = 3, offset = 3)
    log.appendAsFollower(records = fourthBatch)

    assertEquals(ListBuffer(EpochEntry(1, 0), EpochEntry(2, 1), EpochEntry(3, 3)), leaderEpochCache.epochEntries)

    // deliberately remove some of the epoch entries
    leaderEpochCache.clearAndFlushLatest(2)
    assertNotEquals(ListBuffer(EpochEntry(1, 0), EpochEntry(2, 1), EpochEntry(3, 3)), leaderEpochCache.epochEntries)
    log.close()

    // reopen the log and recover from the beginning
    val recoveredLog = Log(logDir, LogConfig(new Properties()), recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats, time = time)
    val recoveredLeaderEpochCache = epochCache(recoveredLog)

    // epoch entries should be recovered
    assertEquals(ListBuffer(EpochEntry(1, 0), EpochEntry(2, 1), EpochEntry(3, 3)), recoveredLeaderEpochCache.epochEntries)
    recoveredLog.close()
  }

  /**
    * Wrap a single record log buffer with leader epoch.
    */
  private def singletonRecordsWithLeaderEpoch(value: Array[Byte],
                                      key: Array[Byte] = null,
                                      leaderEpoch: Int,
                                      offset: Long,
                                      codec: CompressionType = CompressionType.NONE,
                                      timestamp: Long = RecordBatch.NO_TIMESTAMP,
                                      magicValue: Byte = RecordBatch.CURRENT_MAGIC_VALUE): MemoryRecords = {
    val records = Seq(new SimpleRecord(timestamp, key, value))

    val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
    val builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, offset,
      System.currentTimeMillis, leaderEpoch)
    records.foreach(builder.append)
    builder.build()
  }

  def testFirstUnstableOffsetNoTransactionalData() {
    val log = createLog(1024 * 1024)

    val records = MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord("foo".getBytes),
      new SimpleRecord("bar".getBytes),
      new SimpleRecord("baz".getBytes))

    log.appendAsLeader(records, leaderEpoch = 0)
    assertEquals(None, log.firstUnstableOffset)
  }

  @Test
  def testFirstUnstableOffsetWithTransactionalData() {
    val log = createLog(1024 * 1024)

    val pid = 137L
    val epoch = 5.toShort
    var seq = 0

    // add some transactional records
    val records = MemoryRecords.withTransactionalRecords(CompressionType.NONE, pid, epoch, seq,
      new SimpleRecord("foo".getBytes),
      new SimpleRecord("bar".getBytes),
      new SimpleRecord("baz".getBytes))

    val firstAppendInfo = log.appendAsLeader(records, leaderEpoch = 0)
    assertEquals(Some(firstAppendInfo.firstOffset), log.firstUnstableOffset.map(_.messageOffset))

    // add more transactional records
    seq += 3
    log.appendAsLeader(MemoryRecords.withTransactionalRecords(CompressionType.NONE, pid, epoch, seq,
      new SimpleRecord("blah".getBytes)), leaderEpoch = 0)

    // LSO should not have changed
    assertEquals(Some(firstAppendInfo.firstOffset), log.firstUnstableOffset.map(_.messageOffset))

    // now transaction is committed
    val commitAppendInfo = log.appendAsLeader(endTxnRecords(ControlRecordType.COMMIT, pid, epoch),
      isFromClient = false, leaderEpoch = 0)

    // first unstable offset is not updated until the high watermark is advanced
    assertEquals(Some(firstAppendInfo.firstOffset), log.firstUnstableOffset.map(_.messageOffset))
    log.onHighWatermarkIncremented(commitAppendInfo.lastOffset + 1)

    // now there should be no first unstable offset
    assertEquals(None, log.firstUnstableOffset)

    log.close()
  }

  @Test
  def testTransactionIndexUpdated(): Unit = {
    val log = createLog(1024 * 1024)
    val epoch = 0.toShort

    val pid1 = 1L
    val pid2 = 2L
    val pid3 = 3L
    val pid4 = 4L

    val appendPid1 = appendTransactionalAsLeader(log, pid1, epoch)
    val appendPid2 = appendTransactionalAsLeader(log, pid2, epoch)
    val appendPid3 = appendTransactionalAsLeader(log, pid3, epoch)
    val appendPid4 = appendTransactionalAsLeader(log, pid4, epoch)

    // mix transactional and non-transactional data
    appendPid1(5) // nextOffset: 5
    appendNonTransactionalAsLeader(log, 3) // 8
    appendPid2(2) // 10
    appendPid1(4) // 14
    appendPid3(3) // 17
    appendNonTransactionalAsLeader(log, 2) // 19
    appendPid1(10) // 29
    appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT) // 30
    appendPid2(6) // 36
    appendPid4(3) // 39
    appendNonTransactionalAsLeader(log, 10) // 49
    appendPid3(9) // 58
    appendEndTxnMarkerAsLeader(log, pid3, epoch, ControlRecordType.COMMIT) // 59
    appendPid4(8) // 67
    appendPid2(7) // 74
    appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.ABORT) // 75
    appendNonTransactionalAsLeader(log, 10) // 85
    appendPid4(4) // 89
    appendEndTxnMarkerAsLeader(log, pid4, epoch, ControlRecordType.COMMIT) // 90

    val abortedTransactions = allAbortedTransactions(log)
    assertEquals(List(new AbortedTxn(pid1, 0L, 29L, 8L), new AbortedTxn(pid2, 8L, 74L, 36L)), abortedTransactions)

    log.close()
  }

  @Test
  def testFullTransactionIndexRecovery(): Unit = {
    val log = createLog(128)
    val epoch = 0.toShort

    val pid1 = 1L
    val pid2 = 2L
    val pid3 = 3L
    val pid4 = 4L

    val appendPid1 = appendTransactionalAsLeader(log, pid1, epoch)
    val appendPid2 = appendTransactionalAsLeader(log, pid2, epoch)
    val appendPid3 = appendTransactionalAsLeader(log, pid3, epoch)
    val appendPid4 = appendTransactionalAsLeader(log, pid4, epoch)

    // mix transactional and non-transactional data
    appendPid1(5) // nextOffset: 5
    appendNonTransactionalAsLeader(log, 3) // 8
    appendPid2(2) // 10
    appendPid1(4) // 14
    appendPid3(3) // 17
    appendNonTransactionalAsLeader(log, 2) // 19
    appendPid1(10) // 29
    appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT) // 30
    appendPid2(6) // 36
    appendPid4(3) // 39
    appendNonTransactionalAsLeader(log, 10) // 49
    appendPid3(9) // 58
    appendEndTxnMarkerAsLeader(log, pid3, epoch, ControlRecordType.COMMIT) // 59
    appendPid4(8) // 67
    appendPid2(7) // 74
    appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.ABORT) // 75
    appendNonTransactionalAsLeader(log, 10) // 85
    appendPid4(4) // 89
    appendEndTxnMarkerAsLeader(log, pid4, epoch, ControlRecordType.COMMIT) // 90

    // delete all the offset and transaction index files to force recovery
    val indexFilePairs = log.logSegments.map(segment => (segment.index.file, segment.txnIndex.file))
    log.close()
    indexFilePairs.foreach { indexFilePair =>
      Files.deleteIfExists(indexFilePair._1.toPath)
      Files.deleteIfExists(indexFilePair._2.toPath)
    }

    val reloadedLog = createLog(1024)
    val abortedTransactions = allAbortedTransactions(reloadedLog)
    assertEquals(List(new AbortedTxn(pid1, 0L, 29L, 8L), new AbortedTxn(pid2, 8L, 74L, 36L)), abortedTransactions)
    reloadedLog.close()
  }

  @Test
  def testRecoverOnlyLastSegment(): Unit = {
    val log = createLog(128)
    val epoch = 0.toShort

    val pid1 = 1L
    val pid2 = 2L
    val pid3 = 3L
    val pid4 = 4L

    val appendPid1 = appendTransactionalAsLeader(log, pid1, epoch)
    val appendPid2 = appendTransactionalAsLeader(log, pid2, epoch)
    val appendPid3 = appendTransactionalAsLeader(log, pid3, epoch)
    val appendPid4 = appendTransactionalAsLeader(log, pid4, epoch)

    // mix transactional and non-transactional data
    appendPid1(5) // nextOffset: 5
    appendNonTransactionalAsLeader(log, 3) // 8
    appendPid2(2) // 10
    appendPid1(4) // 14
    appendPid3(3) // 17
    appendNonTransactionalAsLeader(log, 2) // 19
    appendPid1(10) // 29
    appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT) // 30
    appendPid2(6) // 36
    appendPid4(3) // 39
    appendNonTransactionalAsLeader(log, 10) // 49
    appendPid3(9) // 58
    appendEndTxnMarkerAsLeader(log, pid3, epoch, ControlRecordType.COMMIT) // 59
    appendPid4(8) // 67
    appendPid2(7) // 74
    appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.ABORT) // 75
    appendNonTransactionalAsLeader(log, 10) // 85
    appendPid4(4) // 89
    appendEndTxnMarkerAsLeader(log, pid4, epoch, ControlRecordType.COMMIT) // 90

    // delete the last offset and transaction index files to force recovery
    val lastSegment = log.logSegments.last
    val recoveryPoint = lastSegment.baseOffset
    val indexFilePair = (lastSegment.index.file, lastSegment.txnIndex.file)
    log.close()
    Files.deleteIfExists(indexFilePair._1.toPath)
    Files.deleteIfExists(indexFilePair._2.toPath)

    val reloadedLog = createLog(1024, recoveryPoint = recoveryPoint)
    val abortedTransactions = allAbortedTransactions(reloadedLog)
    assertEquals(List(new AbortedTxn(pid1, 0L, 29L, 8L), new AbortedTxn(pid2, 8L, 74L, 36L)), abortedTransactions)
    reloadedLog.close()
  }

  @Test
  def testRecoverLastSegmentWithNoSnapshots(): Unit = {
    val log = createLog(128)
    val epoch = 0.toShort

    val pid1 = 1L
    val pid2 = 2L
    val pid3 = 3L
    val pid4 = 4L

    val appendPid1 = appendTransactionalAsLeader(log, pid1, epoch)
    val appendPid2 = appendTransactionalAsLeader(log, pid2, epoch)
    val appendPid3 = appendTransactionalAsLeader(log, pid3, epoch)
    val appendPid4 = appendTransactionalAsLeader(log, pid4, epoch)

    // mix transactional and non-transactional data
    appendPid1(5) // nextOffset: 5
    appendNonTransactionalAsLeader(log, 3) // 8
    appendPid2(2) // 10
    appendPid1(4) // 14
    appendPid3(3) // 17
    appendNonTransactionalAsLeader(log, 2) // 19
    appendPid1(10) // 29
    appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT) // 30
    appendPid2(6) // 36
    appendPid4(3) // 39
    appendNonTransactionalAsLeader(log, 10) // 49
    appendPid3(9) // 58
    appendEndTxnMarkerAsLeader(log, pid3, epoch, ControlRecordType.COMMIT) // 59
    appendPid4(8) // 67
    appendPid2(7) // 74
    appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.ABORT) // 75
    appendNonTransactionalAsLeader(log, 10) // 85
    appendPid4(4) // 89
    appendEndTxnMarkerAsLeader(log, pid4, epoch, ControlRecordType.COMMIT) // 90

    log.close()

    // delete all snapshot files
    logDir.listFiles.filter(f => f.isFile && f.getName.endsWith(Log.PidSnapshotFileSuffix)).foreach { file =>
      Files.delete(file.toPath)
    }

    // delete the last offset and transaction index files to force recovery. this should force us to rebuild
    // the producer state from the start of the log
    val lastSegment = log.logSegments.last
    val recoveryPoint = lastSegment.baseOffset
    val indexFilePair = (lastSegment.index.file, lastSegment.txnIndex.file)
    Files.deleteIfExists(indexFilePair._1.toPath)
    Files.deleteIfExists(indexFilePair._2.toPath)

    val reloadedLog = createLog(1024, recoveryPoint = recoveryPoint)
    val abortedTransactions = allAbortedTransactions(reloadedLog)
    assertEquals(List(new AbortedTxn(pid1, 0L, 29L, 8L), new AbortedTxn(pid2, 8L, 74L, 36L)), abortedTransactions)
    reloadedLog.close()
  }

  @Test
  def testTransactionIndexUpdatedThroughReplication(): Unit = {
    val epoch = 0.toShort
    val log = createLog(1024 * 1024)
    val buffer = ByteBuffer.allocate(2048)

    val pid1 = 1L
    val pid2 = 2L
    val pid3 = 3L
    val pid4 = 4L

    val appendPid1 = appendTransactionalToBuffer(buffer, pid1, epoch)
    val appendPid2 = appendTransactionalToBuffer(buffer, pid2, epoch)
    val appendPid3 = appendTransactionalToBuffer(buffer, pid3, epoch)
    val appendPid4 = appendTransactionalToBuffer(buffer, pid4, epoch)

    appendPid1(0L, 5)
    appendNonTransactionalToBuffer(buffer, 5L, 3)
    appendPid2(8L, 2)
    appendPid1(10L, 4)
    appendPid3(14L, 3)
    appendNonTransactionalToBuffer(buffer, 17L, 2)
    appendPid1(19L, 10)
    appendEndTxnMarkerToBuffer(buffer, pid1, epoch, 29L, ControlRecordType.ABORT)
    appendPid2(30L, 6)
    appendPid4(36L, 3)
    appendNonTransactionalToBuffer(buffer, 39L, 10)
    appendPid3(49L, 9)
    appendEndTxnMarkerToBuffer(buffer, pid3, epoch, 58L, ControlRecordType.COMMIT)
    appendPid4(59L, 8)
    appendPid2(67L, 7)
    appendEndTxnMarkerToBuffer(buffer, pid2, epoch, 74L, ControlRecordType.ABORT)
    appendNonTransactionalToBuffer(buffer, 75L, 10)
    appendPid4(85L, 4)
    appendEndTxnMarkerToBuffer(buffer, pid4, epoch, 89L, ControlRecordType.COMMIT)

    buffer.flip()

    appendAsFollower(log, MemoryRecords.readableRecords(buffer))

    val abortedTransactions = allAbortedTransactions(log)
    assertEquals(List(new AbortedTxn(pid1, 0L, 29L, 8L), new AbortedTxn(pid2, 8L, 74L, 36L)), abortedTransactions)

    log.close()
  }

  @Test(expected = classOf[TransactionCoordinatorFencedException])
  def testZombieCoordinatorFenced(): Unit = {
    val pid = 1L
    val epoch = 0.toShort
    val log = createLog(1024 * 1024)

    val append = appendTransactionalAsLeader(log, pid, epoch)

    append(10)
    appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, coordinatorEpoch = 1)

    append(5)
    appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.COMMIT, coordinatorEpoch = 2)

    try {
      appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, coordinatorEpoch = 1)
    } finally {
      log.close()
    }
  }

  @Test
  def testFirstUnstableOffsetDoesNotExceedLogStartOffsetMidSegment(): Unit = {
    val log = createLog(1024 * 1024)
    val epoch = 0.toShort
    val pid = 1L
    val appendPid = appendTransactionalAsLeader(log, pid, epoch)

    appendPid(5)
    appendNonTransactionalAsLeader(log, 3)
    assertEquals(8L, log.logEndOffset)

    log.roll()
    assertEquals(2, log.logSegments.size)
    appendPid(5)

    assertEquals(Some(0L), log.firstUnstableOffset.map(_.messageOffset))

    log.maybeIncrementLogStartOffset(5L)

    // the first unstable offset should be lower bounded by the log start offset
    assertEquals(Some(5L), log.firstUnstableOffset.map(_.messageOffset))

    log.close()
  }

  @Test
  def testFirstUnstableOffsetDoesNotExceedLogStartOffsetAfterSegmentDeletion(): Unit = {
    val log = createLog(1024 * 1024)
    val epoch = 0.toShort
    val pid = 1L
    val appendPid = appendTransactionalAsLeader(log, pid, epoch)

    appendPid(5)
    appendNonTransactionalAsLeader(log, 3)
    assertEquals(8L, log.logEndOffset)

    log.roll()
    assertEquals(2, log.logSegments.size)
    appendPid(5)

    assertEquals(Some(0L), log.firstUnstableOffset.map(_.messageOffset))

    log.maybeIncrementLogStartOffset(8L)
    log.deleteOldSegments()
    assertEquals(1, log.logSegments.size)

    // the first unstable offset should be lower bounded by the log start offset
    assertEquals(Some(8L), log.firstUnstableOffset.map(_.messageOffset))

    time.sleep(log.config.fileDeleteDelayMs + 1)
    log.close()
  }

  @Test
  def testLastStableOffsetWithMixedProducerData() {
    val log = createLog(1024 * 1024)

    // for convenience, both producers share the same epoch
    val epoch = 5.toShort

    val pid1 = 137L
    val seq1 = 0
    val pid2 = 983L
    val seq2 = 0

    // add some transactional records
    val firstAppendInfo = log.appendAsLeader(MemoryRecords.withTransactionalRecords(CompressionType.NONE, pid1, epoch, seq1,
      new SimpleRecord("a".getBytes),
      new SimpleRecord("b".getBytes),
      new SimpleRecord("c".getBytes)), leaderEpoch = 0)
    assertEquals(Some(firstAppendInfo.firstOffset), log.firstUnstableOffset.map(_.messageOffset))

    // mix in some non-transactional data
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord("g".getBytes),
      new SimpleRecord("h".getBytes),
      new SimpleRecord("i".getBytes)), leaderEpoch = 0)

    // append data from a second transactional producer
    val secondAppendInfo = log.appendAsLeader(MemoryRecords.withTransactionalRecords(CompressionType.NONE, pid2, epoch, seq2,
      new SimpleRecord("d".getBytes),
      new SimpleRecord("e".getBytes),
      new SimpleRecord("f".getBytes)), leaderEpoch = 0)

    // LSO should not have changed
    assertEquals(Some(firstAppendInfo.firstOffset), log.firstUnstableOffset.map(_.messageOffset))

    // now first producer's transaction is aborted
    val abortAppendInfo = log.appendAsLeader(endTxnRecords(ControlRecordType.ABORT, pid1, epoch),
      isFromClient = false, leaderEpoch = 0)
    log.onHighWatermarkIncremented(abortAppendInfo.lastOffset + 1)

    // LSO should now point to one less than the first offset of the second transaction
    assertEquals(Some(secondAppendInfo.firstOffset), log.firstUnstableOffset.map(_.messageOffset))

    // commit the second transaction
    val commitAppendInfo = log.appendAsLeader(endTxnRecords(ControlRecordType.COMMIT, pid2, epoch),
      isFromClient = false, leaderEpoch = 0)
    log.onHighWatermarkIncremented(commitAppendInfo.lastOffset + 1)

    // now there should be no first unstable offset
    assertEquals(None, log.firstUnstableOffset)

    log.close()
  }

  @Test
  def testAbortedTransactionSpanningMultipleSegments() {
    val pid = 137L
    val epoch = 5.toShort
    var seq = 0

    val records = MemoryRecords.withTransactionalRecords(CompressionType.NONE, pid, epoch, seq,
      new SimpleRecord("a".getBytes),
      new SimpleRecord("b".getBytes),
      new SimpleRecord("c".getBytes))

    val log = createLog(messageSizeInBytes = records.sizeInBytes, messagesPerSegment = 1)

    val firstAppendInfo = log.appendAsLeader(records, leaderEpoch = 0)
    assertEquals(Some(firstAppendInfo.firstOffset), log.firstUnstableOffset.map(_.messageOffset))
    assertEquals(Some(0L), log.firstUnstableOffset.map(_.segmentBaseOffset))

    // this write should spill to the second segment
    seq = 3
    log.appendAsLeader(MemoryRecords.withTransactionalRecords(CompressionType.NONE, pid, epoch, seq,
      new SimpleRecord("d".getBytes),
      new SimpleRecord("e".getBytes),
      new SimpleRecord("f".getBytes)), leaderEpoch = 0)
    assertEquals(Some(firstAppendInfo.firstOffset), log.firstUnstableOffset.map(_.messageOffset))
    assertEquals(Some(0L), log.firstUnstableOffset.map(_.segmentBaseOffset))
    assertEquals(3L, log.logEndOffsetMetadata.segmentBaseOffset)

    // now abort the transaction
    val appendInfo = log.appendAsLeader(endTxnRecords(ControlRecordType.ABORT, pid, epoch),
      isFromClient = false, leaderEpoch = 0)
    log.onHighWatermarkIncremented(appendInfo.lastOffset + 1)
    assertEquals(None, log.firstUnstableOffset.map(_.messageOffset))

    // now check that a fetch includes the aborted transaction
    val fetchDataInfo = log.read(0L, 2048, isolationLevel = IsolationLevel.READ_COMMITTED)
    assertEquals(1, fetchDataInfo.abortedTransactions.size)

    assertTrue(fetchDataInfo.abortedTransactions.isDefined)
    assertEquals(new AbortedTransaction(pid, 0), fetchDataInfo.abortedTransactions.get.head)

    log.close()
  }

  private def createLog(messageSizeInBytes: Int, retentionMs: Int = -1, retentionBytes: Int = -1,
                        cleanupPolicy: String = "delete", messagesPerSegment: Int = 5,
                        maxPidExpirationMs: Int = 300000, pidExpirationCheckIntervalMs: Int = 30000,
                        recoveryPoint: Long = 0L, logStartOffset: Long = 0L): Log = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, messageSizeInBytes * messagesPerSegment: Integer)
    logProps.put(LogConfig.RetentionMsProp, retentionMs: Integer)
    logProps.put(LogConfig.RetentionBytesProp, retentionBytes: Integer)
    logProps.put(LogConfig.CleanupPolicyProp, cleanupPolicy)
    logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp, Long.MaxValue.toString)
    val config = LogConfig(logProps)
    Log(logDir,
      config,
      logStartOffset = logStartOffset,
      recoveryPoint = recoveryPoint,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = maxPidExpirationMs,
      producerIdExpirationCheckIntervalMs = pidExpirationCheckIntervalMs)
  }

  private def allAbortedTransactions(log: Log) = log.logSegments.flatMap(_.txnIndex.allAbortedTxns)

  private def appendTransactionalAsLeader(log: Log, producerId: Long, producerEpoch: Short): Int => Unit = {
    var sequence = 0
    numRecords: Int => {
      val simpleRecords = (sequence until sequence + numRecords).map { seq =>
        new SimpleRecord(s"$seq".getBytes)
      }
      val records = MemoryRecords.withTransactionalRecords(CompressionType.NONE, producerId,
        producerEpoch, sequence, simpleRecords: _*)
      log.appendAsLeader(records, leaderEpoch = 0)
      sequence += numRecords
    }
  }

  private def appendEndTxnMarkerAsLeader(log: Log, producerId: Long, producerEpoch: Short,
                                         controlType: ControlRecordType, coordinatorEpoch: Int = 0): Unit = {
    val records = endTxnRecords(controlType, producerId, producerEpoch, coordinatorEpoch = coordinatorEpoch)
    log.appendAsLeader(records, isFromClient = false, leaderEpoch = 0)
  }

  private def appendNonTransactionalAsLeader(log: Log, numRecords: Int): Unit = {
    val simpleRecords = (0 until numRecords).map { seq =>
      new SimpleRecord(s"$seq".getBytes)
    }
    val records = MemoryRecords.withRecords(CompressionType.NONE, simpleRecords: _*)
    log.appendAsLeader(records, leaderEpoch = 0)
  }

  private def appendTransactionalToBuffer(buffer: ByteBuffer, producerId: Long, producerEpoch: Short): (Long, Int) => Unit = {
    var sequence = 0
    (offset: Long, numRecords: Int) => {
      val builder = MemoryRecords.builder(buffer, CompressionType.NONE, offset, producerId, producerEpoch, sequence, true)
      for (seq <- sequence until sequence + numRecords) {
        val record = new SimpleRecord(s"$seq".getBytes)
        builder.append(record)
      }

      sequence += numRecords
      builder.close()
    }
  }

  private def appendEndTxnMarkerToBuffer(buffer: ByteBuffer, producerId: Long, producerEpoch: Short, offset: Long,
                                         controlType: ControlRecordType, coordinatorEpoch: Int = 0): Unit = {
    val marker = new EndTransactionMarker(controlType, coordinatorEpoch)
    MemoryRecords.writeEndTransactionalMarker(buffer, offset, time.milliseconds(), 0, producerId, producerEpoch, marker)
  }

  private def appendNonTransactionalToBuffer(buffer: ByteBuffer, offset: Long, numRecords: Int): Unit = {
    val builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, offset)
    (0 until numRecords).foreach { seq =>
      builder.append(new SimpleRecord(s"$seq".getBytes))
    }
    builder.close()
  }

  private def appendAsFollower(log: Log, records: MemoryRecords, leaderEpoch: Int = 0): Unit = {
    records.batches.asScala.foreach(_.setPartitionLeaderEpoch(leaderEpoch))
    log.appendAsFollower(records)
  }

  private def createCleanShutdownFile(): File = {
    val parentLogDir = logDir.getParentFile
    assertTrue("Data directory %s must exist", parentLogDir.isDirectory)
    val cleanShutdownFile = new File(parentLogDir, Log.CleanShutdownFile)
    cleanShutdownFile.createNewFile()
    assertTrue(".kafka_cleanshutdown must exist", cleanShutdownFile.exists())
    cleanShutdownFile
  }

}
