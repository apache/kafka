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

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.util.Properties

import kafka.api.{ApiVersion, KAFKA_0_11_0_IV0}
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import kafka.server.{BrokerTopicStats, FetchDataInfo, KafkaConfig, LogDirFailureChannel}
import kafka.server.metadata.CachedConfigRepository
import kafka.utils.{CoreUtils, MockTime, Scheduler, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{CompressionType, ControlRecordType, DefaultRecordBatch, MemoryRecords, RecordBatch, RecordVersion, SimpleRecord, TimestampType}
import org.apache.kafka.common.utils.{Time, Utils}
import org.easymock.EasyMock
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.collection.mutable.ListBuffer
import scala.collection.{Iterable, Map, mutable}
import scala.jdk.CollectionConverters._

class LogLoaderTest {
  var config: KafkaConfig = null
  val brokerTopicStats = new BrokerTopicStats
  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val mockTime = new MockTime()

  @BeforeEach
  def setUp(): Unit = {
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
    config = KafkaConfig.fromProps(props)
  }

  @AfterEach
  def tearDown(): Unit = {
    brokerTopicStats.close()
    Utils.delete(tmpDir)
  }

  @Test
  def testLogRecoveryIsCalledUponBrokerCrash(): Unit = {
    // LogManager must realize correctly if the last shutdown was not clean and the logs need
    // to run recovery while loading upon subsequent broker boot up.
    val logDir: File = TestUtils.tempDir()
    val logProps = new Properties()
    val logConfig = LogConfig(logProps)
    val logDirs = Seq(logDir)
    val topicPartition = new TopicPartition("foo", 0)
    var log: Log = null
    val time = new MockTime()
    var cleanShutdownInterceptedValue = false
    case class SimulateError(var hasError: Boolean = false)
    val simulateError = SimulateError()

    // Create a LogManager with some overridden methods to facilitate interception of clean shutdown
    // flag and to inject a runtime error
    def interceptedLogManager(logConfig: LogConfig, logDirs: Seq[File], simulateError: SimulateError): LogManager = {
      new LogManager(logDirs = logDirs.map(_.getAbsoluteFile), initialOfflineDirs = Array.empty[File], new CachedConfigRepository(),
        initialDefaultConfig = logConfig, cleanerConfig = CleanerConfig(enableCleaner = false), recoveryThreadsPerDataDir = 4,
        flushCheckMs = 1000L, flushRecoveryOffsetCheckpointMs = 10000L, flushStartOffsetCheckpointMs = 10000L,
        retentionCheckMs = 1000L, maxPidExpirationMs = 60 * 60 * 1000, scheduler = time.scheduler, time = time,
        brokerTopicStats = new BrokerTopicStats, logDirFailureChannel = new LogDirFailureChannel(logDirs.size), keepPartitionMetadataFile = config.usesTopicId) {

        override def loadLog(logDir: File, hadCleanShutdown: Boolean, recoveryPoints: Map[TopicPartition, Long],
                             logStartOffsets: Map[TopicPartition, Long], topicConfigs: Map[String, LogConfig]): Log = {
          if (simulateError.hasError) {
            throw new RuntimeException("Simulated error")
          }
          cleanShutdownInterceptedValue = hadCleanShutdown
          val topicPartition = Log.parseTopicPartitionName(logDir)
          val config = topicConfigs.getOrElse(topicPartition.topic, currentDefaultConfig)
          val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)
          val logStartOffset = logStartOffsets.getOrElse(topicPartition, 0L)
          val logDirFailureChannel: LogDirFailureChannel = new LogDirFailureChannel(1)
          val maxProducerIdExpirationMs = 60 * 60 * 1000
          val segments = new LogSegments(topicPartition)
          val leaderEpochCache = Log.maybeCreateLeaderEpochCache(logDir, topicPartition, logDirFailureChannel, config.messageFormatVersion.recordVersion)
          val producerStateManager = new ProducerStateManager(topicPartition, logDir, maxProducerIdExpirationMs)
          val loadLogParams = LoadLogParams(logDir, topicPartition, config, time.scheduler, time,
            logDirFailureChannel, hadCleanShutdown, segments, logStartOffset, logRecoveryPoint,
            maxProducerIdExpirationMs, leaderEpochCache, producerStateManager)
          val offsets = LogLoader.load(loadLogParams)
          new Log(logDir, config, segments, offsets.logStartOffset, offsets.recoveryPoint,
            offsets.nextOffsetMetadata, time.scheduler, brokerTopicStats, time, maxPidExpirationMs,
            LogManager.ProducerIdExpirationCheckIntervalMs, topicPartition, leaderEpochCache,
            producerStateManager, logDirFailureChannel, None, true)
        }
      }
    }

    val cleanShutdownFile = new File(logDir, Log.CleanShutdownFile)
    locally {
      val logManager: LogManager = interceptedLogManager(logConfig, logDirs, simulateError)
      log = logManager.getOrCreateLog(topicPartition, isNew = true, topicId = None)

      // Load logs after a clean shutdown
      Files.createFile(cleanShutdownFile.toPath)
      cleanShutdownInterceptedValue = false
      logManager.loadLogs(logManager.fetchTopicConfigOverrides(Set.empty))
      assertTrue(cleanShutdownInterceptedValue, "Unexpected value intercepted for clean shutdown flag")
      assertFalse(cleanShutdownFile.exists(), "Clean shutdown file must not exist after loadLogs has completed")
      // Load logs without clean shutdown file
      cleanShutdownInterceptedValue = true
      logManager.loadLogs(logManager.fetchTopicConfigOverrides(Set.empty))
      assertFalse(cleanShutdownInterceptedValue, "Unexpected value intercepted for clean shutdown flag")
      assertFalse(cleanShutdownFile.exists(), "Clean shutdown file must not exist after loadLogs has completed")
      // Create clean shutdown file and then simulate error while loading logs such that log loading does not complete.
      Files.createFile(cleanShutdownFile.toPath)
      logManager.shutdown()
    }

    locally {
      simulateError.hasError = true
      val logManager: LogManager = interceptedLogManager(logConfig, logDirs, simulateError)
      log = logManager.getOrCreateLog(topicPartition, isNew = true, topicId = None)

      // Simulate error
      assertThrows(classOf[RuntimeException], () => logManager.loadLogs(logManager.fetchTopicConfigOverrides(Set.empty)))
      assertFalse(cleanShutdownFile.exists(), "Clean shutdown file must not have existed")
      // Do not simulate error on next call to LogManager#loadLogs. LogManager must understand that log had unclean shutdown the last time.
      simulateError.hasError = false
      cleanShutdownInterceptedValue = true
      logManager.loadLogs(logManager.fetchTopicConfigOverrides(Set.empty))
      assertFalse(cleanShutdownInterceptedValue, "Unexpected value for clean shutdown flag")
    }
  }

  @Test
  def testProducerSnapshotsRecoveryAfterUncleanShutdownV1(): Unit = {
    testProducerSnapshotsRecoveryAfterUncleanShutdown(ApiVersion.minSupportedFor(RecordVersion.V1).version)
  }

  @Test
  def testProducerSnapshotsRecoveryAfterUncleanShutdownCurrentMessageFormat(): Unit = {
    testProducerSnapshotsRecoveryAfterUncleanShutdown(ApiVersion.latestVersion.version)
  }

  private def createLog(dir: File,
                        config: LogConfig,
                        brokerTopicStats: BrokerTopicStats = brokerTopicStats,
                        logStartOffset: Long = 0L,
                        recoveryPoint: Long = 0L,
                        scheduler: Scheduler = mockTime.scheduler,
                        time: Time = mockTime,
                        maxProducerIdExpirationMs: Int = 60 * 60 * 1000,
                        producerIdExpirationCheckIntervalMs: Int = LogManager.ProducerIdExpirationCheckIntervalMs,
                        lastShutdownClean: Boolean = true): Log = {
    LogTestUtils.createLog(dir, config, brokerTopicStats, scheduler, time, logStartOffset, recoveryPoint,
      maxProducerIdExpirationMs, producerIdExpirationCheckIntervalMs, lastShutdownClean)
  }

  private def createLogWithOffsetOverflow(logConfig: LogConfig): (Log, LogSegment) = {
    LogTestUtils.initializeLogDirWithOverflowedSegment(logDir)

    val log = createLog(logDir, logConfig, recoveryPoint = Long.MaxValue)
    val segmentWithOverflow = LogTestUtils.firstOverflowSegment(log).getOrElse {
      throw new AssertionError("Failed to create log with a segment which has overflowed offsets")
    }

    (log, segmentWithOverflow)
  }

  private def recoverAndCheck(config: LogConfig, expectedKeys: Iterable[Long]): Log = {
    // method is called only in case of recovery from hard reset
    LogTestUtils.recoverAndCheck(logDir, config, expectedKeys, brokerTopicStats, mockTime, mockTime.scheduler)
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
      mockTime.milliseconds, leaderEpoch)
    records.foreach(builder.append)
    builder.build()
  }

  private def testProducerSnapshotsRecoveryAfterUncleanShutdown(messageFormatVersion: String): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 64 * 10, messageFormatVersion = messageFormatVersion)
    var log = createLog(logDir, logConfig)
    assertEquals(None, log.oldestProducerSnapshotOffset)

    for (i <- 0 to 100) {
      val record = new SimpleRecord(mockTime.milliseconds, i.toString.getBytes)
      log.appendAsLeader(TestUtils.records(List(record)), leaderEpoch = 0)
    }

    assertTrue(log.logSegments.size >= 5)
    val segmentOffsets = log.logSegments.toVector.map(_.baseOffset)
    val activeSegmentOffset = segmentOffsets.last

    // We want the recovery point to be past the segment offset and before the last 2 segments including a gap of
    // 1 segment. We collect the data before closing the log.
    val offsetForSegmentAfterRecoveryPoint = segmentOffsets(segmentOffsets.size - 3)
    val offsetForRecoveryPointSegment = segmentOffsets(segmentOffsets.size - 4)
    val (segOffsetsBeforeRecovery, segOffsetsAfterRecovery) = segmentOffsets.toSet.partition(_ < offsetForRecoveryPointSegment)
    val recoveryPoint = offsetForRecoveryPointSegment + 1
    assertTrue(recoveryPoint < offsetForSegmentAfterRecoveryPoint)
    log.close()

    val segmentsWithReads = mutable.Set[LogSegment]()
    val recoveredSegments = mutable.Set[LogSegment]()
    val expectedSegmentsWithReads = mutable.Set[Long]()
    val expectedSnapshotOffsets = mutable.Set[Long]()

    if (logConfig.messageFormatVersion < KAFKA_0_11_0_IV0) {
      expectedSegmentsWithReads += activeSegmentOffset
      expectedSnapshotOffsets ++= log.logSegments.map(_.baseOffset).toVector.takeRight(2) :+ log.logEndOffset
    } else {
      expectedSegmentsWithReads ++= segOffsetsBeforeRecovery ++ Set(activeSegmentOffset)
      expectedSnapshotOffsets ++= log.logSegments.map(_.baseOffset).toVector.takeRight(4) :+ log.logEndOffset
    }

    def createLogWithInterceptedReads(recoveryPoint: Long) = {
      val maxProducerIdExpirationMs = 60 * 60 * 1000
      val topicPartition = Log.parseTopicPartitionName(logDir)
      val logDirFailureChannel = new LogDirFailureChannel(10)
      // Intercept all segment read calls
      val interceptedLogSegments = new LogSegments(topicPartition) {
        override def add(segment: LogSegment): LogSegment = {
          val wrapper = new LogSegment(segment.log, segment.lazyOffsetIndex, segment.lazyTimeIndex, segment.txnIndex, segment.baseOffset,
            segment.indexIntervalBytes, segment.rollJitterMs, mockTime) {

            override def read(startOffset: Long, maxSize: Int, maxPosition: Long, minOneMessage: Boolean): FetchDataInfo = {
              segmentsWithReads += this
              super.read(startOffset, maxSize, maxPosition, minOneMessage)
            }

            override def recover(producerStateManager: ProducerStateManager,
                                 leaderEpochCache: Option[LeaderEpochFileCache]): Int = {
              recoveredSegments += this
              super.recover(producerStateManager, leaderEpochCache)
            }
          }
          super.add(wrapper)
        }
      }
      val leaderEpochCache = Log.maybeCreateLeaderEpochCache(logDir, topicPartition, logDirFailureChannel, logConfig.messageFormatVersion.recordVersion)
      val producerStateManager = new ProducerStateManager(topicPartition, logDir, maxProducerIdExpirationMs)
      val loadLogParams = LoadLogParams(
        logDir,
        topicPartition,
        logConfig,
        mockTime.scheduler,
        mockTime,
        logDirFailureChannel,
        hadCleanShutdown = false,
        interceptedLogSegments,
        0L,
        recoveryPoint,
        maxProducerIdExpirationMs,
        leaderEpochCache,
        producerStateManager)
      val offsets = LogLoader.load(loadLogParams)
      new Log(logDir, logConfig, interceptedLogSegments, offsets.logStartOffset, offsets.recoveryPoint,
        offsets.nextOffsetMetadata, mockTime.scheduler, brokerTopicStats, mockTime,
        maxProducerIdExpirationMs, LogManager.ProducerIdExpirationCheckIntervalMs, topicPartition,
        leaderEpochCache, producerStateManager, logDirFailureChannel, topicId = None,
        keepPartitionMetadataFile = true)
    }

    // Retain snapshots for the last 2 segments
    log.producerStateManager.deleteSnapshotsBefore(segmentOffsets(segmentOffsets.size - 2))
    log = createLogWithInterceptedReads(offsetForRecoveryPointSegment)
    // We will reload all segments because the recovery point is behind the producer snapshot files (pre KAFKA-5829 behaviour)
    assertEquals(expectedSegmentsWithReads, segmentsWithReads.map(_.baseOffset))
    assertEquals(segOffsetsAfterRecovery, recoveredSegments.map(_.baseOffset))
    assertEquals(expectedSnapshotOffsets, LogTestUtils.listProducerSnapshotOffsets(logDir).toSet)
    log.close()
    segmentsWithReads.clear()
    recoveredSegments.clear()

    // Only delete snapshots before the base offset of the recovery point segment (post KAFKA-5829 behaviour) to
    // avoid reading all segments
    log.producerStateManager.deleteSnapshotsBefore(offsetForRecoveryPointSegment)
    log = createLogWithInterceptedReads(recoveryPoint = recoveryPoint)
    assertEquals(Set(activeSegmentOffset), segmentsWithReads.map(_.baseOffset))
    assertEquals(segOffsetsAfterRecovery, recoveredSegments.map(_.baseOffset))
    assertEquals(expectedSnapshotOffsets, LogTestUtils.listProducerSnapshotOffsets(logDir).toSet)

    log.close()
  }

  @Test
  def testSkipLoadingIfEmptyProducerStateBeforeTruncation(): Unit = {
    val stateManager: ProducerStateManager = EasyMock.mock(classOf[ProducerStateManager])
    EasyMock.expect(stateManager.removeStraySnapshots(EasyMock.anyObject())).anyTimes()
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

    val topicPartition = Log.parseTopicPartitionName(logDir)
    val logDirFailureChannel: LogDirFailureChannel = new LogDirFailureChannel(1)
    val config = LogConfig(new Properties())
    val maxProducerIdExpirationMs = 300000
    val segments = new LogSegments(topicPartition)
    val leaderEpochCache = Log.maybeCreateLeaderEpochCache(logDir, topicPartition, logDirFailureChannel, config.messageFormatVersion.recordVersion)
    val offsets = LogLoader.load(LoadLogParams(
      logDir,
      topicPartition,
      config,
      mockTime.scheduler,
      mockTime,
      logDirFailureChannel,
      hadCleanShutdown = false,
      segments,
      0L,
      0L,
      maxProducerIdExpirationMs,
      leaderEpochCache,
      stateManager))
    val log = new Log(logDir,
      config,
      segments = segments,
      logStartOffset = offsets.logStartOffset,
      recoveryPoint = offsets.recoveryPoint,
      nextOffsetMetadata = offsets.nextOffsetMetadata,
      scheduler = mockTime.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = mockTime,
      maxProducerIdExpirationMs = maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs = 30000,
      topicPartition = topicPartition,
      leaderEpochCache = leaderEpochCache,
      producerStateManager = stateManager,
      logDirFailureChannel = logDirFailureChannel,
      topicId = None,
      keepPartitionMetadataFile = true)

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
    EasyMock.expect(stateManager.updateMapEndOffset(1L))
    EasyMock.expect(stateManager.onHighWatermarkUpdated(0L))

    // Finally, we take a snapshot
    stateManager.takeSnapshot()
    EasyMock.expectLastCall().once()

    EasyMock.replay(stateManager)

    log.truncateTo(1L)

    EasyMock.verify(stateManager)
  }

  @Test
  def testRecoverAfterNonMonotonicCoordinatorEpochWrite(): Unit = {
    // Due to KAFKA-9144, we may encounter a coordinator epoch which goes backwards.
    // This test case verifies that recovery logic relaxes validation in this case and
    // just takes the latest write.

    val producerId = 1L
    val coordinatorEpoch = 5
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    var log = createLog(logDir, logConfig)
    val epoch = 0.toShort

    val firstAppendTimestamp = mockTime.milliseconds()
    LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId, epoch, ControlRecordType.ABORT,
      firstAppendTimestamp, coordinatorEpoch = coordinatorEpoch)
    assertEquals(firstAppendTimestamp, log.producerStateManager.lastEntry(producerId).get.lastTimestamp)

    mockTime.sleep(log.maxProducerIdExpirationMs)
    assertEquals(None, log.producerStateManager.lastEntry(producerId))

    val secondAppendTimestamp = mockTime.milliseconds()
    LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId, epoch, ControlRecordType.ABORT,
      secondAppendTimestamp, coordinatorEpoch = coordinatorEpoch - 1)

    log.close()

    // Force recovery by setting the recoveryPoint to the log start
    log = createLog(logDir, logConfig, recoveryPoint = 0L, lastShutdownClean = false)
    assertEquals(secondAppendTimestamp, log.producerStateManager.lastEntry(producerId).get.lastTimestamp)
    log.close()
  }

  @Test
  def testSkipTruncateAndReloadIfOldMessageFormatAndNoCleanShutdown(): Unit = {
    val stateManager: ProducerStateManager = EasyMock.mock(classOf[ProducerStateManager])
    EasyMock.expect(stateManager.removeStraySnapshots(EasyMock.anyObject())).anyTimes()

    stateManager.updateMapEndOffset(0L)
    EasyMock.expectLastCall().anyTimes()

    stateManager.takeSnapshot()
    EasyMock.expectLastCall().anyTimes()

    EasyMock.expect(stateManager.isEmpty).andReturn(true)
    EasyMock.expectLastCall().once()

    EasyMock.expect(stateManager.firstUnstableOffset).andReturn(None)
    EasyMock.expectLastCall().once()

    EasyMock.replay(stateManager)

    val topicPartition = Log.parseTopicPartitionName(logDir)
    val logProps = new Properties()
    logProps.put(LogConfig.MessageFormatVersionProp, "0.10.2")
    val config = LogConfig(logProps)
    val maxProducerIdExpirationMs = 300000
    val logDirFailureChannel = null
    val segments = new LogSegments(topicPartition)
    val leaderEpochCache = Log.maybeCreateLeaderEpochCache(logDir, topicPartition, logDirFailureChannel, config.messageFormatVersion.recordVersion)
    val offsets = LogLoader.load(LoadLogParams(
      logDir,
      topicPartition,
      config,
      mockTime.scheduler,
      mockTime,
      logDirFailureChannel,
      hadCleanShutdown = false,
      segments,
      0L,
      0L,
      maxProducerIdExpirationMs,
      leaderEpochCache,
      stateManager))
    new Log(logDir,
      config,
      segments = segments,
      logStartOffset = offsets.logStartOffset,
      recoveryPoint = offsets.recoveryPoint,
      nextOffsetMetadata = offsets.nextOffsetMetadata,
      scheduler = mockTime.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = mockTime,
      maxProducerIdExpirationMs = maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs = 30000,
      topicPartition = topicPartition,
      leaderEpochCache = leaderEpochCache,
      producerStateManager = stateManager,
      logDirFailureChannel = logDirFailureChannel,
      topicId = None,
      keepPartitionMetadataFile = true)

    EasyMock.verify(stateManager)
  }

  @Test
  def testSkipTruncateAndReloadIfOldMessageFormatAndCleanShutdown(): Unit = {
    val stateManager: ProducerStateManager = EasyMock.mock(classOf[ProducerStateManager])
    EasyMock.expect(stateManager.removeStraySnapshots(EasyMock.anyObject())).anyTimes()

    stateManager.updateMapEndOffset(0L)
    EasyMock.expectLastCall().anyTimes()

    stateManager.takeSnapshot()
    EasyMock.expectLastCall().anyTimes()

    EasyMock.expect(stateManager.isEmpty).andReturn(true)
    EasyMock.expectLastCall().once()

    EasyMock.expect(stateManager.firstUnstableOffset).andReturn(None)
    EasyMock.expectLastCall().once()

    EasyMock.replay(stateManager)

    val topicPartition = Log.parseTopicPartitionName(logDir)
    val logProps = new Properties()
    logProps.put(LogConfig.MessageFormatVersionProp, "0.10.2")
    val config = LogConfig(logProps)
    val maxProducerIdExpirationMs = 300000
    val logDirFailureChannel = null
    val segments = new LogSegments(topicPartition)
    val leaderEpochCache = Log.maybeCreateLeaderEpochCache(logDir, topicPartition, logDirFailureChannel, config.messageFormatVersion.recordVersion)
    val offsets = LogLoader.load(LoadLogParams(
      logDir,
      topicPartition,
      config,
      mockTime.scheduler,
      mockTime,
      logDirFailureChannel,
      hadCleanShutdown = true,
      segments,
      0L,
      0L,
      maxProducerIdExpirationMs,
      leaderEpochCache,
      stateManager))
    new Log(logDir,
      config,
      segments = segments,
      logStartOffset = offsets.logStartOffset,
      recoveryPoint = offsets.recoveryPoint,
      nextOffsetMetadata = offsets.nextOffsetMetadata,
      scheduler = mockTime.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = mockTime,
      maxProducerIdExpirationMs = maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs = 30000,
      topicPartition = topicPartition,
      leaderEpochCache = leaderEpochCache,
      producerStateManager = stateManager,
      logDirFailureChannel = logDirFailureChannel,
      topicId = None,
      keepPartitionMetadataFile = true)

    EasyMock.verify(stateManager)
  }

  @Test
  def testSkipTruncateAndReloadIfNewMessageFormatAndCleanShutdown(): Unit = {
    val stateManager: ProducerStateManager = EasyMock.mock(classOf[ProducerStateManager])
    EasyMock.expect(stateManager.removeStraySnapshots(EasyMock.anyObject())).anyTimes()

    EasyMock.expect(stateManager.latestSnapshotOffset).andReturn(None)

    stateManager.updateMapEndOffset(0L)
    EasyMock.expectLastCall().anyTimes()

    stateManager.takeSnapshot()
    EasyMock.expectLastCall().anyTimes()

    EasyMock.expect(stateManager.isEmpty).andReturn(true)
    EasyMock.expectLastCall().once()

    EasyMock.expect(stateManager.firstUnstableOffset).andReturn(None)
    EasyMock.expectLastCall().once()

    EasyMock.replay(stateManager)

    val topicPartition = Log.parseTopicPartitionName(logDir)
    val logProps = new Properties()
    logProps.put(LogConfig.MessageFormatVersionProp, "0.11.0")
    val config = LogConfig(logProps)
    val maxProducerIdExpirationMs = 300000
    val logDirFailureChannel = null
    val segments = new LogSegments(topicPartition)
    val leaderEpochCache = Log.maybeCreateLeaderEpochCache(logDir, topicPartition, logDirFailureChannel, config.messageFormatVersion.recordVersion)
    val offsets = LogLoader.load(LoadLogParams(
      logDir,
      topicPartition,
      config,
      mockTime.scheduler,
      mockTime,
      logDirFailureChannel,
      hadCleanShutdown = true,
      segments,
      0L,
      0L,
      maxProducerIdExpirationMs,
      leaderEpochCache,
      stateManager))
    new Log(logDir,
      config,
      segments = segments,
      logStartOffset = offsets.logStartOffset,
      recoveryPoint = offsets.recoveryPoint,
      nextOffsetMetadata = offsets.nextOffsetMetadata,
      scheduler = mockTime.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = mockTime,
      maxProducerIdExpirationMs = maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs = 30000,
      topicPartition = topicPartition,
      leaderEpochCache = leaderEpochCache,
      producerStateManager = stateManager,
      logDirFailureChannel = logDirFailureChannel,
      topicId = None,
      keepPartitionMetadataFile = true)

    EasyMock.verify(stateManager)
  }

  @Test
  def testLoadProducersAfterDeleteRecordsMidSegment(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    val pid1 = 1L
    val pid2 = 2L
    val epoch = 0.toShort

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "a".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "b".getBytes)), producerId = pid2,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    assertEquals(2, log.activeProducersWithLastSequence.size)

    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(1L, ClientRecordDeletion)

    // Deleting records should not remove producer state
    assertEquals(2, log.activeProducersWithLastSequence.size)
    val retainedLastSeqOpt = log.activeProducersWithLastSequence.get(pid2)
    assertTrue(retainedLastSeqOpt.isDefined)
    assertEquals(0, retainedLastSeqOpt.get)

    log.close()

    // Because the log start offset did not advance, producer snapshots will still be present and the state will be rebuilt
    val reloadedLog = createLog(logDir, logConfig, logStartOffset = 1L, lastShutdownClean = false)
    assertEquals(2, reloadedLog.activeProducersWithLastSequence.size)
    val reloadedLastSeqOpt = log.activeProducersWithLastSequence.get(pid2)
    assertEquals(retainedLastSeqOpt, reloadedLastSeqOpt)
  }

  @Test
  def testLoadingLogKeepsLargestStrayProducerStateSnapshot(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5, retentionBytes = 0, retentionMs = 1000 * 60, fileDeleteDelayMs = 0)
    val log = createLog(logDir, logConfig)
    val pid1 = 1L
    val epoch = 0.toShort

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes)), producerId = pid1, producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes)), producerId = pid1, producerEpoch = epoch, sequence = 1), leaderEpoch = 0)
    log.roll()

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("c".getBytes)), producerId = pid1, producerEpoch = epoch, sequence = 2), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("d".getBytes)), producerId = pid1, producerEpoch = epoch, sequence = 3), leaderEpoch = 0)

    // Close the log, we should now have 3 segments
    log.close()
    assertEquals(log.logSegments.size, 3)
    // We expect 3 snapshot files, two of which are for the first two segments, the last was written out during log closing.
    assertEquals(Seq(1, 2, 4), ProducerStateManager.listSnapshotFiles(logDir).map(_.offset).sorted)
    // Inject a stray snapshot file within the bounds of the log at offset 3, it should be cleaned up after loading the log
    val straySnapshotFile = Log.producerSnapshotFile(logDir, 3).toPath
    Files.createFile(straySnapshotFile)
    assertEquals(Seq(1, 2, 3, 4), ProducerStateManager.listSnapshotFiles(logDir).map(_.offset).sorted)

    createLog(logDir, logConfig, lastShutdownClean = false)
    // We should clean up the stray producer state snapshot file, but keep the largest snapshot file (4)
    assertEquals(Seq(1, 2, 4), ProducerStateManager.listSnapshotFiles(logDir).map(_.offset).sorted)
  }

  @Test
  def testLoadProducersAfterDeleteRecordsOnSegment(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    val pid1 = 1L
    val pid2 = 2L
    val epoch = 0.toShort

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "a".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "b".getBytes)), producerId = pid2,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)

    assertEquals(2, log.logSegments.size)
    assertEquals(2, log.activeProducersWithLastSequence.size)

    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(1L, ClientRecordDeletion)
    log.deleteOldSegments()

    // Deleting records should not remove producer state
    assertEquals(1, log.logSegments.size)
    assertEquals(2, log.activeProducersWithLastSequence.size)
    val retainedLastSeqOpt = log.activeProducersWithLastSequence.get(pid2)
    assertTrue(retainedLastSeqOpt.isDefined)
    assertEquals(0, retainedLastSeqOpt.get)

    log.close()

    // After reloading log, producer state should not be regenerated
    val reloadedLog = createLog(logDir, logConfig, logStartOffset = 1L, lastShutdownClean = false)
    assertEquals(1, reloadedLog.activeProducersWithLastSequence.size)
    val reloadedEntryOpt = log.activeProducersWithLastSequence.get(pid2)
    assertEquals(retainedLastSeqOpt, reloadedEntryOpt)
  }

  /**
   * Append a bunch of messages to a log and then re-open it both with and without recovery and check that the log re-initializes correctly.
   */
  @Test
  def testLogRecoversToCorrectOffset(): Unit = {
    val numMessages = 100
    val messageSize = 100
    val segmentSize = 7 * messageSize
    val indexInterval = 3 * messageSize
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = segmentSize, indexIntervalBytes = indexInterval, segmentIndexBytes = 4096)
    var log = createLog(logDir, logConfig)
    for(i <- 0 until numMessages)
      log.appendAsLeader(TestUtils.singletonRecords(value = TestUtils.randomBytes(messageSize),
        timestamp = mockTime.milliseconds + i * 10), leaderEpoch = 0)
    assertEquals(numMessages, log.logEndOffset,
      "After appending %d messages to an empty log, the log end offset should be %d".format(numMessages, numMessages))
    val lastIndexOffset = log.activeSegment.offsetIndex.lastOffset
    val numIndexEntries = log.activeSegment.offsetIndex.entries
    val lastOffset = log.logEndOffset
    // After segment is closed, the last entry in the time index should be (largest timestamp -> last offset).
    val lastTimeIndexOffset = log.logEndOffset - 1
    val lastTimeIndexTimestamp  = log.activeSegment.largestTimestamp
    // Depending on when the last time index entry is inserted, an entry may or may not be inserted into the time index.
    val numTimeIndexEntries = log.activeSegment.timeIndex.entries + {
      if (log.activeSegment.timeIndex.lastEntry.offset == log.logEndOffset - 1) 0 else 1
    }
    log.close()

    def verifyRecoveredLog(log: Log, expectedRecoveryPoint: Long): Unit = {
      assertEquals(expectedRecoveryPoint, log.recoveryPoint, s"Unexpected recovery point")
      assertEquals(numMessages, log.logEndOffset, s"Should have $numMessages messages when log is reopened w/o recovery")
      assertEquals(lastIndexOffset, log.activeSegment.offsetIndex.lastOffset, "Should have same last index offset as before.")
      assertEquals(numIndexEntries, log.activeSegment.offsetIndex.entries, "Should have same number of index entries as before.")
      assertEquals(lastTimeIndexTimestamp, log.activeSegment.timeIndex.lastEntry.timestamp, "Should have same last time index timestamp")
      assertEquals(lastTimeIndexOffset, log.activeSegment.timeIndex.lastEntry.offset, "Should have same last time index offset")
      assertEquals(numTimeIndexEntries, log.activeSegment.timeIndex.entries, "Should have same number of time index entries as before.")
    }

    log = createLog(logDir, logConfig, recoveryPoint = lastOffset, lastShutdownClean = false)
    verifyRecoveredLog(log, lastOffset)
    log.close()

    // test recovery case
    val recoveryPoint = 10
    log = createLog(logDir, logConfig, recoveryPoint = recoveryPoint, lastShutdownClean = false)
    // the recovery point should not be updated after unclean shutdown until the log is flushed
    verifyRecoveredLog(log, recoveryPoint)
    log.flush()
    verifyRecoveredLog(log, lastOffset)
    log.close()
  }

  /**
   * Test that if we manually delete an index segment it is rebuilt when the log is re-opened
   */
  @Test
  def testIndexRebuild(): Unit = {
    // publish the messages and close the log
    val numMessages = 200
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1)
    var log = createLog(logDir, logConfig)
    for(i <- 0 until numMessages)
      log.appendAsLeader(TestUtils.singletonRecords(value = TestUtils.randomBytes(10), timestamp = mockTime.milliseconds + i * 10), leaderEpoch = 0)
    val indexFiles = log.logSegments.map(_.lazyOffsetIndex.file)
    val timeIndexFiles = log.logSegments.map(_.lazyTimeIndex.file)
    log.close()

    // delete all the index files
    indexFiles.foreach(_.delete())
    timeIndexFiles.foreach(_.delete())

    // reopen the log
    log = createLog(logDir, logConfig, lastShutdownClean = false)
    assertEquals(numMessages, log.logEndOffset, "Should have %d messages when log is reopened".format(numMessages))
    assertTrue(log.logSegments.head.offsetIndex.entries > 0, "The index should have been rebuilt")
    assertTrue(log.logSegments.head.timeIndex.entries > 0, "The time index should have been rebuilt")
    for(i <- 0 until numMessages) {
      assertEquals(i, LogTestUtils.readLog(log, i, 100).records.batches.iterator.next().lastOffset)
      if (i == 0)
        assertEquals(log.logSegments.head.baseOffset, log.fetchOffsetByTimestamp(mockTime.milliseconds + i * 10).get.offset)
      else
        assertEquals(i, log.fetchOffsetByTimestamp(mockTime.milliseconds + i * 10).get.offset)
    }
    log.close()
  }

  /**
   * Test that if messages format version of the messages in a segment is before 0.10.0, the time index should be empty.
   */
  @Test
  def testRebuildTimeIndexForOldMessages(): Unit = {
    val numMessages = 200
    val segmentSize = 200
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = segmentSize, indexIntervalBytes = 1, messageFormatVersion = "0.9.0")
    var log = createLog(logDir, logConfig)
    for (i <- 0 until numMessages)
      log.appendAsLeader(TestUtils.singletonRecords(value = TestUtils.randomBytes(10),
        timestamp = mockTime.milliseconds + i * 10, magicValue = RecordBatch.MAGIC_VALUE_V1), leaderEpoch = 0)
    val timeIndexFiles = log.logSegments.map(_.lazyTimeIndex.file)
    log.close()

    // Delete the time index.
    timeIndexFiles.foreach(file => Files.delete(file.toPath))

    // The rebuilt time index should be empty
    log = createLog(logDir, logConfig, recoveryPoint = numMessages + 1, lastShutdownClean = false)
    for (segment <- log.logSegments.init) {
      assertEquals(0, segment.timeIndex.entries, "The time index should be empty")
      assertEquals(0, segment.lazyTimeIndex.file.length, "The time index file size should be 0")
    }
  }


  /**
   * Test that if we have corrupted an index segment it is rebuilt when the log is re-opened
   */
  @Test
  def testCorruptIndexRebuild(): Unit = {
    // publish the messages and close the log
    val numMessages = 200
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1)
    var log = createLog(logDir, logConfig)
    for(i <- 0 until numMessages)
      log.appendAsLeader(TestUtils.singletonRecords(value = TestUtils.randomBytes(10), timestamp = mockTime.milliseconds + i * 10), leaderEpoch = 0)
    val indexFiles = log.logSegments.map(_.lazyOffsetIndex.file)
    val timeIndexFiles = log.logSegments.map(_.lazyTimeIndex.file)
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

    // reopen the log with recovery point=0 so that the segment recovery can be triggered
    log = createLog(logDir, logConfig, lastShutdownClean = false)
    assertEquals(numMessages, log.logEndOffset, "Should have %d messages when log is reopened".format(numMessages))
    for(i <- 0 until numMessages) {
      assertEquals(i, LogTestUtils.readLog(log, i, 100).records.batches.iterator.next().lastOffset)
      if (i == 0)
        assertEquals(log.logSegments.head.baseOffset, log.fetchOffsetByTimestamp(mockTime.milliseconds + i * 10).get.offset)
      else
        assertEquals(i, log.fetchOffsetByTimestamp(mockTime.milliseconds + i * 10).get.offset)
    }
    log.close()
  }

  /**
   * When we open a log any index segments without an associated log segment should be deleted.
   */
  @Test
  def testBogusIndexSegmentsAreRemoved(): Unit = {
    val bogusIndex1 = Log.offsetIndexFile(logDir, 0)
    val bogusTimeIndex1 = Log.timeIndexFile(logDir, 0)
    val bogusIndex2 = Log.offsetIndexFile(logDir, 5)
    val bogusTimeIndex2 = Log.timeIndexFile(logDir, 5)

    // The files remain absent until we first access it because we are doing lazy loading for time index and offset index
    // files but in this test case we need to create these files in order to test we will remove them.
    bogusIndex2.createNewFile()
    bogusTimeIndex2.createNewFile()

    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, segmentIndexBytes = 1000, indexIntervalBytes = 1)
    val log = createLog(logDir, logConfig)

    // Force the segment to access the index files because we are doing index lazy loading.
    log.logSegments.toSeq.head.offsetIndex
    log.logSegments.toSeq.head.timeIndex

    assertTrue(bogusIndex1.length > 0,
      "The first index file should have been replaced with a larger file")
    assertTrue(bogusTimeIndex1.length > 0,
      "The first time index file should have been replaced with a larger file")
    assertFalse(bogusIndex2.exists,
      "The second index file should have been deleted.")
    assertFalse(bogusTimeIndex2.exists,
      "The second time index file should have been deleted.")

    // check that we can append to the log
    for (_ <- 0 until 10)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    log.delete()
  }

  /**
   * Verify that truncation works correctly after re-opening the log
   */
  @Test
  def testReopenThenTruncate(): Unit = {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    // create a log
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, segmentIndexBytes = 1000, indexIntervalBytes = 10000)
    var log = createLog(logDir, logConfig)

    // add enough messages to roll over several segments then close and re-open and attempt to truncate
    for (_ <- 0 until 100)
      log.appendAsLeader(createRecords, leaderEpoch = 0)
    log.close()
    log = createLog(logDir, logConfig, lastShutdownClean = false)
    log.truncateTo(3)
    assertEquals(1, log.numberOfSegments, "All but one segment should be deleted.")
    assertEquals(3, log.logEndOffset, "Log end offset should be 3.")
  }

  /**
   * Any files ending in .deleted should be removed when the log is re-opened.
   */
  @Test
  def testOpenDeletesObsoleteFiles(): Unit = {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds - 1000)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, segmentIndexBytes = 1000, retentionMs = 999)
    var log = createLog(logDir, logConfig)

    // append some messages to create some segments
    for (_ <- 0 until 100)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    // expire all segments
    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()
    log.close()
    log = createLog(logDir, logConfig, lastShutdownClean = false)
    assertEquals(1, log.numberOfSegments, "The deleted segments should be gone.")
  }

  @Test
  def testCorruptLog(): Unit = {
    // append some messages to create some segments
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    val recoveryPoint = 50L
    for (_ <- 0 until 10) {
      // create a log and write some messages to it
      logDir.mkdirs()
      var log = createLog(logDir, logConfig)
      val numMessages = 50 + TestUtils.random.nextInt(50)
      for (_ <- 0 until numMessages)
        log.appendAsLeader(createRecords, leaderEpoch = 0)
      val records = log.logSegments.flatMap(_.log.records.asScala.toList).toList
      log.close()

      // corrupt index and log by appending random bytes
      TestUtils.appendNonsenseToFile(log.activeSegment.lazyOffsetIndex.file, TestUtils.random.nextInt(1024) + 1)
      TestUtils.appendNonsenseToFile(log.activeSegment.log.file, TestUtils.random.nextInt(1024) + 1)

      // attempt recovery
      log = createLog(logDir, logConfig, brokerTopicStats, 0L, recoveryPoint, lastShutdownClean = false)
      assertEquals(numMessages, log.logEndOffset)

      val recovered = log.logSegments.flatMap(_.log.records.asScala.toList).toList
      assertEquals(records.size, recovered.size)

      for (i <- records.indices) {
        val expected = records(i)
        val actual = recovered(i)
        assertEquals(expected.key, actual.key, s"Keys not equal")
        assertEquals(expected.value, actual.value, s"Values not equal")
        assertEquals(expected.timestamp, actual.timestamp, s"Timestamps not equal")
      }

      Utils.delete(logDir)
    }
  }

  @Test
  def testOverCompactedLogRecovery(): Unit = {
    // append some messages to create some segments
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    val log = createLog(logDir, logConfig)
    val set1 = MemoryRecords.withRecords(0, CompressionType.NONE, 0, new SimpleRecord("v1".getBytes(), "k1".getBytes()))
    val set2 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 2, CompressionType.NONE, 0, new SimpleRecord("v3".getBytes(), "k3".getBytes()))
    val set3 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 3, CompressionType.NONE, 0, new SimpleRecord("v4".getBytes(), "k4".getBytes()))
    val set4 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 4, CompressionType.NONE, 0, new SimpleRecord("v5".getBytes(), "k5".getBytes()))
    //Writes into an empty log with baseOffset 0
    log.appendAsFollower(set1)
    assertEquals(0L, log.activeSegment.baseOffset)
    //This write will roll the segment, yielding a new segment with base offset = max(1, Integer.MAX_VALUE+2) = Integer.MAX_VALUE+2
    log.appendAsFollower(set2)
    assertEquals(Integer.MAX_VALUE.toLong + 2, log.activeSegment.baseOffset)
    assertTrue(Log.producerSnapshotFile(logDir, Integer.MAX_VALUE.toLong + 2).exists)
    //This will go into the existing log
    log.appendAsFollower(set3)
    assertEquals(Integer.MAX_VALUE.toLong + 2, log.activeSegment.baseOffset)
    //This will go into the existing log
    log.appendAsFollower(set4)
    assertEquals(Integer.MAX_VALUE.toLong + 2, log.activeSegment.baseOffset)
    log.close()
    val indexFiles = logDir.listFiles.filter(file => file.getName.contains(".index"))
    assertEquals(2, indexFiles.length)
    for (file <- indexFiles) {
      val offsetIndex = new OffsetIndex(file, file.getName.replace(".index","").toLong)
      assertTrue(offsetIndex.lastOffset >= 0)
      offsetIndex.close()
    }
    Utils.delete(logDir)
  }

  @Test
  def testLeaderEpochCacheClearedAfterStaticMessageFormatDowngrade(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    val log = createLog(logDir, logConfig)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("foo".getBytes()))), leaderEpoch = 5)
    assertEquals(Some(5), log.latestEpoch)
    log.close()

    // reopen the log with an older message format version and check the cache
    val downgradedLogConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1,
      maxMessageBytes = 64 * 1024, messageFormatVersion = kafka.api.KAFKA_0_10_2_IV0.shortVersion)
    val reopened = createLog(logDir, downgradedLogConfig, lastShutdownClean = false)
    LogTestUtils.assertLeaderEpochCacheEmpty(reopened)

    reopened.appendAsLeader(TestUtils.records(List(new SimpleRecord("bar".getBytes())),
      magicValue = RecordVersion.V1.value), leaderEpoch = 5)
    LogTestUtils.assertLeaderEpochCacheEmpty(reopened)
  }

  @Test
  def testOverCompactedLogRecoveryMultiRecord(): Unit = {
    // append some messages to create some segments
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    val log = createLog(logDir, logConfig)
    val set1 = MemoryRecords.withRecords(0, CompressionType.NONE, 0, new SimpleRecord("v1".getBytes(), "k1".getBytes()))
    val set2 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 2, CompressionType.GZIP, 0,
      new SimpleRecord("v3".getBytes(), "k3".getBytes()),
      new SimpleRecord("v4".getBytes(), "k4".getBytes()))
    val set3 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 4, CompressionType.GZIP, 0,
      new SimpleRecord("v5".getBytes(), "k5".getBytes()),
      new SimpleRecord("v6".getBytes(), "k6".getBytes()))
    val set4 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 6, CompressionType.GZIP, 0,
      new SimpleRecord("v7".getBytes(), "k7".getBytes()),
      new SimpleRecord("v8".getBytes(), "k8".getBytes()))
    //Writes into an empty log with baseOffset 0
    log.appendAsFollower(set1)
    assertEquals(0L, log.activeSegment.baseOffset)
    //This write will roll the segment, yielding a new segment with base offset = max(1, Integer.MAX_VALUE+2) = Integer.MAX_VALUE+2
    log.appendAsFollower(set2)
    assertEquals(Integer.MAX_VALUE.toLong + 2, log.activeSegment.baseOffset)
    assertTrue(Log.producerSnapshotFile(logDir, Integer.MAX_VALUE.toLong + 2).exists)
    //This will go into the existing log
    log.appendAsFollower(set3)
    assertEquals(Integer.MAX_VALUE.toLong + 2, log.activeSegment.baseOffset)
    //This will go into the existing log
    log.appendAsFollower(set4)
    assertEquals(Integer.MAX_VALUE.toLong + 2, log.activeSegment.baseOffset)
    log.close()
    val indexFiles = logDir.listFiles.filter(file => file.getName.contains(".index"))
    assertEquals(2, indexFiles.length)
    for (file <- indexFiles) {
      val offsetIndex = new OffsetIndex(file, file.getName.replace(".index","").toLong)
      assertTrue(offsetIndex.lastOffset >= 0)
      offsetIndex.close()
    }
    Utils.delete(logDir)
  }

  @Test
  def testOverCompactedLogRecoveryMultiRecordV1(): Unit = {
    // append some messages to create some segments
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    val log = createLog(logDir, logConfig)
    val set1 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 0, CompressionType.NONE,
      new SimpleRecord("v1".getBytes(), "k1".getBytes()))
    val set2 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, Integer.MAX_VALUE.toLong + 2, CompressionType.GZIP,
      new SimpleRecord("v3".getBytes(), "k3".getBytes()),
      new SimpleRecord("v4".getBytes(), "k4".getBytes()))
    val set3 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, Integer.MAX_VALUE.toLong + 4, CompressionType.GZIP,
      new SimpleRecord("v5".getBytes(), "k5".getBytes()),
      new SimpleRecord("v6".getBytes(), "k6".getBytes()))
    val set4 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, Integer.MAX_VALUE.toLong + 6, CompressionType.GZIP,
      new SimpleRecord("v7".getBytes(), "k7".getBytes()),
      new SimpleRecord("v8".getBytes(), "k8".getBytes()))
    //Writes into an empty log with baseOffset 0
    log.appendAsFollower(set1)
    assertEquals(0L, log.activeSegment.baseOffset)
    //This write will roll the segment, yielding a new segment with base offset = max(1, 3) = 3
    log.appendAsFollower(set2)
    assertEquals(3, log.activeSegment.baseOffset)
    assertTrue(Log.producerSnapshotFile(logDir, 3).exists)
    //This will also roll the segment, yielding a new segment with base offset = max(5, Integer.MAX_VALUE+4) = Integer.MAX_VALUE+4
    log.appendAsFollower(set3)
    assertEquals(Integer.MAX_VALUE.toLong + 4, log.activeSegment.baseOffset)
    assertTrue(Log.producerSnapshotFile(logDir, Integer.MAX_VALUE.toLong + 4).exists)
    //This will go into the existing log
    log.appendAsFollower(set4)
    assertEquals(Integer.MAX_VALUE.toLong + 4, log.activeSegment.baseOffset)
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
  def testRecoveryOfSegmentWithOffsetOverflow(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, fileDeleteDelayMs = 1000)
    val (log, _) = createLogWithOffsetOverflow(logConfig)
    val expectedKeys = LogTestUtils.keysInLog(log)

    // Run recovery on the log. This should split the segment underneath. Ignore .deleted files as we could have still
    // have them lying around after the split.
    val recoveredLog = recoverAndCheck(logConfig, expectedKeys)
    assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog))

    // Running split again would throw an error

    for (segment <- recoveredLog.logSegments) {
      assertThrows(classOf[IllegalArgumentException], () => log.splitOverflowedSegment(segment))
    }
  }

  @Test
  def testRecoveryAfterCrashDuringSplitPhase1(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, fileDeleteDelayMs = 1000)
    val (log, segmentWithOverflow) = createLogWithOffsetOverflow(logConfig)
    val expectedKeys = LogTestUtils.keysInLog(log)
    val numSegmentsInitial = log.logSegments.size

    // Split the segment
    val newSegments = log.splitOverflowedSegment(segmentWithOverflow)

    // Simulate recovery just after .cleaned file is created, before rename to .swap. On recovery, existing split
    // operation is aborted but the recovery process itself kicks off split which should complete.
    newSegments.reverse.foreach(segment => {
      segment.changeFileSuffixes("", Log.CleanedFileSuffix)
      segment.truncateTo(0)
    })
    for (file <- logDir.listFiles if file.getName.endsWith(Log.DeletedFileSuffix))
      Utils.atomicMoveWithFallback(file.toPath, Paths.get(CoreUtils.replaceSuffix(file.getPath, Log.DeletedFileSuffix, "")))

    val recoveredLog = recoverAndCheck(logConfig, expectedKeys)
    assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog))
    assertEquals(numSegmentsInitial + 1, recoveredLog.logSegments.size)
    recoveredLog.close()
  }

  @Test
  def testRecoveryAfterCrashDuringSplitPhase2(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, fileDeleteDelayMs = 1000)
    val (log, segmentWithOverflow) = createLogWithOffsetOverflow(logConfig)
    val expectedKeys = LogTestUtils.keysInLog(log)
    val numSegmentsInitial = log.logSegments.size

    // Split the segment
    val newSegments = log.splitOverflowedSegment(segmentWithOverflow)

    // Simulate recovery just after one of the new segments has been renamed to .swap. On recovery, existing split
    // operation is aborted but the recovery process itself kicks off split which should complete.
    newSegments.reverse.foreach { segment =>
      if (segment != newSegments.last)
        segment.changeFileSuffixes("", Log.CleanedFileSuffix)
      else
        segment.changeFileSuffixes("", Log.SwapFileSuffix)
      segment.truncateTo(0)
    }
    for (file <- logDir.listFiles if file.getName.endsWith(Log.DeletedFileSuffix))
      Utils.atomicMoveWithFallback(file.toPath, Paths.get(CoreUtils.replaceSuffix(file.getPath, Log.DeletedFileSuffix, "")))

    val recoveredLog = recoverAndCheck(logConfig, expectedKeys)
    assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog))
    assertEquals(numSegmentsInitial + 1, recoveredLog.logSegments.size)
    recoveredLog.close()
  }

  @Test
  def testRecoveryAfterCrashDuringSplitPhase3(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, fileDeleteDelayMs = 1000)
    val (log, segmentWithOverflow) = createLogWithOffsetOverflow(logConfig)
    val expectedKeys = LogTestUtils.keysInLog(log)
    val numSegmentsInitial = log.logSegments.size

    // Split the segment
    val newSegments = log.splitOverflowedSegment(segmentWithOverflow)

    // Simulate recovery right after all new segments have been renamed to .swap. On recovery, existing split operation
    // is completed and the old segment must be deleted.
    newSegments.reverse.foreach(segment => {
      segment.changeFileSuffixes("", Log.SwapFileSuffix)
    })
    for (file <- logDir.listFiles if file.getName.endsWith(Log.DeletedFileSuffix))
      Utils.atomicMoveWithFallback(file.toPath, Paths.get(CoreUtils.replaceSuffix(file.getPath, Log.DeletedFileSuffix, "")))

    // Truncate the old segment
    segmentWithOverflow.truncateTo(0)

    val recoveredLog = recoverAndCheck(logConfig, expectedKeys)
    assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog))
    assertEquals(numSegmentsInitial + 1, recoveredLog.logSegments.size)
    log.close()
  }

  @Test
  def testRecoveryAfterCrashDuringSplitPhase4(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, fileDeleteDelayMs = 1000)
    val (log, segmentWithOverflow) = createLogWithOffsetOverflow(logConfig)
    val expectedKeys = LogTestUtils.keysInLog(log)
    val numSegmentsInitial = log.logSegments.size

    // Split the segment
    val newSegments = log.splitOverflowedSegment(segmentWithOverflow)

    // Simulate recovery right after all new segments have been renamed to .swap and old segment has been deleted. On
    // recovery, existing split operation is completed.
    newSegments.reverse.foreach(_.changeFileSuffixes("", Log.SwapFileSuffix))

    for (file <- logDir.listFiles if file.getName.endsWith(Log.DeletedFileSuffix))
      Utils.delete(file)

    // Truncate the old segment
    segmentWithOverflow.truncateTo(0)

    val recoveredLog = recoverAndCheck(logConfig, expectedKeys)
    assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog))
    assertEquals(numSegmentsInitial + 1, recoveredLog.logSegments.size)
    recoveredLog.close()
  }

  @Test
  def testRecoveryAfterCrashDuringSplitPhase5(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, fileDeleteDelayMs = 1000)
    val (log, segmentWithOverflow) = createLogWithOffsetOverflow(logConfig)
    val expectedKeys = LogTestUtils.keysInLog(log)
    val numSegmentsInitial = log.logSegments.size

    // Split the segment
    val newSegments = log.splitOverflowedSegment(segmentWithOverflow)

    // Simulate recovery right after one of the new segment has been renamed to .swap and the other to .log. On
    // recovery, existing split operation is completed.
    newSegments.last.changeFileSuffixes("", Log.SwapFileSuffix)

    // Truncate the old segment
    segmentWithOverflow.truncateTo(0)

    val recoveredLog = recoverAndCheck(logConfig, expectedKeys)
    assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog))
    assertEquals(numSegmentsInitial + 1, recoveredLog.logSegments.size)
    recoveredLog.close()
  }

  @Test
  def testCleanShutdownFile(): Unit = {
    // append some messages to create some segments
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)

    var recoveryPoint = 0L
    // create a log and write some messages to it
    var log = createLog(logDir, logConfig)
    for (_ <- 0 until 100)
      log.appendAsLeader(createRecords, leaderEpoch = 0)
    log.close()

    // check if recovery was attempted. Even if the recovery point is 0L, recovery should not be attempted as the
    // clean shutdown file exists. Note: Earlier, Log layer relied on the presence of clean shutdown file to determine the status
    // of last shutdown. Now, LogManager checks for the presence of this file and immediately deletes the same. It passes
    // down a clean shutdown flag to the Log layer as log is loaded. Recovery is attempted based on this flag.
    recoveryPoint = log.logEndOffset
    log = createLog(logDir, logConfig)
    assertEquals(recoveryPoint, log.logEndOffset)
  }

  /**
   * Append a bunch of messages to a log and then re-open it with recovery and check that the leader epochs are recovered properly.
   */
  @Test
  def testLogRecoversForLeaderEpoch(): Unit = {
    val log = createLog(logDir, LogConfig())
    val leaderEpochCache = log.leaderEpochCache.get
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
    leaderEpochCache.truncateFromEnd(2)
    assertNotEquals(ListBuffer(EpochEntry(1, 0), EpochEntry(2, 1), EpochEntry(3, 3)), leaderEpochCache.epochEntries)
    log.close()

    // reopen the log and recover from the beginning
    val recoveredLog = createLog(logDir, LogConfig(), lastShutdownClean = false)
    val recoveredLeaderEpochCache = recoveredLog.leaderEpochCache.get

    // epoch entries should be recovered
    assertEquals(ListBuffer(EpochEntry(1, 0), EpochEntry(2, 1), EpochEntry(3, 3)), recoveredLeaderEpochCache.epochEntries)
    recoveredLog.close()
  }

  @Test
  def testFullTransactionIndexRecovery(): Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = 128 * 5)
    val log = createLog(logDir, logConfig)
    val epoch = 0.toShort

    val pid1 = 1L
    val pid2 = 2L
    val pid3 = 3L
    val pid4 = 4L

    val appendPid1 = LogTestUtils.appendTransactionalAsLeader(log, pid1, epoch, mockTime)
    val appendPid2 = LogTestUtils.appendTransactionalAsLeader(log, pid2, epoch, mockTime)
    val appendPid3 = LogTestUtils.appendTransactionalAsLeader(log, pid3, epoch, mockTime)
    val appendPid4 = LogTestUtils.appendTransactionalAsLeader(log, pid4, epoch, mockTime)

    // mix transactional and non-transactional data
    appendPid1(5) // nextOffset: 5
    LogTestUtils.appendNonTransactionalAsLeader(log, 3) // 8
    appendPid2(2) // 10
    appendPid1(4) // 14
    appendPid3(3) // 17
    LogTestUtils.appendNonTransactionalAsLeader(log, 2) // 19
    appendPid1(10) // 29
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT, mockTime.milliseconds()) // 30
    appendPid2(6) // 36
    appendPid4(3) // 39
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 49
    appendPid3(9) // 58
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid3, epoch, ControlRecordType.COMMIT, mockTime.milliseconds()) // 59
    appendPid4(8) // 67
    appendPid2(7) // 74
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.ABORT, mockTime.milliseconds()) // 75
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 85
    appendPid4(4) // 89
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid4, epoch, ControlRecordType.COMMIT, mockTime.milliseconds()) // 90

    // delete all the offset and transaction index files to force recovery
    log.logSegments.foreach { segment =>
      segment.offsetIndex.deleteIfExists()
      segment.txnIndex.deleteIfExists()
    }

    log.close()

    val reloadedLogConfig = LogTest.createLogConfig(segmentBytes = 1024 * 5)
    val reloadedLog = createLog(logDir, reloadedLogConfig, lastShutdownClean = false)
    val abortedTransactions = LogTestUtils.allAbortedTransactions(reloadedLog)
    assertEquals(List(new AbortedTxn(pid1, 0L, 29L, 8L), new AbortedTxn(pid2, 8L, 74L, 36L)), abortedTransactions)
  }

  @Test
  def testRecoverOnlyLastSegment(): Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = 128 * 5)
    val log = createLog(logDir, logConfig)
    val epoch = 0.toShort

    val pid1 = 1L
    val pid2 = 2L
    val pid3 = 3L
    val pid4 = 4L

    val appendPid1 = LogTestUtils.appendTransactionalAsLeader(log, pid1, epoch, mockTime)
    val appendPid2 = LogTestUtils.appendTransactionalAsLeader(log, pid2, epoch, mockTime)
    val appendPid3 = LogTestUtils.appendTransactionalAsLeader(log, pid3, epoch, mockTime)
    val appendPid4 = LogTestUtils.appendTransactionalAsLeader(log, pid4, epoch, mockTime)

    // mix transactional and non-transactional data
    appendPid1(5) // nextOffset: 5
    LogTestUtils.appendNonTransactionalAsLeader(log, 3) // 8
    appendPid2(2) // 10
    appendPid1(4) // 14
    appendPid3(3) // 17
    LogTestUtils.appendNonTransactionalAsLeader(log, 2) // 19
    appendPid1(10) // 29
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT, mockTime.milliseconds()) // 30
    appendPid2(6) // 36
    appendPid4(3) // 39
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 49
    appendPid3(9) // 58
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid3, epoch, ControlRecordType.COMMIT, mockTime.milliseconds()) // 59
    appendPid4(8) // 67
    appendPid2(7) // 74
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.ABORT, mockTime.milliseconds()) // 75
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 85
    appendPid4(4) // 89
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid4, epoch, ControlRecordType.COMMIT, mockTime.milliseconds()) // 90

    // delete the last offset and transaction index files to force recovery
    val lastSegment = log.logSegments.last
    val recoveryPoint = lastSegment.baseOffset
    lastSegment.offsetIndex.deleteIfExists()
    lastSegment.txnIndex.deleteIfExists()

    log.close()

    val reloadedLogConfig = LogTest.createLogConfig(segmentBytes = 1024 * 5)
    val reloadedLog = createLog(logDir, reloadedLogConfig, recoveryPoint = recoveryPoint, lastShutdownClean = false)
    val abortedTransactions = LogTestUtils.allAbortedTransactions(reloadedLog)
    assertEquals(List(new AbortedTxn(pid1, 0L, 29L, 8L), new AbortedTxn(pid2, 8L, 74L, 36L)), abortedTransactions)
  }

  @Test
  def testRecoverLastSegmentWithNoSnapshots(): Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = 128 * 5)
    val log = createLog(logDir, logConfig)
    val epoch = 0.toShort

    val pid1 = 1L
    val pid2 = 2L
    val pid3 = 3L
    val pid4 = 4L

    val appendPid1 = LogTestUtils.appendTransactionalAsLeader(log, pid1, epoch, mockTime)
    val appendPid2 = LogTestUtils.appendTransactionalAsLeader(log, pid2, epoch, mockTime)
    val appendPid3 = LogTestUtils.appendTransactionalAsLeader(log, pid3, epoch, mockTime)
    val appendPid4 = LogTestUtils.appendTransactionalAsLeader(log, pid4, epoch, mockTime)

    // mix transactional and non-transactional data
    appendPid1(5) // nextOffset: 5
    LogTestUtils.appendNonTransactionalAsLeader(log, 3) // 8
    appendPid2(2) // 10
    appendPid1(4) // 14
    appendPid3(3) // 17
    LogTestUtils.appendNonTransactionalAsLeader(log, 2) // 19
    appendPid1(10) // 29
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT, mockTime.milliseconds()) // 30
    appendPid2(6) // 36
    appendPid4(3) // 39
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 49
    appendPid3(9) // 58
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid3, epoch, ControlRecordType.COMMIT, mockTime.milliseconds()) // 59
    appendPid4(8) // 67
    appendPid2(7) // 74
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.ABORT, mockTime.milliseconds()) // 75
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 85
    appendPid4(4) // 89
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid4, epoch, ControlRecordType.COMMIT, mockTime.milliseconds()) // 90

    LogTestUtils.deleteProducerSnapshotFiles(logDir)

    // delete the last offset and transaction index files to force recovery. this should force us to rebuild
    // the producer state from the start of the log
    val lastSegment = log.logSegments.last
    val recoveryPoint = lastSegment.baseOffset
    lastSegment.offsetIndex.deleteIfExists()
    lastSegment.txnIndex.deleteIfExists()

    log.close()

    val reloadedLogConfig = LogTest.createLogConfig(segmentBytes = 1024 * 5)
    val reloadedLog = createLog(logDir, reloadedLogConfig, recoveryPoint = recoveryPoint, lastShutdownClean = false)
    val abortedTransactions = LogTestUtils.allAbortedTransactions(reloadedLog)
    assertEquals(List(new AbortedTxn(pid1, 0L, 29L, 8L), new AbortedTxn(pid2, 8L, 74L, 36L)), abortedTransactions)
  }
}
