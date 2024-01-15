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
import java.nio.file.Files
import java.util.Properties
import kafka.server.BrokerTopicStats
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogConfig, LogDirFailureChannel, LogSegment, LogSegments, LogStartOffsetIncrementReason, ProducerStateManager, ProducerStateManagerConfig}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}

import java.util
import scala.collection.mutable
import scala.compat.java8.OptionConverters._

/**
  * Unit tests for the log cleaning logic
  */
class LogCleanerManagerTest extends Logging {

  val tmpDir: File = TestUtils.tempDir()
  val tmpDir2: File = TestUtils.tempDir()
  val logDir: File = TestUtils.randomPartitionLogDir(tmpDir)
  val logDir2: File = TestUtils.randomPartitionLogDir(tmpDir)
  val topicPartition = new TopicPartition("log", 0)
  val topicPartition2 = new TopicPartition("log2", 0)
  val logProps = new Properties()
  logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, 1024: java.lang.Integer)
  logProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 1024: java.lang.Integer)
  logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
  val logConfig: LogConfig = new LogConfig(logProps)
  val time = new MockTime(1400000000000L, 1000L)  // Tue May 13 16:53:20 UTC 2014 for `currentTimeMs`
  val offset = 999
  val producerStateManagerConfig = new ProducerStateManagerConfig(kafka.server.Defaults.ProducerIdExpirationMs, false)

  val cleanerCheckpoints: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()

  class LogCleanerManagerMock(logDirs: Seq[File],
                              logs: Pool[TopicPartition, UnifiedLog],
                              logDirFailureChannel: LogDirFailureChannel) extends LogCleanerManager(logDirs, logs, logDirFailureChannel) {
    override def allCleanerCheckpoints: Map[TopicPartition, Long] = {
      cleanerCheckpoints.toMap
    }

    override def updateCheckpoints(dataDir: File, partitionToUpdateOrAdd: Option[(TopicPartition,Long)] = None,
                                   partitionToRemove: Option[TopicPartition] = None): Unit = {
      assert(partitionToRemove.isEmpty, "partitionToRemove argument with value not yet handled")
      val (tp, offset) = partitionToUpdateOrAdd.getOrElse(
        throw new IllegalArgumentException("partitionToUpdateOrAdd==None argument not yet handled"))
      cleanerCheckpoints.put(tp, offset)
    }
  }

  @AfterEach
  def tearDown(): Unit = {
    Utils.delete(tmpDir)
  }

  private def setupIncreasinglyFilthyLogs(partitions: Seq[TopicPartition],
                                          startNumBatches: Int,
                                          batchIncrement: Int): Pool[TopicPartition, UnifiedLog] = {
    val logs = new Pool[TopicPartition, UnifiedLog]()
    var numBatches = startNumBatches

    for (tp <- partitions) {
      val log = createLog(2048, TopicConfig.CLEANUP_POLICY_COMPACT, topicPartition = tp)
      logs.put(tp, log)

      writeRecords(log, numBatches = numBatches, recordsPerBatch = 1, batchesPerSegment = 5)
      numBatches += batchIncrement
    }
    logs
  }

  @Test
  def testGrabFilthiestCompactedLogThrowsException(): Unit = {
    val tp = new TopicPartition("A", 1)
    val logSegmentSize = TestUtils.singletonRecords("test".getBytes).sizeInBytes * 10
    val logSegmentsCount = 2
    val tpDir = new File(logDir, "A-1")
    Files.createDirectories(tpDir.toPath)
    val logDirFailureChannel = new LogDirFailureChannel(10)
    val config = createLowRetentionLogConfig(logSegmentSize, TopicConfig.CLEANUP_POLICY_COMPACT)
    val maxTransactionTimeoutMs = 5 * 60 * 1000
    val producerIdExpirationCheckIntervalMs = kafka.server.Defaults.ProducerIdExpirationCheckIntervalMs
    val segments = new LogSegments(tp)
    val leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(tpDir, topicPartition, logDirFailureChannel, config.recordVersion, "")
    val producerStateManager = new ProducerStateManager(topicPartition, tpDir, maxTransactionTimeoutMs, producerStateManagerConfig, time)
    val offsets = new LogLoader(
      tpDir,
      tp,
      config,
      time.scheduler,
      time,
      logDirFailureChannel,
      hadCleanShutdown = true,
      segments,
      0L,
      0L,
      leaderEpochCache.asJava,
      producerStateManager
    ).load()
    val localLog = new LocalLog(tpDir, config, segments, offsets.recoveryPoint,
      offsets.nextOffsetMetadata, time.scheduler, time, tp, logDirFailureChannel)
    // the exception should be caught and the partition that caused it marked as uncleanable
    class LogMock extends UnifiedLog(offsets.logStartOffset, localLog, new BrokerTopicStats,
        producerIdExpirationCheckIntervalMs, leaderEpochCache,
        producerStateManager, _topicId = None, keepPartitionMetadataFile = true) {
      // Throw an error in getFirstBatchTimestampForSegments since it is called in grabFilthiestLog()
      override def getFirstBatchTimestampForSegments(segments: util.Collection[LogSegment]): util.Collection[java.lang.Long] =
        throw new IllegalStateException("Error!")
    }

    val log: UnifiedLog = new LogMock()
    writeRecords(log = log,
      numBatches = logSegmentsCount * 2,
      recordsPerBatch = 10,
      batchesPerSegment = 2
    )

    val logsPool = new Pool[TopicPartition, UnifiedLog]()
    logsPool.put(tp, log)
    val cleanerManager = createCleanerManagerMock(logsPool)
    cleanerCheckpoints.put(tp, 1)

    val thrownException = assertThrows(classOf[LogCleaningException], () => cleanerManager.grabFilthiestCompactedLog(time).get)
    assertEquals(log, thrownException.log)
    assertTrue(thrownException.getCause.isInstanceOf[IllegalStateException])
  }

  @Test
  def testGrabFilthiestCompactedLogReturnsLogWithDirtiestRatio(): Unit = {
    val tp0 = new TopicPartition("wishing-well", 0)
    val tp1 = new TopicPartition("wishing-well", 1)
    val tp2 = new TopicPartition("wishing-well", 2)
    val partitions = Seq(tp0, tp1, tp2)

    // setup logs with cleanable range: [20, 20], [20, 25], [20, 30]
    val logs = setupIncreasinglyFilthyLogs(partitions, startNumBatches = 20, batchIncrement = 5)
    val cleanerManager = createCleanerManagerMock(logs)
    partitions.foreach(partition => cleanerCheckpoints.put(partition, 20))

    val filthiestLog: LogToClean = cleanerManager.grabFilthiestCompactedLog(time).get
    assertEquals(tp2, filthiestLog.topicPartition)
    assertEquals(tp2, filthiestLog.log.topicPartition)
  }

  @Test
  def testGrabFilthiestCompactedLogIgnoresUncleanablePartitions(): Unit = {
    val tp0 = new TopicPartition("wishing-well", 0)
    val tp1 = new TopicPartition("wishing-well", 1)
    val tp2 = new TopicPartition("wishing-well", 2)
    val partitions = Seq(tp0, tp1, tp2)

    // setup logs with cleanable range: [20, 20], [20, 25], [20, 30]
    val logs = setupIncreasinglyFilthyLogs(partitions, startNumBatches = 20, batchIncrement = 5)
    val cleanerManager = createCleanerManagerMock(logs)
    partitions.foreach(partition => cleanerCheckpoints.put(partition, 20))

    cleanerManager.markPartitionUncleanable(logs.get(tp2).dir.getParent, tp2)

    val filthiestLog: LogToClean = cleanerManager.grabFilthiestCompactedLog(time).get
    assertEquals(tp1, filthiestLog.topicPartition)
    assertEquals(tp1, filthiestLog.log.topicPartition)
  }

  @Test
  def testGrabFilthiestCompactedLogIgnoresInProgressPartitions(): Unit = {
    val tp0 = new TopicPartition("wishing-well", 0)
    val tp1 = new TopicPartition("wishing-well", 1)
    val tp2 = new TopicPartition("wishing-well", 2)
    val partitions = Seq(tp0, tp1, tp2)

    // setup logs with cleanable range: [20, 20], [20, 25], [20, 30]
    val logs = setupIncreasinglyFilthyLogs(partitions, startNumBatches = 20, batchIncrement = 5)
    val cleanerManager = createCleanerManagerMock(logs)
    partitions.foreach(partition => cleanerCheckpoints.put(partition, 20))

    cleanerManager.setCleaningState(tp2, LogCleaningInProgress)

    val filthiestLog: LogToClean = cleanerManager.grabFilthiestCompactedLog(time).get
    assertEquals(tp1, filthiestLog.topicPartition)
    assertEquals(tp1, filthiestLog.log.topicPartition)
  }

  @Test
  def testGrabFilthiestCompactedLogIgnoresBothInProgressPartitionsAndUncleanablePartitions(): Unit = {
    val tp0 = new TopicPartition("wishing-well", 0)
    val tp1 = new TopicPartition("wishing-well", 1)
    val tp2 = new TopicPartition("wishing-well", 2)
    val partitions = Seq(tp0, tp1, tp2)

    // setup logs with cleanable range: [20, 20], [20, 25], [20, 30]
    val logs = setupIncreasinglyFilthyLogs(partitions, startNumBatches = 20, batchIncrement = 5)
    val cleanerManager = createCleanerManagerMock(logs)
    partitions.foreach(partition => cleanerCheckpoints.put(partition, 20))

    cleanerManager.setCleaningState(tp2, LogCleaningInProgress)
    cleanerManager.markPartitionUncleanable(logs.get(tp1).dir.getParent, tp1)

    val filthiestLog: Option[LogToClean] = cleanerManager.grabFilthiestCompactedLog(time)
    assertEquals(None, filthiestLog)
  }

  @Test
  def testDirtyOffsetResetIfLargerThanEndOffset(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val logs = setupIncreasinglyFilthyLogs(Seq(tp), startNumBatches = 20, batchIncrement = 5)
    val cleanerManager = createCleanerManagerMock(logs)
    cleanerCheckpoints.put(tp, 200)

    val filthiestLog = cleanerManager.grabFilthiestCompactedLog(time).get
    assertEquals(0L, filthiestLog.firstDirtyOffset)
  }

  @Test
  def testDirtyOffsetResetIfSmallerThanStartOffset(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val logs = setupIncreasinglyFilthyLogs(Seq(tp), startNumBatches = 20, batchIncrement = 5)

    logs.get(tp).maybeIncrementLogStartOffset(10L, LogStartOffsetIncrementReason.ClientRecordDeletion)

    val cleanerManager = createCleanerManagerMock(logs)
    cleanerCheckpoints.put(tp, 0L)

    val filthiestLog = cleanerManager.grabFilthiestCompactedLog(time).get
    assertEquals(10L, filthiestLog.firstDirtyOffset)
  }

  @Test
  def testLogStartOffsetLargerThanActiveSegmentBaseOffset(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val log = createLog(segmentSize = 2048, TopicConfig.CLEANUP_POLICY_COMPACT, tp)

    val logs = new Pool[TopicPartition, UnifiedLog]()
    logs.put(tp, log)

    appendRecords(log, numRecords = 3)
    appendRecords(log, numRecords = 3)
    appendRecords(log, numRecords = 3)

    assertEquals(1, log.logSegments.size)

    log.maybeIncrementLogStartOffset(2L, LogStartOffsetIncrementReason.ClientRecordDeletion)

    val cleanerManager = createCleanerManagerMock(logs)
    cleanerCheckpoints.put(tp, 0L)

    // The active segment is uncleanable and hence not filthy from the POV of the CleanerManager.
    val filthiestLog = cleanerManager.grabFilthiestCompactedLog(time)
    assertEquals(None, filthiestLog)
  }

  @Test
  def testDirtyOffsetLargerThanActiveSegmentBaseOffset(): Unit = {
    // It is possible in the case of an unclean leader election for the checkpoint
    // dirty offset to get ahead of the active segment base offset, but still be
    // within the range of the log.

    val tp = new TopicPartition("foo", 0)

    val logs = new Pool[TopicPartition, UnifiedLog]()
    val log = createLog(2048, TopicConfig.CLEANUP_POLICY_COMPACT, topicPartition = tp)
    logs.put(tp, log)

    appendRecords(log, numRecords = 3)
    appendRecords(log, numRecords = 3)

    assertEquals(1, log.logSegments.size)
    assertEquals(0L, log.activeSegment.baseOffset)

    val cleanerManager = createCleanerManagerMock(logs)
    cleanerCheckpoints.put(tp, 3L)

    // These segments are uncleanable and hence not filthy
    val filthiestLog = cleanerManager.grabFilthiestCompactedLog(time)
    assertEquals(None, filthiestLog)
  }

  /**
    * When checking for logs with segments ready for deletion
    * we shouldn't consider logs where cleanup.policy=delete
    * as they are handled by the LogManager
    */
  @Test
  def testLogsWithSegmentsToDeleteShouldNotConsiderCleanupPolicyDeleteLogs(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes)
    val log: UnifiedLog = createLog(records.sizeInBytes * 5, TopicConfig.CLEANUP_POLICY_DELETE)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    val readyToDelete = cleanerManager.deletableLogs().size
    assertEquals(0, readyToDelete, "should have 0 logs ready to be deleted")
  }

  /**
    * We should find logs with segments ready to be deleted when cleanup.policy=compact,delete
    */
  @Test
  def testLogsWithSegmentsToDeleteShouldConsiderCleanupPolicyCompactDeleteLogs(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: UnifiedLog = createLog(records.sizeInBytes * 5, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    val readyToDelete = cleanerManager.deletableLogs().size
    assertEquals(1, readyToDelete, "should have 1 logs ready to be deleted")
  }

  /**
    * When looking for logs with segments ready to be deleted we should consider
    * logs with cleanup.policy=compact because they may have segments from before the log start offset
    */
  @Test
  def testLogsWithSegmentsToDeleteShouldConsiderCleanupPolicyCompactLogs(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: UnifiedLog = createLog(records.sizeInBytes * 5, TopicConfig.CLEANUP_POLICY_COMPACT)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    val readyToDelete = cleanerManager.deletableLogs().size
    assertEquals(1, readyToDelete, "should have 1 logs ready to be deleted")
  }

  /**
    * log under cleanup should be ineligible for compaction
    */
  @Test
  def testLogsUnderCleanupIneligibleForCompaction(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: UnifiedLog = createLog(records.sizeInBytes * 5, TopicConfig.CLEANUP_POLICY_DELETE)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    log.appendAsLeader(records, leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(records, leaderEpoch = 0)
    log.updateHighWatermark(2L)

    // simulate cleanup thread working on the log partition
    val deletableLog = cleanerManager.pauseCleaningForNonCompactedPartitions()
    assertEquals(1, deletableLog.size, "should have 1 logs ready to be deleted")

    // change cleanup policy from delete to compact
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, log.config.segmentSize: Integer)
    logProps.put(TopicConfig.RETENTION_MS_CONFIG, log.config.retentionMs: java.lang.Long)
    logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
    logProps.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, 0: Integer)
    val config = new LogConfig(logProps)
    log.updateConfig(config)

    // log cleanup inprogress, the log is not available for compaction
    val cleanable = cleanerManager.grabFilthiestCompactedLog(time)
    assertEquals(0, cleanable.size, "should have 0 logs ready to be compacted")

    // log cleanup finished, and log can be picked up for compaction
    cleanerManager.resumeCleaning(deletableLog.map(_._1))
    val cleanable2 = cleanerManager.grabFilthiestCompactedLog(time)
    assertEquals(1, cleanable2.size, "should have 1 logs ready to be compacted")

    // update cleanup policy to delete
    logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
    val config2 = new LogConfig(logProps)
    log.updateConfig(config2)

    // compaction in progress, should have 0 log eligible for log cleanup
    val deletableLog2 = cleanerManager.pauseCleaningForNonCompactedPartitions()
    assertEquals(0, deletableLog2.size, "should have 0 logs ready to be deleted")

    // compaction done, should have 1 log eligible for log cleanup
    cleanerManager.doneDeleting(Seq(cleanable2.get.topicPartition))
    val deletableLog3 = cleanerManager.pauseCleaningForNonCompactedPartitions()
    assertEquals(1, deletableLog3.size, "should have 1 logs ready to be deleted")
  }

  @Test
  def testUpdateCheckpointsShouldAddOffsetToPartition(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: UnifiedLog = createLog(records.sizeInBytes * 5, TopicConfig.CLEANUP_POLICY_COMPACT)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    // expect the checkpoint offset is not the expectedOffset before doing updateCheckpoints
    assertNotEquals(offset, cleanerManager.allCleanerCheckpoints.getOrElse(topicPartition, 0))

    cleanerManager.updateCheckpoints(logDir, partitionToUpdateOrAdd = Option(topicPartition, offset))
    // expect the checkpoint offset is now updated to the expected offset after doing updateCheckpoints
    assertEquals(offset, cleanerManager.allCleanerCheckpoints(topicPartition))
  }

  @Test
  def testUpdateCheckpointsShouldRemovePartitionData(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: UnifiedLog = createLog(records.sizeInBytes * 5, TopicConfig.CLEANUP_POLICY_COMPACT)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    // write some data into the cleaner-offset-checkpoint file
    cleanerManager.updateCheckpoints(logDir, partitionToUpdateOrAdd = Option(topicPartition, offset))
    assertEquals(offset, cleanerManager.allCleanerCheckpoints(topicPartition))

    // updateCheckpoints should remove the topicPartition data in the logDir
    cleanerManager.updateCheckpoints(logDir, partitionToRemove = Option(topicPartition))
    assertFalse(cleanerManager.allCleanerCheckpoints.contains(topicPartition))
  }

  @Test
  def testHandleLogDirFailureShouldRemoveDirAndData(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: UnifiedLog = createLog(records.sizeInBytes * 5, TopicConfig.CLEANUP_POLICY_COMPACT)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    // write some data into the cleaner-offset-checkpoint file in logDir and logDir2
    cleanerManager.updateCheckpoints(logDir, partitionToUpdateOrAdd = Option(topicPartition, offset))
    cleanerManager.updateCheckpoints(logDir2, partitionToUpdateOrAdd = Option(topicPartition2, offset))
    assertEquals(offset, cleanerManager.allCleanerCheckpoints(topicPartition))
    assertEquals(offset, cleanerManager.allCleanerCheckpoints(topicPartition2))

    cleanerManager.handleLogDirFailure(logDir.getAbsolutePath)
    // verify the partition data in logDir is gone, and data in logDir2 is still there
    assertEquals(offset, cleanerManager.allCleanerCheckpoints(topicPartition2))
    assertFalse(cleanerManager.allCleanerCheckpoints.contains(topicPartition))
  }

  @Test
  def testMaybeTruncateCheckpointShouldTruncateData(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: UnifiedLog = createLog(records.sizeInBytes * 5, TopicConfig.CLEANUP_POLICY_COMPACT)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)
    val lowerOffset = 1L
    val higherOffset = 1000L

    // write some data into the cleaner-offset-checkpoint file in logDir
    cleanerManager.updateCheckpoints(logDir, partitionToUpdateOrAdd = Option(topicPartition, offset))
    assertEquals(offset, cleanerManager.allCleanerCheckpoints(topicPartition))

    // we should not truncate the checkpoint data for checkpointed offset <= the given offset (higherOffset)
    cleanerManager.maybeTruncateCheckpoint(logDir, topicPartition, higherOffset)
    assertEquals(offset, cleanerManager.allCleanerCheckpoints(topicPartition))
    // we should truncate the checkpoint data for checkpointed offset > the given offset (lowerOffset)
    cleanerManager.maybeTruncateCheckpoint(logDir, topicPartition, lowerOffset)
    assertEquals(lowerOffset, cleanerManager.allCleanerCheckpoints(topicPartition))
  }

  @Test
  def testAlterCheckpointDirShouldRemoveDataInSrcDirAndAddInNewDir(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: UnifiedLog = createLog(records.sizeInBytes * 5, TopicConfig.CLEANUP_POLICY_COMPACT)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    // write some data into the cleaner-offset-checkpoint file in logDir
    cleanerManager.updateCheckpoints(logDir, partitionToUpdateOrAdd = Option(topicPartition, offset))
    assertEquals(offset, cleanerManager.allCleanerCheckpoints(topicPartition))

    cleanerManager.alterCheckpointDir(topicPartition, logDir, logDir2)
    // verify we still can get the partition offset after alterCheckpointDir
    // This data should locate in logDir2, not logDir
    assertEquals(offset, cleanerManager.allCleanerCheckpoints(topicPartition))

    // force delete the logDir2 from checkpoints, so that the partition data should also be deleted
    cleanerManager.handleLogDirFailure(logDir2.getAbsolutePath)
    assertFalse(cleanerManager.allCleanerCheckpoints.contains(topicPartition))
  }

  /**
    * log under cleanup should still be eligible for log truncation
    */
  @Test
  def testConcurrentLogCleanupAndLogTruncation(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: UnifiedLog = createLog(records.sizeInBytes * 5, TopicConfig.CLEANUP_POLICY_DELETE)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    // log cleanup starts
    val pausedPartitions = cleanerManager.pauseCleaningForNonCompactedPartitions()
    // Log truncation happens due to unclean leader election
    cleanerManager.abortAndPauseCleaning(log.topicPartition)
    cleanerManager.resumeCleaning(Seq(log.topicPartition))
    // log cleanup finishes and pausedPartitions are resumed
    cleanerManager.resumeCleaning(pausedPartitions.map(_._1))

    assertEquals(None, cleanerManager.cleaningState(log.topicPartition))
  }

  /**
    * log under cleanup should still be eligible for topic deletion
    */
  @Test
  def testConcurrentLogCleanupAndTopicDeletion(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key = "test".getBytes)
    val log: UnifiedLog = createLog(records.sizeInBytes * 5, TopicConfig.CLEANUP_POLICY_DELETE)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    // log cleanup starts
    val pausedPartitions = cleanerManager.pauseCleaningForNonCompactedPartitions()
    // Broker processes StopReplicaRequest with delete=true
    cleanerManager.abortCleaning(log.topicPartition)
    // log cleanup finishes and pausedPartitions are resumed
    cleanerManager.resumeCleaning(pausedPartitions.map(_._1))

    assertEquals(None, cleanerManager.cleaningState(log.topicPartition))
  }

  /**
    * When looking for logs with segments ready to be deleted we shouldn't consider
    * logs that have had their partition marked as uncleanable.
    */
  @Test
  def testLogsWithSegmentsToDeleteShouldNotConsiderUncleanablePartitions(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: UnifiedLog = createLog(records.sizeInBytes * 5, TopicConfig.CLEANUP_POLICY_COMPACT)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)
    cleanerManager.markPartitionUncleanable(log.dir.getParent, topicPartition)

    val readyToDelete = cleanerManager.deletableLogs().size
    assertEquals(0, readyToDelete, "should have 0 logs ready to be deleted")
  }

  /**
    * Test computation of cleanable range with no minimum compaction lag settings active where bounded by LSO
    */
  @Test
  def testCleanableOffsetsForNone(): Unit = {
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    while(log.numberOfSegments < 8)
      log.appendAsLeader(records(log.logEndOffset.toInt, log.logEndOffset.toInt, time.milliseconds()), leaderEpoch = 0)

    log.updateHighWatermark(50)

    val lastCleanOffset = Some(0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, lastCleanOffset, time.milliseconds)
    assertEquals(0L, cleanableOffsets.firstDirtyOffset, "The first cleanable offset starts at the beginning of the log.")
    assertEquals(log.highWatermark, log.lastStableOffset, "The high watermark equals the last stable offset as no transactions are in progress")
    assertEquals(log.lastStableOffset, cleanableOffsets.firstUncleanableDirtyOffset, "The first uncleanable offset is bounded by the last stable offset.")
  }

  /**
   * Test computation of cleanable range with no minimum compaction lag settings active where bounded by active segment
   */
  @Test
  def testCleanableOffsetsActiveSegment(): Unit = {
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    while(log.numberOfSegments < 8)
      log.appendAsLeader(records(log.logEndOffset.toInt, log.logEndOffset.toInt, time.milliseconds()), leaderEpoch = 0)

    log.updateHighWatermark(log.logEndOffset)

    val lastCleanOffset = Some(0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, lastCleanOffset, time.milliseconds)
    assertEquals(0L, cleanableOffsets.firstDirtyOffset, "The first cleanable offset starts at the beginning of the log.")
    assertEquals(log.activeSegment.baseOffset, cleanableOffsets.firstUncleanableDirtyOffset, "The first uncleanable offset begins with the active segment.")
  }

  /**
    * Test computation of cleanable range with a minimum compaction lag time
    */
  @Test
  def testCleanableOffsetsForTime(): Unit = {
    val compactionLag = 60 * 60 * 1000
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, 1024: java.lang.Integer)
    logProps.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, compactionLag: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val t0 = time.milliseconds
    while(log.numberOfSegments < 4)
      log.appendAsLeader(records(log.logEndOffset.toInt, log.logEndOffset.toInt, t0), leaderEpoch = 0)

    val activeSegAtT0 = log.activeSegment

    time.sleep(compactionLag + 1)
    val t1 = time.milliseconds

    while (log.numberOfSegments < 8)
      log.appendAsLeader(records(log.logEndOffset.toInt, log.logEndOffset.toInt, t1), leaderEpoch = 0)

    log.updateHighWatermark(log.logEndOffset)

    val lastCleanOffset = Some(0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, lastCleanOffset, time.milliseconds)
    assertEquals(0L, cleanableOffsets.firstDirtyOffset, "The first cleanable offset starts at the beginning of the log.")
    assertEquals(activeSegAtT0.baseOffset, cleanableOffsets.firstUncleanableDirtyOffset, "The first uncleanable offset begins with the second block of log entries.")
  }

  /**
    * Test computation of cleanable range with a minimum compaction lag time that is small enough that
    * the active segment contains it.
    */
  @Test
  def testCleanableOffsetsForShortTime(): Unit = {
    val compactionLag = 60 * 60 * 1000
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, 1024: java.lang.Integer)
    logProps.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, compactionLag: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val t0 = time.milliseconds
    while (log.numberOfSegments < 8)
      log.appendAsLeader(records(log.logEndOffset.toInt, log.logEndOffset.toInt, t0), leaderEpoch = 0)

    log.updateHighWatermark(log.logEndOffset)

    time.sleep(compactionLag + 1)

    val lastCleanOffset = Some(0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, lastCleanOffset, time.milliseconds)
    assertEquals(0L, cleanableOffsets.firstDirtyOffset, "The first cleanable offset starts at the beginning of the log.")
    assertEquals(log.activeSegment.baseOffset, cleanableOffsets.firstUncleanableDirtyOffset, "The first uncleanable offset begins with active segment.")
  }

  @Test
  def testCleanableOffsetsNeedsCheckpointReset(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val logs = setupIncreasinglyFilthyLogs(Seq(tp), startNumBatches = 20, batchIncrement = 5)
    logs.get(tp).maybeIncrementLogStartOffset(10L, LogStartOffsetIncrementReason.ClientRecordDeletion)

    var lastCleanOffset = Some(15L)
    var cleanableOffsets = LogCleanerManager.cleanableOffsets(logs.get(tp), lastCleanOffset, time.milliseconds)
    assertFalse(cleanableOffsets.forceUpdateCheckpoint, "Checkpoint offset should not be reset if valid")

    logs.get(tp).maybeIncrementLogStartOffset(20L, LogStartOffsetIncrementReason.ClientRecordDeletion)
    cleanableOffsets = LogCleanerManager.cleanableOffsets(logs.get(tp), lastCleanOffset, time.milliseconds)
    assertTrue(cleanableOffsets.forceUpdateCheckpoint, "Checkpoint offset needs to be reset if less than log start offset")

    lastCleanOffset = Some(25L)
    cleanableOffsets = LogCleanerManager.cleanableOffsets(logs.get(tp), lastCleanOffset, time.milliseconds)
    assertTrue(cleanableOffsets.forceUpdateCheckpoint, "Checkpoint offset needs to be reset if greater than log end offset")
  }

  @Test
  def testUndecidedTransactionalDataNotCleanable(): Unit = {
    val compactionLag = 60 * 60 * 1000
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, 1024: java.lang.Integer)
    logProps.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, compactionLag: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val producerId = 15L
    val producerEpoch = 0.toShort
    val sequence = 0
    log.appendAsLeader(MemoryRecords.withTransactionalRecords(CompressionType.NONE, producerId, producerEpoch, sequence,
      new SimpleRecord(time.milliseconds(), "1".getBytes, "a".getBytes),
      new SimpleRecord(time.milliseconds(), "2".getBytes, "b".getBytes)), leaderEpoch = 0)
    log.appendAsLeader(MemoryRecords.withTransactionalRecords(CompressionType.NONE, producerId, producerEpoch, sequence + 2,
      new SimpleRecord(time.milliseconds(), "3".getBytes, "c".getBytes)), leaderEpoch = 0)
    log.roll()
    log.updateHighWatermark(3L)

    time.sleep(compactionLag + 1)
    // although the compaction lag has been exceeded, the undecided data should not be cleaned
    var cleanableOffsets = LogCleanerManager.cleanableOffsets(log, Some(0L), time.milliseconds())
    assertEquals(0L, cleanableOffsets.firstDirtyOffset)
    assertEquals(0L, cleanableOffsets.firstUncleanableDirtyOffset)

    log.appendAsLeader(MemoryRecords.withEndTransactionMarker(time.milliseconds(), producerId, producerEpoch,
      new EndTransactionMarker(ControlRecordType.ABORT, 15)), leaderEpoch = 0,
      origin = AppendOrigin.COORDINATOR)
    log.roll()
    log.updateHighWatermark(4L)

    // the first segment should now become cleanable immediately
    cleanableOffsets = LogCleanerManager.cleanableOffsets(log, Some(0L), time.milliseconds())
    assertEquals(0L, cleanableOffsets.firstDirtyOffset)
    assertEquals(3L, cleanableOffsets.firstUncleanableDirtyOffset)

    time.sleep(compactionLag + 1)

    // the second segment becomes cleanable after the compaction lag
    cleanableOffsets = LogCleanerManager.cleanableOffsets(log, Some(0L), time.milliseconds())
    assertEquals(0L, cleanableOffsets.firstDirtyOffset)
    assertEquals(4L, cleanableOffsets.firstUncleanableDirtyOffset)
  }

  @Test
  def testDoneCleaning(): Unit = {
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, 1024: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))
    while(log.numberOfSegments < 8)
      log.appendAsLeader(records(log.logEndOffset.toInt, log.logEndOffset.toInt, time.milliseconds()), leaderEpoch = 0)

    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    assertThrows(classOf[IllegalStateException], () => cleanerManager.doneCleaning(topicPartition, log.dir, 1))

    cleanerManager.setCleaningState(topicPartition, LogCleaningPaused(1))
    assertThrows(classOf[IllegalStateException], () => cleanerManager.doneCleaning(topicPartition, log.dir, 1))

    cleanerManager.setCleaningState(topicPartition, LogCleaningInProgress)
    val endOffset = 1L
    cleanerManager.doneCleaning(topicPartition, log.dir, endOffset)
    assertTrue(cleanerManager.cleaningState(topicPartition).isEmpty)
    assertTrue(cleanerManager.allCleanerCheckpoints.contains(topicPartition))
    assertEquals(Some(endOffset), cleanerManager.allCleanerCheckpoints.get(topicPartition))

    cleanerManager.setCleaningState(topicPartition, LogCleaningAborted)
    cleanerManager.doneCleaning(topicPartition, log.dir, endOffset)
    assertEquals(LogCleaningPaused(1), cleanerManager.cleaningState(topicPartition).get)
    assertTrue(cleanerManager.allCleanerCheckpoints.contains(topicPartition))
  }

  @Test
  def testDoneDeleting(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: UnifiedLog = createLog(records.sizeInBytes * 5, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)
    val tp = new TopicPartition("log", 0)

    assertThrows(classOf[IllegalStateException], () => cleanerManager.doneDeleting(Seq(tp)))

    cleanerManager.setCleaningState(tp, LogCleaningPaused(1))
    assertThrows(classOf[IllegalStateException], () => cleanerManager.doneDeleting(Seq(tp)))

    cleanerManager.setCleaningState(tp, LogCleaningInProgress)
    cleanerManager.doneDeleting(Seq(tp))
    assertTrue(cleanerManager.cleaningState(tp).isEmpty)

    cleanerManager.setCleaningState(tp, LogCleaningAborted)
    cleanerManager.doneDeleting(Seq(tp))
    assertEquals(LogCleaningPaused(1), cleanerManager.cleaningState(tp).get)
  }

  /**
   * Logs with invalid checkpoint offsets should update their checkpoint offset even if the log doesn't need cleaning
   */
  @Test
  def testCheckpointUpdatedForInvalidOffsetNoCleaning(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val logs = setupIncreasinglyFilthyLogs(Seq(tp), startNumBatches = 20, batchIncrement = 5)

    logs.get(tp).maybeIncrementLogStartOffset(20L, LogStartOffsetIncrementReason.ClientRecordDeletion)
    val cleanerManager = createCleanerManagerMock(logs)
    cleanerCheckpoints.put(tp, 15L)

    val filthiestLog = cleanerManager.grabFilthiestCompactedLog(time)
    assertEquals(None, filthiestLog, "Log should not be selected for cleaning")
    assertEquals(20L, cleanerCheckpoints(tp), "Unselected log should have checkpoint offset updated")
  }

  /**
   * Logs with invalid checkpoint offsets should update their checkpoint offset even if they aren't selected
   * for immediate cleaning
   */
  @Test
  def testCheckpointUpdatedForInvalidOffsetNotSelected(): Unit = {
    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    val partitions = Seq(tp0, tp1)

    // create two logs, one with an invalid offset, and one that is dirtier than the log with an invalid offset
    val logs = setupIncreasinglyFilthyLogs(partitions, startNumBatches = 20, batchIncrement = 5)
    logs.get(tp0).maybeIncrementLogStartOffset(15L, LogStartOffsetIncrementReason.ClientRecordDeletion)
    val cleanerManager = createCleanerManagerMock(logs)
    cleanerCheckpoints.put(tp0, 10L)
    cleanerCheckpoints.put(tp1, 5L)

    val filthiestLog = cleanerManager.grabFilthiestCompactedLog(time).get
    assertEquals(tp1, filthiestLog.topicPartition, "Dirtier log should be selected")
    assertEquals(15L, cleanerCheckpoints(tp0), "Unselected log should have checkpoint offset updated")
  }

  private def createCleanerManager(log: UnifiedLog): LogCleanerManager = {
    val logs = new Pool[TopicPartition, UnifiedLog]()
    logs.put(topicPartition, log)
    new LogCleanerManager(Seq(logDir, logDir2), logs, null)
  }

  private def createCleanerManagerMock(pool: Pool[TopicPartition, UnifiedLog]): LogCleanerManagerMock = {
    new LogCleanerManagerMock(Seq(logDir), pool, null)
  }

  private def createLog(segmentSize: Int,
                        cleanupPolicy: String,
                        topicPartition: TopicPartition = new TopicPartition("log", 0)): UnifiedLog = {
    val config = createLowRetentionLogConfig(segmentSize, cleanupPolicy)
    val partitionDir = new File(logDir, UnifiedLog.logDirName(topicPartition))

    UnifiedLog(
      dir = partitionDir,
      config = config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      time = time,
      brokerTopicStats = new BrokerTopicStats,
      maxTransactionTimeoutMs = 5 * 60 * 1000,
      producerStateManagerConfig = producerStateManagerConfig,
      producerIdExpirationCheckIntervalMs = kafka.server.Defaults.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10),
      topicId = None,
      keepPartitionMetadataFile = true)
  }

  private def createLowRetentionLogConfig(segmentSize: Int, cleanupPolicy: String): LogConfig = {
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, segmentSize: Integer)
    logProps.put(TopicConfig.RETENTION_MS_CONFIG, 1: Integer)
    logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicy)
    logProps.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, 0.05: java.lang.Double) // small for easier and clearer tests

    new LogConfig(logProps)
  }

  private def writeRecords(log: UnifiedLog,
                           numBatches: Int,
                           recordsPerBatch: Int,
                           batchesPerSegment: Int): Unit = {
    for (i <- 0 until numBatches) {
      appendRecords(log, recordsPerBatch)
      if (i % batchesPerSegment == 0)
        log.roll()
    }
    log.roll()
  }

  private def appendRecords(log: UnifiedLog, numRecords: Int): Unit = {
    val startOffset = log.logEndOffset
    val endOffset = startOffset + numRecords
    var lastTimestamp = 0L
    val records = (startOffset until endOffset).map { offset =>
      val currentTimestamp = time.milliseconds()
      if (offset == endOffset - 1)
        lastTimestamp = currentTimestamp
      new SimpleRecord(currentTimestamp, s"key-$offset".getBytes, s"value-$offset".getBytes)
    }

    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE, records:_*), leaderEpoch = 1)
    log.maybeIncrementHighWatermark(log.logEndOffsetMetadata)
  }

  private def makeLog(dir: File = logDir, config: LogConfig) = {
    UnifiedLog(
      dir = dir,
      config = config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      time = time,
      brokerTopicStats = new BrokerTopicStats,
      maxTransactionTimeoutMs = 5 * 60 * 1000,
      producerStateManagerConfig = producerStateManagerConfig,
      producerIdExpirationCheckIntervalMs = kafka.server.Defaults.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10),
      topicId = None,
      keepPartitionMetadataFile = true
    )
  }

  private def records(key: Int, value: Int, timestamp: Long) =
    MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord(timestamp, key.toString.getBytes, value.toString.getBytes))

}
