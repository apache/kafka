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

import kafka.server.{BrokerTopicStats, LogDirFailureChannel}
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Utils
import org.junit.Assert._
import org.junit.{After, Test}
import org.scalatest.Assertions.intercept

import scala.collection.mutable

/**
  * Unit tests for the log cleaning logic
  */
class LogCleanerManagerTest extends Logging {

  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val topicPartition = new TopicPartition("log", 0)
  val logProps = new Properties()
  logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
  logProps.put(LogConfig.SegmentIndexBytesProp, 1024: java.lang.Integer)
  logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
  val logConfig = LogConfig(logProps)
  val time = new MockTime(1400000000000L, 1000L)  // Tue May 13 16:53:20 UTC 2014 for `currentTimeMs`

  val cleanerCheckpoints: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()

  class LogCleanerManagerMock(logDirs: Seq[File],
                              logs: Pool[TopicPartition, Log],
                              logDirFailureChannel: LogDirFailureChannel) extends LogCleanerManager(logDirs, logs, logDirFailureChannel) {
    override def allCleanerCheckpoints: Map[TopicPartition, Long] = {
      cleanerCheckpoints.toMap
    }
  }

  @After
  def tearDown(): Unit = {
    Utils.delete(tmpDir)
  }

  @Test
  def testGrabFilthiestCompactedLogReturnsLogWithDirtiestRatio(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes)
    val log1: Log = createLog(records.sizeInBytes * 5, LogConfig.Compact, 1)
    val log2: Log = createLog(records.sizeInBytes * 10, LogConfig.Compact, 2)
    val log3: Log = createLog(records.sizeInBytes * 15, LogConfig.Compact, 3)

    val logs = new Pool[TopicPartition, Log]()
    val tp1 = new TopicPartition("wishing well", 0) // active segment starts at 0
    logs.put(tp1, log1)
    val tp2 = new TopicPartition("wishing well", 1) // active segment starts at 10
    logs.put(tp2, log2)
    val tp3 = new TopicPartition("wishing well", 2) // // active segment starts at 20
    logs.put(tp3, log3)
    val cleanerManager: LogCleanerManagerMock = createCleanerManager(logs, toMock = true).asInstanceOf[LogCleanerManagerMock]
    cleanerCheckpoints.put(tp1, 0) // all clean
    cleanerCheckpoints.put(tp2, 1) // dirtiest - 9 dirty messages
    cleanerCheckpoints.put(tp3, 15) // 5 dirty messages

    val filthiestLog: LogToClean = cleanerManager.grabFilthiestCompactedLog(time).get

    assertEquals(log2, filthiestLog.log)
    assertEquals(tp2, filthiestLog.topicPartition)
  }

  @Test
  def testGrabFilthiestCompactedLogIgnoresUncleanablePartitions(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes)
    val log1: Log = createLog(records.sizeInBytes * 5, LogConfig.Compact, 1)
    val log2: Log = createLog(records.sizeInBytes * 10, LogConfig.Compact, 2)
    val log3: Log = createLog(records.sizeInBytes * 15, LogConfig.Compact, 3)

    val logs = new Pool[TopicPartition, Log]()
    val tp1 = new TopicPartition("wishing well", 0) // active segment starts at 0
    logs.put(tp1, log1)
    val tp2 = new TopicPartition("wishing well", 1) // active segment starts at 10
    logs.put(tp2, log2)
    val tp3 = new TopicPartition("wishing well", 2) // // active segment starts at 20
    logs.put(tp3, log3)
    val cleanerManager: LogCleanerManagerMock = createCleanerManager(logs, toMock = true).asInstanceOf[LogCleanerManagerMock]
    cleanerCheckpoints.put(tp1, 0) // all clean
    cleanerCheckpoints.put(tp2, 1) // dirtiest - 9 dirty messages
    cleanerCheckpoints.put(tp3, 15) // 5 dirty messages
    cleanerManager.markPartitionUncleanable(log2.dir.getParent, tp2)

    val filthiestLog: LogToClean = cleanerManager.grabFilthiestCompactedLog(time).get

    assertEquals(log3, filthiestLog.log)
    assertEquals(tp3, filthiestLog.topicPartition)
  }

  @Test
  def testGrabFilthiestCompactedLogIgnoresInProgressPartitions(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes)
    val log1: Log = createLog(records.sizeInBytes * 5, LogConfig.Compact, 1)
    val log2: Log = createLog(records.sizeInBytes * 10, LogConfig.Compact, 2)
    val log3: Log = createLog(records.sizeInBytes * 15, LogConfig.Compact, 3)

    val logs = new Pool[TopicPartition, Log]()
    val tp1 = new TopicPartition("wishing well", 0) // active segment starts at 0
    logs.put(tp1, log1)
    val tp2 = new TopicPartition("wishing well", 1) // active segment starts at 10
    logs.put(tp2, log2)
    val tp3 = new TopicPartition("wishing well", 2) // // active segment starts at 20
    logs.put(tp3, log3)
    val cleanerManager: LogCleanerManagerMock = createCleanerManager(logs, toMock = true).asInstanceOf[LogCleanerManagerMock]
    cleanerCheckpoints.put(tp1, 0) // all clean
    cleanerCheckpoints.put(tp2, 1) // dirtiest - 9 dirty messages
    cleanerCheckpoints.put(tp3, 15) // 5 dirty messages
    cleanerManager.setCleaningState(tp2, LogCleaningInProgress)

    val filthiestLog: LogToClean = cleanerManager.grabFilthiestCompactedLog(time).get

    assertEquals(log3, filthiestLog.log)
    assertEquals(tp3, filthiestLog.topicPartition)
  }

  @Test
  def testGrabFilthiestCompactedLogIgnoresBothInProgressPartitionsAndUncleanablePartitions(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes)
    val log1: Log = createLog(records.sizeInBytes * 5, LogConfig.Compact, 1)
    val log2: Log = createLog(records.sizeInBytes * 10, LogConfig.Compact, 2)
    val log3: Log = createLog(records.sizeInBytes * 15, LogConfig.Compact, 3)

    val logs = new Pool[TopicPartition, Log]()
    val tp1 = new TopicPartition("wishing well", 0) // active segment starts at 0
    logs.put(tp1, log1)
    val tp2 = new TopicPartition("wishing well", 1) // active segment starts at 10
    logs.put(tp2, log2)
    val tp3 = new TopicPartition("wishing well", 2) // // active segment starts at 20
    logs.put(tp3, log3)
    val cleanerManager: LogCleanerManagerMock = createCleanerManager(logs, toMock = true).asInstanceOf[LogCleanerManagerMock]
    cleanerCheckpoints.put(tp1, 0) // all clean
    cleanerCheckpoints.put(tp2, 1) // dirtiest - 9 dirty messages
    cleanerCheckpoints.put(tp3, 15) // 5 dirty messages
    cleanerManager.setCleaningState(tp2, LogCleaningInProgress)
    cleanerManager.markPartitionUncleanable(log3.dir.getParent, tp3)

    val filthiestLog: Option[LogToClean] = cleanerManager.grabFilthiestCompactedLog(time)

    assertTrue(filthiestLog.isEmpty)
  }

  /**
    * When checking for logs with segments ready for deletion
    * we shouldn't consider logs where cleanup.policy=delete
    * as they are handled by the LogManager
    */
  @Test
  def testLogsWithSegmentsToDeleteShouldNotConsiderCleanupPolicyDeleteLogs(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes)
    val log: Log = createLog(records.sizeInBytes * 5, LogConfig.Delete)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    val readyToDelete = cleanerManager.deletableLogs().size
    assertEquals("should have 0 logs ready to be deleted", 0, readyToDelete)
  }

  /**
    * We should find logs with segments ready to be deleted when cleanup.policy=compact,delete
    */
  @Test
  def testLogsWithSegmentsToDeleteShouldConsiderCleanupPolicyCompactDeleteLogs(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: Log = createLog(records.sizeInBytes * 5, LogConfig.Compact + "," + LogConfig.Delete)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    val readyToDelete = cleanerManager.deletableLogs().size
    assertEquals("should have 1 logs ready to be deleted", 1, readyToDelete)
  }

  /**
    * When looking for logs with segments ready to be deleted we should consider
    * logs with cleanup.policy=compact because they may have segments from before the log start offset
    */
  @Test
  def testLogsWithSegmentsToDeleteShouldConsiderCleanupPolicyCompactLogs(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: Log = createLog(records.sizeInBytes * 5, LogConfig.Compact)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    val readyToDelete = cleanerManager.deletableLogs().size
    assertEquals("should have 1 logs ready to be deleted", 1, readyToDelete)
  }

  /**
    * log under cleanup should be ineligible for compaction
    */
  @Test
  def testLogsUnderCleanupIneligibleForCompaction(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: Log = createLog(records.sizeInBytes * 5, LogConfig.Delete)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    log.appendAsLeader(records, leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(records, leaderEpoch = 0)
    log.onHighWatermarkIncremented(2L)

    // simulate cleanup thread working on the log partition
    val deletableLog = cleanerManager.pauseCleaningForNonCompactedPartitions()
    assertEquals("should have 1 logs ready to be deleted", 1, deletableLog.size)

    // change cleanup policy from delete to compact
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, log.config.segmentSize)
    logProps.put(LogConfig.RetentionMsProp, log.config.retentionMs)
    logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    logProps.put(LogConfig.MinCleanableDirtyRatioProp, 0: Integer)
    val config = LogConfig(logProps)
    log.config = config

    // log cleanup inprogress, the log is not available for compaction
    val cleanable = cleanerManager.grabFilthiestCompactedLog(time)
    assertEquals("should have 0 logs ready to be compacted", 0, cleanable.size)

    // log cleanup finished, and log can be picked up for compaction
    cleanerManager.resumeCleaning(deletableLog.map(_._1))
    val cleanable2 = cleanerManager.grabFilthiestCompactedLog(time)
    assertEquals("should have 1 logs ready to be compacted", 1, cleanable2.size)

    // update cleanup policy to delete
    logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Delete)
    val config2 = LogConfig(logProps)
    log.config = config2

    // compaction in progress, should have 0 log eligible for log cleanup
    val deletableLog2 = cleanerManager.pauseCleaningForNonCompactedPartitions()
    assertEquals("should have 0 logs ready to be deleted", 0, deletableLog2.size)

    // compaction done, should have 1 log eligible for log cleanup
    cleanerManager.doneDeleting(Seq(cleanable2.get.topicPartition))
    val deletableLog3 = cleanerManager.pauseCleaningForNonCompactedPartitions()
    assertEquals("should have 1 logs ready to be deleted", 1, deletableLog3.size)
  }

  /**
    * log under cleanup should still be eligible for log truncation
    */
  @Test
  def testConcurrentLogCleanupAndLogTruncation(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: Log = createLog(records.sizeInBytes * 5, LogConfig.Delete)
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
    val log: Log = createLog(records.sizeInBytes * 5, LogConfig.Delete)
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
    val log: Log = createLog(records.sizeInBytes * 5, LogConfig.Compact)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)
    cleanerManager.markPartitionUncleanable(log.dir.getParent, topicPartition)

    val readyToDelete = cleanerManager.deletableLogs().size
    assertEquals("should have 0 logs ready to be deleted", 0, readyToDelete)
  }

  /**
    * Test computation of cleanable range with no minimum compaction lag settings active
    */
  @Test
  def testCleanableOffsetsForNone(): Unit = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    while(log.numberOfSegments < 8)
      log.appendAsLeader(records(log.logEndOffset.toInt, log.logEndOffset.toInt, time.milliseconds()), leaderEpoch = 0)

    val lastClean = Map(topicPartition -> 0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, topicPartition, lastClean, time.milliseconds)
    assertEquals("The first cleanable offset starts at the beginning of the log.", 0L, cleanableOffsets._1)
    assertEquals("The first uncleanable offset begins with the active segment.", log.activeSegment.baseOffset, cleanableOffsets._2)
  }

  /**
    * Test computation of cleanable range with a minimum compaction lag time
    */
  @Test
  def testCleanableOffsetsForTime(): Unit = {
    val compactionLag = 60 * 60 * 1000
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    logProps.put(LogConfig.MinCompactionLagMsProp, compactionLag: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val t0 = time.milliseconds
    while(log.numberOfSegments < 4)
      log.appendAsLeader(records(log.logEndOffset.toInt, log.logEndOffset.toInt, t0), leaderEpoch = 0)

    val activeSegAtT0 = log.activeSegment

    time.sleep(compactionLag + 1)
    val t1 = time.milliseconds

    while (log.numberOfSegments < 8)
      log.appendAsLeader(records(log.logEndOffset.toInt, log.logEndOffset.toInt, t1), leaderEpoch = 0)

    val lastClean = Map(topicPartition -> 0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, topicPartition, lastClean, time.milliseconds)
    assertEquals("The first cleanable offset starts at the beginning of the log.", 0L, cleanableOffsets._1)
    assertEquals("The first uncleanable offset begins with the second block of log entries.", activeSegAtT0.baseOffset, cleanableOffsets._2)
  }

  /**
    * Test computation of cleanable range with a minimum compaction lag time that is small enough that
    * the active segment contains it.
    */
  @Test
  def testCleanableOffsetsForShortTime(): Unit = {
    val compactionLag = 60 * 60 * 1000
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    logProps.put(LogConfig.MinCompactionLagMsProp, compactionLag: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val t0 = time.milliseconds
    while (log.numberOfSegments < 8)
      log.appendAsLeader(records(log.logEndOffset.toInt, log.logEndOffset.toInt, t0), leaderEpoch = 0)

    time.sleep(compactionLag + 1)

    val lastClean = Map(topicPartition -> 0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, topicPartition, lastClean, time.milliseconds)
    assertEquals("The first cleanable offset starts at the beginning of the log.", 0L, cleanableOffsets._1)
    assertEquals("The first uncleanable offset begins with active segment.", log.activeSegment.baseOffset, cleanableOffsets._2)
  }

  @Test
  def testUndecidedTransactionalDataNotCleanable(): Unit = {
    val compactionLag = 60 * 60 * 1000
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    logProps.put(LogConfig.MinCompactionLagMsProp, compactionLag: java.lang.Integer)

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
    log.onHighWatermarkIncremented(3L)

    time.sleep(compactionLag + 1)
    // although the compaction lag has been exceeded, the undecided data should not be cleaned
    var cleanableOffsets = LogCleanerManager.cleanableOffsets(log, topicPartition,
      Map(topicPartition -> 0L), time.milliseconds())
    assertEquals(0L, cleanableOffsets._1)
    assertEquals(0L, cleanableOffsets._2)

    log.appendAsLeader(MemoryRecords.withEndTransactionMarker(time.milliseconds(), producerId, producerEpoch,
      new EndTransactionMarker(ControlRecordType.ABORT, 15)), leaderEpoch = 0, isFromClient = false)
    log.roll()
    log.onHighWatermarkIncremented(4L)

    // the first segment should now become cleanable immediately
    cleanableOffsets = LogCleanerManager.cleanableOffsets(log, topicPartition,
      Map(topicPartition -> 0L), time.milliseconds())
    assertEquals(0L, cleanableOffsets._1)
    assertEquals(3L, cleanableOffsets._2)

    time.sleep(compactionLag + 1)

    // the second segment becomes cleanable after the compaction lag
    cleanableOffsets = LogCleanerManager.cleanableOffsets(log, topicPartition,
      Map(topicPartition -> 0L), time.milliseconds())
    assertEquals(0L, cleanableOffsets._1)
    assertEquals(4L, cleanableOffsets._2)
  }

  @Test
  def testDoneCleaning(): Unit = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))
    while(log.numberOfSegments < 8)
      log.appendAsLeader(records(log.logEndOffset.toInt, log.logEndOffset.toInt, time.milliseconds()), leaderEpoch = 0)

    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    intercept[IllegalStateException](cleanerManager.doneCleaning(topicPartition, log.dir, 1))

    cleanerManager.setCleaningState(topicPartition, LogCleaningPaused(1))
    intercept[IllegalStateException](cleanerManager.doneCleaning(topicPartition, log.dir, 1))

    cleanerManager.setCleaningState(topicPartition, LogCleaningInProgress)
    cleanerManager.doneCleaning(topicPartition, log.dir, 1)
    assertTrue(cleanerManager.cleaningState(topicPartition).isEmpty)
    assertTrue(cleanerManager.allCleanerCheckpoints.get(topicPartition).nonEmpty)

    cleanerManager.setCleaningState(topicPartition, LogCleaningAborted)
    cleanerManager.doneCleaning(topicPartition, log.dir, 1)
    assertEquals(LogCleaningPaused(1), cleanerManager.cleaningState(topicPartition).get)
    assertTrue(cleanerManager.allCleanerCheckpoints.get(topicPartition).nonEmpty)
  }

  @Test
  def testDoneDeleting(): Unit = {
    val records = TestUtils.singletonRecords("test".getBytes, key="test".getBytes)
    val log: Log = createLog(records.sizeInBytes * 5, LogConfig.Compact + "," + LogConfig.Delete)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)
    val tp = new TopicPartition("log", 0)

    intercept[IllegalStateException](cleanerManager.doneDeleting(Seq(tp)))

    cleanerManager.setCleaningState(tp, LogCleaningPaused(1))
    intercept[IllegalStateException](cleanerManager.doneDeleting(Seq(tp)))

    cleanerManager.setCleaningState(tp, LogCleaningInProgress)
    cleanerManager.doneDeleting(Seq(tp))
    assertTrue(cleanerManager.cleaningState(tp).isEmpty)

    cleanerManager.setCleaningState(tp, LogCleaningAborted)
    cleanerManager.doneDeleting(Seq(tp))
    assertEquals(LogCleaningPaused(1), cleanerManager.cleaningState(tp).get)
  }

  private def createCleanerManager(log: Log): LogCleanerManager = {
    val logs = new Pool[TopicPartition, Log]()
    logs.put(topicPartition, log)
    createCleanerManager(logs)
  }

  private def createCleanerManager(pool: Pool[TopicPartition, Log], toMock: Boolean = false): LogCleanerManager = {
    if (toMock)
      new LogCleanerManagerMock(Array(logDir), pool, null)
    else
      new LogCleanerManager(Array(logDir), pool, null)
  }

  private def createLog(segmentSize: Int, cleanupPolicy: String, segmentsCount: Int = 0): Log = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: Integer)
    logProps.put(LogConfig.RetentionMsProp, 1: Integer)
    logProps.put(LogConfig.CleanupPolicyProp, cleanupPolicy)
    logProps.put(LogConfig.MinCleanableDirtyRatioProp, 0.05: java.lang.Double) // small for easier and clearer tests

    val config = LogConfig(logProps)
    val partitionDir = new File(logDir, "log-0")
    val log = Log(partitionDir,
      config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      time = time,
      brokerTopicStats = new BrokerTopicStats,
      maxProducerIdExpirationMs = 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10))
    for (i <- 0 until segmentsCount) {
      val startOffset = i * 10
      val endOffset = startOffset + 10
      val segment = LogUtils.createSegment(startOffset, logDir)
      var lastTimestamp = 0L
      val records = (startOffset until endOffset).map { offset =>
        val currentTimestamp = time.milliseconds()
        if (offset == endOffset - 1)
          lastTimestamp = currentTimestamp

        new SimpleRecord(currentTimestamp, s"key-$offset".getBytes, s"value-$offset".getBytes)
      }

      segment.append(endOffset, lastTimestamp, endOffset, MemoryRecords.withRecords(CompressionType.NONE, records:_*))
      log.addSegment(segment)
    }
    log
  }

  private def makeLog(dir: File = logDir, config: LogConfig) =
    Log(dir = dir, config = config, logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      time = time, brokerTopicStats = new BrokerTopicStats, maxProducerIdExpirationMs = 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10))

  private def records(key: Int, value: Int, timestamp: Long) =
    MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord(timestamp, key.toString.getBytes, value.toString.getBytes))

}
