/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import kafka.server.checkpoints.LeaderEpochCheckpointFile
import kafka.server.{BrokerTopicStats, FetchDataInfo, FetchIsolation, FetchLogEnd, LogDirFailureChannel}
import kafka.utils.{Scheduler, TestUtils}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.record.{CompressionType, ControlRecordType, EndTransactionMarker, FileRecords, MemoryRecords, RecordBatch, SimpleRecord}
import org.apache.kafka.common.utils.{Time, Utils}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}

import scala.collection.Iterable
import scala.jdk.CollectionConverters._

object LogTestUtils {
  /**
    *  Create a segment with the given base offset
    */
  def createSegment(offset: Long,
                    logDir: File,
                    indexIntervalBytes: Int = 10,
                    time: Time = Time.SYSTEM): LogSegment = {
    val ms = FileRecords.open(Log.logFile(logDir, offset))
    val idx = LazyIndex.forOffset(Log.offsetIndexFile(logDir, offset), offset, maxIndexSize = 1000)
    val timeIdx = LazyIndex.forTime(Log.timeIndexFile(logDir, offset), offset, maxIndexSize = 1500)
    val txnIndex = new TransactionIndex(offset, Log.transactionIndexFile(logDir, offset))

    new LogSegment(ms, idx, timeIdx, txnIndex, offset, indexIntervalBytes, 0, time)
  }

  def createLogConfig(segmentMs: Long = Defaults.SegmentMs,
                      segmentBytes: Int = Defaults.SegmentSize,
                      retentionMs: Long = Defaults.RetentionMs,
                      retentionBytes: Long = Defaults.RetentionSize,
                      segmentJitterMs: Long = Defaults.SegmentJitterMs,
                      cleanupPolicy: String = Defaults.CleanupPolicy,
                      maxMessageBytes: Int = Defaults.MaxMessageSize,
                      indexIntervalBytes: Int = Defaults.IndexInterval,
                      segmentIndexBytes: Int = Defaults.MaxIndexSize,
                      messageFormatVersion: String = Defaults.MessageFormatVersion,
                      fileDeleteDelayMs: Long = Defaults.FileDeleteDelayMs): LogConfig = {
    val logProps = new Properties()

    logProps.put(LogConfig.SegmentMsProp, segmentMs: java.lang.Long)
    logProps.put(LogConfig.SegmentBytesProp, segmentBytes: Integer)
    logProps.put(LogConfig.RetentionMsProp, retentionMs: java.lang.Long)
    logProps.put(LogConfig.RetentionBytesProp, retentionBytes: java.lang.Long)
    logProps.put(LogConfig.SegmentJitterMsProp, segmentJitterMs: java.lang.Long)
    logProps.put(LogConfig.CleanupPolicyProp, cleanupPolicy)
    logProps.put(LogConfig.MaxMessageBytesProp, maxMessageBytes: Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, indexIntervalBytes: Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, segmentIndexBytes: Integer)
    logProps.put(LogConfig.MessageFormatVersionProp, messageFormatVersion)
    logProps.put(LogConfig.FileDeleteDelayMsProp, fileDeleteDelayMs: java.lang.Long)
    LogConfig(logProps)
  }

  def createLog(dir: File,
                config: LogConfig,
                brokerTopicStats: BrokerTopicStats,
                scheduler: Scheduler,
                time: Time,
                logStartOffset: Long = 0L,
                recoveryPoint: Long = 0L,
                maxProducerIdExpirationMs: Int = 60 * 60 * 1000,
                producerIdExpirationCheckIntervalMs: Int = LogManager.ProducerIdExpirationCheckIntervalMs,
                lastShutdownClean: Boolean = true,
                topicId: Option[Uuid] = None): Log = {
    Log(dir = dir,
      config = config,
      logStartOffset = logStartOffset,
      recoveryPoint = recoveryPoint,
      scheduler = scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10),
      lastShutdownClean = lastShutdownClean,
      topicId = topicId,
      keepPartitionMetadataFile = true)
  }

  /**
   * Check if the given log contains any segment with records that cause offset overflow.
   * @param log Log to check
   * @return true if log contains at least one segment with offset overflow; false otherwise
   */
  def hasOffsetOverflow(log: Log): Boolean = firstOverflowSegment(log).isDefined

  def firstOverflowSegment(log: Log): Option[LogSegment] = {
    def hasOverflow(baseOffset: Long, batch: RecordBatch): Boolean =
      batch.lastOffset > baseOffset + Int.MaxValue || batch.baseOffset < baseOffset

    for (segment <- log.logSegments) {
      val overflowBatch = segment.log.batches.asScala.find(batch => hasOverflow(segment.baseOffset, batch))
      if (overflowBatch.isDefined)
        return Some(segment)
    }
    None
  }

  private def rawSegment(logDir: File, baseOffset: Long): FileRecords =
    FileRecords.open(Log.logFile(logDir, baseOffset))

  /**
   * Initialize the given log directory with a set of segments, one of which will have an
   * offset which overflows the segment
   */
  def initializeLogDirWithOverflowedSegment(logDir: File): Unit = {
    def writeSampleBatches(baseOffset: Long, segment: FileRecords): Long = {
      def record(offset: Long) = {
        val data = offset.toString.getBytes
        new SimpleRecord(data, data)
      }

      segment.append(MemoryRecords.withRecords(baseOffset, CompressionType.NONE, 0,
        record(baseOffset)))
      segment.append(MemoryRecords.withRecords(baseOffset + 1, CompressionType.NONE, 0,
        record(baseOffset + 1),
        record(baseOffset + 2)))
      segment.append(MemoryRecords.withRecords(baseOffset + Int.MaxValue - 1, CompressionType.NONE, 0,
        record(baseOffset + Int.MaxValue - 1)))
      // Need to create the offset files explicitly to avoid triggering segment recovery to truncate segment.
      Log.offsetIndexFile(logDir, baseOffset).createNewFile()
      Log.timeIndexFile(logDir, baseOffset).createNewFile()
      baseOffset + Int.MaxValue
    }

    def writeNormalSegment(baseOffset: Long): Long = {
      val segment = rawSegment(logDir, baseOffset)
      try writeSampleBatches(baseOffset, segment)
      finally segment.close()
    }

    def writeOverflowSegment(baseOffset: Long): Long = {
      val segment = rawSegment(logDir, baseOffset)
      try {
        val nextOffset = writeSampleBatches(baseOffset, segment)
        writeSampleBatches(nextOffset, segment)
      } finally segment.close()
    }

    // We create three segments, the second of which contains offsets which overflow
    var nextOffset = 0L
    nextOffset = writeNormalSegment(nextOffset)
    nextOffset = writeOverflowSegment(nextOffset)
    writeNormalSegment(nextOffset)
  }

  /* extract all the keys from a log */
  def keysInLog(log: Log): Iterable[Long] = {
    for (logSegment <- log.logSegments;
         batch <- logSegment.log.batches.asScala if !batch.isControlBatch;
         record <- batch.asScala if record.hasValue && record.hasKey)
      yield TestUtils.readString(record.key).toLong
  }

  def recoverAndCheck(logDir: File, config: LogConfig, expectedKeys: Iterable[Long], brokerTopicStats: BrokerTopicStats, time: Time, scheduler: Scheduler): Log = {
    // Recover log file and check that after recovery, keys are as expected
    // and all temporary files have been deleted
    val recoveredLog = createLog(logDir, config, brokerTopicStats, scheduler, time, lastShutdownClean = false)
    time.sleep(config.fileDeleteDelayMs + 1)
    for (file <- logDir.listFiles) {
      assertFalse(file.getName.endsWith(Log.DeletedFileSuffix), "Unexpected .deleted file after recovery")
      assertFalse(file.getName.endsWith(Log.CleanedFileSuffix), "Unexpected .cleaned file after recovery")
      assertFalse(file.getName.endsWith(Log.SwapFileSuffix), "Unexpected .swap file after recovery")
    }
    assertEquals(expectedKeys, LogTest.keysInLog(recoveredLog))
    assertFalse(LogTest.hasOffsetOverflow(recoveredLog))
    recoveredLog
  }

  def appendEndTxnMarkerAsLeader(log: Log,
                                 producerId: Long,
                                 producerEpoch: Short,
                                 controlType: ControlRecordType,
                                 timestamp: Long,
                                 coordinatorEpoch: Int = 0,
                                 leaderEpoch: Int = 0): LogAppendInfo = {
    val records = endTxnRecords(controlType, producerId, producerEpoch,
      coordinatorEpoch = coordinatorEpoch, timestamp = timestamp)
    log.appendAsLeader(records, origin = AppendOrigin.Coordinator, leaderEpoch = leaderEpoch)
  }

  private def endTxnRecords(controlRecordType: ControlRecordType,
                            producerId: Long,
                            epoch: Short,
                            offset: Long = 0L,
                            coordinatorEpoch: Int,
                            partitionLeaderEpoch: Int = 0,
                            timestamp: Long): MemoryRecords = {
    val marker = new EndTransactionMarker(controlRecordType, coordinatorEpoch)
    MemoryRecords.withEndTransactionMarker(offset, timestamp, partitionLeaderEpoch, producerId, epoch, marker)
  }

  def readLog(log: Log,
                      startOffset: Long,
                      maxLength: Int,
                      isolation: FetchIsolation = FetchLogEnd,
                      minOneMessage: Boolean = true): FetchDataInfo = {
    log.read(startOffset, maxLength, isolation, minOneMessage)
  }

  def allAbortedTransactions(log: Log): Iterable[AbortedTxn] = log.logSegments.flatMap(_.txnIndex.allAbortedTxns)

  def deleteProducerSnapshotFiles(logDir: File): Unit = {
    val files = logDir.listFiles.filter(f => f.isFile && f.getName.endsWith(Log.ProducerSnapshotFileSuffix))
    files.foreach(Utils.delete)
  }

  def listProducerSnapshotOffsets(logDir: File): Seq[Long] =
    ProducerStateManager.listSnapshotFiles(logDir).map(_.offset).sorted

  def assertLeaderEpochCacheEmpty(log: Log): Unit = {
    assertEquals(None, log.leaderEpochCache)
    assertEquals(None, log.latestEpoch)
    assertFalse(LeaderEpochCheckpointFile.newFile(log.dir).exists())
  }

  def appendNonTransactionalAsLeader(log: Log, numRecords: Int): Unit = {
    val simpleRecords = (0 until numRecords).map { seq =>
      new SimpleRecord(s"$seq".getBytes)
    }
    val records = MemoryRecords.withRecords(CompressionType.NONE, simpleRecords: _*)
    log.appendAsLeader(records, leaderEpoch = 0)
  }

  def appendTransactionalAsLeader(log: Log,
                                  producerId: Long,
                                  producerEpoch: Short,
                                  time: Time): Int => Unit = {
    appendIdempotentAsLeader(log, producerId, producerEpoch, time, isTransactional = true)
  }

  def appendIdempotentAsLeader(log: Log,
                               producerId: Long,
                               producerEpoch: Short,
                               time: Time,
                               isTransactional: Boolean = false): Int => Unit = {
    var sequence = 0
    numRecords: Int => {
      val simpleRecords = (sequence until sequence + numRecords).map { seq =>
        new SimpleRecord(time.milliseconds(), s"$seq".getBytes)
      }

      val records = if (isTransactional) {
        MemoryRecords.withTransactionalRecords(CompressionType.NONE, producerId,
          producerEpoch, sequence, simpleRecords: _*)
      } else {
        MemoryRecords.withIdempotentRecords(CompressionType.NONE, producerId,
          producerEpoch, sequence, simpleRecords: _*)
      }

      log.appendAsLeader(records, leaderEpoch = 0)
      sequence += numRecords
    }
  }
}
