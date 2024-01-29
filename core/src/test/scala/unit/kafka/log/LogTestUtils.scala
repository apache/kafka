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

import kafka.log.remote.RemoteLogManager

import java.io.File
import java.util.Properties
import kafka.server.BrokerTopicStats
import kafka.utils.TestUtils
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.record.{CompressionType, ControlRecordType, EndTransactionMarker, FileRecords, MemoryRecords, RecordBatch, SimpleRecord}
import org.apache.kafka.common.utils.{Time, Utils}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}

import java.nio.file.Files
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.server.config.Defaults
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile
import org.apache.kafka.storage.internals.log.{AbortedTxn, AppendOrigin, FetchDataInfo, FetchIsolation, LazyIndex, LogAppendInfo, LogConfig, LogDirFailureChannel, LogFileUtils, LogOffsetsListener, LogSegment, ProducerStateManager, ProducerStateManagerConfig, TransactionIndex}

import scala.jdk.CollectionConverters._

object LogTestUtils {
  /**
    *  Create a segment with the given base offset
    */
  def createSegment(offset: Long,
                    logDir: File,
                    indexIntervalBytes: Int = 10,
                    time: Time = Time.SYSTEM): LogSegment = {
    val ms = FileRecords.open(LogFileUtils.logFile(logDir, offset))
    val idx = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(logDir, offset), offset, 1000)
    val timeIdx = LazyIndex.forTime(LogFileUtils.timeIndexFile(logDir, offset), offset, 1500)
    val txnIndex = new TransactionIndex(offset, UnifiedLog.transactionIndexFile(logDir, offset))

    new LogSegment(ms, idx, timeIdx, txnIndex, offset, indexIntervalBytes, 0, time)
  }

  def createLogConfig(segmentMs: Long = LogConfig.DEFAULT_SEGMENT_MS,
                      segmentBytes: Int = LogConfig.DEFAULT_SEGMENT_BYTES,
                      retentionMs: Long = LogConfig.DEFAULT_RETENTION_MS,
                      localRetentionMs: Long = LogConfig.DEFAULT_LOCAL_RETENTION_MS,
                      retentionBytes: Long = LogConfig.DEFAULT_RETENTION_BYTES,
                      localRetentionBytes: Long = LogConfig.DEFAULT_LOCAL_RETENTION_BYTES,
                      segmentJitterMs: Long = LogConfig.DEFAULT_SEGMENT_JITTER_MS,
                      cleanupPolicy: String = LogConfig.DEFAULT_CLEANUP_POLICY,
                      maxMessageBytes: Int = LogConfig.DEFAULT_MAX_MESSAGE_BYTES,
                      indexIntervalBytes: Int = LogConfig.DEFAULT_INDEX_INTERVAL_BYTES,
                      segmentIndexBytes: Int = LogConfig.DEFAULT_SEGMENT_INDEX_BYTES,
                      fileDeleteDelayMs: Long = LogConfig.DEFAULT_FILE_DELETE_DELAY_MS,
                      remoteLogStorageEnable: Boolean = LogConfig.DEFAULT_REMOTE_STORAGE_ENABLE): LogConfig = {
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_MS_CONFIG, segmentMs: java.lang.Long)
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, segmentBytes: Integer)
    logProps.put(TopicConfig.RETENTION_MS_CONFIG, retentionMs: java.lang.Long)
    logProps.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, localRetentionMs: java.lang.Long)
    logProps.put(TopicConfig.RETENTION_BYTES_CONFIG, retentionBytes: java.lang.Long)
    logProps.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, localRetentionBytes: java.lang.Long)
    logProps.put(TopicConfig.SEGMENT_JITTER_MS_CONFIG, segmentJitterMs: java.lang.Long)
    logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicy)
    logProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, maxMessageBytes: Integer)
    logProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, indexIntervalBytes: Integer)
    logProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, segmentIndexBytes: Integer)
    logProps.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, fileDeleteDelayMs: java.lang.Long)
    logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, remoteLogStorageEnable: java.lang.Boolean)
    new LogConfig(logProps)
  }

  def createLog(dir: File,
                config: LogConfig,
                brokerTopicStats: BrokerTopicStats,
                scheduler: Scheduler,
                time: Time,
                logStartOffset: Long = 0L,
                recoveryPoint: Long = 0L,
                maxTransactionTimeoutMs: Int = 5 * 60 * 1000,
                producerStateManagerConfig: ProducerStateManagerConfig = new ProducerStateManagerConfig(Defaults.PRODUCER_ID_EXPIRATION_MS, false),
                producerIdExpirationCheckIntervalMs: Int = Defaults.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS,
                lastShutdownClean: Boolean = true,
                topicId: Option[Uuid] = None,
                keepPartitionMetadataFile: Boolean = true,
                numRemainingSegments: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int],
                remoteStorageSystemEnable: Boolean = false,
                remoteLogManager: Option[RemoteLogManager] = None,
                logOffsetsListener: LogOffsetsListener = LogOffsetsListener.NO_OP_OFFSETS_LISTENER): UnifiedLog = {
    UnifiedLog(
      dir = dir,
      config = config,
      logStartOffset = logStartOffset,
      recoveryPoint = recoveryPoint,
      scheduler = scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time,
      maxTransactionTimeoutMs = maxTransactionTimeoutMs,
      producerStateManagerConfig = producerStateManagerConfig,
      producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10),
      lastShutdownClean = lastShutdownClean,
      topicId = topicId,
      keepPartitionMetadataFile = keepPartitionMetadataFile,
      numRemainingSegments = numRemainingSegments,
      remoteStorageSystemEnable = remoteStorageSystemEnable,
      logOffsetsListener = logOffsetsListener
    )
  }

  /**
   * Check if the given log contains any segment with records that cause offset overflow.
   * @param log Log to check
   * @return true if log contains at least one segment with offset overflow; false otherwise
   */
  def hasOffsetOverflow(log: UnifiedLog): Boolean = firstOverflowSegment(log).isDefined

  def firstOverflowSegment(log: UnifiedLog): Option[LogSegment] = {
    def hasOverflow(baseOffset: Long, batch: RecordBatch): Boolean =
      batch.lastOffset > baseOffset + Int.MaxValue || batch.baseOffset < baseOffset

    for (segment <- log.logSegments.asScala) {
      val overflowBatch = segment.log.batches.asScala.find(batch => hasOverflow(segment.baseOffset, batch))
      if (overflowBatch.isDefined)
        return Some(segment)
    }
    None
  }

  def rawSegment(logDir: File, baseOffset: Long): FileRecords =
    FileRecords.open(LogFileUtils.logFile(logDir, baseOffset))

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
      Files.createFile(LogFileUtils.offsetIndexFile(logDir, baseOffset).toPath)
      Files.createFile(LogFileUtils.timeIndexFile(logDir, baseOffset).toPath)
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
  def keysInLog(log: UnifiedLog): Iterable[Long] = {
    for (logSegment <- log.logSegments.asScala;
         batch <- logSegment.log.batches.asScala if !batch.isControlBatch;
         record <- batch.asScala if record.hasValue && record.hasKey)
      yield TestUtils.readString(record.key).toLong
  }

  def recoverAndCheck(logDir: File, config: LogConfig, expectedKeys: Iterable[Long], brokerTopicStats: BrokerTopicStats, time: Time, scheduler: Scheduler): UnifiedLog = {
    // Recover log file and check that after recovery, keys are as expected
    // and all temporary files have been deleted
    val recoveredLog = createLog(logDir, config, brokerTopicStats, scheduler, time, lastShutdownClean = false)
    time.sleep(config.fileDeleteDelayMs + 1)
    for (file <- logDir.listFiles) {
      assertFalse(file.getName.endsWith(LogFileUtils.DELETED_FILE_SUFFIX), "Unexpected .deleted file after recovery")
      assertFalse(file.getName.endsWith(UnifiedLog.CleanedFileSuffix), "Unexpected .cleaned file after recovery")
      assertFalse(file.getName.endsWith(UnifiedLog.SwapFileSuffix), "Unexpected .swap file after recovery")
    }
    assertEquals(expectedKeys, keysInLog(recoveredLog))
    assertFalse(hasOffsetOverflow(recoveredLog))
    recoveredLog
  }

  def appendEndTxnMarkerAsLeader(log: UnifiedLog,
                                 producerId: Long,
                                 producerEpoch: Short,
                                 controlType: ControlRecordType,
                                 timestamp: Long,
                                 coordinatorEpoch: Int = 0,
                                 leaderEpoch: Int = 0): LogAppendInfo = {
    val records = endTxnRecords(controlType, producerId, producerEpoch,
      coordinatorEpoch = coordinatorEpoch, timestamp = timestamp)
    log.appendAsLeader(records, origin = AppendOrigin.COORDINATOR, leaderEpoch = leaderEpoch)
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

  def readLog(log: UnifiedLog,
              startOffset: Long,
              maxLength: Int,
              isolation: FetchIsolation = FetchIsolation.LOG_END,
              minOneMessage: Boolean = true): FetchDataInfo = {
    log.read(startOffset, maxLength, isolation, minOneMessage)
  }

  def allAbortedTransactions(log: UnifiedLog): Iterable[AbortedTxn] =
    log.logSegments.asScala.flatMap(_.txnIndex.allAbortedTxns.asScala)

  def deleteProducerSnapshotFiles(logDir: File): Unit = {
    val files = logDir.listFiles.filter(f => f.isFile && f.getName.endsWith(LogFileUtils.PRODUCER_SNAPSHOT_FILE_SUFFIX))
    files.foreach(Utils.delete)
  }

  def listProducerSnapshotOffsets(logDir: File): Seq[Long] =
    ProducerStateManager.listSnapshotFiles(logDir).asScala.map(_.offset).sorted.toSeq

  def assertLeaderEpochCacheEmpty(log: UnifiedLog): Unit = {
    assertEquals(None, log.leaderEpochCache)
    assertEquals(None, log.latestEpoch)
    assertFalse(LeaderEpochCheckpointFile.newFile(log.dir).exists())
  }

  def appendNonTransactionalAsLeader(log: UnifiedLog, numRecords: Int): Unit = {
    val simpleRecords = (0 until numRecords).map { seq =>
      new SimpleRecord(s"$seq".getBytes)
    }
    val records = MemoryRecords.withRecords(CompressionType.NONE, simpleRecords: _*)
    log.appendAsLeader(records, leaderEpoch = 0)
  }

  def appendTransactionalAsLeader(log: UnifiedLog,
                                  producerId: Long,
                                  producerEpoch: Short,
                                  time: Time): Int => Unit = {
    appendIdempotentAsLeader(log, producerId, producerEpoch, time, isTransactional = true)
  }

  def appendIdempotentAsLeader(log: UnifiedLog,
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
