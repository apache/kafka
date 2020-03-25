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

import java.io.{File, RandomAccessFile}
import java.nio._
import java.nio.file.Paths
import java.util.Properties
import java.util.concurrent.{CountDownLatch, TimeUnit}

import kafka.common._
import kafka.server.{BrokerTopicStats, LogDirFailureChannel}
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Utils
import org.junit.Assert._
import org.junit.{After, Test}
import org.scalatest.junit.JUnitSuite

import scala.collection.JavaConverters._
import scala.collection._

/**
 * Unit tests for the log cleaning logic
 */
class LogCleanerTest extends JUnitSuite {

  val tmpdir = TestUtils.tempDir()
  val dir = TestUtils.randomPartitionLogDir(tmpdir)
  val logProps = new Properties()
  logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
  logProps.put(LogConfig.SegmentIndexBytesProp, 1024: java.lang.Integer)
  logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
  logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp, Long.MaxValue.toString)
  val logConfig = LogConfig(logProps)
  val time = new MockTime()
  val throttler = new Throttler(desiredRatePerSec = Double.MaxValue, checkIntervalMs = Long.MaxValue, time = time)

  @After
  def teardown(): Unit = {
    Utils.delete(tmpdir)
  }

  /**
   * Test simple log cleaning
   */
  @Test
  def testCleanSegments(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    // append messages to the log until we have four segments
    while(log.numberOfSegments < 4)
      log.appendAsLeader(record(log.logEndOffset.toInt, log.logEndOffset.toInt), leaderEpoch = 0)
    val keysFound = LogTest.keysInLog(log)
    assertEquals(0L until log.logEndOffset, keysFound)

    // pretend we have the following keys
    val keys = immutable.ListSet(1L, 3L, 5L, 7L, 9L)
    val map = new FakeOffsetMap(Int.MaxValue)
    keys.foreach(k => map.put(key(k), Long.MaxValue))

    // clean the log
    val segments = log.logSegments.take(3).toSeq
    val stats = new CleanerStats()
    val expectedBytesRead = segments.map(_.size).sum
    cleaner.cleanSegments(log, segments, map, 0L, stats)
    val shouldRemain = LogTest.keysInLog(log).filter(!keys.contains(_))
    assertEquals(shouldRemain, LogTest.keysInLog(log))
    assertEquals(expectedBytesRead, stats.bytesRead)
  }

  @Test
  def testCleanSegmentsWithConcurrentSegmentDeletion(): Unit = {
    val deleteStartLatch = new CountDownLatch(1)
    val deleteCompleteLatch = new CountDownLatch(1)

    // Construct a log instance. The replaceSegments() method of the log instance is overridden so that
    // it waits for another thread to execute deleteOldSegments()
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024 : java.lang.Integer)
    logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact + "," + LogConfig.Delete)
    val topicPartition = Log.parseTopicPartitionName(dir)
    val producerStateManager = new ProducerStateManager(topicPartition, dir)
    val log = new Log(dir,
                      config = LogConfig.fromProps(logConfig.originals, logProps),
                      logStartOffset = 0L,
                      recoveryPoint = 0L,
                      scheduler = time.scheduler,
                      brokerTopicStats = new BrokerTopicStats, time,
                      maxProducerIdExpirationMs = 60 * 60 * 1000,
                      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
                      topicPartition = topicPartition,
                      producerStateManager = producerStateManager,
                      logDirFailureChannel = new LogDirFailureChannel(10)) {
      override def replaceSegments(newSegments: Seq[LogSegment], oldSegments: Seq[LogSegment], isRecoveredSwapFile: Boolean = false): Unit = {
        deleteStartLatch.countDown()
        if (!deleteCompleteLatch.await(5000, TimeUnit.MILLISECONDS)) {
          throw new IllegalStateException("Log segment deletion timed out")
        }
        super.replaceSegments(newSegments, oldSegments, isRecoveredSwapFile)
      }
    }

    // Start a thread which execute log.deleteOldSegments() right before replaceSegments() is executed
    val t = new Thread() {
      override def run(): Unit = {
        deleteStartLatch.await(5000, TimeUnit.MILLISECONDS)
        log.maybeIncrementLogStartOffset(log.activeSegment.baseOffset)
        log.onHighWatermarkIncremented(log.activeSegment.baseOffset)
        log.deleteOldSegments()
        deleteCompleteLatch.countDown()
      }
    }
    t.start()

    // Append records so that segment number increase to 3
    while (log.numberOfSegments < 3) {
      log.appendAsLeader(record(key = 0, log.logEndOffset.toInt), leaderEpoch = 0)
      log.roll()
    }
    assertEquals(3, log.numberOfSegments)

    // Remember reference to the first log and determine its file name expected for async deletion
    val firstLogFile = log.logSegments.head.log
    val expectedFileName = CoreUtils.replaceSuffix(firstLogFile.file.getPath, "", Log.DeletedFileSuffix)

    // Clean the log. This should trigger replaceSegments() and deleteOldSegments();
    val offsetMap = new FakeOffsetMap(Int.MaxValue)
    val cleaner = makeCleaner(Int.MaxValue)
    val segments = log.logSegments(0, log.activeSegment.baseOffset).toSeq
    val stats = new CleanerStats()
    cleaner.buildOffsetMap(log, 0, log.activeSegment.baseOffset, offsetMap, stats)
    cleaner.cleanSegments(log, segments, offsetMap, 0L, stats)

    // Validate based on the file name that log segment file is renamed exactly once for async deletion
    assertEquals(expectedFileName, firstLogFile.file().getPath)
    assertEquals(2, log.numberOfSegments)
  }

  @Test
  def testSizeTrimmedForPreallocatedAndCompactedTopic(): Unit = {
    val originalMaxFileSize = 1024;
    val cleaner = makeCleaner(2)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, originalMaxFileSize: java.lang.Integer)
    logProps.put(LogConfig.CleanupPolicyProp, "compact": java.lang.String)
    logProps.put(LogConfig.PreAllocateEnableProp, "true": java.lang.String)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    log.appendAsLeader(record(0,0), leaderEpoch = 0) // offset 0
    log.appendAsLeader(record(1,1), leaderEpoch = 0) // offset 1
    log.appendAsLeader(record(0,0), leaderEpoch = 0) // offset 2
    log.appendAsLeader(record(1,1), leaderEpoch = 0) // offset 3
    log.appendAsLeader(record(0,0), leaderEpoch = 0) // offset 4
    // roll the segment, so we can clean the messages already appended
    log.roll()

    // clean the log with only one message removed
    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 2, log.activeSegment.baseOffset))

    assertTrue("Cleaned segment file should be trimmed to its real size.",
      log.logSegments.iterator.next.log.channel().size() < originalMaxFileSize)
  }

  @Test
  def testDuplicateCheckAfterCleaning(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 2048: java.lang.Integer)
    var log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val producerEpoch = 0.toShort
    val pid1 = 1
    val pid2 = 2
    val pid3 = 3
    val pid4 = 4

    appendIdempotentAsLeader(log, pid1, producerEpoch)(Seq(1, 2, 3))
    appendIdempotentAsLeader(log, pid2, producerEpoch)(Seq(3, 1, 4))
    appendIdempotentAsLeader(log, pid3, producerEpoch)(Seq(1, 4))

    log.roll()
    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 0L, log.activeSegment.baseOffset))
    assertEquals(List(2, 5, 7), lastOffsetsPerBatchInLog(log))
    assertEquals(Map(pid1 -> 2, pid2 -> 2, pid3 -> 1), lastSequencesInLog(log))
    assertEquals(List(2, 3, 1, 4), LogTest.keysInLog(log))
    assertEquals(List(1, 3, 6, 7), offsetsInLog(log))

    // we have to reload the log to validate that the cleaner maintained sequence numbers correctly
    def reloadLog(): Unit = {
      log.close()
      log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps), recoveryPoint = 0L)
    }

    reloadLog()

    // check duplicate append from producer 1
    var logAppendInfo = appendIdempotentAsLeader(log, pid1, producerEpoch)(Seq(1, 2, 3))
    assertEquals(0L, logAppendInfo.firstOffset.get)
    assertEquals(2L, logAppendInfo.lastOffset)

    // check duplicate append from producer 3
    logAppendInfo = appendIdempotentAsLeader(log, pid3, producerEpoch)(Seq(1, 4))
    assertEquals(6L, logAppendInfo.firstOffset.get)
    assertEquals(7L, logAppendInfo.lastOffset)

    // check duplicate append from producer 2
    logAppendInfo = appendIdempotentAsLeader(log, pid2, producerEpoch)(Seq(3, 1, 4))
    assertEquals(3L, logAppendInfo.firstOffset.get)
    assertEquals(5L, logAppendInfo.lastOffset)

    // do one more append and a round of cleaning to force another deletion from producer 1's batch
    appendIdempotentAsLeader(log, pid4, producerEpoch)(Seq(2))
    log.roll()
    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 0L, log.activeSegment.baseOffset))
    assertEquals(Map(pid1 -> 2, pid2 -> 2, pid3 -> 1, pid4 -> 0), lastSequencesInLog(log))
    assertEquals(List(2, 5, 7, 8), lastOffsetsPerBatchInLog(log))
    assertEquals(List(3, 1, 4, 2), LogTest.keysInLog(log))
    assertEquals(List(3, 6, 7, 8), offsetsInLog(log))

    reloadLog()

    // duplicate append from producer1 should still be fine
    logAppendInfo = appendIdempotentAsLeader(log, pid1, producerEpoch)(Seq(1, 2, 3))
    assertEquals(0L, logAppendInfo.firstOffset.get)
    assertEquals(2L, logAppendInfo.lastOffset)
  }

  @Test
  def testBasicTransactionAwareCleaning(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 2048: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val producerEpoch = 0.toShort
    val pid1 = 1
    val pid2 = 2

    val appendProducer1 = appendTransactionalAsLeader(log, pid1, producerEpoch)
    val appendProducer2 = appendTransactionalAsLeader(log, pid2, producerEpoch)

    appendProducer1(Seq(1, 2))
    appendProducer2(Seq(2, 3))
    appendProducer1(Seq(3, 4))
    log.appendAsLeader(abortMarker(pid1, producerEpoch), leaderEpoch = 0, isFromClient = false)
    log.appendAsLeader(commitMarker(pid2, producerEpoch), leaderEpoch = 0, isFromClient = false)
    appendProducer1(Seq(2))
    log.appendAsLeader(commitMarker(pid1, producerEpoch), leaderEpoch = 0, isFromClient = false)

    val abortedTransactions = log.collectAbortedTransactions(log.logStartOffset, log.logEndOffset)

    log.roll()
    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 0L, log.activeSegment.baseOffset))
    assertEquals(List(3, 2), LogTest.keysInLog(log))
    assertEquals(List(3, 6, 7, 8, 9), offsetsInLog(log))

    // ensure the transaction index is still correct
    assertEquals(abortedTransactions, log.collectAbortedTransactions(log.logStartOffset, log.logEndOffset))
  }

  @Test
  def testCleanWithTransactionsSpanningSegments(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val producerEpoch = 0.toShort
    val pid1 = 1
    val pid2 = 2
    val pid3 = 3

    val appendProducer1 = appendTransactionalAsLeader(log, pid1, producerEpoch)
    val appendProducer2 = appendTransactionalAsLeader(log, pid2, producerEpoch)
    val appendProducer3 = appendTransactionalAsLeader(log, pid3, producerEpoch)

    appendProducer1(Seq(1, 2))
    appendProducer3(Seq(2, 3))
    appendProducer2(Seq(3, 4))

    log.roll()

    appendProducer2(Seq(5, 6))
    appendProducer3(Seq(6, 7))
    appendProducer1(Seq(7, 8))
    log.appendAsLeader(abortMarker(pid2, producerEpoch), leaderEpoch = 0, isFromClient = false)
    appendProducer3(Seq(8, 9))
    log.appendAsLeader(commitMarker(pid3, producerEpoch), leaderEpoch = 0, isFromClient = false)
    appendProducer1(Seq(9, 10))
    log.appendAsLeader(abortMarker(pid1, producerEpoch), leaderEpoch = 0, isFromClient = false)

    // we have only cleaned the records in the first segment
    val dirtyOffset = cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 0L, log.activeSegment.baseOffset))._1
    assertEquals(List(2, 3, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10), LogTest.keysInLog(log))

    log.roll()

    // append a couple extra segments in the new segment to ensure we have sequence numbers
    appendProducer2(Seq(11))
    appendProducer1(Seq(12))

    // finally only the keys from pid3 should remain
    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, dirtyOffset, log.activeSegment.baseOffset))
    assertEquals(List(2, 3, 6, 7, 8, 9, 11, 12), LogTest.keysInLog(log))
  }

  @Test
  def testCommitMarkerRemoval(): Unit = {
    val tp = new TopicPartition("test", 0)
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 256: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val producerEpoch = 0.toShort
    val producerId = 1L
    val appendProducer = appendTransactionalAsLeader(log, producerId, producerEpoch)

    appendProducer(Seq(1))
    appendProducer(Seq(2, 3))
    log.appendAsLeader(commitMarker(producerId, producerEpoch), leaderEpoch = 0, isFromClient = false)
    appendProducer(Seq(2))
    log.appendAsLeader(commitMarker(producerId, producerEpoch), leaderEpoch = 0, isFromClient = false)
    log.roll()

    // cannot remove the marker in this pass because there are still valid records
    var dirtyOffset = cleaner.doClean(LogToClean(tp, log, 0L, 100L), deleteHorizonMs = Long.MaxValue)._1
    assertEquals(List(1, 3, 2), LogTest.keysInLog(log))
    assertEquals(List(0, 2, 3, 4, 5), offsetsInLog(log))

    appendProducer(Seq(1, 3))
    log.appendAsLeader(commitMarker(producerId, producerEpoch), leaderEpoch = 0, isFromClient = false)
    log.roll()

    // the first cleaning preserves the commit marker (at offset 3) since there were still records for the transaction
    dirtyOffset = cleaner.doClean(LogToClean(tp, log, dirtyOffset, 100L), deleteHorizonMs = Long.MaxValue)._1
    assertEquals(List(2, 1, 3), LogTest.keysInLog(log))
    assertEquals(List(3, 4, 5, 6, 7, 8), offsetsInLog(log))

    // delete horizon forced to 0 to verify marker is not removed early
    dirtyOffset = cleaner.doClean(LogToClean(tp, log, dirtyOffset, 100L), deleteHorizonMs = 0L)._1
    assertEquals(List(2, 1, 3), LogTest.keysInLog(log))
    assertEquals(List(3, 4, 5, 6, 7, 8), offsetsInLog(log))

    // clean again with large delete horizon and verify the marker is removed
    dirtyOffset = cleaner.doClean(LogToClean(tp, log, dirtyOffset, 100L), deleteHorizonMs = Long.MaxValue)._1
    assertEquals(List(2, 1, 3), LogTest.keysInLog(log))
    assertEquals(List(4, 5, 6, 7, 8), offsetsInLog(log))
  }

  /**
   * Tests log cleaning with batches that are deleted where no additional messages
   * are available to read in the buffer. Cleaning should continue from the next offset.
   */
  @Test
  def testDeletedBatchesWithNoMessagesRead(): Unit = {
    val tp = new TopicPartition("test", 0)
    val cleaner = makeCleaner(capacity = Int.MaxValue, maxMessageSize = 100)
    val logProps = new Properties()
    logProps.put(LogConfig.MaxMessageBytesProp, 100: java.lang.Integer)
    logProps.put(LogConfig.SegmentBytesProp, 1000: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val producerEpoch = 0.toShort
    val producerId = 1L
    val appendProducer = appendTransactionalAsLeader(log, producerId, producerEpoch)

    appendProducer(Seq(1))
    log.appendAsLeader(abortMarker(producerId, producerEpoch), leaderEpoch = 0, isFromClient = false)
    appendProducer(Seq(2))
    appendProducer(Seq(2))
    log.appendAsLeader(commitMarker(producerId, producerEpoch), leaderEpoch = 0, isFromClient = false)
    log.roll()

    cleaner.doClean(LogToClean(tp, log, 0L, 100L), deleteHorizonMs = Long.MaxValue)
    assertEquals(List(2), LogTest.keysInLog(log))
    assertEquals(List(1, 3, 4), offsetsInLog(log))

    cleaner.doClean(LogToClean(tp, log, 0L, 100L), deleteHorizonMs = Long.MaxValue)
    assertEquals(List(2), LogTest.keysInLog(log))
    assertEquals(List(3, 4), offsetsInLog(log))
  }

  @Test
  def testCommitMarkerRetentionWithEmptyBatch(): Unit = {
    val tp = new TopicPartition("test", 0)
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 256: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val producerEpoch = 0.toShort
    val producer1 = appendTransactionalAsLeader(log, 1L, producerEpoch)
    val producer2 = appendTransactionalAsLeader(log, 2L, producerEpoch)

    // [{Producer1: 2, 3}]
    producer1(Seq(2, 3)) // offsets 0, 1
    log.roll()

    // [{Producer1: 2, 3}], [{Producer2: 2, 3}, {Producer2: Commit}]
    producer2(Seq(2, 3)) // offsets 2, 3
    log.appendAsLeader(commitMarker(2L, producerEpoch), leaderEpoch = 0, isFromClient = false) // offset 4
    log.roll()

    // [{Producer1: 2, 3}], [{Producer2: 2, 3}, {Producer2: Commit}], [{2}, {3}, {Producer1: Commit}]
    //  {0, 1},              {2, 3},            {4},                   {5}, {6}, {7} ==> Offsets
    log.appendAsLeader(record(2, 2), leaderEpoch = 0) // offset 5
    log.appendAsLeader(record(3, 3), leaderEpoch = 0) // offset 6
    log.appendAsLeader(commitMarker(1L, producerEpoch), leaderEpoch = 0, isFromClient = false) // offset 7
    log.roll()

    // first time through the records are removed
    // Expected State: [{Producer1: EmptyBatch}, {Producer2: EmptyBatch}, {Producer2: Commit}, {2}, {3}]
    var dirtyOffset = cleaner.doClean(LogToClean(tp, log, 0L, 100L), deleteHorizonMs = Long.MaxValue)._1
    assertEquals(List(2, 3), LogTest.keysInLog(log))
    assertEquals(List(4, 5, 6), offsetsInLog(log))
    assertEquals(List(1, 3, 4, 5, 6), lastOffsetsPerBatchInLog(log))

    // the empty batch remains if cleaned again because it still holds the last sequence
    // Expected State: [{Producer1: EmptyBatch}, {Producer2: EmptyBatch}, {Producer2: Commit}, {2}, {3}]
    dirtyOffset = cleaner.doClean(LogToClean(tp, log, dirtyOffset, 100L), deleteHorizonMs = Long.MaxValue)._1
    assertEquals(List(2, 3), LogTest.keysInLog(log))
    assertEquals(List(4, 5, 6), offsetsInLog(log))
    assertEquals(List(1, 3, 4, 5, 6), lastOffsetsPerBatchInLog(log))

    // append a new record from the producer to allow cleaning of the empty batch
    // [{Producer1: EmptyBatch}, {Producer2: EmptyBatch}, {Producer2: Commit}, {2}, {3}] [{Producer2: 1}, {Producer2: Commit}]
    //  {1},                     {3},                     {4},                 {5}, {6},  {8},            {9} ==> Offsets
    producer2(Seq(1)) // offset 8
    log.appendAsLeader(commitMarker(2L, producerEpoch), leaderEpoch = 0, isFromClient = false) // offset 9
    log.roll()

    // Expected State: [{Producer1: EmptyBatch}, {Producer2: Commit}, {2}, {3}, {Producer2: 1}, {Producer2: Commit}]
    dirtyOffset = cleaner.doClean(LogToClean(tp, log, dirtyOffset, 100L), deleteHorizonMs = Long.MaxValue)._1
    assertEquals(List(2, 3, 1), LogTest.keysInLog(log))
    assertEquals(List(4, 5, 6, 8, 9), offsetsInLog(log))
    assertEquals(List(1, 4, 5, 6, 8, 9), lastOffsetsPerBatchInLog(log))

    // Expected State: [{Producer1: EmptyBatch}, {2}, {3}, {Producer2: 1}, {Producer2: Commit}]
    dirtyOffset = cleaner.doClean(LogToClean(tp, log, dirtyOffset, 100L), deleteHorizonMs = Long.MaxValue)._1
    assertEquals(List(2, 3, 1), LogTest.keysInLog(log))
    assertEquals(List(5, 6, 8, 9), offsetsInLog(log))
    assertEquals(List(1, 5, 6, 8, 9), lastOffsetsPerBatchInLog(log))
  }

  @Test
  def testCleanEmptyControlBatch(): Unit = {
    val tp = new TopicPartition("test", 0)
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 256: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val producerEpoch = 0.toShort

    // [{Producer1: Commit}, {2}, {3}]
    log.appendAsLeader(commitMarker(1L, producerEpoch), leaderEpoch = 0, isFromClient = false) // offset 7
    log.appendAsLeader(record(2, 2), leaderEpoch = 0) // offset 2
    log.appendAsLeader(record(3, 3), leaderEpoch = 0) // offset 3
    log.roll()

    // first time through the control batch is retained as an empty batch
    // Expected State: [{Producer1: EmptyBatch}], [{2}, {3}]
    var dirtyOffset = cleaner.doClean(LogToClean(tp, log, 0L, 100L), deleteHorizonMs = Long.MaxValue)._1
    assertEquals(List(2, 3), LogTest.keysInLog(log))
    assertEquals(List(1, 2), offsetsInLog(log))
    assertEquals(List(0, 1, 2), lastOffsetsPerBatchInLog(log))

    // the empty control batch does not cause an exception when cleaned
    // Expected State: [{Producer1: EmptyBatch}], [{2}, {3}]
    dirtyOffset = cleaner.doClean(LogToClean(tp, log, dirtyOffset, 100L), deleteHorizonMs = Long.MaxValue)._1
    assertEquals(List(2, 3), LogTest.keysInLog(log))
    assertEquals(List(1, 2), offsetsInLog(log))
    assertEquals(List(0, 1, 2), lastOffsetsPerBatchInLog(log))
  }

  @Test
  def testAbortMarkerRemoval(): Unit = {
    val tp = new TopicPartition("test", 0)
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 256: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val producerEpoch = 0.toShort
    val producerId = 1L
    val appendProducer = appendTransactionalAsLeader(log, producerId, producerEpoch)

    appendProducer(Seq(1))
    appendProducer(Seq(2, 3))
    log.appendAsLeader(abortMarker(producerId, producerEpoch), leaderEpoch = 0, isFromClient = false)
    appendProducer(Seq(3))
    log.appendAsLeader(commitMarker(producerId, producerEpoch), leaderEpoch = 0, isFromClient = false)
    log.roll()

    // delete horizon set to 0 to verify marker is not removed early
    val dirtyOffset = cleaner.doClean(LogToClean(tp, log, 0L, 100L), deleteHorizonMs = 0L)._1
    assertEquals(List(3), LogTest.keysInLog(log))
    assertEquals(List(3, 4, 5), offsetsInLog(log))

    // clean again with large delete horizon and verify the marker is removed
    cleaner.doClean(LogToClean(tp, log, dirtyOffset, 100L), deleteHorizonMs = Long.MaxValue)
    assertEquals(List(3), LogTest.keysInLog(log))
    assertEquals(List(4, 5), offsetsInLog(log))
  }

  @Test
  def testEmptyBatchRemovalWithSequenceReuse(): Unit = {
    // The group coordinator always writes batches beginning with sequence number 0. This test
    // ensures that we still remove old empty batches and transaction markers under this expectation.

    val producerEpoch = 0.toShort
    val producerId = 1L
    val tp = new TopicPartition("test", 0)
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 2048: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val appendFirstTransaction = appendTransactionalAsLeader(log, producerId, producerEpoch, isFromClient = false)
    appendFirstTransaction(Seq(1))
    log.appendAsLeader(commitMarker(producerId, producerEpoch), leaderEpoch = 0, isFromClient = false)

    val appendSecondTransaction = appendTransactionalAsLeader(log, producerId, producerEpoch, isFromClient = false)
    appendSecondTransaction(Seq(2))
    log.appendAsLeader(commitMarker(producerId, producerEpoch), leaderEpoch = 0, isFromClient = false)

    log.appendAsLeader(record(1, 1), leaderEpoch = 0)
    log.appendAsLeader(record(2, 1), leaderEpoch = 0)

    // Roll the log to ensure that the data is cleanable.
    log.roll()

    // Both transactional batches will be cleaned. The last one will remain in the log
    // as an empty batch in order to preserve the producer sequence number and epoch
    cleaner.doClean(LogToClean(tp, log, 0L, 100L), deleteHorizonMs = Long.MaxValue)
    assertEquals(List(1, 3, 4, 5), offsetsInLog(log))
    assertEquals(List(1, 2, 3, 4, 5), lastOffsetsPerBatchInLog(log))

    // On the second round of cleaning, the marker from the first transaction should be removed.
    cleaner.doClean(LogToClean(tp, log, 0L, 100L), deleteHorizonMs = Long.MaxValue)
    assertEquals(List(3, 4, 5), offsetsInLog(log))
    assertEquals(List(2, 3, 4, 5), lastOffsetsPerBatchInLog(log))
  }

  @Test
  def testAbortMarkerRetentionWithEmptyBatch(): Unit = {
    val tp = new TopicPartition("test", 0)
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 256: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val producerEpoch = 0.toShort
    val producerId = 1L
    val appendProducer = appendTransactionalAsLeader(log, producerId, producerEpoch)

    appendProducer(Seq(2, 3)) // batch last offset is 1
    log.appendAsLeader(abortMarker(producerId, producerEpoch), leaderEpoch = 0, isFromClient = false)
    log.roll()

    def assertAbortedTransactionIndexed(): Unit = {
      val abortedTxns = log.collectAbortedTransactions(0L, 100L)
      assertEquals(1, abortedTxns.size)
      assertEquals(producerId, abortedTxns.head.producerId)
      assertEquals(0, abortedTxns.head.firstOffset)
      assertEquals(2, abortedTxns.head.lastOffset)
    }

    assertAbortedTransactionIndexed()

    // first time through the records are removed
    var dirtyOffset = cleaner.doClean(LogToClean(tp, log, 0L, 100L), deleteHorizonMs = Long.MaxValue)._1
    assertAbortedTransactionIndexed()
    assertEquals(List(), LogTest.keysInLog(log))
    assertEquals(List(2), offsetsInLog(log)) // abort marker is retained
    assertEquals(List(1, 2), lastOffsetsPerBatchInLog(log)) // empty batch is retained

    // the empty batch remains if cleaned again because it still holds the last sequence
    dirtyOffset = cleaner.doClean(LogToClean(tp, log, dirtyOffset, 100L), deleteHorizonMs = Long.MaxValue)._1
    assertAbortedTransactionIndexed()
    assertEquals(List(), LogTest.keysInLog(log))
    assertEquals(List(2), offsetsInLog(log)) // abort marker is still retained
    assertEquals(List(1, 2), lastOffsetsPerBatchInLog(log)) // empty batch is retained

    // now update the last sequence so that the empty batch can be removed
    appendProducer(Seq(1))
    log.roll()

    dirtyOffset = cleaner.doClean(LogToClean(tp, log, dirtyOffset, 100L), deleteHorizonMs = Long.MaxValue)._1
    assertAbortedTransactionIndexed()
    assertEquals(List(1), LogTest.keysInLog(log))
    assertEquals(List(2, 3), offsetsInLog(log)) // abort marker is not yet gone because we read the empty batch
    assertEquals(List(2, 3), lastOffsetsPerBatchInLog(log)) // but we do not preserve the empty batch

    dirtyOffset = cleaner.doClean(LogToClean(tp, log, dirtyOffset, 100L), deleteHorizonMs = Long.MaxValue)._1
    assertEquals(List(1), LogTest.keysInLog(log))
    assertEquals(List(3), offsetsInLog(log)) // abort marker is gone
    assertEquals(List(3), lastOffsetsPerBatchInLog(log))

    // we do not bother retaining the aborted transaction in the index
    assertEquals(0, log.collectAbortedTransactions(0L, 100L).size)
  }

  /**
   * Test log cleaning with logs containing messages larger than default message size
   */
  @Test
  def testLargeMessage() {
    val largeMessageSize = 1024 * 1024
    // Create cleaner with very small default max message size
    val cleaner = makeCleaner(Int.MaxValue, maxMessageSize=1024)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, largeMessageSize * 16: java.lang.Integer)
    logProps.put(LogConfig.MaxMessageBytesProp, largeMessageSize * 2: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    while(log.numberOfSegments < 2)
      log.appendAsLeader(record(log.logEndOffset.toInt, Array.fill(largeMessageSize)(0: Byte)), leaderEpoch = 0)
    val keysFound = LogTest.keysInLog(log)
    assertEquals(0L until log.logEndOffset, keysFound)

    // pretend we have the following keys
    val keys = immutable.ListSet(1L, 3L, 5L, 7L, 9L)
    val map = new FakeOffsetMap(Int.MaxValue)
    keys.foreach(k => map.put(key(k), Long.MaxValue))

    // clean the log
    val stats = new CleanerStats()
    cleaner.cleanSegments(log, Seq(log.logSegments.head), map, 0L, stats)
    val shouldRemain = LogTest.keysInLog(log).filter(!keys.contains(_))
    assertEquals(shouldRemain, LogTest.keysInLog(log))
  }

  /**
   * Test log cleaning with logs containing messages larger than topic's max message size
   */
  @Test
  def testMessageLargerThanMaxMessageSize() {
    val (log, offsetMap) = createLogWithMessagesLargerThanMaxSize(largeMessageSize = 1024 * 1024)

    val cleaner = makeCleaner(Int.MaxValue, maxMessageSize=1024)
    cleaner.cleanSegments(log, Seq(log.logSegments.head), offsetMap, 0L, new CleanerStats)
    val shouldRemain = LogTest.keysInLog(log).filter(k => !offsetMap.map.containsKey(k.toString))
    assertEquals(shouldRemain, LogTest.keysInLog(log))
  }

  /**
   * Test log cleaning with logs containing messages larger than topic's max message size
   * where header is corrupt
   */
  @Test
  def testMessageLargerThanMaxMessageSizeWithCorruptHeader() {
    val (log, offsetMap) = createLogWithMessagesLargerThanMaxSize(largeMessageSize = 1024 * 1024)
    val file = new RandomAccessFile(log.logSegments.head.log.file, "rw")
    file.seek(Records.MAGIC_OFFSET)
    file.write(0xff)
    file.close()

    val cleaner = makeCleaner(Int.MaxValue, maxMessageSize=1024)
    intercept[CorruptRecordException] {
      cleaner.cleanSegments(log, Seq(log.logSegments.head), offsetMap, 0L, new CleanerStats)
    }
  }

  /**
   * Test log cleaning with logs containing messages larger than topic's max message size
   * where message size is corrupt and larger than bytes available in log segment.
   */
  @Test
  def testCorruptMessageSizeLargerThanBytesAvailable() {
    val (log, offsetMap) = createLogWithMessagesLargerThanMaxSize(largeMessageSize = 1024 * 1024)
    val file = new RandomAccessFile(log.logSegments.head.log.file, "rw")
    file.setLength(1024)
    file.close()

    val cleaner = makeCleaner(Int.MaxValue, maxMessageSize=1024)
    intercept[CorruptRecordException] {
      cleaner.cleanSegments(log, Seq(log.logSegments.head), offsetMap, 0L, new CleanerStats)
    }
  }

  def createLogWithMessagesLargerThanMaxSize(largeMessageSize: Int): (Log, FakeOffsetMap) = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, largeMessageSize * 16: java.lang.Integer)
    logProps.put(LogConfig.MaxMessageBytesProp, largeMessageSize * 2: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    while(log.numberOfSegments < 2)
      log.appendAsLeader(record(log.logEndOffset.toInt, Array.fill(largeMessageSize)(0: Byte)), leaderEpoch = 0)
    val keysFound = LogTest.keysInLog(log)
    assertEquals(0L until log.logEndOffset, keysFound)

    // Decrease the log's max message size
    logProps.put(LogConfig.MaxMessageBytesProp, largeMessageSize / 2: java.lang.Integer)
    log.config = LogConfig.fromProps(logConfig.originals, logProps)

    // pretend we have the following keys
    val keys = immutable.ListSet(1, 3, 5, 7, 9)
    val map = new FakeOffsetMap(Int.MaxValue)
    keys.foreach(k => map.put(key(k), Long.MaxValue))

    (log, map)
  }

  @Test
  def testCleaningWithDeletes(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    // append messages with the keys 0 through N
    while(log.numberOfSegments < 2)
      log.appendAsLeader(record(log.logEndOffset.toInt, log.logEndOffset.toInt), leaderEpoch = 0)

    // delete all even keys between 0 and N
    val leo = log.logEndOffset
    for(key <- 0 until leo.toInt by 2)
      log.appendAsLeader(tombstoneRecord(key), leaderEpoch = 0)

    // append some new unique keys to pad out to a new active segment
    while(log.numberOfSegments < 4)
      log.appendAsLeader(record(log.logEndOffset.toInt, log.logEndOffset.toInt), leaderEpoch = 0)

    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 0, log.activeSegment.baseOffset))
    val keys = LogTest.keysInLog(log).toSet
    assertTrue("None of the keys we deleted should still exist.",
               (0 until leo.toInt by 2).forall(!keys.contains(_)))
  }

  def testLogCleanerStats(): Unit = {
    // because loadFactor is 0.75, this means we can fit 2 messages in the map
    val cleaner = makeCleaner(2)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    log.appendAsLeader(record(0,0), leaderEpoch = 0) // offset 0
    log.appendAsLeader(record(1,1), leaderEpoch = 0) // offset 1
    log.appendAsLeader(record(0,0), leaderEpoch = 0) // offset 2
    log.appendAsLeader(record(1,1), leaderEpoch = 0) // offset 3
    log.appendAsLeader(record(0,0), leaderEpoch = 0) // offset 4
    // roll the segment, so we can clean the messages already appended
    log.roll()

    val initialLogSize = log.size

    val (endOffset, stats) = cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 2, log.activeSegment.baseOffset))
    assertEquals(5, endOffset)
    assertEquals(5, stats.messagesRead)
    assertEquals(initialLogSize, stats.bytesRead)
    assertEquals(2, stats.messagesWritten)
    assertEquals(log.size, stats.bytesWritten)
    assertEquals(0, stats.invalidMessagesRead)
    assertTrue(stats.endTime >= stats.startTime)
  }

  @Test
  def testLogCleanerRetainsProducerLastSequence(): Unit = {
    val cleaner = makeCleaner(10)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))
    log.appendAsLeader(record(0, 0), leaderEpoch = 0) // offset 0
    log.appendAsLeader(record(0, 1, producerId = 1, producerEpoch = 0, sequence = 0), leaderEpoch = 0) // offset 1
    log.appendAsLeader(record(0, 2, producerId = 2, producerEpoch = 0, sequence = 0), leaderEpoch = 0) // offset 2
    log.appendAsLeader(record(0, 3, producerId = 3, producerEpoch = 0, sequence = 0), leaderEpoch = 0) // offset 3
    log.appendAsLeader(record(1, 1, producerId = 2, producerEpoch = 0, sequence = 1), leaderEpoch = 0) // offset 4

    // roll the segment, so we can clean the messages already appended
    log.roll()

    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 0L, log.activeSegment.baseOffset))
    assertEquals(List(1, 3, 4), lastOffsetsPerBatchInLog(log))
    assertEquals(Map(1L -> 0, 2L -> 1, 3L -> 0), lastSequencesInLog(log))
    assertEquals(List(0, 1), LogTest.keysInLog(log))
    assertEquals(List(3, 4), offsetsInLog(log))
  }

  @Test
  def testLogCleanerRetainsLastSequenceEvenIfTransactionAborted(): Unit = {
    val cleaner = makeCleaner(10)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val producerEpoch = 0.toShort
    val producerId = 1L
    val appendProducer = appendTransactionalAsLeader(log, producerId, producerEpoch)

    appendProducer(Seq(1))
    appendProducer(Seq(2, 3))
    log.appendAsLeader(abortMarker(producerId, producerEpoch), leaderEpoch = 0, isFromClient = false)
    log.roll()

    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 0L, log.activeSegment.baseOffset))
    assertEquals(List(2, 3), lastOffsetsPerBatchInLog(log))
    assertEquals(Map(producerId -> 2), lastSequencesInLog(log))
    assertEquals(List(), LogTest.keysInLog(log))
    assertEquals(List(3), offsetsInLog(log))

    // Append a new entry from the producer and verify that the empty batch is cleaned up
    appendProducer(Seq(1, 5))
    log.roll()
    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 0L, log.activeSegment.baseOffset))

    assertEquals(List(3, 5), lastOffsetsPerBatchInLog(log))
    assertEquals(Map(producerId -> 4), lastSequencesInLog(log))
    assertEquals(List(1, 5), LogTest.keysInLog(log))
    assertEquals(List(3, 4, 5), offsetsInLog(log))
  }

  @Test
  def testPartialSegmentClean(): Unit = {
    // because loadFactor is 0.75, this means we can fit 2 messages in the map
    val cleaner = makeCleaner(2)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    log.appendAsLeader(record(0,0), leaderEpoch = 0) // offset 0
    log.appendAsLeader(record(1,1), leaderEpoch = 0) // offset 1
    log.appendAsLeader(record(0,0), leaderEpoch = 0) // offset 2
    log.appendAsLeader(record(1,1), leaderEpoch = 0) // offset 3
    log.appendAsLeader(record(0,0), leaderEpoch = 0) // offset 4
    // roll the segment, so we can clean the messages already appended
    log.roll()

    // clean the log with only one message removed
    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 2, log.activeSegment.baseOffset))
    assertEquals(List(1,0,1,0), LogTest.keysInLog(log))
    assertEquals(List(1,2,3,4), offsetsInLog(log))

    // continue to make progress, even though we can only clean one message at a time
    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 3, log.activeSegment.baseOffset))
    assertEquals(List(0,1,0), LogTest.keysInLog(log))
    assertEquals(List(2,3,4), offsetsInLog(log))

    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 4, log.activeSegment.baseOffset))
    assertEquals(List(1,0), LogTest.keysInLog(log))
    assertEquals(List(3,4), offsetsInLog(log))
  }

  @Test
  def testCleaningWithUncleanableSection(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    // Number of distinct keys. For an effective test this should be small enough such that each log segment contains some duplicates.
    val N = 10
    val numCleanableSegments = 2
    val numTotalSegments = 7

    // append messages with the keys 0 through N-1, values equal offset
    while(log.numberOfSegments <= numCleanableSegments)
      log.appendAsLeader(record(log.logEndOffset.toInt % N, log.logEndOffset.toInt), leaderEpoch = 0)

    // at this point one message past the cleanable segments has been added
    // the entire segment containing the first uncleanable offset should not be cleaned.
    val firstUncleanableOffset = log.logEndOffset + 1  // +1  so it is past the baseOffset

    while(log.numberOfSegments < numTotalSegments - 1)
      log.appendAsLeader(record(log.logEndOffset.toInt % N, log.logEndOffset.toInt), leaderEpoch = 0)

    // the last (active) segment has just one message

    def distinctValuesBySegment = log.logSegments.map(s => s.log.records.asScala.map(record => TestUtils.readString(record.value)).toSet.size).toSeq

    val disctinctValuesBySegmentBeforeClean = distinctValuesBySegment
    assertTrue("Test is not effective unless each segment contains duplicates. Increase segment size or decrease number of keys.",
      distinctValuesBySegment.reverse.tail.forall(_ > N))

    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 0, firstUncleanableOffset))

    val distinctValuesBySegmentAfterClean = distinctValuesBySegment

    assertTrue("The cleanable segments should have fewer number of values after cleaning",
      disctinctValuesBySegmentBeforeClean.zip(distinctValuesBySegmentAfterClean).take(numCleanableSegments).forall { case (before, after) => after < before })
    assertTrue("The uncleanable segments should have the same number of values after cleaning", disctinctValuesBySegmentBeforeClean.zip(distinctValuesBySegmentAfterClean)
      .slice(numCleanableSegments, numTotalSegments).forall { x => x._1 == x._2 })
  }

  @Test
  def testLogToClean(): Unit = {
    // create a log with small segment size
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 100: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    // create 6 segments with only one message in each segment
    def createRecorcs = TestUtils.singletonRecords(value = Array.fill[Byte](25)(0), key = 1.toString.getBytes)
    for (_ <- 0 until 6)
      log.appendAsLeader(createRecorcs, leaderEpoch = 0)

    val logToClean = LogToClean(new TopicPartition("test", 0), log, log.activeSegment.baseOffset, log.activeSegment.baseOffset)

    assertEquals("Total bytes of LogToClean should equal size of all segments excluding the active segment",
      logToClean.totalBytes, log.size - log.activeSegment.size)
  }

  @Test
  def testLogToCleanWithUncleanableSection(): Unit = {
    // create a log with small segment size
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 100: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    // create 6 segments with only one message in each segment
    def createRecords = TestUtils.singletonRecords(value = Array.fill[Byte](25)(0), key = 1.toString.getBytes)
    for (_ <- 0 until 6)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    // segments [0,1] are clean; segments [2, 3] are cleanable; segments [4,5] are uncleanable
    val segs = log.logSegments.toSeq
    val logToClean = LogToClean(new TopicPartition("test", 0), log, segs(2).baseOffset, segs(4).baseOffset)

    val expectedCleanSize = segs.take(2).map(_.size).sum
    val expectedCleanableSize = segs.slice(2, 4).map(_.size).sum

    assertEquals("Uncleanable bytes of LogToClean should equal size of all segments prior the one containing first dirty",
      logToClean.cleanBytes, expectedCleanSize)
    assertEquals("Cleanable bytes of LogToClean should equal size of all segments from the one containing first dirty offset" +
      " to the segment prior to the one with the first uncleanable offset",
      logToClean.cleanableBytes, expectedCleanableSize)
    assertEquals("Total bytes should be the sum of the clean and cleanable segments", logToClean.totalBytes, expectedCleanSize + expectedCleanableSize)
    assertEquals("Total cleanable ratio should be the ratio of cleanable size to clean plus cleanable", logToClean.cleanableRatio,
      expectedCleanableSize / (expectedCleanSize + expectedCleanableSize).toDouble, 1.0e-6d)
  }

  @Test
  def testCleaningWithUnkeyedMessages(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)

    // create a log with compaction turned off so we can append unkeyed messages
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Delete)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    // append unkeyed messages
    while(log.numberOfSegments < 2)
      log.appendAsLeader(unkeyedRecord(log.logEndOffset.toInt), leaderEpoch = 0)
    val numInvalidMessages = unkeyedMessageCountInLog(log)

    val sizeWithUnkeyedMessages = log.size

    // append keyed messages
    while(log.numberOfSegments < 3)
      log.appendAsLeader(record(log.logEndOffset.toInt, log.logEndOffset.toInt), leaderEpoch = 0)

    val expectedSizeAfterCleaning = log.size - sizeWithUnkeyedMessages
    val (_, stats) = cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 0, log.activeSegment.baseOffset))

    assertEquals("Log should only contain keyed messages after cleaning.", 0, unkeyedMessageCountInLog(log))
    assertEquals("Log should only contain keyed messages after cleaning.", expectedSizeAfterCleaning, log.size)
    assertEquals("Cleaner should have seen %d invalid messages.", numInvalidMessages, stats.invalidMessagesRead)
  }

  def lastOffsetsPerBatchInLog(log: Log): Iterable[Long] = {
    for (segment <- log.logSegments; batch <- segment.log.batches.asScala)
      yield batch.lastOffset
  }

  def lastSequencesInLog(log: Log): Map[Long, Int] = {
    (for (segment <- log.logSegments;
          batch <- segment.log.batches.asScala if !batch.isControlBatch && batch.hasProducerId)
      yield batch.producerId -> batch.lastSequence).toMap
  }

  /* extract all the offsets from a log */
  def offsetsInLog(log: Log): Iterable[Long] =
    log.logSegments.flatMap(s => s.log.records.asScala.filter(_.hasValue).filter(_.hasKey).map(m => m.offset))

  def unkeyedMessageCountInLog(log: Log) =
    log.logSegments.map(s => s.log.records.asScala.filter(_.hasValue).count(m => !m.hasKey)).sum

  def abortCheckDone(topicPartition: TopicPartition): Unit = {
    throw new LogCleaningAbortedException()
  }

  /**
   * Test that abortion during cleaning throws a LogCleaningAbortedException
   */
  @Test
  def testCleanSegmentsWithAbort(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue, abortCheckDone)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    // append messages to the log until we have four segments
    while(log.numberOfSegments < 4)
      log.appendAsLeader(record(log.logEndOffset.toInt, log.logEndOffset.toInt), leaderEpoch = 0)

    val keys = LogTest.keysInLog(log)
    val map = new FakeOffsetMap(Int.MaxValue)
    keys.foreach(k => map.put(key(k), Long.MaxValue))
    intercept[LogCleaningAbortedException] {
      cleaner.cleanSegments(log, log.logSegments.take(3).toSeq, map, 0L, new CleanerStats())
    }
  }

  /**
   * Validate the logic for grouping log segments together for cleaning
   */
  @Test
  def testSegmentGrouping(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 300: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    // append some messages to the log
    var i = 0
    while(log.numberOfSegments < 10) {
      log.appendAsLeader(TestUtils.singletonRecords(value = "hello".getBytes, key = "hello".getBytes), leaderEpoch = 0)
      i += 1
    }

    // grouping by very large values should result in a single group with all the segments in it
    var groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Int.MaxValue, maxIndexSize = Int.MaxValue, log.logEndOffset)
    assertEquals(1, groups.size)
    assertEquals(log.numberOfSegments, groups.head.size)
    checkSegmentOrder(groups)

    // grouping by very small values should result in all groups having one entry
    groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = 1, maxIndexSize = Int.MaxValue, log.logEndOffset)
    assertEquals(log.numberOfSegments, groups.size)
    assertTrue("All groups should be singletons.", groups.forall(_.size == 1))
    checkSegmentOrder(groups)
    groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Int.MaxValue, maxIndexSize = 1, log.logEndOffset)
    assertEquals(log.numberOfSegments, groups.size)
    assertTrue("All groups should be singletons.", groups.forall(_.size == 1))
    checkSegmentOrder(groups)

    val groupSize = 3

    // check grouping by log size
    val logSize = log.logSegments.take(groupSize).map(_.size).sum.toInt + 1
    groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = logSize, maxIndexSize = Int.MaxValue, log.logEndOffset)
    checkSegmentOrder(groups)
    assertTrue("All but the last group should be the target size.", groups.dropRight(1).forall(_.size == groupSize))

    // check grouping by index size
    val indexSize = log.logSegments.take(groupSize).map(_.offsetIndex.sizeInBytes).sum + 1
    groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Int.MaxValue, maxIndexSize = indexSize, log.logEndOffset)
    checkSegmentOrder(groups)
    assertTrue("All but the last group should be the target size.", groups.dropRight(1).forall(_.size == groupSize))
  }

  /**
   * Validate the logic for grouping log segments together for cleaning when only a small number of
   * messages are retained, but the range of offsets is greater than Int.MaxValue. A group should not
   * contain a range of offsets greater than Int.MaxValue to ensure that relative offsets can be
   * stored in 4 bytes.
   */
  @Test
  def testSegmentGroupingWithSparseOffsets(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 400: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    // fill up first segment
    while (log.numberOfSegments == 1)
      log.appendAsLeader(TestUtils.singletonRecords(value = "hello".getBytes, key = "hello".getBytes), leaderEpoch = 0)

    // forward offset and append message to next segment at offset Int.MaxValue
    val records = messageWithOffset("hello".getBytes, "hello".getBytes, Int.MaxValue - 1)
    log.appendAsFollower(records)
    log.appendAsLeader(TestUtils.singletonRecords(value = "hello".getBytes, key = "hello".getBytes), leaderEpoch = 0)
    assertEquals(Int.MaxValue, log.activeSegment.offsetIndex.lastOffset)

    // grouping should result in a single group with maximum relative offset of Int.MaxValue
    var groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Int.MaxValue, maxIndexSize = Int.MaxValue, log.logEndOffset)
    assertEquals(1, groups.size)

    // append another message, making last offset of second segment > Int.MaxValue
    log.appendAsLeader(TestUtils.singletonRecords(value = "hello".getBytes, key = "hello".getBytes), leaderEpoch = 0)

    // grouping should not group the two segments to ensure that maximum relative offset in each group <= Int.MaxValue
    groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Int.MaxValue, maxIndexSize = Int.MaxValue, log.logEndOffset)
    assertEquals(2, groups.size)
    checkSegmentOrder(groups)

    // append more messages, creating new segments, further grouping should still occur
    while (log.numberOfSegments < 4)
      log.appendAsLeader(TestUtils.singletonRecords(value = "hello".getBytes, key = "hello".getBytes), leaderEpoch = 0)

    groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Int.MaxValue, maxIndexSize = Int.MaxValue, log.logEndOffset)
    assertEquals(log.numberOfSegments - 1, groups.size)
    for (group <- groups)
      assertTrue("Relative offset greater than Int.MaxValue", group.last.offsetIndex.lastOffset - group.head.offsetIndex.baseOffset <= Int.MaxValue)
    checkSegmentOrder(groups)
  }

  /**
   * Following the loading of a log segment where the index file is zero sized,
   * the index returned would be the base offset.  Sometimes the log file would
   * contain data with offsets in excess of the baseOffset which would cause
   * the log cleaner to group together segments with a range of > Int.MaxValue
   * this test replicates that scenario to ensure that the segments are grouped
   * correctly.
   */
  @Test
  def testSegmentGroupingFollowingLoadOfZeroIndex(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 400: java.lang.Integer)

    //mimic the effect of loading an empty index file
    logProps.put(LogConfig.IndexIntervalBytesProp, 400: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val record1 = messageWithOffset("hello".getBytes, "hello".getBytes, 0)
    log.appendAsFollower(record1)
    val record2 = messageWithOffset("hello".getBytes, "hello".getBytes, 1)
    log.appendAsFollower(record2)
    log.roll(Some(Int.MaxValue/2)) // starting a new log segment at offset Int.MaxValue/2
    val record3 = messageWithOffset("hello".getBytes, "hello".getBytes, Int.MaxValue/2)
    log.appendAsFollower(record3)
    val record4 = messageWithOffset("hello".getBytes, "hello".getBytes, Int.MaxValue.toLong + 1)
    log.appendAsFollower(record4)

    assertTrue("Actual offset range should be > Int.MaxValue", log.logEndOffset - 1 - log.logStartOffset > Int.MaxValue)
    assertTrue("index.lastOffset is reporting the wrong last offset", log.logSegments.last.offsetIndex.lastOffset - log.logStartOffset <= Int.MaxValue)

    // grouping should result in two groups because the second segment takes the offset range > MaxInt
    val groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Int.MaxValue, maxIndexSize = Int.MaxValue, log.logEndOffset)
    assertEquals(2, groups.size)

    for (group <- groups)
      assertTrue("Relative offset greater than Int.MaxValue", group.last.readNextOffset - 1 - group.head.baseOffset <= Int.MaxValue)
    checkSegmentOrder(groups)
  }

  private def checkSegmentOrder(groups: Seq[Seq[LogSegment]]): Unit = {
    val offsets = groups.flatMap(_.map(_.baseOffset))
    assertEquals("Offsets should be in increasing order.", offsets.sorted, offsets)
  }

  /**
   * Test building an offset map off the log
   */
  @Test
  def testBuildOffsetMap(): Unit = {
    val map = new FakeOffsetMap(1000)
    val log = makeLog()
    val cleaner = makeCleaner(Int.MaxValue)
    val start = 0
    val end = 500
    writeToLog(log, (start until end) zip (start until end))

    def checkRange(map: FakeOffsetMap, start: Int, end: Int) {
      val stats = new CleanerStats()
      cleaner.buildOffsetMap(log, start, end, map, stats)
      val endOffset = map.latestOffset + 1
      assertEquals("Last offset should be the end offset.", end, endOffset)
      assertEquals("Should have the expected number of messages in the map.", end-start, map.size)
      for(i <- start until end)
        assertEquals("Should find all the keys", i.toLong, map.get(key(i)))
      assertEquals("Should not find a value too small", -1L, map.get(key(start - 1)))
      assertEquals("Should not find a value too large", -1L, map.get(key(end)))
      assertEquals(end - start, stats.mapMessagesRead)
    }

    val segments = log.logSegments.toSeq
    checkRange(map, 0, segments(1).baseOffset.toInt)
    checkRange(map, segments(1).baseOffset.toInt, segments(3).baseOffset.toInt)
    checkRange(map, segments(3).baseOffset.toInt, log.logEndOffset.toInt)
  }

  @Test
  def testSegmentWithOffsetOverflow(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    logProps.put(LogConfig.FileDeleteDelayMsProp, 1000: java.lang.Integer)
    val config = LogConfig.fromProps(logConfig.originals, logProps)

    LogTest.initializeLogDirWithOverflowedSegment(dir)

    val log = makeLog(config = config, recoveryPoint = Long.MaxValue)
    val segmentWithOverflow = LogTest.firstOverflowSegment(log).getOrElse {
      fail("Failed to create log with a segment which has overflowed offsets")
    }

    val numSegmentsInitial = log.logSegments.size
    val allKeys = LogTest.keysInLog(log).toList
    val expectedKeysAfterCleaning = mutable.MutableList[Long]()

    // pretend we want to clean every alternate key
    val offsetMap = new FakeOffsetMap(Int.MaxValue)
    for (k <- 1 until allKeys.size by 2) {
      expectedKeysAfterCleaning += allKeys(k - 1)
      offsetMap.put(key(allKeys(k)), Long.MaxValue)
    }

    // Try to clean segment with offset overflow. This will trigger log split and the cleaning itself must abort.
    assertThrows[LogCleaningAbortedException] {
      cleaner.cleanSegments(log, List(segmentWithOverflow), offsetMap, 0L, new CleanerStats())
    }
    assertEquals(numSegmentsInitial + 1, log.logSegments.size)
    assertEquals(allKeys, LogTest.keysInLog(log))
    assertFalse(LogTest.hasOffsetOverflow(log))

    // Clean each segment now that split is complete.
    for (segmentToClean <- log.logSegments)
      cleaner.cleanSegments(log, List(segmentToClean), offsetMap, 0L, new CleanerStats())
    assertEquals(expectedKeysAfterCleaning, LogTest.keysInLog(log))
    assertFalse(LogTest.hasOffsetOverflow(log))
    log.close()
  }

  /**
   * Tests recovery if broker crashes at the following stages during the cleaning sequence
   * <ol>
   *   <li> Cleaner has created .cleaned log containing multiple segments, swap sequence not yet started
   *   <li> .cleaned log renamed to .swap, old segment files not yet renamed to .deleted
   *   <li> .cleaned log renamed to .swap, old segment files renamed to .deleted, but not yet deleted
   *   <li> .swap suffix removed, completing the swap, but async delete of .deleted files not yet complete
   * </ol>
   */
  @Test
  def testRecoveryAfterCrash(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 300: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    logProps.put(LogConfig.FileDeleteDelayMsProp, 10: java.lang.Integer)

    val config = LogConfig.fromProps(logConfig.originals, logProps)

   // create a log and append some messages
    var log = makeLog(config = config)
    var messageCount = 0
    while (log.numberOfSegments < 10) {
      log.appendAsLeader(record(log.logEndOffset.toInt, log.logEndOffset.toInt), leaderEpoch = 0)
      messageCount += 1
    }
    val allKeys = LogTest.keysInLog(log)

    // pretend we have odd-numbered keys
    val offsetMap = new FakeOffsetMap(Int.MaxValue)
    for (k <- 1 until messageCount by 2)
      offsetMap.put(key(k), Long.MaxValue)

    // clean the log
    cleaner.cleanSegments(log, log.logSegments.take(9).toSeq, offsetMap, 0L, new CleanerStats())
    // clear scheduler so that async deletes don't run
    time.scheduler.clear()
    var cleanedKeys = LogTest.keysInLog(log)
    log.close()

    // 1) Simulate recovery just after .cleaned file is created, before rename to .swap
    //    On recovery, clean operation is aborted. All messages should be present in the log
    log.logSegments.head.changeFileSuffixes("", Log.CleanedFileSuffix)
    for (file <- dir.listFiles if file.getName.endsWith(Log.DeletedFileSuffix)) {
      Utils.atomicMoveWithFallback(file.toPath, Paths.get(CoreUtils.replaceSuffix(file.getPath, Log.DeletedFileSuffix, "")))
    }
    log = recoverAndCheck(config, allKeys)

    // clean again
    cleaner.cleanSegments(log, log.logSegments.take(9).toSeq, offsetMap, 0L, new CleanerStats())
    // clear scheduler so that async deletes don't run
    time.scheduler.clear()
    cleanedKeys = LogTest.keysInLog(log)
    log.close()

    // 2) Simulate recovery just after swap file is created, before old segment files are
    //    renamed to .deleted. Clean operation is resumed during recovery.
    log.logSegments.head.changeFileSuffixes("", Log.SwapFileSuffix)
    for (file <- dir.listFiles if file.getName.endsWith(Log.DeletedFileSuffix)) {
      Utils.atomicMoveWithFallback(file.toPath, Paths.get(CoreUtils.replaceSuffix(file.getPath, Log.DeletedFileSuffix, "")))
    }
    log = recoverAndCheck(config, cleanedKeys)

    // add some more messages and clean the log again
    while (log.numberOfSegments < 10) {
      log.appendAsLeader(record(log.logEndOffset.toInt, log.logEndOffset.toInt), leaderEpoch = 0)
      messageCount += 1
    }
    for (k <- 1 until messageCount by 2)
      offsetMap.put(key(k), Long.MaxValue)
    cleaner.cleanSegments(log, log.logSegments.take(9).toSeq, offsetMap, 0L, new CleanerStats())
    // clear scheduler so that async deletes don't run
    time.scheduler.clear()
    cleanedKeys = LogTest.keysInLog(log)

    // 3) Simulate recovery after swap file is created and old segments files are renamed
    //    to .deleted. Clean operation is resumed during recovery.
    log.logSegments.head.changeFileSuffixes("", Log.SwapFileSuffix)
    log = recoverAndCheck(config, cleanedKeys)

    // add some more messages and clean the log again
    while (log.numberOfSegments < 10) {
      log.appendAsLeader(record(log.logEndOffset.toInt, log.logEndOffset.toInt), leaderEpoch = 0)
      messageCount += 1
    }
    for (k <- 1 until messageCount by 2)
      offsetMap.put(key(k), Long.MaxValue)
    cleaner.cleanSegments(log, log.logSegments.take(9).toSeq, offsetMap, 0L, new CleanerStats())
    // clear scheduler so that async deletes don't run
    time.scheduler.clear()
    cleanedKeys = LogTest.keysInLog(log)
    log.close()

    // 4) Simulate recovery after swap is complete, but async deletion
    //    is not yet complete. Clean operation is resumed during recovery.
    log = recoverAndCheck(config, cleanedKeys)
    log.close()
  }

  @Test
  def testBuildOffsetMapFakeLarge(): Unit = {
    val map = new FakeOffsetMap(1000)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 120: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 120: java.lang.Integer)
    logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    val logConfig = LogConfig(logProps)
    val log = makeLog(config = logConfig)
    val cleaner = makeCleaner(Int.MaxValue)
    val keyStart = 0
    val keyEnd = 2
    val offsetStart = 0L
    val offsetEnd = 7206178L
    val offsetSeq = Seq(offsetStart, offsetEnd)
    writeToLog(log, (keyStart until keyEnd) zip (keyStart until keyEnd), offsetSeq)
    cleaner.buildOffsetMap(log, keyStart, offsetEnd + 1L, map, new CleanerStats())
    assertEquals("Last offset should be the end offset.", offsetEnd, map.latestOffset)
    assertEquals("Should have the expected number of messages in the map.", keyEnd - keyStart, map.size)
    assertEquals("Map should contain first value", 0L, map.get(key(0)))
    assertEquals("Map should contain second value", offsetEnd, map.get(key(1)))
  }

  /**
   * Test building a partial offset map of part of a log segment
   */
  @Test
  def testBuildPartialOffsetMap(): Unit = {
    // because loadFactor is 0.75, this means we can fit 2 messages in the map
    val map = new FakeOffsetMap(3)
    val log = makeLog()
    val cleaner = makeCleaner(2)

    log.appendAsLeader(record(0,0), leaderEpoch = 0)
    log.appendAsLeader(record(1,1), leaderEpoch = 0)
    log.appendAsLeader(record(2,2), leaderEpoch = 0)
    log.appendAsLeader(record(3,3), leaderEpoch = 0)
    log.appendAsLeader(record(4,4), leaderEpoch = 0)
    log.roll()

    val stats = new CleanerStats()
    cleaner.buildOffsetMap(log, 2, Int.MaxValue, map, stats)
    assertEquals(2, map.size)
    assertEquals(-1, map.get(key(0)))
    assertEquals(2, map.get(key(2)))
    assertEquals(3, map.get(key(3)))
    assertEquals(-1, map.get(key(4)))
    assertEquals(4, stats.mapMessagesRead)
  }

  /**
   * This test verifies that messages corrupted by KAFKA-4298 are fixed by the cleaner
   */
  @Test
  def testCleanCorruptMessageSet() {
    val codec = CompressionType.GZIP

    val logProps = new Properties()
    logProps.put(LogConfig.CompressionTypeProp, codec.name)
    val logConfig = LogConfig(logProps)

    val log = makeLog(config = logConfig)
    val cleaner = makeCleaner(10)

    // messages are constructed so that the payload matches the expecting offset to
    // make offset validation easier after cleaning

    // one compressed log entry with duplicates
    val dupSetKeys = (0 until 2) ++ (0 until 2)
    val dupSetOffset = 25
    val dupSet = dupSetKeys zip (dupSetOffset until dupSetOffset + dupSetKeys.size)

    // and one without (should still be fixed by the cleaner)
    val noDupSetKeys = 3 until 5
    val noDupSetOffset = 50
    val noDupSet = noDupSetKeys zip (noDupSetOffset until noDupSetOffset + noDupSetKeys.size)

    log.appendAsFollower(invalidCleanedMessage(dupSetOffset, dupSet, codec))
    log.appendAsFollower(invalidCleanedMessage(noDupSetOffset, noDupSet, codec))

    log.roll()

    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 0, log.activeSegment.baseOffset))

    for (segment <- log.logSegments; batch <- segment.log.batches.asScala; record <- batch.asScala) {
      assertTrue(record.hasMagic(batch.magic))
      val value = TestUtils.readString(record.value).toLong
      assertEquals(record.offset, value)
    }
  }

  /**
   * Verify that the client can handle corrupted messages. Located here for now since the client
   * does not support writing messages with the old magic.
   */
  @Test
  def testClientHandlingOfCorruptMessageSet(): Unit = {
    import JavaConverters._

    val keys = 1 until 10
    val offset = 50
    val set = keys zip (offset until offset + keys.size)

    val corruptedMessage = invalidCleanedMessage(offset, set)
    val records = MemoryRecords.readableRecords(corruptedMessage.buffer)

    for (logEntry <- records.records.asScala) {
      val offset = logEntry.offset
      val value = TestUtils.readString(logEntry.value).toLong
      assertEquals(offset, value)
    }
  }

  @Test
  def testCleanTombstone(): Unit = {
    val logConfig = LogConfig(new Properties())

    val log = makeLog(config = logConfig)
    val cleaner = makeCleaner(10)

    // Append a message with a large timestamp.
    log.appendAsLeader(TestUtils.singletonRecords(value = "0".getBytes,
                                          key = "0".getBytes,
                                          timestamp = time.milliseconds() + logConfig.deleteRetentionMs + 10000), leaderEpoch = 0)
    log.roll()
    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 0, log.activeSegment.baseOffset))
    // Append a tombstone with a small timestamp and roll out a new log segment.
    log.appendAsLeader(TestUtils.singletonRecords(value = null,
                                          key = "0".getBytes,
                                          timestamp = time.milliseconds() - logConfig.deleteRetentionMs - 10000), leaderEpoch = 0)
    log.roll()
    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 1, log.activeSegment.baseOffset))
    assertEquals("The tombstone should be retained.", 1, log.logSegments.head.log.batches.iterator.next().lastOffset)
    // Append a message and roll out another log segment.
    log.appendAsLeader(TestUtils.singletonRecords(value = "1".getBytes,
                                          key = "1".getBytes,
                                          timestamp = time.milliseconds()), leaderEpoch = 0)
    log.roll()
    cleaner.clean(LogToClean(new TopicPartition("test", 0), log, 2, log.activeSegment.baseOffset))
    assertEquals("The tombstone should be retained.", 1, log.logSegments.head.log.batches.iterator.next().lastOffset)
  }

  private def writeToLog(log: Log, keysAndValues: Iterable[(Int, Int)], offsetSeq: Iterable[Long]): Iterable[Long] = {
    for(((key, value), offset) <- keysAndValues.zip(offsetSeq))
      yield log.appendAsFollower(messageWithOffset(key, value, offset)).lastOffset
  }

  private def invalidCleanedMessage(initialOffset: Long,
                                    keysAndValues: Iterable[(Int, Int)],
                                    codec: CompressionType = CompressionType.GZIP): MemoryRecords = {
    // this function replicates the old versions of the cleaner which under some circumstances
    // would write invalid compressed message sets with the outer magic set to 1 and the inner
    // magic set to 0
    val records = keysAndValues.map(kv =>
      LegacyRecord.create(RecordBatch.MAGIC_VALUE_V0,
        RecordBatch.NO_TIMESTAMP,
        kv._1.toString.getBytes,
        kv._2.toString.getBytes))

    val buffer = ByteBuffer.allocate(math.min(math.max(records.map(_.sizeInBytes()).sum / 2, 1024), 1 << 16))
    val builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V1, codec, TimestampType.CREATE_TIME, initialOffset)

    var offset = initialOffset
    records.foreach { record =>
      builder.appendUncheckedWithOffset(offset, record)
      offset += 1
    }

    builder.build()
  }

  private def messageWithOffset(key: Array[Byte], value: Array[Byte], offset: Long): MemoryRecords =
    MemoryRecords.withRecords(offset, CompressionType.NONE, 0, new SimpleRecord(key, value))

  private def messageWithOffset(key: Int, value: Int, offset: Long): MemoryRecords =
    messageWithOffset(key.toString.getBytes, value.toString.getBytes, offset)

  private def makeLog(dir: File = dir, config: LogConfig = logConfig, recoveryPoint: Long = 0L) =
    Log(dir = dir, config = config, logStartOffset = 0L, recoveryPoint = recoveryPoint, scheduler = time.scheduler,
      time = time, brokerTopicStats = new BrokerTopicStats, maxProducerIdExpirationMs = 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10))

  private def makeCleaner(capacity: Int, checkDone: TopicPartition => Unit = _ => (), maxMessageSize: Int = 64*1024) =
    new Cleaner(id = 0,
                offsetMap = new FakeOffsetMap(capacity),
                ioBufferSize = maxMessageSize,
                maxIoBufferSize = maxMessageSize,
                dupBufferLoadFactor = 0.75,
                throttler = throttler,
                time = time,
                checkDone = checkDone)

  private def writeToLog(log: Log, seq: Iterable[(Int, Int)]): Iterable[Long] = {
    for ((key, value) <- seq) yield log.appendAsLeader(record(key, value), leaderEpoch = 0).firstOffset.get
  }

  private def key(id: Long) = ByteBuffer.wrap(id.toString.getBytes)

  private def record(key: Int, value: Int,
             producerId: Long = RecordBatch.NO_PRODUCER_ID,
             producerEpoch: Short = RecordBatch.NO_PRODUCER_EPOCH,
             sequence: Int = RecordBatch.NO_SEQUENCE,
             partitionLeaderEpoch: Int = RecordBatch.NO_PARTITION_LEADER_EPOCH): MemoryRecords = {
    MemoryRecords.withIdempotentRecords(RecordBatch.CURRENT_MAGIC_VALUE, 0L, CompressionType.NONE, producerId, producerEpoch, sequence,
      partitionLeaderEpoch, new SimpleRecord(key.toString.getBytes, value.toString.getBytes))
  }

  private def appendTransactionalAsLeader(log: Log,
                                          producerId: Long,
                                          producerEpoch: Short,
                                          leaderEpoch: Int = 0,
                                          isFromClient: Boolean = true): Seq[Int] => LogAppendInfo = {
    appendIdempotentAsLeader(log, producerId, producerEpoch, isTransactional = true, isFromClient = isFromClient)
  }

  private def appendIdempotentAsLeader(log: Log, producerId: Long,
                                       producerEpoch: Short,
                                       isTransactional: Boolean = false,
                                       leaderEpoch: Int = 0,
                                       isFromClient: Boolean = true): Seq[Int] => LogAppendInfo = {
    var sequence = 0
    keys: Seq[Int] => {
      val simpleRecords = keys.map { key =>
        val keyBytes = key.toString.getBytes
        new SimpleRecord(time.milliseconds(), keyBytes, keyBytes) // the value doesn't matter since we validate offsets
      }
      val records = if (isTransactional)
        MemoryRecords.withTransactionalRecords(CompressionType.NONE, producerId, producerEpoch, sequence, simpleRecords: _*)
      else
        MemoryRecords.withIdempotentRecords(CompressionType.NONE, producerId, producerEpoch, sequence, simpleRecords: _*)
      sequence += simpleRecords.size
      log.appendAsLeader(records, leaderEpoch, isFromClient)
    }
  }

  private def commitMarker(producerId: Long, producerEpoch: Short, timestamp: Long = time.milliseconds()): MemoryRecords =
    endTxnMarker(producerId, producerEpoch, ControlRecordType.COMMIT, 0L, timestamp)

  private def abortMarker(producerId: Long, producerEpoch: Short, timestamp: Long = time.milliseconds()): MemoryRecords =
    endTxnMarker(producerId, producerEpoch, ControlRecordType.ABORT, 0L, timestamp)

  private def endTxnMarker(producerId: Long, producerEpoch: Short, controlRecordType: ControlRecordType,
                   offset: Long, timestamp: Long): MemoryRecords = {
    val endTxnMarker = new EndTransactionMarker(controlRecordType, 0)
    MemoryRecords.withEndTransactionMarker(offset, timestamp, RecordBatch.NO_PARTITION_LEADER_EPOCH,
      producerId, producerEpoch, endTxnMarker)
  }

  private def record(key: Int, value: Array[Byte]): MemoryRecords =
    TestUtils.singletonRecords(key = key.toString.getBytes, value = value)

  private def unkeyedRecord(value: Int): MemoryRecords =
    TestUtils.singletonRecords(value = value.toString.getBytes)

  private def tombstoneRecord(key: Int): MemoryRecords = record(key, null)

  private def recoverAndCheck(config: LogConfig, expectedKeys: Iterable[Long]): Log = {
    LogTest.recoverAndCheck(dir, config, expectedKeys, new BrokerTopicStats(), time, time.scheduler)
  }
}

class FakeOffsetMap(val slots: Int) extends OffsetMap {
  val map = new java.util.HashMap[String, Long]()
  var lastOffset = -1L

  private def keyFor(key: ByteBuffer) =
    new String(Utils.readBytes(key.duplicate), "UTF-8")

  override def put(key: ByteBuffer, offset: Long): Unit = {
    lastOffset = offset
    map.put(keyFor(key), offset)
  }

  override def get(key: ByteBuffer): Long = {
    val k = keyFor(key)
    if(map.containsKey(k))
      map.get(k)
    else
      -1L
  }

  override def clear(): Unit = map.clear()

  override def size: Int = map.size

  override def latestOffset: Long = lastOffset

  override def updateLatestOffset(offset: Long): Unit = {
    lastOffset = offset
  }

  override def toString: String = map.toString
}
