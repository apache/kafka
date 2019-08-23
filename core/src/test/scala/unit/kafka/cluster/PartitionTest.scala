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
package kafka.cluster

import java.io.File
import java.nio.ByteBuffer
import java.util.{Optional, Properties}
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.AtomicBoolean

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Metric
import kafka.api.{ApiVersion, LeaderAndIsr}
import kafka.common.UnexpectedAppendOffsetException
import kafka.log.{Defaults => _, _}
import kafka.server._
import kafka.server.checkpoints.OffsetCheckpoints
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{ApiException, OffsetNotAvailableException, ReplicaNotAvailableException}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{EpochEndOffset, IsolationLevel, LeaderAndIsrRequest, ListOffsetRequest}
import org.junit.{After, Before, Test}
import org.junit.Assert._
import org.mockito.Mockito.{doNothing, mock, when}
import org.scalatest.Assertions.assertThrows
import org.mockito.ArgumentMatchers
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class PartitionTest {

  val brokerId = 101
  val topicPartition = new TopicPartition("test-topic", 0)
  val time = new MockTime()
  var tmpDir: File = _
  var logDir1: File = _
  var logDir2: File = _
  var logManager: LogManager = _
  var logConfig: LogConfig = _
  val stateStore: PartitionStateStore = mock(classOf[PartitionStateStore])
  val delayedOperations: DelayedOperations = mock(classOf[DelayedOperations])
  val metadataCache: MetadataCache = mock(classOf[MetadataCache])
  val offsetCheckpoints: OffsetCheckpoints = mock(classOf[OffsetCheckpoints])
  var partition: Partition = _

  @Before
  def setup(): Unit = {
    TestUtils.clearYammerMetrics()

    val logProps = createLogProperties(Map.empty)
    logConfig = LogConfig(logProps)

    tmpDir = TestUtils.tempDir()
    logDir1 = TestUtils.randomPartitionLogDir(tmpDir)
    logDir2 = TestUtils.randomPartitionLogDir(tmpDir)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, CleanerConfig(enableCleaner = false), time)
    logManager.startup()

    partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      stateStore,
      delayedOperations,
      metadataCache,
      logManager)

    when(stateStore.fetchTopicConfig()).thenReturn(createLogProperties(Map.empty))
    when(offsetCheckpoints.fetch(ArgumentMatchers.anyString, ArgumentMatchers.eq(topicPartition)))
      .thenReturn(None)
  }

  private def createLogProperties(overrides: Map[String, String]): Properties = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 512: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 999: java.lang.Integer)
    overrides.foreach { case (k, v) => logProps.put(k, v) }
    logProps
  }

  @After
  def tearDown(): Unit = {
    logManager.shutdown()
    Utils.delete(tmpDir)
    TestUtils.clearYammerMetrics()
  }

  @Test
  def testMakeLeaderUpdatesEpochCache(): Unit = {
    val leaderEpoch = 8

    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    log.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 0,
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes)
    ), leaderEpoch = 0)
    log.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 5,
      new SimpleRecord("k3".getBytes, "v3".getBytes),
      new SimpleRecord("k4".getBytes, "v4".getBytes)
    ), leaderEpoch = 5)
    assertEquals(4, log.logEndOffset)

    val partition = setupPartitionWithMocks(leaderEpoch = leaderEpoch, isLeader = true, log = log)
    assertEquals(Some(4), partition.leaderLogIfLocal.map(_.logEndOffset))

    val epochEndOffset = partition.lastOffsetForLeaderEpoch(currentLeaderEpoch = Optional.of[Integer](leaderEpoch),
      leaderEpoch = leaderEpoch, fetchOnlyFromLeader = true)
    assertEquals(4, epochEndOffset.endOffset)
    assertEquals(leaderEpoch, epochEndOffset.leaderEpoch)
  }

  @Test
  def testMakeLeaderDoesNotUpdateEpochCacheForOldFormats(): Unit = {
    val leaderEpoch = 8

    val logConfig = LogConfig(createLogProperties(Map(
      LogConfig.MessageFormatVersionProp -> kafka.api.KAFKA_0_10_2_IV0.shortVersion)))
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    log.appendAsLeader(TestUtils.records(List(
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes)),
      magicValue = RecordVersion.V1.value
    ), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(
      new SimpleRecord("k3".getBytes, "v3".getBytes),
      new SimpleRecord("k4".getBytes, "v4".getBytes)),
      magicValue = RecordVersion.V1.value
    ), leaderEpoch = 5)
    assertEquals(4, log.logEndOffset)

    val partition = setupPartitionWithMocks(leaderEpoch = leaderEpoch, isLeader = true, log = log)
    assertEquals(Some(4), partition.leaderLogIfLocal.map(_.logEndOffset))
    assertEquals(None, log.latestEpoch)

    val epochEndOffset = partition.lastOffsetForLeaderEpoch(currentLeaderEpoch = Optional.of[Integer](leaderEpoch),
      leaderEpoch = leaderEpoch, fetchOnlyFromLeader = true)
    assertEquals(EpochEndOffset.UNDEFINED_EPOCH_OFFSET, epochEndOffset.endOffset)
    assertEquals(EpochEndOffset.UNDEFINED_EPOCH, epochEndOffset.leaderEpoch)
  }

  @Test
  // Verify that partition.removeFutureLocalReplica() and partition.maybeReplaceCurrentWithFutureReplica() can run concurrently
  def testMaybeReplaceCurrentWithFutureReplica(): Unit = {
    val latch = new CountDownLatch(1)

    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir1.getAbsolutePath)
    partition.createLogIfNotExists(brokerId, isNew = true, isFutureReplica = false, offsetCheckpoints)
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir2.getAbsolutePath)
    partition.maybeCreateFutureReplica(logDir2.getAbsolutePath, offsetCheckpoints)

    val thread1 = new Thread {
      override def run(): Unit = {
        latch.await()
        partition.removeFutureLocalReplica()
      }
    }

    val thread2 = new Thread {
      override def run(): Unit = {
        latch.await()
        partition.maybeReplaceCurrentWithFutureReplica()
      }
    }

    thread1.start()
    thread2.start()

    latch.countDown()
    thread1.join()
    thread2.join()
    assertEquals(None, partition.futureLog)
  }

  @Test
  // Verify that replacement works when the replicas have the same log end offset but different base offsets in the
  // active segment
  def testMaybeReplaceCurrentWithFutureReplicaDifferentBaseOffsets(): Unit = {
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir1.getAbsolutePath)
    partition.createLogIfNotExists(brokerId, isNew = true, isFutureReplica = false, offsetCheckpoints)
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir2.getAbsolutePath)
    partition.maybeCreateFutureReplica(logDir2.getAbsolutePath, offsetCheckpoints)

    // Write records with duplicate keys to current replica and roll at offset 6
    val currentLog = partition.log.get
    currentLog.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 0,
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k1".getBytes, "v2".getBytes),
      new SimpleRecord("k1".getBytes, "v3".getBytes),
      new SimpleRecord("k2".getBytes, "v4".getBytes),
      new SimpleRecord("k2".getBytes, "v5".getBytes),
      new SimpleRecord("k2".getBytes, "v6".getBytes)
    ), leaderEpoch = 0)
    currentLog.roll()
    currentLog.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 0,
      new SimpleRecord("k3".getBytes, "v7".getBytes),
      new SimpleRecord("k4".getBytes, "v8".getBytes)
    ), leaderEpoch = 0)

    // Write to the future replica as if the log had been compacted, and do not roll the segment

    val buffer = ByteBuffer.allocate(1024)
    val builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE,
      TimestampType.CREATE_TIME, 0L, RecordBatch.NO_TIMESTAMP, 0)
    builder.appendWithOffset(2L, new SimpleRecord("k1".getBytes, "v3".getBytes))
    builder.appendWithOffset(5L, new SimpleRecord("k2".getBytes, "v6".getBytes))
    builder.appendWithOffset(6L, new SimpleRecord("k3".getBytes, "v7".getBytes))
    builder.appendWithOffset(7L, new SimpleRecord("k4".getBytes, "v8".getBytes))

    val futureLog = partition.futureLocalLogOrException
    futureLog.appendAsFollower(builder.build())

    assertTrue(partition.maybeReplaceCurrentWithFutureReplica())
  }

  @Test
  def testFetchOffsetSnapshotEpochValidationForLeader(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    def assertSnapshotError(expectedError: Errors, currentLeaderEpoch: Optional[Integer]): Unit = {
      try {
        partition.fetchOffsetSnapshot(currentLeaderEpoch, fetchOnlyFromLeader = true)
        assertEquals(Errors.NONE, expectedError)
      } catch {
        case error: ApiException => assertEquals(expectedError, Errors.forException(error))
      }
    }

    assertSnapshotError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1))
    assertSnapshotError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1))
    assertSnapshotError(Errors.NONE, Optional.of(leaderEpoch))
    assertSnapshotError(Errors.NONE, Optional.empty())
  }

  @Test
  def testFetchOffsetSnapshotEpochValidationForFollower(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = false)

    def assertSnapshotError(expectedError: Errors,
                            currentLeaderEpoch: Optional[Integer],
                            fetchOnlyLeader: Boolean): Unit = {
      try {
        partition.fetchOffsetSnapshot(currentLeaderEpoch, fetchOnlyFromLeader = fetchOnlyLeader)
        assertEquals(Errors.NONE, expectedError)
      } catch {
        case error: ApiException => assertEquals(expectedError, Errors.forException(error))
      }
    }

    assertSnapshotError(Errors.NONE, Optional.of(leaderEpoch), fetchOnlyLeader = false)
    assertSnapshotError(Errors.NONE, Optional.empty(), fetchOnlyLeader = false)
    assertSnapshotError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = false)
    assertSnapshotError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = false)

    assertSnapshotError(Errors.NOT_LEADER_FOR_PARTITION, Optional.of(leaderEpoch), fetchOnlyLeader = true)
    assertSnapshotError(Errors.NOT_LEADER_FOR_PARTITION, Optional.empty(), fetchOnlyLeader = true)
    assertSnapshotError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = true)
    assertSnapshotError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = true)
  }

  @Test
  def testOffsetForLeaderEpochValidationForLeader(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    def assertLastOffsetForLeaderError(error: Errors, currentLeaderEpochOpt: Optional[Integer]): Unit = {
      val endOffset = partition.lastOffsetForLeaderEpoch(currentLeaderEpochOpt, 0,
        fetchOnlyFromLeader = true)
      assertEquals(error, endOffset.error)
    }

    assertLastOffsetForLeaderError(Errors.NONE, Optional.empty())
    assertLastOffsetForLeaderError(Errors.NONE, Optional.of(leaderEpoch))
    assertLastOffsetForLeaderError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1))
    assertLastOffsetForLeaderError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1))
  }

  @Test
  def testOffsetForLeaderEpochValidationForFollower(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = false)

    def assertLastOffsetForLeaderError(error: Errors,
                                       currentLeaderEpochOpt: Optional[Integer],
                                       fetchOnlyLeader: Boolean): Unit = {
      val endOffset = partition.lastOffsetForLeaderEpoch(currentLeaderEpochOpt, 0,
        fetchOnlyFromLeader = fetchOnlyLeader)
      assertEquals(error, endOffset.error)
    }

    assertLastOffsetForLeaderError(Errors.NONE, Optional.empty(), fetchOnlyLeader = false)
    assertLastOffsetForLeaderError(Errors.NONE, Optional.of(leaderEpoch), fetchOnlyLeader = false)
    assertLastOffsetForLeaderError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = false)
    assertLastOffsetForLeaderError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = false)

    assertLastOffsetForLeaderError(Errors.NOT_LEADER_FOR_PARTITION, Optional.empty(), fetchOnlyLeader = true)
    assertLastOffsetForLeaderError(Errors.NOT_LEADER_FOR_PARTITION, Optional.of(leaderEpoch), fetchOnlyLeader = true)
    assertLastOffsetForLeaderError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = true)
    assertLastOffsetForLeaderError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = true)
  }

  @Test
  def testReadRecordEpochValidationForLeader(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    def assertReadRecordsError(error: Errors,
                               currentLeaderEpochOpt: Optional[Integer]): Unit = {
      try {
        partition.readRecords(0L, currentLeaderEpochOpt,
          maxBytes = 1024,
          fetchIsolation = FetchLogEnd,
          fetchOnlyFromLeader = true,
          minOneMessage = false)
        if (error != Errors.NONE)
          fail(s"Expected readRecords to fail with error $error")
      } catch {
        case e: Exception =>
          assertEquals(error, Errors.forException(e))
      }
    }

    assertReadRecordsError(Errors.NONE, Optional.empty())
    assertReadRecordsError(Errors.NONE, Optional.of(leaderEpoch))
    assertReadRecordsError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1))
    assertReadRecordsError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1))
  }

  @Test
  def testReadRecordEpochValidationForFollower(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = false)

    def assertReadRecordsError(error: Errors,
                                       currentLeaderEpochOpt: Optional[Integer],
                                       fetchOnlyLeader: Boolean): Unit = {
      try {
        partition.readRecords(0L, currentLeaderEpochOpt,
          maxBytes = 1024,
          fetchIsolation = FetchLogEnd,
          fetchOnlyFromLeader = fetchOnlyLeader,
          minOneMessage = false)
        if (error != Errors.NONE)
          fail(s"Expected readRecords to fail with error $error")
      } catch {
        case e: Exception =>
          assertEquals(error, Errors.forException(e))
      }
    }

    assertReadRecordsError(Errors.NONE, Optional.empty(), fetchOnlyLeader = false)
    assertReadRecordsError(Errors.NONE, Optional.of(leaderEpoch), fetchOnlyLeader = false)
    assertReadRecordsError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = false)
    assertReadRecordsError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = false)

    assertReadRecordsError(Errors.NOT_LEADER_FOR_PARTITION, Optional.empty(), fetchOnlyLeader = true)
    assertReadRecordsError(Errors.NOT_LEADER_FOR_PARTITION, Optional.of(leaderEpoch), fetchOnlyLeader = true)
    assertReadRecordsError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = true)
    assertReadRecordsError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = true)
  }

  @Test
  def testFetchOffsetForTimestampEpochValidationForLeader(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    def assertFetchOffsetError(error: Errors,
                               currentLeaderEpochOpt: Optional[Integer]): Unit = {
      try {
        partition.fetchOffsetForTimestamp(0L,
          isolationLevel = None,
          currentLeaderEpoch = currentLeaderEpochOpt,
          fetchOnlyFromLeader = true)
        if (error != Errors.NONE)
          fail(s"Expected readRecords to fail with error $error")
      } catch {
        case e: Exception =>
          assertEquals(error, Errors.forException(e))
      }
    }

    assertFetchOffsetError(Errors.NONE, Optional.empty())
    assertFetchOffsetError(Errors.NONE, Optional.of(leaderEpoch))
    assertFetchOffsetError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1))
    assertFetchOffsetError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1))
  }

  @Test
  def testFetchOffsetForTimestampEpochValidationForFollower(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = false)

    def assertFetchOffsetError(error: Errors,
                               currentLeaderEpochOpt: Optional[Integer],
                               fetchOnlyLeader: Boolean): Unit = {
      try {
        partition.fetchOffsetForTimestamp(0L,
          isolationLevel = None,
          currentLeaderEpoch = currentLeaderEpochOpt,
          fetchOnlyFromLeader = fetchOnlyLeader)
        if (error != Errors.NONE)
          fail(s"Expected readRecords to fail with error $error")
      } catch {
        case e: Exception =>
          assertEquals(error, Errors.forException(e))
      }
    }

    assertFetchOffsetError(Errors.NONE, Optional.empty(), fetchOnlyLeader = false)
    assertFetchOffsetError(Errors.NONE, Optional.of(leaderEpoch), fetchOnlyLeader = false)
    assertFetchOffsetError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = false)
    assertFetchOffsetError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = false)

    assertFetchOffsetError(Errors.NOT_LEADER_FOR_PARTITION, Optional.empty(), fetchOnlyLeader = true)
    assertFetchOffsetError(Errors.NOT_LEADER_FOR_PARTITION, Optional.of(leaderEpoch), fetchOnlyLeader = true)
    assertFetchOffsetError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = true)
    assertFetchOffsetError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = true)
  }

  @Test
  def testFetchLatestOffsetIncludesLeaderEpoch(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    val timestampAndOffsetOpt = partition.fetchOffsetForTimestamp(ListOffsetRequest.LATEST_TIMESTAMP,
      isolationLevel = None,
      currentLeaderEpoch = Optional.empty(),
      fetchOnlyFromLeader = true)

    assertTrue(timestampAndOffsetOpt.isDefined)

    val timestampAndOffset = timestampAndOffsetOpt.get
    assertEquals(Optional.of(leaderEpoch), timestampAndOffset.leaderEpoch)
  }

  /**
    * This test checks that after a new leader election, we don't answer any ListOffsetsRequest until
    * the HW of the new leader has caught up to its startLogOffset for this epoch. From a client
    * perspective this helps guarantee monotonic offsets
    *
    * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-207%3A+Offsets+returned+by+ListOffsetsResponse+should+be+monotonically+increasing+even+during+a+partition+leader+change">KIP-207</a>
    */
  @Test
  def testMonotonicOffsetsAfterLeaderChange(): Unit = {
    val controllerEpoch = 3
    val leader = brokerId
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val controllerId = brokerId + 3
    val replicas = List(leader, follower1, follower2)
    val isr = List[Integer](leader, follower2).asJava
    val leaderEpoch = 8
    val batch1 = TestUtils.records(records = List(
      new SimpleRecord(10, "k1".getBytes, "v1".getBytes),
      new SimpleRecord(11,"k2".getBytes, "v2".getBytes)))
    val batch2 = TestUtils.records(records = List(new SimpleRecord("k3".getBytes, "v1".getBytes),
      new SimpleRecord(20,"k4".getBytes, "v2".getBytes),
      new SimpleRecord(21,"k5".getBytes, "v3".getBytes)))

    val leaderState = new LeaderAndIsrRequest.PartitionState(
      controllerEpoch, leader, leaderEpoch, isr, 1, replicas.map(Int.box).asJava, true
    )

    assertTrue("Expected first makeLeader() to return 'leader changed'",
      partition.makeLeader(controllerId, leaderState, 0, offsetCheckpoints))
    assertEquals("Current leader epoch", leaderEpoch, partition.getLeaderEpoch)
    assertEquals("ISR", Set[Integer](leader, follower2), partition.inSyncReplicaIds)

    // after makeLeader(() call, partition should know about all the replicas
    // append records with initial leader epoch
    partition.appendRecordsToLeader(batch1, isFromClient = true)
    partition.appendRecordsToLeader(batch2, isFromClient = true)
    assertEquals("Expected leader's HW not move", partition.localLogOrException.logStartOffset,
      partition.localLogOrException.highWatermark)

    // let the follower in ISR move leader's HW to move further but below LEO
    def updateFollowerFetchState(followerId: Int, fetchOffsetMetadata: LogOffsetMetadata): Unit = {
      partition.updateFollowerFetchState(
        followerId,
        followerFetchOffsetMetadata = fetchOffsetMetadata,
        followerStartOffset = 0L,
        followerFetchTimeMs = time.milliseconds(),
        leaderEndOffset = partition.localLogOrException.logEndOffset)
    }

    def fetchOffsetsForTimestamp(timestamp: Long, isolation: Option[IsolationLevel]): Either[ApiException, Option[TimestampAndOffset]] = {
      try {
        Right(partition.fetchOffsetForTimestamp(
          timestamp = timestamp,
          isolationLevel = isolation,
          currentLeaderEpoch = Optional.of(partition.getLeaderEpoch),
          fetchOnlyFromLeader = true
        ))
      } catch {
        case e: ApiException => Left(e)
      }
    }

    when(stateStore.expandIsr(controllerEpoch, new LeaderAndIsr(leader, leaderEpoch,
      List(leader, follower2, follower1), 1)))
      .thenReturn(Some(2))

    updateFollowerFetchState(follower1, LogOffsetMetadata(0))
    updateFollowerFetchState(follower1, LogOffsetMetadata(2))

    updateFollowerFetchState(follower2, LogOffsetMetadata(0))
    updateFollowerFetchState(follower2, LogOffsetMetadata(2))

    // At this point, the leader has gotten 5 writes, but followers have only fetched two
    assertEquals(2, partition.localLogOrException.highWatermark)

    // Get the LEO
    fetchOffsetsForTimestamp(ListOffsetRequest.LATEST_TIMESTAMP, None) match {
      case Right(Some(offsetAndTimestamp)) => assertEquals(5, offsetAndTimestamp.offset)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e) => fail("Should not have seen an error")
    }

    // Get the HW
    fetchOffsetsForTimestamp(ListOffsetRequest.LATEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => assertEquals(2, offsetAndTimestamp.offset)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e) => fail("Should not have seen an error")
    }

    // Get a offset beyond the HW by timestamp, get a None
    assertEquals(Right(None), fetchOffsetsForTimestamp(30, Some(IsolationLevel.READ_UNCOMMITTED)))

    // Make into a follower
    val followerState = new LeaderAndIsrRequest.PartitionState(
      controllerEpoch, follower2, leaderEpoch + 1, isr, 4, replicas.map(Int.box).asJava, false
    )
    assertTrue(partition.makeFollower(controllerId, followerState, 1, offsetCheckpoints))

    // Back to leader, this resets the startLogOffset for this epoch (to 2), we're now in the fault condition
    val newLeaderState = new LeaderAndIsrRequest.PartitionState(
      controllerEpoch, leader, leaderEpoch + 2, isr, 5, replicas.map(Int.box).asJava, false
    )
    assertTrue(partition.makeLeader(controllerId, newLeaderState, 2, offsetCheckpoints))

    // Try to get offsets as a client
    fetchOffsetsForTimestamp(ListOffsetRequest.LATEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => fail("Should have failed with OffsetNotAvailable")
      case Right(None) => fail("Should have seen an error")
      case Left(e: OffsetNotAvailableException) => // ok
      case Left(e: ApiException) => fail(s"Expected OffsetNotAvailableException, got $e")
    }

    // If request is not from a client, we skip the check
    fetchOffsetsForTimestamp(ListOffsetRequest.LATEST_TIMESTAMP, None) match {
      case Right(Some(offsetAndTimestamp)) => assertEquals(5, offsetAndTimestamp.offset)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e: ApiException) => fail(s"Got ApiException $e")
    }

    // If we request the earliest timestamp, we skip the check
    fetchOffsetsForTimestamp(ListOffsetRequest.EARLIEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => assertEquals(0, offsetAndTimestamp.offset)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e: ApiException) => fail(s"Got ApiException $e")
    }

    // If we request an offset by timestamp earlier than the HW, we are ok
    fetchOffsetsForTimestamp(11, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) =>
        assertEquals(1, offsetAndTimestamp.offset)
        assertEquals(11, offsetAndTimestamp.timestamp)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e: ApiException) => fail(s"Got ApiException $e")
    }

    // Request an offset by timestamp beyond the HW, get an error now since we're in a bad state
    fetchOffsetsForTimestamp(100, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => fail("Should have failed")
      case Right(None) => fail("Should have failed")
      case Left(e: OffsetNotAvailableException) => // ok
      case Left(e: ApiException) => fail(s"Should have seen OffsetNotAvailableException, saw $e")
    }

    when(stateStore.expandIsr(controllerEpoch, new LeaderAndIsr(leader, leaderEpoch + 2,
      List(leader, follower2, follower1), 5)))
      .thenReturn(Some(2))

    // Next fetch from replicas, HW is moved up to 5 (ahead of the LEO)
    updateFollowerFetchState(follower1, LogOffsetMetadata(5))
    updateFollowerFetchState(follower2, LogOffsetMetadata(5))

    // Error goes away
    fetchOffsetsForTimestamp(ListOffsetRequest.LATEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => assertEquals(5, offsetAndTimestamp.offset)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e: ApiException) => fail(s"Got ApiException $e")
    }

    // Now we see None instead of an error for out of range timestamp
    assertEquals(Right(None), fetchOffsetsForTimestamp(100, Some(IsolationLevel.READ_UNCOMMITTED)))
  }

  private def setupPartitionWithMocks(leaderEpoch: Int,
                                      isLeader: Boolean,
                                      log: Log = logManager.getOrCreateLog(topicPartition, logConfig)): Partition = {
    partition.createLogIfNotExists(brokerId, isNew = false, isFutureReplica = false, offsetCheckpoints)

    val controllerId = 0
    val controllerEpoch = 0
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val isr = replicas

    if (isLeader) {
      assertTrue("Expected become leader transition to succeed",
        partition.makeLeader(controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, brokerId,
          leaderEpoch, isr, 1, replicas, true), 0, offsetCheckpoints))
      assertEquals(leaderEpoch, partition.getLeaderEpoch)
    } else {
      assertTrue("Expected become follower transition to succeed",
        partition.makeFollower(controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, brokerId + 1,
          leaderEpoch, isr, 1, replicas, true), 0, offsetCheckpoints))
      assertEquals(leaderEpoch, partition.getLeaderEpoch)
      assertEquals(None, partition.leaderLogIfLocal)
    }

    partition
  }

  @Test
  def testAppendRecordsAsFollowerBelowLogStartOffset(): Unit = {
    partition.createLogIfNotExists(brokerId, isNew = false, isFutureReplica = false, offsetCheckpoints)
    val log = partition.localLogOrException

    val initialLogStartOffset = 5L
    partition.truncateFullyAndStartAt(initialLogStartOffset, isFuture = false)
    assertEquals(s"Log end offset after truncate fully and start at $initialLogStartOffset:",
                 initialLogStartOffset, log.logEndOffset)
    assertEquals(s"Log start offset after truncate fully and start at $initialLogStartOffset:",
                 initialLogStartOffset, log.logStartOffset)

    // verify that we cannot append records that do not contain log start offset even if the log is empty
    assertThrows[UnexpectedAppendOffsetException] {
      // append one record with offset = 3
      partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 3L), isFuture = false)
    }
    assertEquals(s"Log end offset should not change after failure to append", initialLogStartOffset, log.logEndOffset)

    // verify that we can append records that contain log start offset, even when first
    // offset < log start offset if the log is empty
    val newLogStartOffset = 4L
    val records = createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                     new SimpleRecord("k2".getBytes, "v2".getBytes),
                                     new SimpleRecord("k3".getBytes, "v3".getBytes)),
                                baseOffset = newLogStartOffset)
    partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false)
    assertEquals(s"Log end offset after append of 3 records with base offset $newLogStartOffset:", 7L, log.logEndOffset)
    assertEquals(s"Log start offset after append of 3 records with base offset $newLogStartOffset:", newLogStartOffset, log.logStartOffset)

    // and we can append more records after that
    partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 7L), isFuture = false)
    assertEquals(s"Log end offset after append of 1 record at offset 7:", 8L, log.logEndOffset)
    assertEquals(s"Log start offset not expected to change:", newLogStartOffset, log.logStartOffset)

    // but we cannot append to offset < log start if the log is not empty
    assertThrows[UnexpectedAppendOffsetException] {
      val records2 = createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                        new SimpleRecord("k2".getBytes, "v2".getBytes)),
                                   baseOffset = 3L)
      partition.appendRecordsToFollowerOrFutureReplica(records2, isFuture = false)
    }
    assertEquals(s"Log end offset should not change after failure to append", 8L, log.logEndOffset)

    // we still can append to next offset
    partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 8L), isFuture = false)
    assertEquals(s"Log end offset after append of 1 record at offset 8:", 9L, log.logEndOffset)
    assertEquals(s"Log start offset not expected to change:", newLogStartOffset, log.logStartOffset)
  }

  @Test
  def testListOffsetIsolationLevels(): Unit = {
    val controllerId = 0
    val controllerEpoch = 0
    val leaderEpoch = 5
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val isr = replicas

    doNothing().when(delayedOperations).checkAndCompleteFetch()

    partition.createLogIfNotExists(brokerId, isNew = false, isFutureReplica = false, offsetCheckpoints)

    assertTrue("Expected become leader transition to succeed",
      partition.makeLeader(controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, brokerId,
        leaderEpoch, isr, 1, replicas, true), 0, offsetCheckpoints))
    assertEquals(leaderEpoch, partition.getLeaderEpoch)

    val records = createTransactionalRecords(List(
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes),
      new SimpleRecord("k3".getBytes, "v3".getBytes)),
      baseOffset = 0L)
    partition.appendRecordsToLeader(records, isFromClient = true)

    def fetchLatestOffset(isolationLevel: Option[IsolationLevel]): TimestampAndOffset = {
      val res = partition.fetchOffsetForTimestamp(ListOffsetRequest.LATEST_TIMESTAMP,
        isolationLevel = isolationLevel,
        currentLeaderEpoch = Optional.empty(),
        fetchOnlyFromLeader = true)
      assertTrue(res.isDefined)
      res.get
    }

    def fetchEarliestOffset(isolationLevel: Option[IsolationLevel]): TimestampAndOffset = {
      val res = partition.fetchOffsetForTimestamp(ListOffsetRequest.EARLIEST_TIMESTAMP,
        isolationLevel = isolationLevel,
        currentLeaderEpoch = Optional.empty(),
        fetchOnlyFromLeader = true)
      assertTrue(res.isDefined)
      res.get
    }

    assertEquals(3L, fetchLatestOffset(isolationLevel = None).offset)
    assertEquals(0L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_UNCOMMITTED)).offset)
    assertEquals(0L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_COMMITTED)).offset)

    partition.log.get.updateHighWatermark(1L)

    assertEquals(3L, fetchLatestOffset(isolationLevel = None).offset)
    assertEquals(1L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_UNCOMMITTED)).offset)
    assertEquals(0L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_COMMITTED)).offset)

    assertEquals(0L, fetchEarliestOffset(isolationLevel = None).offset)
    assertEquals(0L, fetchEarliestOffset(isolationLevel = Some(IsolationLevel.READ_UNCOMMITTED)).offset)
    assertEquals(0L, fetchEarliestOffset(isolationLevel = Some(IsolationLevel.READ_COMMITTED)).offset)
  }

  @Test
  def testGetReplica(): Unit = {
    assertEquals(None, partition.log)
    assertThrows[ReplicaNotAvailableException] {
      partition.localLogOrException
    }
  }

  @Test
  def testAppendRecordsToFollowerWithNoReplicaThrowsException(): Unit = {
    assertThrows[ReplicaNotAvailableException] {
      partition.appendRecordsToFollowerOrFutureReplica(
           createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 0L), isFuture = false)
    }
  }

  @Test
  def testMakeFollowerWithNoLeaderIdChange(): Unit = {
    // Start off as follower
    var partitionStateInfo = new LeaderAndIsrRequest.PartitionState(0, 1, 1,
      List[Integer](0, 1, 2, brokerId).asJava, 1, List[Integer](0, 1, 2, brokerId).asJava, false)
    partition.makeFollower(0, partitionStateInfo, 0, offsetCheckpoints)

    // Request with same leader and epoch increases by only 1, do become-follower steps
    partitionStateInfo = new LeaderAndIsrRequest.PartitionState(0, 1, 4,
      List[Integer](0, 1, 2, brokerId).asJava, 1, List[Integer](0, 1, 2, brokerId).asJava, false)
    assertTrue(partition.makeFollower(0, partitionStateInfo, 2, offsetCheckpoints))

    // Request with same leader and same epoch, skip become-follower steps
    partitionStateInfo = new LeaderAndIsrRequest.PartitionState(0, 1, 4,
      List[Integer](0, 1, 2, brokerId).asJava, 1, List[Integer](0, 1, 2, brokerId).asJava, false)
    assertFalse(partition.makeFollower(0, partitionStateInfo, 2, offsetCheckpoints))
  }

  @Test
  def testFollowerDoesNotJoinISRUntilCaughtUpToOffsetWithinCurrentLeaderEpoch(): Unit = {
    val controllerEpoch = 3
    val leader = brokerId
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val controllerId = brokerId + 3
    val replicas = List[Integer](leader, follower1, follower2).asJava
    val isr = List[Integer](leader, follower2).asJava
    val leaderEpoch = 8
    val batch1 = TestUtils.records(records = List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                                  new SimpleRecord("k2".getBytes, "v2".getBytes)))
    val batch2 = TestUtils.records(records = List(new SimpleRecord("k3".getBytes, "v1".getBytes),
                                                  new SimpleRecord("k4".getBytes, "v2".getBytes),
                                                  new SimpleRecord("k5".getBytes, "v3".getBytes)))
    val batch3 = TestUtils.records(records = List(new SimpleRecord("k6".getBytes, "v1".getBytes),
                                                  new SimpleRecord("k7".getBytes, "v2".getBytes)))

    val leaderState = new LeaderAndIsrRequest.PartitionState(controllerEpoch, leader, leaderEpoch, isr, 1, replicas, true)
    assertTrue("Expected first makeLeader() to return 'leader changed'",
               partition.makeLeader(controllerId, leaderState, 0, offsetCheckpoints))
    assertEquals("Current leader epoch", leaderEpoch, partition.getLeaderEpoch)
    assertEquals("ISR", Set[Integer](leader, follower2), partition.inSyncReplicaIds)

    // after makeLeader(() call, partition should know about all the replicas
    // append records with initial leader epoch
    val lastOffsetOfFirstBatch = partition.appendRecordsToLeader(batch1, isFromClient = true).lastOffset
    partition.appendRecordsToLeader(batch2, isFromClient = true)
    assertEquals("Expected leader's HW not move", partition.localLogOrException.logStartOffset,
      partition.log.get.highWatermark)

    // let the follower in ISR move leader's HW to move further but below LEO
    def updateFollowerFetchState(followerId: Int, fetchOffsetMetadata: LogOffsetMetadata): Unit = {
      partition.updateFollowerFetchState(
        followerId,
        followerFetchOffsetMetadata = fetchOffsetMetadata,
        followerStartOffset = 0L,
        followerFetchTimeMs = time.milliseconds(),
        leaderEndOffset = partition.localLogOrException.logEndOffset)
    }

    updateFollowerFetchState(follower2, LogOffsetMetadata(0))
    updateFollowerFetchState(follower2, LogOffsetMetadata(lastOffsetOfFirstBatch))
    assertEquals("Expected leader's HW", lastOffsetOfFirstBatch, partition.log.get.highWatermark)

    // current leader becomes follower and then leader again (without any new records appended)
    val followerState = new LeaderAndIsrRequest.PartitionState(controllerEpoch, follower2, leaderEpoch + 1, isr, 1,
      replicas, false)
    partition.makeFollower(controllerId, followerState, 1, offsetCheckpoints)

    val newLeaderState = new LeaderAndIsrRequest.PartitionState(controllerEpoch, leader, leaderEpoch + 2, isr, 1,
      replicas, false)
    assertTrue("Expected makeLeader() to return 'leader changed' after makeFollower()",
               partition.makeLeader(controllerEpoch, newLeaderState, 2, offsetCheckpoints))
    val currentLeaderEpochStartOffset = partition.localLogOrException.logEndOffset

    // append records with the latest leader epoch
    partition.appendRecordsToLeader(batch3, isFromClient = true)

    // fetch from follower not in ISR from log start offset should not add this follower to ISR
    updateFollowerFetchState(follower1, LogOffsetMetadata(0))
    updateFollowerFetchState(follower1, LogOffsetMetadata(lastOffsetOfFirstBatch))
    assertEquals("ISR", Set[Integer](leader, follower2), partition.inSyncReplicaIds)

    // fetch from the follower not in ISR from start offset of the current leader epoch should
    // add this follower to ISR
    when(stateStore.expandIsr(controllerEpoch, new LeaderAndIsr(leader, leaderEpoch + 2,
      List(leader, follower2, follower1), 1))).thenReturn(Some(2))
    updateFollowerFetchState(follower1, LogOffsetMetadata(currentLeaderEpochStartOffset))
    assertEquals("ISR", Set[Integer](leader, follower1, follower2), partition.inSyncReplicaIds)
  }

  /**
   * Verify that delayed fetch operations which are completed when records are appended don't result in deadlocks.
   * Delayed fetch operations acquire Partition leaderIsrUpdate read lock for one or more partitions. So they
   * need to be completed after releasing the lock acquired to append records. Otherwise, waiting writers
   * (e.g. to check if ISR needs to be shrinked) can trigger deadlock in request handler threads waiting for
   * read lock of one Partition while holding on to read lock of another Partition.
   */
  @Test
  def testDelayedFetchAfterAppendRecords(): Unit = {
    val controllerId = 0
    val controllerEpoch = 0
    val leaderEpoch = 5
    val replicaIds = List[Integer](brokerId, brokerId + 1).asJava
    val isr = replicaIds
    val logConfig = LogConfig(new Properties)

    val topicPartitions = (0 until 5).map { i => new TopicPartition("test-topic", i) }
    val logs = topicPartitions.map { tp => logManager.getOrCreateLog(tp, logConfig) }
    val partitions = ListBuffer.empty[Partition]

    logs.foreach { log =>
      val tp = log.topicPartition
      val delayedOperations: DelayedOperations = mock(classOf[DelayedOperations])
      val partition = new Partition(tp,
        replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
        interBrokerProtocolVersion = ApiVersion.latestVersion,
        localBrokerId = brokerId,
        time,
        stateStore,
        delayedOperations,
        metadataCache,
        logManager)

      when(delayedOperations.checkAndCompleteFetch())
        .thenAnswer(new Answer[Unit] {
          override def answer(invocation: InvocationOnMock): Unit = {
            // Acquire leaderIsrUpdate read lock of a different partition when completing delayed fetch
            val anotherPartition = (tp.partition + 1) % topicPartitions.size
            val partition = partitions(anotherPartition)
            partition.fetchOffsetSnapshot(Optional.of(leaderEpoch), fetchOnlyFromLeader = true)
          }
        })

      partition.setLog(log, isFutureLog = false)
      val leaderState = new LeaderAndIsrRequest.PartitionState(controllerEpoch, brokerId,
        leaderEpoch, isr, 1, replicaIds, true)
      partition.makeLeader(controllerId, leaderState, 0, offsetCheckpoints)
      partitions += partition
    }

    def createRecords(baseOffset: Long): MemoryRecords = {
      val records = List(
        new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes))
      val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
      val builder = MemoryRecords.builder(
        buf, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, TimestampType.CREATE_TIME,
        baseOffset, time.milliseconds, 0)
      records.foreach(builder.append)
      builder.build()
    }

    val done = new AtomicBoolean()
    val executor = Executors.newFixedThreadPool(topicPartitions.size + 1)
    try {
      // Invoke some operation that acquires leaderIsrUpdate write lock on one thread
      executor.submit(CoreUtils.runnable {
        while (!done.get) {
          partitions.foreach(_.maybeShrinkIsr(10000))
        }
      })
      // Append records to partitions, one partition-per-thread
      val futures = partitions.map { partition =>
        executor.submit(CoreUtils.runnable {
          (1 to 10000).foreach { _ => partition.appendRecordsToLeader(createRecords(baseOffset = 0), isFromClient = true) }
        })
      }
      futures.foreach(_.get(15, TimeUnit.SECONDS))
      done.set(true)
    } catch {
      case e: TimeoutException =>
        val allThreads = TestUtils.allThreadStackTraces()
        fail(s"Test timed out with exception $e, thread stack traces: $allThreads")
    } finally {
      executor.shutdownNow()
      executor.awaitTermination(5, TimeUnit.SECONDS)
    }
  }

  def createRecords(records: Iterable[SimpleRecord], baseOffset: Long, partitionLeaderEpoch: Int = 0): MemoryRecords = {
    val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
    val builder = MemoryRecords.builder(
      buf, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, TimestampType.LOG_APPEND_TIME,
      baseOffset, time.milliseconds, partitionLeaderEpoch)
    records.foreach(builder.append)
    builder.build()
  }

  def createTransactionalRecords(records: Iterable[SimpleRecord],
                                 baseOffset: Long,
                                 partitionLeaderEpoch: Int = 0): MemoryRecords = {
    val producerId = 1L
    val producerEpoch = 0.toShort
    val baseSequence = 0
    val isTransactional = true
    val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
    val builder = MemoryRecords.builder(buf, CompressionType.NONE, baseOffset, producerId,
      producerEpoch, baseSequence, isTransactional)
    records.foreach(builder.append)
    builder.build()
  }

  /**
    * Test for AtMinIsr partition state. We set the partition replica set size as 3, but only set one replica as an ISR.
    * As the default minIsr configuration is 1, then the partition should be at min ISR (isAtMinIsr = true).
    */
  @Test
  def testAtMinIsr(): Unit = {
    val controllerEpoch = 3
    val leader = brokerId
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val controllerId = brokerId + 3
    val replicas = List[Integer](leader, follower1, follower2).asJava
    val isr = List[Integer](leader).asJava
    val leaderEpoch = 8

    assertFalse(partition.isAtMinIsr)
    // Make isr set to only have leader to trigger AtMinIsr (default min isr config is 1)
    val leaderState = new LeaderAndIsrRequest.PartitionState(controllerEpoch, leader, leaderEpoch, isr, 1, replicas, true)
    partition.makeLeader(controllerId, leaderState, 0, offsetCheckpoints)
    assertTrue(partition.isAtMinIsr)
  }

  @Test
  def testUpdateFollowerFetchState(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    seedLogData(log, numRecords = 6, leaderEpoch = 4)

    val controllerId = 0
    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List[Integer](brokerId, remoteBrokerId).asJava
    val isr = replicas

    doNothing().when(delayedOperations).checkAndCompleteFetch()

    partition.createLogIfNotExists(brokerId, isNew = false, isFutureReplica = false, offsetCheckpoints)

    val initializeTimeMs = time.milliseconds()
    assertTrue("Expected become leader transition to succeed",
      partition.makeLeader(controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, brokerId,
        leaderEpoch, isr, 1, replicas, true), 0, offsetCheckpoints))

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(Log.UnknownOffset, remoteReplica.logStartOffset)

    time.sleep(500)

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(3),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 6L)

    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(3L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    time.sleep(500)

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(6L),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 6L)

    assertEquals(time.milliseconds(), remoteReplica.lastCaughtUpTimeMs)
    assertEquals(6L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

  }

  @Test
  def testIsrExpansion(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerId = 0
    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List(brokerId, remoteBrokerId)
    val isr = List[Integer](brokerId).asJava

    doNothing().when(delayedOperations).checkAndCompleteFetch()

    partition.createLogIfNotExists(brokerId, isNew = false, isFutureReplica = false, offsetCheckpoints)
    assertTrue(
      "Expected become leader transition to succeed",
      partition.makeLeader(
        controllerId,
        new LeaderAndIsrRequest.PartitionState(
          controllerEpoch, brokerId, leaderEpoch, isr, 1, replicas.map(Int.box).asJava, true),
        0,
        offsetCheckpoints
      )
    )
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(Log.UnknownOffset, remoteReplica.logStartOffset)

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(3),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 6L)

    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(3L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    // The next update should bring the follower back into the ISR
    val updatedLeaderAndIsr = LeaderAndIsr(
      leader = brokerId,
      leaderEpoch = leaderEpoch,
      isr = List(brokerId, remoteBrokerId),
      zkVersion = 1)
    when(stateStore.expandIsr(controllerEpoch, updatedLeaderAndIsr)).thenReturn(Some(2))

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 6L)

    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(10L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)
  }

  @Test
  def testIsrNotExpandedIfUpdateFails(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerId = 0
    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List[Integer](brokerId, remoteBrokerId).asJava
    val isr = List[Integer](brokerId).asJava

    doNothing().when(delayedOperations).checkAndCompleteFetch()

    partition.createLogIfNotExists(brokerId, isNew = false, isFutureReplica = false, offsetCheckpoints)
    assertTrue("Expected become leader transition to succeed",
      partition.makeLeader(controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, brokerId,
        leaderEpoch, isr, 1, replicas, true), 0, offsetCheckpoints))
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(Log.UnknownOffset, remoteReplica.logStartOffset)

    // Mock the expected ISR update failure
    val updatedLeaderAndIsr = LeaderAndIsr(
      leader = brokerId,
      leaderEpoch = leaderEpoch,
      isr = List(brokerId, remoteBrokerId),
      zkVersion = 1)
    when(stateStore.expandIsr(controllerEpoch, updatedLeaderAndIsr)).thenReturn(None)

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 10L)

    // Follower state is updated, but the ISR has not expanded
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(10L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)
  }

  @Test
  def testMaybeShrinkIsr(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerId = 0
    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List(brokerId, remoteBrokerId)
    val isr = List[Integer](brokerId, remoteBrokerId).asJava

    doNothing().when(delayedOperations).checkAndCompleteFetch()

    val initializeTimeMs = time.milliseconds()
    partition.createLogIfNotExists(brokerId, isNew = false, isFutureReplica = false, offsetCheckpoints)
    assertTrue(
      "Expected become leader transition to succeed",
      partition.makeLeader(
        controllerId,
        new LeaderAndIsrRequest.PartitionState(
          controllerEpoch, brokerId, leaderEpoch, isr, 1, replicas.map(Int.box).asJava, true),
        0,
        offsetCheckpoints
      )
    )
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(Log.UnknownOffset, remoteReplica.logStartOffset)

    // On initialization, the replica is considered caught up and should not be removed
    partition.maybeShrinkIsr(10000)
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)

    // If enough time passes without a fetch update, the ISR should shrink
    time.sleep(10001)
    val updatedLeaderAndIsr = LeaderAndIsr(
      leader = brokerId,
      leaderEpoch = leaderEpoch,
      isr = List(brokerId),
      zkVersion = 1)
    when(stateStore.shrinkIsr(controllerEpoch, updatedLeaderAndIsr)).thenReturn(Some(2))

    partition.maybeShrinkIsr(10000)
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(10L, partition.localLogOrException.highWatermark)
  }

  @Test
  def testShouldNotShrinkIsrIfPreviousFetchIsCaughtUp(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerId = 0
    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List(brokerId, remoteBrokerId)
    val isr = List[Integer](brokerId, remoteBrokerId).asJava

    doNothing().when(delayedOperations).checkAndCompleteFetch()

    val initializeTimeMs = time.milliseconds()
    partition.createLogIfNotExists(brokerId, isNew = false, isFutureReplica = false, offsetCheckpoints)
    assertTrue(
      "Expected become leader transition to succeed",
      partition.makeLeader(
        controllerId,
        new LeaderAndIsrRequest.PartitionState(
          controllerEpoch, brokerId, leaderEpoch, isr, 1, replicas.map(Int.box).asJava, true),
        0,
        offsetCheckpoints
      )
    )
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(Log.UnknownOffset, remoteReplica.logStartOffset)

    // There is a short delay before the first fetch. The follower is not yet caught up to the log end.
    time.sleep(5000)
    val firstFetchTimeMs = time.milliseconds()
    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(5),
      followerStartOffset = 0L,
      followerFetchTimeMs = firstFetchTimeMs,
      leaderEndOffset = 10L)
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(5L, partition.localLogOrException.highWatermark)
    assertEquals(5L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    // Some new data is appended, but the follower catches up to the old end offset.
    // The total elapsed time from initialization is larger than the max allowed replica lag.
    time.sleep(5001)
    seedLogData(log, numRecords = 5, leaderEpoch = leaderEpoch)
    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 15L)
    assertEquals(firstFetchTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(10L, partition.localLogOrException.highWatermark)
    assertEquals(10L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    // The ISR should not be shrunk because the follower has caught up with the leader at the
    // time of the first fetch.
    partition.maybeShrinkIsr(10000)
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
  }

  @Test
  def testShouldNotShrinkIsrIfFollowerCaughtUpToLogEnd(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerId = 0
    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List(brokerId, remoteBrokerId)
    val isr = List[Integer](brokerId, remoteBrokerId).asJava

    doNothing().when(delayedOperations).checkAndCompleteFetch()

    val initializeTimeMs = time.milliseconds()
    partition.createLogIfNotExists(brokerId, isNew = false, isFutureReplica = false, offsetCheckpoints)
    assertTrue(
      "Expected become leader transition to succeed",
      partition.makeLeader(
        controllerId,
        new LeaderAndIsrRequest.PartitionState(
          controllerEpoch, brokerId, leaderEpoch, isr, 1, replicas.map(Int.box).asJava, true),
        0,
        offsetCheckpoints
      )
    )
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(Log.UnknownOffset, remoteReplica.logStartOffset)

    // The follower catches up to the log end immediately.
    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 10L)
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(10L, partition.localLogOrException.highWatermark)
    assertEquals(10L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    // Sleep longer than the max allowed follower lag
    time.sleep(10001)

    // The ISR should not be shrunk because the follower is caught up to the leader's log end
    partition.maybeShrinkIsr(10000)
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
  }

  @Test
  def testIsrNotShrunkIfUpdateFails(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerId = 0
    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List[Integer](brokerId, remoteBrokerId).asJava
    val isr = List[Integer](brokerId, remoteBrokerId).asJava

    doNothing().when(delayedOperations).checkAndCompleteFetch()

    val initializeTimeMs = time.milliseconds()
    partition.createLogIfNotExists(brokerId, isNew = false, isFutureReplica = false, offsetCheckpoints)
    assertTrue("Expected become leader transition to succeed",
      partition.makeLeader(controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, brokerId,
        leaderEpoch, isr, 1, replicas, true), 0, offsetCheckpoints))
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(Log.UnknownOffset, remoteReplica.logStartOffset)

    time.sleep(10001)

    // Mock the expected ISR update failure
    val updatedLeaderAndIsr = LeaderAndIsr(
      leader = brokerId,
      leaderEpoch = leaderEpoch,
      isr = List(brokerId),
      zkVersion = 1)
    when(stateStore.shrinkIsr(controllerEpoch, updatedLeaderAndIsr)).thenReturn(None)

    partition.maybeShrinkIsr(10000)
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(0L, partition.localLogOrException.highWatermark)
  }

  @Test
  def testUseCheckpointToInitializeHighWatermark(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    seedLogData(log, numRecords = 6, leaderEpoch = 5)

    when(offsetCheckpoints.fetch(logDir1.getAbsolutePath, topicPartition))
      .thenReturn(Some(4L))

    val controllerId = 0
    val controllerEpoch = 3
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val leaderState = new LeaderAndIsrRequest.PartitionState(controllerEpoch, brokerId,
      6, replicas, 1, replicas, false)
    partition.makeLeader(controllerId, leaderState, 0, offsetCheckpoints)
    assertEquals(4, partition.localLogOrException.highWatermark)
  }

  @Test
  def testAddAndRemoveMetrics(): Unit = {
    val metricsToCheck = List(
      "UnderReplicated",
      "UnderMinIsr",
      "InSyncReplicasCount",
      "ReplicasCount",
      "LastStableOffsetLag",
      "AtMinIsr")

    def getMetric(metric: String): Option[Metric] = {
      Metrics.defaultRegistry().allMetrics().asScala.filterKeys { metricName =>
        metricName.getName == metric && metricName.getType == "Partition"
      }.headOption.map(_._2)
    }

    assertTrue(metricsToCheck.forall(getMetric(_).isDefined))

    Partition.removeMetrics(topicPartition)

    assertEquals(Set(), Metrics.defaultRegistry().allMetrics().asScala.keySet.filter(_.getType == "Partition"))
  }

  private def seedLogData(log: Log, numRecords: Int, leaderEpoch: Int): Unit = {
    for (i <- 0 until numRecords) {
      val records = MemoryRecords.withRecords(0L, CompressionType.NONE, leaderEpoch,
        new SimpleRecord(s"k$i".getBytes, s"v$i".getBytes))
      log.appendAsLeader(records, leaderEpoch)
    }
  }

}
