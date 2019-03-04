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

import kafka.api.{ApiVersion, Request}
import kafka.common.UnexpectedAppendOffsetException
import kafka.log.{Defaults => _, _}
import kafka.server._
import kafka.utils.{CoreUtils, MockScheduler, MockTime, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ReplicaNotAvailableException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{EpochEndOffset, IsolationLevel, LeaderAndIsrRequest, ListOffsetRequest}
import org.junit.{After, Before, Test}
import org.junit.Assert._
import org.scalatest.Assertions.assertThrows
import org.easymock.{Capture, EasyMock, IAnswer}

import scala.collection.JavaConverters._

class PartitionTest {

  val brokerId = 101
  val topicPartition = new TopicPartition("test-topic", 0)
  val time = new MockTime()
  val brokerTopicStats = new BrokerTopicStats
  val metrics = new Metrics

  var tmpDir: File = _
  var logDir1: File = _
  var logDir2: File = _
  var replicaManager: ReplicaManager = _
  var logManager: LogManager = _
  var logConfig: LogConfig = _

  @Before
  def setup(): Unit = {
    val logProps = createLogProperties(Map.empty)
    logConfig = LogConfig(logProps)

    tmpDir = TestUtils.tempDir()
    logDir1 = TestUtils.randomPartitionLogDir(tmpDir)
    logDir2 = TestUtils.randomPartitionLogDir(tmpDir)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, CleanerConfig(enableCleaner = false), time)
    logManager.startup()

    val brokerProps = TestUtils.createBrokerConfig(brokerId, TestUtils.MockZkConnect)
    brokerProps.put(KafkaConfig.LogDirsProp, Seq(logDir1, logDir2).map(_.getAbsolutePath).mkString(","))
    val brokerConfig = KafkaConfig.fromProps(brokerProps)
    val kafkaZkClient: KafkaZkClient = EasyMock.createMock(classOf[KafkaZkClient])
    replicaManager = new ReplicaManager(
      config = brokerConfig, metrics, time, zkClient = kafkaZkClient, new MockScheduler(time),
      logManager, new AtomicBoolean(false), QuotaFactory.instantiate(brokerConfig, metrics, time, ""),
      brokerTopicStats, new MetadataCache(brokerId), new LogDirFailureChannel(brokerConfig.logDirs.size))

    EasyMock.expect(kafkaZkClient.getEntityConfigs(EasyMock.anyString(), EasyMock.anyString())).andReturn(logProps).anyTimes()
    EasyMock.expect(kafkaZkClient.conditionalUpdatePath(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
      .andReturn((true, 0)).anyTimes()
    EasyMock.replay(kafkaZkClient)
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
    brokerTopicStats.close()
    metrics.close()

    logManager.shutdown()
    Utils.delete(tmpDir)
    logManager.liveLogDirs.foreach(Utils.delete)
    replicaManager.shutdown(checkpointHW = false)
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
    assertEquals(Some(4), partition.leaderReplicaIfLocal.map(_.logEndOffset.messageOffset))

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
    assertEquals(Some(4), partition.leaderReplicaIfLocal.map(_.logEndOffset.messageOffset))
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
    val log1 = logManager.getOrCreateLog(topicPartition, logConfig)
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir2.getAbsolutePath)
    val log2 = logManager.getOrCreateLog(topicPartition, logConfig, isFuture = true)
    val currentReplica = new Replica(brokerId, topicPartition, time, log = Some(log1))
    val futureReplica = new Replica(Request.FutureLocalReplicaId, topicPartition, time, log = Some(log2))
    val partition = Partition(topicPartition, time, replicaManager)

    partition.addReplicaIfNotExists(futureReplica)
    partition.addReplicaIfNotExists(currentReplica)
    assertEquals(Some(currentReplica), partition.localReplica)
    assertEquals(Some(futureReplica), partition.futureLocalReplica)

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
    assertEquals(None, partition.futureLocalReplica)
  }

  // Verify that replacement works when the replicas have the same log end offset but different base offsets in the
  // active segment
  @Test
  def testMaybeReplaceCurrentWithFutureReplicaDifferentBaseOffsets(): Unit = {
    // Write records with duplicate keys to current replica and roll at offset 6
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir1.getAbsolutePath)
    val log1 = logManager.getOrCreateLog(topicPartition, logConfig)
    log1.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 0,
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k1".getBytes, "v2".getBytes),
      new SimpleRecord("k1".getBytes, "v3".getBytes),
      new SimpleRecord("k2".getBytes, "v4".getBytes),
      new SimpleRecord("k2".getBytes, "v5".getBytes),
      new SimpleRecord("k2".getBytes, "v6".getBytes)
    ), leaderEpoch = 0)
    log1.roll()
    log1.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 0,
      new SimpleRecord("k3".getBytes, "v7".getBytes),
      new SimpleRecord("k4".getBytes, "v8".getBytes)
    ), leaderEpoch = 0)

    // Write to the future replica as if the log had been compacted, and do not roll the segment
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir2.getAbsolutePath)
    val log2 = logManager.getOrCreateLog(topicPartition, logConfig, isFuture = true)
    val buffer = ByteBuffer.allocate(1024)
    var builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE,
      TimestampType.CREATE_TIME, 0L, RecordBatch.NO_TIMESTAMP, 0)
    builder.appendWithOffset(2L, new SimpleRecord("k1".getBytes, "v3".getBytes))
    builder.appendWithOffset(5L, new SimpleRecord("k2".getBytes, "v6".getBytes))
    builder.appendWithOffset(6L, new SimpleRecord("k3".getBytes, "v7".getBytes))
    builder.appendWithOffset(7L, new SimpleRecord("k4".getBytes, "v8".getBytes))

    log2.appendAsFollower(builder.build())

    val currentReplica = new Replica(brokerId, topicPartition, time, log = Some(log1))
    val futureReplica = new Replica(Request.FutureLocalReplicaId, topicPartition, time, log = Some(log2))
    val partition = Partition(topicPartition, time, replicaManager)

    partition.addReplicaIfNotExists(futureReplica)
    partition.addReplicaIfNotExists(currentReplica)
    assertEquals(Some(currentReplica), partition.localReplica)
    assertEquals(Some(futureReplica), partition.futureLocalReplica)

    assertTrue(partition.maybeReplaceCurrentWithFutureReplica())
  }

  @Test
  def testFetchOffsetSnapshotEpochValidationForLeader(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    def assertSnapshotError(expectedError: Errors, currentLeaderEpoch: Optional[Integer]): Unit = {
      partition.fetchOffsetSnapshotOrError(currentLeaderEpoch, fetchOnlyFromLeader = true) match {
        case Left(_) => assertEquals(Errors.NONE, expectedError)
        case Right(error) => assertEquals(expectedError, error)
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
      partition.fetchOffsetSnapshotOrError(currentLeaderEpoch, fetchOnlyFromLeader = fetchOnlyLeader) match {
        case Left(_) => assertEquals(expectedError, Errors.NONE)
        case Right(error) => assertEquals(expectedError, error)
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


  private def setupPartitionWithMocks(leaderEpoch: Int,
                                      isLeader: Boolean,
                                      log: Log = logManager.getOrCreateLog(topicPartition, logConfig)): Partition = {
    val replica = new Replica(brokerId, topicPartition, time, log = Some(log))
    val replicaManager: ReplicaManager = EasyMock.mock(classOf[ReplicaManager])
    val zkClient: KafkaZkClient = EasyMock.mock(classOf[KafkaZkClient])

    val partition = new Partition(topicPartition,
      isOffline = false,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      replicaManager,
      logManager,
      zkClient)

    EasyMock.replay(replicaManager, zkClient)

    partition.addReplicaIfNotExists(replica)

    val controllerId = 0
    val controllerEpoch = 0
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val isr = replicas

    if (isLeader) {
      assertTrue("Expected become leader transition to succeed",
        partition.makeLeader(controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, brokerId,
          leaderEpoch, isr, 1, replicas, true), 0))
      assertEquals(leaderEpoch, partition.getLeaderEpoch)
      assertEquals(Some(replica), partition.leaderReplicaIfLocal)
    } else {
      assertTrue("Expected become follower transition to succeed",
        partition.makeFollower(controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, brokerId + 1,
          leaderEpoch, isr, 1, replicas, true), 0))
      assertEquals(leaderEpoch, partition.getLeaderEpoch)
      assertEquals(None, partition.leaderReplicaIfLocal)
    }

    partition
  }

  @Test
  def testAppendRecordsAsFollowerBelowLogStartOffset(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    val replica = new Replica(brokerId, topicPartition, time, log = Some(log))
    val partition = Partition(topicPartition, time, replicaManager)
    partition.addReplicaIfNotExists(replica)
    assertEquals(Some(replica), partition.localReplica)

    val initialLogStartOffset = 5L
    partition.truncateFullyAndStartAt(initialLogStartOffset, isFuture = false)
    assertEquals(s"Log end offset after truncate fully and start at $initialLogStartOffset:",
                 initialLogStartOffset, replica.logEndOffset.messageOffset)
    assertEquals(s"Log start offset after truncate fully and start at $initialLogStartOffset:",
                 initialLogStartOffset, replica.logStartOffset)

    // verify that we cannot append records that do not contain log start offset even if the log is empty
    assertThrows[UnexpectedAppendOffsetException] {
      // append one record with offset = 3
      partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 3L), isFuture = false)
    }
    assertEquals(s"Log end offset should not change after failure to append", initialLogStartOffset, replica.logEndOffset.messageOffset)

    // verify that we can append records that contain log start offset, even when first
    // offset < log start offset if the log is empty
    val newLogStartOffset = 4L
    val records = createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                     new SimpleRecord("k2".getBytes, "v2".getBytes),
                                     new SimpleRecord("k3".getBytes, "v3".getBytes)),
                                baseOffset = newLogStartOffset)
    partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false)
    assertEquals(s"Log end offset after append of 3 records with base offset $newLogStartOffset:", 7L, replica.logEndOffset.messageOffset)
    assertEquals(s"Log start offset after append of 3 records with base offset $newLogStartOffset:", newLogStartOffset, replica.logStartOffset)

    // and we can append more records after that
    partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 7L), isFuture = false)
    assertEquals(s"Log end offset after append of 1 record at offset 7:", 8L, replica.logEndOffset.messageOffset)
    assertEquals(s"Log start offset not expected to change:", newLogStartOffset, replica.logStartOffset)

    // but we cannot append to offset < log start if the log is not empty
    assertThrows[UnexpectedAppendOffsetException] {
      val records2 = createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                        new SimpleRecord("k2".getBytes, "v2".getBytes)),
                                   baseOffset = 3L)
      partition.appendRecordsToFollowerOrFutureReplica(records2, isFuture = false)
    }
    assertEquals(s"Log end offset should not change after failure to append", 8L, replica.logEndOffset.messageOffset)

    // we still can append to next offset
    partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 8L), isFuture = false)
    assertEquals(s"Log end offset after append of 1 record at offset 8:", 9L, replica.logEndOffset.messageOffset)
    assertEquals(s"Log start offset not expected to change:", newLogStartOffset, replica.logStartOffset)
  }

  @Test
  def testListOffsetIsolationLevels(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    val replica = new Replica(brokerId, topicPartition, time, log = Some(log))
    val replicaManager: ReplicaManager = EasyMock.mock(classOf[ReplicaManager])
    val zkClient: KafkaZkClient = EasyMock.mock(classOf[KafkaZkClient])

    val partition = new Partition(topicPartition,
      isOffline = false,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      replicaManager,
      logManager,
      zkClient)

    val controllerId = 0
    val controllerEpoch = 0
    val leaderEpoch = 5
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val isr = replicas

    EasyMock.expect(replicaManager.tryCompleteDelayedFetch(EasyMock.anyObject[TopicPartitionOperationKey]))
        .andVoid()

    EasyMock.replay(replicaManager, zkClient)

    partition.addReplicaIfNotExists(replica)

    assertTrue("Expected become leader transition to succeed",
      partition.makeLeader(controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, brokerId,
        leaderEpoch, isr, 1, replicas, true), 0))
    assertEquals(leaderEpoch, partition.getLeaderEpoch)
    assertEquals(Some(replica), partition.leaderReplicaIfLocal)

    val records = createTransactionalRecords(List(
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes),
      new SimpleRecord("k3".getBytes, "v3".getBytes)),
      baseOffset = 0L)
    partition.appendRecordsToLeader(records, isFromClient = true)

    def fetchLatestOffset(isolationLevel: Option[IsolationLevel]): TimestampOffset = {
      partition.fetchOffsetForTimestamp(ListOffsetRequest.LATEST_TIMESTAMP,
        isolationLevel = isolationLevel,
        currentLeaderEpoch = Optional.empty(),
        fetchOnlyFromLeader = true)
    }

    def fetchEarliestOffset(isolationLevel: Option[IsolationLevel]): TimestampOffset = {
      partition.fetchOffsetForTimestamp(ListOffsetRequest.EARLIEST_TIMESTAMP,
        isolationLevel = isolationLevel,
        currentLeaderEpoch = Optional.empty(),
        fetchOnlyFromLeader = true)
    }

    assertEquals(3L, fetchLatestOffset(isolationLevel = None).offset)
    assertEquals(0L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_UNCOMMITTED)).offset)
    assertEquals(0L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_COMMITTED)).offset)

    replica.highWatermark = LogOffsetMetadata(1L)

    assertEquals(3L, fetchLatestOffset(isolationLevel = None).offset)
    assertEquals(1L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_UNCOMMITTED)).offset)
    assertEquals(0L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_COMMITTED)).offset)

    assertEquals(0L, fetchEarliestOffset(isolationLevel = None).offset)
    assertEquals(0L, fetchEarliestOffset(isolationLevel = Some(IsolationLevel.READ_UNCOMMITTED)).offset)
    assertEquals(0L, fetchEarliestOffset(isolationLevel = Some(IsolationLevel.READ_COMMITTED)).offset)
  }

  @Test
  def testGetReplica(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    val replica = new Replica(brokerId, topicPartition, time, log = Some(log))
    val partition = Partition(topicPartition, time, replicaManager)

    assertEquals(None, partition.localReplica)
    assertThrows[ReplicaNotAvailableException] {
      partition.localReplicaOrException
    }

    partition.addReplicaIfNotExists(replica)
    assertEquals(Some(replica), partition.localReplica)
    assertEquals(replica, partition.localReplicaOrException)
  }

  @Test
  def testAppendRecordsToFollowerWithNoReplicaThrowsException(): Unit = {
    val partition = Partition(topicPartition, time, replicaManager)
    assertThrows[ReplicaNotAvailableException] {
      partition.appendRecordsToFollowerOrFutureReplica(
           createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 0L), isFuture = false)
    }
  }

  @Test
  def testMakeFollowerWithNoLeaderIdChange(): Unit = {
    val partition = Partition(topicPartition, time, replicaManager)

    // Start off as follower
    var partitionStateInfo = new LeaderAndIsrRequest.PartitionState(0, 1, 1,
      List[Integer](0, 1, 2).asJava, 1, List[Integer](0, 1, 2).asJava, false)
    partition.makeFollower(0, partitionStateInfo, 0)

    // Request with same leader and epoch increases by only 1, do become-follower steps
    partitionStateInfo = new LeaderAndIsrRequest.PartitionState(0, 1, 4,
      List[Integer](0, 1, 2).asJava, 1, List[Integer](0, 1, 2).asJava, false)
    assertTrue(partition.makeFollower(0, partitionStateInfo, 2))

    // Request with same leader and same epoch, skip become-follower steps
    partitionStateInfo = new LeaderAndIsrRequest.PartitionState(0, 1, 4,
      List[Integer](0, 1, 2).asJava, 1, List[Integer](0, 1, 2).asJava, false)
    assertFalse(partition.makeFollower(0, partitionStateInfo, 2))
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

    val partition = Partition(topicPartition, time, replicaManager)
    assertTrue("Expected first makeLeader() to return 'leader changed'",
               partition.makeLeader(controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, leader, leaderEpoch, isr, 1, replicas, true), 0))
    assertEquals("Current leader epoch", leaderEpoch, partition.getLeaderEpoch)
    assertEquals("ISR", Set[Integer](leader, follower2), partition.inSyncReplicas.map(_.brokerId))

    // after makeLeader(() call, partition should know about all the replicas
    val leaderReplica = partition.getReplica(leader).get
    val follower1Replica = partition.getReplica(follower1).get
    val follower2Replica = partition.getReplica(follower2).get

    // append records with initial leader epoch
    val lastOffsetOfFirstBatch = partition.appendRecordsToLeader(batch1, isFromClient = true).lastOffset
    partition.appendRecordsToLeader(batch2, isFromClient = true)
    assertEquals("Expected leader's HW not move", leaderReplica.logStartOffset, leaderReplica.highWatermark.messageOffset)

    // let the follower in ISR move leader's HW to move further but below LEO
    def readResult(fetchInfo: FetchDataInfo, leaderReplica: Replica): LogReadResult = {
      LogReadResult(info = fetchInfo,
                    highWatermark = leaderReplica.highWatermark.messageOffset,
                    leaderLogStartOffset = leaderReplica.logStartOffset,
                    leaderLogEndOffset = leaderReplica.logEndOffset.messageOffset,
                    followerLogStartOffset = 0,
                    fetchTimeMs = time.milliseconds,
                    readSize = 10240,
                    lastStableOffset = None)
    }
    partition.updateReplicaLogReadResult(
      follower2Replica, readResult(FetchDataInfo(LogOffsetMetadata(0), batch1), leaderReplica))
    partition.updateReplicaLogReadResult(
      follower2Replica, readResult(FetchDataInfo(LogOffsetMetadata(lastOffsetOfFirstBatch), batch2), leaderReplica))
    assertEquals("Expected leader's HW", lastOffsetOfFirstBatch, leaderReplica.highWatermark.messageOffset)

    // current leader becomes follower and then leader again (without any new records appended)
    partition.makeFollower(
      controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, follower2, leaderEpoch + 1, isr, 1, replicas, false), 1)
    assertTrue("Expected makeLeader() to return 'leader changed' after makeFollower()",
               partition.makeLeader(controllerEpoch, new LeaderAndIsrRequest.PartitionState(
                 controllerEpoch, leader, leaderEpoch + 2, isr, 1, replicas, false), 2))
    val currentLeaderEpochStartOffset = leaderReplica.logEndOffset.messageOffset

    // append records with the latest leader epoch
    partition.appendRecordsToLeader(batch3, isFromClient = true)

    // fetch from follower not in ISR from log start offset should not add this follower to ISR
    partition.updateReplicaLogReadResult(follower1Replica,
                                         readResult(FetchDataInfo(LogOffsetMetadata(0), batch1), leaderReplica))
    partition.updateReplicaLogReadResult(follower1Replica,
                                         readResult(FetchDataInfo(LogOffsetMetadata(lastOffsetOfFirstBatch), batch2), leaderReplica))
    assertEquals("ISR", Set[Integer](leader, follower2), partition.inSyncReplicas.map(_.brokerId))

    // fetch from the follower not in ISR from start offset of the current leader epoch should
    // add this follower to ISR
    partition.updateReplicaLogReadResult(follower1Replica,
                                         readResult(FetchDataInfo(LogOffsetMetadata(currentLeaderEpochStartOffset), batch3), leaderReplica))
    assertEquals("ISR", Set[Integer](leader, follower1, follower2), partition.inSyncReplicas.map(_.brokerId))
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
    val replicaManager: ReplicaManager = EasyMock.mock(classOf[ReplicaManager])
    val zkClient: KafkaZkClient = EasyMock.mock(classOf[KafkaZkClient])
    val controllerId = 0
    val controllerEpoch = 0
    val leaderEpoch = 5
    val replicaIds = List[Integer](brokerId, brokerId + 1).asJava
    val isr = replicaIds
    val logConfig = LogConfig(new Properties)

    val topicPartitions = (0 until 5).map { i => new TopicPartition("test-topic", i) }
    val logs = topicPartitions.map { tp => logManager.getOrCreateLog(tp, logConfig) }
    val replicas = logs.map { log => new Replica(brokerId, log.topicPartition, time, log = Some(log)) }
    val partitions = replicas.map { replica =>
      val tp = replica.topicPartition
      val partition = new Partition(tp,
        isOffline = false,
        replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
        interBrokerProtocolVersion = ApiVersion.latestVersion,
        localBrokerId = brokerId,
        time,
        replicaManager,
        logManager,
        zkClient)
      partition.addReplicaIfNotExists(replica)
      partition.makeLeader(controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, brokerId,
        leaderEpoch, isr, 1, replicaIds, true), 0)
      partition
    }

    // Acquire leaderIsrUpdate read lock of a different partition when completing delayed fetch
    val tpKey: Capture[TopicPartitionOperationKey] = EasyMock.newCapture()
    EasyMock.expect(replicaManager.tryCompleteDelayedFetch(EasyMock.capture(tpKey)))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          val anotherPartition = (tpKey.getValue.partition + 1) % topicPartitions.size
          val partition = partitions(anotherPartition)
          partition.fetchOffsetSnapshot(Optional.of(leaderEpoch), fetchOnlyFromLeader = true)
        }
      }).anyTimes()
    EasyMock.replay(replicaManager, zkClient)

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
      futures.foreach(_.get(10, TimeUnit.SECONDS))
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

}
