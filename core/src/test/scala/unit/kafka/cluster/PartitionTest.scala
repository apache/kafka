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

import java.net.InetAddress
import com.yammer.metrics.core.Metric
import kafka.log.LogManager
import kafka.server._
import kafka.utils._
import org.apache.kafka.common.errors.{ApiException, FencedLeaderEpochException, InconsistentTopicIdException, InvalidTxnStateException, NotLeaderOrFollowerException, OffsetNotAvailableException, OffsetOutOfRangeException, PolicyViolationException, UnknownLeaderEpochException}
import org.apache.kafka.common.message.{AlterPartitionResponseData, FetchResponseData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{AlterPartitionResponse, FetchRequest, ListOffsetsRequest, RequestHeader}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{DirectoryId, IsolationLevel, TopicPartition, Uuid}
import org.apache.kafka.metadata.{LeaderRecoveryState, MetadataCache, PartitionRegistration}
import org.apache.kafka.server.config.ReplicationConfigs
import org.apache.kafka.server.replica.Replica
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt, anyLong}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock

import java.lang.{Long => JLong}
import java.nio.ByteBuffer
import java.util.Optional
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Semaphore}
import kafka.server.metadata.KRaftMetadataCache
import kafka.server.share.DelayedShareFetch
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.replica.ClientMetadata.DefaultClientMetadata
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.server.common.{ControllerRequestCompletionHandler, NodeToControllerChannelManager, RequestLocal}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.partition.{AlterPartitionListener, OngoingReassignmentState, PartitionListener, PendingShrinkIsr, SimpleAssignmentState}
import org.apache.kafka.server.purgatory.{DelayedDeleteRecords, DelayedOperationPurgatory, TopicPartitionOperationKey}
import org.apache.kafka.server.share.fetch.DelayedShareFetchPartitionKey
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, UnexpectedAppendOffsetException}
import org.apache.kafka.server.util.{KafkaScheduler, MockTime}
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpoints
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache
import org.apache.kafka.storage.internals.log.{AppendOrigin, CleanerConfig, EpochEntry, LocalLog, LogAppendInfo, LogConfig, LogDirFailureChannel, LogLoader, LogOffsetMetadata, LogOffsetsListener, LogReadInfo, LogSegments, LogStartOffsetIncrementReason, ProducerStateManager, ProducerStateManagerConfig, UnifiedLog, VerificationGuard}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.lang
import java.util
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOption

object PartitionTest {
  class MockPartitionListener extends PartitionListener {
    private var highWatermark: Long = -1L
    private var failed: Boolean = false
    private var deleted: Boolean = false
    private var follower: Boolean = false

    override def onHighWatermarkUpdated(partition: TopicPartition, offset: Long): Unit = {
      highWatermark = offset
    }

    override def onFailed(partition: TopicPartition): Unit = {
      failed = true
    }

    override def onDeleted(partition: TopicPartition): Unit = {
      deleted = true
    }

    override def onBecomingFollower(partition: TopicPartition): Unit = {
      follower = true
    }

    private def clear(): Unit = {
      highWatermark = -1L
      failed = false
      deleted = false
      follower = false
    }

    /**
     * Verifies the callbacks that have been triggered since the last
     * verification. Values different from `-1` are the ones that have
     * been updated.
     */
    def verify(
      expectedHighWatermark: Long = -1L,
      expectedFailed: Boolean = false,
      expectedDeleted: Boolean = false,
      expectedFollower: Boolean = false
    ): Unit = {
      assertEquals(expectedHighWatermark, highWatermark,
        "Unexpected high watermark")
      assertEquals(expectedFailed, failed,
        "Unexpected failed")
      assertEquals(expectedDeleted, deleted,
        "Unexpected deleted")
      assertEquals(expectedFollower, follower,
        "Unexpected follower")
      clear()
    }
  }

  def followerFetchParams(
    replicaId: Int,
    replicaEpoch: Long = 1L,
    maxWaitMs: Long = 0L,
    minBytes: Int = 1,
    maxBytes: Int = Int.MaxValue
  ): FetchParams = {
    new FetchParams(
      replicaId,
      replicaEpoch,
      maxWaitMs,
      minBytes,
      maxBytes,
      FetchIsolation.LOG_END,
      Optional.empty()
    )
  }

  def consumerFetchParams(
    maxWaitMs: Long = 0L,
    minBytes: Int = 1,
    maxBytes: Int = Int.MaxValue,
    clientMetadata: Option[ClientMetadata] = None,
    isolation: FetchIsolation = FetchIsolation.HIGH_WATERMARK
  ): FetchParams = {
    new FetchParams(
      FetchRequest.CONSUMER_REPLICA_ID,
      -1,
      maxWaitMs,
      minBytes,
      maxBytes,
      isolation,
      clientMetadata.toJava
    )
  }
}

class PartitionTest extends AbstractPartitionTest {
  import PartitionTest._

  @Test
  def testLastFetchedOffsetValidation(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = Optional.empty)
    def append(leaderEpoch: Int, count: Int): Unit = {
      val recordArray = (1 to count).map { i =>
        new SimpleRecord(s"$i".getBytes)
      }
      val records = MemoryRecords.withRecords(0L, Compression.NONE, leaderEpoch,
        recordArray: _*)
      log.appendAsLeader(records, leaderEpoch)
    }

    append(leaderEpoch = 0, count = 2) // 0
    append(leaderEpoch = 3, count = 3) // 2
    append(leaderEpoch = 3, count = 3) // 5
    append(leaderEpoch = 4, count = 5) // 8
    append(leaderEpoch = 7, count = 1) // 13
    append(leaderEpoch = 9, count = 3) // 14
    assertEquals(17L, log.logEndOffset)

    val leaderEpoch = 10
    val logStartOffset = 0L
    val partition = setupPartitionWithMocks(leaderEpoch = leaderEpoch, isLeader = true)
    addBrokerEpochToMockMetadataCache(metadataCache, Array(remoteReplicaId))

    def epochEndOffset(epoch: Int, endOffset: Long): FetchResponseData.EpochEndOffset = {
      new FetchResponseData.EpochEndOffset()
        .setEpoch(epoch)
        .setEndOffset(endOffset)
    }

    def read(lastFetchedEpoch: Int, fetchOffset: Long): LogReadInfo = {
      fetchFollower(
        partition,
        remoteReplicaId,
        fetchOffset,
        logStartOffset,
        leaderEpoch = Some(leaderEpoch),
        lastFetchedEpoch = Some(lastFetchedEpoch)
      )
    }

    def assertDivergence(
      divergingEpoch: FetchResponseData.EpochEndOffset,
      readInfo: LogReadInfo
    ): Unit = {
      assertEquals(Optional.of(divergingEpoch), readInfo.divergingEpoch)
      assertEquals(0, readInfo.fetchedData.records.sizeInBytes)
    }

    def assertNoDivergence(readInfo: LogReadInfo): Unit = {
      assertEquals(Optional.empty(), readInfo.divergingEpoch)
    }

    assertDivergence(epochEndOffset(epoch = 0, endOffset = 2), read(lastFetchedEpoch = 2, fetchOffset = 5))
    assertDivergence(epochEndOffset(epoch = 0, endOffset= 2), read(lastFetchedEpoch = 0, fetchOffset = 4))
    assertDivergence(epochEndOffset(epoch = 4, endOffset = 13), read(lastFetchedEpoch = 6, fetchOffset = 6))
    assertDivergence(epochEndOffset(epoch = 4, endOffset = 13), read(lastFetchedEpoch = 5, fetchOffset = 9))
    assertDivergence(epochEndOffset(epoch = 10, endOffset = 17), read(lastFetchedEpoch = 10, fetchOffset = 18))
    assertNoDivergence(read(lastFetchedEpoch = 0, fetchOffset = 2))
    assertNoDivergence(read(lastFetchedEpoch = 7, fetchOffset = 14))
    assertNoDivergence(read(lastFetchedEpoch = 9, fetchOffset = 17))
    assertNoDivergence(read(lastFetchedEpoch = 10, fetchOffset = 17))

    // Reads from epochs larger than we know about should cause an out of range error
    assertThrows(classOf[OffsetOutOfRangeException], () => read(lastFetchedEpoch = 11, fetchOffset = 5))

    // Move log start offset to the middle of epoch 3
    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(5L, LogStartOffsetIncrementReason.ClientRecordDeletion)

    assertDivergence(epochEndOffset(epoch = 2, endOffset = 5), read(lastFetchedEpoch = 2, fetchOffset = 8))
    assertNoDivergence(read(lastFetchedEpoch = 0, fetchOffset = 5))
    assertNoDivergence(read(lastFetchedEpoch = 3, fetchOffset = 5))

    assertThrows(classOf[OffsetOutOfRangeException], () => read(lastFetchedEpoch = 0, fetchOffset = 0))

    // Fetch offset lower than start offset should throw OffsetOutOfRangeException
    log.maybeIncrementLogStartOffset(10, LogStartOffsetIncrementReason.ClientRecordDeletion)
    assertThrows(classOf[OffsetOutOfRangeException], () => read(lastFetchedEpoch = 5, fetchOffset = 6)) // diverging
    assertThrows(classOf[OffsetOutOfRangeException], () => read(lastFetchedEpoch = 3, fetchOffset = 6)) // not diverging
  }

  @Test
  def testMakeLeaderUpdatesEpochCache(): Unit = {
    val leaderEpoch = 8

    val log = logManager.getOrCreateLog(topicPartition, topicId = Optional.empty)
    log.appendAsLeader(MemoryRecords.withRecords(0L, Compression.NONE, 0,
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes)
    ), 0)
    log.appendAsLeader(MemoryRecords.withRecords(0L, Compression.NONE, 5,
      new SimpleRecord("k3".getBytes, "v3".getBytes),
      new SimpleRecord("k4".getBytes, "v4".getBytes)
    ), 5)
    assertEquals(4, log.logEndOffset)

    val partition = setupPartitionWithMocks(leaderEpoch = leaderEpoch, isLeader = true)
    assertEquals(Some(4), partition.leaderLogIfLocal.map(_.logEndOffset))

    val epochEndOffset = partition.lastOffsetForLeaderEpoch(currentLeaderEpoch = Optional.of[Integer](leaderEpoch),
      leaderEpoch = leaderEpoch, fetchOnlyFromLeader = true)
    assertEquals(4, epochEndOffset.endOffset)
    assertEquals(leaderEpoch, epochEndOffset.leaderEpoch)
  }

  // Verify that partition.removeFutureLocalReplica() and partition.maybeReplaceCurrentWithFutureReplica() can run concurrently
  @Test
  def testMaybeReplaceCurrentWithFutureReplica(): Unit = {
    val latch = new CountDownLatch(1)

    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir1.getAbsolutePath)
    partition.createLogIfNotExists(isNew = true, isFutureReplica = false, offsetCheckpoints, None)
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
  def testReplicaFetchToFollower(): Unit = {
    val followerId = brokerId + 1
    val leaderId = brokerId + 2
    val replicas = Array(brokerId, followerId, leaderId)
    val isr = Array(brokerId, followerId, leaderId)
    val leaderEpoch = 8
    val partitionEpoch = 1

    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(leaderId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(partitionEpoch)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeFollower(partitionRegistration, isNew = true, offsetCheckpoints, None))

    def assertFetchFromReplicaFails[T <: ApiException](
      expectedExceptionClass: Class[T],
      leaderEpoch: Option[Int]
    ): Unit = {
      assertThrows(expectedExceptionClass, () => {
        fetchFollower(
          partition,
          replicaId = followerId,
          fetchOffset = 0L,
          leaderEpoch = leaderEpoch
        )
      })
    }

    assertFetchFromReplicaFails(classOf[NotLeaderOrFollowerException], None)
    assertFetchFromReplicaFails(classOf[NotLeaderOrFollowerException], Some(leaderEpoch))
    assertFetchFromReplicaFails(classOf[UnknownLeaderEpochException], Some(leaderEpoch + 1))
    assertFetchFromReplicaFails(classOf[FencedLeaderEpochException], Some(leaderEpoch - 1))
  }

  @Test
  def testFetchFromUnrecognizedFollower(): Unit = {
    val leader = brokerId
    val validReplica = brokerId + 1
    val addingReplica1 = brokerId + 2
    val addingReplica2 = brokerId + 3
    val replicas = Array(leader, validReplica)
    val isr = Array(leader, validReplica)
    val leaderEpoch = 8
    val partitionEpoch = 1
    addBrokerEpochToMockMetadataCache(metadataCache, Array(leader, addingReplica1, addingReplica2))

    var partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(leader)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(partitionEpoch)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, topicId))

    assertThrows(classOf[UnknownLeaderEpochException], () => {
      fetchFollower(
        partition,
        replicaId = addingReplica1,
        fetchOffset = 0L,
        leaderEpoch = Some(leaderEpoch)
      )
    })
    assertEquals(None, partition.getReplica(addingReplica1).map(_.stateSnapshot.logEndOffset))

    assertThrows(classOf[NotLeaderOrFollowerException], () => {
      fetchFollower(
        partition,
        replicaId = addingReplica2,
        fetchOffset = 0L,
        leaderEpoch = None
      )
    })
    assertEquals(None, partition.getReplica(addingReplica2).map(_.stateSnapshot.logEndOffset))

    // The replicas are added as part of a reassignment
    val newReplicas = Array(leader, validReplica, addingReplica1, addingReplica2)
    val newPartitionEpoch = partitionEpoch + 1
    val addingReplicas = Array(addingReplica1, addingReplica2)

    partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(leader)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(newPartitionEpoch)
      .setReplicas(newReplicas)
      .setAddingReplicas(addingReplicas)
      .setDirectories(DirectoryId.unassignedArray(newReplicas.length))
      .build()
    assertFalse(partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None))

    // Now the fetches are allowed
    assertEquals(0L, fetchFollower(
      partition,
      replicaId = addingReplica1,
      fetchOffset = 0L,
      leaderEpoch = Some(leaderEpoch)
    ).logEndOffset)
    assertEquals(Some(0L), partition.getReplica(addingReplica1).map(_.stateSnapshot.logEndOffset))

    assertEquals(0L, fetchFollower(
      partition,
      replicaId = addingReplica2,
      fetchOffset = 0L,
      leaderEpoch = None
    ).logEndOffset)
    assertEquals(Some(0L), partition.getReplica(addingReplica2).map(_.stateSnapshot.logEndOffset))
  }

  // Verify that partition.makeFollower() and partition.appendRecordsToFollowerOrFutureReplica() can run concurrently
  @Test
  def testMakeFollowerWithWithFollowerAppendRecords(): Unit = {
    val appendSemaphore = new Semaphore(0)
    val mockTime = new MockTime()
    val prevLeaderEpoch = 0
    val replicas = Array(0, 1, 2, brokerId)

    partition = new Partition(
      topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterPartitionManager) {

      override def createLog(isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints, topicId: Option[Uuid], targetLogDirectoryId: Option[Uuid]): UnifiedLog = {
        val log = super.createLog(isNew, isFutureReplica, offsetCheckpoints, None, None)
        val logDirFailureChannel = new LogDirFailureChannel(1)
        val segments = new LogSegments(log.topicPartition)
        val leaderEpochCache = UnifiedLog.createLeaderEpochCache(
          log.dir, log.topicPartition, logDirFailureChannel, Optional.empty, time.scheduler)
        val maxTransactionTimeoutMs = 5 * 60 * 1000
        val producerStateManagerConfig = new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, true)
        val producerStateManager = new ProducerStateManager(
          log.topicPartition,
          log.dir,
          maxTransactionTimeoutMs,
          producerStateManagerConfig,
          mockTime
        )
        val offsets = new LogLoader(
          log.dir,
          log.topicPartition,
          log.config,
          mockTime.scheduler,
          mockTime,
          logDirFailureChannel,
          true,
          segments,
          0L,
          0L,
          leaderEpochCache,
          producerStateManager,
          new ConcurrentHashMap[String, Integer],
          false
        ).load()
        val localLog = new LocalLog(log.dir, log.config, segments, offsets.recoveryPoint,
          offsets.nextOffsetMetadata, mockTime.scheduler, mockTime, log.topicPartition,
          logDirFailureChannel)
        new SlowLog(log, topicId.toJava, offsets.logStartOffset, localLog, leaderEpochCache, producerStateManager, appendSemaphore)
      }
    }

    partition.createLogIfNotExists(isNew = true, isFutureReplica = false, offsetCheckpoints, None)
    var partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(2)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(prevLeaderEpoch)
      .setIsr(replicas)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = false, offsetCheckpoints, None))


    val appendThread = new Thread {
      override def run(): Unit = {
        val records = createRecords(
          util.List.of(
            new SimpleRecord("k1".getBytes, "v1".getBytes),
            new SimpleRecord("k2".getBytes, "v2".getBytes)
          ),
          baseOffset = 0,
          partitionLeaderEpoch = prevLeaderEpoch
        )
        partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false, prevLeaderEpoch)
      }
    }
    appendThread.start()
    TestUtils.waitUntilTrue(() => appendSemaphore.hasQueuedThreads, "follower log append is not called.")

    partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(2)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(prevLeaderEpoch + 1)
      .setIsr(replicas)
      .setPartitionEpoch(2)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeFollower(partitionRegistration, isNew = false, offsetCheckpoints, None))

    appendSemaphore.release()
    appendThread.join()

    assertEquals(2L, partition.localLogOrException.logEndOffset)
    assertEquals(2L, partition.leaderReplicaIdOpt.get)
  }

  @Test
  // Verify that replacement works when the replicas have the same log end offset but different base offsets in the
  // active segment
  def testMaybeReplaceCurrentWithFutureReplicaDifferentBaseOffsets(): Unit = {
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir1.getAbsolutePath)
    partition.createLogIfNotExists(isNew = true, isFutureReplica = false, offsetCheckpoints, None)
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir2.getAbsolutePath)
    partition.maybeCreateFutureReplica(logDir2.getAbsolutePath, offsetCheckpoints)

    // Write records with duplicate keys to current replica and roll at offset 6
    val currentLog = partition.log.get
    currentLog.appendAsLeader(MemoryRecords.withRecords(0L, Compression.NONE, 0,
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k1".getBytes, "v2".getBytes),
      new SimpleRecord("k1".getBytes, "v3".getBytes),
      new SimpleRecord("k2".getBytes, "v4".getBytes),
      new SimpleRecord("k2".getBytes, "v5".getBytes),
      new SimpleRecord("k2".getBytes, "v6".getBytes)
    ), 0)
    currentLog.roll()
    currentLog.appendAsLeader(MemoryRecords.withRecords(0L, Compression.NONE, 0,
      new SimpleRecord("k3".getBytes, "v7".getBytes),
      new SimpleRecord("k4".getBytes, "v8".getBytes)
    ), 0)

    // Write to the future replica as if the log had been compacted, and do not roll the segment

    val buffer = ByteBuffer.allocate(1024)
    val builder = MemoryRecords.builder(
      buffer,
      RecordBatch.CURRENT_MAGIC_VALUE,
      Compression.NONE,
      TimestampType.CREATE_TIME,
      0L, // baseOffset
      RecordBatch.NO_TIMESTAMP,
      0 // partitionLeaderEpoch
    )
    builder.appendWithOffset(2L, new SimpleRecord("k1".getBytes, "v3".getBytes))
    builder.appendWithOffset(5L, new SimpleRecord("k2".getBytes, "v6".getBytes))
    builder.appendWithOffset(6L, new SimpleRecord("k3".getBytes, "v7".getBytes))
    builder.appendWithOffset(7L, new SimpleRecord("k4".getBytes, "v8".getBytes))

    val futureLog = partition.futureLocalLogOrException
    futureLog.appendAsFollower(builder.build(), 0)

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

    assertSnapshotError(Errors.NOT_LEADER_OR_FOLLOWER, Optional.of(leaderEpoch), fetchOnlyLeader = true)
    assertSnapshotError(Errors.NOT_LEADER_OR_FOLLOWER, Optional.empty(), fetchOnlyLeader = true)
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
      assertEquals(error.code, endOffset.errorCode)
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
      assertEquals(error.code, endOffset.errorCode)
    }

    assertLastOffsetForLeaderError(Errors.NONE, Optional.empty(), fetchOnlyLeader = false)
    assertLastOffsetForLeaderError(Errors.NONE, Optional.of(leaderEpoch), fetchOnlyLeader = false)
    assertLastOffsetForLeaderError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = false)
    assertLastOffsetForLeaderError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = false)

    assertLastOffsetForLeaderError(Errors.NOT_LEADER_OR_FOLLOWER, Optional.empty(), fetchOnlyLeader = true)
    assertLastOffsetForLeaderError(Errors.NOT_LEADER_OR_FOLLOWER, Optional.of(leaderEpoch), fetchOnlyLeader = true)
    assertLastOffsetForLeaderError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = true)
    assertLastOffsetForLeaderError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = true)
  }

  @Test
  def testLeaderEpochValidationOnLeader(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    addBrokerEpochToMockMetadataCache(metadataCache, Array(remoteReplicaId))

    def sendFetch(leaderEpoch: Option[Int]): LogReadInfo = {
      fetchFollower(
        partition,
        remoteReplicaId,
        fetchOffset = 0L,
        leaderEpoch = leaderEpoch
      )
    }

    assertEquals(0L, sendFetch(leaderEpoch = None).logEndOffset)
    assertEquals(0L, sendFetch(leaderEpoch = Some(leaderEpoch)).logEndOffset)
    assertThrows(classOf[FencedLeaderEpochException], () => sendFetch(Some(leaderEpoch - 1)))
    assertThrows(classOf[UnknownLeaderEpochException], () => sendFetch(Some(leaderEpoch + 1)))
  }

  @Test
  def testLeaderEpochValidationOnFollower(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = false)

    def sendFetch(
      leaderEpoch: Option[Int],
      clientMetadata: Option[ClientMetadata]
    ): LogReadInfo = {
      fetchConsumer(
        partition,
        fetchOffset = 0L,
        leaderEpoch = leaderEpoch,
        clientMetadata = clientMetadata
      )
    }

    // Follower fetching is only allowed when the client provides metadata
    assertThrows(classOf[NotLeaderOrFollowerException], () => sendFetch(None, None))
    assertThrows(classOf[NotLeaderOrFollowerException], () => sendFetch(Some(leaderEpoch), None))
    assertThrows(classOf[FencedLeaderEpochException], () => sendFetch(Some(leaderEpoch - 1), None))
    assertThrows(classOf[UnknownLeaderEpochException], () => sendFetch(Some(leaderEpoch + 1), None))

    val clientMetadata = new DefaultClientMetadata(
      "rack",
      "clientId",
      InetAddress.getLoopbackAddress,
      KafkaPrincipal.ANONYMOUS,
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT).value
    )
    assertEquals(0L, sendFetch(leaderEpoch = None, Some(clientMetadata)).logEndOffset)
    assertEquals(0L, sendFetch(leaderEpoch = Some(leaderEpoch), Some(clientMetadata)).logEndOffset)
    assertThrows(classOf[FencedLeaderEpochException], () => sendFetch(Some(leaderEpoch - 1), Some(clientMetadata)))
    assertThrows(classOf[UnknownLeaderEpochException], () => sendFetch(Some(leaderEpoch + 1), Some(clientMetadata)))
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

    assertFetchOffsetError(Errors.NOT_LEADER_OR_FOLLOWER, Optional.empty(), fetchOnlyLeader = true)
    assertFetchOffsetError(Errors.NOT_LEADER_OR_FOLLOWER, Optional.of(leaderEpoch), fetchOnlyLeader = true)
    assertFetchOffsetError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = true)
    assertFetchOffsetError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = true)
  }

  @Test
  def testFetchLatestOffsetIncludesLeaderEpoch(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    val timestampAndOffsetOpt = partition.fetchOffsetForTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP,
      isolationLevel = None,
      currentLeaderEpoch = Optional.empty(),
      fetchOnlyFromLeader = true).timestampAndOffsetOpt

    assertTrue(timestampAndOffsetOpt.isPresent)

    val timestampAndOffset = timestampAndOffsetOpt.get
    assertEquals(leaderEpoch, timestampAndOffset.leaderEpoch.get)
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
    val leader = brokerId
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val replicas = Array(leader, follower1, follower2)
    val isr = Array(leader, follower2)
    val leaderEpoch = 8
    val batch1 = TestUtils.records(records = List(
      new SimpleRecord(10, "k1".getBytes, "v1".getBytes),
      new SimpleRecord(11,"k2".getBytes, "v2".getBytes)))
    val batch2 = TestUtils.records(records = List(new SimpleRecord("k3".getBytes, "v1".getBytes),
      new SimpleRecord(20,"k4".getBytes, "v2".getBytes),
      new SimpleRecord(21,"k5".getBytes, "v3".getBytes)))
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    val leaderRegistration = new PartitionRegistration.Builder()
      .setLeader(leader)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(leaderRegistration, isNew = true, offsetCheckpoints, topicId), "Expected first makeLeader() to return 'leader changed'")

    assertEquals(leaderEpoch, partition.getLeaderEpoch, "Current leader epoch")
    assertEquals(util.Set.of(leader, follower2), partition.partitionState.isr, "ISR")

    val requestLocal = RequestLocal.withThreadConfinedCaching
    // after makeLeader(() call, partition should know about all the replicas
    // append records with initial leader epoch
    partition.appendRecordsToLeader(batch1, origin = AppendOrigin.CLIENT, requiredAcks = 0, requestLocal)
    partition.appendRecordsToLeader(batch2, origin = AppendOrigin.CLIENT, requiredAcks = 0, requestLocal)
    assertEquals(partition.localLogOrException.logStartOffset, partition.localLogOrException.highWatermark,
      "Expected leader's HW not move")

    def fetchOffsetsForTimestamp(timestamp: Long, isolation: Option[IsolationLevel]): Either[ApiException, Option[TimestampAndOffset]] = {
      try {
        val offsetResultHolder = partition.fetchOffsetForTimestamp(
          timestamp = timestamp,
          isolationLevel = isolation,
          currentLeaderEpoch = Optional.of(partition.getLeaderEpoch),
          fetchOnlyFromLeader = true
        )
        val timestampAndOffsetOpt = offsetResultHolder.timestampAndOffsetOpt
        if (timestampAndOffsetOpt.isEmpty || offsetResultHolder.lastFetchableOffset.isPresent &&
          timestampAndOffsetOpt.get.offset >= offsetResultHolder.lastFetchableOffset.get) {
          offsetResultHolder.maybeOffsetsError.map(e => throw e)
        }
        Right(if (timestampAndOffsetOpt.isPresent) Some(timestampAndOffsetOpt.get) else None)
      } catch {
        case e: ApiException => Left(e)
      }
    }

    // let the follower in ISR move leader's HW to move further but below LEO
    fetchFollower(partition, replicaId = follower1, fetchOffset = 0L)
    fetchFollower(partition, replicaId = follower1, fetchOffset = 2L)

    fetchFollower(partition, replicaId = follower2, fetchOffset = 0L)
    fetchFollower(partition, replicaId = follower2, fetchOffset = 2L)

    // Simulate successful ISR update
    alterPartitionManager.completeIsrUpdate(2)

    // At this point, the leader has gotten 5 writes, but followers have only fetched two
    assertEquals(2, partition.localLogOrException.highWatermark)

    // Get the LEO
    fetchOffsetsForTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, None) match {
      case Right(Some(offsetAndTimestamp)) => assertEquals(5, offsetAndTimestamp.offset)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e) => fail("Should not have seen an error")
    }

    // Get the HW
    fetchOffsetsForTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => assertEquals(2, offsetAndTimestamp.offset)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e) => fail("Should not have seen an error")
    }

    // Get a offset beyond the HW by timestamp, get a None
    assertEquals(Right(None), fetchOffsetsForTimestamp(30, Some(IsolationLevel.READ_UNCOMMITTED)))

    // Make into a follower
    val followerRegistration = new PartitionRegistration.Builder()
      .setLeader(follower2)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch + 1)
      .setIsr(isr)
      .setPartitionEpoch(4)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeFollower(followerRegistration, isNew = false, offsetCheckpoints, None))

    // Back to leader, this resets the startLogOffset for this epoch (to 2), we're now in the fault condition
    val newLeaderRegistration = new PartitionRegistration.Builder()
      .setLeader(leader)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch + 2)
      .setIsr(isr)
      .setPartitionEpoch(5)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(newLeaderRegistration, isNew = false, offsetCheckpoints, None))

    // Try to get offsets as a client
    fetchOffsetsForTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => fail("Should have failed with OffsetNotAvailable")
      case Right(None) => fail("Should have seen an error")
      case Left(e: OffsetNotAvailableException) => // ok
      case Left(e: ApiException) => fail(s"Expected OffsetNotAvailableException, got $e")
    }

    // If request is not from a client, we skip the check
    fetchOffsetsForTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, None) match {
      case Right(Some(offsetAndTimestamp)) => assertEquals(5, offsetAndTimestamp.offset)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e: ApiException) => fail(s"Got ApiException $e")
    }

    // If we request the earliest timestamp, we skip the check
    fetchOffsetsForTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => assertEquals(0, offsetAndTimestamp.offset)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e: ApiException) => fail(s"Got ApiException $e")
    }

    // If we request the earliest local timestamp, we skip the check
    fetchOffsetsForTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
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

    // Next fetch from replicas, HW is moved up to 5 (ahead of the LEO)
    fetchFollower(partition, replicaId = follower1, fetchOffset = 5L)
    fetchFollower(partition, replicaId = follower2, fetchOffset = 5L)

    // Simulate successful ISR update
    alterPartitionManager.completeIsrUpdate(6)

    // Error goes away
    fetchOffsetsForTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => assertEquals(5, offsetAndTimestamp.offset)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e: ApiException) => fail(s"Got ApiException $e")
    }

    // Now we see None instead of an error for out of range timestamp
    assertEquals(Right(None), fetchOffsetsForTimestamp(100, Some(IsolationLevel.READ_UNCOMMITTED)))
  }

  @Test
  def testAppendRecordsAsFollowerBelowLogStartOffset(): Unit = {
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val log = partition.localLogOrException
    val epoch = 1
    val replicas = Array(0, 1, 2, brokerId)

    // Start off as follower
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(1)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(epoch)
      .setIsr(replicas)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    partition.makeFollower(partitionRegistration, isNew = false, offsetCheckpoints, None)

    val initialLogStartOffset = 5L
    partition.truncateFullyAndStartAt(initialLogStartOffset, isFuture = false)
    assertEquals(initialLogStartOffset, log.logEndOffset,
      s"Log end offset after truncate fully and start at $initialLogStartOffset:")
    assertEquals(initialLogStartOffset, log.logStartOffset,
      s"Log start offset after truncate fully and start at $initialLogStartOffset:")

    // verify that we cannot append records that do not contain log start offset even if the log is empty
    assertThrows(
      classOf[UnexpectedAppendOffsetException],
      // append one record with offset = 3
      () => partition.appendRecordsToFollowerOrFutureReplica(
        createRecords(util.List.of(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 3L),
        isFuture = false,
        partitionLeaderEpoch = epoch
      )
    )
    assertEquals(initialLogStartOffset, log.logEndOffset,
      s"Log end offset should not change after failure to append")

    // verify that we can append records that contain log start offset, even when first
    // offset < log start offset if the log is empty
    val newLogStartOffset = 4L
    val records = createRecords(util.List.of(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                     new SimpleRecord("k2".getBytes, "v2".getBytes),
                                     new SimpleRecord("k3".getBytes, "v3".getBytes)),
                                baseOffset = newLogStartOffset)
    partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false, partitionLeaderEpoch = epoch)
    assertEquals(7L, log.logEndOffset, s"Log end offset after append of 3 records with base offset $newLogStartOffset:")
    assertEquals(newLogStartOffset, log.logStartOffset, s"Log start offset after append of 3 records with base offset $newLogStartOffset:")

    // and we can append more records after that
    partition.appendRecordsToFollowerOrFutureReplica(
      createRecords(util.List.of(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 7L),
      isFuture = false,
      partitionLeaderEpoch = epoch
    )
    assertEquals(8L, log.logEndOffset, s"Log end offset after append of 1 record at offset 7:")
    assertEquals(newLogStartOffset, log.logStartOffset, s"Log start offset not expected to change:")

    // but we cannot append to offset < log start if the log is not empty
    val records2 = createRecords(util.List.of(new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes)),
      baseOffset = 3L)
    assertThrows(
      classOf[UnexpectedAppendOffsetException],
      () => partition.appendRecordsToFollowerOrFutureReplica(records2, isFuture = false, partitionLeaderEpoch = epoch)
    )
    assertEquals(8L, log.logEndOffset, s"Log end offset should not change after failure to append")

    // we still can append to next offset
    partition.appendRecordsToFollowerOrFutureReplica(
      createRecords(util.List.of(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 8L),
      isFuture = false,
      partitionLeaderEpoch = epoch
    )
    assertEquals(9L, log.logEndOffset, s"Log end offset after append of 1 record at offset 8:")
    assertEquals(newLogStartOffset, log.logStartOffset, s"Log start offset not expected to change:")
  }

  @Test
  def testListOffsetIsolationLevels(): Unit = {
    val leaderEpoch = 5
    val replicas = Array(brokerId, brokerId + 1)
    val isr = replicas

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

    val partitionRegistration = new PartitionRegistration.Builder()
        .setLeader(brokerId)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setPartitionEpoch(1)
        .setReplicas(replicas)
        .setDirectories(DirectoryId.unassignedArray(replicas.length))
        .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None), "Expected become leader transition to succeed")

    assertEquals(leaderEpoch, partition.getLeaderEpoch)

    val records = createTransactionalRecords(util.List.of(
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes),
      new SimpleRecord("k3".getBytes, "v3".getBytes)),
      baseOffset = 0L,
      producerId = 2L)
    val verificationGuard = partition.maybeStartTransactionVerification(2L, 0, 0, supportsEpochBump = true)
    partition.appendRecordsToLeader(records, origin = AppendOrigin.CLIENT, requiredAcks = 0, RequestLocal.withThreadConfinedCaching, verificationGuard)

    def fetchOffset(isolationLevel: Option[IsolationLevel], timestamp: Long): TimestampAndOffset = {
      val res = partition.fetchOffsetForTimestamp(timestamp,
        isolationLevel = isolationLevel,
        currentLeaderEpoch = Optional.empty(),
        fetchOnlyFromLeader = true).timestampAndOffsetOpt
      assertTrue(res.isPresent)
      res.get
    }

    def fetchLatestOffset(isolationLevel: Option[IsolationLevel]): TimestampAndOffset = {
      fetchOffset(isolationLevel, ListOffsetsRequest.LATEST_TIMESTAMP)
    }

    def fetchEarliestOffset(isolationLevel: Option[IsolationLevel]): TimestampAndOffset = {
      fetchOffset(isolationLevel, ListOffsetsRequest.EARLIEST_TIMESTAMP)
    }

    def fetchEarliestLocalOffset(isolationLevel: Option[IsolationLevel]): TimestampAndOffset = {
      fetchOffset(isolationLevel, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP)
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

    assertEquals(0L, fetchEarliestLocalOffset(isolationLevel = None).offset)
    assertEquals(0L, fetchEarliestLocalOffset(isolationLevel = Some(IsolationLevel.READ_UNCOMMITTED)).offset)
    assertEquals(0L, fetchEarliestLocalOffset(isolationLevel = Some(IsolationLevel.READ_COMMITTED)).offset)
  }

  @Test
  def testGetReplica(): Unit = {
    assertEquals(None, partition.log)
    assertThrows(classOf[NotLeaderOrFollowerException], () =>
      partition.localLogOrException
    )
  }

  @Test
  def testAppendRecordsToFollowerWithNoReplicaThrowsException(): Unit = {
    assertThrows(
      classOf[NotLeaderOrFollowerException],
      () => partition.appendRecordsToFollowerOrFutureReplica(
        createRecords(util.List.of(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 0L),
        isFuture = false,
        partitionLeaderEpoch = 0
      )
    )
  }

  @Test
  def testMakeFollowerWithNoLeaderIdChange(): Unit = {
    val replicas = Array(0, 1, 2, brokerId)
    // Start off as follower
    var partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(1)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(1)
      .setIsr(replicas)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    partition.makeFollower(partitionRegistration, isNew = false, offsetCheckpoints, None)

    // Request with same leader and epoch increases by only 1, do become-follower steps
    partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(1)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(4)
      .setIsr(replicas)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeFollower(partitionRegistration, isNew = false, offsetCheckpoints, None))

    // Request with same leader and same epoch, skip become-follower steps
    partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(1)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(4)
      .setIsr(replicas)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertFalse(partition.makeFollower(partitionRegistration, isNew = false, offsetCheckpoints, None))
  }

  @Test
  def testFollowerDoesNotJoinISRUntilCaughtUpToOffsetWithinCurrentLeaderEpoch(): Unit = {
    val leader = brokerId
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val replicas = Array(leader, follower1, follower2)
    val isr = Array(leader, follower2)
    val leaderEpoch = 8
    val batch1 = TestUtils.records(records = List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                                  new SimpleRecord("k2".getBytes, "v2".getBytes)))
    val batch2 = TestUtils.records(records = List(new SimpleRecord("k3".getBytes, "v1".getBytes),
                                                  new SimpleRecord("k4".getBytes, "v2".getBytes),
                                                  new SimpleRecord("k5".getBytes, "v3".getBytes)))
    val batch3 = TestUtils.records(records = List(new SimpleRecord("k6".getBytes, "v1".getBytes),
                                                  new SimpleRecord("k7".getBytes, "v2".getBytes)))
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    val leaderRegistration = new PartitionRegistration.Builder()
      .setLeader(leader)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(leaderRegistration, isNew = true, offsetCheckpoints, topicId), "Expected first makeLeader() to return 'leader changed'")
    assertEquals(leaderEpoch, partition.getLeaderEpoch, "Current leader epoch")
    assertEquals(util.Set.of(leader, follower2), partition.partitionState.isr, "ISR")

    val requestLocal = RequestLocal.withThreadConfinedCaching

    // after makeLeader(() call, partition should know about all the replicas
    // append records with initial leader epoch
    val lastOffsetOfFirstBatch = partition.appendRecordsToLeader(batch1, origin = AppendOrigin.CLIENT,
      requiredAcks = 0, requestLocal).lastOffset
    partition.appendRecordsToLeader(batch2, origin = AppendOrigin.CLIENT, requiredAcks = 0, requestLocal)
    assertEquals(partition.localLogOrException.logStartOffset, partition.log.get.highWatermark, "Expected leader's HW not move")

    // let the follower in ISR move leader's HW to move further but below LEO
    fetchFollower(partition, replicaId = follower2, fetchOffset = 0)
    fetchFollower(partition, replicaId = follower2, fetchOffset = lastOffsetOfFirstBatch + 1)
    assertEquals(lastOffsetOfFirstBatch + 1, partition.log.get.highWatermark, "Expected leader's HW")

    // current leader becomes follower and then leader again (without any new records appended)
    val followerRegistration = new PartitionRegistration.Builder()
      .setLeader(follower2)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch + 1)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    partition.makeFollower(followerRegistration, isNew = false, offsetCheckpoints, None)

    val newLeaderRegistration = new PartitionRegistration.Builder()
      .setLeader(leader)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch + 2)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(newLeaderRegistration, isNew = false, offsetCheckpoints, topicId),
      "Expected makeLeader() to return 'leader changed' after makeFollower()")
    val currentLeaderEpochStartOffset = partition.localLogOrException.logEndOffset

    // append records with the latest leader epoch
    partition.appendRecordsToLeader(batch3, origin = AppendOrigin.CLIENT, requiredAcks = 0, requestLocal)

    // fetch from follower not in ISR from log start offset should not add this follower to ISR
    fetchFollower(partition, replicaId = follower1, fetchOffset = 0)
    fetchFollower(partition, replicaId = follower1, fetchOffset = lastOffsetOfFirstBatch)
    assertEquals(util.Set.of(leader, follower2), partition.partitionState.isr, "ISR")

    // fetch from the follower not in ISR from start offset of the current leader epoch should
    // add this follower to ISR
    fetchFollower(partition, replicaId = follower1, fetchOffset = currentLeaderEpochStartOffset)

    // Expansion does not affect the ISR
    assertEquals(util.Set.of(leader, follower2), partition.partitionState.isr, "ISR")
    assertEquals(util.Set.of(leader, follower1, follower2), partition.partitionState.maximalIsr, "ISR")
    assertEquals(alterPartitionManager.isrUpdates.head.leaderAndIsr.isr.asScala.toSet,
      Set(leader, follower1, follower2), "AlterIsr")
  }

  def createRecords(records: lang.Iterable[SimpleRecord], baseOffset: Long, partitionLeaderEpoch: Int = 0): MemoryRecords = {
    val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records))
    val builder = MemoryRecords.builder(
      buf, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.LOG_APPEND_TIME,
      baseOffset, time.milliseconds, partitionLeaderEpoch)
    records.forEach(builder.append)
    builder.build()
  }

  def createIdempotentRecords(records: lang.Iterable[SimpleRecord],
                              baseOffset: Long,
                              baseSequence: Int = 0,
                              producerId: Long = 1L): MemoryRecords = {
    val producerEpoch = 0.toShort
    val isTransactional = false
    val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records))
    val builder = MemoryRecords.builder(buf, Compression.NONE, baseOffset, producerId,
      producerEpoch, baseSequence, isTransactional)
    records.forEach(builder.append)
    builder.build()
  }

  def createTransactionalRecords(records: lang.Iterable[SimpleRecord],
                                 baseOffset: Long,
                                 baseSequence: Int = 0,
                                 producerId: Long = 1L): MemoryRecords = {
    val producerEpoch = 0.toShort
    val isTransactional = true
    val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records))
    val builder = MemoryRecords.builder(buf, Compression.NONE, baseOffset, producerId,
      producerEpoch, baseSequence, isTransactional)
    records.forEach(builder.append)
    builder.build()
  }

  /**
    * Test for AtMinIsr partition state. We set the partition replica set size as 3, but only set one replica as an ISR.
    * As the default minIsr configuration is 1, then the partition should be at min ISR (isAtMinIsr = true).
    */
  @Test
  def testAtMinIsr(): Unit = {
    val leader = brokerId
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val replicas = Array(leader, follower1, follower2)
    val isr = Array(leader)
    val leaderEpoch = 8

    assertFalse(partition.isAtMinIsr)
    // Make isr set to only have leader to trigger AtMinIsr (default min isr config is 1)
    val leaderRegistration = new PartitionRegistration.Builder()
      .setLeader(leader)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    partition.makeLeader(leaderRegistration, isNew = true, offsetCheckpoints, None)
    assertTrue(partition.isAtMinIsr)
  }

  @Test
  def testIsUnderMinIsr(): Unit = {
    val replicas = Array(brokerId, brokerId + 1)
    configRepository.setTopicConfig(topicPartition.topic, TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
    partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterPartitionManager)
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, topicId = None)

    var leaderRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(0)
      .setIsr(replicas)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .setPartitionEpoch(1)
      .build()
    partition.makeLeader(leaderRegistration, isNew = true, offsetCheckpoints, None)
    assertFalse(partition.isUnderMinIsr)

    leaderRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(1)
      .setIsr(Array(brokerId))
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .setPartitionEpoch(2)
      .build()
    partition.makeLeader(leaderRegistration, isNew = false, offsetCheckpoints, None)
    assertTrue(partition.isUnderMinIsr)
  }

  @Test
  def testUpdateFollowerFetchState(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = Optional.empty)
    seedLogData(log, numRecords = 6, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Array(brokerId, remoteBrokerId)
    val isr = replicas
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

    val initializeTimeMs = time.milliseconds()
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None), "Expected become leader transition to succeed")

    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = initializeTimeMs,
      logStartOffset = UnifiedLog.UNKNOWN_OFFSET,
      logEndOffset = UnifiedLog.UNKNOWN_OFFSET
    )

    time.sleep(500)

    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = 3L)
    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = initializeTimeMs,
      logStartOffset = 0L,
      logEndOffset = 3L
    )

    time.sleep(500)

    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = 6L)
    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = time.milliseconds(),
      logStartOffset = 0L,
      logEndOffset = 6L
    )
  }

  @Test
  def testIsReplicaIsrEligibleWithEmptyReplicaMap(): Unit = {
    val partition = spy(new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterPartitionManager))

    when(offsetCheckpoints.fetch(ArgumentMatchers.anyString, ArgumentMatchers.eq(topicPartition)))
      .thenReturn(Optional.empty[JLong])
    val log = logManager.getOrCreateLog(topicPartition, topicId = Optional.empty)
    seedLogData(log, numRecords = 6, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Array(brokerId, remoteBrokerId)
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

    val initializeTimeMs = time.milliseconds()
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(Array(brokerId))
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None), "Expected become leader transition to succeed")
    doAnswer(_ => {
      // simulate topic is deleted at the moment
      partition.delete()
      val replica = new Replica(remoteBrokerId, topicPartition, metadataCache)
      partition.updateFollowerFetchState(replica, mock(classOf[LogOffsetMetadata]), 0, initializeTimeMs, 0, defaultBrokerEpoch(remoteBrokerId))
      mock(classOf[LogReadInfo])
    }).when(partition).fetchRecords(any(), any(), anyLong(), anyInt(), anyBoolean(), anyBoolean())

    assertDoesNotThrow(() => fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = 3L))
  }

  @Test
  def testInvalidAlterPartitionRequestsAreNotRetried(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Array(brokerId, remoteBrokerId)
    val isr = Array(brokerId)
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None), "Expected become leader transition to succeed")
    assertEquals(util.Set.of(brokerId), partition.partitionState.isr)

    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = 0L,
      logStartOffset = UnifiedLog.UNKNOWN_OFFSET,
      logEndOffset = UnifiedLog.UNKNOWN_OFFSET
    )

    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = 10L)

    // Check that the isr didn't change and alter update is scheduled
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(util.Set.of(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)
    assertEquals(1, alterPartitionManager.isrUpdates.size)
    assertEquals(Set(brokerId, remoteBrokerId), alterPartitionManager.isrUpdates.head.leaderAndIsr.isr.asScala.toSet)

    // Simulate invalid request failure
    alterPartitionManager.failIsrUpdate(Errors.INVALID_REQUEST)

    // Still no ISR change and no retry
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(util.Set.of(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)
    assertEquals(0, alterPartitionManager.isrUpdates.size)

    assertEquals(0, alterPartitionListener.expands.get)
    assertEquals(0, alterPartitionListener.shrinks.get)
    assertEquals(1, alterPartitionListener.failures.get)
  }

  @Test
  def testIsrExpansion(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Array(brokerId, remoteBrokerId)
    val isr = Array(brokerId)
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None), "Expected become leader transition to succeed")
    assertEquals(util.Set.of(brokerId), partition.partitionState.isr)

    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = 0L,
      logStartOffset = UnifiedLog.UNKNOWN_OFFSET,
      logEndOffset = UnifiedLog.UNKNOWN_OFFSET
    )

    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = 3L)
    assertEquals(util.Set.of(brokerId), partition.partitionState.isr)
    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = 0L,
      logStartOffset = 0L,
      logEndOffset = 3L
    )

    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = 10L)
    assertEquals(alterPartitionManager.isrUpdates.size, 1)
    val isrItem = alterPartitionManager.isrUpdates.head
    assertEquals(isrItem.leaderAndIsr.isr, util.Set.of[Integer](brokerId, remoteBrokerId))
    isrItem.leaderAndIsr.isrWithBrokerEpoch.asScala.foreach { brokerState =>
      // the broker epochs should be equal to broker epoch of the leader
      assertEquals(defaultBrokerEpoch(brokerState.brokerId()), brokerState.brokerEpoch())
    }
    assertEquals(util.Set.of(brokerId), partition.partitionState.isr)
    assertEquals(util.Set.of(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)
    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = time.milliseconds(),
      logStartOffset = 0L,
      logEndOffset = 10L
    )

    // Complete the ISR expansion
    alterPartitionManager.completeIsrUpdate(2)
    assertEquals(util.Set.of(brokerId, remoteBrokerId), partition.partitionState.isr)

    assertEquals(alterPartitionListener.expands.get, 1)
    assertEquals(alterPartitionListener.shrinks.get, 0)
    assertEquals(alterPartitionListener.failures.get, 0)
  }

  @Test
  def testIsrNotExpandedIfUpdateFails(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Array(brokerId, remoteBrokerId)
    val isr = Array(brokerId)
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None), "Expected become leader transition to succeed")
    assertEquals(util.Set.of(brokerId), partition.partitionState.isr)

    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = 0L,
      logStartOffset = UnifiedLog.UNKNOWN_OFFSET,
      logEndOffset = UnifiedLog.UNKNOWN_OFFSET
    )

    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = 10L)

    // Follower state is updated, but the ISR has not expanded
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(util.Set.of(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)
    assertEquals(alterPartitionManager.isrUpdates.size, 1)
    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = time.milliseconds(),
      logStartOffset = 0L,
      logEndOffset = 10L
    )

    // Simulate failure callback
    alterPartitionManager.failIsrUpdate(Errors.INVALID_UPDATE_VERSION)

    // Still no ISR change and it doesn't retry
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(util.Set.of(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)
    assertEquals(alterPartitionManager.isrUpdates.size, 0)
    assertEquals(alterPartitionListener.expands.get, 0)
    assertEquals(alterPartitionListener.shrinks.get, 0)
    assertEquals(alterPartitionListener.failures.get, 1)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("fenced", "shutdown", "unfenced"))
  def testHighWatermarkIncreasesWithFencedOrShutdownFollower(brokerState: String): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = Optional.empty)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Array(brokerId, remoteBrokerId)
    val shrinkedIsr = Array(brokerId)

    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    val partition = new Partition(
      topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterPartitionManager
    )

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    var partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(replicas)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = false, offsetCheckpoints, None), "Expected become leader transition to succeed")

    assertEquals(replicas.toSet.asJava, partition.partitionState.isr)
    assertEquals(replicas.toSet.asJava, partition.partitionState.maximalIsr)

    // Fetch to let the follower catch up to the log end offset
    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = log.logEndOffset)

    // Follower fetches and catches up to the log end offset.
    assertReplicaState(
      partition,
      remoteBrokerId,
      lastCaughtUpTimeMs = time.milliseconds(),
      logStartOffset = 0L,
      logEndOffset = log.logEndOffset
    )
    // Check that the leader updated the HWM to the LEO which is what the follower has
    assertEquals(log.logEndOffset, partition.localLogOrException.highWatermark)

    if (brokerState == "fenced") {
      when(metadataCache.isBrokerFenced(remoteBrokerId)).thenReturn(true)
    } else if (brokerState == "shutdown") {
      when(metadataCache.isBrokerShuttingDown(remoteBrokerId)).thenReturn(true)
    }

    // Append records to the log as leader of the current epoch
    seedLogData(log, numRecords = 10, leaderEpoch)

    // Controller shrinks the ISR after
    partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(shrinkedIsr)
      .setPartitionEpoch(2)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertFalse(partition.makeLeader(partitionRegistration, isNew = false, offsetCheckpoints, None), "Expected to stay leader")

    assertTrue(partition.isLeader)
    assertEquals(shrinkedIsr.toSet.asJava, partition.partitionState.isr)
    assertEquals(shrinkedIsr.toSet.asJava, partition.partitionState.maximalIsr)
    assertEquals(Set.empty, partition.getOutOfSyncReplicas(partition.replicaLagTimeMaxMs))

    // In the case of unfenced, the HWM doesn't increase, otherwise the HWM increases because the
    // fenced and shutdown replica is not considered during HWM calculation.
    if (brokerState == "unfenced") {
      assertEquals(10, partition.localLogOrException.highWatermark)
    } else {
      assertEquals(20, partition.localLogOrException.highWatermark)
    }
  }

  @Test
  def testIsrNotExpandedIfReplicaIsFencedOrShutdown(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Array(brokerId, remoteBrokerId)
    val isr = Array(brokerId)

    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    // Mark the remote broker as eligible or ineligible in the metadata cache of the leader.
    // When using kraft, we can make the broker ineligible by fencing it.
    def markRemoteReplicaEligible(eligible: Boolean): Unit = {
      when(metadataCache.isBrokerFenced(remoteBrokerId)).thenReturn(!eligible)
    }

    val partition = new Partition(
      topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterPartitionManager
    )

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None), "Expected become leader transition to succeed")
    assertEquals(isr.toSet.asJava, partition.partitionState.isr)
    assertEquals(isr.toSet.asJava, partition.partitionState.maximalIsr)

    markRemoteReplicaEligible(true)

    // Fetch to let the follower catch up to the log end offset and
    // to check if an expansion is possible.
    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = log.logEndOffset)

    // Follower fetches and catches up to the log end offset.
    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = time.milliseconds(),
      logStartOffset = 0L,
      logEndOffset = log.logEndOffset
    )

    // Expansion is triggered.
    assertEquals(isr.toSet.asJava, partition.partitionState.isr)
    assertEquals(replicas.toSet.asJava, partition.partitionState.maximalIsr)
    assertEquals(1, alterPartitionManager.isrUpdates.size)

    // Controller rejects the expansion because the broker is fenced or offline.
    alterPartitionManager.failIsrUpdate(Errors.INELIGIBLE_REPLICA)

    // The leader reverts back to the previous ISR.
    assertEquals(isr.toSet.asJava, partition.partitionState.isr)
    assertEquals(isr.toSet.asJava, partition.partitionState.maximalIsr)
    assertFalse(partition.partitionState.isInflight)
    assertEquals(0, alterPartitionManager.isrUpdates.size)

    // The leader eventually learns about the fenced or offline broker.
    markRemoteReplicaEligible(false)

    // The follower fetches again.
    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = log.logEndOffset)

    // Expansion is not triggered because the follower is fenced.
    assertEquals(isr.toSet.asJava, partition.partitionState.isr)
    assertEquals(isr.toSet.asJava, partition.partitionState.maximalIsr)
    assertFalse(partition.partitionState.isInflight)
    assertEquals(0, alterPartitionManager.isrUpdates.size)

    // The broker is eventually unfenced or brought back online.
    markRemoteReplicaEligible(true)

    // The follower fetches again.
    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = log.logEndOffset)

    // Expansion is triggered.
    assertEquals(isr.toSet.asJava, partition.partitionState.isr)
    assertEquals(replicas.toSet.asJava, partition.partitionState.maximalIsr)
    assertTrue(partition.partitionState.isInflight)
    assertEquals(1, alterPartitionManager.isrUpdates.size)

    // Expansion succeeds.
    alterPartitionManager.completeIsrUpdate(newPartitionEpoch = 1)

    // ISR is committed.
    assertEquals(replicas.toSet.asJava, partition.partitionState.isr)
    assertEquals(replicas.toSet.asJava, partition.partitionState.maximalIsr)
    assertFalse(partition.partitionState.isInflight)
    assertEquals(0, alterPartitionManager.isrUpdates.size)
  }

  @Test
  def testIsrCanExpandedIfBrokerEpochsMatchWithKraftMetadataCache(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId1 = brokerId + 1
    val remoteBrokerId2 = brokerId + 2
    val replicas = Array(brokerId, remoteBrokerId1, remoteBrokerId2)
    val isr = Array(brokerId, remoteBrokerId2)

    val metadataCache: KRaftMetadataCache = mock(classOf[KRaftMetadataCache])

    // Mark the remote broker 1 as eligible in the metadata cache.
    when(metadataCache.isBrokerFenced(remoteBrokerId1)).thenReturn(false)
    when(metadataCache.isBrokerShuttingDown(remoteBrokerId1)).thenReturn(false)

    val partition = new Partition(
      topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterPartitionManager,
    )

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None), "Expected become leader transition to succeed")
    assertEquals(isr.toSet.asJava, partition.partitionState.isr)
    assertEquals(isr.toSet.asJava, partition.partitionState.maximalIsr)

    // Fetch to let the follower catch up to the log end offset, but using a wrong broker epoch. The expansion should fail.
    addBrokerEpochToMockMetadataCache(metadataCache, Array(brokerId, remoteBrokerId2))
    // Create a race case where the replica epoch get bumped right after the previous fetch succeeded.
    val wrongReplicaEpoch = defaultBrokerEpoch(remoteBrokerId1) - 1
    when(metadataCache.getAliveBrokerEpoch(remoteBrokerId1)).thenReturn(Optional.of(wrongReplicaEpoch), Optional.of(defaultBrokerEpoch(remoteBrokerId1)))
    fetchFollower(partition,
      replicaId = remoteBrokerId1,
      fetchOffset = log.logEndOffset,
      replicaEpoch = Some(wrongReplicaEpoch)
    )

    assertReplicaState(partition, remoteBrokerId1,
      lastCaughtUpTimeMs = time.milliseconds(),
      logStartOffset = 0L,
      logEndOffset = log.logEndOffset,
      brokerEpoch = Some(wrongReplicaEpoch)
    )

    // Expansion is not triggered.
    assertEquals(isr.toSet.asJava, partition.partitionState.isr)
    assertEquals(isr.toSet.asJava, partition.partitionState.maximalIsr)
    assertEquals(0, alterPartitionManager.isrUpdates.size)

    // Fetch again, this time with correct default broker epoch.
    fetchFollower(partition,
      replicaId = remoteBrokerId1,
      fetchOffset = log.logEndOffset
    )

    // Follower should still catch up to the log end offset.
    assertReplicaState(partition, remoteBrokerId1,
      lastCaughtUpTimeMs = time.milliseconds(),
      logStartOffset = 0L,
      logEndOffset = log.logEndOffset
    )

    // Expansion is triggered.
    assertEquals(isr.toSet.asJava, partition.partitionState.isr)
    assertEquals(replicas.toSet.asJava, partition.partitionState.maximalIsr)
    assertEquals(1, alterPartitionManager.isrUpdates.size)
    val isrUpdate = alterPartitionManager.isrUpdates.head
    isrUpdate.leaderAndIsr.isrWithBrokerEpoch.asScala.foreach { brokerState =>
      if (brokerState.brokerId() == remoteBrokerId2) {
        // remoteBrokerId2 has not received any fetch request yet, it does not have broker epoch.
        assertEquals(-1, brokerState.brokerEpoch())
      } else {
        assertEquals(defaultBrokerEpoch(brokerState.brokerId()), brokerState.brokerEpoch())
      }
    }
  }

  @Test
  def testFenceFollowerFetchWithStaleBrokerEpoch(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = Optional.empty)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)
    val leaderEpoch = 5
    val remoteBrokerId1 = brokerId + 1
    val replicas = Array(brokerId, remoteBrokerId1)
    val isr = Array(brokerId, remoteBrokerId1)

    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    val partition = new Partition(
      topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterPartitionManager
    )

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None), "Expected become leader transition to succeed")
    assertEquals(isr.toSet.asJava, partition.partitionState.isr)
    assertEquals(isr.toSet.asJava, partition.partitionState.maximalIsr)

    val expectedReplicaEpoch = defaultBrokerEpoch(remoteBrokerId1)
    fetchFollower(partition,
      replicaId = remoteBrokerId1,
      fetchOffset = log.logEndOffset,
      replicaEpoch = Some(expectedReplicaEpoch)
    )

    // Fetch to let the follower catch up to the log end offset, but using a stale broker epoch. The Fetch should not
    // be able to update the fetch state.
    val wrongReplicaEpoch = defaultBrokerEpoch(remoteBrokerId1) - 1

    assertThrows(classOf[NotLeaderOrFollowerException], () => fetchFollower(partition,
      replicaId = remoteBrokerId1,
      fetchOffset = log.logEndOffset,
      replicaEpoch = Some(wrongReplicaEpoch)
    ))

    assertReplicaState(partition, remoteBrokerId1,
      lastCaughtUpTimeMs = time.milliseconds(),
      logStartOffset = 0L,
      logEndOffset = log.logEndOffset,
      brokerEpoch = Some(expectedReplicaEpoch)
    )
  }

  @Test
  def testIsrNotExpandedIfReplicaIsInControlledShutdown(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Array(brokerId, remoteBrokerId)
    val isr = Array(brokerId)

    addBrokerEpochToMockMetadataCache(metadataCache, replicas)
    val partition = new Partition(
      topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterPartitionManager
    )

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None), "Expected become leader transition to succeed")
    assertEquals(isr.toSet.asJava, partition.partitionState.isr)
    assertEquals(isr.toSet.asJava, partition.partitionState.maximalIsr)

    // Fetch to let the follower catch up to the log end offset and
    // to check if an expansion is possible.
    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = log.logEndOffset)

    // Follower fetches and catches up to the log end offset.
    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = time.milliseconds(),
      logStartOffset = 0L,
      logEndOffset = log.logEndOffset
    )

    // Expansion is triggered.
    assertEquals(isr.toSet.asJava, partition.partitionState.isr)
    assertEquals(replicas.toSet.asJava, partition.partitionState.maximalIsr)
    assertEquals(1, alterPartitionManager.isrUpdates.size)

    // Controller rejects the expansion because the broker is in controlled shutdown.
    alterPartitionManager.failIsrUpdate(Errors.INELIGIBLE_REPLICA)

    // The leader reverts back to the previous ISR.
    assertEquals(isr.toSet.asJava, partition.partitionState.isr)
    assertEquals(isr.toSet.asJava, partition.partitionState.maximalIsr)
    assertFalse(partition.partitionState.isInflight)
    assertEquals(0, alterPartitionManager.isrUpdates.size)

    // The leader eventually learns about the in controlled shutdown broker.
    when(metadataCache.isBrokerShuttingDown(remoteBrokerId)).thenReturn(true)

    // The follower fetches again.
    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = log.logEndOffset)

    // Expansion is not triggered because the follower is fenced.
    assertEquals(isr.toSet.asJava, partition.partitionState.isr)
    assertEquals(isr.toSet.asJava, partition.partitionState.maximalIsr)
    assertFalse(partition.partitionState.isInflight)
    assertEquals(0, alterPartitionManager.isrUpdates.size)

    // The broker eventually comes back.
    when(metadataCache.isBrokerShuttingDown(remoteBrokerId)).thenReturn(false)

    // The follower fetches again.
    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = log.logEndOffset)

    // Expansion is triggered.
    assertEquals(isr.toSet.asJava, partition.partitionState.isr)
    assertEquals(replicas.toSet.asJava, partition.partitionState.maximalIsr)
    assertTrue(partition.partitionState.isInflight)
    assertEquals(1, alterPartitionManager.isrUpdates.size)

    // Expansion succeeds.
    alterPartitionManager.completeIsrUpdate(newPartitionEpoch= 1)

    // ISR is committed.
    assertEquals(replicas.toSet.asJava, partition.partitionState.isr)
    assertEquals(replicas.toSet.asJava, partition.partitionState.maximalIsr)
    assertFalse(partition.partitionState.isInflight)
    assertEquals(0, alterPartitionManager.isrUpdates.size)
  }

  @Test
  def testRetryShrinkIsr(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = Optional.empty)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Array(brokerId, remoteBrokerId)
    val isr = Array(brokerId, remoteBrokerId)
    val topicId = Uuid.randomUuid()

    assertTrue(makeLeader(
      topicId = Some(topicId),
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      partitionEpoch = 1,
      isNew = true
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)

    // Sleep enough time to shrink the ISR
    time.sleep(partition.replicaLagTimeMaxMs + 1)

    // Try to shrink the ISR
    partition.maybeShrinkIsr()
    assertEquals(alterPartitionManager.isrUpdates.size, 1)
    assertEquals(alterPartitionManager.isrUpdates.head.leaderAndIsr.isr, util.Set.of[Integer](brokerId))
    assertEquals(util.Set.of(brokerId, remoteBrokerId), partition.partitionState.isr)
    assertEquals(util.Set.of(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)

    // The shrink fails and we retry
    alterPartitionManager.failIsrUpdate(Errors.NETWORK_EXCEPTION)
    assertEquals(0, alterPartitionListener.shrinks.get)
    assertEquals(1, alterPartitionListener.failures.get)
    assertEquals(1, partition.getPartitionEpoch)
    assertEquals(alterPartitionManager.isrUpdates.size, 1)
    assertEquals(util.Set.of(brokerId, remoteBrokerId), partition.partitionState.isr)
    assertEquals(util.Set.of(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    // The shrink succeeds after retrying
    alterPartitionManager.completeIsrUpdate(newPartitionEpoch = 2)
    assertEquals(1, alterPartitionListener.shrinks.get)
    assertEquals(2, partition.getPartitionEpoch)
    assertEquals(alterPartitionManager.isrUpdates.size, 0)
    assertEquals(util.Set.of(brokerId), partition.partitionState.isr)
    assertEquals(util.Set.of(brokerId), partition.partitionState.maximalIsr)
    assertEquals(log.logEndOffset, partition.localLogOrException.highWatermark)
  }

  @Test
  def testMaybeShrinkIsr(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId1 = brokerId + 1
    val remoteBrokerId2 = brokerId + 2
    val replicas = Array(brokerId, remoteBrokerId1, remoteBrokerId2)
    val isr = Array(brokerId, remoteBrokerId1, remoteBrokerId2)
    val initializeTimeMs = time.milliseconds()

    val metadataCache = mock(classOf[KRaftMetadataCache])
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    val partition = new Partition(
      topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterPartitionManager
    )
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None), "Expected become leader transition to succeed")

    assertEquals(0L, partition.localLogOrException.highWatermark)
    fetchFollower(partition, replicaId = remoteBrokerId1, fetchOffset = log.logEndOffset)

    assertReplicaState(partition, remoteBrokerId2,
      lastCaughtUpTimeMs = initializeTimeMs,
      logStartOffset = UnifiedLog.UNKNOWN_OFFSET,
      logEndOffset = UnifiedLog.UNKNOWN_OFFSET
    )

    // On initialization, the replica is considered caught up and should not be removed
    partition.maybeShrinkIsr()
    assertEquals(alterPartitionManager.isrUpdates.size, 0)
    assertEquals(util.Set.of(brokerId, remoteBrokerId1, remoteBrokerId2), partition.partitionState.isr)

    // If enough time passes without a fetch update, the ISR should shrink after the following maybeShrinkIsr
    time.sleep(partition.replicaLagTimeMaxMs + 1)

    // Shrink the ISR
    partition.maybeShrinkIsr()
    assertEquals(0, alterPartitionListener.shrinks.get)
    assertEquals(alterPartitionManager.isrUpdates.size, 1)
    assertEquals(alterPartitionManager.isrUpdates.head.leaderAndIsr.isr, util.Set.of[Integer](brokerId, remoteBrokerId1))
    val isrUpdate = alterPartitionManager.isrUpdates.head
    isrUpdate.leaderAndIsr.isrWithBrokerEpoch.asScala.foreach { brokerState =>
      assertEquals(defaultBrokerEpoch(brokerState.brokerId()), brokerState.brokerEpoch())
    }
    assertEquals(util.Set.of(brokerId, remoteBrokerId1, remoteBrokerId2), partition.partitionState.isr)
    assertEquals(util.Set.of(brokerId, remoteBrokerId1, remoteBrokerId2), partition.partitionState.maximalIsr)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    // After the ISR shrink completes, the ISR state should be updated and the
    // high watermark should be advanced
    alterPartitionManager.completeIsrUpdate(newPartitionEpoch = 2)
    assertEquals(1, alterPartitionListener.shrinks.get)
    assertEquals(2, partition.getPartitionEpoch)
    assertEquals(alterPartitionManager.isrUpdates.size, 0)
    assertEquals(util.Set.of(brokerId, remoteBrokerId1), partition.partitionState.isr)
    assertEquals(util.Set.of(brokerId, remoteBrokerId1), partition.partitionState.maximalIsr)
    assertEquals(log.logEndOffset, partition.localLogOrException.highWatermark)
  }

  @Test
  def testHighWatermarkAdvanceShouldNotAdvanceWhenUnderMinISR(): Unit = {
    configRepository.setTopicConfig(topicPartition.topic, TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3")
    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId1 = brokerId + 1
    val remoteBrokerId2 = brokerId + 2
    val replicas = Array(brokerId, remoteBrokerId1, remoteBrokerId2)
    val isr = Array(brokerId, remoteBrokerId1)

    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    val partition = new Partition(
      topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterPartitionManager
    )
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None), "Expected become leader transition to succeed")

    assertTrue(partition.isUnderMinIsr)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    fetchFollower(partition, replicaId = remoteBrokerId1, fetchOffset = log.logEndOffset)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    // Though the maximum ISR has been larger than min ISR, the HWM can't advance.
    fetchFollower(partition, replicaId = remoteBrokerId2, fetchOffset = log.logEndOffset)
    assertEquals(0L, partition.localLogOrException.highWatermark)
    assertEquals(3, partition.partitionState.maximalIsr.size)
    assertEquals(1, alterPartitionManager.isrUpdates.size)

    // Update the ISR to size 3
    alterPartitionManager.completeIsrUpdate(2)
    assertFalse(partition.isUnderMinIsr)
    assertEquals(log.logEndOffset, partition.localLogOrException.highWatermark)
  }

  @Test
  def testAlterIsrLeaderAndIsrRace(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Array(brokerId, remoteBrokerId)
    val isr = Array(brokerId, remoteBrokerId)
    val initializeTimeMs = time.milliseconds()

    assertTrue(makeLeader(
      topicId = topicId,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      partitionEpoch = 1,
      isNew = true
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)

    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = initializeTimeMs,
      logStartOffset = UnifiedLog.UNKNOWN_OFFSET,
      logEndOffset = UnifiedLog.UNKNOWN_OFFSET
    )

    // Shrink the ISR
    time.sleep(partition.replicaLagTimeMaxMs + 1)
    partition.maybeShrinkIsr()
    assertTrue(partition.partitionState.isInflight)

    // Become leader again, reset the ISR state
    assertFalse(makeLeader(
      topicId = topicId,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      partitionEpoch = 2,
      isNew = false
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)
    assertFalse(partition.partitionState.isInflight, "ISR should be committed and not inflight")

    // Try the shrink again, should not submit until AlterIsr response arrives
    time.sleep(partition.replicaLagTimeMaxMs + 1)
    partition.maybeShrinkIsr()
    assertFalse(partition.partitionState.isInflight, "ISR should still be committed and not inflight")

    // Complete the AlterIsr update and now we can make modifications again
    alterPartitionManager.completeIsrUpdate(10)
    partition.maybeShrinkIsr()
    assertTrue(partition.partitionState.isInflight, "ISR should be pending a shrink")
  }

  @Test
  def testShouldNotShrinkIsrIfPreviousFetchIsCaughtUp(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Array(brokerId, remoteBrokerId)
    val isr = Array(brokerId, remoteBrokerId)
    val initializeTimeMs = time.milliseconds()
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    assertTrue(makeLeader(
      topicId = topicId,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      partitionEpoch = 1,
      isNew = true
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)

    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = initializeTimeMs,
      logStartOffset = UnifiedLog.UNKNOWN_OFFSET,
      logEndOffset = UnifiedLog.UNKNOWN_OFFSET
    )

    // There is a short delay before the first fetch. The follower is not yet caught up to the log end.
    time.sleep(5000)
    val firstFetchTimeMs = time.milliseconds()
    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = 5L, fetchTimeMs = firstFetchTimeMs)
    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = initializeTimeMs,
      logStartOffset = 0L,
      logEndOffset = 5L
    )
    assertEquals(5L, partition.localLogOrException.highWatermark)

    // Some new data is appended, but the follower catches up to the old end offset.
    // The total elapsed time from initialization is larger than the max allowed replica lag.
    time.sleep(5001)
    seedLogData(log, numRecords = 5, leaderEpoch = leaderEpoch)
    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = 10L, fetchTimeMs = time.milliseconds())
    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = firstFetchTimeMs,
      logStartOffset = 0L,
      logEndOffset = 10L
    )
    assertEquals(10L, partition.localLogOrException.highWatermark)

    // The ISR should not be shrunk because the follower has caught up with the leader at the
    // time of the first fetch.
    partition.maybeShrinkIsr()
    assertEquals(util.Set.of(brokerId, remoteBrokerId), partition.partitionState.isr)
    assertEquals(alterPartitionManager.isrUpdates.size, 0)
  }

  @Test
  def testShouldNotShrinkIsrIfFollowerCaughtUpToLogEnd(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Array(brokerId, remoteBrokerId)
    val isr = Array(brokerId, remoteBrokerId)
    val initializeTimeMs = time.milliseconds()
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    assertTrue(makeLeader(
      topicId = topicId,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      partitionEpoch = 1,
      isNew = true
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)

    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = initializeTimeMs,
      logStartOffset = UnifiedLog.UNKNOWN_OFFSET,
      logEndOffset = UnifiedLog.UNKNOWN_OFFSET
    )

    // The follower catches up to the log end immediately.
    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = 10L)
    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = time.milliseconds(),
      logStartOffset = 0L,
      logEndOffset = 10L
    )
    assertEquals(10L, partition.localLogOrException.highWatermark)

    // Sleep longer than the max allowed follower lag
    time.sleep(30001)

    // The ISR should not be shrunk because the follower is caught up to the leader's log end
    partition.maybeShrinkIsr()
    assertEquals(util.Set.of(brokerId, remoteBrokerId), partition.partitionState.isr)
    assertEquals(alterPartitionManager.isrUpdates.size, 0)
  }

  @Test
  def testIsrNotShrunkIfUpdateFails(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Array(brokerId, remoteBrokerId)
    val isr = Array(brokerId, remoteBrokerId)
    val initializeTimeMs = time.milliseconds()

    assertTrue(makeLeader(
      topicId = topicId,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      partitionEpoch = 1,
      isNew = true
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)

    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = initializeTimeMs,
      logStartOffset = UnifiedLog.UNKNOWN_OFFSET,
      logEndOffset = UnifiedLog.UNKNOWN_OFFSET
    )

    time.sleep(30001)

    // Enqueue and AlterIsr that will fail
    partition.maybeShrinkIsr()
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(alterPartitionManager.isrUpdates.size, 1)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    // Simulate failure callback
    alterPartitionManager.failIsrUpdate(Errors.INVALID_UPDATE_VERSION)

    // Ensure ISR hasn't changed
    assertEquals(partition.partitionState.getClass, classOf[PendingShrinkIsr])
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(alterPartitionManager.isrUpdates.size, 0)
    assertEquals(0L, partition.localLogOrException.highWatermark)
  }

  @Test
  def testAlterIsrNewLeaderElected(): Unit = {
    handleAlterIsrFailure(Errors.NEW_LEADER_ELECTED,
      (brokerId: Int, remoteBrokerId: Int, partition: Partition) => {
        assertEquals(partition.partitionState.isr, util.Set.of(brokerId))
        assertEquals(partition.partitionState.maximalIsr, util.Set.of(brokerId, remoteBrokerId))
        assertEquals(alterPartitionManager.isrUpdates.size, 0)
      })
  }

  @Test
  def testAlterIsrUnknownTopic(): Unit = {
    handleAlterIsrFailure(Errors.UNKNOWN_TOPIC_OR_PARTITION,
      (brokerId: Int, remoteBrokerId: Int, partition: Partition) => {
        assertEquals(partition.partitionState.isr, util.Set.of(brokerId))
        assertEquals(partition.partitionState.maximalIsr, util.Set.of(brokerId, remoteBrokerId))
        assertEquals(alterPartitionManager.isrUpdates.size, 0)
      })
  }

  @Test
  def testAlterIsrInvalidVersion(): Unit = {
    handleAlterIsrFailure(Errors.INVALID_UPDATE_VERSION,
      (brokerId: Int, remoteBrokerId: Int, partition: Partition) => {
        assertEquals(partition.partitionState.isr, util.Set.of(brokerId))
        assertEquals(partition.partitionState.maximalIsr, util.Set.of(brokerId, remoteBrokerId))
        assertEquals(alterPartitionManager.isrUpdates.size, 0)
      })
  }

  @Test
  def testAlterIsrUnexpectedError(): Unit = {
    handleAlterIsrFailure(Errors.UNKNOWN_SERVER_ERROR,
      (brokerId: Int, remoteBrokerId: Int, partition: Partition) => {
        // We retry these
        assertEquals(partition.partitionState.isr, util.Set.of(brokerId))
        assertEquals(partition.partitionState.maximalIsr, util.Set.of(brokerId, remoteBrokerId))
        assertEquals(alterPartitionManager.isrUpdates.size, 1)
      })
  }

  def handleAlterIsrFailure(error: Errors, callback: (Int, Int, Partition) => Unit): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Array(brokerId, remoteBrokerId)
    val isr = Array(brokerId)
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    assertTrue(makeLeader(
      topicId = topicId,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      partitionEpoch = 1,
      isNew = true
    ))
    assertEquals(10L, partition.localLogOrException.highWatermark)

    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = 0L,
      logStartOffset = UnifiedLog.UNKNOWN_OFFSET,
      logEndOffset = UnifiedLog.UNKNOWN_OFFSET
    )

    // This will attempt to expand the ISR
    val firstFetchTimeMs = time.milliseconds()
    fetchFollower(partition, replicaId = remoteBrokerId, fetchOffset = 10L, fetchTimeMs = firstFetchTimeMs)

    // Follower state is updated, but the ISR has not expanded
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(util.Set.of(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)
    assertEquals(alterPartitionManager.isrUpdates.size, 1)
    assertReplicaState(partition, remoteBrokerId,
      lastCaughtUpTimeMs = firstFetchTimeMs,
      logStartOffset = 0L,
      logEndOffset = 10L
    )

    // Failure
    alterPartitionManager.failIsrUpdate(error)
    callback(brokerId, remoteBrokerId, partition)
  }

  private def createClientResponseWithAlterPartitionResponse(
    topicPartition: TopicPartition,
    partitionErrorCode: Short,
    isr: util.List[Integer] = util.List.of[Integer],
    leaderEpoch: Int = 0,
    partitionEpoch: Int = 0
  ): ClientResponse = {
    val alterPartitionResponseData = new AlterPartitionResponseData()
    val topicResponse = new AlterPartitionResponseData.TopicData().setTopicId(topicId.get)

    topicResponse.partitions.add(new AlterPartitionResponseData.PartitionData()
      .setPartitionIndex(topicPartition.partition)
      .setIsr(isr)
      .setLeaderEpoch(leaderEpoch)
      .setPartitionEpoch(partitionEpoch)
      .setErrorCode(partitionErrorCode))
    alterPartitionResponseData.topics.add(topicResponse)

    val alterPartitionResponse = new AlterPartitionResponse(alterPartitionResponseData)

    new ClientResponse(new RequestHeader(ApiKeys.ALTER_PARTITION, 0, "client", 1),
      null, null, 0, 0, false, null, null, alterPartitionResponse)
  }

  @Test
  def testPartitionShouldRetryAlterPartitionRequest(): Unit = {
    val mockChannelManager = mock(classOf[NodeToControllerChannelManager])
    val alterPartitionManager = new DefaultAlterPartitionManager(
      controllerChannelManager = mockChannelManager,
      scheduler = mock(classOf[KafkaScheduler]),
      time = time,
      brokerId = brokerId,
      brokerEpochSupplier = () => 0
    )

    partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterPartitionManager)

    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val follower3 = brokerId + 3
    val replicas = Array(brokerId, follower1, follower2, follower3)
    val isr = Array(brokerId, follower1, follower2)
    val partitionEpoch = 1
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    doNothing().when(delayedOperations).checkAndCompleteAll()

    // Fail the first alter partition request with a retryable error to trigger a retry from the partition callback
    val alterPartitionResponseWithUnknownServerError =
      createClientResponseWithAlterPartitionResponse(topicPartition, Errors.UNKNOWN_SERVER_ERROR.code)

    // Complete the ISR expansion
    val alterPartitionResponseWithoutError =
      createClientResponseWithAlterPartitionResponse(topicPartition, Errors.NONE.code, util.List.of[Integer](brokerId, follower1, follower2, follower3), leaderEpoch, partitionEpoch + 1)

    when(mockChannelManager.sendRequest(any(), any()))
      .thenAnswer { invocation =>
        val controllerRequestCompletionHandler = invocation.getArguments()(1).asInstanceOf[ControllerRequestCompletionHandler]
        controllerRequestCompletionHandler.onComplete(alterPartitionResponseWithUnknownServerError)
      }
      .thenAnswer { invocation =>
        val controllerRequestCompletionHandler = invocation.getArguments()(1).asInstanceOf[ControllerRequestCompletionHandler]
        controllerRequestCompletionHandler.onComplete(alterPartitionResponseWithoutError)
      }

    assertTrue(makeLeader(
      topicId = topicId,
      leaderEpoch,
      isr,
      replicas,
      partitionEpoch,
      isNew = true
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)

    // Expand ISR
    fetchFollower(partition, replicaId = follower3, fetchOffset = 10L)

    assertEquals(util.Set.of(brokerId, follower1, follower2, follower3), partition.partitionState.isr)
    assertEquals(partitionEpoch + 1, partition.getPartitionEpoch)
    // Verify that the AlterPartition request was sent twice
    verify(mockChannelManager, times(2)).sendRequest(any(), any())
    // After the retry, the partition state should be committed
    assertFalse(partition.partitionState.isInflight)
  }

  @Test
  def testSingleInFlightAlterIsr(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val leaderEpoch = 5
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val follower3 = brokerId + 3
    val replicas = Array(brokerId, follower1, follower2, follower3)
    val isr = Array(brokerId, follower1, follower2)
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    doNothing().when(delayedOperations).checkAndCompleteAll()

    assertTrue(makeLeader(
      topicId = topicId,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      partitionEpoch = 1,
      isNew = true
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)

    // Expand ISR
    fetchFollower(partition, replicaId = follower3, fetchOffset = 10L)
    assertEquals(util.Set.of(brokerId, follower1, follower2), partition.partitionState.isr)
    assertEquals(util.Set.of(brokerId, follower1, follower2, follower3), partition.partitionState.maximalIsr)

    // One AlterIsr request in-flight
    assertEquals(alterPartitionManager.isrUpdates.size, 1)

    // Try to modify ISR again, should do nothing
    time.sleep(partition.replicaLagTimeMaxMs + 1)
    partition.maybeShrinkIsr()
    assertEquals(alterPartitionManager.isrUpdates.size, 1)
  }

  @Test
  def testUseCheckpointToInitializeHighWatermark(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = topicId.toJava)
    seedLogData(log, numRecords = 6, leaderEpoch = 5)

    when(offsetCheckpoints.fetch(logDir1.getAbsolutePath, topicPartition))
      .thenReturn(Optional.of(long2Long(4L)))

    val replicas = Array(brokerId, brokerId + 1)
    val leaderRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(6)
      .setIsr(replicas)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    partition.makeLeader(leaderRegistration, isNew = false, offsetCheckpoints, None)
    assertEquals(4, partition.localLogOrException.highWatermark)
  }

  @Test
  def testTopicIdAndPartitionMetadataFileForLeader(): Unit = {
    val leaderEpoch = 5
    val topicId = Uuid.randomUuid()
    val replicas = Array(brokerId, brokerId + 1)
    val leaderRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(replicas)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    partition.makeLeader(leaderRegistration, isNew = false, offsetCheckpoints, Some(topicId))

    checkTopicId(topicId, partition)

    // Create new Partition object for same topicPartition
    val partition2 = new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterPartitionManager)

    // partition2 should not yet be associated with the log, but should be able to get ID
    assertTrue(partition2.topicId.isDefined)
    assertEquals(topicId, partition2.topicId.get)
    assertFalse(partition2.log.isDefined)

    // Calling makeLeader with a new topic ID should not overwrite the old topic ID. We should get an InconsistentTopicIdException.
    // This scenario should not occur, since the topic ID check will fail.
    assertThrows(classOf[InconsistentTopicIdException], () => partition2.makeLeader(leaderRegistration, isNew = false, offsetCheckpoints, Some(Uuid.randomUuid())))

    // Calling makeLeader with no topic ID should not overwrite the old topic ID. We should get the original log.
    partition2.makeLeader(leaderRegistration, isNew = false, offsetCheckpoints, None)
    checkTopicId(topicId, partition2)
  }

  @Test
  def testTopicIdAndPartitionMetadataFileForFollower(): Unit = {
    val leaderEpoch = 5
    val topicId = Uuid.randomUuid()
    val replicas = Array(brokerId, brokerId + 1)
    val leaderRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(replicas)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    partition.makeLeader(leaderRegistration, isNew = false, offsetCheckpoints, Some(topicId))

    checkTopicId(topicId, partition)

    // Create new Partition object for same topicPartition
    val partition2 = new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterPartitionManager)

    // partition2 should not yet be associated with the log, but should be able to get ID
    assertTrue(partition2.topicId.isDefined)
    assertEquals(topicId, partition2.topicId.get)
    assertFalse(partition2.log.isDefined)

    // Calling makeFollower with a new topic ID should not overwrite the old topic ID. We should get an InconsistentTopicIdException.
    // This scenario should not occur, since the topic ID check will fail.
    assertThrows(classOf[InconsistentTopicIdException], () => partition2.makeFollower(leaderRegistration, isNew = false, offsetCheckpoints, Some(Uuid.randomUuid())))

    // Calling makeFollower with no topic ID should not overwrite the old topic ID. We should get the original log.
    partition2.makeFollower(leaderRegistration, isNew = false, offsetCheckpoints, None)
    checkTopicId(topicId, partition2)
  }

  def checkTopicId(expectedTopicId: Uuid, partition: Partition): Unit = {
    assertTrue(partition.topicId.isDefined)
    assertEquals(expectedTopicId, partition.topicId.get)
    assertTrue(partition.log.isDefined)
    val log = partition.log.get
    assertEquals(expectedTopicId, log.topicId.get)
    assertTrue(log.partitionMetadataFile.get.exists())
    assertEquals(expectedTopicId, log.partitionMetadataFile.get.read().topicId)
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
      KafkaYammerMetrics.defaultRegistry().allMetrics().asScala.find { case (metricName, _) =>
        metricName.getName == metric && metricName.getType == "Partition"
      }.map(_._2)
    }

    assertTrue(metricsToCheck.forall(getMetric(_).isDefined))

    Partition.removeMetrics(topicPartition)

    assertEquals(Set(), KafkaYammerMetrics.defaultRegistry().allMetrics().asScala.keySet.filter(_.getType == "Partition"))
  }

  @Test
  def testUnderReplicatedPartitionsCorrectSemantics(): Unit = {
    val replicas = Array(brokerId, brokerId + 1, brokerId + 2)
    val isr = Array(brokerId, brokerId + 1)
    val leaderRegistrationBuilder = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(6)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
    partition.makeLeader(leaderRegistrationBuilder.build(), isNew = false, offsetCheckpoints, None)
    assertTrue(partition.isUnderReplicated)

    partition.makeLeader(leaderRegistrationBuilder.setIsr(replicas).build(), isNew = false, offsetCheckpoints, None)
    assertFalse(partition.isUnderReplicated)
  }

  @Test
  def testUpdateAssignmentAndIsr(): Unit = {
    val topicPartition = new TopicPartition("test", 1)
    val partition = new Partition(
      topicPartition, 1000, 0, () => defaultBrokerEpoch(0),
      Time.SYSTEM, mock(classOf[AlterPartitionListener]), mock(classOf[DelayedOperations]),
      mock(classOf[KRaftMetadataCache]), mock(classOf[LogManager]), mock(classOf[AlterPartitionManager]))

    val replicas = Seq(0, 1, 2, 3)
    val followers = Seq(1, 2, 3)
    val isr = Set(0, 1, 2, 3)
    val adding = Seq(4, 5)
    val removing = Seq(1, 2)

    // Test with ongoing reassignment
    partition.updateAssignmentAndIsr(
      replicas,
      isLeader = true,
      isr,
      adding,
      removing,
      LeaderRecoveryState.RECOVERED
    )

    assertTrue(partition.assignmentState.isInstanceOf[OngoingReassignmentState], "The assignmentState is not OngoingReassignmentState")
    assertEquals(replicas.map(Int.box).asJava, partition.assignmentState.replicas)
    assertEquals(isr.map(Int.box).asJava, partition.partitionState.isr)
    assertEquals(adding.map(Int.box).asJava, partition.assignmentState.asInstanceOf[OngoingReassignmentState].addingReplicas)
    assertEquals(removing.map(Int.box).asJava, partition.assignmentState.asInstanceOf[OngoingReassignmentState].removingReplicas)
    assertEquals(followers, partition.remoteReplicas.map(_.brokerId))

    // Test with simple assignment
    val replicas2 = Seq(0, 3, 4, 5)
    val followers2 = Seq(3, 4, 5)
    val isr2 = Set(0, 3, 4, 5)
    partition.updateAssignmentAndIsr(
      replicas2,
      isLeader = true,
      isr2,
      Seq.empty,
      Seq.empty,
      LeaderRecoveryState.RECOVERED
    )

    assertTrue(partition.assignmentState.isInstanceOf[SimpleAssignmentState], "The assignmentState is not SimpleAssignmentState")
    assertEquals(replicas2.map(Int.box).asJava, partition.assignmentState.replicas)
    assertEquals(isr2.map(Int.box).asJava, partition.partitionState.isr)
    assertEquals(followers2, partition.remoteReplicas.map(_.brokerId))

    // Test with no followers
    val replicas3 = Seq(1, 2, 3, 4)
    partition.updateAssignmentAndIsr(
      replicas3,
      isLeader = false,
      Set.empty,
      Seq.empty,
      Seq.empty,
      LeaderRecoveryState.RECOVERED
    )

    assertTrue(partition.assignmentState.isInstanceOf[SimpleAssignmentState], "The assignmentState is not SimpleAssignmentState")
    assertEquals(replicas3, partition.assignmentState.replicas.asScala)
    assertEquals(util.Set.of(), partition.partitionState.isr)
    assertEquals(Seq.empty, partition.remoteReplicas.map(_.brokerId))
  }

  /**
   * Test when log is getting initialized, its config remains untouched after initialization is done.
   */
  @Test
  def testLogConfigNotDirty(): Unit = {
    logManager.shutdown()
    val spyConfigRepository = spy(configRepository)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, configRepository = spyConfigRepository,
      cleanerConfig = new CleanerConfig(false), time = time)
    val spyLogManager = spy(logManager)
    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      spyLogManager,
      alterPartitionManager)

    partition.createLog(isNew = true, isFutureReplica = false, offsetCheckpoints, topicId = None, targetLogDirectoryId = None)

    // Validate that initializingLog and finishedInitializingLog was called
    verify(spyLogManager).initializingLog(ArgumentMatchers.eq(topicPartition))
    verify(spyLogManager).finishedInitializingLog(ArgumentMatchers.eq(topicPartition), ArgumentMatchers.any())

    // We should retrieve configs only once
    verify(spyConfigRepository, times(1)).topicConfig(topicPartition.topic())
  }

  /**
   * Test when log is getting initialized, its config remains gets reloaded if Topic config gets changed
   * before initialization is done.
   */
  @Test
  def testLogConfigDirtyAsTopicUpdated(): Unit = {
    logManager.shutdown()
    val spyConfigRepository = spy(configRepository)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, configRepository = spyConfigRepository,
      cleanerConfig = new CleanerConfig(false), time = time)
    val spyLogManager = spy(logManager)
    doAnswer((_: InvocationOnMock) => {
      logManager.initializingLog(topicPartition)
      logManager.topicConfigUpdated(topicPartition.topic())
    }).when(spyLogManager).initializingLog(ArgumentMatchers.eq(topicPartition))

    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      spyLogManager,
      alterPartitionManager)

    partition.createLog(isNew = true, isFutureReplica = false, offsetCheckpoints, topicId = None, targetLogDirectoryId = None)

    // Validate that initializingLog and finishedInitializingLog was called
    verify(spyLogManager).initializingLog(ArgumentMatchers.eq(topicPartition))
    verify(spyLogManager).finishedInitializingLog(ArgumentMatchers.eq(topicPartition), ArgumentMatchers.any())

    // We should retrieve configs twice, once before log is created, and second time once
    // we find log config is dirty and refresh it.
    verify(spyConfigRepository, times(2)).topicConfig(topicPartition.topic())
  }

  /**
   * Test when log is getting initialized, its config remains gets reloaded if Broker config gets changed
   * before initialization is done.
   */
  @Test
  def testLogConfigDirtyAsBrokerUpdated(): Unit = {
    logManager.shutdown()
    val spyConfigRepository = spy(configRepository)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, configRepository = spyConfigRepository,
      cleanerConfig = new CleanerConfig(false), time = time)
    logManager.startup(Set.empty)

    val spyLogManager = spy(logManager)
    doAnswer((_: InvocationOnMock) => {
      logManager.initializingLog(topicPartition)
      logManager.brokerConfigUpdated()
    }).when(spyLogManager).initializingLog(ArgumentMatchers.eq(topicPartition))

    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      spyLogManager,
      alterPartitionManager)

    partition.createLog(isNew = true, isFutureReplica = false, offsetCheckpoints, topicId = None, targetLogDirectoryId = None)

    // Validate that initializingLog and finishedInitializingLog was called
    verify(spyLogManager).initializingLog(ArgumentMatchers.eq(topicPartition))
    verify(spyLogManager).finishedInitializingLog(ArgumentMatchers.eq(topicPartition), ArgumentMatchers.any())

    // We should get configs twice, once before log is created, and second time once
    // we find log config is dirty and refresh it.
    verify(spyConfigRepository, times(2)).topicConfig(topicPartition.topic())
  }

  @Test
  def testDoNotResetReplicaStateIfLeaderEpochIsNotBumped(): Unit = {
    val leaderId = brokerId
    val followerId = brokerId + 1
    val replicas = Array(leaderId, followerId)
    val leaderEpoch = 8
    val topicId = Uuid.randomUuid()
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)

    val LeaderRegistrationBuilder = new PartitionRegistration.Builder()
      .setLeader(leaderId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(Array(leaderId))
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
    assertTrue(partition.makeLeader(LeaderRegistrationBuilder.build(), isNew = true, offsetCheckpoints, Some(topicId)))
    assertEquals(1, partition.getPartitionEpoch)
    assertEquals(leaderEpoch, partition.getLeaderEpoch)
    assertEquals(util.Set.of(leaderId), partition.partitionState.isr)

    // Follower's state is initialized with unknown offset because it is not
    // in the ISR.
    assertReplicaState(partition, followerId,
      lastCaughtUpTimeMs = 0L,
      logStartOffset = UnifiedLog.UNKNOWN_OFFSET,
      logEndOffset = UnifiedLog.UNKNOWN_OFFSET
    )

    // Follower fetches and updates its replica state.
    fetchFollower(partition, replicaId = followerId, fetchOffset = 0L)
    assertReplicaState(partition, followerId,
      lastCaughtUpTimeMs = time.milliseconds(),
      logStartOffset = 0L,
      logEndOffset = 0L
    )

    // makeLeader is called again with the same leader epoch but with
    // a newer partition epoch. This can happen in KRaft when a partition
    // is reassigned. The leader epoch is not bumped when we add replicas.
    assertFalse(partition.makeLeader(LeaderRegistrationBuilder.setPartitionEpoch(2).build(), isNew = false, offsetCheckpoints, Some(topicId)))
    assertEquals(2, partition.getPartitionEpoch)
    assertEquals(leaderEpoch, partition.getLeaderEpoch)
    assertEquals(util.Set.of(leaderId), partition.partitionState.isr)

    // Follower's state has not been reset.
    assertReplicaState(partition, followerId,
      lastCaughtUpTimeMs = time.milliseconds(),
      logStartOffset = 0L,
      logEndOffset = 0L
    )
  }

  @Test
  def testDoNotUpdateEpochStartOffsetIfLeaderEpochIsNotBumped(): Unit = {
    val leaderId = brokerId
    val followerId = brokerId + 1
    val replicas = Array(leaderId, followerId)
    val leaderEpoch = 8
    val topicId = Uuid.randomUuid()

    val LeaderRegistrationBuilder = new PartitionRegistration.Builder()
      .setLeader(leaderId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(Array(leaderId))
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
    assertTrue(partition.makeLeader(LeaderRegistrationBuilder.build(), isNew = true, offsetCheckpoints, Some(topicId)))
    assertEquals(1, partition.getPartitionEpoch)
    assertEquals(leaderEpoch, partition.getLeaderEpoch)
    assertEquals(util.Set.of(leaderId), partition.partitionState.isr)
    assertEquals(Some(0L), partition.leaderEpochStartOffsetOpt)

    val leaderLog = partition.localLogOrException
    assertEquals(Optional.of(new EpochEntry(leaderEpoch, 0L)), leaderLog.leaderEpochCache.latestEntry)

    // Write to the log to increment the log end offset.
    leaderLog.appendAsLeader(MemoryRecords.withRecords(0L, Compression.NONE, 0,
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k1".getBytes, "v1".getBytes)
    ), leaderEpoch)

    // makeLeader is called again with the same leader epoch but with
    // a newer partition epoch.
    assertFalse(partition.makeLeader(LeaderRegistrationBuilder.setPartitionEpoch(2).build(), isNew = false, offsetCheckpoints, Some(topicId)))
    assertEquals(2, partition.getPartitionEpoch)
    assertEquals(leaderEpoch, partition.getLeaderEpoch)
    assertEquals(util.Set.of(leaderId), partition.partitionState.isr)
    assertEquals(Some(0L), partition.leaderEpochStartOffsetOpt)
    assertEquals(Optional.of(new EpochEntry(leaderEpoch, 0L)), leaderLog.leaderEpochCache.latestEntry)
  }

  @Test
  def testIgnoreLeaderPartitionStateChangeWithOlderPartitionEpoch(): Unit = {
    val leaderId = brokerId
    val replicas = Array(leaderId)
    val leaderEpoch = 8
    val topicId = Uuid.randomUuid()

    val LeaderRegistrationBuilder = new PartitionRegistration.Builder()
      .setLeader(leaderId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(Array(leaderId))
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
    assertTrue(partition.makeLeader(LeaderRegistrationBuilder.build(), isNew = true, offsetCheckpoints, Some(topicId)))
    assertEquals(1, partition.getPartitionEpoch)
    assertEquals(leaderEpoch, partition.getLeaderEpoch)

    // makeLeader is called again with the same leader epoch but with
    // a older partition epoch.
    assertFalse(partition.makeLeader(LeaderRegistrationBuilder.setPartitionEpoch(0).build(), isNew = false, offsetCheckpoints, Some(topicId)))
    assertEquals(1, partition.getPartitionEpoch)
    assertEquals(leaderEpoch, partition.getLeaderEpoch)
  }

  @Test
  def testIgnoreFollowerPartitionStateChangeWithOlderPartitionEpoch(): Unit = {
    val leaderId = brokerId
    val followerId = brokerId + 1
    val replicas = Array(leaderId, followerId)
    val leaderEpoch = 8
    val topicId = Uuid.randomUuid()

    val LeaderRegistrationBuilder = new PartitionRegistration.Builder()
      .setLeader(followerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(Array(leaderId))
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
    assertTrue(partition.makeLeader(LeaderRegistrationBuilder.build(), isNew = true, offsetCheckpoints, Some(topicId)))
    assertEquals(1, partition.getPartitionEpoch)
    assertEquals(leaderEpoch, partition.getLeaderEpoch)

    // makeLeader is called again with the same leader epoch but with
    // a older partition epoch.
    assertFalse(partition.makeLeader(LeaderRegistrationBuilder.setIsr(Array(leaderId, followerId)).build(), isNew = true, offsetCheckpoints, Some(topicId)))
    assertEquals(1, partition.getPartitionEpoch)
    assertEquals(leaderEpoch, partition.getLeaderEpoch)
  }

  @Test
  def testFollowerShouldNotHaveAnyRemoteReplicaStates(): Unit = {
    val localReplica = brokerId
    val remoteReplica1 = brokerId + 1
    val remoteReplica2 = brokerId + 2
    val replicas = Array(localReplica, remoteReplica1, remoteReplica2)
    val topicId = Uuid.randomUuid()

    // The local replica is the leader.
    val leaderRegistrationBuilder = new PartitionRegistration.Builder()
      .setLeader(localReplica)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(1)
      .setIsr(replicas)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
    assertTrue(partition.makeLeader(leaderRegistrationBuilder.build(), isNew = true, offsetCheckpoints, Some(topicId)))
    assertEquals(1, partition.getPartitionEpoch)
    assertEquals(1, partition.getLeaderEpoch)
    assertEquals(Some(localReplica), partition.leaderReplicaIdOpt)
    assertEquals(replicas.toSet.asJava, partition.partitionState.isr)
    assertEquals(Seq(remoteReplica1, remoteReplica2), partition.remoteReplicas.map(_.brokerId).toSeq)
    assertEquals(replicas.toList.asJava, partition.assignmentState.replicas)

    // The local replica becomes a follower.
    val updatedLeaderRegistration = leaderRegistrationBuilder
      .setLeader(remoteReplica1)
      .setLeaderEpoch(2)
      .setPartitionEpoch(2)
      .build()
    assertTrue(partition.makeFollower(updatedLeaderRegistration, isNew = false, offsetCheckpoints, Some(topicId)))
    assertEquals(2, partition.getPartitionEpoch)
    assertEquals(2, partition.getLeaderEpoch)
    assertEquals(Some(remoteReplica1), partition.leaderReplicaIdOpt)
    assertEquals(util.Set.of(), partition.partitionState.isr)
    assertEquals(Seq.empty, partition.remoteReplicas.map(_.brokerId).toSeq)
    assertEquals(replicas.toList.asJava, partition.assignmentState.replicas)
  }

  @Test
  def testAddAndRemoveListeners(): Unit = {
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, topicId = topicId)

    val replicas = Array(brokerId, brokerId + 1)
    val isr = replicas
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(0)
      .setIsr(isr)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .setPartitionEpoch(1)
      .build()
    partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None)
    val listener1 = new MockPartitionListener()
    val listener2 = new MockPartitionListener()

    assertTrue(partition.maybeAddListener(listener1))
    listener1.verify()

    partition.appendRecordsToLeader(
      records = TestUtils.records(List(new SimpleRecord("k1".getBytes, "v1".getBytes))),
      origin = AppendOrigin.CLIENT,
      requiredAcks = 0,
      requestLocal = RequestLocal.noCaching
    )

    listener1.verify()
    listener2.verify()

    assertTrue(partition.maybeAddListener(listener2))
    listener2.verify()

    partition.appendRecordsToLeader(
      records = TestUtils.records(List(new SimpleRecord("k2".getBytes, "v2".getBytes))),
      origin = AppendOrigin.CLIENT,
      requiredAcks = 0,
      requestLocal = RequestLocal.noCaching
    )

    fetchFollower(
      partition = partition,
      replicaId = brokerId + 1,
      fetchOffset = partition.localLogOrException.logEndOffset
    )

    listener1.verify(expectedHighWatermark = partition.localLogOrException.logEndOffset)
    listener2.verify(expectedHighWatermark = partition.localLogOrException.logEndOffset)

    partition.removeListener(listener1)

    partition.appendRecordsToLeader(
      records = TestUtils.records(List(new SimpleRecord("k3".getBytes, "v3".getBytes))),
      origin = AppendOrigin.CLIENT,
      requiredAcks = 0,
      requestLocal = RequestLocal.noCaching
    )

    fetchFollower(
      partition = partition,
      replicaId = brokerId + 1,
      fetchOffset = partition.localLogOrException.logEndOffset
    )

    listener1.verify()
    listener2.verify(expectedHighWatermark = partition.localLogOrException.logEndOffset)
  }

  @Test
  def testAddListenerFailsWhenPartitionIsDeleted(): Unit = {
    val replicas = Array(brokerId, brokerId + 1)
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, topicId = topicId)

    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(0)
      .setIsr(replicas)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .setPartitionEpoch(1)
      .build()
    partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None)

    partition.delete()

    assertFalse(partition.maybeAddListener(new MockPartitionListener()))
  }

  @Test
  def testPartitionListenerWhenLogOffsetsChanged(): Unit = {
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, topicId = topicId)

    val replicas = Array(brokerId, brokerId + 1)
    val isr = Array(brokerId, brokerId + 1)
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(0)
      .setIsr(isr)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .setPartitionEpoch(1)
      .build()
    partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None)

    val listener = new MockPartitionListener()
    assertTrue(partition.maybeAddListener(listener))
    listener.verify()

    partition.appendRecordsToLeader(
      records = TestUtils.records(List(new SimpleRecord("k1".getBytes, "v1".getBytes))),
      origin = AppendOrigin.CLIENT,
      requiredAcks = 0,
      requestLocal = RequestLocal.noCaching
    )

    listener.verify()

    fetchFollower(
      partition = partition,
      replicaId = brokerId + 1,
      fetchOffset = partition.localLogOrException.logEndOffset
    )

    listener.verify(expectedHighWatermark = partition.localLogOrException.logEndOffset)

    partition.truncateFullyAndStartAt(0L, isFuture = false)

    listener.verify(expectedHighWatermark = 0L)
  }

  @Test
  def testPartitionListenerWhenPartitionFailed(): Unit = {
    val replicas = Array(brokerId, brokerId + 1)
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, topicId = topicId)

    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(0)
      .setIsr(replicas)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .setPartitionEpoch(1)
      .build()
    partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None)

    val listener = new MockPartitionListener()
    assertTrue(partition.maybeAddListener(listener))
    listener.verify()

    partition.markOffline()
    listener.verify(expectedFailed = true)
  }

  @Test
  def testPartitionListenerWhenPartitionIsDeleted(): Unit = {
    val replicas = Array(brokerId, brokerId + 1)
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, topicId = topicId)

    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(0)
      .setIsr(replicas)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .setPartitionEpoch(1)
      .build()
    partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None)

    val listener = new MockPartitionListener()
    assertTrue(partition.maybeAddListener(listener))
    listener.verify()

    partition.delete()
    listener.verify(expectedDeleted = true)
  }

  @Test
  def testPartitionListenerWhenCurrentIsReplacedWithFutureLog(): Unit = {
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir1.getAbsolutePath)
    partition.createLogIfNotExists(isNew = true, isFutureReplica = false, offsetCheckpoints, topicId = topicId)
    assertTrue(partition.log.isDefined)

    val replicas = Array(brokerId, brokerId + 1)
    val isr = replicas
    val epoch = 0
    addBrokerEpochToMockMetadataCache(metadataCache, replicas)
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(epoch)
      .setIsr(isr)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .setPartitionEpoch(1)
      .build()
    partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None)

    val listener = new MockPartitionListener()
    assertTrue(partition.maybeAddListener(listener))
    listener.verify()

    val records = TestUtils.records(List(
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes)
    ))

    partition.appendRecordsToLeader(
      records = records,
      origin = AppendOrigin.CLIENT,
      requiredAcks = 0,
      requestLocal = RequestLocal.noCaching
    )

    listener.verify()

    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir2.getAbsolutePath)
    partition.maybeCreateFutureReplica(logDir2.getAbsolutePath, offsetCheckpoints)
    assertTrue(partition.futureLog.isDefined)
    val futureLog = partition.futureLog.get

    partition.appendRecordsToFollowerOrFutureReplica(
      records = records,
      isFuture = true,
      partitionLeaderEpoch = epoch
    )

    listener.verify()

    assertTrue(partition.maybeReplaceCurrentWithFutureReplica())
    assertEquals(futureLog, partition.log.get)

    partition.appendRecordsToLeader(
      records = TestUtils.records(List(new SimpleRecord("k3".getBytes, "v3".getBytes))),
      origin = AppendOrigin.CLIENT,
      requiredAcks = 0,
      requestLocal = RequestLocal.noCaching
    )

    fetchFollower(
      partition = partition,
      replicaId = brokerId + 1,
      fetchOffset = partition.localLogOrException.logEndOffset
    )

    listener.verify(expectedHighWatermark = partition.localLogOrException.logEndOffset)
  }

  @Test
  def testMaybeStartTransactionVerification(): Unit = {
    val leaderEpoch = 5
    val replicas = Array(brokerId, brokerId + 1)
    val isr = replicas
    val producerId = 22L

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    assertTrue(partition.makeLeader(partitionRegistration, isNew = true, offsetCheckpoints, None), "Expected become leader transition to succeed")
    assertEquals(leaderEpoch, partition.getLeaderEpoch)

    val idempotentRecords = createIdempotentRecords(util.List.of(
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes),
      new SimpleRecord("k3".getBytes, "v3".getBytes)),
      baseOffset = 0L,
      producerId = producerId)
    partition.appendRecordsToLeader(idempotentRecords, origin = AppendOrigin.CLIENT, requiredAcks = 1, RequestLocal.withThreadConfinedCaching)

    def transactionRecords() = createTransactionalRecords(util.List.of(
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes),
      new SimpleRecord("k3".getBytes, "v3".getBytes)),
      baseOffset = 0L,
      baseSequence = 3,
      producerId = producerId)

    // When VerificationGuard is not there, we should not be able to append.
    assertThrows(classOf[InvalidTxnStateException], () => partition.appendRecordsToLeader(transactionRecords(), origin = AppendOrigin.CLIENT, requiredAcks = 1, RequestLocal.withThreadConfinedCaching))

    // Before appendRecordsToLeader is called, ReplicaManager will call maybeStartTransactionVerification. We should get a non-sentinel VerificationGuard.
    val verificationGuard = partition.maybeStartTransactionVerification(producerId, 3, 0, supportsEpochBump = true)
    assertNotEquals(VerificationGuard.SENTINEL, verificationGuard)

    // With the wrong VerificationGuard, append should fail.
    assertThrows(classOf[InvalidTxnStateException], () => partition.appendRecordsToLeader(transactionRecords(),
      origin = AppendOrigin.CLIENT, requiredAcks = 1, RequestLocal.withThreadConfinedCaching, new VerificationGuard()))

    // We should return the same VerificationGuard when we still need to verify. Append should proceed.
    val verificationGuard2 = partition.maybeStartTransactionVerification(producerId, 3, 0, supportsEpochBump = true)
    assertEquals(verificationGuard, verificationGuard2)
    partition.appendRecordsToLeader(transactionRecords(), origin = AppendOrigin.CLIENT, requiredAcks = 1, RequestLocal.withThreadConfinedCaching, verificationGuard)

    // We should no longer need a VerificationGuard. Future appends without VerificationGuard will also succeed.
    val verificationGuard3 = partition.maybeStartTransactionVerification(producerId, 3, 0, supportsEpochBump = true)
    assertEquals(VerificationGuard.SENTINEL, verificationGuard3)
    partition.appendRecordsToLeader(transactionRecords(), origin = AppendOrigin.CLIENT, requiredAcks = 1, RequestLocal.withThreadConfinedCaching)
  }

  private def makeLeader(
    topicId: Option[Uuid],
    leaderEpoch: Int,
    isr: Array[Int],
    replicas: Array[Int],
    partitionEpoch: Int,
    isNew: Boolean,
    partition: Partition = partition
  ): Boolean = {
    partition.createLogIfNotExists(
      isNew = isNew,
      isFutureReplica = false,
      offsetCheckpoints,
      topicId
    )
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(partitionEpoch)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    val newLeader = partition.makeLeader(partitionRegistration, isNew = isNew, offsetCheckpoints, topicId)
    assertTrue(partition.isLeader)
    assertFalse(partition.partitionState.isInflight)
    assertEquals(topicId, partition.topicId)
    assertEquals(leaderEpoch, partition.getLeaderEpoch)
    assertEquals(isr.toSet.asJava, partition.partitionState.isr)
    assertEquals(isr.toSet.asJava, partition.partitionState.maximalIsr)
    assertEquals(partitionEpoch, partition.getPartitionEpoch)
    newLeader
  }

  private def seedLogData(log: UnifiedLog, numRecords: Int, leaderEpoch: Int): Unit = {
    for (i <- 0 until numRecords) {
      val records = MemoryRecords.withRecords(0L, Compression.NONE, leaderEpoch,
        new SimpleRecord(s"k$i".getBytes, s"v$i".getBytes))
      log.appendAsLeader(records, leaderEpoch)
    }
  }

  private class SlowLog(
    log: UnifiedLog,
    topicId: Optional[Uuid],
    logStartOffset: Long,
    localLog: LocalLog,
    leaderEpochCache: LeaderEpochFileCache,
    producerStateManager: ProducerStateManager,
    appendSemaphore: Semaphore
  ) extends UnifiedLog(
    logStartOffset,
    localLog,
    new BrokerTopicStats,
    log.producerIdExpirationCheckIntervalMs,
    leaderEpochCache,
    producerStateManager,
    topicId,
    false,
    LogOffsetsListener.NO_OP_OFFSETS_LISTENER) {

    override def appendAsFollower(records: MemoryRecords, epoch: Int): LogAppendInfo = {
      appendSemaphore.acquire()
      val appendInfo = super.appendAsFollower(records, epoch)
      appendInfo
    }
  }

  private def assertReplicaState(
    partition: Partition,
    replicaId: Int,
    lastCaughtUpTimeMs: Long,
    logEndOffset: Long,
    logStartOffset: Long,
    brokerEpoch: Option[Long] = Option.empty
  ): Unit = {
    partition.getReplica(replicaId) match {
      case Some(replica) =>
        val replicaState = replica.stateSnapshot
        assertEquals(lastCaughtUpTimeMs, replicaState.lastCaughtUpTimeMs,
          "Unexpected Last Caught Up Time")
        assertEquals(logEndOffset, replicaState.logEndOffset,
          "Unexpected Log End Offset")
        assertEquals(logStartOffset, replicaState.logStartOffset,
          "Unexpected Log Start Offset")
        if (brokerEpoch.isDefined) {
          assertEquals(brokerEpoch.get, replicaState.brokerEpoch.get,
            "brokerEpochs mismatch")
        }

      case None =>
        fail(s"Replica $replicaId not found.")
    }
  }

  private def fetchConsumer(
    partition: Partition,
    fetchOffset: Long,
    leaderEpoch: Option[Int],
    clientMetadata: Option[ClientMetadata],
    maxBytes: Int = Int.MaxValue,
    lastFetchedEpoch: Option[Int] = None,
    fetchTimeMs: Long = time.milliseconds(),
    topicId: Uuid = Uuid.ZERO_UUID,
    isolation: FetchIsolation = FetchIsolation.HIGH_WATERMARK
  ): LogReadInfo = {
    val fetchParams = consumerFetchParams(
      maxBytes = maxBytes,
      clientMetadata = clientMetadata,
      isolation = isolation
    )

    val fetchPartitionData = new FetchRequest.PartitionData(
      topicId,
      fetchOffset,
      FetchRequest.INVALID_LOG_START_OFFSET,
      maxBytes,
      leaderEpoch.map(Int.box).toJava,
      lastFetchedEpoch.map(Int.box).toJava
    )

    partition.fetchRecords(
      fetchParams,
      fetchPartitionData,
      fetchTimeMs,
      maxBytes,
      minOneMessage = true,
      updateFetchState = false
    )
  }

  private def fetchFollower(
    partition: Partition,
    replicaId: Int,
    fetchOffset: Long,
    logStartOffset: Long = 0L,
    maxBytes: Int = Int.MaxValue,
    leaderEpoch: Option[Int] = None,
    lastFetchedEpoch: Option[Int] = None,
    fetchTimeMs: Long = time.milliseconds(),
    topicId: Uuid = Uuid.ZERO_UUID,
    replicaEpoch: Option[Long] = Option.empty
  ): LogReadInfo = {
    val fetchParams = followerFetchParams(
      replicaId,
      maxBytes = maxBytes,
      replicaEpoch = if (replicaEpoch.isEmpty) defaultBrokerEpoch(replicaId) else replicaEpoch.get
    )

    val fetchPartitionData = new FetchRequest.PartitionData(
      topicId,
      fetchOffset,
      logStartOffset,
      maxBytes,
      leaderEpoch.map(Int.box).toJava,
      lastFetchedEpoch.map(Int.box).toJava
    )

    partition.fetchRecords(
      fetchParams,
      fetchPartitionData,
      fetchTimeMs,
      maxBytes,
      minOneMessage = true,
      updateFetchState = true
    )
  }

  private def addBrokerEpochToMockMetadataCache(metadataCache: MetadataCache, brokers: Array[Int]): Unit = {
    brokers.foreach { broker =>
      when(metadataCache.getAliveBrokerEpoch(broker)).thenReturn(Optional.of(defaultBrokerEpoch(broker)))
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def makeLeaderInvokesgetOrCreateLog_OnOnlineLogDir(isNew: Boolean): Unit = {
    // Given
    logManager.shutdown()
    val spyConfigRepository = spy(configRepository)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, configRepository = spyConfigRepository,
      cleanerConfig = new CleanerConfig(false), time = time)
    val spyLogManager = spy(logManager)
    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      spyLogManager,
      alterPartitionManager)
    val leaderEpoch = 1
    val replicas = Array(brokerId, brokerId + 1)
    val isr = replicas
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    val topicId = Uuid.randomUuid()
    val targetDirectory = DirectoryId.random()
    when(spyLogManager.hasOfflineLogDirs()).thenReturn(true)
    when(spyLogManager.onlineLogDirId(targetDirectory)).thenReturn(true)

    // When
    val res = partition.makeLeader(partitionRegistration, isNew = isNew, offsetCheckpoints, Some(topicId), Some(targetDirectory))

    // Then
    assertTrue(res)
    verify(spyLogManager, times(1)).getOrCreateLog(topicPartition, isNew, isFuture = false, Optional.of(topicId), Some(targetDirectory))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def makeFollowerInvokesgetOrCreateLog_OnOnlineLogDir(isNew: Boolean): Unit = {
    // Given
    logManager.shutdown()
    val spyConfigRepository = spy(configRepository)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, configRepository = spyConfigRepository,
      cleanerConfig = new CleanerConfig(false), time = time)
    val spyLogManager = spy(logManager)
    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      spyLogManager,
      alterPartitionManager)
    val leaderEpoch = 1
    val replicas = Array(brokerId, brokerId + 1)
    val isr = replicas
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    val topicId = Uuid.randomUuid()
    val targetDirectory = DirectoryId.random()
    when(spyLogManager.hasOfflineLogDirs()).thenReturn(true)
    when(spyLogManager.onlineLogDirId(targetDirectory)).thenReturn(true)

    // When
    val res = partition.makeFollower(partitionRegistration, isNew = isNew, offsetCheckpoints, Some(topicId), Some(targetDirectory))

    // Then
    assertTrue(res)
    verify(spyLogManager, times(1)).getOrCreateLog(topicPartition, isNew, isFuture = false, Optional.of(topicId), Some(targetDirectory))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def makeLeaderInvokesgetOrCreateLog_WhenNoLogDirOffline(isNew: Boolean): Unit = {
    // Given
    logManager.shutdown()
    val spyConfigRepository = spy(configRepository)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, configRepository = spyConfigRepository,
      cleanerConfig = new CleanerConfig(false), time = time)
    val spyLogManager = spy(logManager)
    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      spyLogManager,
      alterPartitionManager)
    val leaderEpoch = 1
    val replicas = Array(brokerId, brokerId + 1)
    val isr = replicas
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    val topicId = Uuid.randomUuid()
    val targetDirectory = DirectoryId.random()
    when(spyLogManager.hasOfflineLogDirs()).thenReturn(false)
    when(spyLogManager.onlineLogDirId(targetDirectory)).thenReturn(false)

    // When
    val res = partition.makeLeader(partitionRegistration, isNew = isNew, offsetCheckpoints, Some(topicId), Some(targetDirectory))

    // Then
    assertTrue(res)
    verify(spyLogManager, times(1)).getOrCreateLog(topicPartition, isNew, isFuture = false, Optional.of(topicId), Some(targetDirectory))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def makeFollowerInvokesgetOrCreateLog_WhenNoLogDirOffline(isNew: Boolean): Unit = {
    // Given
    logManager.shutdown()
    val spyConfigRepository = spy(configRepository)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, configRepository = spyConfigRepository,
      cleanerConfig = new CleanerConfig(false), time = time)
    val spyLogManager = spy(logManager)
    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      spyLogManager,
      alterPartitionManager)
    val leaderEpoch = 1
    val replicas = Array(brokerId, brokerId + 1)
    val isr = replicas
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    val topicId = Uuid.randomUuid()
    val targetDirectory = DirectoryId.random()
    when(spyLogManager.hasOfflineLogDirs()).thenReturn(false)
    when(spyLogManager.onlineLogDirId(targetDirectory)).thenReturn(false)

    // When
    val res = partition.makeFollower(partitionRegistration, isNew = isNew, offsetCheckpoints, Some(topicId), Some(targetDirectory))

    // Then
    assertTrue(res)
    verify(spyLogManager, times(1)).getOrCreateLog(topicPartition, isNew, isFuture = false, Optional.of(topicId), Some(targetDirectory))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def makeLeaderInvokesgetOrCreateLog_WhenTargetDirIsUnassigned(isNew: Boolean): Unit = {
    // Given
    logManager.shutdown()
    val spyConfigRepository = spy(configRepository)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, configRepository = spyConfigRepository,
      cleanerConfig = new CleanerConfig(false), time = time)
    val spyLogManager = spy(logManager)
    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      spyLogManager,
      alterPartitionManager)
    val leaderEpoch = 1
    val replicas = Array(brokerId, brokerId + 1)
    val isr = replicas
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    val topicId = Uuid.randomUuid()
    val targetDirectory = DirectoryId.UNASSIGNED
    when(spyLogManager.hasOfflineLogDirs()).thenReturn(true)
    when(spyLogManager.onlineLogDirId(targetDirectory)).thenReturn(false)

    // When
    val res = partition.makeLeader(partitionRegistration, isNew, offsetCheckpoints, Some(topicId), Some(targetDirectory))

    // Then
    assertTrue(res)
    verify(spyLogManager, times(1)).getOrCreateLog(topicPartition, isNew, isFuture = false, Optional.of(topicId), Some(targetDirectory))
  }


  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def makeFollowerInvokesgetOrCreateLog_WhenTargetDirIsUnassigned(isNew: Boolean): Unit = {
    // Given
    logManager.shutdown()
    val spyConfigRepository = spy(configRepository)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, configRepository = spyConfigRepository,
      cleanerConfig = new CleanerConfig(false), time = time)
    val spyLogManager = spy(logManager)
    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      spyLogManager,
      alterPartitionManager)
    val leaderEpoch = 1
    val replicas = Array(brokerId, brokerId + 1)
    val isr = replicas
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()
    val topicId = Uuid.randomUuid()
    val targetDirectory = DirectoryId.UNASSIGNED
    when(spyLogManager.hasOfflineLogDirs()).thenReturn(true)
    when(spyLogManager.onlineLogDirId(targetDirectory)).thenReturn(false)

    // When
    val res = partition.makeFollower(partitionRegistration, isNew, offsetCheckpoints, Some(topicId), Some(targetDirectory))

    // Then
    assertTrue(res)
    verify(spyLogManager, times(1)).getOrCreateLog(topicPartition, isNew, isFuture = false, Optional.of(topicId), Some(targetDirectory))
  }

  @Test
  def tryCompleteDelayedRequestsCatchesExceptions(): Unit = {
    val requestKey = new TopicPartitionOperationKey(topicPartition)

    val produce = mock(classOf[DelayedOperationPurgatory[DelayedProduce]])
    when(produce.checkAndComplete(requestKey)).thenThrow(new RuntimeException("uh oh"))

    val fetch = mock(classOf[DelayedOperationPurgatory[DelayedFetch]])
    when(fetch.checkAndComplete(requestKey)).thenThrow(new RuntimeException("uh oh"))

    val deleteRecords = mock(classOf[DelayedOperationPurgatory[DelayedDeleteRecords]])
    when(deleteRecords.checkAndComplete(requestKey)).thenThrow(new RuntimeException("uh oh"))

    val shareFetch = mock(classOf[DelayedOperationPurgatory[DelayedShareFetch]])
    when(shareFetch.checkAndComplete(new DelayedShareFetchPartitionKey(topicId.get, topicPartition.partition())))
      .thenThrow(new RuntimeException("uh oh"))

    val delayedOperations = new DelayedOperations(topicId, topicPartition, produce, fetch, deleteRecords, shareFetch)
    val spyLogManager = spy(logManager)
    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      spyLogManager,
      alterPartitionManager)
    partition.tryCompleteDelayedRequests()
  }

  @Test
  def testDeleteRecordsOnLeaderWithEmptyPolicy(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    val emptyPolicyConfig = new LogConfig(util.Map.of(
      TopicConfig.CLEANUP_POLICY_CONFIG, ""
    ))

    val mockLog = mock(classOf[UnifiedLog])
    when(mockLog.config).thenReturn(emptyPolicyConfig)
    when(mockLog.logEndOffset).thenReturn(2L)
    when(mockLog.logStartOffset).thenReturn(0L)
    when(mockLog.highWatermark).thenReturn(2L)
    when(mockLog.maybeIncrementLogStartOffset(any(), any())).thenReturn(true)

    partition.setLog(mockLog, false)

    val result = partition.deleteRecordsOnLeader(1L)
    assertEquals(1L, result.requestedOffset)
  }

  @Test
  def testDeleteRecordsOnLeaderWithCompactPolicy(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    val emptyPolicyConfig = new LogConfig(util.Map.of(
      TopicConfig.CLEANUP_POLICY_CONFIG, "compact"
    ))

    val mockLog = mock(classOf[UnifiedLog])
    when(mockLog.config).thenReturn(emptyPolicyConfig)
    when(mockLog.logEndOffset).thenReturn(2L)
    when(mockLog.logStartOffset).thenReturn(0L)
    when(mockLog.highWatermark).thenReturn(2L)
    when(mockLog.maybeIncrementLogStartOffset(any(), any())).thenReturn(true)

    partition.setLog(mockLog, false)
    assertThrows(classOf[PolicyViolationException], () =>  partition.deleteRecordsOnLeader(1L))
  }
}
