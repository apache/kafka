/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import kafka.cluster.Partition
import org.apache.kafka.server.quota.QuotaFactory.UNBOUNDED_QUOTA
import kafka.server.ReplicaAlterLogDirsThread.ReassignmentState
import kafka.utils.TestUtils
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.internal.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.metadata.KRaftMetadataCache
import org.apache.kafka.server.{PartitionFetchState, ReplicaState, common}
import org.apache.kafka.server.common.{DirectoryEventHandler, KRaftVersion, OffsetAndEpoch}
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.quota.{ReplicaQuota, ReplicationQuotaManager}
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}
import org.apache.kafka.storage.internals.log.{LogManager, UnifiedLog}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyBoolean}
import org.mockito.Mockito.{doNothing, mock, never, times, verify, verifyNoInteractions, verifyNoMoreInteractions, when}
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}

import java.util.{Optional, OptionalInt, OptionalLong}
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class ReplicaAlterLogDirsThreadTest {

  private val t1p0 = new TopicPartition("topic1", 0)
  private val t1p1 = new TopicPartition("topic1", 1)
  private val topicId = Uuid.randomUuid()
  private val topicNames = collection.immutable.Map(topicId -> "topic1")
  private val tid1p0 = new TopicIdPartition(topicId, t1p0)
  private val failedPartitions = new FailedPartitions
  private val metadataCache = new KRaftMetadataCache(1, () => KRaftVersion.LATEST_PRODUCTION)

  private def initialFetchState(fetchOffset: Long, leaderEpoch: Int = 1): InitialFetchState = {
    InitialFetchState(topicId = Some(topicId), leader = new BrokerEndPoint(0, "localhost", 9092),
      initOffset = fetchOffset, currentLeaderEpoch = leaderEpoch)
  }

  @Test
  def shouldNotAddPartitionIfFutureLogIsNotDefined(): Unit = {
    val brokerId = 1
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId))

    val replicaManager = Mockito.mock(classOf[ReplicaManager])
    val quotaManager = Mockito.mock(classOf[ReplicationQuotaManager])

    when(replicaManager.futureLogExists(t1p0)).thenReturn(false)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      new BrokerTopicStats,
      config.replicaFetchBackoffMs)

    val addedPartitions = thread.addPartitions(Map(t1p0 -> initialFetchState(0L)))
    assertEquals(Set.empty, addedPartitions)
    assertEquals(0, thread.partitionCount)
    assertEquals(None, thread.fetchState(t1p0))
  }

  @Test
  def shouldUpdateLeaderEpochAfterFencedEpochError(): Unit = {
    val brokerId = 1
    val partitionId = 0
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId))

    val partition = Mockito.mock(classOf[Partition])
    val replicaManager = Mockito.mock(classOf[ReplicaManager])
    val quotaManager = Mockito.mock(classOf[ReplicationQuotaManager])
    val futureLog = Mockito.mock(classOf[UnifiedLog])

    val leaderEpoch = 5
    val logEndOffset = 0

    when(partition.partitionId).thenReturn(partitionId)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(replicaManager.onlinePartition(t1p0)).thenReturn(Some(partition))
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)

    when(quotaManager.isQuotaExceeded).thenReturn(false)

    when(partition.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(logEndOffset))
    when(partition.futureLocalLogOrException).thenReturn(futureLog)
    doNothing().when(partition).truncateTo(offset = 0, isFuture = true)
    when(partition.maybeReplaceCurrentWithFutureReplica()).thenReturn(true)
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("gOZOXHnkR9eiA1W9ZuLk8A")))

    when(futureLog.logStartOffset).thenReturn(0L)
    when(futureLog.logEndOffset).thenReturn(0L)
    when(futureLog.latestEpoch).thenReturn(Optional.empty)

    val fencedRequestData = new FetchRequest.PartitionData(topicId, 0L, 0L,
      config.replicaFetchMaxBytes, Optional.of(leaderEpoch - 1))
    val fencedResponseData = new FetchPartitionData(
      Errors.FENCED_LEADER_EPOCH,
      -1,
      -1,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false)
    mockFetchFromCurrentLog(tid1p0, fencedRequestData, config, replicaManager, fencedResponseData)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-log-dirs-thread",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      new BrokerTopicStats,
      config.replicaFetchBackoffMs)

    // Initially we add the partition with an older epoch which results in an error
    thread.addPartitions(Map(t1p0 -> initialFetchState(fetchOffset = 0L, leaderEpoch - 1)))
    assertTrue(thread.fetchState(t1p0).isDefined)
    assertEquals(1, thread.partitionCount)

    thread.doWork()

    assertTrue(failedPartitions.contains(t1p0))
    assertEquals(None, thread.fetchState(t1p0))
    assertEquals(0, thread.partitionCount)

    // Next we update the epoch and assert that we can continue
    thread.addPartitions(Map(t1p0 -> initialFetchState(fetchOffset = 0L, leaderEpoch)))
    assertEquals(Some(leaderEpoch), thread.fetchState(t1p0).map(_.currentLeaderEpoch))
    assertEquals(1, thread.partitionCount)

    val requestData = new FetchRequest.PartitionData(topicId, 0L, 0L,
      config.replicaFetchMaxBytes, Optional.of(leaderEpoch))
    val responseData = new FetchPartitionData(
      Errors.NONE,
      0L,
      0L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false)
    mockFetchFromCurrentLog(tid1p0, requestData, config, replicaManager, responseData)

    thread.doWork()

    assertFalse(failedPartitions.contains(t1p0))
    assertEquals(None, thread.fetchState(t1p0))
    assertEquals(0, thread.partitionCount)
  }

  @Test
  def shouldReplaceCurrentLogDirWhenCaughtUp(): Unit = {
    val brokerId = 1
    val partitionId = 0
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId))

    val partition = Mockito.mock(classOf[Partition])
    val replicaManager = Mockito.mock(classOf[ReplicaManager])
    val quotaManager = Mockito.mock(classOf[ReplicationQuotaManager])
    val futureLog = Mockito.mock(classOf[UnifiedLog])

    val leaderEpoch = 5
    val logEndOffset = 0

    when(partition.partitionId).thenReturn(partitionId)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(replicaManager.onlinePartition(t1p0)).thenReturn(Some(partition))
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)

    when(quotaManager.isQuotaExceeded).thenReturn(false)

    when(partition.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(logEndOffset))
    when(partition.futureLocalLogOrException).thenReturn(futureLog)
    doNothing().when(partition).truncateTo(offset = 0, isFuture = true)
    when(partition.maybeReplaceCurrentWithFutureReplica()).thenReturn(true)
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("PGLOjDjKQaCOXFOtxymIig")))

    when(futureLog.logStartOffset).thenReturn(0L)
    when(futureLog.logEndOffset).thenReturn(0L)
    when(futureLog.latestEpoch).thenReturn(Optional.empty)

    val requestData = new FetchRequest.PartitionData(topicId, 0L, 0L,
      config.replicaFetchMaxBytes, Optional.of(leaderEpoch))
    val responseData = new FetchPartitionData(
      Errors.NONE,
      0L,
      0L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false)
    mockFetchFromCurrentLog(tid1p0, requestData, config, replicaManager, responseData)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      new BrokerTopicStats,
      config.replicaFetchBackoffMs)

    thread.addPartitions(Map(t1p0 -> initialFetchState(fetchOffset = 0L, leaderEpoch)))
    assertTrue(thread.fetchState(t1p0).isDefined)
    assertEquals(1, thread.partitionCount)

    thread.doWork()

    assertEquals(None, thread.fetchState(t1p0))
    assertEquals(0, thread.partitionCount)
  }

  private def updateReassignmentState(thread: ReplicaAlterLogDirsThread, partitionId:Int, newState: ReassignmentState) = {
    topicNames.get(topicId).map(topicName => {
      thread.updateReassignmentState(new TopicPartition(topicName, partitionId), newState)
    })
  }

  @Test
  def shouldReplaceCurrentLogDirWhenCaughtUpWithAfterAssignmentRequestHasBeenCompleted(): Unit = {
    val brokerId = 1
    val partitionId = 0
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId))

    val partition = Mockito.mock(classOf[Partition])
    val replicaManager = Mockito.mock(classOf[ReplicaManager])
    val quotaManager = Mockito.mock(classOf[ReplicationQuotaManager])

    val directoryEventHandler = mock(classOf[DirectoryEventHandler])

    val futureLog = Mockito.mock(classOf[UnifiedLog])

    val leaderEpoch = 5
    val logEndOffset = 0
    val currentDirectoryId = Uuid.fromString("EzI9SqkFQKW1iFc1ZwP9SQ")

    when(partition.partitionId).thenReturn(partitionId)
    when(partition.topicId).thenReturn(Some(topicId))
    when(partition.futureReplicaDirectoryId()).thenReturn(Some(Uuid.randomUuid()))
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(replicaManager.onlinePartition(t1p0)).thenReturn(Some(partition))
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)

    when(quotaManager.isQuotaExceeded).thenReturn(false)

    when(partition.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(logEndOffset))
    when(partition.futureLocalLogOrException).thenReturn(futureLog)
    doNothing().when(partition).truncateTo(offset = 0, isFuture = true)
    when(partition.maybeReplaceCurrentWithFutureReplica()).thenReturn(true)
    when(partition.runCallbackIfFutureReplicaCaughtUp(any())).thenReturn(true)
    when(partition.logDirectoryId()).thenReturn(Some(currentDirectoryId))

    when(futureLog.logStartOffset).thenReturn(0L)
    when(futureLog.logEndOffset).thenReturn(0L)
    when(futureLog.latestEpoch).thenReturn(Optional.empty)

    val requestData = new FetchRequest.PartitionData(topicId, 0L, 0L,
      config.replicaFetchMaxBytes, Optional.of(leaderEpoch))
    val responseData = new FetchPartitionData(
      Errors.NONE,
      0L,
      0L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false)
    mockFetchFromCurrentLog(tid1p0, requestData, config, replicaManager, responseData)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      new BrokerTopicStats,
      config.replicaFetchBackoffMs,
      directoryEventHandler)

    thread.addPartitions(Map(t1p0 -> initialFetchState(fetchOffset = 0L, leaderEpoch)))

    assertTrue(thread.fetchState(t1p0).isDefined)
    assertEquals(1, thread.partitionCount)

    // Don't promote future replica if no assignment state for this partition
    thread.doWork()
    assertTrue(thread.fetchState(t1p0).isDefined)
    assertEquals(1, thread.partitionCount)

    updateReassignmentState(thread, partitionId, ReassignmentState.Queued)

    // Don't promote future replica if assignment request is queued but not completed
    thread.doWork()
    assertTrue(thread.fetchState(t1p0).isDefined)
    assertEquals(1, thread.partitionCount)
    updateReassignmentState(thread, partitionId, ReassignmentState.Accepted)

    // Promote future replica if assignment request is completed
    thread.doWork()
    assertEquals(None, thread.fetchState(t1p0))
    assertEquals(0, thread.partitionCount)
    verifyNoInteractions(directoryEventHandler)

  }

  @Test
  def shouldRevertAnyScheduledAssignmentRequestIfAssignmentIsCancelled(): Unit = {
    val brokerId = 1
    val partitionId = 0
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId))

    val partition = Mockito.mock(classOf[Partition])
    val replicaManager = Mockito.mock(classOf[ReplicaManager])
    val quotaManager = Mockito.mock(classOf[ReplicationQuotaManager])
    val directoryEventHandler = mock(classOf[DirectoryEventHandler])

    val futureLog = Mockito.mock(classOf[UnifiedLog])

    val leaderEpoch = 5
    val logEndOffset = 0

    when(partition.partitionId).thenReturn(partitionId)
    when(partition.topicId).thenReturn(Some(topicId))
    when(partition.futureReplicaDirectoryId()).thenReturn(Some(Uuid.randomUuid()))
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.randomUuid()))
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(replicaManager.onlinePartition(t1p0)).thenReturn(Some(partition))
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)

    when(quotaManager.isQuotaExceeded).thenReturn(false)

    when(partition.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(logEndOffset))
    when(partition.futureLocalLogOrException).thenReturn(futureLog)
    doNothing().when(partition).truncateTo(offset = 0, isFuture = true)
    when(partition.maybeReplaceCurrentWithFutureReplica()).thenReturn(true)
    when(partition.runCallbackIfFutureReplicaCaughtUp(any())).thenReturn(true)

    when(futureLog.logStartOffset).thenReturn(0L)
    when(futureLog.logEndOffset).thenReturn(0L)
    when(futureLog.latestEpoch).thenReturn(Optional.empty)

    val requestData = new FetchRequest.PartitionData(topicId, 0L, 0L,
      config.replicaFetchMaxBytes, Optional.of(leaderEpoch))
    val responseData = new FetchPartitionData(
      Errors.NONE,
      0L,
      0L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false)
    mockFetchFromCurrentLog(tid1p0, requestData, config, replicaManager, responseData)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      new BrokerTopicStats,
      config.replicaFetchBackoffMs,
      directoryEventHandler)

    thread.addPartitions(Map(t1p0 -> initialFetchState(fetchOffset = 0L, leaderEpoch)))

    assertTrue(thread.fetchState(t1p0).isDefined)
    assertEquals(1, thread.partitionCount)

    // Don't promote future replica if no assignment state for this partition
    thread.doWork()
    assertTrue(thread.fetchState(t1p0).isDefined)
    assertEquals(1, thread.partitionCount)

    updateReassignmentState(thread, partitionId, ReassignmentState.Queued)

    // revert assignment and delete request state if assignment is cancelled
    thread.removePartitions(Set(t1p0))
    assertTrue(thread.fetchState(t1p0).isEmpty)
    assertEquals(0, thread.partitionCount)
    val topicIdPartitionCaptureT1p0: ArgumentCaptor[org.apache.kafka.server.common.TopicIdPartition] =
      ArgumentCaptor.forClass(classOf[org.apache.kafka.server.common.TopicIdPartition])
    val logIdCaptureT1p0: ArgumentCaptor[Uuid] = ArgumentCaptor.forClass(classOf[Uuid])

    verify(directoryEventHandler).handleAssignment(topicIdPartitionCaptureT1p0.capture(), logIdCaptureT1p0.capture(),
      ArgumentMatchers.eq("Reverting reassignment for canceled future replica"), any())

    assertEquals(new org.apache.kafka.server.common.TopicIdPartition(topicId, t1p0.partition()), topicIdPartitionCaptureT1p0.getValue)
    assertEquals(partition.logDirectoryId().get, logIdCaptureT1p0.getValue)
  }

  @Test
  def shouldRevertReassignmentsForIncompleteFutureReplicaPromotions(): Unit = {
    val replicaManager = Mockito.mock(classOf[ReplicaManager])
    val directoryEventHandler = mock(classOf[DirectoryEventHandler])
    val quotaManager = Mockito.mock(classOf[ReplicationQuotaManager])
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      Mockito.mock(classOf[BrokerTopicStats]),
      0,
      directoryEventHandler)

    val tp = Seq.range(0, 4).map(new TopicPartition("t", _))
    val tips = Seq.range(0, 4).map(new common.TopicIdPartition(topicId, _))
    val dirIds = Seq.range(0, 4).map(i => Uuid.fromString(s"TESTBROKER0000DIR${i}AAAA"))
    tp.foreach(tp => thread.promotionStates.put(tp, ReplicaAlterLogDirsThread.PromotionState(ReassignmentState.None, Some(topicId), Some(dirIds(tp.partition())))))
    thread.updateReassignmentState(tp(0), ReassignmentState.None)
    thread.updateReassignmentState(tp(1), ReassignmentState.Queued)
    thread.updateReassignmentState(tp(2), ReassignmentState.Accepted)
    thread.updateReassignmentState(tp(3), ReassignmentState.Effective)

    thread.removePartitions(tp.toSet)

    verify(directoryEventHandler).handleAssignment(ArgumentMatchers.eq(tips(1)), ArgumentMatchers.eq(dirIds(1)),
      ArgumentMatchers.eq("Reverting reassignment for canceled future replica"), any())
    verify(directoryEventHandler).handleAssignment(ArgumentMatchers.eq(tips(2)), ArgumentMatchers.eq(dirIds(2)),
      ArgumentMatchers.eq("Reverting reassignment for canceled future replica"), any())
    verifyNoMoreInteractions(directoryEventHandler)
  }

  private def mockFetchFromCurrentLog(topicIdPartition: TopicIdPartition,
                                      requestData: FetchRequest.PartitionData,
                                      config: KafkaConfig,
                                      replicaManager: ReplicaManager,
                                      responseData: FetchPartitionData): Unit = {
    val callbackCaptor: ArgumentCaptor[Seq[(TopicIdPartition, FetchPartitionData)] => Unit] =
      ArgumentCaptor.forClass(classOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit])

    val expectedFetchParams = new FetchParams(
      FetchRequest.FUTURE_LOCAL_REPLICA_ID,
      -1,
      0L,
      0,
      config.replicaFetchResponseMaxBytes,
      FetchIsolation.LOG_END,
      Optional.empty()
    )

    when(replicaManager.fetchMessages(
      params = ArgumentMatchers.eq(expectedFetchParams),
      fetchInfos = ArgumentMatchers.eq(Seq(topicIdPartition -> requestData)),
      quota = ArgumentMatchers.eq(UNBOUNDED_QUOTA),
      responseCallback = callbackCaptor.capture(),
    )).thenAnswer(_ => {
      callbackCaptor.getValue.apply(Seq((topicIdPartition, responseData)))
    })
  }

  @Test
  def issuesEpochRequestFromLocalReplica(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))

    //Setup all dependencies

    val partitionT1p0: Partition = mock(classOf[Partition])
    val partitionT1p1: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val partitionT1p0Id = 0
    val partitionT1p1Id = 1
    val leaderEpochT1p0 = 2
    val leaderEpochT1p1 = 5
    val leoT1p0 = 13
    val leoT1p1 = 232

    //Stubs
    when(partitionT1p0.partitionId).thenReturn(partitionT1p0Id)
    when(partitionT1p0.partitionId).thenReturn(partitionT1p1Id)

    when(replicaManager.getPartitionOrException(t1p0))
      .thenReturn(partitionT1p0)
    when(partitionT1p0.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpochT1p0, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionT1p0Id)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpochT1p0)
        .setEndOffset(leoT1p0))

    when(replicaManager.getPartitionOrException(t1p1))
      .thenReturn(partitionT1p1)
    when(partitionT1p1.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpochT1p1, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionT1p1Id)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpochT1p1)
        .setEndOffset(leoT1p1))

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, null)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      null,
      null,
      config.replicaFetchBackoffMs)

    val result = thread.leader.fetchEpochEndOffsets(java.util.Map.of(
      t1p0, new OffsetForLeaderPartition()
        .setPartition(t1p0.partition)
        .setLeaderEpoch(leaderEpochT1p0),
      t1p1, new OffsetForLeaderPartition()
        .setPartition(t1p1.partition)
        .setLeaderEpoch(leaderEpochT1p1))).asScala

    val expected = Map(
      t1p0 -> new EpochEndOffset()
        .setPartition(t1p0.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpochT1p0)
        .setEndOffset(leoT1p0),
      t1p1 -> new EpochEndOffset()
        .setPartition(t1p1.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpochT1p1)
        .setEndOffset(leoT1p1)
    )

    assertEquals(expected, result, "results from leader epoch request should have offset from local replica")
  }

  @Test
  def fetchEpochsFromLeaderShouldHandleExceptionFromGetLocalReplica(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))

    //Setup all dependencies
    val partitionT1p0: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val partitionId = 0
    val leaderEpoch = 2
    val leo = 13

    //Stubs
    when(partitionT1p0.partitionId).thenReturn(partitionId)

    when(replicaManager.getPartitionOrException(t1p0))
      .thenReturn(partitionT1p0)
    when(partitionT1p0.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(leo))

    when(replicaManager.getPartitionOrException(t1p1))
      .thenThrow(new KafkaStorageException)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, null)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      null,
      null,
      config.replicaFetchBackoffMs)

    val result = thread.leader.fetchEpochEndOffsets(java.util.Map.of(
      t1p0, new OffsetForLeaderPartition()
        .setPartition(t1p0.partition)
        .setLeaderEpoch(leaderEpoch),
      t1p1, new OffsetForLeaderPartition()
        .setPartition(t1p1.partition)
        .setLeaderEpoch(leaderEpoch))).asScala

    val expected = Map(
      t1p0 -> new EpochEndOffset()
        .setPartition(t1p0.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(leo),
      t1p1 -> new EpochEndOffset()
        .setPartition(t1p1.partition)
        .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code)
    )

    assertEquals(expected, result)
  }

  @Test
  def shouldTruncateToDivergingEpochOffsetFromFetchResponse(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncateCapture: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val quotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val futureLog: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val leaderEpoch = 2
    val futureReplicaLEO = 191
    val replicaLEO = 190

    //Stubs
    when(partition.partitionId).thenReturn(0)
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("Jsg8ufNCQYONNquPt7VYpA")))

    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(replicaManager.onlinePartition(t1p0)).thenReturn(Some(partition))
    when(replicaManager.logManager).thenReturn(logManager)

    when(futureLog.logStartOffset).thenReturn(0L)
    when(futureLog.logEndOffset).thenReturn(futureReplicaLEO)
    when(futureLog.latestEpoch).thenReturn(Optional.of(leaderEpoch))
    when(futureLog.endOffsetForEpoch(leaderEpoch)).thenReturn(
      Optional.of(new OffsetAndEpoch(futureReplicaLEO, leaderEpoch)))

    // The fetch request must carry the future log's latest epoch as the last fetched epoch,
    // and the current replica replies with a diverging epoch instead of the partition data.
    val requestData = new FetchRequest.PartitionData(topicId, 0L, 0L,
      config.replicaFetchMaxBytes, Optional.of(1), Optional.of(leaderEpoch))
    val responseData = new FetchPartitionData(
      Errors.NONE,
      0L,
      0L,
      MemoryRecords.EMPTY,
      Optional.of(new FetchResponseData.EpochEndOffset().setEpoch(leaderEpoch).setEndOffset(replicaLEO)),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false)
    mockFetchFromCurrentLog(tid1p0, requestData, config, replicaManager, responseData)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      null,
      config.replicaFetchBackoffMs)
    thread.addPartitions(Map(t1p0 -> initialFetchState(0L)))

    //Run it
    thread.doWork()

    //We should have truncated to the diverging epoch end offset from the fetch response
    verify(partition).truncateTo(truncateCapture.capture(), ArgumentMatchers.eq(true))
    verify(partition, never()).lastOffsetForLeaderEpoch(any(), ArgumentMatchers.anyInt(), anyBoolean())
    assertEquals(replicaLEO, truncateCapture.getValue)
  }

  @Test
  def shouldTruncateToEndOffsetOfLargestCommonEpoch(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncateToCapture: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val quotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val futureLog: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val partitionId = 0
    val leaderEpoch = 5
    val futureReplicaLEO = 195
    val replicaLEO = 200
    val replicaEpochEndOffset = 190
    val futureReplicaEpochEndOffset = 191

    //Stubs
    when(partition.partitionId).thenReturn(partitionId)
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("n6WOe2zPScqZLIreCWN6Ug")))

    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(replicaManager.onlinePartition(t1p0)).thenReturn(Some(partition))
    when(replicaManager.logManager).thenReturn(logManager)

    when(futureLog.logStartOffset).thenReturn(0L)
    when(futureLog.logEndOffset).thenReturn(futureReplicaLEO)
    when(futureLog.latestEpoch)
      .thenReturn(Optional.of(leaderEpoch))
      .thenReturn(Optional.of(leaderEpoch - 2))

    // the current replica truncated to an epoch unknown to the future replica, so the future
    // replica's end offset for that epoch corresponds to a smaller leader epoch
    when(futureLog.endOffsetForEpoch(leaderEpoch - 1)).thenReturn(
      Optional.of(new OffsetAndEpoch(futureReplicaLEO, leaderEpoch - 2)))
    when(futureLog.endOffsetForEpoch(leaderEpoch - 2)).thenReturn(
      Optional.of(new OffsetAndEpoch(futureReplicaEpochEndOffset, leaderEpoch - 2)))

    // First fetch carries the future log's latest epoch; the current replica replies with a
    // diverging epoch unknown to the future replica
    val firstRequestData = new FetchRequest.PartitionData(topicId, 0L, 0L,
      config.replicaFetchMaxBytes, Optional.of(1), Optional.of(leaderEpoch))
    val firstResponseData = new FetchPartitionData(
      Errors.NONE,
      0L,
      0L,
      MemoryRecords.EMPTY,
      Optional.of(new FetchResponseData.EpochEndOffset().setEpoch(leaderEpoch - 1).setEndOffset(replicaLEO)),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false)
    mockFetchFromCurrentLog(tid1p0, firstRequestData, config, replicaManager, firstResponseData)

    // After the intermediate truncation, the second fetch carries the older epoch and the current
    // replica replies with the end offset of the largest common epoch
    val secondRequestData = new FetchRequest.PartitionData(topicId, futureReplicaLEO, 0L,
      config.replicaFetchMaxBytes, Optional.of(1), Optional.of(leaderEpoch - 2))
    val secondResponseData = new FetchPartitionData(
      Errors.NONE,
      0L,
      0L,
      MemoryRecords.EMPTY,
      Optional.of(new FetchResponseData.EpochEndOffset().setEpoch(leaderEpoch - 2).setEndOffset(replicaEpochEndOffset)),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false)
    mockFetchFromCurrentLog(tid1p0, secondRequestData, config, replicaManager, secondResponseData)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      null,
      config.replicaFetchBackoffMs)
    thread.addPartitions(Map(t1p0 -> initialFetchState(0L)))

    // First run results in an intermediate truncation and a fetch with the older epoch
    thread.doWork()
    // Second run truncates to the end offset of the largest common epoch
    thread.doWork()

    //We should have truncated to the offsets in the diverging epoch responses
    verify(partition, times(2)).truncateTo(truncateToCapture.capture(), ArgumentMatchers.eq(true))
    assertTrue(truncateToCapture.getAllValues.asScala.contains(replicaEpochEndOffset),
               "Expected offset " + replicaEpochEndOffset + " in captured truncation offsets " + truncateToCapture.getAllValues)
    verify(partition, never()).lastOffsetForLeaderEpoch(any(), ArgumentMatchers.anyInt(), anyBoolean())
  }

  @Test
  def shouldTruncateToInitialFetchOffsetIfFutureLogHasNoEpochs(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncated: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val quotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val futureLog: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val responseCallback: ArgumentCaptor[Seq[(TopicIdPartition, FetchPartitionData)] => Unit] = ArgumentCaptor.forClass(classOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit])

    val initialFetchOffset = 100

    //Stubs
    when(replicaManager.getPartitionOrException(t1p0))
      .thenReturn(partition)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)

    when(replicaManager.logManager).thenReturn(logManager)
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("b2e1ihvGQiu6A504oKoddQ")))

    // pretend this is a completely new future replica, with no leader epochs recorded
    when(futureLog.latestEpoch).thenReturn(Optional.empty)

    stubWithFetchMessages(log, null, futureLog, partition, replicaManager, responseCallback)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      null,
      config.replicaFetchBackoffMs)
    thread.addPartitions(Map(t1p0 -> initialFetchState(initialFetchOffset)))

    //Run it
    thread.doWork()

    //We should have truncated to initial fetch offset
    verify(partition).truncateTo(truncated.capture(), isFuture = ArgumentMatchers.eq(true))
    assertEquals(initialFetchOffset,
                 truncated.getValue, "Expected future replica to truncate to initial fetch offset if the future log has no epochs")
  }

  @Test
  def shouldNotIssueLeaderEpochRequests(): Unit = {

    //Setup all dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val quotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val futureLog: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val responseCallback: ArgumentCaptor[Seq[(TopicIdPartition, FetchPartitionData)] => Unit] = ArgumentCaptor.forClass(classOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit])

    val leaderEpoch = 5
    val futureReplicaLEO = 190

    when(partition.partitionId).thenReturn(0)
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("dybMM9CpRP2s6HSslW4NHg")))

    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.getPartitionOrException(t1p0))
        .thenReturn(partition)

    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(futureLog.latestEpoch).thenReturn(Optional.of(leaderEpoch))
    when(futureLog.logEndOffset).thenReturn(futureReplicaLEO)
    when(replicaManager.logManager).thenReturn(logManager)
    stubWithFetchMessages(log, null, futureLog, partition, replicaManager, responseCallback)

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      null,
      config.replicaFetchBackoffMs)
    thread.addPartitions(Map(t1p0 -> initialFetchState(0L)))

    // loop few times
    (0 to 3).foreach { _ =>
      thread.doWork()
    }

    verify(partition, never()).lastOffsetForLeaderEpoch(any(), ArgumentMatchers.anyInt(), anyBoolean())
    verify(partition, never()).truncateTo(ArgumentMatchers.anyLong(), anyBoolean())
  }

  @Test
  def shouldFetchOneReplicaAtATime(): Unit = {

    //Setup all dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val quotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val futureLog: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    //Stubs
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)
    when(replicaManager.getPartitionOrException(t1p1)).thenReturn(partition)
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("Y0qUL19gSmKAXmohmrUM4g")))
    stub(log, null, futureLog, partition, replicaManager)

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leaderEpoch = 1
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      null,
      config.replicaFetchBackoffMs)
    thread.addPartitions(Map(
      t1p0 -> initialFetchState(0L, leaderEpoch),
      t1p1 -> initialFetchState(0L, leaderEpoch)))

    val result = thread.leader.buildFetch(java.util.Map.of(
      t1p0, new PartitionFetchState(Optional.of(topicId), 150, Optional.empty, leaderEpoch, Optional.empty, ReplicaState.FETCHING, Optional.empty),
      t1p1, new PartitionFetchState(Optional.of(topicId), 160, Optional.empty, leaderEpoch, Optional.empty, ReplicaState.FETCHING, Optional.empty)))
    val fetchRequestOpt = result.result
    val partitionsWithError = result.partitionsWithError
    assertTrue(fetchRequestOpt.isPresent)
    val fetchRequest = fetchRequestOpt.get.fetchRequest
    assertFalse(fetchRequest.fetchData.isEmpty)
    assertTrue(partitionsWithError.isEmpty)
    val request = fetchRequest.build()
    assertEquals(0, request.minBytes)
    val fetchInfos = request.fetchData(topicNames.asJava).asScala.toSeq
    assertEquals(1, fetchInfos.length)
    assertEquals(t1p0, fetchInfos.head._1.topicPartition, "Expected fetch request for first partition")
    assertEquals(150, fetchInfos.head._2.fetchOffset)
  }

  @Test
  def shouldFetchNonDelayedAndNonTruncatingReplicas(): Unit = {

    //Setup all dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val quotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val futureLog: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    //Stubs
    val startOffset = 123
    when(futureLog.logStartOffset).thenReturn(startOffset)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)
    when(replicaManager.getPartitionOrException(t1p1)).thenReturn(partition)
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("rtrdy3nsQwO1OQUEUYGxRQ")))
    stub(log, null, futureLog, partition, replicaManager)

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leaderEpoch = 1
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      null,
      config.replicaFetchBackoffMs)
    thread.addPartitions(Map(
      t1p0 -> initialFetchState(0L, leaderEpoch),
      t1p1 -> initialFetchState(0L, leaderEpoch)))

    // one partition is ready and one is truncating
    val result1 = thread.leader.buildFetch(java.util.Map.of(
      t1p0, new PartitionFetchState(Optional.of(topicId), 150, Optional.empty(), leaderEpoch, Optional.empty(),
        ReplicaState.FETCHING, Optional.empty()),
      t1p1, new PartitionFetchState(Optional.of(topicId), 160, Optional.empty(), leaderEpoch, Optional.empty(),
        ReplicaState.TRUNCATING, Optional.empty())
    ))
    val fetchRequestOpt1 = result1.result
    val partitionsWithError1 = result1.partitionsWithError

    assertTrue(fetchRequestOpt1.isPresent)
    val fetchRequest = fetchRequestOpt1.get
    assertFalse(fetchRequest.fetchRequest.fetchData.isEmpty)
    assertTrue(partitionsWithError1.isEmpty)
    val fetchInfos = fetchRequest.fetchRequest.build().fetchData(topicNames.asJava).asScala.toSeq
    assertEquals(1, fetchInfos.length)
    assertEquals(t1p0, fetchInfos.head._1.topicPartition, "Expected fetch request for non-truncating partition")
    assertEquals(150, fetchInfos.head._2.fetchOffset)

    // one partition is ready and one is delayed
    val result2 = thread.leader.buildFetch(java.util.Map.of(
      t1p0, new PartitionFetchState(Optional.of(topicId), 140, Optional.empty(), leaderEpoch, Optional.empty(),
        ReplicaState.FETCHING, Optional.empty()),
      t1p1, new PartitionFetchState(Optional.of(topicId), 160, Optional.empty(), leaderEpoch, Optional.of(5000L),
        ReplicaState.FETCHING, Optional.empty())
    ))
    val fetchRequest2Opt = result2.result
    val partitionsWithError2 = result2.partitionsWithError

    assertTrue(fetchRequest2Opt.isPresent)
    val fetchRequest2 = fetchRequest2Opt.get
    assertFalse(fetchRequest2.fetchRequest.fetchData().isEmpty)
    assertTrue(partitionsWithError2.isEmpty())
    val fetchInfos2 = fetchRequest2.fetchRequest.build().fetchData(topicNames.asJava).asScala.toSeq
    assertEquals(1, fetchInfos2.length)
    assertEquals(t1p0, fetchInfos2.head._1.topicPartition, "Expected fetch request for non-delayed partition")
    assertEquals(140, fetchInfos2.head._2.fetchOffset)

    // both partitions are delayed
    val result3 = thread.leader.buildFetch(java.util.Map.of(
      t1p0, new PartitionFetchState(Optional.of(topicId), 140, Optional.empty(), leaderEpoch, Optional.of(5000L),
        ReplicaState.FETCHING, Optional.empty()),
      t1p1, new PartitionFetchState(Optional.of(topicId), 160, Optional.empty(), leaderEpoch, Optional.of(5000L),
        ReplicaState.FETCHING, Optional.empty())
    ))
    val fetchRequest3Opt = result3.result
    val partitionsWithError3 = result3.partitionsWithError
    assertTrue(fetchRequest3Opt.isEmpty, "Expected no fetch requests since all partitions are delayed")
    assertTrue(partitionsWithError3.isEmpty())
  }

  def stub(logT1p0: UnifiedLog, logT1p1: UnifiedLog, futureLog: UnifiedLog, partition: Partition,
           replicaManager: ReplicaManager): Unit = {
    when(replicaManager.localLog(t1p0)).thenReturn(Some(logT1p0))
    when(replicaManager.localLogOrException(t1p0)).thenReturn(logT1p0)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(replicaManager.onlinePartition(t1p0)).thenReturn(Some(partition))
    when(replicaManager.localLog(t1p1)).thenReturn(Some(logT1p1))
    when(replicaManager.localLogOrException(t1p1)).thenReturn(logT1p1)
    when(replicaManager.futureLocalLogOrException(t1p1)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p1)).thenReturn(true)
    when(replicaManager.onlinePartition(t1p1)).thenReturn(Some(partition))
  }

  def stubWithFetchMessages(logT1p0: UnifiedLog, logT1p1: UnifiedLog, futureLog: UnifiedLog, partition: Partition, replicaManager: ReplicaManager,
                            responseCallback: ArgumentCaptor[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]): Unit = {
    stub(logT1p0, logT1p1, futureLog, partition, replicaManager)
    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      responseCallback.capture()
    )).thenAnswer(_ => responseCallback.getValue.apply(Seq.empty[(TopicIdPartition, FetchPartitionData)]))
  }
}
