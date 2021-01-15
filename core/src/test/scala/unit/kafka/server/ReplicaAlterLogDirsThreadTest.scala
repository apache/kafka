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

import java.util.Optional

import kafka.api.Request
import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.log.{Log, LogManager}
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.utils.{DelayedItem, TestUtils}
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.{IsolationLevel, TopicPartition}
import org.easymock.EasyMock._
import org.easymock.{Capture, CaptureType, EasyMock, IExpectationSetters}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{doNothing, when}
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

class ReplicaAlterLogDirsThreadTest {

  private val t1p0 = new TopicPartition("topic1", 0)
  private val t1p1 = new TopicPartition("topic1", 1)
  private val failedPartitions = new FailedPartitions

  private def initialFetchState(fetchOffset: Long, leaderEpoch: Int = 1): InitialFetchState = {
    InitialFetchState(leader = new BrokerEndPoint(0, "localhost", 9092),
      initOffset = fetchOffset, currentLeaderEpoch = leaderEpoch)
  }

  @Test
  def shouldNotAddPartitionIfFutureLogIsNotDefined(): Unit = {
    val brokerId = 1
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId, "localhost:1234"))

    val replicaManager = Mockito.mock(classOf[ReplicaManager])
    val quotaManager = Mockito.mock(classOf[ReplicationQuotaManager])

    when(replicaManager.futureLogExists(t1p0)).thenReturn(false)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread",
      sourceBroker = endPoint,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = new BrokerTopicStats)

    val addedPartitions = thread.addPartitions(Map(t1p0 -> initialFetchState(0L)))
    assertEquals(Set.empty, addedPartitions)
    assertEquals(0, thread.partitionCount)
    assertEquals(None, thread.fetchState(t1p0))
  }

  @Test
  def shouldUpdateLeaderEpochAfterFencedEpochError(): Unit = {
    val brokerId = 1
    val partitionId = 0
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId, "localhost:1234"))

    val partition = Mockito.mock(classOf[Partition])
    val replicaManager = Mockito.mock(classOf[ReplicaManager])
    val quotaManager = Mockito.mock(classOf[ReplicationQuotaManager])
    val futureLog = Mockito.mock(classOf[Log])

    val leaderEpoch = 5
    val logEndOffset = 0

    when(partition.partitionId).thenReturn(partitionId)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(replicaManager.nonOfflinePartition(t1p0)).thenReturn(Some(partition))
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

    when(futureLog.logStartOffset).thenReturn(0L)
    when(futureLog.logEndOffset).thenReturn(0L)
    when(futureLog.latestEpoch).thenReturn(None)

    val fencedRequestData = new FetchRequest.PartitionData(0L, 0L,
      config.replicaFetchMaxBytes, Optional.of(leaderEpoch - 1))
    val fencedResponseData = FetchPartitionData(
      error = Errors.FENCED_LEADER_EPOCH,
      highWatermark = -1,
      logStartOffset = -1,
      records = MemoryRecords.EMPTY,
      divergingEpoch = None,
      lastStableOffset = None,
      abortedTransactions = None,
      preferredReadReplica = None,
      isReassignmentFetch = false)
    mockFetchFromCurrentLog(t1p0, fencedRequestData, config, replicaManager, fencedResponseData)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread",
      sourceBroker = endPoint,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = new BrokerTopicStats)

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

    val requestData = new FetchRequest.PartitionData(0L, 0L,
      config.replicaFetchMaxBytes, Optional.of(leaderEpoch))
    val responseData = FetchPartitionData(
      error = Errors.NONE,
      highWatermark = 0L,
      logStartOffset = 0L,
      records = MemoryRecords.EMPTY,
      divergingEpoch = None,
      lastStableOffset = None,
      abortedTransactions = None,
      preferredReadReplica = None,
      isReassignmentFetch = false)
    mockFetchFromCurrentLog(t1p0, requestData, config, replicaManager, responseData)

    thread.doWork()

    assertFalse(failedPartitions.contains(t1p0))
    assertEquals(None, thread.fetchState(t1p0))
    assertEquals(0, thread.partitionCount)
  }

  @Test
  def shouldReplaceCurrentLogDirWhenCaughtUp(): Unit = {
    val brokerId = 1
    val partitionId = 0
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId, "localhost:1234"))

    val partition = Mockito.mock(classOf[Partition])
    val replicaManager = Mockito.mock(classOf[ReplicaManager])
    val quotaManager = Mockito.mock(classOf[ReplicationQuotaManager])
    val futureLog = Mockito.mock(classOf[Log])

    val leaderEpoch = 5
    val logEndOffset = 0

    when(partition.partitionId).thenReturn(partitionId)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(replicaManager.nonOfflinePartition(t1p0)).thenReturn(Some(partition))
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

    when(futureLog.logStartOffset).thenReturn(0L)
    when(futureLog.logEndOffset).thenReturn(0L)
    when(futureLog.latestEpoch).thenReturn(None)

    val requestData = new FetchRequest.PartitionData(0L, 0L,
      config.replicaFetchMaxBytes, Optional.of(leaderEpoch))
    val responseData = FetchPartitionData(
      error = Errors.NONE,
      highWatermark = 0L,
      logStartOffset = 0L,
      records = MemoryRecords.EMPTY,
      divergingEpoch = None,
      lastStableOffset = None,
      abortedTransactions = None,
      preferredReadReplica = None,
      isReassignmentFetch = false)
    mockFetchFromCurrentLog(t1p0, requestData, config, replicaManager, responseData)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread",
      sourceBroker = endPoint,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = new BrokerTopicStats)

    thread.addPartitions(Map(t1p0 -> initialFetchState(fetchOffset = 0L, leaderEpoch)))
    assertTrue(thread.fetchState(t1p0).isDefined)
    assertEquals(1, thread.partitionCount)

    thread.doWork()

    assertEquals(None, thread.fetchState(t1p0))
    assertEquals(0, thread.partitionCount)
  }

  private def mockFetchFromCurrentLog(topicPartition: TopicPartition,
                                      requestData: FetchRequest.PartitionData,
                                      config: KafkaConfig,
                                      replicaManager: ReplicaManager,
                                      responseData: FetchPartitionData): Unit = {
    val callbackCaptor: ArgumentCaptor[Seq[(TopicPartition, FetchPartitionData)] => Unit] =
      ArgumentCaptor.forClass(classOf[Seq[(TopicPartition, FetchPartitionData)] => Unit])
    when(replicaManager.fetchMessages(
      timeout = ArgumentMatchers.eq(0L),
      replicaId = ArgumentMatchers.eq(Request.FutureLocalReplicaId),
      fetchMinBytes = ArgumentMatchers.eq(0),
      fetchMaxBytes = ArgumentMatchers.eq(config.replicaFetchResponseMaxBytes),
      hardMaxBytesLimit = ArgumentMatchers.eq(false),
      fetchInfos = ArgumentMatchers.eq(Seq(topicPartition -> requestData)),
      quota = ArgumentMatchers.eq(UnboundedQuota),
      responseCallback = callbackCaptor.capture(),
      isolationLevel = ArgumentMatchers.eq(IsolationLevel.READ_UNCOMMITTED),
      clientMetadata = ArgumentMatchers.eq(None)
    )).thenAnswer(_ => {
      callbackCaptor.getValue.apply(Seq((topicPartition, responseData)))
    })
  }

  @Test
  def issuesEpochRequestFromLocalReplica(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))

    //Setup all dependencies

    val partitionT1p0: Partition = createMock(classOf[Partition])
    val partitionT1p1: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    val partitionT1p0Id = 0
    val partitionT1p1Id = 1
    val leaderEpochT1p0 = 2
    val leaderEpochT1p1 = 5
    val leoT1p0 = 13
    val leoT1p1 = 232

    //Stubs
    expect(partitionT1p0.partitionId).andStubReturn(partitionT1p0Id)
    expect(partitionT1p0.partitionId).andStubReturn(partitionT1p1Id)

    expect(replicaManager.getPartitionOrException(t1p0))
      .andStubReturn(partitionT1p0)
    expect(partitionT1p0.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpochT1p0, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset()
        .setPartition(partitionT1p0Id)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpochT1p0)
        .setEndOffset(leoT1p0))
      .anyTimes()

    expect(replicaManager.getPartitionOrException(t1p1))
      .andStubReturn(partitionT1p1)
    expect(partitionT1p1.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpochT1p1, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset()
        .setPartition(partitionT1p1Id)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpochT1p1)
        .setEndOffset(leoT1p1))
      .anyTimes()

    replay(partitionT1p0, partitionT1p1, replicaManager)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = replicaManager,
      quota = null,
      brokerTopicStats = null)

    val result = thread.fetchEpochEndOffsets(Map(
      t1p0 -> new OffsetForLeaderPartition()
        .setPartition(t1p0.partition)
        .setLeaderEpoch(leaderEpochT1p0),
      t1p1 -> new OffsetForLeaderPartition()
        .setPartition(t1p1.partition)
        .setLeaderEpoch(leaderEpochT1p1)))

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
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))

    //Setup all dependencies
    val partitionT1p0: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    val partitionId = 0
    val leaderEpoch = 2
    val leo = 13

    //Stubs
    expect(partitionT1p0.partitionId).andStubReturn(partitionId)

    expect(replicaManager.getPartitionOrException(t1p0))
      .andStubReturn(partitionT1p0)
    expect(partitionT1p0.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(leo))
      .anyTimes()

    expect(replicaManager.getPartitionOrException(t1p1))
      .andThrow(new KafkaStorageException).once()

    replay(partitionT1p0, replicaManager)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = replicaManager,
      quota = null,
      brokerTopicStats = null)

    val result = thread.fetchEpochEndOffsets(Map(
      t1p0 -> new OffsetForLeaderPartition()
        .setPartition(t1p0.partition)
        .setLeaderEpoch(leaderEpoch),
      t1p1 -> new OffsetForLeaderPartition()
        .setPartition(t1p1.partition)
        .setLeaderEpoch(leaderEpoch)))

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
  def shouldTruncateToReplicaOffset(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncateCaptureT1p0: Capture[Long] = newCapture(CaptureType.ALL)
    val truncateCaptureT1p1: Capture[Long] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val logT1p0: Log = createNiceMock(classOf[Log])
    val logT1p1: Log = createNiceMock(classOf[Log])
    // one future replica mock because our mocking methods return same values for both future replicas
    val futureLogT1p0: Log = createNiceMock(classOf[Log])
    val futureLogT1p1: Log = createNiceMock(classOf[Log])
    val partitionT1p0: Partition = createMock(classOf[Partition])
    val partitionT1p1: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])
    val responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]  = EasyMock.newCapture()

    val partitionT1p0Id = 0
    val partitionT1p1Id = 1
    val leaderEpoch = 2
    val futureReplicaLEO = 191
    val replicaT1p0LEO = 190
    val replicaT1p1LEO = 192

    //Stubs
    expect(partitionT1p0.partitionId).andStubReturn(partitionT1p0Id)
    expect(partitionT1p1.partitionId).andStubReturn(partitionT1p1Id)

    expect(replicaManager.getPartitionOrException(t1p0))
      .andStubReturn(partitionT1p0)
    expect(replicaManager.getPartitionOrException(t1p1))
      .andStubReturn(partitionT1p1)
    expect(replicaManager.futureLocalLogOrException(t1p0)).andStubReturn(futureLogT1p0)
    expect(replicaManager.futureLogExists(t1p0)).andStubReturn(true)
    expect(replicaManager.futureLocalLogOrException(t1p1)).andStubReturn(futureLogT1p1)
    expect(replicaManager.futureLogExists(t1p1)).andStubReturn(true)
    expect(partitionT1p0.truncateTo(capture(truncateCaptureT1p0), anyBoolean())).anyTimes()
    expect(partitionT1p1.truncateTo(capture(truncateCaptureT1p1), anyBoolean())).anyTimes()

    expect(futureLogT1p0.logEndOffset).andReturn(futureReplicaLEO).anyTimes()
    expect(futureLogT1p1.logEndOffset).andReturn(futureReplicaLEO).anyTimes()

    expect(futureLogT1p0.latestEpoch).andReturn(Some(leaderEpoch)).anyTimes()
    expect(futureLogT1p0.endOffsetForEpoch(leaderEpoch)).andReturn(
      Some(OffsetAndEpoch(futureReplicaLEO, leaderEpoch))).anyTimes()
    expect(partitionT1p0.lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset()
        .setPartition(partitionT1p0Id)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(replicaT1p0LEO))
      .anyTimes()

    expect(futureLogT1p1.latestEpoch).andReturn(Some(leaderEpoch)).anyTimes()
    expect(futureLogT1p1.endOffsetForEpoch(leaderEpoch)).andReturn(
      Some(OffsetAndEpoch(futureReplicaLEO, leaderEpoch))).anyTimes()
    expect(partitionT1p1.lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset()
        .setPartition(partitionT1p1Id)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(replicaT1p1LEO))
      .anyTimes()

    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stubWithFetchMessages(logT1p0, logT1p1, futureLogT1p0, partitionT1p0, replicaManager, responseCallback)

    replay(replicaManager, logManager, quotaManager, partitionT1p0, partitionT1p1, logT1p0, logT1p1, futureLogT1p0, futureLogT1p1)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(t1p0 -> initialFetchState(0L), t1p1 -> initialFetchState(0L)))

    //Run it
    thread.doWork()

    //We should have truncated to the offsets in the response
    assertEquals(replicaT1p0LEO, truncateCaptureT1p0.getValue)
    assertEquals(futureReplicaLEO, truncateCaptureT1p1.getValue)
  }

  @Test
  def shouldTruncateToEndOffsetOfLargestCommonEpoch(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncateToCapture: Capture[Long] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val log: Log = createNiceMock(classOf[Log])
    // one future replica mock because our mocking methods return same values for both future replicas
    val futureLog: Log = createNiceMock(classOf[Log])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])
    val responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]  = EasyMock.newCapture()

    val partitionId = 0
    val leaderEpoch = 5
    val futureReplicaLEO = 195
    val replicaLEO = 200
    val replicaEpochEndOffset = 190
    val futureReplicaEpochEndOffset = 191

    //Stubs
    expect(partition.partitionId).andStubReturn(partitionId)

    expect(replicaManager.getPartitionOrException(t1p0))
      .andStubReturn(partition)
    expect(replicaManager.futureLocalLogOrException(t1p0)).andStubReturn(futureLog)
    expect(replicaManager.futureLogExists(t1p0)).andStubReturn(true)

    expect(partition.truncateTo(capture(truncateToCapture), EasyMock.eq(true))).anyTimes()
    expect(futureLog.logEndOffset).andReturn(futureReplicaLEO).anyTimes()
    expect(futureLog.latestEpoch).andReturn(Some(leaderEpoch)).once()
    expect(futureLog.latestEpoch).andReturn(Some(leaderEpoch - 2)).times(3)

    // leader replica truncated and fetched new offsets with new leader epoch
    expect(partition.lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch - 1)
        .setEndOffset(replicaLEO))
      .anyTimes()
    // but future replica does not know about this leader epoch, so returns a smaller leader epoch
    expect(futureLog.endOffsetForEpoch(leaderEpoch - 1)).andReturn(
      Some(OffsetAndEpoch(futureReplicaLEO, leaderEpoch - 2))).anyTimes()
    // finally, the leader replica knows about the leader epoch and returns end offset
    expect(partition.lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch - 2, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch - 2)
        .setEndOffset(replicaEpochEndOffset))
      .anyTimes()
    expect(futureLog.endOffsetForEpoch(leaderEpoch - 2)).andReturn(
      Some(OffsetAndEpoch(futureReplicaEpochEndOffset, leaderEpoch - 2))).anyTimes()

    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stubWithFetchMessages(log, null, futureLog, partition, replicaManager, responseCallback)

    replay(replicaManager, logManager, quotaManager, partition, log, futureLog)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(t1p0 -> initialFetchState(0L)))

    // First run will result in another offset for leader epoch request
    thread.doWork()
    // Second run should actually truncate
    thread.doWork()

    //We should have truncated to the offsets in the response
    assertTrue(truncateToCapture.getValues.asScala.contains(replicaEpochEndOffset),
               "Expected offset " + replicaEpochEndOffset + " in captured truncation offsets " + truncateToCapture.getValues)
  }

  @Test
  def shouldTruncateToInitialFetchOffsetIfReplicaReturnsUndefinedOffset(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncated: Capture[Long] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val log: Log = createNiceMock(classOf[Log])
    val futureLog: Log = createNiceMock(classOf[Log])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])
    val responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]  = EasyMock.newCapture()

    val initialFetchOffset = 100

    //Stubs
    expect(replicaManager.getPartitionOrException(t1p0))
      .andStubReturn(partition)
    expect(partition.truncateTo(capture(truncated), isFuture = EasyMock.eq(true))).anyTimes()
    expect(replicaManager.futureLocalLogOrException(t1p0)).andStubReturn(futureLog)
    expect(replicaManager.futureLogExists(t1p0)).andStubReturn(true)

    expect(replicaManager.logManager).andReturn(logManager).anyTimes()

    // pretend this is a completely new future replica, with no leader epochs recorded
    expect(futureLog.latestEpoch).andReturn(None).anyTimes()

    stubWithFetchMessages(log, null, futureLog, partition, replicaManager, responseCallback)
    replay(replicaManager, logManager, quotaManager, partition, log, futureLog)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(t1p0 -> initialFetchState(initialFetchOffset)))

    //Run it
    thread.doWork()

    //We should have truncated to initial fetch offset
    assertEquals(initialFetchOffset,
                 truncated.getValue, "Expected future replica to truncate to initial fetch offset if replica returns UNDEFINED_EPOCH_OFFSET")
  }

  @Test
  def shouldPollIndefinitelyIfReplicaNotAvailable(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncated: Capture[Long] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val log: Log = createNiceMock(classOf[Log])
    val futureLog: Log = createNiceMock(classOf[Log])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])
    val responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]  = EasyMock.newCapture()

    val partitionId = 0
    val futureReplicaLeaderEpoch = 1
    val futureReplicaLEO = 290
    val replicaLEO = 300

    //Stubs
    expect(partition.partitionId).andStubReturn(partitionId)

    expect(replicaManager.getPartitionOrException(t1p0))
      .andStubReturn(partition)
    expect(partition.truncateTo(capture(truncated), isFuture = EasyMock.eq(true))).once()

    expect(replicaManager.futureLocalLogOrException(t1p0)).andStubReturn(futureLog)
    expect(replicaManager.futureLogExists(t1p0)).andStubReturn(true)
    expect(futureLog.logEndOffset).andReturn(futureReplicaLEO).anyTimes()
    expect(futureLog.latestEpoch).andStubReturn(Some(futureReplicaLeaderEpoch))
    expect(futureLog.endOffsetForEpoch(futureReplicaLeaderEpoch)).andReturn(
      Some(OffsetAndEpoch(futureReplicaLEO, futureReplicaLeaderEpoch)))
    expect(replicaManager.localLog(t1p0)).andReturn(Some(log)).anyTimes()

    // this will cause fetchEpochsFromLeader return an error with undefined offset
    expect(partition.lastOffsetForLeaderEpoch(Optional.of(1), futureReplicaLeaderEpoch, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.REPLICA_NOT_AVAILABLE.code))
      .times(3)
      .andReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(futureReplicaLeaderEpoch)
        .setEndOffset(replicaLEO))

    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.fetchMessages(
      EasyMock.anyLong(),
      EasyMock.anyInt(),
      EasyMock.anyInt(),
      EasyMock.anyInt(),
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.capture(responseCallback),
      EasyMock.anyObject(),
      EasyMock.anyObject())
    ).andAnswer(() => responseCallback.getValue.apply(Seq.empty[(TopicPartition, FetchPartitionData)])).anyTimes()

    replay(replicaManager, logManager, quotaManager, partition, log, futureLog)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(t1p0 -> initialFetchState(0L)))

    // Run thread 3 times (exactly number of times we mock exception for getReplicaOrException)
    (0 to 2).foreach { _ =>
      thread.doWork()
    }

    // Nothing happened since the replica was not available
    assertEquals(0, truncated.getValues.size())

    // Next time we loop, getReplicaOrException will return replica
    thread.doWork()

    // Now the final call should have actually done a truncation (to offset futureReplicaLEO)
    assertEquals(futureReplicaLEO, truncated.getValue)
  }

  @Test
  def shouldFetchLeaderEpochOnFirstFetchOnly(): Unit = {

    //Setup all dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val log: Log = createNiceMock(classOf[Log])
    val futureLog: Log = createNiceMock(classOf[Log])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])
    val responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]  = EasyMock.newCapture()

    val partitionId = 0
    val leaderEpoch = 5
    val futureReplicaLEO = 190
    val replicaLEO = 213

    expect(partition.partitionId).andStubReturn(partitionId)

    expect(replicaManager.getPartitionOrException(t1p0))
        .andStubReturn(partition)
    expect(partition.lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch, fetchOnlyFromLeader = false))
        .andReturn(new EpochEndOffset()
          .setPartition(partitionId)
          .setErrorCode(Errors.NONE.code)
          .setLeaderEpoch(leaderEpoch)
          .setEndOffset(replicaLEO))
    expect(partition.truncateTo(futureReplicaLEO, isFuture = true)).once()

    expect(replicaManager.futureLocalLogOrException(t1p0)).andStubReturn(futureLog)
    expect(replicaManager.futureLogExists(t1p0)).andStubReturn(true)
    expect(futureLog.latestEpoch).andStubReturn(Some(leaderEpoch))
    expect(futureLog.logEndOffset).andStubReturn(futureReplicaLEO)
    expect(futureLog.endOffsetForEpoch(leaderEpoch)).andReturn(
      Some(OffsetAndEpoch(futureReplicaLEO, leaderEpoch)))
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stubWithFetchMessages(log, null, futureLog, partition, replicaManager, responseCallback)

    replay(replicaManager, logManager, quotaManager, partition, log, futureLog)

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(t1p0 -> initialFetchState(0L)))

    // loop few times
    (0 to 3).foreach { _ =>
      thread.doWork()
    }

    //Assert that truncate to is called exactly once (despite more loops)
    verify(partition)
  }

  @Test
  def shouldFetchOneReplicaAtATime(): Unit = {

    //Setup all dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val log: Log = createNiceMock(classOf[Log])
    val futureLog: Log = createNiceMock(classOf[Log])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    //Stubs
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stub(log, null, futureLog, partition, replicaManager)

    replay(replicaManager, logManager, quotaManager, partition, log)

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leaderEpoch = 1
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(
      t1p0 -> initialFetchState(0L, leaderEpoch),
      t1p1 -> initialFetchState(0L, leaderEpoch)))

    val ResultWithPartitions(fetchRequestOpt, partitionsWithError) = thread.buildFetch(Map(
      t1p0 -> PartitionFetchState(150, None, leaderEpoch, None, state = Fetching, lastFetchedEpoch = None),
      t1p1 -> PartitionFetchState(160, None, leaderEpoch, None, state = Fetching, lastFetchedEpoch = None)))

    assertTrue(fetchRequestOpt.isDefined)
    val fetchRequest = fetchRequestOpt.get.fetchRequest
    assertFalse(fetchRequest.fetchData.isEmpty)
    assertFalse(partitionsWithError.nonEmpty)
    val request = fetchRequest.build()
    assertEquals(0, request.minBytes)
    val fetchInfos = request.fetchData.asScala.toSeq
    assertEquals(1, fetchInfos.length)
    assertEquals(t1p0, fetchInfos.head._1, "Expected fetch request for first partition")
    assertEquals(150, fetchInfos.head._2.fetchOffset)
  }

  @Test
  def shouldFetchNonDelayedAndNonTruncatingReplicas(): Unit = {

    //Setup all dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val log: Log = createNiceMock(classOf[Log])
    val futureLog: Log = createNiceMock(classOf[Log])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    //Stubs
    val startOffset = 123
    expect(futureLog.logStartOffset).andReturn(startOffset).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stub(log, null, futureLog, partition, replicaManager)

    replay(replicaManager, logManager, quotaManager, partition, log, futureLog)

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leaderEpoch = 1
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(
      t1p0 -> initialFetchState(0L, leaderEpoch),
      t1p1 -> initialFetchState(0L, leaderEpoch)))

    // one partition is ready and one is truncating
    val ResultWithPartitions(fetchRequestOpt, partitionsWithError) = thread.buildFetch(Map(
        t1p0 -> PartitionFetchState(150, None, leaderEpoch, state = Fetching, lastFetchedEpoch = None),
        t1p1 -> PartitionFetchState(160, None, leaderEpoch, state = Truncating, lastFetchedEpoch = None)))

    assertTrue(fetchRequestOpt.isDefined)
    val fetchRequest = fetchRequestOpt.get
    assertFalse(fetchRequest.partitionData.isEmpty)
    assertFalse(partitionsWithError.nonEmpty)
    val fetchInfos = fetchRequest.fetchRequest.build().fetchData.asScala.toSeq
    assertEquals(1, fetchInfos.length)
    assertEquals(t1p0, fetchInfos.head._1, "Expected fetch request for non-truncating partition")
    assertEquals(150, fetchInfos.head._2.fetchOffset)

    // one partition is ready and one is delayed
    val ResultWithPartitions(fetchRequest2Opt, partitionsWithError2) = thread.buildFetch(Map(
        t1p0 -> PartitionFetchState(140, None, leaderEpoch, state = Fetching, lastFetchedEpoch = None),
        t1p1 -> PartitionFetchState(160, None, leaderEpoch, delay = Some(new DelayedItem(5000)), state = Fetching, lastFetchedEpoch = None)))

    assertTrue(fetchRequest2Opt.isDefined)
    val fetchRequest2 = fetchRequest2Opt.get
    assertFalse(fetchRequest2.partitionData.isEmpty)
    assertFalse(partitionsWithError2.nonEmpty)
    val fetchInfos2 = fetchRequest2.fetchRequest.build().fetchData.asScala.toSeq
    assertEquals(1, fetchInfos2.length)
    assertEquals(t1p0, fetchInfos2.head._1, "Expected fetch request for non-delayed partition")
    assertEquals(140, fetchInfos2.head._2.fetchOffset)

    // both partitions are delayed
    val ResultWithPartitions(fetchRequest3Opt, partitionsWithError3) = thread.buildFetch(Map(
        t1p0 -> PartitionFetchState(140, None, leaderEpoch, delay = Some(new DelayedItem(5000)), state = Fetching, lastFetchedEpoch = None),
        t1p1 -> PartitionFetchState(160, None, leaderEpoch, delay = Some(new DelayedItem(5000)), state = Fetching, lastFetchedEpoch = None)))
    assertTrue(fetchRequest3Opt.isEmpty, "Expected no fetch requests since all partitions are delayed")
    assertFalse(partitionsWithError3.nonEmpty)
  }

  def stub(logT1p0: Log, logT1p1: Log, futureLog: Log, partition: Partition,
           replicaManager: ReplicaManager): IExpectationSetters[Option[Partition]] = {
    expect(replicaManager.localLog(t1p0)).andReturn(Some(logT1p0)).anyTimes()
    expect(replicaManager.localLogOrException(t1p0)).andReturn(logT1p0).anyTimes()
    expect(replicaManager.futureLocalLogOrException(t1p0)).andReturn(futureLog).anyTimes()
    expect(replicaManager.futureLogExists(t1p0)).andStubReturn(true)
    expect(replicaManager.nonOfflinePartition(t1p0)).andReturn(Some(partition)).anyTimes()
    expect(replicaManager.localLog(t1p1)).andReturn(Some(logT1p1)).anyTimes()
    expect(replicaManager.localLogOrException(t1p1)).andReturn(logT1p1).anyTimes()
    expect(replicaManager.futureLocalLogOrException(t1p1)).andReturn(futureLog).anyTimes()
    expect(replicaManager.futureLogExists(t1p1)).andStubReturn(true)
    expect(replicaManager.nonOfflinePartition(t1p1)).andReturn(Some(partition)).anyTimes()
  }

  def stubWithFetchMessages(logT1p0: Log, logT1p1: Log, futureLog: Log, partition: Partition, replicaManager: ReplicaManager,
          responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]): IExpectationSetters[Unit] = {
    stub(logT1p0, logT1p1, futureLog, partition, replicaManager)
    expect(replicaManager.fetchMessages(
      EasyMock.anyLong(),
      EasyMock.anyInt(),
      EasyMock.anyInt(),
      EasyMock.anyInt(),
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.capture(responseCallback),
      EasyMock.anyObject(),
      EasyMock.anyObject())
    ).andAnswer(() => responseCallback.getValue.apply(Seq.empty[(TopicPartition, FetchPartitionData)])).anyTimes()
  }
}
