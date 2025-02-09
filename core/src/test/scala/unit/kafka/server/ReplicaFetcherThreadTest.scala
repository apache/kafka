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
import kafka.log.{LogManager, UnifiedLog}
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import kafka.server.QuotaFactory.UNBOUNDED_QUOTA
import kafka.server.epoch.util.MockBlockingSender
import kafka.utils.TestUtils
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, RecordBatch, RecordValidationStats, SimpleRecord}
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion, OffsetAndEpoch}
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.storage.internals.log.LogAppendInfo
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyLong}
import org.mockito.Mockito.{mock, times, verify, when}

import java.nio.charset.StandardCharsets
import java.util
import java.util.{Collections, Optional, OptionalInt}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class ReplicaFetcherThreadTest {

  private val t1p0 = new TopicPartition("topic1", 0)
  private val t1p1 = new TopicPartition("topic1", 1)
  private val t2p1 = new TopicPartition("topic2", 1)

  private val topicId1 = Uuid.randomUuid()
  private val topicId2 = Uuid.randomUuid()

  private val topicIds = Map("topic1" -> topicId1, "topic2" -> topicId2)

  private val brokerEndPoint = new BrokerEndPoint(0, "localhost", 1000)
  private val failedPartitions = new FailedPartitions

  private val metadataCache = MetadataCache.kRaftMetadataCache(0, () => KRaftVersion.LATEST_PRODUCTION)

  private def initialFetchState(topicId: Option[Uuid], fetchOffset: Long, leaderEpoch: Int = 1): InitialFetchState = {
    InitialFetchState(topicId = topicId, leader = new BrokerEndPoint(0, "localhost", 9092),
      initOffset = fetchOffset, currentLeaderEpoch = leaderEpoch)
  }

  @AfterEach
  def cleanup(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  private def createReplicaFetcherThread(
    name: String,
    fetcherId: Int,
    brokerConfig: KafkaConfig,
    failedPartitions: FailedPartitions,
    replicaMgr: ReplicaManager,
    quota: ReplicaQuota,
    leaderEndpointBlockingSend: BlockingSend,
    metadataVersion: MetadataVersion = MetadataVersion.latestTesting()
  ): ReplicaFetcherThread = {
    val logContext = new LogContext(s"[ReplicaFetcher replicaId=${brokerConfig.brokerId}, leaderId=${leaderEndpointBlockingSend.brokerEndPoint().id}, fetcherId=$fetcherId] ")
    val fetchSessionHandler = new FetchSessionHandler(logContext, leaderEndpointBlockingSend.brokerEndPoint().id)
    val leader = new RemoteLeaderEndPoint(logContext.logPrefix, leaderEndpointBlockingSend, fetchSessionHandler,
      brokerConfig, replicaMgr, quota, () => brokerConfig.interBrokerProtocolVersion, () => 1)
    new ReplicaFetcherThread(name,
      leader,
      brokerConfig,
      failedPartitions,
      replicaMgr,
      quota,
      logContext.logPrefix,
      () => metadataVersion)
  }

  @Test
  def shouldSendLatestRequestVersionsByDefault(): Unit = {
    // Check unstable versions
    val testingVersion = MetadataVersion.latestTesting
    assertEquals(
      ApiKeys.FETCH.latestVersion(true),
      testingVersion.fetchRequestVersion
    )
    assertEquals(
      ApiKeys.LIST_OFFSETS.latestVersion(true),
      testingVersion.listOffsetRequestVersion
    )
  }

  /**
    * Assert that all partitions' states are as expected
    *
    */
  def assertPartitionStates(fetcher: AbstractFetcherThread,
                            shouldBeReadyForFetch: Boolean,
                            shouldBeTruncatingLog: Boolean,
                            shouldBeDelayed: Boolean): Unit = {
    for (tp <- List(t1p0, t1p1, t2p1)) {
      assertTrue(fetcher.fetchState(tp).isDefined)
      val fetchState = fetcher.fetchState(tp).get

      assertEquals(shouldBeReadyForFetch, fetchState.isReadyForFetch,
        s"Partition $tp should${if (!shouldBeReadyForFetch) " NOT" else ""} be ready for fetching")

      assertEquals(shouldBeTruncatingLog, fetchState.isTruncating,
        s"Partition $tp should${if (!shouldBeTruncatingLog) " NOT" else ""} be truncating its log")

      assertEquals(shouldBeDelayed, fetchState.isDelayed,
        s"Partition $tp should${if (!shouldBeDelayed) " NOT" else ""} be delayed")
    }
  }

  @Test
  def shouldHandleExceptionFromBlockingSend(): Unit = {
    val props = TestUtils.createBrokerConfig(1)
    val config = KafkaConfig.fromProps(props)
    val mockBlockingSend: BlockingSend = mock(classOf[BlockingSend])
    when(mockBlockingSend.brokerEndPoint()).thenReturn(brokerEndPoint)
    when(mockBlockingSend.sendRequest(any())).thenThrow(new NullPointerException)

    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))

    val thread = createReplicaFetcherThread(
      "bob",
      0,
      config,
      failedPartitions,
      replicaManager,
      null,
      mockBlockingSend
    )

    val result = thread.leader.fetchEpochEndOffsets(Map(
      t1p0 -> new OffsetForLeaderPartition()
        .setPartition(t1p0.partition)
        .setLeaderEpoch(0),
      t1p1 -> new OffsetForLeaderPartition()
        .setPartition(t1p1.partition)
        .setLeaderEpoch(0)))

    val expected = Map(
      t1p0 -> newOffsetForLeaderPartitionResult(t1p0, Errors.UNKNOWN_SERVER_ERROR, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET),
      t1p1 -> newOffsetForLeaderPartitionResult(t1p1, Errors.UNKNOWN_SERVER_ERROR, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
    )

    assertEquals(expected, result, "results from leader epoch request should have undefined offset")
    verify(mockBlockingSend).sendRequest(any())
  }

  @Test
  def shouldNotFetchLeaderEpochOnFirstFetchWithTruncateOnFetch(): Unit = {
    verifyFetchLeaderEpochOnFirstFetch(MetadataVersion.latestTesting, epochFetchCount = 0)
  }

  private def verifyFetchLeaderEpochOnFirstFetch(metadataVersion: MetadataVersion, epochFetchCount: Int): Unit = {
    val props = TestUtils.createBrokerConfig(1)
    val config = KafkaConfig.fromProps(props)

    //Setup all dependencies
    val logManager: LogManager = mock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val leaderEpoch = 5

    //Stubs
    when(partition.localLogOrException).thenReturn(log)
    when(log.highWatermark).thenReturn(0)
    when(log.latestEpoch).thenReturn(Some(leaderEpoch))
    when(log.endOffsetForEpoch(leaderEpoch)).thenReturn(
      Some(new OffsetAndEpoch(0, leaderEpoch)))
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.replicaAlterLogDirsManager).thenReturn(replicaAlterLogDirsManager)
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))
    stub(partition, replicaManager, log)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsets = Map(
      t1p0 -> newOffsetForLeaderPartitionResult(t1p0, leaderEpoch, 1),
      t1p1 -> newOffsetForLeaderPartitionResult(t1p1, leaderEpoch, 1)).asJava

    //Create the fetcher thread
    val mockNetwork = new MockBlockingSender(offsets, brokerEndPoint, Time.SYSTEM)
    val thread = createReplicaFetcherThread(
      "bob",
      0,
      config,
      failedPartitions,
      replicaManager,
      UNBOUNDED_QUOTA,
      mockNetwork,
      metadataVersion
    )
    thread.addPartitions(Map(t1p0 -> initialFetchState(Some(topicId1), 0L), t1p1 -> initialFetchState(Some(topicId1), 0L)))

    //Loop 1
    thread.doWork()
    assertEquals(epochFetchCount, mockNetwork.epochFetchCount)
    assertEquals(1, mockNetwork.fetchCount)

    //Loop 2 we should not fetch epochs
    thread.doWork()
    assertEquals(epochFetchCount, mockNetwork.epochFetchCount)
    assertEquals(2, mockNetwork.fetchCount)

    //Loop 3 we should not fetch epochs
    thread.doWork()
    assertEquals(epochFetchCount, mockNetwork.epochFetchCount)
    assertEquals(3, mockNetwork.fetchCount)
  }

  @Test
  def shouldTruncateIfLeaderRepliesWithDivergingEpochNotKnownToFollower(): Unit = {

    // Create a capture to track what partitions/offsets are truncated
    val truncateToCapture: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))

    // Setup all dependencies
    val quota: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val initialLEO = 200
    var latestLogEpoch: Option[Int] = Some(5)

    // Stubs
    when(partition.localLogOrException).thenReturn(log)
    when(log.highWatermark).thenReturn(115)
    when(log.latestEpoch).thenAnswer(_ => latestLogEpoch)
    when(log.endOffsetForEpoch(4)).thenReturn(Some(new OffsetAndEpoch(149, 4)))
    when(log.endOffsetForEpoch(3)).thenReturn(Some(new OffsetAndEpoch(129, 2)))
    when(log.endOffsetForEpoch(2)).thenReturn(Some(new OffsetAndEpoch(119, 1)))
    when(log.logEndOffset).thenReturn(initialLEO)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.localLogOrException(any[TopicPartition])).thenReturn(log)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.replicaAlterLogDirsManager).thenReturn(replicaAlterLogDirsManager)
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))
    stub(partition, replicaManager, log)

    // Create the fetcher thread
    val mockNetwork = new MockBlockingSender(Collections.emptyMap(), brokerEndPoint, Time.SYSTEM)
    val logContext = new LogContext(s"[ReplicaFetcher replicaId=${config.brokerId}, leaderId=${brokerEndPoint.id}, fetcherId=0] ")
    val fetchSessionHandler = new FetchSessionHandler(logContext, brokerEndPoint.id)
    val leader = new RemoteLeaderEndPoint(logContext.logPrefix, mockNetwork, fetchSessionHandler, config,
      replicaManager, quota, () => config.interBrokerProtocolVersion, () => 1)
    val thread = new ReplicaFetcherThread("bob", leader, config, failedPartitions,
      replicaManager, quota, logContext.logPrefix, () => config.interBrokerProtocolVersion) {
      override def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: FetchData): Option[LogAppendInfo] = None
    }
    thread.addPartitions(Map(t1p0 -> initialFetchState(Some(topicId1), initialLEO), t1p1 -> initialFetchState(Some(topicId1), initialLEO)))
    val partitions = Set(t1p0, t1p1)

    // Loop 1 -- both topic partitions skip epoch fetch and send fetch request since we can truncate
    // later based on diverging epochs in fetch response.
    thread.doWork()
    assertEquals(0, mockNetwork.epochFetchCount)
    assertEquals(1, mockNetwork.fetchCount)
    partitions.foreach { tp => assertEquals(Fetching, thread.fetchState(tp).get.state) }

    def partitionData(partition: Int, divergingEpoch: FetchResponseData.EpochEndOffset): FetchResponseData.PartitionData = {
      new FetchResponseData.PartitionData()
        .setPartitionIndex(partition)
        .setLastStableOffset(0)
        .setLogStartOffset(0)
        .setDivergingEpoch(divergingEpoch)
    }

    // Loop 2 should truncate based on diverging epoch and continue to send fetch requests.
    mockNetwork.setFetchPartitionDataForNextResponse(Map(
      t1p0 -> partitionData(t1p0.partition, new FetchResponseData.EpochEndOffset().setEpoch(4).setEndOffset(140)),
      t1p1 -> partitionData(t1p1.partition, new FetchResponseData.EpochEndOffset().setEpoch(4).setEndOffset(141))
    ))
    mockNetwork.setIdsForNextResponse(topicIds)
    latestLogEpoch = Some(4)
    thread.doWork()
    assertEquals(0, mockNetwork.epochFetchCount)
    assertEquals(2, mockNetwork.fetchCount)
    verify(partition, times(2)).truncateTo(truncateToCapture.capture(), anyBoolean())
    assertTrue(truncateToCapture.getAllValues.asScala.contains(140),
      "Expected " + t1p0 + " to truncate to offset 140 (truncation offsets: " + truncateToCapture.getAllValues + ")")
    assertTrue(truncateToCapture.getAllValues.asScala.contains(141),
      "Expected " + t1p1 + " to truncate to offset 141 (truncation offsets: " + truncateToCapture.getAllValues + ")")
    partitions.foreach { tp => assertEquals(Fetching, thread.fetchState(tp).get.state) }

    // Loop 3 should truncate because of diverging epoch. Offset truncation is not complete
    // because divergent epoch is not known to follower. We truncate and stay in Fetching state.
    mockNetwork.setFetchPartitionDataForNextResponse(Map(
      t1p0 -> partitionData(t1p0.partition, new FetchResponseData.EpochEndOffset().setEpoch(3).setEndOffset(130)),
      t1p1 -> partitionData(t1p1.partition, new FetchResponseData.EpochEndOffset().setEpoch(3).setEndOffset(131))
    ))
    mockNetwork.setIdsForNextResponse(topicIds)
    thread.doWork()
    assertEquals(0, mockNetwork.epochFetchCount)
    assertEquals(3, mockNetwork.fetchCount)
    verify(partition, times(4)).truncateTo(truncateToCapture.capture(), anyBoolean())
    assertTrue(truncateToCapture.getAllValues.asScala.contains(129),
      "Expected to truncate to offset 129 (truncation offsets: " + truncateToCapture.getAllValues + ")")
    partitions.foreach { tp => assertEquals(Fetching, thread.fetchState(tp).get.state) }

    // Loop 4 should truncate because of diverging epoch. Offset truncation is not complete
    // because divergent epoch is not known to follower. Last fetched epoch cannot be determined
    // from the log. We truncate and stay in Fetching state.
    mockNetwork.setFetchPartitionDataForNextResponse(Map(
      t1p0 -> partitionData(t1p0.partition, new FetchResponseData.EpochEndOffset().setEpoch(2).setEndOffset(120)),
      t1p1 -> partitionData(t1p1.partition, new FetchResponseData.EpochEndOffset().setEpoch(2).setEndOffset(121))
    ))
    mockNetwork.setIdsForNextResponse(topicIds)
    latestLogEpoch = None
    thread.doWork()
    assertEquals(0, mockNetwork.epochFetchCount)
    assertEquals(4, mockNetwork.fetchCount)
    verify(partition, times(6)).truncateTo(truncateToCapture.capture(), anyBoolean())
    assertTrue(truncateToCapture.getAllValues.asScala.contains(119),
      "Expected to truncate to offset 119 (truncation offsets: " + truncateToCapture.getAllValues + ")")
    partitions.foreach { tp => assertEquals(Fetching, thread.fetchState(tp).get.state) }
  }

  @Test
  def testTruncateOnFetchDoesNotUpdateHighWatermark(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val quota: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])

    val logEndOffset = 150
    val highWatermark = 130

    when(log.highWatermark).thenReturn(highWatermark)
    when(log.latestEpoch).thenReturn(Some(5))
    when(log.endOffsetForEpoch(4)).thenReturn(Some(new OffsetAndEpoch(149, 4)))
    when(log.logEndOffset).thenReturn(logEndOffset)

    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.replicaAlterLogDirsManager).thenReturn(replicaAlterLogDirsManager)

    when(replicaManager.localLogOrException(t1p0)).thenReturn(log)
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)

    when(partition.localLogOrException).thenReturn(log)
    when(partition.appendRecordsToFollowerOrFutureReplica(any(), any())).thenReturn(None)

    val logContext = new LogContext(s"[ReplicaFetcher replicaId=${config.brokerId}, leaderId=${brokerEndPoint.id}, fetcherId=0] ")

    val mockNetwork = new MockBlockingSender(
      Collections.emptyMap(),
      brokerEndPoint,
      Time.SYSTEM
    )

    val leader = new RemoteLeaderEndPoint(
      logContext.logPrefix,
      mockNetwork,
      new FetchSessionHandler(logContext, brokerEndPoint.id),
      config,
      replicaManager,
      quota,
      () => config.interBrokerProtocolVersion,
      () => 1
    )

    val thread = new ReplicaFetcherThread(
      "fetcher-thread",
      leader,
      config,
      failedPartitions,
      replicaManager,
      quota,
      logContext.logPrefix,
      () => config.interBrokerProtocolVersion
    )

    thread.addPartitions(Map(
      t1p0 -> initialFetchState(Some(topicId1), logEndOffset))
    )

    // Prepare the fetch response data.
    mockNetwork.setFetchPartitionDataForNextResponse(Map(
      t1p0 -> new FetchResponseData.PartitionData()
        .setPartitionIndex(t1p0.partition)
        .setLastStableOffset(0)
        .setLogStartOffset(0)
        .setHighWatermark(160) // HWM is higher on the leader.
        .setDivergingEpoch(new FetchResponseData.EpochEndOffset()
          .setEpoch(4)
          .setEndOffset(140))
    ))
    mockNetwork.setIdsForNextResponse(topicIds)

    // Sends the fetch request and processes the response. This should truncate the
    // log but it should not update the high watermark.
    thread.doWork()

    assertEquals(1, mockNetwork.fetchCount)
    verify(partition, times(1)).truncateTo(140, false)
    verify(log, times(0)).maybeUpdateHighWatermark(anyLong())
  }

  @Test
  def testLagIsUpdatedWhenNoRecords(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val quota: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val lastFetchedEpoch = 2

    when(log.highWatermark).thenReturn(0)
    when(log.latestEpoch).thenReturn(Some(lastFetchedEpoch))
    when(log.endOffsetForEpoch(0)).thenReturn(Some(new OffsetAndEpoch(0, 0)))
    when(log.logEndOffset).thenReturn(0)
    when(log.maybeUpdateHighWatermark(0)).thenReturn(None)

    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.localLogOrException(t1p0)).thenReturn(log)
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))

    when(partition.localLogOrException).thenReturn(log)
    when(partition.appendRecordsToFollowerOrFutureReplica(any(), any())).thenReturn(Some(new LogAppendInfo(
      -1,
      0,
      OptionalInt.empty,
      RecordBatch.NO_TIMESTAMP,
      RecordBatch.NO_TIMESTAMP,
      -1L,
      RecordValidationStats.EMPTY,
      CompressionType.NONE,
      -1, // No records.
      -1L
    )))

    val logContext = new LogContext(s"[ReplicaFetcher replicaId=${config.brokerId}, leaderId=${brokerEndPoint.id}, fetcherId=0] ")

    val mockNetwork = new MockBlockingSender(
      Collections.emptyMap(),
      brokerEndPoint,
      Time.SYSTEM
    )

    val leader = new RemoteLeaderEndPoint(
      logContext.logPrefix,
      mockNetwork,
      new FetchSessionHandler(logContext, brokerEndPoint.id),
      config,
      replicaManager,
      quota,
      () => config.interBrokerProtocolVersion,
      () => 1
    )

    val thread = new ReplicaFetcherThread(
      "fetcher-thread",
      leader,
      config,
      failedPartitions,
      replicaManager,
      quota,
      logContext.logPrefix,
      () => config.interBrokerProtocolVersion
    )

    thread.addPartitions(Map(
      t1p0 -> initialFetchState(Some(topicId1), 0))
    )

    // Lag is initialized to None when the partition fetch
    // state is created.
    assertEquals(None, thread.fetchState(t1p0).flatMap(_.lag))

    // Prepare the fetch response data.
    mockNetwork.setFetchPartitionDataForNextResponse(Map(
      t1p0 -> new FetchResponseData.PartitionData()
        .setPartitionIndex(t1p0.partition)
        .setLastStableOffset(0)
        .setLogStartOffset(0)
        .setHighWatermark(0)
        .setRecords(MemoryRecords.EMPTY) // No records.
    ))
    mockNetwork.setIdsForNextResponse(topicIds)

    // Sends the fetch request and processes the response.
    thread.doWork()
    assertEquals(1, mockNetwork.fetchCount)

    // Lag is set to Some(0).
    assertEquals(Some(0), thread.fetchState(t1p0).flatMap(_.lag))
    assertEquals(Some(lastFetchedEpoch), thread.fetchState(t1p0).flatMap(_.lastFetchedEpoch))
  }

  @Test
  def shouldCatchExceptionFromBlockingSendWhenShuttingDownReplicaFetcherThread(): Unit = {
    val props = TestUtils.createBrokerConfig(1)
    val config = KafkaConfig.fromProps(props)

    val mockBlockingSend: BlockingSend = mock(classOf[BlockingSend])
    when(mockBlockingSend.brokerEndPoint()).thenReturn(brokerEndPoint)
    when(mockBlockingSend.initiateClose()).thenThrow(new IllegalArgumentException())
    when(mockBlockingSend.close()).thenThrow(new IllegalStateException())

    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))

    val thread = createReplicaFetcherThread(
      "bob",
      0,
      config,
      failedPartitions,
      replicaManager,
      null,
      mockBlockingSend
    )
    thread.start()

    // Verify that:
    //   1) IllegalArgumentException thrown by BlockingSend#initiateClose() during `initiateShutdown` is not propagated
    //   2) BlockingSend.close() is invoked even if BlockingSend#initiateClose() fails
    //   3) IllegalStateException thrown by BlockingSend.close() during `awaitShutdown` is not propagated
    thread.initiateShutdown()
    thread.awaitShutdown()
    verify(mockBlockingSend).initiateClose()
    verify(mockBlockingSend).close()
  }

  @Test
  def shouldUpdateReassignmentBytesInMetrics(): Unit = {
    assertProcessPartitionDataWhen(isReassigning = true)
  }

  @Test
  def shouldNotUpdateReassignmentBytesInMetricsWhenNoReassignmentsInProgress(): Unit = {
    assertProcessPartitionDataWhen(isReassigning = false)
  }

  @Test
  def testBuildFetch(): Unit = {
    val tid1p0 = new TopicIdPartition(topicId1, t1p0)
    val tid1p1 = new TopicIdPartition(topicId1, t1p1)
    val tid2p1 = new TopicIdPartition(topicId2, t2p1)

    val props = TestUtils.createBrokerConfig(1)
    val config = KafkaConfig.fromProps(props)
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val mockBlockingSend: BlockingSend = mock(classOf[BlockingSend])
    val replicaQuota: ReplicaQuota = mock(classOf[ReplicaQuota])
    val log: UnifiedLog = mock(classOf[UnifiedLog])

    when(mockBlockingSend.brokerEndPoint()).thenReturn(brokerEndPoint)
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))
    when(replicaManager.localLogOrException(any[TopicPartition])).thenReturn(log)
    when(replicaQuota.isThrottled(any[TopicPartition])).thenReturn(false)
    when(log.logStartOffset).thenReturn(0)

    val logContext = new LogContext(s"[ReplicaFetcher replicaId=${config.brokerId}, leaderId=${brokerEndPoint.id}, fetcherId=0] ")
    val fetchSessionHandler = new FetchSessionHandler(logContext, brokerEndPoint.id)
    val leader = new RemoteLeaderEndPoint(logContext.logPrefix, mockBlockingSend, fetchSessionHandler, config,
      replicaManager, replicaQuota, () => config.interBrokerProtocolVersion, () => 1)
    val thread = new ReplicaFetcherThread("bob",
      leader,
      config,
      failedPartitions,
      replicaManager,
      replicaQuota,
      logContext.logPrefix,
      () => config.interBrokerProtocolVersion)

    val leaderEpoch = 1

    val partitionMap = Map(
        t1p0 -> PartitionFetchState(Some(topicId1), 150, None, leaderEpoch, None, state = Fetching, lastFetchedEpoch = None),
        t1p1 -> PartitionFetchState(Some(topicId1), 155, None, leaderEpoch, None, state = Fetching, lastFetchedEpoch = None),
        t2p1 -> PartitionFetchState(Some(topicId2), 160, None, leaderEpoch, None, state = Fetching, lastFetchedEpoch = None))

    val ResultWithPartitions(fetchRequestOpt, _) = thread.leader.buildFetch(partitionMap)

    assertTrue(fetchRequestOpt.isDefined)
    val fetchRequestBuilder = fetchRequestOpt.get.fetchRequest

    val partitionDataMap = partitionMap.map { case (tp, state) =>
      (tp, new FetchRequest.PartitionData(state.topicId.get, state.fetchOffset, 0L,
        config.replicaFetchMaxBytes, Optional.of(state.currentLeaderEpoch), Optional.empty()))
    }

    assertEquals(partitionDataMap.asJava, fetchRequestBuilder.fetchData())
    assertEquals(0, fetchRequestBuilder.replaced().size)
    assertEquals(0, fetchRequestBuilder.removed().size)

    val responseData = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    responseData.put(tid1p0, new FetchResponseData.PartitionData())
    responseData.put(tid1p1, new FetchResponseData.PartitionData())
    responseData.put(tid2p1, new FetchResponseData.PartitionData())
    val fetchResponse = FetchResponse.of(Errors.NONE, 0, 123, responseData)

    leader.fetchSessionHandler.handleResponse(fetchResponse, ApiKeys.FETCH.latestVersion())

    // Remove t1p0, change the ID for t2p1, and keep t1p1 the same
    val newTopicId = Uuid.randomUuid()
    val partitionMap2 = Map(
      t1p1 -> PartitionFetchState(Some(topicId1), 155, None, leaderEpoch, None, state = Fetching, lastFetchedEpoch = None),
      t2p1 -> PartitionFetchState(Some(newTopicId), 160, None, leaderEpoch, None, state = Fetching, lastFetchedEpoch = None))
    val ResultWithPartitions(fetchRequestOpt2, _) = thread.leader.buildFetch(partitionMap2)

    // Since t1p1 didn't change, we drop that one
    val partitionDataMap2 = partitionMap2.drop(1).map { case (tp, state) =>
      (tp, new FetchRequest.PartitionData(state.topicId.get, state.fetchOffset, 0L,
        config.replicaFetchMaxBytes, Optional.of(state.currentLeaderEpoch), Optional.empty()))
    }

    assertTrue(fetchRequestOpt2.isDefined)
    val fetchRequestBuilder2 = fetchRequestOpt2.get.fetchRequest
    assertEquals(partitionDataMap2.asJava, fetchRequestBuilder2.fetchData())
    assertEquals(Collections.singletonList(tid2p1), fetchRequestBuilder2.replaced())
    assertEquals(Collections.singletonList(tid1p0), fetchRequestBuilder2.removed())
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testLocalFetchCompletionIfHighWatermarkUpdated(highWatermarkUpdated: Boolean): Unit = {
    val props = TestUtils.createBrokerConfig(1)
    val config = KafkaConfig.fromProps(props)
    val highWatermarkReceivedFromLeader = 100L

    val mockBlockingSend: BlockingSend = mock(classOf[BlockingSend])
    when(mockBlockingSend.brokerEndPoint()).thenReturn(brokerEndPoint)

    val maybeNewHighWatermark = if (highWatermarkUpdated) {
      Some(highWatermarkReceivedFromLeader)
    } else {
      None
    }
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    when(log.maybeUpdateHighWatermark(highWatermarkReceivedFromLeader))
      .thenReturn(maybeNewHighWatermark)

    val appendInfo: Option[LogAppendInfo] = Some(mock(classOf[LogAppendInfo]))

    val partition: Partition = mock(classOf[Partition])
    when(partition.localLogOrException).thenReturn(log)
    when(partition.appendRecordsToFollowerOrFutureReplica(any[MemoryRecords], any[Boolean])).thenReturn(appendInfo)

    // Capture the argument at the time of invocation.
    val completeDelayedFetchRequestsArgument = mutable.Buffer.empty[TopicPartition]
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getPartitionOrException(any[TopicPartition])).thenReturn(partition)
    when(replicaManager.completeDelayedFetchRequests(any[Seq[TopicPartition]])).thenAnswer(invocation =>
      completeDelayedFetchRequestsArgument ++= invocation.getArguments()(0).asInstanceOf[Seq[TopicPartition]]
    )
    val brokerTopicStats = new BrokerTopicStats
    when(replicaManager.brokerTopicStats).thenReturn(brokerTopicStats)

    val replicaQuota: ReplicaQuota = mock(classOf[ReplicaQuota])

    val thread = createReplicaFetcherThread(
      name = "replica-fetcher",
      fetcherId = 0,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = replicaManager,
      quota = replicaQuota,
      leaderEndpointBlockingSend = mockBlockingSend)

    val tp0 = new TopicPartition("testTopic", 0)
    val tp1 = new TopicPartition("testTopic", 1)
    val records = MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord(1000, "foo".getBytes(StandardCharsets.UTF_8)))
    val partitionData = new FetchResponseData.PartitionData()
      .setRecords(records)
      .setHighWatermark(highWatermarkReceivedFromLeader)

    thread.processPartitionData(tp0, 0, partitionData.setPartitionIndex(0))
    thread.processPartitionData(tp1, 0, partitionData.setPartitionIndex(1))
    verify(replicaManager, times(0)).completeDelayedFetchRequests(any[Seq[TopicPartition]])

    thread.doWork()
    if (highWatermarkUpdated) {
      assertEquals(Seq(tp0, tp1), completeDelayedFetchRequestsArgument)
      verify(replicaManager, times(1)).completeDelayedFetchRequests(any[Seq[TopicPartition]])
    } else {
      verify(replicaManager, times(0)).completeDelayedFetchRequests(any[Seq[TopicPartition]])
    }
    assertEquals(mutable.Buffer.empty, thread.partitionsWithNewHighWatermark)
  }

  private def newOffsetForLeaderPartitionResult(
   tp: TopicPartition,
   leaderEpoch: Int,
   endOffset: Long
 ): EpochEndOffset = {
    newOffsetForLeaderPartitionResult(tp, Errors.NONE, leaderEpoch, endOffset)
  }

  private def newOffsetForLeaderPartitionResult(
    tp: TopicPartition,
    error: Errors,
    leaderEpoch: Int,
    endOffset: Long
  ): EpochEndOffset = {
    new EpochEndOffset()
      .setPartition(tp.partition)
      .setErrorCode(error.code)
      .setLeaderEpoch(leaderEpoch)
      .setEndOffset(endOffset)
  }

  private def assertProcessPartitionDataWhen(isReassigning: Boolean): Unit = {
    val props = TestUtils.createBrokerConfig(1)
    val config = KafkaConfig.fromProps(props)

    val mockBlockingSend: BlockingSend = mock(classOf[BlockingSend])
    when(mockBlockingSend.brokerEndPoint()).thenReturn(brokerEndPoint)

    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val records = MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord(1000, "foo".getBytes(StandardCharsets.UTF_8)))
    when(log.maybeUpdateHighWatermark(hw = 0)).thenReturn(None)

    val partition: Partition = mock(classOf[Partition])
    when(partition.localLogOrException).thenReturn(log)
    when(partition.isReassigning).thenReturn(isReassigning)
    when(partition.isAddingLocalReplica).thenReturn(isReassigning)
    when(partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false)).thenReturn(None)

    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getPartitionOrException(any[TopicPartition])).thenReturn(partition)
    val brokerTopicStats = new BrokerTopicStats
    when(replicaManager.brokerTopicStats).thenReturn(brokerTopicStats)

    val replicaQuota: ReplicaQuota = mock(classOf[ReplicaQuota])

    val thread = createReplicaFetcherThread(
      "bob",
      0,
      config,
      failedPartitions,
      replicaManager,
      replicaQuota,
      mockBlockingSend
    )

    val partitionData: thread.FetchData = new FetchResponseData.PartitionData()
      .setPartitionIndex(t1p0.partition)
      .setLastStableOffset(0)
      .setLogStartOffset(0)
      .setRecords(records)
    thread.processPartitionData(t1p0, 0, partitionData)

    if (isReassigning)
      assertEquals(records.sizeInBytes(), brokerTopicStats.allTopicsStats.reassignmentBytesInPerSec.get.count())
    else
      assertEquals(0, brokerTopicStats.allTopicsStats.reassignmentBytesInPerSec.get.count())

    assertEquals(records.sizeInBytes(), brokerTopicStats.allTopicsStats.replicationBytesInRate.get.count())
  }

  def stub(partition: Partition, replicaManager: ReplicaManager, log: UnifiedLog): Unit = {
    when(replicaManager.localLogOrException(t1p0)).thenReturn(log)
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)
    when(replicaManager.localLogOrException(t1p1)).thenReturn(log)
    when(replicaManager.getPartitionOrException(t1p1)).thenReturn(partition)
    when(replicaManager.localLogOrException(t2p1)).thenReturn(log)
    when(replicaManager.getPartitionOrException(t2p1)).thenReturn(partition)
  }
}
