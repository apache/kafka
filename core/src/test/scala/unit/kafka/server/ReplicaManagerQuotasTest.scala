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

import java.io.File
import java.util.{Collections, Optional, Properties}
import kafka.cluster.Partition
import kafka.log.{LogManager, LogOffsetSnapshot, UnifiedLog}
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.metadata.LeaderRecoveryState
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.{AdditionalMatchers, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt, anyLong}
import org.mockito.Mockito.{mock, when}

import scala.jdk.CollectionConverters._

class ReplicaManagerQuotasTest {
  val configs = TestUtils.createBrokerConfigs(2, TestUtils.MockZkConnect).map(KafkaConfig.fromProps(_, new Properties()))
  val time = new MockTime
  val metrics = new Metrics
  val record = new SimpleRecord("some-data-in-a-message".getBytes())
  val topicPartition1 = new TopicPartition("test-topic", 1)
  val topicPartition2 = new TopicPartition("test-topic", 2)
  val topicId = Uuid.randomUuid()
  val topicIds = Collections.singletonMap("test-topic", topicId)
  val topicIdPartition1 = new TopicIdPartition(topicId, topicPartition1)
  val topicIdPartition2 = new TopicIdPartition(topicId, topicPartition2)
  val fetchInfo = Seq(
    topicIdPartition1 -> new PartitionData(Uuid.ZERO_UUID, 0, 0, 100, Optional.empty()),
    topicIdPartition2 -> new PartitionData(Uuid.ZERO_UUID, 0, 0, 100, Optional.empty()))
  var quotaManager: QuotaManagers = _
  var replicaManager: ReplicaManager = _

  @Test
  def shouldExcludeSubsequentThrottledPartitions(): Unit = {
    setUpMocks(fetchInfo)
    val followerReplicaId = configs.last.brokerId

    val quota = mockQuota()
    when(quota.isQuotaExceeded)
      .thenReturn(false)
      .thenReturn(true)

    val fetch = replicaManager.readFromLocalLog(
      replicaId = followerReplicaId,
      fetchOnlyFromLeader = true,
      fetchIsolation = FetchHighWatermark,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      readPartitionInfo = fetchInfo,
      quota = quota,
      clientMetadata = None)
    assertEquals(1, fetch.find(_._1 == topicIdPartition1).get._2.info.records.batches.asScala.size,
      "Given two partitions, with only one throttled, we should get the first")

    assertEquals(0, fetch.find(_._1 == topicIdPartition2).get._2.info.records.batches.asScala.size,
      "But we shouldn't get the second")
  }

  @Test
  def shouldGetNoMessagesIfQuotasExceededOnSubsequentPartitions(): Unit = {
    setUpMocks(fetchInfo)
    val followerReplicaId = configs.last.brokerId

    val quota = mockQuota()
    when(quota.isQuotaExceeded)
      .thenReturn(true)
      .thenReturn(true)

    val fetch = replicaManager.readFromLocalLog(
      replicaId = followerReplicaId,
      fetchOnlyFromLeader = true,
      fetchIsolation = FetchHighWatermark,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      readPartitionInfo = fetchInfo,
      quota = quota,
      clientMetadata = None)
    assertEquals(0, fetch.find(_._1 == topicIdPartition1).get._2.info.records.batches.asScala.size,
      "Given two partitions, with both throttled, we should get no messages")
    assertEquals(0, fetch.find(_._1 == topicIdPartition2).get._2.info.records.batches.asScala.size,
      "Given two partitions, with both throttled, we should get no messages")
  }

  @Test
  def shouldGetBothMessagesIfQuotasAllow(): Unit = {
    setUpMocks(fetchInfo)
    val followerReplicaId = configs.last.brokerId

    val quota = mockQuota()
    when(quota.isQuotaExceeded)
      .thenReturn(false)
      .thenReturn(false)

    val fetch = replicaManager.readFromLocalLog(
      replicaId = followerReplicaId,
      fetchOnlyFromLeader = true,
      fetchIsolation = FetchHighWatermark,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      readPartitionInfo = fetchInfo,
      quota = quota,
      clientMetadata = None)
    assertEquals(1, fetch.find(_._1 == topicIdPartition1).get._2.info.records.batches.asScala.size,
      "Given two partitions, with both non-throttled, we should get both messages")
    assertEquals(1, fetch.find(_._1 == topicIdPartition2).get._2.info.records.batches.asScala.size,
      "Given two partitions, with both non-throttled, we should get both messages")
  }

  @Test
  def shouldIncludeInSyncThrottledReplicas(): Unit = {
    setUpMocks(fetchInfo, bothReplicasInSync = true)
    val followerReplicaId = configs.last.brokerId

    val quota = mockQuota()
    when(quota.isQuotaExceeded)
      .thenReturn(false)
      .thenReturn(true)

    val fetch = replicaManager.readFromLocalLog(
      replicaId = followerReplicaId,
      fetchOnlyFromLeader = true,
      fetchIsolation = FetchHighWatermark,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      readPartitionInfo = fetchInfo,
      quota = quota,
      clientMetadata = None)
    assertEquals(1, fetch.find(_._1 == topicIdPartition1).get._2.info.records.batches.asScala.size,
      "Given two partitions, with only one throttled, we should get the first")

    assertEquals(1, fetch.find(_._1 == topicIdPartition2).get._2.info.records.batches.asScala.size,
      "But we should get the second too since it's throttled but in sync")
  }

  @Test
  def shouldIncludeThrottledReplicasForConsumerFetch(): Unit = {
    setUpMocks(fetchInfo)

    val quota = mockQuota()
    when(quota.isQuotaExceeded).thenReturn(true)

    val fetch = replicaManager.readFromLocalLog(
      replicaId = FetchRequest.CONSUMER_REPLICA_ID,
      fetchOnlyFromLeader = true,
      fetchIsolation = FetchHighWatermark,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      readPartitionInfo = fetchInfo,
      quota = quota,
      clientMetadata = None).toMap

    assertEquals(1, fetch(topicIdPartition1).info.records.batches.asScala.size,
      "Replication throttled partitions should return data for consumer fetch")

    assertEquals(1, fetch(topicIdPartition2).info.records.batches.asScala.size,
      "Replication throttled partitions should return data for consumer fetch")
  }

  @Test
  def testCompleteInDelayedFetchWithReplicaThrottling(): Unit = {
    // Set up DelayedFetch where there is data to return to a follower replica, either in-sync or out of sync
    def setupDelayedFetch(isReplicaInSync: Boolean): DelayedFetch = {
      val endOffsetMetadata = LogOffsetMetadata(messageOffset = 100L, segmentBaseOffset = 0L, relativePositionInSegment = 500)
      val partition: Partition = mock(classOf[Partition])

      val offsetSnapshot = LogOffsetSnapshot(
        logStartOffset = 0L,
        logEndOffset = endOffsetMetadata,
        highWatermark = endOffsetMetadata,
        lastStableOffset = endOffsetMetadata)
      when(partition.fetchOffsetSnapshot(Optional.empty(), fetchOnlyFromLeader = true))
          .thenReturn(offsetSnapshot)

      val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
      when(replicaManager.getPartitionOrException(any[TopicPartition]))
        .thenReturn(partition)

      when(replicaManager.shouldLeaderThrottle(any[ReplicaQuota], any[Partition], anyInt))
        .thenReturn(!isReplicaInSync)
      when(partition.getReplica(1)).thenReturn(None)

      val tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("t1", 0))
      val fetchPartitionStatus = FetchPartitionStatus(LogOffsetMetadata(messageOffset = 50L, segmentBaseOffset = 0L,
         relativePositionInSegment = 250), new PartitionData(Uuid.ZERO_UUID, 50, 0, 1, Optional.empty()))
      val fetchMetadata = FetchMetadata(fetchMinBytes = 1,
        fetchMaxBytes = 1000,
        hardMaxBytesLimit = true,
        fetchOnlyLeader = true,
        fetchIsolation = FetchLogEnd,
        isFromFollower = true,
        replicaId = 1,
        fetchPartitionStatus = List((tp, fetchPartitionStatus))
      )
      new DelayedFetch(delayMs = 600, fetchMetadata = fetchMetadata, replicaManager = replicaManager,
        quota = null, clientMetadata = None, responseCallback = null) {
        override def forceComplete(): Boolean = true
      }
    }

    assertTrue(setupDelayedFetch(isReplicaInSync = true).tryComplete(), "In sync replica should complete")
    assertFalse(setupDelayedFetch(isReplicaInSync = false).tryComplete(), "Out of sync replica should not complete")
  }

  @Test
  def testCompleteInDelayedFetchConsumerFetch(): Unit = {
    // Set up DelayedFetch where there is data to return to a consumer, either for the current segment or an older segment
    def setupDelayedFetch(isFetchFromOlderSegment: Boolean): DelayedFetch = {
      val endOffsetMetadata = if (isFetchFromOlderSegment)
        LogOffsetMetadata(messageOffset = 100L, segmentBaseOffset = 0L, relativePositionInSegment = 500)
      else
        LogOffsetMetadata(messageOffset = 150L, segmentBaseOffset = 50L, relativePositionInSegment = 500)
      val partition: Partition = mock(classOf[Partition])

      val offsetSnapshot = LogOffsetSnapshot(
        logStartOffset = 0L,
        logEndOffset = endOffsetMetadata,
        highWatermark = endOffsetMetadata,
        lastStableOffset = endOffsetMetadata)
      when(partition.fetchOffsetSnapshot(Optional.empty(), fetchOnlyFromLeader = true))
        .thenReturn(offsetSnapshot)

      val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
      when(replicaManager.getPartitionOrException(any[TopicPartition]))
        .thenReturn(partition)

      val tidp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("t1", 0))
      val fetchPartitionStatus = FetchPartitionStatus(LogOffsetMetadata(messageOffset = 50L, segmentBaseOffset = 0L,
        relativePositionInSegment = 250), new PartitionData(Uuid.ZERO_UUID, 50, 0, 1, Optional.empty()))
      val fetchMetadata = FetchMetadata(fetchMinBytes = 1,
        fetchMaxBytes = 1000,
        hardMaxBytesLimit = true,
        fetchOnlyLeader = true,
        fetchIsolation = FetchLogEnd,
        isFromFollower = false,
        replicaId = FetchRequest.CONSUMER_REPLICA_ID,
        fetchPartitionStatus = List((tidp, fetchPartitionStatus))
      )
      new DelayedFetch(delayMs = 600, fetchMetadata = fetchMetadata, replicaManager = replicaManager,
        quota = null, clientMetadata = None, responseCallback = null) {
        override def forceComplete(): Boolean = true
      }
    }

    assertTrue(setupDelayedFetch(isFetchFromOlderSegment = false).tryComplete(), "Consumer fetch replica should complete if reading from current segment")
    assertTrue(setupDelayedFetch(isFetchFromOlderSegment = true).tryComplete(), "Consumer fetch replica should complete if reading from older segment")
  }

  def setUpMocks(fetchInfo: Seq[(TopicIdPartition, PartitionData)], record: SimpleRecord = this.record,
                 bothReplicasInSync: Boolean = false): Unit = {
    val scheduler: KafkaScheduler = mock(classOf[KafkaScheduler])

    //Create log which handles both a regular read and a 0 bytes read
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    when(log.logStartOffset).thenReturn(0L)
    when(log.logEndOffset).thenReturn(20L)
    when(log.highWatermark).thenReturn(5)
    when(log.lastStableOffset).thenReturn(5)
    when(log.logEndOffsetMetadata).thenReturn(LogOffsetMetadata(20L))
    when(log.topicId).thenReturn(Some(topicId))

    //if we ask for len 1 return a message
    when(log.read(anyLong,
      maxLength = AdditionalMatchers.geq(1),
      isolation = any[FetchIsolation],
      minOneMessage = anyBoolean)).thenReturn(
      FetchDataInfo(
        LogOffsetMetadata(0L, 0L, 0),
        MemoryRecords.withRecords(CompressionType.NONE, record)
      ))

    //if we ask for len = 0, return 0 messages
    when(log.read(anyLong,
      maxLength = ArgumentMatchers.eq(0),
      isolation = any[FetchIsolation],
      minOneMessage = anyBoolean)).thenReturn(
      FetchDataInfo(
        LogOffsetMetadata(0L, 0L, 0),
        MemoryRecords.EMPTY
      ))

    //Create log manager
    val logManager: LogManager = mock(classOf[LogManager])

    //Return the same log for each partition as it doesn't matter
    when(logManager.getLog(any[TopicPartition], anyBoolean)).thenReturn(Some(log))
    when(logManager.liveLogDirs).thenReturn(Array.empty[File])

    val alterIsrManager: AlterIsrManager = mock(classOf[AlterIsrManager])

    val leaderBrokerId = configs.head.brokerId
    quotaManager = QuotaFactory.instantiate(configs.head, metrics, time, "")
    replicaManager = new ReplicaManager(
      metrics = metrics,
      config = configs.head,
      time = time,
      scheduler = scheduler,
      logManager = logManager,
      quotaManagers = quotaManager,
      metadataCache = MetadataCache.zkMetadataCache(leaderBrokerId),
      logDirFailureChannel = new LogDirFailureChannel(configs.head.logDirs.size),
      alterIsrManager = alterIsrManager)

    //create the two replicas
    for ((p, _) <- fetchInfo) {
      val partition = replicaManager.createPartition(p.topicPartition)
      log.updateHighWatermark(5)
      partition.leaderReplicaIdOpt = Some(leaderBrokerId)
      partition.setLog(log, isFutureLog = false)

      partition.updateAssignmentAndIsr(
        assignment = Seq(leaderBrokerId, configs.last.brokerId),
        isr = if (bothReplicasInSync) Set(leaderBrokerId, configs.last.brokerId) else Set(leaderBrokerId),
        addingReplicas = Seq.empty,
        removingReplicas = Seq.empty,
        leaderRecoveryState = LeaderRecoveryState.RECOVERED
      )
    }
  }

  @AfterEach
  def tearDown(): Unit = {
    Option(replicaManager).foreach(_.shutdown(false))
    Option(quotaManager).foreach(_.shutdown())
    metrics.close()
  }

  def mockQuota(): ReplicaQuota = {
    val quota: ReplicaQuota = mock(classOf[ReplicaQuota])
    when(quota.isThrottled(any[TopicPartition])).thenReturn(true)
    quota
  }
}
