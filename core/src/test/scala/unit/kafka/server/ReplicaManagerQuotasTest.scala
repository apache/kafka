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
import kafka.cluster.{Partition, PartitionTest}
import kafka.log.{LogManager, UnifiedLog}
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils._
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams}
import org.apache.kafka.server.util.{KafkaScheduler, MockTime}
import org.apache.kafka.storage.internals.log.{FetchDataInfo, LogConfig, LogDirFailureChannel, LogOffsetMetadata, LogOffsetSnapshot}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt, anyLong}
import org.mockito.Mockito.{mock, when}
import org.mockito.{AdditionalMatchers, ArgumentMatchers}

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

    val fetchParams = PartitionTest.followerFetchParams(followerReplicaId)
    val fetch = replicaManager.readFromLog(fetchParams, fetchInfo, quota, readFromPurgatory = false)
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

    val fetchParams = PartitionTest.followerFetchParams(followerReplicaId)
    val fetch = replicaManager.readFromLog(fetchParams, fetchInfo, quota, readFromPurgatory = false)
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

    val fetchParams = PartitionTest.followerFetchParams(followerReplicaId)
    val fetch = replicaManager.readFromLog(fetchParams, fetchInfo, quota, readFromPurgatory = false)
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

    val fetchParams = PartitionTest.followerFetchParams(followerReplicaId)
    val fetch = replicaManager.readFromLog(fetchParams, fetchInfo, quota, readFromPurgatory = false)
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

    val fetchParams = PartitionTest.consumerFetchParams()
    val fetch = replicaManager.readFromLog(fetchParams, fetchInfo, quota, readFromPurgatory = false).toMap
    assertEquals(1, fetch(topicIdPartition1).info.records.batches.asScala.size,
      "Replication throttled partitions should return data for consumer fetch")
    assertEquals(1, fetch(topicIdPartition2).info.records.batches.asScala.size,
      "Replication throttled partitions should return data for consumer fetch")
  }

  @Test
  def testCompleteInDelayedFetchWithReplicaThrottling(): Unit = {
    // Set up DelayedFetch where there is data to return to a follower replica, either in-sync or out of sync
    def setupDelayedFetch(isReplicaInSync: Boolean): DelayedFetch = {
      val endOffsetMetadata = new LogOffsetMetadata(100L, 0L, 500)
      val partition: Partition = mock(classOf[Partition])

      val offsetSnapshot = new LogOffsetSnapshot(
        0L,
        endOffsetMetadata,
        endOffsetMetadata,
        endOffsetMetadata)
      when(partition.fetchOffsetSnapshot(Optional.empty(), fetchOnlyFromLeader = true))
          .thenReturn(offsetSnapshot)

      val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
      when(replicaManager.getPartitionOrException(any[TopicPartition]))
        .thenReturn(partition)

      when(replicaManager.shouldLeaderThrottle(any[ReplicaQuota], any[Partition], anyInt))
        .thenReturn(!isReplicaInSync)
      when(partition.getReplica(1)).thenReturn(None)

      val tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("t1", 0))
      val fetchPartitionStatus = FetchPartitionStatus(
        new LogOffsetMetadata(50L, 0L, 250),
        new PartitionData(Uuid.ZERO_UUID, 50, 0, 1, Optional.empty()))
      val fetchParams = new FetchParams(
        ApiKeys.FETCH.latestVersion,
        1,
        1,
        600,
        1,
        1000,
        FetchIsolation.LOG_END,
        Optional.empty()
      )

      new DelayedFetch(
        params = fetchParams,
        fetchPartitionStatus = Seq(tp -> fetchPartitionStatus),
        replicaManager = replicaManager,
        quota = null,
        responseCallback = null
      ) {
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
        new LogOffsetMetadata(100L, 0L, 500)
      else
        new LogOffsetMetadata(150L, 50L, 500)
      val partition: Partition = mock(classOf[Partition])

      val offsetSnapshot = new LogOffsetSnapshot(
        0L,
        endOffsetMetadata,
        endOffsetMetadata,
        endOffsetMetadata)
      when(partition.fetchOffsetSnapshot(Optional.empty(), fetchOnlyFromLeader = true))
        .thenReturn(offsetSnapshot)

      val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
      when(replicaManager.getPartitionOrException(any[TopicPartition]))
        .thenReturn(partition)

      val tidp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("t1", 0))
      val fetchPartitionStatus = FetchPartitionStatus(
        new LogOffsetMetadata(50L, 0L, 250),
        new PartitionData(Uuid.ZERO_UUID, 50, 0, 1, Optional.empty()))
      val fetchParams = new FetchParams(
        ApiKeys.FETCH.latestVersion,
        FetchRequest.CONSUMER_REPLICA_ID,
        -1,
        600L,
        1,
        1000,
        FetchIsolation.HIGH_WATERMARK,
        Optional.empty()
      )

      new DelayedFetch(
        params = fetchParams,
        fetchPartitionStatus = Seq(tidp -> fetchPartitionStatus),
        replicaManager = replicaManager,
        quota = null,
        responseCallback = null
      ) {
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
    when(log.logEndOffsetMetadata).thenReturn(new LogOffsetMetadata(20L))
    when(log.topicId).thenReturn(Some(topicId))
    when(log.config).thenReturn(new LogConfig(Collections.emptyMap()))

    //if we ask for len 1 return a message
    when(log.read(anyLong,
      maxLength = AdditionalMatchers.geq(1),
      isolation = any[FetchIsolation],
      minOneMessage = anyBoolean)).thenReturn(
      new FetchDataInfo(
        new LogOffsetMetadata(0L, 0L, 0),
        MemoryRecords.withRecords(Compression.NONE, record)
      ))

    //if we ask for len = 0, return 0 messages
    when(log.read(anyLong,
      maxLength = ArgumentMatchers.eq(0),
      isolation = any[FetchIsolation],
      minOneMessage = anyBoolean)).thenReturn(
      new FetchDataInfo(
        new LogOffsetMetadata(0L, 0L, 0),
        MemoryRecords.EMPTY
      ))

    when(log.maybeIncrementHighWatermark(
      any[LogOffsetMetadata]
    )).thenReturn(None)

    //Create log manager
    val logManager: LogManager = mock(classOf[LogManager])

    //Return the same log for each partition as it doesn't matter
    when(logManager.getLog(any[TopicPartition], anyBoolean)).thenReturn(Some(log))
    when(logManager.liveLogDirs).thenReturn(Array.empty[File])

    val alterIsrManager: AlterPartitionManager = mock(classOf[AlterPartitionManager])

    val leaderBrokerId = configs.head.brokerId
    quotaManager = QuotaFactory.instantiate(configs.head, metrics, time, "")
    replicaManager = new ReplicaManager(
      metrics = metrics,
      config = configs.head,
      time = time,
      scheduler = scheduler,
      logManager = logManager,
      quotaManagers = quotaManager,
      metadataCache = MetadataCache.zkMetadataCache(leaderBrokerId, configs.head.interBrokerProtocolVersion),
      logDirFailureChannel = new LogDirFailureChannel(configs.head.logDirs.size),
      alterPartitionManager = alterIsrManager)

    //create the two replicas
    for ((p, _) <- fetchInfo) {
      val partition = replicaManager.createPartition(p.topicPartition)
      log.updateHighWatermark(5)
      partition.leaderReplicaIdOpt = Some(leaderBrokerId)
      partition.setLog(log, isFutureLog = false)

      partition.updateAssignmentAndIsr(
        replicas = Seq(leaderBrokerId, configs.last.brokerId),
        isLeader = true,
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
