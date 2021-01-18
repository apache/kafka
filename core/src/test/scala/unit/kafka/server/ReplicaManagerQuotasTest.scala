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
import java.util.{Optional, Properties}
import java.util.concurrent.atomic.AtomicBoolean

import kafka.cluster.Partition
import kafka.log.{Log, LogManager, LogOffsetSnapshot}
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.easymock.EasyMock
import EasyMock._
import kafka.server.QuotaFactory.QuotaManagers
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}

import scala.jdk.CollectionConverters._

class ReplicaManagerQuotasTest {
  val configs = TestUtils.createBrokerConfigs(2, TestUtils.MockZkConnect).map(KafkaConfig.fromProps(_, new Properties()))
  val time = new MockTime
  val metrics = new Metrics
  val record = new SimpleRecord("some-data-in-a-message".getBytes())
  val topicPartition1 = new TopicPartition("test-topic", 1)
  val topicPartition2 = new TopicPartition("test-topic", 2)
  val fetchInfo = Seq(
    topicPartition1 -> new PartitionData(0, 0, 100, Optional.empty()),
    topicPartition2 -> new PartitionData(0, 0, 100, Optional.empty()))
  var quotaManager: QuotaManagers = _
  var replicaManager: ReplicaManager = _

  @Test
  def shouldExcludeSubsequentThrottledPartitions(): Unit = {
    setUpMocks(fetchInfo)
    val followerReplicaId = configs.last.brokerId

    val quota = mockQuota(1000000)
    expect(quota.isQuotaExceeded).andReturn(false).once()
    expect(quota.isQuotaExceeded).andReturn(true).once()
    replay(quota)

    val fetch = replicaManager.readFromLocalLog(
      replicaId = followerReplicaId,
      fetchOnlyFromLeader = true,
      fetchIsolation = FetchHighWatermark,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      readPartitionInfo = fetchInfo,
      quota = quota,
      clientMetadata = None)
    assertEquals(1, fetch.find(_._1 == topicPartition1).get._2.info.records.batches.asScala.size,
      "Given two partitions, with only one throttled, we should get the first")

    assertEquals(0, fetch.find(_._1 == topicPartition2).get._2.info.records.batches.asScala.size,
      "But we shouldn't get the second")
  }

  @Test
  def shouldGetNoMessagesIfQuotasExceededOnSubsequentPartitions(): Unit = {
    setUpMocks(fetchInfo)
    val followerReplicaId = configs.last.brokerId

    val quota = mockQuota(1000000)
    expect(quota.isQuotaExceeded).andReturn(true).once()
    expect(quota.isQuotaExceeded).andReturn(true).once()
    replay(quota)

    val fetch = replicaManager.readFromLocalLog(
      replicaId = followerReplicaId,
      fetchOnlyFromLeader = true,
      fetchIsolation = FetchHighWatermark,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      readPartitionInfo = fetchInfo,
      quota = quota,
      clientMetadata = None)
    assertEquals(0, fetch.find(_._1 == topicPartition1).get._2.info.records.batches.asScala.size,
      "Given two partitions, with both throttled, we should get no messages")
    assertEquals(0, fetch.find(_._1 == topicPartition2).get._2.info.records.batches.asScala.size,
      "Given two partitions, with both throttled, we should get no messages")
  }

  @Test
  def shouldGetBothMessagesIfQuotasAllow(): Unit = {
    setUpMocks(fetchInfo)
    val followerReplicaId = configs.last.brokerId

    val quota = mockQuota(1000000)
    expect(quota.isQuotaExceeded).andReturn(false).once()
    expect(quota.isQuotaExceeded).andReturn(false).once()
    replay(quota)

    val fetch = replicaManager.readFromLocalLog(
      replicaId = followerReplicaId,
      fetchOnlyFromLeader = true,
      fetchIsolation = FetchHighWatermark,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      readPartitionInfo = fetchInfo,
      quota = quota,
      clientMetadata = None)
    assertEquals(1, fetch.find(_._1 == topicPartition1).get._2.info.records.batches.asScala.size,
      "Given two partitions, with both non-throttled, we should get both messages")
    assertEquals(1, fetch.find(_._1 == topicPartition2).get._2.info.records.batches.asScala.size,
      "Given two partitions, with both non-throttled, we should get both messages")
  }

  @Test
  def shouldIncludeInSyncThrottledReplicas(): Unit = {
    setUpMocks(fetchInfo, bothReplicasInSync = true)
    val followerReplicaId = configs.last.brokerId

    val quota = mockQuota(1000000)
    expect(quota.isQuotaExceeded).andReturn(false).once()
    expect(quota.isQuotaExceeded).andReturn(true).once()
    replay(quota)

    val fetch = replicaManager.readFromLocalLog(
      replicaId = followerReplicaId,
      fetchOnlyFromLeader = true,
      fetchIsolation = FetchHighWatermark,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      readPartitionInfo = fetchInfo,
      quota = quota,
      clientMetadata = None)
    assertEquals(1, fetch.find(_._1 == topicPartition1).get._2.info.records.batches.asScala.size,
      "Given two partitions, with only one throttled, we should get the first")

    assertEquals(1, fetch.find(_._1 == topicPartition2).get._2.info.records.batches.asScala.size,
      "But we should get the second too since it's throttled but in sync")
  }

  @Test
  def testCompleteInDelayedFetchWithReplicaThrottling(): Unit = {
    // Set up DelayedFetch where there is data to return to a follower replica, either in-sync or out of sync
    def setupDelayedFetch(isReplicaInSync: Boolean): DelayedFetch = {
      val endOffsetMetadata = LogOffsetMetadata(messageOffset = 100L, segmentBaseOffset = 0L, relativePositionInSegment = 500)
      val partition: Partition = EasyMock.createMock(classOf[Partition])

      val offsetSnapshot = LogOffsetSnapshot(
        logStartOffset = 0L,
        logEndOffset = endOffsetMetadata,
        highWatermark = endOffsetMetadata,
        lastStableOffset = endOffsetMetadata)
      EasyMock.expect(partition.fetchOffsetSnapshot(Optional.empty(), fetchOnlyFromLeader = true))
          .andReturn(offsetSnapshot)

      val replicaManager: ReplicaManager = EasyMock.createMock(classOf[ReplicaManager])
      EasyMock.expect(replicaManager.getPartitionOrException(EasyMock.anyObject[TopicPartition]))
        .andReturn(partition).anyTimes()

      EasyMock.expect(replicaManager.shouldLeaderThrottle(EasyMock.anyObject[ReplicaQuota], EasyMock.anyObject[Partition], EasyMock.anyObject[Int]))
        .andReturn(!isReplicaInSync).anyTimes()
      EasyMock.expect(partition.getReplica(1)).andReturn(None)
      EasyMock.replay(replicaManager, partition)

      val tp = new TopicPartition("t1", 0)
      val fetchPartitionStatus = FetchPartitionStatus(LogOffsetMetadata(messageOffset = 50L, segmentBaseOffset = 0L,
         relativePositionInSegment = 250), new PartitionData(50, 0, 1, Optional.empty()))
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

  def setUpMocks(fetchInfo: Seq[(TopicPartition, PartitionData)], record: SimpleRecord = this.record,
                 bothReplicasInSync: Boolean = false): Unit = {
    val zkClient: KafkaZkClient = EasyMock.createMock(classOf[KafkaZkClient])
    val scheduler: KafkaScheduler = createNiceMock(classOf[KafkaScheduler])

    //Create log which handles both a regular read and a 0 bytes read
    val log: Log = createNiceMock(classOf[Log])
    expect(log.logStartOffset).andReturn(0L).anyTimes()
    expect(log.logEndOffset).andReturn(20L).anyTimes()
    expect(log.highWatermark).andReturn(5).anyTimes()
    expect(log.lastStableOffset).andReturn(5).anyTimes()
    expect(log.logEndOffsetMetadata).andReturn(LogOffsetMetadata(20L)).anyTimes()

    //if we ask for len 1 return a message
    expect(log.read(anyObject(),
      maxLength = geq(1),
      isolation = anyObject(),
      minOneMessage = anyBoolean())).andReturn(
      FetchDataInfo(
        LogOffsetMetadata(0L, 0L, 0),
        MemoryRecords.withRecords(CompressionType.NONE, record)
      )).anyTimes()

    //if we ask for len = 0, return 0 messages
    expect(log.read(anyObject(),
      maxLength = EasyMock.eq(0),
      isolation = anyObject(),
      minOneMessage = anyBoolean())).andReturn(
      FetchDataInfo(
        LogOffsetMetadata(0L, 0L, 0),
        MemoryRecords.EMPTY
      )).anyTimes()
    replay(log)

    //Create log manager
    val logManager: LogManager = createMock(classOf[LogManager])

    //Return the same log for each partition as it doesn't matter
    expect(logManager.getLog(anyObject(), anyBoolean())).andReturn(Some(log)).anyTimes()
    expect(logManager.liveLogDirs).andReturn(Array.empty[File]).anyTimes()
    replay(logManager)

    val alterIsrManager: AlterIsrManager = createMock(classOf[AlterIsrManager])

    val leaderBrokerId = configs.head.brokerId
    quotaManager = QuotaFactory.instantiate(configs.head, metrics, time, "")
    replicaManager = new ReplicaManager(configs.head, metrics, time, zkClient, scheduler, logManager,
      new AtomicBoolean(false), quotaManager,
      new BrokerTopicStats, new MetadataCache(leaderBrokerId), new LogDirFailureChannel(configs.head.logDirs.size), alterIsrManager)

    //create the two replicas
    for ((p, _) <- fetchInfo) {
      val partition = replicaManager.createPartition(p)
      log.updateHighWatermark(5)
      partition.leaderReplicaIdOpt = Some(leaderBrokerId)
      partition.setLog(log, isFutureLog = false)

      partition.updateAssignmentAndIsr(
        assignment = Seq(leaderBrokerId, configs.last.brokerId),
        isr = if (bothReplicasInSync) Set(leaderBrokerId, configs.last.brokerId) else Set(leaderBrokerId),
        addingReplicas = Seq.empty,
        removingReplicas = Seq.empty
      )
    }
  }

  @AfterEach
  def tearDown(): Unit = {
    Option(replicaManager).foreach(_.shutdown(false))
    Option(quotaManager).foreach(_.shutdown())
    metrics.close()
  }

  def mockQuota(bound: Long): ReplicaQuota = {
    val quota: ReplicaQuota = createMock(classOf[ReplicaQuota])
    expect(quota.isThrottled(anyObject())).andReturn(true).anyTimes()
    quota
  }
}
