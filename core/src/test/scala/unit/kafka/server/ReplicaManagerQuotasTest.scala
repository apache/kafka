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

import kafka.cluster.{Partition, Replica}
import kafka.log.{Log, LogManager, LogOffsetSnapshot}
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.easymock.EasyMock
import EasyMock._
import org.junit.Assert._
import org.junit.{After, Test}

import scala.collection.JavaConverters._

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
      quota = quota)
    assertEquals("Given two partitions, with only one throttled, we should get the first", 1,
      fetch.find(_._1 == topicPartition1).get._2.info.records.batches.asScala.size)

    assertEquals("But we shouldn't get the second", 0,
      fetch.find(_._1 == topicPartition2).get._2.info.records.batches.asScala.size)
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
      quota = quota)
    assertEquals("Given two partitions, with both throttled, we should get no messages", 0,
      fetch.find(_._1 == topicPartition1).get._2.info.records.batches.asScala.size)
    assertEquals("Given two partitions, with both throttled, we should get no messages", 0,
      fetch.find(_._1 == topicPartition2).get._2.info.records.batches.asScala.size)
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
      quota = quota)
    assertEquals("Given two partitions, with both non-throttled, we should get both messages", 1,
      fetch.find(_._1 == topicPartition1).get._2.info.records.batches.asScala.size)
    assertEquals("Given two partitions, with both non-throttled, we should get both messages", 1,
      fetch.find(_._1 == topicPartition2).get._2.info.records.batches.asScala.size)
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
      quota = quota)
    assertEquals("Given two partitions, with only one throttled, we should get the first", 1,
      fetch.find(_._1 == topicPartition1).get._2.info.records.batches.asScala.size)

    assertEquals("But we should get the second too since it's throttled but in sync", 1,
      fetch.find(_._1 == topicPartition2).get._2.info.records.batches.asScala.size)
  }

  @Test
  def testCompleteInDelayedFetchWithReplicaThrottling(): Unit = {
    // Set up DelayedFetch where there is data to return to a follower replica, either in-sync or out of sync
    def setupDelayedFetch(isReplicaInSync: Boolean): DelayedFetch = {
      val endOffsetMetadata = new LogOffsetMetadata(messageOffset = 100L, segmentBaseOffset = 0L, relativePositionInSegment = 500)
      val partition: Partition = EasyMock.createMock(classOf[Partition])

      val offsetSnapshot = LogOffsetSnapshot(
        logStartOffset = 0L,
        logEndOffset = endOffsetMetadata,
        highWatermark = endOffsetMetadata,
        lastStableOffset = endOffsetMetadata)
      EasyMock.expect(partition.fetchOffsetSnapshot(Optional.empty(), fetchOnlyFromLeader = true))
          .andReturn(offsetSnapshot)

      val replicaManager: ReplicaManager = EasyMock.createMock(classOf[ReplicaManager])
      EasyMock.expect(replicaManager.getPartitionOrException(
        EasyMock.anyObject[TopicPartition], EasyMock.anyBoolean()))
        .andReturn(partition).anyTimes()

      EasyMock.expect(replicaManager.shouldLeaderThrottle(EasyMock.anyObject[ReplicaQuota], EasyMock.anyObject[TopicPartition], EasyMock.anyObject[Int]))
        .andReturn(!isReplicaInSync).anyTimes()
      EasyMock.replay(replicaManager, partition)

      val tp = new TopicPartition("t1", 0)
      val fetchPartitionStatus = FetchPartitionStatus(new LogOffsetMetadata(messageOffset = 50L, segmentBaseOffset = 0L,
         relativePositionInSegment = 250), new PartitionData(50, 0, 1, Optional.empty()))
      val fetchMetadata = FetchMetadata(fetchMinBytes = 1,
        fetchMaxBytes = 1000,
        hardMaxBytesLimit = true,
        fetchOnlyLeader = true,
        fetchIsolation = FetchLogEnd,
        isFromFollower = true,
        replicaId = 1,
        fetchPartitionStatus = List((tp, fetchPartitionStatus)))
      new DelayedFetch(delayMs = 600, fetchMetadata = fetchMetadata, replicaManager = replicaManager,
        quota = null, responseCallback = null) {
        override def forceComplete(): Boolean = true
      }
    }

    assertTrue("In sync replica should complete", setupDelayedFetch(isReplicaInSync = true).tryComplete())
    assertFalse("Out of sync replica should not complete", setupDelayedFetch(isReplicaInSync = false).tryComplete())
  }

  def setUpMocks(fetchInfo: Seq[(TopicPartition, PartitionData)], record: SimpleRecord = this.record, bothReplicasInSync: Boolean = false) {
    val zkClient: KafkaZkClient = EasyMock.createMock(classOf[KafkaZkClient])
    val scheduler: KafkaScheduler = createNiceMock(classOf[KafkaScheduler])

    //Create log which handles both a regular read and a 0 bytes read
    val log: Log = createNiceMock(classOf[Log])
    expect(log.logStartOffset).andReturn(0L).anyTimes()
    expect(log.logEndOffset).andReturn(20L).anyTimes()
    expect(log.logEndOffsetMetadata).andReturn(new LogOffsetMetadata(20L)).anyTimes()

    //if we ask for len 1 return a message
    expect(log.read(anyObject(),
      maxLength = geq(1),
      maxOffset = anyObject(),
      minOneMessage = anyBoolean(),
      includeAbortedTxns = EasyMock.eq(false))).andReturn(
      FetchDataInfo(
        new LogOffsetMetadata(0L, 0L, 0),
        MemoryRecords.withRecords(CompressionType.NONE, record)
      )).anyTimes()

    //if we ask for len = 0, return 0 messages
    expect(log.read(anyObject(),
      maxLength = EasyMock.eq(0),
      maxOffset = anyObject(),
      minOneMessage = anyBoolean(),
      includeAbortedTxns = EasyMock.eq(false))).andReturn(
      FetchDataInfo(
        new LogOffsetMetadata(0L, 0L, 0),
        MemoryRecords.EMPTY
      )).anyTimes()
    replay(log)

    //Create log manager
    val logManager: LogManager = createMock(classOf[LogManager])

    //Return the same log for each partition as it doesn't matter
    expect(logManager.getLog(anyObject(), anyBoolean())).andReturn(Some(log)).anyTimes()
    expect(logManager.liveLogDirs).andReturn(Array.empty[File]).anyTimes()
    replay(logManager)

    replicaManager = new ReplicaManager(configs.head, metrics, time, zkClient, scheduler, logManager,
      new AtomicBoolean(false), QuotaFactory.instantiate(configs.head, metrics, time, ""),
      new BrokerTopicStats, new MetadataCache(configs.head.brokerId), new LogDirFailureChannel(configs.head.logDirs.size))

    //create the two replicas
    for ((p, _) <- fetchInfo) {
      val partition = replicaManager.getOrCreatePartition(p)
      val leaderReplica = new Replica(configs.head.brokerId, p, time, 0, Some(log))
      leaderReplica.highWatermark = new LogOffsetMetadata(5)
      partition.leaderReplicaIdOpt = Some(leaderReplica.brokerId)
      val followerReplica = new Replica(configs.last.brokerId, p, time, 0, Some(log))
      val allReplicas = Set(leaderReplica, followerReplica)
      allReplicas.foreach(partition.addReplicaIfNotExists)
      if (bothReplicasInSync) {
        partition.inSyncReplicas = allReplicas
        followerReplica.highWatermark = new LogOffsetMetadata(5)
      } else {
        partition.inSyncReplicas = Set(leaderReplica)
        followerReplica.highWatermark = new LogOffsetMetadata(0)
      }
    }
  }

  @After
  def tearDown() {
    if (replicaManager != null)
      replicaManager.shutdown(false)
    metrics.close()
  }

  def mockQuota(bound: Long): ReplicaQuota = {
    val quota: ReplicaQuota = createMock(classOf[ReplicaQuota])
    expect(quota.isThrottled(anyObject())).andReturn(true).anyTimes()
    quota
  }
}
