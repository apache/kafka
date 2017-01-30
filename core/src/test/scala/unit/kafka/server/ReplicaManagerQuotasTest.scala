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

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.cluster.Replica
import kafka.log.Log
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.record.{MemoryRecords, Record}
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
  val record = Record.create("some-data-in-a-message".getBytes())
  val topicPartition1 = new TopicPartition("test-topic", 1)
  val topicPartition2 = new TopicPartition("test-topic", 2)
  val fetchInfo = Seq(topicPartition1 -> new PartitionData(0, 100), topicPartition2 -> new PartitionData(0, 100))
  var replicaManager: ReplicaManager = null

  @Test
  def shouldExcludeSubsequentThrottledPartitions(): Unit = {
    setUpMocks(fetchInfo)
    val followerReplicaId = configs.last.brokerId

    val quota = mockQuota(1000000)
    expect(quota.isQuotaExceeded()).andReturn(false).once()
    expect(quota.isQuotaExceeded()).andReturn(true).once()
    replay(quota)

    val fetch = replicaManager.readFromLocalLog(
      replicaId = followerReplicaId,
      fetchOnlyFromLeader = true,
      readOnlyCommitted = true,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      readPartitionInfo = fetchInfo,
      quota = quota)
    assertEquals("Given two partitions, with only one throttled, we should get the first", 1,
      fetch.find(_._1 == topicPartition1).get._2.info.records.shallowEntries.asScala.size)

    assertEquals("But we shouldn't get the second", 0,
      fetch.find(_._1 == topicPartition2).get._2.info.records.shallowEntries.asScala.size)
  }

  @Test
  def shouldGetNoMessagesIfQuotasExceededOnSubsequentPartitions(): Unit = {
    setUpMocks(fetchInfo)
    val followerReplicaId = configs.last.brokerId

    val quota = mockQuota(1000000)
    expect(quota.isQuotaExceeded()).andReturn(true).once()
    expect(quota.isQuotaExceeded()).andReturn(true).once()
    replay(quota)

    val fetch = replicaManager.readFromLocalLog(
      replicaId = followerReplicaId,
      fetchOnlyFromLeader = true,
      readOnlyCommitted = true,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      readPartitionInfo = fetchInfo,
      quota = quota)
    assertEquals("Given two partitions, with both throttled, we should get no messages", 0,
      fetch.find(_._1 == topicPartition1).get._2.info.records.shallowEntries.asScala.size)
    assertEquals("Given two partitions, with both throttled, we should get no messages", 0,
      fetch.find(_._1 == topicPartition2).get._2.info.records.shallowEntries.asScala.size)
  }

  @Test
  def shouldGetBothMessagesIfQuotasAllow(): Unit = {
    setUpMocks(fetchInfo)
    val followerReplicaId = configs.last.brokerId

    val quota = mockQuota(1000000)
    expect(quota.isQuotaExceeded()).andReturn(false).once()
    expect(quota.isQuotaExceeded()).andReturn(false).once()
    replay(quota)

    val fetch = replicaManager.readFromLocalLog(
      replicaId = followerReplicaId,
      fetchOnlyFromLeader = true,
      readOnlyCommitted = true,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      readPartitionInfo = fetchInfo,
      quota = quota)
    assertEquals("Given two partitions, with both non-throttled, we should get both messages", 1,
      fetch.find(_._1 == topicPartition1).get._2.info.records.shallowEntries.asScala.size)
    assertEquals("Given two partitions, with both non-throttled, we should get both messages", 1,
      fetch.find(_._1 == topicPartition2).get._2.info.records.shallowEntries.asScala.size)
  }

  @Test
  def shouldIncludeInSyncThrottledReplicas(): Unit = {
    setUpMocks(fetchInfo, bothReplicasInSync = true)
    val followerReplicaId = configs.last.brokerId

    val quota = mockQuota(1000000)
    expect(quota.isQuotaExceeded()).andReturn(false).once()
    expect(quota.isQuotaExceeded()).andReturn(true).once()
    replay(quota)

    val fetch = replicaManager.readFromLocalLog(
      replicaId = followerReplicaId,
      fetchOnlyFromLeader = true,
      readOnlyCommitted = true,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      readPartitionInfo = fetchInfo,
      quota = quota)
    assertEquals("Given two partitions, with only one throttled, we should get the first", 1,
      fetch.find(_._1 == topicPartition1).get._2.info.records.shallowEntries.asScala.size)

    assertEquals("But we should get the second too since it's throttled but in sync", 1,
      fetch.find(_._1 == topicPartition2).get._2.info.records.shallowEntries.asScala.size)
  }

  def setUpMocks(fetchInfo: Seq[(TopicPartition, PartitionData)], record: Record = this.record, bothReplicasInSync: Boolean = false) {
    val zkUtils = createNiceMock(classOf[ZkUtils])
    val scheduler = createNiceMock(classOf[KafkaScheduler])

    //Create log which handles both a regular read and a 0 bytes read
    val log = createMock(classOf[Log])
    expect(log.logEndOffset).andReturn(20L).anyTimes()
    expect(log.logEndOffsetMetadata).andReturn(new LogOffsetMetadata(20L)).anyTimes()

    //if we ask for len 1 return a message
    expect(log.read(anyObject(), geq(1), anyObject(), anyObject())).andReturn(
      FetchDataInfo(
        new LogOffsetMetadata(0L, 0L, 0),
        MemoryRecords.withRecords(record)
      )).anyTimes()

    //if we ask for len = 0, return 0 messages
    expect(log.read(anyObject(), EasyMock.eq(0), anyObject(), anyObject())).andReturn(
      FetchDataInfo(
        new LogOffsetMetadata(0L, 0L, 0),
        MemoryRecords.EMPTY
      )).anyTimes()
    replay(log)

    //Create log manager
    val logManager = createMock(classOf[kafka.log.LogManager])

    //Return the same log for each partition as it doesn't matter
    expect(logManager.getLog(anyObject())).andReturn(Some(log)).anyTimes()
    replay(logManager)

    replicaManager = new ReplicaManager(configs.head, metrics, time, zkUtils, scheduler, logManager,
      new AtomicBoolean(false), QuotaFactory.instantiate(configs.head, metrics, time).follower)

    //create the two replicas
    for ((p, _) <- fetchInfo) {
      val partition = replicaManager.getOrCreatePartition(p)
      val leaderReplica = new Replica(configs.head.brokerId, partition, time, 0, Some(log))
      leaderReplica.highWatermark = new LogOffsetMetadata(5)
      partition.leaderReplicaIdOpt = Some(leaderReplica.brokerId)
      val followerReplica = new Replica(configs.last.brokerId, partition, time, 0, Some(log))
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
    replicaManager.shutdown(false)
    metrics.close()
  }

  def mockQuota(bound: Long): ReplicaQuota = {
    val quota = createMock(classOf[ReplicaQuota])
    expect(quota.isThrottled(anyObject())).andReturn(true).anyTimes()
    quota
  }
}
