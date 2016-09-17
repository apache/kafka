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

import kafka.api._
import kafka.cluster.Replica
import kafka.common.TopicAndPartition
import kafka.log.Log
import kafka.message.{ByteBufferMessageSet, Message}
import kafka.utils._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.{MockTime => JMockTime}
import org.easymock.EasyMock
import org.easymock.EasyMock._
import org.junit.Assert._
import org.junit.{After, Test}


class ReplicaManagerQuotasTest {
  val configs = TestUtils.createBrokerConfigs(2, TestUtils.MockZkConnect).map(KafkaConfig.fromProps(_, new Properties()))
  val time = new MockTime
  val jTime = new JMockTime
  val metrics = new Metrics
  val message = new Message("some-data-in-a-message".getBytes())
  val topicAndPartition1 = TopicAndPartition("test-topic", 1)
  val topicAndPartition2 = TopicAndPartition("test-topic", 2)
  val fetchInfo = Seq(topicAndPartition1 -> PartitionFetchInfo(0, 100), topicAndPartition2 -> PartitionFetchInfo(0, 100))
  var replicaManager: ReplicaManager = null

  @Test
  def shouldExcludeSubsequentThrottledPartitions(): Unit = {
    setUpMocks(fetchInfo)

    val quota = mockQuota(1000000)
    expect(quota.isQuotaExceeded()).andReturn(false).once()
    expect(quota.isQuotaExceeded()).andReturn(true).once()
    replay(quota)

    val fetch = replicaManager.readFromLocalLog(true, true, Int.MaxValue, false, fetchInfo, quota)
    assertEquals("Given two partitions, with only one throttled, we should get the first", 1,
      fetch.find(_._1 == topicAndPartition1).get._2.info.messageSet.size)

    assertEquals("But we shouldn't get the second", 0,
      fetch.find(_._1 == topicAndPartition2).get._2.info.messageSet.size)
  }

  @Test
  def shouldGetNoMessagesIfQuotasExceededOnSubsequentPartitions(): Unit = {
    setUpMocks(fetchInfo)

    val quota = mockQuota(1000000)
    expect(quota.isQuotaExceeded()).andReturn(true).once()
    expect(quota.isQuotaExceeded()).andReturn(true).once()
    replay(quota)

    val fetch = replicaManager.readFromLocalLog(true, true, Int.MaxValue, false, fetchInfo, quota)
    assertEquals("Given two partitions, with both throttled, we should get no messages", 0,
      fetch.find(_._1 == topicAndPartition1).get._2.info.messageSet.size)
    assertEquals("Given two partitions, with both throttled, we should get no messages", 0,
      fetch.find(_._1 == topicAndPartition2).get._2.info.messageSet.size)
  }

  @Test
  def shouldGetBothMessagesIfQuotasAllow(): Unit = {
    setUpMocks(fetchInfo)

    val quota = mockQuota(1000000)
    expect(quota.isQuotaExceeded()).andReturn(false).once()
    expect(quota.isQuotaExceeded()).andReturn(false).once()
    replay(quota)

    val fetch = replicaManager.readFromLocalLog(true, true, Int.MaxValue, false, fetchInfo, quota)
    assertEquals("Given two partitions, with both non-throttled, we should get both messages", 1,
      fetch.find(_._1 == topicAndPartition1).get._2.info.messageSet.size)
    assertEquals("Given two partitions, with both non-throttled, we should get both messages", 1,
      fetch.find(_._1 == topicAndPartition2).get._2.info.messageSet.size)
  }

  def setUpMocks(fetchInfo: Seq[(TopicAndPartition, PartitionFetchInfo)], message: Message = this.message) {
    val zkUtils = createNiceMock(classOf[ZkUtils])
    val scheduler = createNiceMock(classOf[KafkaScheduler])

    //Create log which handles both a regular read and a 0 bytes read
    val log = createMock(classOf[Log])
    expect(log.logEndOffset).andReturn(20L).anyTimes()
    expect(log.logEndOffsetMetadata).andReturn(new LogOffsetMetadata(20L)).anyTimes()

    //if we ask for len 1 return a message
    expect(log.read(anyObject(), geq(1), anyObject(), anyObject())).andReturn(
      new FetchDataInfo(
        new LogOffsetMetadata(0L, 0L, 0),
        new ByteBufferMessageSet(message)
      )).anyTimes()

    //if we ask for len = 0, return 0 messages
    expect(log.read(anyObject(), EasyMock.eq(0), anyObject(), anyObject())).andReturn(
      new FetchDataInfo(
        new LogOffsetMetadata(0L, 0L, 0),
        new ByteBufferMessageSet()
      )).anyTimes()
    replay(log)

    //Create log manager
    val logManager = createMock(classOf[kafka.log.LogManager])

    //Return the same log for each partition as it doesn't matter
    expect(logManager.getLog(anyObject())).andReturn(Some(log)).anyTimes()
    replay(logManager)

    replicaManager = new ReplicaManager(configs.head, metrics, time, jTime, zkUtils, scheduler, logManager,
      new AtomicBoolean(false), QuotaFactory.instantiate(configs.head, metrics, time).follower)

    //create the two replicas
    for ((p, _) <- fetchInfo) {
      val partition = replicaManager.getOrCreatePartition(p.topic, p.partition)
      val replica = new Replica(configs.head.brokerId, partition, time, 0, Some(log))
      replica.highWatermark = new LogOffsetMetadata(5)
      partition.leaderReplicaIdOpt = Some(replica.brokerId)
      val allReplicas = List(replica)
      allReplicas.foreach(partition.addReplicaIfNotExists(_))
      partition.inSyncReplicas = allReplicas.toSet
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
