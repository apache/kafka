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
package kafka.server.epoch

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

import kafka.cluster.Replica
import kafka.server._
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.EpochEndOffset
import org.apache.kafka.common.requests.EpochEndOffset._
import org.easymock.EasyMock._
import org.junit.Assert._
import org.junit.Test


class OffsetsForLeaderEpochTest {
  private val config = TestUtils.createBrokerConfigs(1, TestUtils.MockZkConnect).map(KafkaConfig.fromProps).head
  private val time = new MockTime
  private val metrics = new Metrics
  private val tp = new TopicPartition("topic", 1)

  @Test
  def shouldGetEpochsFromReplica(): Unit = {
    //Given
    val epochAndOffset = (5, 42L)
    val epochRequested: Integer = 5
    val request = Map(tp -> epochRequested)

    //Stubs
    val mockLog = createNiceMock(classOf[kafka.log.Log])
    val mockCache = createNiceMock(classOf[kafka.server.epoch.LeaderEpochCache])
    val logManager = createNiceMock(classOf[kafka.log.LogManager])
    expect(mockCache.endOffsetFor(epochRequested)).andReturn(epochAndOffset)
    expect(mockLog.leaderEpochCache).andReturn(mockCache).anyTimes()
    expect(logManager.liveLogDirs).andReturn(Array.empty[File]).anyTimes()
    replay(mockCache, mockLog, logManager)

    // create a replica manager with 1 partition that has 1 replica
    val replicaManager = new ReplicaManager(config, metrics, time, null, null, logManager, new AtomicBoolean(false),
      QuotaFactory.instantiate(config, metrics, time, ""), new BrokerTopicStats,
      new MetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size))
    val partition = replicaManager.getOrCreatePartition(tp)
    val leaderReplica = new Replica(config.brokerId, partition.topicPartition, time, 0, Some(mockLog))
    partition.addReplicaIfNotExists(leaderReplica)
    partition.leaderReplicaIdOpt = Some(config.brokerId)

    //When
    val response = replicaManager.lastOffsetForLeaderEpoch(request)

    //Then
    assertEquals(new EpochEndOffset(Errors.NONE, epochAndOffset._1, epochAndOffset._2), response(tp))
  }

  @Test
  def shouldReturnNoLeaderForPartitionIfThrown(): Unit = {
    val logManager = createNiceMock(classOf[kafka.log.LogManager])
    expect(logManager.liveLogDirs).andReturn(Array.empty[File]).anyTimes()
    replay(logManager)

    //create a replica manager with 1 partition that has 0 replica
    val replicaManager = new ReplicaManager(config, metrics, time, null, null, logManager, new AtomicBoolean(false),
      QuotaFactory.instantiate(config, metrics, time, ""), new BrokerTopicStats,
      new MetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size))
    replicaManager.getOrCreatePartition(tp)

    //Given
    val epochRequested: Integer = 5
    val request = Map(tp -> epochRequested)

    //When
    val response = replicaManager.lastOffsetForLeaderEpoch(request)

    //Then
    assertEquals(new EpochEndOffset(Errors.NOT_LEADER_FOR_PARTITION, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), response(tp))
  }

  @Test
  def shouldReturnUnknownTopicOrPartitionIfThrown(): Unit = {
    val logManager = createNiceMock(classOf[kafka.log.LogManager])
    expect(logManager.liveLogDirs).andReturn(Array.empty[File]).anyTimes()
    replay(logManager)

    //create a replica manager with 0 partition
    val replicaManager = new ReplicaManager(config, metrics, time, null, null, logManager, new AtomicBoolean(false),
      QuotaFactory.instantiate(config, metrics, time, ""), new BrokerTopicStats,
      new MetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size))

    //Given
    val epochRequested: Integer = 5
    val request = Map(tp -> epochRequested)

    //When
    val response = replicaManager.lastOffsetForLeaderEpoch(request)

    //Then
    assertEquals(new EpochEndOffset(Errors.UNKNOWN_TOPIC_OR_PARTITION, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), response(tp))
  }
}
