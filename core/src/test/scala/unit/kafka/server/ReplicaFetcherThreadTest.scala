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

import kafka.cluster.{BrokerEndPoint, Replica}
import kafka.log.LogManager
import kafka.cluster.Partition
import kafka.server.epoch.LeaderEpochCache
import kafka.server.epoch.util.ReplicaFetcherMockBlockingSend
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.requests.EpochEndOffset
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.utils.SystemTime
import org.easymock.EasyMock._
import org.easymock.{Capture, CaptureType}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.{Map, mutable}

class ReplicaFetcherThreadTest {

  private val t1p0 = new TopicPartition("topic1", 0)
  private val t1p1 = new TopicPartition("topic1", 1)
  private val t2p1 = new TopicPartition("topic2", 1)

  private val brokerEndPoint = new BrokerEndPoint(0, "localhost", 1000)

  @Test
  def shouldNotIssueLeaderEpochRequestIfInterbrokerVersionBelow11(): Unit = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    props.put(KafkaConfig.InterBrokerProtocolVersionProp, "0.10.2")
    props.put(KafkaConfig.LogMessageFormatVersionProp, "0.10.2")
    val config = KafkaConfig.fromProps(props)
    val thread = new ReplicaFetcherThread(
      name = "bob",
      fetcherId = 0,
      sourceBroker = brokerEndPoint,
      brokerConfig = config,
      replicaMgr = null,
      metrics =  new Metrics(),
      time = new SystemTime(),
      quota = null,
      leaderEndpointBlockingSend = None)

    val result = thread.fetchEpochsFromLeader(Map(t1p0 -> 0, t1p1 -> 0))

    val expected = Map(
      t1p0 -> new EpochEndOffset(Errors.NONE, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET),
      t1p1 -> new EpochEndOffset(Errors.NONE, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
    )

    assertEquals("results from leader epoch request should have undefined offset", expected, result)
  }

  @Test
  def shouldHandleExceptionFromBlockingSend(): Unit = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    val config = KafkaConfig.fromProps(props)
    val mockBlockingSend = createMock(classOf[BlockingSend])

    expect(mockBlockingSend.sendRequest(anyObject())).andThrow(new NullPointerException).once()
    replay(mockBlockingSend)

    val thread = new ReplicaFetcherThread(
      name = "bob",
      fetcherId = 0,
      sourceBroker = brokerEndPoint,
      brokerConfig = config,
      replicaMgr = null,
      metrics =  new Metrics(),
      time = new SystemTime(),
      quota = null,
      leaderEndpointBlockingSend = Some(mockBlockingSend))

    val result = thread.fetchEpochsFromLeader(Map(t1p0 -> 0, t1p1 -> 0))

    val expected = Map(
      t1p0 -> new EpochEndOffset(Errors.UNKNOWN_SERVER_ERROR, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET),
      t1p1 -> new EpochEndOffset(Errors.UNKNOWN_SERVER_ERROR, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
    )

    assertEquals("results from leader epoch request should have undefined offset", expected, result)
    verify(mockBlockingSend)
  }

  @Test
  def shouldFetchLeaderEpochOnFirstFetchOnlyIfLeaderEpochKnownToBoth(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))

    //Setup all dependencies
    val quota = createNiceMock(classOf[ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val logManager = createMock(classOf[LogManager])
    val replicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createMock(classOf[ReplicaManager])

    val leaderEpoch = 5

    //Stubs
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(replica.logEndOffset).andReturn(new LogOffsetMetadata(0)).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(0)).anyTimes()
    expect(leaderEpochs.latestEpoch).andReturn(leaderEpoch)
    expect(leaderEpochs.endOffsetFor(leaderEpoch)).andReturn((leaderEpoch, 0)).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)

    //Expectations
    expect(partition.truncateTo(anyLong(), anyBoolean())).once

    replay(leaderEpochs, replicaManager, logManager, quota, replica)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsets = Map(t1p0 -> new EpochEndOffset(leaderEpoch, 1), t1p1 -> new EpochEndOffset(leaderEpoch, 1)).asJava

    //Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsets, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> 0, t1p1 -> 0))

    //Loop 1
    thread.doWork()
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(1, mockNetwork.fetchCount)

    //Loop 2 we should not fetch epochs
    thread.doWork()
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(2, mockNetwork.fetchCount)

    //Loop 3 we should not fetch epochs
    thread.doWork()
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(3, mockNetwork.fetchCount)

    //Assert that truncate to is called exactly once (despite two loops)
    verify(logManager)
  }

  @Test
  def shouldTruncateToOffsetSpecifiedInEpochOffsetResponse(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncateToCapture: Capture[Long] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val configs = TestUtils.createBrokerConfigs(1, "localhost:1234").map(KafkaConfig.fromProps)
    val quota = createNiceMock(classOf[ReplicationQuotaManager])
    val leaderEpochs = createMock(classOf[LeaderEpochCache])
    val logManager = createMock(classOf[LogManager])
    val replicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createMock(classOf[ReplicaManager])

    val leaderEpoch = 5
    val initialLEO = 200

    //Stubs
    expect(partition.truncateTo(capture(truncateToCapture), anyBoolean())).anyTimes()
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(replica.logEndOffset).andReturn(new LogOffsetMetadata(initialLEO)).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(initialLEO - 1)).anyTimes()
    expect(leaderEpochs.latestEpoch).andReturn(leaderEpoch).anyTimes()
    expect(leaderEpochs.endOffsetFor(leaderEpoch)).andReturn((leaderEpoch, initialLEO)).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)

    replay(leaderEpochs, replicaManager, logManager, quota, replica, partition)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = Map(t1p0 -> new EpochEndOffset(leaderEpoch, 156), t2p1 -> new EpochEndOffset(leaderEpoch, 172)).asJava

    //Create the thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, configs(0), replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> 0, t2p1 -> 0))

    //Run it
    thread.doWork()

    //We should have truncated to the offsets in the response
    assertTrue("Expected " + t1p0 + " to truncate to offset 156 (truncation offsets: " + truncateToCapture.getValues + ")",
               truncateToCapture.getValues.asScala.contains(156))
    assertTrue("Expected " + t2p1 + " to truncate to offset 172 (truncation offsets: " + truncateToCapture.getValues + ")",
               truncateToCapture.getValues.asScala.contains(172))
  }

  @Test
  def shouldTruncateToOffsetSpecifiedInEpochOffsetResponseIfFollowerHasNoMoreEpochs(): Unit = {
    // Create a capture to track what partitions/offsets are truncated
    val truncateToCapture: Capture[Long] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val configs = TestUtils.createBrokerConfigs(1, "localhost:1234").map(KafkaConfig.fromProps)
    val quota = createNiceMock(classOf[ReplicationQuotaManager])
    val leaderEpochs = createMock(classOf[LeaderEpochCache])
    val logManager = createMock(classOf[LogManager])
    val replicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createMock(classOf[ReplicaManager])

    val leaderEpochAtFollower = 5
    val leaderEpochAtLeader = 4
    val initialLEO = 200

    //Stubs
    expect(partition.truncateTo(capture(truncateToCapture), anyBoolean())).anyTimes()
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(replica.logEndOffset).andReturn(new LogOffsetMetadata(initialLEO)).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(initialLEO - 3)).anyTimes()
    expect(leaderEpochs.latestEpoch).andReturn(leaderEpochAtFollower).anyTimes()
    expect(leaderEpochs.endOffsetFor(leaderEpochAtLeader)).andReturn((UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)

    replay(leaderEpochs, replicaManager, logManager, quota, replica, partition)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = Map(t1p0 -> new EpochEndOffset(leaderEpochAtLeader, 156),
                           t2p1 -> new EpochEndOffset(leaderEpochAtLeader, 202)).asJava

    //Create the thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, configs(0), replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> 0, t2p1 -> 0))

    //Run it
    thread.doWork()

    //We should have truncated to the offsets in the response
    assertTrue("Expected " + t1p0 + " to truncate to offset 156 (truncation offsets: " + truncateToCapture.getValues + ")",
               truncateToCapture.getValues.asScala.contains(156))
    assertTrue("Expected " + t2p1 + " to truncate to offset " + initialLEO +
               " (truncation offsets: " + truncateToCapture.getValues + ")",
               truncateToCapture.getValues.asScala.contains(initialLEO))
  }

  @Test
  def shouldFetchLeaderEpochSecondTimeIfLeaderRepliesWithEpochNotKnownToFollower(): Unit = {

    // Create a capture to track what partitions/offsets are truncated
    val truncateToCapture: Capture[Long] = newCapture(CaptureType.ALL)

    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))

    // Setup all dependencies
    val quota = createNiceMock(classOf[ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val logManager = createMock(classOf[LogManager])
    val replicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createMock(classOf[ReplicaManager])

    val initialLEO = 200

    // Stubs
    expect(partition.truncateTo(capture(truncateToCapture), anyBoolean())).anyTimes()
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(replica.logEndOffset).andReturn(new LogOffsetMetadata(initialLEO)).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(initialLEO - 2)).anyTimes()
    expect(leaderEpochs.latestEpoch).andReturn(5)
    expect(leaderEpochs.endOffsetFor(4)).andReturn((3, 120)).anyTimes()
    expect(leaderEpochs.endOffsetFor(3)).andReturn((3, 120)).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)

    replay(leaderEpochs, replicaManager, logManager, quota, replica, partition)

    // Define the offsets for the OffsetsForLeaderEpochResponse
    val offsets = Map(t1p0 -> new EpochEndOffset(4, 155), t1p1 -> new EpochEndOffset(4, 143)).asJava

    // Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsets, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> 0, t1p1 -> 0))

    // Loop 1 -- both topic partitions will need to fetch another leader epoch
    thread.doWork()
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(0, mockNetwork.fetchCount)

    // Loop 2 should do the second fetch for both topic partitions because the leader replied with
    // epoch 4 while follower knows only about epoch 3
    val nextOffsets = Map(t1p0 -> new EpochEndOffset(3, 101), t1p1 -> new EpochEndOffset(3, 102)).asJava
    mockNetwork.setOffsetsForNextResponse(nextOffsets)
    thread.doWork()
    assertEquals(2, mockNetwork.epochFetchCount)
    assertEquals(1, mockNetwork.fetchCount)
    assertEquals("OffsetsForLeaderEpochRequest version.",
                 1, mockNetwork.lastUsedOffsetForLeaderEpochVersion)

    //Loop 3 we should not fetch epochs
    thread.doWork()
    assertEquals(2, mockNetwork.epochFetchCount)
    assertEquals(2, mockNetwork.fetchCount)


    //We should have truncated to the offsets in the second response
    assertTrue("Expected " + t1p1 + " to truncate to offset 102 (truncation offsets: " + truncateToCapture.getValues + ")",
               truncateToCapture.getValues.asScala.contains(102))
    assertTrue("Expected " + t1p0 + " to truncate to offset 101 (truncation offsets: " + truncateToCapture.getValues + ")",
               truncateToCapture.getValues.asScala.contains(101))
  }

  @Test
  def shouldUseLeaderEndOffsetIfInterBrokerVersionBelow20(): Unit = {

    // Create a capture to track what partitions/offsets are truncated
    val truncateToCapture: Capture[Long] = newCapture(CaptureType.ALL)

    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    props.put(KafkaConfig.InterBrokerProtocolVersionProp, "0.11.0")
    val config = KafkaConfig.fromProps(props)

    // Setup all dependencies
    val quota = createNiceMock(classOf[ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val logManager = createMock(classOf[LogManager])
    val replicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createMock(classOf[ReplicaManager])

    val initialLEO = 200

    // Stubs
    expect(partition.truncateTo(capture(truncateToCapture), anyBoolean())).anyTimes()
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(replica.logEndOffset).andReturn(new LogOffsetMetadata(initialLEO)).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(initialLEO - 2)).anyTimes()
    expect(leaderEpochs.latestEpoch).andReturn(5)
    expect(leaderEpochs.endOffsetFor(4)).andReturn((3, 120)).anyTimes()
    expect(leaderEpochs.endOffsetFor(3)).andReturn((3, 120)).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)

    replay(leaderEpochs, replicaManager, logManager, quota, replica, partition)

    // Define the offsets for the OffsetsForLeaderEpochResponse with undefined epoch to simulate
    // older protocol version
    val offsets = Map(t1p0 -> new EpochEndOffset(EpochEndOffset.UNDEFINED_EPOCH, 155), t1p1 -> new EpochEndOffset(EpochEndOffset.UNDEFINED_EPOCH, 143)).asJava

    // Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsets, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> 0, t1p1 -> 0))

    // Loop 1 -- both topic partitions will truncate to leader offset even though they don't know
    // about leader epoch
    thread.doWork()
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(1, mockNetwork.fetchCount)
    assertEquals("OffsetsForLeaderEpochRequest version.",
                 0, mockNetwork.lastUsedOffsetForLeaderEpochVersion)

    //Loop 2 we should not fetch epochs
    thread.doWork()
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(2, mockNetwork.fetchCount)

    //We should have truncated to the offsets in the first response
    assertTrue("Expected " + t1p0 + " to truncate to offset 155 (truncation offsets: " + truncateToCapture.getValues + ")",
               truncateToCapture.getValues.asScala.contains(155))
    assertTrue("Expected " + t1p1 + " to truncate to offset 143 (truncation offsets: " + truncateToCapture.getValues + ")",
               truncateToCapture.getValues.asScala.contains(143))
  }

  @Test
  def shouldTruncateToInitialFetchOffsetIfLeaderReturnsUndefinedOffset(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncated: Capture[Long] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val configs = TestUtils.createBrokerConfigs(1, "localhost:1234").map(KafkaConfig.fromProps)
    val quota = createNiceMock(classOf[ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val logManager = createMock(classOf[LogManager])
    val replicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createMock(classOf[ReplicaManager])

    val initialFetchOffset = 100
    val initialLeo = 300

    //Stubs
    expect(partition.truncateTo(capture(truncated), anyBoolean())).anyTimes()
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(replica.logEndOffset).andReturn(new LogOffsetMetadata(initialLeo)).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(initialFetchOffset)).anyTimes()
    expect(leaderEpochs.latestEpoch).andReturn(5)
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)
    replay(leaderEpochs, replicaManager, logManager, quota, replica, partition)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = Map(t1p0 -> new EpochEndOffset(EpochEndOffset.UNDEFINED_EPOCH, EpochEndOffset.UNDEFINED_EPOCH_OFFSET)).asJava

    //Create the thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, configs(0), replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> initialFetchOffset))

    //Run it
    thread.doWork()

    //We should have truncated to initial fetch offset
    assertEquals(initialFetchOffset, truncated.getValue)
  }

  @Test
  def shouldPollIndefinitelyIfLeaderReturnsAnyException(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncated: Capture[Long] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val configs = TestUtils.createBrokerConfigs(1, "localhost:1234").map(KafkaConfig.fromProps)
    val quota = createNiceMock(classOf[kafka.server.ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val logManager = createMock(classOf[kafka.log.LogManager])
    val replicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createMock(classOf[kafka.server.ReplicaManager])

    val leaderEpoch = 5
    val highWaterMark = 100
    val initialLeo = 300

    //Stubs
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(highWaterMark)).anyTimes()
    expect(partition.truncateTo(capture(truncated), anyBoolean())).anyTimes()
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(replica.logEndOffset).andReturn(new LogOffsetMetadata(initialLeo)).anyTimes()
    expect(leaderEpochs.latestEpoch).andReturn(leaderEpoch)
    // this is for the last reply with EpochEndOffset(5, 156)
    expect(leaderEpochs.endOffsetFor(leaderEpoch)).andReturn((leaderEpoch, initialLeo)).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)
    replay(leaderEpochs, replicaManager, logManager, quota, replica, partition)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = mutable.Map(
      t1p0 -> new EpochEndOffset(NOT_LEADER_FOR_PARTITION, EpochEndOffset.UNDEFINED_EPOCH, EpochEndOffset.UNDEFINED_EPOCH_OFFSET),
      t1p1 -> new EpochEndOffset(UNKNOWN_SERVER_ERROR, EpochEndOffset.UNDEFINED_EPOCH, EpochEndOffset.UNDEFINED_EPOCH_OFFSET)
    ).asJava

    //Create the thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, configs(0), replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> 0, t2p1 -> 0))

    //Run thread 3 times
    (0 to 3).foreach { _ =>
      thread.doWork()
    }

    //Then should loop continuously while there is no leader
    assertEquals(0, truncated.getValues.size())

    //New leader elected and replies
    offsetsReply.put(t1p0, new EpochEndOffset(leaderEpoch, 156))

    thread.doWork()

    //Now the final call should have actually done a truncation (to offset 156)
    assertEquals(156, truncated.getValue)
  }

  @Test
  def shouldMovePartitionsOutOfTruncatingLogState(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))

    //Setup all stubs
    val quota = createNiceMock(classOf[ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val logManager = createNiceMock(classOf[LogManager])
    val replicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createNiceMock(classOf[ReplicaManager])

    val leaderEpoch = 4

    //Stub return values
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(replica.logEndOffset).andReturn(new LogOffsetMetadata(0)).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(0)).anyTimes()
    expect(leaderEpochs.latestEpoch).andReturn(leaderEpoch)
    expect(leaderEpochs.endOffsetFor(leaderEpoch)).andReturn((leaderEpoch, 0)).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)

    replay(leaderEpochs, replicaManager, logManager, quota, replica)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsetsReply = Map(
      t1p0 -> new EpochEndOffset(leaderEpoch, 1), t1p1 -> new EpochEndOffset(leaderEpoch, 1)
    ).asJava

    //Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))

    //When
    thread.addPartitions(Map(t1p0 -> 0, t1p1 -> 0))

    //Then all partitions should start in an TruncatingLog state
    assertTrue(thread.partitionStates.partitionStates().asScala.forall(_.value().truncatingLog))

    //When
    thread.doWork()

    //Then none should be TruncatingLog anymore
    assertFalse(thread.partitionStates.partitionStates().asScala.forall(_.value().truncatingLog))
  }

  @Test
  def shouldFilterPartitionsMadeLeaderDuringLeaderEpochRequest(): Unit ={
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val truncateToCapture: Capture[Long] = newCapture(CaptureType.ALL)
    val initialLEO = 100

    //Setup all stubs
    val quota = createNiceMock(classOf[ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val logManager = createNiceMock(classOf[LogManager])
    val replicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createNiceMock(classOf[ReplicaManager])

    //Stub return values
    expect(partition.truncateTo(capture(truncateToCapture), anyBoolean())).once
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(replica.logEndOffset).andReturn(new LogOffsetMetadata(initialLEO)).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(initialLEO - 2)).anyTimes()
    expect(leaderEpochs.latestEpoch).andReturn(5)
    expect(leaderEpochs.endOffsetFor(5)).andReturn((5, initialLEO)).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)

    replay(leaderEpochs, replicaManager, logManager, quota, replica, partition)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsetsReply = Map(
      t1p0 -> new EpochEndOffset(5, 52), t1p1 -> new EpochEndOffset(5, 49)
    ).asJava

    //Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))

    //When
    thread.addPartitions(Map(t1p0 -> 0, t1p1 -> 0))

    //When the epoch request is outstanding, remove one of the partitions to simulate a leader change. We do this via a callback passed to the mock thread
    val partitionThatBecameLeader = t1p0
    mockNetwork.setEpochRequestCallback(() => {
      thread.removePartitions(Set(partitionThatBecameLeader))
    })

    //When
    thread.doWork()

    //Then we should not have truncated the partition that became leader. Exactly one partition should be truncated.
    assertEquals(49, truncateToCapture.getValue)
  }

  def stub(replica: Replica, partition: Partition, replicaManager: ReplicaManager) = {
    expect(replicaManager.getReplica(t1p0)).andReturn(Some(replica)).anyTimes()
    expect(replicaManager.getReplicaOrException(t1p0)).andReturn(replica).anyTimes()
    expect(replicaManager.getPartition(t1p0)).andReturn(Some(partition)).anyTimes()
    expect(replicaManager.getReplica(t1p1)).andReturn(Some(replica)).anyTimes()
    expect(replicaManager.getReplicaOrException(t1p1)).andReturn(replica).anyTimes()
    expect(replicaManager.getPartition(t1p1)).andReturn(Some(partition)).anyTimes()
    expect(replicaManager.getReplica(t2p1)).andReturn(Some(replica)).anyTimes()
    expect(replicaManager.getReplicaOrException(t2p1)).andReturn(replica).anyTimes()
    expect(replicaManager.getPartition(t2p1)).andReturn(Some(partition)).anyTimes()
  }
}
