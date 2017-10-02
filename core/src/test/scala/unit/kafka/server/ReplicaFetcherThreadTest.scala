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
import kafka.server.epoch.LeaderEpochCache
import org.apache.kafka.common.requests.EpochEndOffset._
import kafka.server.epoch.util.ReplicaFetcherMockBlockingSend
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.requests.EpochEndOffset
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

  @Test
  def shouldNotIssueLeaderEpochRequestIfInterbrokerVersionBelow11(): Unit = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    props.put(KafkaConfig.InterBrokerProtocolVersionProp, "0.10.2")
    props.put(KafkaConfig.LogMessageFormatVersionProp, "0.10.2")
    val config = KafkaConfig.fromProps(props)
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaFetcherThread(
      name = "bob",
      fetcherId = 0,
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = null,
      metrics =  new Metrics(),
      time = new SystemTime(),
      quota = null,
      leaderEndpointBlockingSend = None)

    val result = thread.fetchEpochsFromLeader(Map(t1p0 -> 0, t1p1 -> 0))

    val expected = Map(
      t1p0 -> new EpochEndOffset(Errors.NONE, UNDEFINED_EPOCH_OFFSET),
      t1p1 -> new EpochEndOffset(Errors.NONE, UNDEFINED_EPOCH_OFFSET)
    )

    assertEquals("results from leader epoch request should have undefined offset", expected, result)
  }

  @Test
  def shouldHandleExceptionFromBlockingSend(): Unit = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    val config = KafkaConfig.fromProps(props)
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val mockBlockingSend = createMock(classOf[BlockingSend])

    expect(mockBlockingSend.sendRequest(anyObject())).andThrow(new NullPointerException).once()
    replay(mockBlockingSend)

    val thread = new ReplicaFetcherThread(
      name = "bob",
      fetcherId = 0,
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = null,
      metrics =  new Metrics(),
      time = new SystemTime(),
      quota = null,
      leaderEndpointBlockingSend = Some(mockBlockingSend))

    val result = thread.fetchEpochsFromLeader(Map(t1p0 -> 0, t1p1 -> 0))

    val expected = Map(
      t1p0 -> new EpochEndOffset(Errors.UNKNOWN_SERVER_ERROR, UNDEFINED_EPOCH_OFFSET),
      t1p1 -> new EpochEndOffset(Errors.UNKNOWN_SERVER_ERROR, UNDEFINED_EPOCH_OFFSET)
    )

    assertEquals("results from leader epoch request should have undefined offset", expected, result)
    verify(mockBlockingSend)
  }

  @Test
  def shouldFetchLeaderEpochOnFirstFetchOnly(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))

    //Setup all dependencies
    val quota = createNiceMock(classOf[ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val logManager = createMock(classOf[LogManager])
    val replica = createNiceMock(classOf[Replica])
    val replicaManager = createMock(classOf[ReplicaManager])

    //Stubs
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(replica.logEndOffset).andReturn(new LogOffsetMetadata(0)).anyTimes()
    expect(leaderEpochs.latestEpoch).andReturn(5)
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stub(replica, replicaManager)


    //Expectations
    expect(logManager.truncateTo(anyObject())).times(2)

    replay(leaderEpochs, replicaManager, logManager, quota, replica)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsets = Map(t1p0 -> new EpochEndOffset(1), t1p1 -> new EpochEndOffset(1)).asJava

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsets, endPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, endPoint, config, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
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
    val truncateToCapture: Capture[Map[TopicPartition, Long]] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val configs = TestUtils.createBrokerConfigs(1, "localhost:1234").map(KafkaConfig.fromProps)
    val quota = createNiceMock(classOf[ReplicationQuotaManager])
    val leaderEpochs = createMock(classOf[LeaderEpochCache])
    val logManager = createMock(classOf[LogManager])
    val replica = createNiceMock(classOf[Replica])
    val replicaManager = createMock(classOf[ReplicaManager])

    val initialLEO = 200

    //Stubs
    expect(logManager.truncateTo(capture(truncateToCapture))).times(2)
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(replica.logEndOffset).andReturn(new LogOffsetMetadata(initialLEO)).anyTimes()
    expect(leaderEpochs.latestEpoch).andReturn(5).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stub(replica, replicaManager)


    replay(leaderEpochs, replicaManager, logManager, quota, replica)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = Map(t1p0 -> new EpochEndOffset(156), t2p1 -> new EpochEndOffset(172)).asJava

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, endPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, endPoint, configs(0), replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> 0, t2p1 -> 0))

    //Run it
    thread.doWork()

    //We should have truncated to the offsets in the response
    val truncationPoints = truncateToCapture.getValues.asScala.flatMap(_.toSeq).toMap
    assertEquals(156, truncationPoints(t1p0))
    assertEquals(172, truncationPoints(t2p1))
  }

  @Test
  def shouldTruncateToHighWatermarkIfLeaderReturnsUndefinedOffset(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncated: Capture[Map[TopicPartition, Long]] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val configs = TestUtils.createBrokerConfigs(1, "localhost:1234").map(KafkaConfig.fromProps)
    val quota = createNiceMock(classOf[ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val logManager = createMock(classOf[LogManager])
    val replica = createNiceMock(classOf[Replica])
    val replicaManager = createMock(classOf[ReplicaManager])

    val highWaterMark = 100
    val initialLeo = 300

    //Stubs
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(highWaterMark)).anyTimes()
    expect(logManager.truncateTo(capture(truncated))).once
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(replica.logEndOffset).andReturn(new LogOffsetMetadata(initialLeo)).anyTimes()
    expect(leaderEpochs.latestEpoch).andReturn(5)
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stub(replica, replicaManager)
    replay(leaderEpochs, replicaManager, logManager, quota, replica)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = Map(t1p0 -> new EpochEndOffset(EpochEndOffset.UNDEFINED_EPOCH_OFFSET)).asJava

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, endPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, endPoint, configs(0), replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> 0))

    //Run it
    thread.doWork()

    //We should have truncated to the highwatermark for partitino 2 only
    assertEquals(highWaterMark, truncated.getValue.get(t1p0).get)
  }

  @Test
  def shouldPollIndefinitelyIfLeaderReturnsAnyException(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncated: Capture[Map[TopicPartition, Long]] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val configs = TestUtils.createBrokerConfigs(1, "localhost:1234").map(KafkaConfig.fromProps)
    val quota = createNiceMock(classOf[kafka.server.ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val logManager = createMock(classOf[kafka.log.LogManager])
    val replica = createNiceMock(classOf[Replica])
    val replicaManager = createMock(classOf[kafka.server.ReplicaManager])

    val highWaterMark = 100
    val initialLeo = 300

    //Stubs
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(highWaterMark)).anyTimes()
    expect(logManager.truncateTo(capture(truncated))).anyTimes()
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(replica.logEndOffset).andReturn(new LogOffsetMetadata(initialLeo)).anyTimes()
    expect(leaderEpochs.latestEpoch).andReturn(5)
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stub(replica, replicaManager)
    replay(leaderEpochs, replicaManager, logManager, quota, replica)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = mutable.Map(
      t1p0 -> new EpochEndOffset(NOT_LEADER_FOR_PARTITION, EpochEndOffset.UNDEFINED_EPOCH_OFFSET),
      t1p1 -> new EpochEndOffset(UNKNOWN_SERVER_ERROR, EpochEndOffset.UNDEFINED_EPOCH_OFFSET)
    ).asJava

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, endPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, endPoint, configs(0), replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> 0, t2p1 -> 0))

    //Run thread 3 times
    (0 to 3).foreach { _ =>
      thread.doWork()
    }

    //Then should loop continuously while there is no leader
    for (invocation <- truncated.getValues().asScala)
      assertEquals(0, invocation.size)

    //New leader elected and replies
    offsetsReply.put(t1p0, new EpochEndOffset(156))

    thread.doWork()

    //Now the final call should have actually done a truncation (to offset 156)
    assertEquals(156, truncated.getValues.asScala.last.get(t1p0).get)
  }

  @Test
  def shouldMovePartitionsOutOfTruncatingLogState(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))

    //Setup all stubs
    val quota = createNiceMock(classOf[ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val logManager = createNiceMock(classOf[LogManager])
    val replica = createNiceMock(classOf[Replica])
    val replicaManager = createNiceMock(classOf[ReplicaManager])

    //Stub return values
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(replica.logEndOffset).andReturn(new LogOffsetMetadata(0)).anyTimes()
    expect(leaderEpochs.latestEpoch).andReturn(5)
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stub(replica, replicaManager)

    replay(leaderEpochs, replicaManager, logManager, quota, replica)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsetsReply = Map(
      t1p0 -> new EpochEndOffset(1), t1p1 -> new EpochEndOffset(1)
    ).asJava

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, endPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, endPoint, config, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))

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
    val truncateToCapture: Capture[Map[TopicPartition, Long]] = newCapture(CaptureType.ALL)
    val initialLEO = 100

    //Setup all stubs
    val quota = createNiceMock(classOf[ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val logManager = createNiceMock(classOf[LogManager])
    val replica = createNiceMock(classOf[Replica])
    val replicaManager = createNiceMock(classOf[ReplicaManager])

    //Stub return values
    expect(logManager.truncateTo(capture(truncateToCapture))).once
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(replica.logEndOffset).andReturn(new LogOffsetMetadata(initialLEO)).anyTimes()
    expect(leaderEpochs.latestEpoch).andReturn(5)
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stub(replica, replicaManager)

    replay(leaderEpochs, replicaManager, logManager, quota, replica)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsetsReply = Map(
      t1p0 -> new EpochEndOffset(52), t1p1 -> new EpochEndOffset(49)
    ).asJava

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, endPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, endPoint, config, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))

    //When
    thread.addPartitions(Map(t1p0 -> 0, t1p1 -> 0))

    //When the epoch request is outstanding, remove one of the partitions to simulate a leader change. We do this via a callback passed to the mock thread
    val partitionThatBecameLeader = t1p0
    mockNetwork.setEpochRequestCallback(() => {
      thread.removePartitions(Set(partitionThatBecameLeader))
    })

    //When
    thread.doWork()

    //Then we should not have truncated the partition that became leader
    assertEquals(None, truncateToCapture.getValue.get(partitionThatBecameLeader))
    assertEquals(49, truncateToCapture.getValue.get(t1p1).get)
  }

  def stub(replica: Replica, replicaManager: ReplicaManager) = {
    expect(replicaManager.getReplica(t1p0)).andReturn(Some(replica)).anyTimes()
    expect(replicaManager.getReplicaOrException(t1p0)).andReturn(replica).anyTimes()
    expect(replicaManager.getReplica(t1p1)).andReturn(Some(replica)).anyTimes()
    expect(replicaManager.getReplicaOrException(t1p1)).andReturn(replica).anyTimes()
    expect(replicaManager.getReplica(t2p1)).andReturn(Some(replica)).anyTimes()
    expect(replicaManager.getReplicaOrException(t2p1)).andReturn(replica).anyTimes()
  }
}