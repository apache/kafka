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

import java.util.Optional

import kafka.cluster.{BrokerEndPoint, Partition, Replica}
import kafka.log.LogManager
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.server.epoch.util.ReplicaFetcherMockBlockingSend
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.{EpochEndOffset, OffsetsForLeaderEpochRequest}
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
  private val failedPartitions = new FailedPartitions

  private def offsetAndEpoch(fetchOffset: Long, leaderEpoch: Int = 1): OffsetAndEpoch = {
    OffsetAndEpoch(offset = fetchOffset, leaderEpoch = leaderEpoch)
  }

  @Test
  def shouldSendLatestRequestVersionsByDefault(): Unit = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    val config = KafkaConfig.fromProps(props)
    val thread = new ReplicaFetcherThread(
      name = "bob",
      fetcherId = 0,
      sourceBroker = brokerEndPoint,
      brokerConfig = config,
      failedPartitions: FailedPartitions,
      replicaMgr = null,
      metrics =  new Metrics(),
      time = new SystemTime(),
      quota = UnboundedQuota,
      leaderEndpointBlockingSend = None)
    assertEquals(ApiKeys.FETCH.latestVersion, thread.fetchRequestVersion)
    assertEquals(ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion, thread.offsetForLeaderEpochRequestVersion)
    assertEquals(ApiKeys.LIST_OFFSETS.latestVersion, thread.listOffsetRequestVersion)
  }

  @Test
  def shouldFetchLeaderEpochRequestIfLastEpochDefinedForSomePartitions(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))

    //Setup all dependencies
    val quota: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    val leaderEpoch = 5

    //Stubs
    expect(replica.logEndOffset).andReturn(0).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(0)).anyTimes()
    expect(replica.latestEpoch).andReturn(Some(leaderEpoch)).once()
    expect(replica.latestEpoch).andReturn(Some(leaderEpoch)).once()
    expect(replica.latestEpoch).andReturn(None).once()  // t2p1 doesnt support epochs
    expect(replica.endOffsetForEpoch(leaderEpoch)).andReturn(
      Some(OffsetAndEpoch(0, leaderEpoch))).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)

    //Expectations
    expect(partition.truncateTo(anyLong(), anyBoolean())).once

    replay(replicaManager, logManager, quota, replica)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsets = Map(t1p0 -> new EpochEndOffset(leaderEpoch, 1),
      t1p1 -> new EpochEndOffset(leaderEpoch, 1)).asJava

    //Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsets, brokerEndPoint, new SystemTime())

    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))

    // topic 1 supports epoch, t2 doesn't
    thread.addPartitions(Map(
      t1p0 -> offsetAndEpoch(0L),
      t1p1 -> offsetAndEpoch(0L),
      t2p1 -> offsetAndEpoch(0L)))

    assertPartitionStates(thread, shouldBeReadyForFetch = false, shouldBeTruncatingLog = true, shouldBeDelayed = false)
    //Loop 1
    thread.doWork()
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(1, mockNetwork.fetchCount)

    assertPartitionStates(thread, shouldBeReadyForFetch = true, shouldBeTruncatingLog = false, shouldBeDelayed = false)

    //Loop 2 we should not fetch epochs
    thread.doWork()
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(2, mockNetwork.fetchCount)

    assertPartitionStates(thread, shouldBeReadyForFetch = true, shouldBeTruncatingLog = false, shouldBeDelayed = false)

    //Loop 3 we should not fetch epochs
    thread.doWork()
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(3, mockNetwork.fetchCount)

    assertPartitionStates(thread, shouldBeReadyForFetch = true, shouldBeTruncatingLog = false, shouldBeDelayed = false)

    //Assert that truncate to is called exactly once (despite two loops)
    verify(logManager)
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

      assertEquals(
        s"Partition $tp should${if (!shouldBeReadyForFetch) " NOT" else ""} be ready for fetching",
        shouldBeReadyForFetch, fetchState.isReadyForFetch)

      assertEquals(
        s"Partition $tp should${if (!shouldBeTruncatingLog) " NOT" else ""} be truncating its log",
        shouldBeTruncatingLog, fetchState.isTruncating)

      assertEquals(
        s"Partition $tp should${if (!shouldBeDelayed) " NOT" else ""} be delayed",
        shouldBeDelayed, fetchState.isDelayed)
    }
  }

  @Test
  def shouldHandleExceptionFromBlockingSend(): Unit = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    val config = KafkaConfig.fromProps(props)
    val mockBlockingSend: BlockingSend = createMock(classOf[BlockingSend])

    expect(mockBlockingSend.sendRequest(anyObject())).andThrow(new NullPointerException).once()
    replay(mockBlockingSend)

    val thread = new ReplicaFetcherThread(
      name = "bob",
      fetcherId = 0,
      sourceBroker = brokerEndPoint,
      brokerConfig = config,
      failedPartitions: FailedPartitions,
      replicaMgr = null,
      metrics =  new Metrics(),
      time = new SystemTime(),
      quota = null,
      leaderEndpointBlockingSend = Some(mockBlockingSend))

    val result = thread.fetchEpochEndOffsets(Map(
      t1p0 -> new OffsetsForLeaderEpochRequest.PartitionData(Optional.empty(), 0),
      t1p1 -> new OffsetsForLeaderEpochRequest.PartitionData(Optional.empty(), 0)))

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
    val logManager: LogManager = createMock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    val leaderEpoch = 5

    //Stubs
    expect(replica.logEndOffset).andReturn(0).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(0)).anyTimes()
    expect(replica.latestEpoch).andReturn(Some(leaderEpoch)).anyTimes()
    expect(replica.endOffsetForEpoch(leaderEpoch)).andReturn(
      Some(OffsetAndEpoch(0, leaderEpoch))).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)

    //Expectations
    expect(partition.truncateTo(anyLong(), anyBoolean())).once

    replay(replicaManager, logManager, replica)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsets = Map(t1p0 -> new EpochEndOffset(leaderEpoch, 1), t1p1 -> new EpochEndOffset(leaderEpoch, 1)).asJava

    //Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsets, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager,
      new Metrics, new SystemTime, UnboundedQuota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> offsetAndEpoch(0L), t1p1 -> offsetAndEpoch(0L)))

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
    val quota: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    val leaderEpoch = 5
    val initialLEO = 200

    //Stubs
    expect(partition.truncateTo(capture(truncateToCapture), anyBoolean())).anyTimes()
    expect(replica.logEndOffset).andReturn(initialLEO).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(initialLEO - 1)).anyTimes()
    expect(replica.latestEpoch).andReturn(Some(leaderEpoch)).anyTimes()
    expect(replica.endOffsetForEpoch(leaderEpoch)).andReturn(
      Some(OffsetAndEpoch(initialLEO, leaderEpoch))).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)

    replay(replicaManager, logManager, quota, replica, partition)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = Map(t1p0 -> new EpochEndOffset(leaderEpoch, 156), t2p1 -> new EpochEndOffset(leaderEpoch, 172)).asJava

    //Create the thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, configs(0), failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> offsetAndEpoch(0L), t2p1 -> offsetAndEpoch(0L)))

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
    val quota: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    val leaderEpochAtFollower = 5
    val leaderEpochAtLeader = 4
    val initialLEO = 200

    //Stubs
    expect(partition.truncateTo(capture(truncateToCapture), anyBoolean())).anyTimes()
    expect(replica.logEndOffset).andReturn(initialLEO).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(initialLEO - 3)).anyTimes()
    expect(replica.latestEpoch).andReturn(Some(leaderEpochAtFollower)).anyTimes()
    expect(replica.endOffsetForEpoch(leaderEpochAtLeader)).andReturn(None).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)

    replay(replicaManager, logManager, quota, replica, partition)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = Map(t1p0 -> new EpochEndOffset(leaderEpochAtLeader, 156),
                           t2p1 -> new EpochEndOffset(leaderEpochAtLeader, 202)).asJava

    //Create the thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, configs(0), failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> offsetAndEpoch(0L), t2p1 -> offsetAndEpoch(0L)))

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
    val quota: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    val initialLEO = 200

    // Stubs
    expect(partition.truncateTo(capture(truncateToCapture), anyBoolean())).anyTimes()
    expect(replica.logEndOffset).andReturn(initialLEO).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(initialLEO - 2)).anyTimes()
    expect(replica.latestEpoch).andReturn(Some(5)).anyTimes()
    expect(replica.endOffsetForEpoch(4)).andReturn(
      Some(OffsetAndEpoch(120, 3))).anyTimes()
    expect(replica.endOffsetForEpoch(3)).andReturn(
      Some(OffsetAndEpoch(120, 3))).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)

    replay(replicaManager, logManager, quota, replica, partition)

    // Define the offsets for the OffsetsForLeaderEpochResponse
    val offsets = Map(t1p0 -> new EpochEndOffset(4, 155), t1p1 -> new EpochEndOffset(4, 143)).asJava

    // Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsets, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> offsetAndEpoch(0L), t1p1 -> offsetAndEpoch(0L)))

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
      3, mockNetwork.lastUsedOffsetForLeaderEpochVersion)

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
    val quota: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    val initialLEO = 200

    // Stubs
    expect(partition.truncateTo(capture(truncateToCapture), anyBoolean())).anyTimes()
    expect(replica.logEndOffset).andReturn(initialLEO).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(initialLEO - 2)).anyTimes()
    expect(replica.latestEpoch).andReturn(Some(5)).anyTimes()
    expect(replica.endOffsetForEpoch(4)).andReturn(
      Some(OffsetAndEpoch(120, 3))).anyTimes()
    expect(replica.endOffsetForEpoch(3)).andReturn(
      Some(OffsetAndEpoch(120, 3))).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)

    replay(replicaManager, logManager, quota, replica, partition)

    // Define the offsets for the OffsetsForLeaderEpochResponse with undefined epoch to simulate
    // older protocol version
    val offsets = Map(t1p0 -> new EpochEndOffset(EpochEndOffset.UNDEFINED_EPOCH, 155), t1p1 -> new EpochEndOffset(EpochEndOffset.UNDEFINED_EPOCH, 143)).asJava

    // Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsets, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> offsetAndEpoch(0L), t1p1 -> offsetAndEpoch(0L)))

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
    val quota: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    val initialFetchOffset = 100
    val initialLeo = 300

    //Stubs
    expect(partition.truncateTo(capture(truncated), anyBoolean())).anyTimes()
    expect(replica.logEndOffset).andReturn(initialLeo).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(initialFetchOffset)).anyTimes()
    expect(replica.latestEpoch).andReturn(Some(5))
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)
    replay(replicaManager, logManager, quota, replica, partition)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = Map(t1p0 -> new EpochEndOffset(EpochEndOffset.UNDEFINED_EPOCH, EpochEndOffset.UNDEFINED_EPOCH_OFFSET)).asJava

    //Create the thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, configs(0), failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> offsetAndEpoch(initialFetchOffset)))

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
    val quota: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    val leaderEpoch = 5
    val highWaterMark = 100
    val initialLeo = 300

    //Stubs
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(highWaterMark)).anyTimes()
    expect(partition.truncateTo(capture(truncated), anyBoolean())).anyTimes()
    expect(replica.logEndOffset).andReturn(initialLeo).anyTimes()
    expect(replica.latestEpoch).andReturn(Some(leaderEpoch)).anyTimes()
    // this is for the last reply with EpochEndOffset(5, 156)
    expect(replica.endOffsetForEpoch(leaderEpoch)).andReturn(
      Some(OffsetAndEpoch(initialLeo, leaderEpoch))).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)
    replay(replicaManager, logManager, quota, replica, partition)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = mutable.Map(
      t1p0 -> new EpochEndOffset(NOT_LEADER_FOR_PARTITION, EpochEndOffset.UNDEFINED_EPOCH, EpochEndOffset.UNDEFINED_EPOCH_OFFSET),
      t1p1 -> new EpochEndOffset(UNKNOWN_SERVER_ERROR, EpochEndOffset.UNDEFINED_EPOCH, EpochEndOffset.UNDEFINED_EPOCH_OFFSET)
    ).asJava

    //Create the thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, configs(0), failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> offsetAndEpoch(0L), t1p1 -> offsetAndEpoch(0L)))

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
    val quota: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createNiceMock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createNiceMock(classOf[ReplicaManager])

    val leaderEpoch = 4

    //Stub return values
    expect(replica.logEndOffset).andReturn(0).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(0)).anyTimes()
    expect(replica.latestEpoch).andReturn(Some(leaderEpoch)).anyTimes()
    expect(replica.endOffsetForEpoch(leaderEpoch)).andReturn(
      Some(OffsetAndEpoch(0, leaderEpoch))).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)

    replay(replicaManager, logManager, quota, replica)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsetsReply = Map(
      t1p0 -> new EpochEndOffset(leaderEpoch, 1), t1p1 -> new EpochEndOffset(leaderEpoch, 1)
    ).asJava

    //Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))

    //When
    thread.addPartitions(Map(t1p0 -> offsetAndEpoch(0L), t1p1 -> offsetAndEpoch(0L)))

    //Then all partitions should start in an TruncatingLog state
    assertEquals(Option(Truncating), thread.fetchState(t1p0).map(_.state))
    assertEquals(Option(Truncating), thread.fetchState(t1p1).map(_.state))

    //When
    thread.doWork()

    //Then none should be TruncatingLog anymore
    assertEquals(Option(Fetching), thread.fetchState(t1p0).map(_.state))
    assertEquals(Option(Fetching), thread.fetchState(t1p1).map(_.state))
  }

  @Test
  def shouldFilterPartitionsMadeLeaderDuringLeaderEpochRequest(): Unit ={
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val truncateToCapture: Capture[Long] = newCapture(CaptureType.ALL)
    val initialLEO = 100

    //Setup all stubs
    val quota: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createNiceMock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = createMock(classOf[ReplicaAlterLogDirsManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createNiceMock(classOf[ReplicaManager])

    //Stub return values
    expect(partition.truncateTo(capture(truncateToCapture), anyBoolean())).once
    expect(replica.logEndOffset).andReturn(initialLEO).anyTimes()
    expect(replica.highWatermark).andReturn(new LogOffsetMetadata(initialLEO - 2)).anyTimes()
    expect(replica.latestEpoch).andReturn(Some(5)).anyTimes()
    expect(replica.endOffsetForEpoch(5)).andReturn(Some(OffsetAndEpoch(initialLEO, 5))).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.replicaAlterLogDirsManager).andReturn(replicaAlterLogDirsManager).anyTimes()
    stub(replica, partition, replicaManager)

    replay(replicaManager, logManager, quota, replica, partition)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsetsReply = Map(
      t1p0 -> new EpochEndOffset(5, 52), t1p1 -> new EpochEndOffset(5, 49)
    ).asJava

    //Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))

    //When
    thread.addPartitions(Map(t1p0 -> offsetAndEpoch(0L), t1p1 -> offsetAndEpoch(0L)))

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

  @Test
  def shouldCatchExceptionFromBlockingSendWhenShuttingDownReplicaFetcherThread(): Unit = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    val config = KafkaConfig.fromProps(props)
    val mockBlockingSend: BlockingSend = createMock(classOf[BlockingSend])

    expect(mockBlockingSend.initiateClose()).andThrow(new IllegalArgumentException()).once()
    expect(mockBlockingSend.close()).andThrow(new IllegalStateException()).once()
    replay(mockBlockingSend)

    val thread = new ReplicaFetcherThread(
      name = "bob",
      fetcherId = 0,
      sourceBroker = brokerEndPoint,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = null,
      metrics =  new Metrics(),
      time = new SystemTime(),
      quota = null,
      leaderEndpointBlockingSend = Some(mockBlockingSend))
    thread.start()

    // Verify that:
    //   1) IllegalArgumentException thrown by BlockingSend#initiateClose() during `initiateShutdown` is not propagated
    //   2) BlockingSend.close() is invoked even if BlockingSend#initiateClose() fails
    //   3) IllegalStateException thrown by BlockingSend.close() during `awaitShutdown` is not propagated
    thread.initiateShutdown()
    thread.awaitShutdown()
    verify(mockBlockingSend)
  }

  def stub(replica: Replica, partition: Partition, replicaManager: ReplicaManager) = {
    expect(replicaManager.localReplica(t1p0)).andReturn(Some(replica)).anyTimes()
    expect(replicaManager.localReplicaOrException(t1p0)).andReturn(replica).anyTimes()
    expect(replicaManager.getPartition(t1p0)).andReturn(Some(partition)).anyTimes()
    expect(replicaManager.localReplica(t1p1)).andReturn(Some(replica)).anyTimes()
    expect(replicaManager.localReplicaOrException(t1p1)).andReturn(replica).anyTimes()
    expect(replicaManager.getPartition(t1p1)).andReturn(Some(partition)).anyTimes()
    expect(replicaManager.localReplica(t2p1)).andReturn(Some(replica)).anyTimes()
    expect(replicaManager.localReplicaOrException(t2p1)).andReturn(replica).anyTimes()
    expect(replicaManager.getPartition(t2p1)).andReturn(Some(partition)).anyTimes()
  }
}
