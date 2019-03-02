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
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import kafka.utils.{DelayedItem, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{EpochEndOffset, OffsetsForLeaderEpochRequest}
import org.apache.kafka.common.requests.EpochEndOffset.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}
import org.easymock.EasyMock._
import org.easymock.{Capture, CaptureType, EasyMock, IAnswer}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.{Map, Seq}

class ReplicaAlterLogDirsThreadTest {

  private val t1p0 = new TopicPartition("topic1", 0)
  private val t1p1 = new TopicPartition("topic1", 1)

  private def offsetAndEpoch(fetchOffset: Long, leaderEpoch: Int = 1): OffsetAndEpoch = {
    OffsetAndEpoch(offset = fetchOffset, leaderEpoch = leaderEpoch)
  }

  @Test
  def issuesEpochRequestFromLocalReplica(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))

    //Setup all dependencies
    val partitionT1p0: Partition = createMock(classOf[Partition])
    val partitionT1p1: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    val leaderEpochT1p0 = 2
    val leaderEpochT1p1 = 5
    val leoT1p0 = 13
    val leoT1p1 = 232

    //Stubs
    expect(replicaManager.getPartitionOrException(t1p0, expectLeader = false))
      .andStubReturn(partitionT1p0)
    expect(partitionT1p0.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpochT1p0, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset(leaderEpochT1p0, leoT1p0))
      .anyTimes()

    expect(replicaManager.getPartitionOrException(t1p1, expectLeader = false))
      .andStubReturn(partitionT1p1)
    expect(partitionT1p1.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpochT1p1, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset(leaderEpochT1p1, leoT1p1))
      .anyTimes()

    replay(partitionT1p0, partitionT1p1, replicaManager)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = replicaManager,
      quota = null,
      brokerTopicStats = null)

    val result = thread.fetchEpochEndOffsets(Map(
      t1p0 -> new OffsetsForLeaderEpochRequest.PartitionData(Optional.empty(), leaderEpochT1p0),
      t1p1 -> new OffsetsForLeaderEpochRequest.PartitionData(Optional.empty(), leaderEpochT1p1)))

    val expected = Map(
      t1p0 -> new EpochEndOffset(Errors.NONE, leaderEpochT1p0, leoT1p0),
      t1p1 -> new EpochEndOffset(Errors.NONE, leaderEpochT1p1, leoT1p1)
    )

    assertEquals("results from leader epoch request should have offset from local replica",
                 expected, result)
  }

  @Test
  def fetchEpochsFromLeaderShouldHandleExceptionFromGetLocalReplica(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))

    //Setup all dependencies
    val partitionT1p0: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    val leaderEpoch = 2
    val leo = 13

    //Stubs
    expect(replicaManager.getPartitionOrException(t1p0, expectLeader = false))
      .andStubReturn(partitionT1p0)
    expect(partitionT1p0.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset(leaderEpoch, leo))
      .anyTimes()

    expect(replicaManager.getPartitionOrException(t1p1, expectLeader = false))
      .andThrow(new KafkaStorageException).once()

    replay(partitionT1p0, replicaManager)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = replicaManager,
      quota = null,
      brokerTopicStats = null)

    val result = thread.fetchEpochEndOffsets(Map(
      t1p0 -> new OffsetsForLeaderEpochRequest.PartitionData(Optional.empty(), leaderEpoch),
      t1p1 -> new OffsetsForLeaderEpochRequest.PartitionData(Optional.empty(), leaderEpoch)))

    val expected = Map(
      t1p0 -> new EpochEndOffset(Errors.NONE, leaderEpoch, leo),
      t1p1 -> new EpochEndOffset(Errors.KAFKA_STORAGE_ERROR, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
    )

    assertEquals(expected, result)
  }

  @Test
  def shouldTruncateToReplicaOffset(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncateCaptureT1p0: Capture[Long] = newCapture(CaptureType.ALL)
    val truncateCaptureT1p1: Capture[Long] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val replicaT1p0: Replica = createNiceMock(classOf[Replica])
    val replicaT1p1: Replica = createNiceMock(classOf[Replica])
    // one future replica mock because our mocking methods return same values for both future replicas
    val futureReplicaT1p0: Replica = createNiceMock(classOf[Replica])
    val futureReplicaT1p1: Replica = createNiceMock(classOf[Replica])
    val partitionT1p0: Partition = createMock(classOf[Partition])
    val partitionT1p1: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])
    val responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]  = EasyMock.newCapture()

    val leaderEpoch = 2
    val futureReplicaLEO = 191
    val replicaT1p0LEO = 190
    val replicaT1p1LEO = 192

    //Stubs
    expect(replicaManager.getPartitionOrException(t1p0, expectLeader = false))
      .andStubReturn(partitionT1p0)
    expect(replicaManager.getPartitionOrException(t1p1, expectLeader = false))
      .andStubReturn(partitionT1p1)
    expect(replicaManager.futureLocalReplicaOrException(t1p0)).andStubReturn(futureReplicaT1p0)
    expect(replicaManager.futureLocalReplicaOrException(t1p1)).andStubReturn(futureReplicaT1p1)
    expect(partitionT1p0.truncateTo(capture(truncateCaptureT1p0), anyBoolean())).anyTimes()
    expect(partitionT1p1.truncateTo(capture(truncateCaptureT1p1), anyBoolean())).anyTimes()

    expect(futureReplicaT1p0.logEndOffset).andReturn(futureReplicaLEO).anyTimes()
    expect(futureReplicaT1p1.logEndOffset).andReturn(futureReplicaLEO).anyTimes()

    expect(futureReplicaT1p0.latestEpoch).andReturn(Some(leaderEpoch)).anyTimes()
    expect(futureReplicaT1p0.endOffsetForEpoch(leaderEpoch)).andReturn(
      Some(OffsetAndEpoch(futureReplicaLEO, leaderEpoch))).anyTimes()
    expect(partitionT1p0.lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset(leaderEpoch, replicaT1p0LEO))
      .anyTimes()

    expect(futureReplicaT1p1.latestEpoch).andReturn(Some(leaderEpoch)).anyTimes()
    expect(futureReplicaT1p1.endOffsetForEpoch(leaderEpoch)).andReturn(
      Some(OffsetAndEpoch(futureReplicaLEO, leaderEpoch))).anyTimes()
    expect(partitionT1p1.lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset(leaderEpoch, replicaT1p1LEO))
      .anyTimes()

    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stubWithFetchMessages(replicaT1p0, replicaT1p1, futureReplicaT1p0, partitionT1p0, replicaManager, responseCallback)

    replay(replicaManager, logManager, quotaManager, replicaT1p0, replicaT1p1,
      futureReplicaT1p0, partitionT1p0, partitionT1p1)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(t1p0 -> offsetAndEpoch(0L), t1p1 -> offsetAndEpoch(0L)))

    //Run it
    thread.doWork()

    //We should have truncated to the offsets in the response
    assertEquals(replicaT1p0LEO, truncateCaptureT1p0.getValue)
    assertEquals(futureReplicaLEO, truncateCaptureT1p1.getValue)
  }

  @Test
  def shouldTruncateToEndOffsetOfLargestCommonEpoch(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncateToCapture: Capture[Long] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    // one future replica mock because our mocking methods return same values for both future replicas
    val futureReplica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])
    val responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]  = EasyMock.newCapture()

    val leaderEpoch = 5
    val futureReplicaLEO = 195
    val replicaLEO = 200
    val replicaEpochEndOffset = 190
    val futureReplicaEpochEndOffset = 191

    //Stubs
    expect(replicaManager.getPartitionOrException(t1p0, expectLeader = false))
      .andStubReturn(partition)
    expect(replicaManager.futureLocalReplicaOrException(t1p0)).andStubReturn(futureReplica)

    expect(partition.truncateTo(capture(truncateToCapture), EasyMock.eq(true))).anyTimes()
    expect(futureReplica.logEndOffset).andReturn(futureReplicaLEO).anyTimes()
    expect(futureReplica.latestEpoch).andReturn(Some(leaderEpoch)).once()
    expect(futureReplica.latestEpoch).andReturn(Some(leaderEpoch - 2)).once()

    // leader replica truncated and fetched new offsets with new leader epoch
    expect(partition.lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset(leaderEpoch - 1, replicaLEO))
      .anyTimes()
    // but future replica does not know about this leader epoch, so returns a smaller leader epoch
    expect(futureReplica.endOffsetForEpoch(leaderEpoch - 1)).andReturn(
      Some(OffsetAndEpoch(futureReplicaLEO, leaderEpoch - 2))).anyTimes()
    // finally, the leader replica knows about the leader epoch and returns end offset
    expect(partition.lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch - 2, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset(leaderEpoch - 2, replicaEpochEndOffset))
      .anyTimes()
    expect(futureReplica.endOffsetForEpoch(leaderEpoch - 2)).andReturn(
      Some(OffsetAndEpoch(futureReplicaEpochEndOffset, leaderEpoch - 2))).anyTimes()

    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stubWithFetchMessages(replica, replica, futureReplica, partition, replicaManager, responseCallback)

    replay(replicaManager, logManager, quotaManager, replica, futureReplica, partition)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(t1p0 -> offsetAndEpoch(0L)))

    // First run will result in another offset for leader epoch request
    thread.doWork()
    // Second run should actually truncate
    thread.doWork()

    //We should have truncated to the offsets in the response
    assertTrue("Expected offset " + replicaEpochEndOffset + " in captured truncation offsets " + truncateToCapture.getValues,
               truncateToCapture.getValues.asScala.contains(replicaEpochEndOffset))
  }

  @Test
  def shouldTruncateToInitialFetchOffsetIfReplicaReturnsUndefinedOffset(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncated: Capture[Long] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    val futureReplica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])
    val responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]  = EasyMock.newCapture()

    val initialFetchOffset = 100
    val futureReplicaLEO = 111

    //Stubs
    expect(replicaManager.getPartitionOrException(t1p0, expectLeader = false))
      .andStubReturn(partition)
    expect(partition.truncateTo(capture(truncated), isFuture = EasyMock.eq(true))).anyTimes()
    expect(replicaManager.futureLocalReplicaOrException(t1p0)).andStubReturn(futureReplica)

    expect(futureReplica.logEndOffset).andReturn(futureReplicaLEO).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()

    // pretend this is a completely new future replica, with no leader epochs recorded
    expect(futureReplica.latestEpoch).andReturn(None).anyTimes()

    stubWithFetchMessages(replica, replica, futureReplica, partition, replicaManager, responseCallback)
    replay(replicaManager, logManager, quotaManager, replica, futureReplica, partition)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(t1p0 -> offsetAndEpoch(initialFetchOffset)))

    //Run it
    thread.doWork()

    //We should have truncated to initial fetch offset
    assertEquals("Expected future replica to truncate to initial fetch offset if replica returns UNDEFINED_EPOCH_OFFSET",
                 initialFetchOffset, truncated.getValue)
  }

  @Test
  def shouldPollIndefinitelyIfReplicaNotAvailable(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncated: Capture[Long] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    val futureReplica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])
    val responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]  = EasyMock.newCapture()

    val futureReplicaLeaderEpoch = 1
    val futureReplicaLEO = 290
    val replicaLEO = 300

    //Stubs
    expect(replicaManager.getPartitionOrException(t1p0, expectLeader = false))
      .andStubReturn(partition)
    expect(partition.truncateTo(capture(truncated), isFuture = EasyMock.eq(true))).once()

    expect(replicaManager.futureLocalReplicaOrException(t1p0)).andStubReturn(futureReplica)
    expect(futureReplica.latestEpoch).andStubReturn(Some(futureReplicaLeaderEpoch))
    expect(futureReplica.endOffsetForEpoch(futureReplicaLeaderEpoch)).andReturn(
      Some(OffsetAndEpoch(futureReplicaLEO, futureReplicaLeaderEpoch)))
    expect(futureReplica.logEndOffset).andReturn(futureReplicaLEO).anyTimes()
    expect(replicaManager.localReplica(t1p0)).andReturn(Some(replica)).anyTimes()
    expect(replicaManager.futureLocalReplica(t1p0)).andReturn(Some(futureReplica)).anyTimes()
    expect(replicaManager.futureLocalReplicaOrException(t1p0)).andReturn(futureReplica).anyTimes()

    // this will cause fetchEpochsFromLeader return an error with undefined offset
    expect(partition.lastOffsetForLeaderEpoch(Optional.of(1), futureReplicaLeaderEpoch, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset(Errors.REPLICA_NOT_AVAILABLE, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET))
      .times(3)
      .andReturn(new EpochEndOffset(futureReplicaLeaderEpoch, replicaLEO))

    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.fetchMessages(
      EasyMock.anyLong(),
      EasyMock.anyInt(),
      EasyMock.anyInt(),
      EasyMock.anyInt(),
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.capture(responseCallback),
      EasyMock.anyObject()))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          responseCallback.getValue.apply(Seq.empty[(TopicPartition, FetchPartitionData)])
        }
      }).anyTimes()

    replay(replicaManager, logManager, quotaManager, replica, futureReplica, partition)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(t1p0 -> offsetAndEpoch(0L)))

    // Run thread 3 times (exactly number of times we mock exception for getReplicaOrException)
    (0 to 2).foreach { _ =>
      thread.doWork()
    }

    // Nothing happened since the replica was not available
    assertEquals(0, truncated.getValues.size())

    // Next time we loop, getReplicaOrException will return replica
    thread.doWork()

    // Now the final call should have actually done a truncation (to offset futureReplicaLEO)
    assertEquals(futureReplicaLEO, truncated.getValue)
  }

  @Test
  def shouldFetchLeaderEpochOnFirstFetchOnly(): Unit = {

    //Setup all dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    val futureReplica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])
    val responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]  = EasyMock.newCapture()

    val leaderEpoch = 5
    val futureReplicaLEO = 190
    val replicaLEO = 213

    expect(replicaManager.getPartitionOrException(t1p0, expectLeader = false))
        .andStubReturn(partition)
    expect(partition.lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch, fetchOnlyFromLeader = false))
        .andReturn(new EpochEndOffset(leaderEpoch, replicaLEO))
    expect(partition.truncateTo(futureReplicaLEO, isFuture = true)).once()

    expect(replicaManager.futureLocalReplicaOrException(t1p0)).andStubReturn(futureReplica)
    expect(futureReplica.latestEpoch).andStubReturn(Some(leaderEpoch))
    expect(futureReplica.logEndOffset).andReturn(futureReplicaLEO).anyTimes()
    expect(futureReplica.endOffsetForEpoch(leaderEpoch)).andReturn(
      Some(OffsetAndEpoch(futureReplicaLEO, leaderEpoch)))
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stubWithFetchMessages(replica, replica, futureReplica, partition, replicaManager, responseCallback)

    replay(replicaManager, logManager, quotaManager, replica, futureReplica, partition)

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(t1p0 -> offsetAndEpoch(0L)))

    // loop few times
    (0 to 3).foreach { _ =>
      thread.doWork()
    }

    //Assert that truncate to is called exactly once (despite more loops)
    verify(partition)
  }

  @Test
  def shouldFetchOneReplicaAtATime(): Unit = {

    //Setup all dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    val futureReplica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    //Stubs
    expect(futureReplica.logStartOffset).andReturn(123).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stub(replica, replica, futureReplica, partition, replicaManager)

    replay(replicaManager, logManager, quotaManager, replica, futureReplica, partition)

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leaderEpoch = 1
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(
      t1p0 -> offsetAndEpoch(0L, leaderEpoch),
      t1p1 -> offsetAndEpoch(0L, leaderEpoch)))

    val ResultWithPartitions(fetchRequestOpt, partitionsWithError) = thread.buildFetch(Map(
      t1p0 -> PartitionFetchState(150, leaderEpoch, state = Fetching),
      t1p1 -> PartitionFetchState(160, leaderEpoch, state = Fetching)))

    assertTrue(fetchRequestOpt.isDefined)
    val fetchRequest = fetchRequestOpt.get
    assertFalse(fetchRequest.fetchData.isEmpty)
    assertFalse(partitionsWithError.nonEmpty)
    val request = fetchRequest.build()
    assertEquals(0, request.minBytes)
    val fetchInfos = request.fetchData.asScala.toSeq
    assertEquals(1, fetchInfos.length)
    assertEquals("Expected fetch request for largest partition", t1p1, fetchInfos.head._1)
    assertEquals(160, fetchInfos.head._2.fetchOffset)
  }

  @Test
  def shouldFetchNonDelayedAndNonTruncatingReplicas(): Unit = {

    //Setup all dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = createMock(classOf[LogManager])
    val replica: Replica = createNiceMock(classOf[Replica])
    val futureReplica: Replica = createNiceMock(classOf[Replica])
    val partition: Partition = createMock(classOf[Partition])
    val replicaManager: ReplicaManager = createMock(classOf[ReplicaManager])

    //Stubs
    expect(futureReplica.logStartOffset).andReturn(123).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stub(replica, replica, futureReplica, partition, replicaManager)

    replay(replicaManager, logManager, quotaManager, replica, futureReplica, partition)

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leaderEpoch = 1
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(
      t1p0 -> offsetAndEpoch(0L, leaderEpoch),
      t1p1 -> offsetAndEpoch(0L, leaderEpoch)))

    // one partition is ready and one is truncating
    val ResultWithPartitions(fetchRequestOpt, partitionsWithError) = thread.buildFetch(Map(
        t1p0 -> PartitionFetchState(150, leaderEpoch, state = Fetching),
        t1p1 -> PartitionFetchState(160, leaderEpoch, state = Truncating)))

    assertTrue(fetchRequestOpt.isDefined)
    val fetchRequest = fetchRequestOpt.get
    assertFalse(fetchRequest.fetchData.isEmpty)
    assertFalse(partitionsWithError.nonEmpty)
    val fetchInfos = fetchRequest.build().fetchData.asScala.toSeq
    assertEquals(1, fetchInfos.length)
    assertEquals("Expected fetch request for non-truncating partition", t1p0, fetchInfos.head._1)
    assertEquals(150, fetchInfos.head._2.fetchOffset)

    // one partition is ready and one is delayed
    val ResultWithPartitions(fetchRequest2Opt, partitionsWithError2) = thread.buildFetch(Map(
        t1p0 -> PartitionFetchState(140, leaderEpoch, state = Fetching),
        t1p1 -> PartitionFetchState(160, leaderEpoch, delay = new DelayedItem(5000), state = Fetching)))

    assertTrue(fetchRequest2Opt.isDefined)
    val fetchRequest2 = fetchRequest2Opt.get
    assertFalse(fetchRequest2.fetchData.isEmpty)
    assertFalse(partitionsWithError2.nonEmpty)
    val fetchInfos2 = fetchRequest2.build().fetchData.asScala.toSeq
    assertEquals(1, fetchInfos2.length)
    assertEquals("Expected fetch request for non-delayed partition", t1p0, fetchInfos2.head._1)
    assertEquals(140, fetchInfos2.head._2.fetchOffset)

    // both partitions are delayed
    val ResultWithPartitions(fetchRequest3Opt, partitionsWithError3) = thread.buildFetch(Map(
        t1p0 -> PartitionFetchState(140, leaderEpoch, delay = new DelayedItem(5000), state = Fetching),
        t1p1 -> PartitionFetchState(160, leaderEpoch, delay = new DelayedItem(5000), state = Fetching)))
    assertTrue("Expected no fetch requests since all partitions are delayed", fetchRequest3Opt.isEmpty)
    assertFalse(partitionsWithError3.nonEmpty)
  }

  def stub(replicaT1p0: Replica, replicaT1p1: Replica, futureReplica: Replica, partition: Partition, replicaManager: ReplicaManager) = {
    expect(replicaManager.localReplica(t1p0)).andReturn(Some(replicaT1p0)).anyTimes()
    expect(replicaManager.futureLocalReplica(t1p0)).andReturn(Some(futureReplica)).anyTimes()
    expect(replicaManager.localReplicaOrException(t1p0)).andReturn(replicaT1p0).anyTimes()
    expect(replicaManager.futureLocalReplicaOrException(t1p0)).andReturn(futureReplica).anyTimes()
    expect(replicaManager.getPartition(t1p0)).andReturn(Some(partition)).anyTimes()
    expect(replicaManager.localReplica(t1p1)).andReturn(Some(replicaT1p1)).anyTimes()
    expect(replicaManager.futureLocalReplica(t1p1)).andReturn(Some(futureReplica)).anyTimes()
    expect(replicaManager.localReplicaOrException(t1p1)).andReturn(replicaT1p1).anyTimes()
    expect(replicaManager.futureLocalReplicaOrException(t1p1)).andReturn(futureReplica).anyTimes()
    expect(replicaManager.getPartition(t1p1)).andReturn(Some(partition)).anyTimes()
  }

  def stubWithFetchMessages(replicaT1p0: Replica, replicaT1p1: Replica, futureReplica: Replica,
                            partition: Partition, replicaManager: ReplicaManager,
                            responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]) = {
    stub(replicaT1p0, replicaT1p1, futureReplica, partition, replicaManager)
    expect(replicaManager.fetchMessages(
      EasyMock.anyLong(),
      EasyMock.anyInt(),
      EasyMock.anyInt(),
      EasyMock.anyInt(),
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.capture(responseCallback),
      EasyMock.anyObject()))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          responseCallback.getValue.apply(Seq.empty[(TopicPartition, FetchPartitionData)])
        }
      }).anyTimes()
  }
}
