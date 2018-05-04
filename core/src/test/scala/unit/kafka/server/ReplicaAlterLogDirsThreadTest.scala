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


import kafka.api.Request
import kafka.cluster.{BrokerEndPoint, Replica, Partition}
import kafka.log.LogManager
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import kafka.server.FetchPartitionData
import kafka.server.epoch.LeaderEpochCache
import org.apache.kafka.common.errors.{ReplicaNotAvailableException, KafkaStorageException}
import kafka.utils.{DelayedItem, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{EpochEndOffset, FetchResponse, FetchMetadata => JFetchMetadata}
import org.apache.kafka.common.requests.FetchResponse.PartitionData
import org.apache.kafka.common.requests.EpochEndOffset.{UNDEFINED_EPOCH_OFFSET, UNDEFINED_EPOCH}
import org.apache.kafka.common.utils.SystemTime
import org.easymock.EasyMock._
import org.easymock.{Capture, CaptureType, EasyMock, IAnswer}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.collection.{Map, mutable}

class ReplicaAlterLogDirsThreadTest {

  private val t1p0 = new TopicPartition("topic1", 0)
  private val t1p1 = new TopicPartition("topic1", 1)

  @Test
  def issuesEpochRequestFromLocalReplica(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))

    //Setup all dependencies
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val replica = createNiceMock(classOf[Replica])
    val futureReplica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createMock(classOf[ReplicaManager])

    val leaderEpoch = 2
    val leo = 13

    //Stubs
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(leaderEpochs.endOffsetFor(leaderEpoch)).andReturn(leo).anyTimes()
    stub(replica, replica, futureReplica, partition, replicaManager)

    replay(leaderEpochs, replicaManager, replica)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = replicaManager,
      quota = null,
      brokerTopicStats = null)

    val result = thread.fetchEpochsFromLeader(Map(t1p0 -> leaderEpoch, t1p1 -> leaderEpoch))

    val expected = Map(
      t1p0 -> new EpochEndOffset(Errors.NONE, leo),
      t1p1 -> new EpochEndOffset(Errors.NONE, leo)
    )

    assertEquals("results from leader epoch request should have offset from local replica",
                 expected, result)
  }

  @Test
  def fetchEpochsFromLeaderShouldHandleExceptionFromGetLocalReplica(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))

    //Setup all dependencies
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val replica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createMock(classOf[ReplicaManager])

    val leaderEpoch = 2
    val leo = 13

    //Stubs
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(leaderEpochs.endOffsetFor(leaderEpoch)).andReturn(leo).anyTimes()
    expect(replicaManager.getReplicaOrException(t1p0)).andReturn(replica).anyTimes()
    expect(replicaManager.getPartition(t1p0)).andReturn(Some(partition)).anyTimes()
    expect(replicaManager.getReplicaOrException(t1p1)).andThrow(new KafkaStorageException).once()
    expect(replicaManager.getPartition(t1p1)).andReturn(Some(partition)).anyTimes()

    replay(leaderEpochs, replicaManager, replica)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = replicaManager,
      quota = null,
      brokerTopicStats = null)

    val result = thread.fetchEpochsFromLeader(Map(t1p0 -> leaderEpoch, t1p1 -> leaderEpoch))

    val expected = Map(
      t1p0 -> new EpochEndOffset(Errors.NONE, leo),
      t1p1 -> new EpochEndOffset(Errors.KAFKA_STORAGE_ERROR, UNDEFINED_EPOCH_OFFSET)
    )

    assertEquals(expected, result)
  }

  @Test
  def shouldTruncateToReplicaOffset(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncateToCapture: Capture[Long] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val leaderEpochsT1p0 = createMock(classOf[LeaderEpochCache])
    val leaderEpochsT1p1 = createMock(classOf[LeaderEpochCache])
    val futureReplicaLeaderEpochs = createMock(classOf[LeaderEpochCache])
    val logManager = createMock(classOf[LogManager])
    val replicaT1p0 = createNiceMock(classOf[Replica])
    val replicaT1p1 = createNiceMock(classOf[Replica])
    // one future replica mock because our mocking methods return same values for both future replicas
    val futureReplica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createMock(classOf[ReplicaManager])
    val responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]  = EasyMock.newCapture()

    val leaderEpoch = 2
    val futureReplicaLEO = 191
    val replicaT1p0LEO = 190
    val replicaT1p1LEO = 192

    //Stubs
    expect(partition.truncateTo(capture(truncateToCapture), anyBoolean())).anyTimes()
    expect(replicaT1p0.epochs).andReturn(Some(leaderEpochsT1p0)).anyTimes()
    expect(replicaT1p1.epochs).andReturn(Some(leaderEpochsT1p1)).anyTimes()
    expect(futureReplica.epochs).andReturn(Some(futureReplicaLeaderEpochs)).anyTimes()
    expect(futureReplica.logEndOffset).andReturn(new LogOffsetMetadata(futureReplicaLEO)).anyTimes()
    expect(futureReplicaLeaderEpochs.latestEpoch).andReturn(leaderEpoch).anyTimes()
    expect(leaderEpochsT1p0.endOffsetFor(leaderEpoch)).andReturn(replicaT1p0LEO).anyTimes()
    expect(leaderEpochsT1p1.endOffsetFor(leaderEpoch)).andReturn(replicaT1p1LEO).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stubWithFetchMessages(replicaT1p0, replicaT1p1, futureReplica, partition, replicaManager, responseCallback)

    replay(leaderEpochsT1p0, leaderEpochsT1p1, futureReplicaLeaderEpochs, replicaManager,
           logManager, quotaManager, replicaT1p0, replicaT1p1, futureReplica, partition)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(t1p0 -> 0, t1p1 -> 0))

    //Run it
    thread.doWork()

    //We should have truncated to the offsets in the response
    assertTrue(truncateToCapture.getValues.asScala.contains(replicaT1p0LEO))
    assertTrue(truncateToCapture.getValues.asScala.contains(futureReplicaLEO))
  }

  @Test
  def shouldTruncateToInitialFetchOffsetIfReplicaReturnsUndefinedOffset(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncated: Capture[Long] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
    val quotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager = createMock(classOf[LogManager])
    val replica = createNiceMock(classOf[Replica])
    val futureReplica = createNiceMock(classOf[Replica])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val futureReplicaLeaderEpochs = createMock(classOf[LeaderEpochCache])
    val partition = createMock(classOf[Partition])
    val replicaManager = createMock(classOf[ReplicaManager])
    val responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]  = EasyMock.newCapture()

    val initialFetchOffset = 100
    val futureReplicaLEO = 111

    //Stubs
    expect(partition.truncateTo(capture(truncated), anyBoolean())).anyTimes()
    expect(futureReplica.logEndOffset).andReturn(new LogOffsetMetadata(futureReplicaLEO)).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(futureReplica.epochs).andReturn(Some(futureReplicaLeaderEpochs)).anyTimes()

    // pretend this is a completely new future replica, with no leader epochs recorded
    expect(futureReplicaLeaderEpochs.latestEpoch).andReturn(UNDEFINED_EPOCH).anyTimes()

    // since UNDEFINED_EPOCH is -1 wich will be lower than any valid leader epoch, the method
    // will return UNDEFINED_EPOCH_OFFSET if requested epoch is lower than the first epoch cached
    expect(leaderEpochs.endOffsetFor(UNDEFINED_EPOCH)).andReturn(UNDEFINED_EPOCH_OFFSET).anyTimes()
    stubWithFetchMessages(replica, replica, futureReplica, partition, replicaManager, responseCallback)
    replay(replicaManager, logManager, quotaManager, leaderEpochs, futureReplicaLeaderEpochs,
           replica, futureReplica, partition)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(t1p0 -> initialFetchOffset))

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
    val quotaManager = createNiceMock(classOf[kafka.server.ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val futureReplicaLeaderEpochs = createMock(classOf[LeaderEpochCache])
    val logManager = createMock(classOf[kafka.log.LogManager])
    val replica = createNiceMock(classOf[Replica])
    val futureReplica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createMock(classOf[kafka.server.ReplicaManager])
    val responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]  = EasyMock.newCapture()

    val futureReplicaLeaderEpoch = 1
    val futureReplicaLEO = 290
    val replicaLEO = 300

    //Stubs
    expect(partition.truncateTo(capture(truncated), anyBoolean())).anyTimes()
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(futureReplica.epochs).andReturn(Some(futureReplicaLeaderEpochs)).anyTimes()

    expect(futureReplicaLeaderEpochs.latestEpoch).andReturn(futureReplicaLeaderEpoch).anyTimes()
    expect(leaderEpochs.endOffsetFor(futureReplicaLeaderEpoch)).andReturn(replicaLEO).anyTimes()

    expect(futureReplica.logEndOffset).andReturn(new LogOffsetMetadata(futureReplicaLEO)).anyTimes()
    expect(replicaManager.getReplica(t1p0)).andReturn(Some(replica)).anyTimes()
    expect(replicaManager.getReplica(t1p0, Request.FutureLocalReplicaId)).andReturn(Some(futureReplica)).anyTimes()
    expect(replicaManager.getReplicaOrException(t1p0, Request.FutureLocalReplicaId)).andReturn(futureReplica).anyTimes()
    // this will cause fetchEpochsFromLeader return an error with undefined offset
    expect(replicaManager.getReplicaOrException(t1p0)).andThrow(new ReplicaNotAvailableException("")).times(3)
    expect(replicaManager.getReplicaOrException(t1p0)).andReturn(replica).once()
    expect(replicaManager.getPartition(t1p0)).andReturn(Some(partition)).anyTimes()
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

    replay(leaderEpochs, futureReplicaLeaderEpochs, replicaManager, logManager, quotaManager,
           replica, futureReplica, partition)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(t1p0 -> 0))

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
    val quotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochCache])
    val futureReplicaLeaderEpochs = createMock(classOf[LeaderEpochCache])
    val logManager = createMock(classOf[LogManager])
    val replica = createNiceMock(classOf[Replica])
    val futureReplica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createMock(classOf[ReplicaManager])
    val responseCallback: Capture[Seq[(TopicPartition, FetchPartitionData)] => Unit]  = EasyMock.newCapture()

    val leaderEpoch = 5
    val futureReplicaLEO = 190
    val replicaLEO = 213

    //Stubs
    expect(partition.truncateTo(futureReplicaLEO, true)).once()
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(futureReplica.epochs).andReturn(Some(futureReplicaLeaderEpochs)).anyTimes()

    expect(futureReplica.logEndOffset).andReturn(new LogOffsetMetadata(futureReplicaLEO)).anyTimes()
    expect(futureReplicaLeaderEpochs.latestEpoch).andReturn(leaderEpoch)
    expect(leaderEpochs.endOffsetFor(leaderEpoch)).andReturn(replicaLEO)
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stubWithFetchMessages(replica, replica, futureReplica, partition, replicaManager, responseCallback)

    replay(leaderEpochs, futureReplicaLeaderEpochs, replicaManager, logManager, quotaManager,
           replica, futureReplica, partition)

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      sourceBroker = endPoint,
      brokerConfig = config,
      replicaMgr = replicaManager,
      quota = quotaManager,
      brokerTopicStats = null)
    thread.addPartitions(Map(t1p0 -> 0))

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
    val quotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager = createMock(classOf[LogManager])
    val replica = createNiceMock(classOf[Replica])
    val futureReplica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createMock(classOf[ReplicaManager])

    //Stubs
    expect(futureReplica.logStartOffset).andReturn(123).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stub(replica, replica, futureReplica, partition, replicaManager)

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
    thread.addPartitions(Map(t1p0 -> 0, t1p1 -> 0))

    val ResultWithPartitions(fetchRequest, partitionsWithError) =
      thread.buildFetchRequest(Seq((t1p0, new PartitionFetchState(150)), (t1p1, new PartitionFetchState(160))))

    assertFalse(fetchRequest.isEmpty)
    assertFalse(partitionsWithError.nonEmpty)
    val request = fetchRequest.underlying.build()
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
    val quotaManager = createNiceMock(classOf[ReplicationQuotaManager])
    val logManager = createMock(classOf[LogManager])
    val replica = createNiceMock(classOf[Replica])
    val futureReplica = createNiceMock(classOf[Replica])
    val partition = createMock(classOf[Partition])
    val replicaManager = createMock(classOf[ReplicaManager])

    //Stubs
    expect(futureReplica.logStartOffset).andReturn(123).anyTimes()
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    stub(replica, replica, futureReplica, partition, replicaManager)

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
    thread.addPartitions(Map(t1p0 -> 0, t1p1 -> 0))

    // one partition is ready and one is truncating
    val ResultWithPartitions(fetchRequest, partitionsWithError) =
      thread.buildFetchRequest(Seq(
        (t1p0, new PartitionFetchState(150)),
        (t1p1, new PartitionFetchState(160, truncatingLog=true))))

    assertFalse(fetchRequest.isEmpty)
    assertFalse(partitionsWithError.nonEmpty)
    val fetchInfos = fetchRequest.underlying.build().fetchData.asScala.toSeq
    assertEquals(1, fetchInfos.length)
    assertEquals("Expected fetch request for non-truncating partition", t1p0, fetchInfos.head._1)
    assertEquals(150, fetchInfos.head._2.fetchOffset)

    // one partition is ready and one is delayed
    val ResultWithPartitions(fetchRequest2, partitionsWithError2) =
      thread.buildFetchRequest(Seq(
        (t1p0, new PartitionFetchState(140)),
        (t1p1, new PartitionFetchState(160, delay=new DelayedItem(5000)))))

    assertFalse(fetchRequest2.isEmpty)
    assertFalse(partitionsWithError2.nonEmpty)
    val fetchInfos2 = fetchRequest2.underlying.build().fetchData.asScala.toSeq
    assertEquals(1, fetchInfos2.length)
    assertEquals("Expected fetch request for non-delayed partition", t1p0, fetchInfos2.head._1)
    assertEquals(140, fetchInfos2.head._2.fetchOffset)

    // both partitions are delayed
    val ResultWithPartitions(fetchRequest3, partitionsWithError3) =
      thread.buildFetchRequest(Seq(
        (t1p0, new PartitionFetchState(140, delay=new DelayedItem(5000))),
        (t1p1, new PartitionFetchState(160, delay=new DelayedItem(5000)))))
    assertTrue("Expected no fetch requests since all partitions are delayed", fetchRequest3.isEmpty)
    assertFalse(partitionsWithError3.nonEmpty)
  }

  def stub(replicaT1p0: Replica, replicaT1p1: Replica, futureReplica: Replica, partition: Partition, replicaManager: ReplicaManager) = {
    expect(replicaManager.getReplica(t1p0)).andReturn(Some(replicaT1p0)).anyTimes()
    expect(replicaManager.getReplica(t1p0, Request.FutureLocalReplicaId)).andReturn(Some(futureReplica)).anyTimes()
    expect(replicaManager.getReplicaOrException(t1p0)).andReturn(replicaT1p0).anyTimes()
    expect(replicaManager.getReplicaOrException(t1p0, Request.FutureLocalReplicaId)).andReturn(futureReplica).anyTimes()
    expect(replicaManager.getPartition(t1p0)).andReturn(Some(partition)).anyTimes()
    expect(replicaManager.getReplica(t1p1)).andReturn(Some(replicaT1p1)).anyTimes()
    expect(replicaManager.getReplica(t1p1, Request.FutureLocalReplicaId)).andReturn(Some(futureReplica)).anyTimes()
    expect(replicaManager.getReplicaOrException(t1p1)).andReturn(replicaT1p1).anyTimes()
    expect(replicaManager.getReplicaOrException(t1p1, Request.FutureLocalReplicaId)).andReturn(futureReplica).anyTimes()
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
