/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.util.Optional

import scala.collection.Seq
import kafka.cluster.Partition
import kafka.log.LogOffsetSnapshot
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{FencedLeaderEpochException, NotLeaderOrFollowerException}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest
import org.easymock.{EasyMock, EasyMockSupport}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._

class DelayedFetchTest extends EasyMockSupport {
  private val maxBytes = 1024
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val replicaQuota: ReplicaQuota = mock(classOf[ReplicaQuota])

  @Test
  def testFetchWithFencedEpoch(): Unit = {
    val topicPartition = new TopicPartition("topic", 0)
    val fetchOffset = 500L
    val logStartOffset = 0L
    val currentLeaderEpoch = Optional.of[Integer](10)
    val replicaId = 1

    val fetchStatus = FetchPartitionStatus(
      startOffsetMetadata = LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
    val fetchMetadata = buildFetchMetadata(replicaId, topicPartition, fetchStatus)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      delayMs = 500,
      fetchMetadata = fetchMetadata,
      replicaManager = replicaManager,
      quota = replicaQuota,
      clientMetadata = None,
      responseCallback = callback)

    val partition: Partition = mock(classOf[Partition])

    EasyMock.expect(replicaManager.getPartitionOrException(topicPartition))
        .andReturn(partition)
    EasyMock.expect(partition.fetchOffsetSnapshot(
        currentLeaderEpoch,
        fetchOnlyFromLeader = true))
        .andThrow(new FencedLeaderEpochException("Requested epoch has been fenced"))
    EasyMock.expect(replicaManager.isAddingReplica(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(false)

    expectReadFromReplica(replicaId, topicPartition, fetchStatus.fetchInfo, Errors.FENCED_LEADER_EPOCH)

    replayAll()

    assertTrue(delayedFetch.tryComplete())
    assertTrue(delayedFetch.isCompleted)
    assertTrue(fetchResultOpt.isDefined)

    val fetchResult = fetchResultOpt.get
    assertEquals(Errors.FENCED_LEADER_EPOCH, fetchResult.error)
  }

  @Test
  def testNotLeaderOrFollower(): Unit = {
    val topicPartition = new TopicPartition("topic", 0)
    val fetchOffset = 500L
    val logStartOffset = 0L
    val currentLeaderEpoch = Optional.of[Integer](10)
    val replicaId = 1

    val fetchStatus = FetchPartitionStatus(
      startOffsetMetadata = LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
    val fetchMetadata = buildFetchMetadata(replicaId, topicPartition, fetchStatus)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      delayMs = 500,
      fetchMetadata = fetchMetadata,
      replicaManager = replicaManager,
      quota = replicaQuota,
      clientMetadata = None,
      responseCallback = callback)

    EasyMock.expect(replicaManager.getPartitionOrException(topicPartition))
      .andThrow(new NotLeaderOrFollowerException(s"Replica for $topicPartition not available"))
    expectReadFromReplica(replicaId, topicPartition, fetchStatus.fetchInfo, Errors.NOT_LEADER_OR_FOLLOWER)
    EasyMock.expect(replicaManager.isAddingReplica(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(false)

    replayAll()

    assertTrue(delayedFetch.tryComplete())
    assertTrue(delayedFetch.isCompleted)
    assertTrue(fetchResultOpt.isDefined)
  }

  @Test
  def testDivergingEpoch(): Unit = {
    val topicPartition = new TopicPartition("topic", 0)
    val fetchOffset = 500L
    val logStartOffset = 0L
    val currentLeaderEpoch = Optional.of[Integer](10)
    val lastFetchedEpoch = Optional.of[Integer](9)
    val replicaId = 1

    val fetchStatus = FetchPartitionStatus(
      startOffsetMetadata = LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch, lastFetchedEpoch))
    val fetchMetadata = buildFetchMetadata(replicaId, topicPartition, fetchStatus)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      delayMs = 500,
      fetchMetadata = fetchMetadata,
      replicaManager = replicaManager,
      quota = replicaQuota,
      clientMetadata = None,
      responseCallback = callback)

    val partition: Partition = mock(classOf[Partition])
    EasyMock.expect(replicaManager.getPartitionOrException(topicPartition)).andReturn(partition)
    val endOffsetMetadata = LogOffsetMetadata(messageOffset = 500L, segmentBaseOffset = 0L, relativePositionInSegment = 500)
    EasyMock.expect(partition.fetchOffsetSnapshot(
      currentLeaderEpoch,
      fetchOnlyFromLeader = true))
      .andReturn(LogOffsetSnapshot(0L, endOffsetMetadata, endOffsetMetadata, endOffsetMetadata))
    EasyMock.expect(partition.lastOffsetForLeaderEpoch(currentLeaderEpoch, lastFetchedEpoch.get, fetchOnlyFromLeader = false))
      .andReturn(new EpochEndOffset()
        .setPartition(topicPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(lastFetchedEpoch.get)
        .setEndOffset(fetchOffset - 1))
    EasyMock.expect(replicaManager.isAddingReplica(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(false)
    expectReadFromReplica(replicaId, topicPartition, fetchStatus.fetchInfo, Errors.NONE)
    replayAll()

    assertTrue(delayedFetch.tryComplete())
    assertTrue(delayedFetch.isCompleted)
    assertTrue(fetchResultOpt.isDefined)
  }

  private def buildFetchMetadata(replicaId: Int,
                                 topicPartition: TopicPartition,
                                 fetchStatus: FetchPartitionStatus): FetchMetadata = {
    FetchMetadata(fetchMinBytes = 1,
      fetchMaxBytes = maxBytes,
      hardMaxBytesLimit = false,
      fetchOnlyLeader = true,
      fetchIsolation = FetchLogEnd,
      isFromFollower = true,
      replicaId = replicaId,
      fetchPartitionStatus = Seq((topicPartition, fetchStatus)))
  }

  private def expectReadFromReplica(replicaId: Int,
                                    topicPartition: TopicPartition,
                                    fetchPartitionData: FetchRequest.PartitionData,
                                    error: Errors): Unit = {
    EasyMock.expect(replicaManager.readFromLocalLog(
      replicaId = replicaId,
      fetchOnlyFromLeader = true,
      fetchIsolation = FetchLogEnd,
      fetchMaxBytes = maxBytes,
      hardMaxBytesLimit = false,
      readPartitionInfo = Seq((topicPartition, fetchPartitionData)),
      clientMetadata = None,
      quota = replicaQuota))
      .andReturn(Seq((topicPartition, buildReadResult(error))))
  }

  private def buildReadResult(error: Errors): LogReadResult = {
    LogReadResult(
      exception = if (error != Errors.NONE) Some(error.exception) else None,
      info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
      divergingEpoch = None,
      highWatermark = -1L,
      leaderLogStartOffset = -1L,
      leaderLogEndOffset = -1L,
      followerLogStartOffset = -1L,
      fetchTimeMs = -1L,
      lastStableOffset = None)
  }

}
