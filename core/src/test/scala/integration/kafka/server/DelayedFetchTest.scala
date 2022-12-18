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
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.common.errors.{FencedLeaderEpochException, NotLeaderOrFollowerException}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.server.log.internals.LogOffsetMetadata
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.{mock, when}

class DelayedFetchTest {
  private val maxBytes = 1024
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val replicaQuota: ReplicaQuota = mock(classOf[ReplicaQuota])

  @Test
  def testFetchWithFencedEpoch(): Unit = {
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val fetchOffset = 500L
    val logStartOffset = 0L
    val currentLeaderEpoch = Optional.of[Integer](10)
    val replicaId = 1

    val fetchStatus = FetchPartitionStatus(
      startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
    val fetchParams = buildFollowerFetchParams(replicaId, maxWaitMs = 500)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      params = fetchParams,
      fetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
      replicaManager = replicaManager,
      quota = replicaQuota,
      responseCallback = callback
    )

    val partition: Partition = mock(classOf[Partition])

    when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition))
        .thenReturn(partition)
    when(partition.fetchOffsetSnapshot(
        currentLeaderEpoch,
        fetchOnlyFromLeader = true))
        .thenThrow(new FencedLeaderEpochException("Requested epoch has been fenced"))
    when(replicaManager.isAddingReplica(any(), anyInt())).thenReturn(false)

    expectReadFromReplica(fetchParams, topicIdPartition, fetchStatus.fetchInfo, Errors.FENCED_LEADER_EPOCH)

    assertTrue(delayedFetch.tryComplete())
    assertTrue(delayedFetch.isCompleted)
    assertTrue(fetchResultOpt.isDefined)

    val fetchResult = fetchResultOpt.get
    assertEquals(Errors.FENCED_LEADER_EPOCH, fetchResult.error)
  }

  @Test
  def testNotLeaderOrFollower(): Unit = {
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val fetchOffset = 500L
    val logStartOffset = 0L
    val currentLeaderEpoch = Optional.of[Integer](10)
    val replicaId = 1

    val fetchStatus = FetchPartitionStatus(
      startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
    val fetchParams = buildFollowerFetchParams(replicaId, maxWaitMs = 500)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      params = fetchParams,
      fetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
      replicaManager = replicaManager,
      quota = replicaQuota,
      responseCallback = callback
    )

    when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition))
      .thenThrow(new NotLeaderOrFollowerException(s"Replica for $topicIdPartition not available"))
    expectReadFromReplica(fetchParams, topicIdPartition, fetchStatus.fetchInfo, Errors.NOT_LEADER_OR_FOLLOWER)
    when(replicaManager.isAddingReplica(any(), anyInt())).thenReturn(false)

    assertTrue(delayedFetch.tryComplete())
    assertTrue(delayedFetch.isCompleted)
    assertTrue(fetchResultOpt.isDefined)
  }

  @Test
  def testDivergingEpoch(): Unit = {
    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
    val fetchOffset = 500L
    val logStartOffset = 0L
    val currentLeaderEpoch = Optional.of[Integer](10)
    val lastFetchedEpoch = Optional.of[Integer](9)
    val replicaId = 1

    val fetchStatus = FetchPartitionStatus(
      startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(topicIdPartition.topicId, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch, lastFetchedEpoch))
    val fetchParams = buildFollowerFetchParams(replicaId, maxWaitMs = 500)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      params = fetchParams,
      fetchPartitionStatus = Seq(topicIdPartition -> fetchStatus),
      replicaManager = replicaManager,
      quota = replicaQuota,
      responseCallback = callback
    )

    val partition: Partition = mock(classOf[Partition])
    when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition)).thenReturn(partition)
    val endOffsetMetadata = new LogOffsetMetadata(500L, 0L, 500)
    when(partition.fetchOffsetSnapshot(
      currentLeaderEpoch,
      fetchOnlyFromLeader = true))
      .thenReturn(LogOffsetSnapshot(0L, endOffsetMetadata, endOffsetMetadata, endOffsetMetadata))
    when(partition.lastOffsetForLeaderEpoch(currentLeaderEpoch, lastFetchedEpoch.get, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(topicIdPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(lastFetchedEpoch.get)
        .setEndOffset(fetchOffset - 1))
    when(replicaManager.isAddingReplica(any(), anyInt())).thenReturn(false)
    expectReadFromReplica(fetchParams, topicIdPartition, fetchStatus.fetchInfo, Errors.NONE)

    assertTrue(delayedFetch.tryComplete())
    assertTrue(delayedFetch.isCompleted)
    assertTrue(fetchResultOpt.isDefined)
  }

  private def buildFollowerFetchParams(
    replicaId: Int,
    maxWaitMs: Int
  ): FetchParams = {
    FetchParams(
      requestVersion = ApiKeys.FETCH.latestVersion,
      replicaId = replicaId,
      maxWaitMs = maxWaitMs,
      minBytes = 1,
      maxBytes = maxBytes,
      isolation = FetchLogEnd,
      clientMetadata = None
    )
  }

  private def expectReadFromReplica(
    fetchParams: FetchParams,
    topicIdPartition: TopicIdPartition,
    fetchPartitionData: FetchRequest.PartitionData,
    error: Errors
  ): Unit = {
    when(replicaManager.readFromLocalLog(
      fetchParams,
      readPartitionInfo = Seq((topicIdPartition, fetchPartitionData)),
      quota = replicaQuota,
      readFromPurgatory = true
    )).thenReturn(Seq((topicIdPartition, buildReadResult(error))))
  }

  private def buildReadResult(error: Errors): LogReadResult = {
    LogReadResult(
      exception = if (error != Errors.NONE) Some(error.exception) else None,
      info = FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
      divergingEpoch = None,
      highWatermark = -1L,
      leaderLogStartOffset = -1L,
      leaderLogEndOffset = -1L,
      followerLogStartOffset = -1L,
      fetchTimeMs = -1L,
      lastStableOffset = None)
  }

}
