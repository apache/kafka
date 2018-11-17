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

import kafka.cluster.Partition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.FencedLeaderEpochException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest
import org.easymock.{EasyMock, EasyMockSupport}
import org.junit.Test
import org.junit.Assert._

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
      responseCallback = callback)

    val partition: Partition = mock(classOf[Partition])

    EasyMock.expect(replicaManager.getPartitionOrException(topicPartition, expectLeader = true))
        .andReturn(partition)
    EasyMock.expect(partition.fetchOffsetSnapshot(currentLeaderEpoch, fetchOnlyFromLeader = true))
        .andThrow(new FencedLeaderEpochException("Requested epoch has been fenced"))

    expectReadFromReplicaWithError(replicaId, topicPartition, fetchStatus.fetchInfo, Errors.FENCED_LEADER_EPOCH)

    replayAll()

    assertTrue(delayedFetch.tryComplete())
    assertTrue(delayedFetch.isCompleted)
    assertTrue(fetchResultOpt.isDefined)

    val fetchResult = fetchResultOpt.get
    assertEquals(Errors.FENCED_LEADER_EPOCH, fetchResult.error)
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

  private def expectReadFromReplicaWithError(replicaId: Int,
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
      quota = replicaQuota))
      .andReturn(Seq((topicPartition, buildReadResultWithError(error))))
  }

  private def buildReadResultWithError(error: Errors): LogReadResult = {
    LogReadResult(
      exception = Some(error.exception),
      info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
      highWatermark = -1L,
      leaderLogStartOffset = -1L,
      leaderLogEndOffset = -1L,
      followerLogStartOffset = -1L,
      fetchTimeMs = -1L,
      readSize = -1,
      lastStableOffset = None)
  }

}
