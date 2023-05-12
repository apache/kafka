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

import kafka.cluster.Partition
import org.apache.kafka.common.errors.NotLeaderOrFollowerException
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.storage.internals.log._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

import java.util.Optional
import java.util.concurrent.CompletableFuture

import scala.collection._

class DelayedRemoteFetchTest {
  private val maxBytes = 1024
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
  private val fetchOffset = 500L
  private val logStartOffset = 0L
  private val currentLeaderEpoch = Optional.of[Integer](10)
  private val replicaId = 1

  private val fetchStatus = FetchPartitionStatus(
    startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
    fetchInfo = new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
  private val fetchParams = buildFollowerFetchParams(replicaId, maxWaitMs = 500)

  @Test
  def testFetch(): Unit = {
    var actualTopicPartition: Option[TopicIdPartition] = None
    var fetchResultOpt: Option[FetchPartitionData] = None

    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      assertEquals(1, responses.size)
      actualTopicPartition = Some(responses.head._1)
      fetchResultOpt = Some(responses.head._2)
    }

    val future: CompletableFuture[RemoteLogReadResult] = new CompletableFuture[RemoteLogReadResult]()
    future.complete(null)
    val fetchInfo: RemoteStorageFetchInfo = new RemoteStorageFetchInfo(0, false, topicIdPartition.topicPartition(), null, null, false)
    val highWatermark = 100
    val leaderLogStartOffset = 10
    val logReadInfo = buildReadResult(Errors.NONE, highWatermark, leaderLogStartOffset)

    val delayedRemoteFetch = new DelayedRemoteFetch(null, future, fetchInfo, Seq(topicIdPartition -> fetchStatus), fetchParams,
      Seq(topicIdPartition -> logReadInfo), replicaManager, callback)

    when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition))
      .thenReturn(mock(classOf[Partition]))

    assertTrue(delayedRemoteFetch.tryComplete())
    assertTrue(delayedRemoteFetch.isCompleted)
    assertTrue(actualTopicPartition.isDefined)
    assertEquals(topicIdPartition, actualTopicPartition.get)
    assertTrue(fetchResultOpt.isDefined)

    val fetchResult = fetchResultOpt.get
    assertEquals(Errors.NONE, fetchResult.error)
    assertEquals(highWatermark, fetchResult.highWatermark)
    assertEquals(leaderLogStartOffset, fetchResult.logStartOffset)
  }

  @Test
  def testNotLeaderOrFollower(): Unit = {
    var actualTopicPartition: Option[TopicIdPartition] = None
    var fetchResultOpt: Option[FetchPartitionData] = None

    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      assertEquals(1, responses.size)
      actualTopicPartition = Some(responses.head._1)
      fetchResultOpt = Some(responses.head._2)
    }

    // throw exception while getPartition
    when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition))
      .thenThrow(new NotLeaderOrFollowerException(s"Replica for $topicIdPartition not available"))

    val future: CompletableFuture[RemoteLogReadResult] = new CompletableFuture[RemoteLogReadResult]()
    val fetchInfo: RemoteStorageFetchInfo = new RemoteStorageFetchInfo(0, false, topicIdPartition.topicPartition(), null, null, false)

    val logReadInfo = buildReadResult(Errors.NONE)

    val delayedRemoteFetch = new DelayedRemoteFetch(null, future, fetchInfo, Seq(topicIdPartition -> fetchStatus), fetchParams,
      Seq(topicIdPartition -> logReadInfo), replicaManager, callback)

    // delayed remote fetch should still be able to complete
    assertTrue(delayedRemoteFetch.tryComplete())
    assertTrue(delayedRemoteFetch.isCompleted)
    assertEquals(topicIdPartition, actualTopicPartition.get)
    assertTrue(fetchResultOpt.isDefined)
  }

  @Test
  def testErrorLogReadInfo(): Unit = {
    var actualTopicPartition: Option[TopicIdPartition] = None
    var fetchResultOpt: Option[FetchPartitionData] = None

    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      assertEquals(1, responses.size)
      actualTopicPartition = Some(responses.head._1)
      fetchResultOpt = Some(responses.head._2)
    }

    when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition))
      .thenReturn(mock(classOf[Partition]))

    val future: CompletableFuture[RemoteLogReadResult] = new CompletableFuture[RemoteLogReadResult]()
    future.complete(null)
    val fetchInfo: RemoteStorageFetchInfo = new RemoteStorageFetchInfo(0, false, topicIdPartition.topicPartition(), null, null, false)

    // build a read result with error
    val logReadInfo = buildReadResult(Errors.FENCED_LEADER_EPOCH)

    val delayedRemoteFetch = new DelayedRemoteFetch(null, future, fetchInfo, Seq(topicIdPartition -> fetchStatus), fetchParams,
      Seq(topicIdPartition -> logReadInfo), replicaManager, callback)

    assertTrue(delayedRemoteFetch.tryComplete())
    assertTrue(delayedRemoteFetch.isCompleted)
    assertEquals(topicIdPartition, actualTopicPartition.get)
    assertTrue(fetchResultOpt.isDefined)
    assertEquals(Errors.FENCED_LEADER_EPOCH, fetchResultOpt.get.error)
  }

  private def buildFollowerFetchParams(replicaId: Int,
                                       maxWaitMs: Int): FetchParams = {
    new FetchParams(
      ApiKeys.FETCH.latestVersion,
      replicaId,
      1,
      maxWaitMs,
      1,
      maxBytes,
      FetchIsolation.LOG_END,
      Optional.empty()
    )
  }

  private def buildReadResult(error: Errors,
                              highWatermark: Int = 0,
                              leaderLogStartOffset: Int = 0): LogReadResult = {
    LogReadResult(
      exception = if (error != Errors.NONE) Some(error.exception) else None,
      info = new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
      divergingEpoch = None,
      highWatermark = highWatermark,
      leaderLogStartOffset = leaderLogStartOffset,
      leaderLogEndOffset = -1L,
      followerLogStartOffset = -1L,
      fetchTimeMs = -1L,
      lastStableOffset = None)
  }

}
