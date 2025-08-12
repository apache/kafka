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

import com.yammer.metrics.core.Meter
import kafka.cluster.Partition
import org.apache.kafka.common.errors.NotLeaderOrFollowerException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.server.LogReadResult
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}
import org.apache.kafka.storage.internals.log._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyBoolean
import org.mockito.Mockito.{mock, never, verify, when}

import java.util.{Collections, Optional, OptionalLong}
import java.util.concurrent.{CompletableFuture, Future}
import scala.collection._
import scala.jdk.CollectionConverters._

class DelayedRemoteFetchTest {
  private val maxBytes = 1024
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic")
  private val topicIdPartition2 = new TopicIdPartition(Uuid.randomUuid(), 0, "topic2")
  private val fetchOffset = 500L
  private val logStartOffset = 0L
  private val currentLeaderEpoch = Optional.of[Integer](10)
  private val remoteFetchMaxWaitMs = 500

  private val fetchStatus = FetchPartitionStatus(
    startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
    fetchInfo = new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
  private val fetchParams = buildFetchParams(replicaId = -1, maxWaitMs = 500)

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
    future.complete(buildRemoteReadResult(Errors.NONE))
    val fetchInfo: RemoteStorageFetchInfo = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null)
    val highWatermark = 100
    val leaderLogStartOffset = 10
    val logReadInfo = buildReadResult(Errors.NONE, highWatermark, leaderLogStartOffset)

    val delayedRemoteFetch = new DelayedRemoteFetch(
      java.util.Collections.emptyMap[TopicIdPartition, Future[Void]](),
      java.util.Collections.singletonMap(topicIdPartition, future),
      java.util.Collections.singletonMap(topicIdPartition, fetchInfo),
      remoteFetchMaxWaitMs,
      Seq(topicIdPartition -> fetchStatus),
      fetchParams,
      Seq(topicIdPartition -> logReadInfo),
      replicaManager,
      callback)

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
  def testFollowerFetch(): Unit = {
    var actualTopicPartition: Option[TopicIdPartition] = None
    var fetchResultOpt: Option[FetchPartitionData] = None

    def callback(responses: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      assertEquals(1, responses.size)
      actualTopicPartition = Some(responses.head._1)
      fetchResultOpt = Some(responses.head._2)
    }

    val future: CompletableFuture[RemoteLogReadResult] = new CompletableFuture[RemoteLogReadResult]()
    future.complete(buildRemoteReadResult(Errors.NONE))
    val fetchInfo: RemoteStorageFetchInfo = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null)
    val highWatermark = 100
    val leaderLogStartOffset = 10
    val logReadInfo = buildReadResult(Errors.NONE, highWatermark, leaderLogStartOffset)
    val fetchParams = buildFetchParams(replicaId = 1, maxWaitMs = 500)

    assertThrows(classOf[IllegalStateException], () => new DelayedRemoteFetch(
      java.util.Collections.emptyMap[TopicIdPartition, Future[Void]](),
      java.util.Collections.singletonMap(topicIdPartition, future),
      java.util.Collections.singletonMap(topicIdPartition, fetchInfo),
      remoteFetchMaxWaitMs,
      Seq(topicIdPartition -> fetchStatus),
      fetchParams,
      Seq(topicIdPartition -> logReadInfo),
      replicaManager,
      callback))
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
    val fetchInfo: RemoteStorageFetchInfo = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null)

    val logReadInfo = buildReadResult(Errors.NONE)

    val delayedRemoteFetch =  new DelayedRemoteFetch(
      java.util.Collections.emptyMap[TopicIdPartition, Future[Void]](),
      java.util.Collections.singletonMap(topicIdPartition, future),
      java.util.Collections.singletonMap(topicIdPartition, fetchInfo),
      remoteFetchMaxWaitMs,
      Seq(topicIdPartition -> fetchStatus),
      fetchParams,
      Seq(topicIdPartition -> logReadInfo),
      replicaManager,
      callback)

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
    future.complete(buildRemoteReadResult(Errors.NONE))
    val fetchInfo: RemoteStorageFetchInfo = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null)

    // build a read result with error
    val logReadInfo = buildReadResult(Errors.FENCED_LEADER_EPOCH)

    val delayedRemoteFetch = new DelayedRemoteFetch(
      java.util.Collections.emptyMap[TopicIdPartition, Future[Void]](),
      java.util.Collections.singletonMap(topicIdPartition, future),
      java.util.Collections.singletonMap(topicIdPartition, fetchInfo),
      remoteFetchMaxWaitMs,
      Seq(topicIdPartition -> fetchStatus),
      fetchParams,
      Seq(topicIdPartition -> logReadInfo),
      replicaManager,
      callback)

    assertTrue(delayedRemoteFetch.tryComplete())
    assertTrue(delayedRemoteFetch.isCompleted)
    assertEquals(topicIdPartition, actualTopicPartition.get)
    assertTrue(fetchResultOpt.isDefined)
    assertEquals(Errors.FENCED_LEADER_EPOCH, fetchResultOpt.get.error)
  }

  @Test
  def testRequestExpiry(): Unit = {
    val responses = mutable.Map[TopicIdPartition, FetchPartitionData]()

    def callback(responseSeq: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      responseSeq.foreach { case (tp, data) =>
        responses.put(tp, data)
      }
    }

    def expiresPerSecValue(): Double = {
      val allMetrics = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      val metric = allMetrics.find { case (n, _) => n.getMBeanName.endsWith("kafka.server:type=DelayedRemoteFetchMetrics,name=ExpiresPerSec") }

      if (metric.isEmpty)
        0
      else
        metric.get._2.asInstanceOf[Meter].count
    }

    val remoteFetchTaskExpired = mock(classOf[Future[Void]])
    val remoteFetchTask2 = mock(classOf[Future[Void]])
    // complete the 2nd task, and keep the 1st one expired
    when(remoteFetchTask2.isDone).thenReturn(true)

    // Create futures - one completed, one not
    val future1: CompletableFuture[RemoteLogReadResult] = new CompletableFuture[RemoteLogReadResult]()
    val future2: CompletableFuture[RemoteLogReadResult] = new CompletableFuture[RemoteLogReadResult]()
    // Only complete one remote fetch
    future2.complete(buildRemoteReadResult(Errors.NONE))

    val fetchInfo1 = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null)
    val fetchInfo2 = new RemoteStorageFetchInfo(0, false, topicIdPartition2, null, null)

    val highWatermark = 100
    val leaderLogStartOffset = 10

    val logReadInfo1 = buildReadResult(Errors.NONE, highWatermark, leaderLogStartOffset)
    val logReadInfo2 = buildReadResult(Errors.NONE)

    val fetchStatus1 = FetchPartitionStatus(
      startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
    val fetchStatus2 = FetchPartitionStatus(
      startOffsetMetadata = new LogOffsetMetadata(fetchOffset + 100),
      fetchInfo = new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset + 100, logStartOffset, maxBytes, currentLeaderEpoch))

    // Set up maps for multiple partitions
    val remoteFetchTasks = new java.util.HashMap[TopicIdPartition, Future[Void]]()
    val remoteFetchResults = new java.util.HashMap[TopicIdPartition, CompletableFuture[RemoteLogReadResult]]()
    val remoteFetchInfos = new java.util.HashMap[TopicIdPartition, RemoteStorageFetchInfo]()

    remoteFetchTasks.put(topicIdPartition, remoteFetchTaskExpired)
    remoteFetchTasks.put(topicIdPartition2, remoteFetchTask2)
    remoteFetchResults.put(topicIdPartition, future1)
    remoteFetchResults.put(topicIdPartition2, future2)
    remoteFetchInfos.put(topicIdPartition, fetchInfo1)
    remoteFetchInfos.put(topicIdPartition2, fetchInfo2)

    val delayedRemoteFetch = new DelayedRemoteFetch(
      remoteFetchTasks,
      remoteFetchResults,
      remoteFetchInfos,
      remoteFetchMaxWaitMs,
      Seq(topicIdPartition -> fetchStatus1, topicIdPartition2 -> fetchStatus2),
      fetchParams,
      Seq(topicIdPartition -> logReadInfo1, topicIdPartition2 -> logReadInfo2),
      replicaManager,
      callback)

    when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition))
      .thenReturn(mock(classOf[Partition]))
    when(replicaManager.getPartitionOrException(topicIdPartition2.topicPartition))
      .thenReturn(mock(classOf[Partition]))

    // Verify that the ExpiresPerSec metric is zero before fetching
    val existingMetricVal = expiresPerSecValue()
    // Verify the delayedRemoteFetch is not completed yet
    assertFalse(delayedRemoteFetch.isCompleted)

    // Force the delayed remote fetch to expire
    delayedRemoteFetch.run()

    // Check that the expired task was cancelled and force-completed
    verify(remoteFetchTaskExpired).cancel(anyBoolean())
    verify(remoteFetchTask2, never()).cancel(anyBoolean())
    assertTrue(delayedRemoteFetch.isCompleted)

    // Check that the ExpiresPerSec metric was incremented
    assertTrue(expiresPerSecValue() > existingMetricVal)

    // Fetch results should include 2 results and the expired one should return local read results
    assertEquals(2, responses.size)
    assertTrue(responses.contains(topicIdPartition))
    assertTrue(responses.contains(topicIdPartition2))

    assertEquals(Errors.NONE, responses(topicIdPartition).error)
    assertEquals(highWatermark, responses(topicIdPartition).highWatermark)
    assertEquals(leaderLogStartOffset, responses(topicIdPartition).logStartOffset)

    assertEquals(Errors.NONE, responses(topicIdPartition2).error)
  }

  @Test
  def testMultiplePartitions(): Unit = {
    val responses = mutable.Map[TopicIdPartition, FetchPartitionData]()

    def callback(responseSeq: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      responseSeq.foreach { case (tp, data) =>
        responses.put(tp, data)
      }
    }

    // Create futures - one completed, one not
    val future1: CompletableFuture[RemoteLogReadResult] = new CompletableFuture[RemoteLogReadResult]()
    val future2: CompletableFuture[RemoteLogReadResult] = new CompletableFuture[RemoteLogReadResult]()
    // Only complete one remote fetch
    future1.complete(buildRemoteReadResult(Errors.NONE))

    val fetchInfo1 = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null)
    val fetchInfo2 = new RemoteStorageFetchInfo(0, false, topicIdPartition2, null, null)

    val highWatermark1 = 100
    val leaderLogStartOffset1 = 10
    val highWatermark2 = 200
    val leaderLogStartOffset2 = 20

    val logReadInfo1 = buildReadResult(Errors.NONE, 100, 10)
    val logReadInfo2 = buildReadResult(Errors.NONE, 200, 20)

    val fetchStatus1 = FetchPartitionStatus(
      startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
    val fetchStatus2 = FetchPartitionStatus(
      startOffsetMetadata = new LogOffsetMetadata(fetchOffset + 100),
      fetchInfo = new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset + 100, logStartOffset, maxBytes, currentLeaderEpoch))

    // Set up maps for multiple partitions
    val remoteFetchResults = new java.util.HashMap[TopicIdPartition, CompletableFuture[RemoteLogReadResult]]()
    val remoteFetchInfos = new java.util.HashMap[TopicIdPartition, RemoteStorageFetchInfo]()

    remoteFetchResults.put(topicIdPartition, future1)
    remoteFetchResults.put(topicIdPartition2, future2)
    remoteFetchInfos.put(topicIdPartition, fetchInfo1)
    remoteFetchInfos.put(topicIdPartition2, fetchInfo2)

    val delayedRemoteFetch = new DelayedRemoteFetch(
      Collections.emptyMap[TopicIdPartition, Future[Void]](),
      remoteFetchResults,
      remoteFetchInfos,
      remoteFetchMaxWaitMs,
      Seq(topicIdPartition -> fetchStatus1, topicIdPartition2 -> fetchStatus2),
      fetchParams,
      Seq(topicIdPartition -> logReadInfo1, topicIdPartition2 -> logReadInfo2),
      replicaManager,
      callback)

    when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition))
      .thenReturn(mock(classOf[Partition]))
    when(replicaManager.getPartitionOrException(topicIdPartition2.topicPartition))
      .thenReturn(mock(classOf[Partition]))

    // Should not complete since future2 is not done
    assertFalse(delayedRemoteFetch.tryComplete())
    assertFalse(delayedRemoteFetch.isCompleted)

    // Complete future2
    future2.complete(buildRemoteReadResult(Errors.NONE))

    // Now it should complete
    assertTrue(delayedRemoteFetch.tryComplete())
    assertTrue(delayedRemoteFetch.isCompleted)

    // Verify both partitions were processed without error
    assertEquals(2, responses.size)
    assertTrue(responses.contains(topicIdPartition))
    assertTrue(responses.contains(topicIdPartition2))

    assertEquals(Errors.NONE, responses(topicIdPartition).error)
    assertEquals(highWatermark1, responses(topicIdPartition).highWatermark)
    assertEquals(leaderLogStartOffset1, responses(topicIdPartition).logStartOffset)

    assertEquals(Errors.NONE, responses(topicIdPartition2).error)
    assertEquals(highWatermark2, responses(topicIdPartition2).highWatermark)
    assertEquals(leaderLogStartOffset2, responses(topicIdPartition2).logStartOffset)
  }

  @Test
  def testMultiplePartitionsWithFailedResults(): Unit = {
    val responses = mutable.Map[TopicIdPartition, FetchPartitionData]()

    def callback(responseSeq: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      responseSeq.foreach { case (tp, data) =>
        responses.put(tp, data)
      }
    }

    // Create futures - one successful, one with error
    val future1: CompletableFuture[RemoteLogReadResult] = new CompletableFuture[RemoteLogReadResult]()
    val future2: CompletableFuture[RemoteLogReadResult] = new CompletableFuture[RemoteLogReadResult]()

    // Created 1 successful result and 1 failed result
    future1.complete(buildRemoteReadResult(Errors.NONE))
    future2.complete(buildRemoteReadResult(Errors.UNKNOWN_SERVER_ERROR))

    val fetchInfo1 = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null)
    val fetchInfo2 = new RemoteStorageFetchInfo(0, false, topicIdPartition2, null, null)

    val logReadInfo1 = buildReadResult(Errors.NONE, 100, 10)
    val logReadInfo2 = buildReadResult(Errors.NONE, 200, 20)

    val fetchStatus1 = FetchPartitionStatus(
      startOffsetMetadata = new LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
    val fetchStatus2 = FetchPartitionStatus(
      startOffsetMetadata = new LogOffsetMetadata(fetchOffset + 100),
      fetchInfo = new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset + 100, logStartOffset, maxBytes, currentLeaderEpoch))

    // Set up maps for multiple partitions
    val remoteFetchResults = new java.util.HashMap[TopicIdPartition, CompletableFuture[RemoteLogReadResult]]()
    val remoteFetchInfos = new java.util.HashMap[TopicIdPartition, RemoteStorageFetchInfo]()

    remoteFetchResults.put(topicIdPartition, future1)
    remoteFetchResults.put(topicIdPartition2, future2)
    remoteFetchInfos.put(topicIdPartition, fetchInfo1)
    remoteFetchInfos.put(topicIdPartition2, fetchInfo2)

    val delayedRemoteFetch = new DelayedRemoteFetch(
      Collections.emptyMap[TopicIdPartition, Future[Void]](),
      remoteFetchResults,
      remoteFetchInfos,
      remoteFetchMaxWaitMs,
      Seq(topicIdPartition -> fetchStatus1, topicIdPartition2 -> fetchStatus2),
      fetchParams,
      Seq(topicIdPartition -> logReadInfo1, topicIdPartition2 -> logReadInfo2),
      replicaManager,
      callback)

    when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition))
      .thenReturn(mock(classOf[Partition]))
    when(replicaManager.getPartitionOrException(topicIdPartition2.topicPartition))
      .thenReturn(mock(classOf[Partition]))

    assertTrue(delayedRemoteFetch.tryComplete())
    assertTrue(delayedRemoteFetch.isCompleted)

    // Verify both partitions were processed
    assertEquals(2, responses.size)
    assertTrue(responses.contains(topicIdPartition))
    assertTrue(responses.contains(topicIdPartition2))

    // First partition should be successful
    val fetchResult1 = responses(topicIdPartition)
    assertEquals(Errors.NONE, fetchResult1.error)

    // Second partition should have an error due to remote fetch failure
    val fetchResult2 = responses(topicIdPartition2)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, fetchResult2.error)
  }

  private def buildFetchParams(replicaId: Int,
                               maxWaitMs: Int): FetchParams = {
    new FetchParams(
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
    new LogReadResult(
      new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY, false, Optional.empty(),
        Optional.of(mock(classOf[RemoteStorageFetchInfo]))),
      Optional.empty(),
      highWatermark,
      leaderLogStartOffset,
      -1L,
      -1L,
      -1L,
      OptionalLong.empty(),
      if (error != Errors.NONE) Optional.of[Throwable](error.exception) else Optional.empty[Throwable]())
  }

  private def buildRemoteReadResult(error: Errors): RemoteLogReadResult = {
    new RemoteLogReadResult(
      Optional.of(new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY)),
      if (error != Errors.NONE) Optional.of[Throwable](error.exception) else Optional.empty[Throwable]())
  }
}
