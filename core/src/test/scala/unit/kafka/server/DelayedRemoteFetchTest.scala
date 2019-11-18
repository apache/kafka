/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.concurrent.CompletableFuture

import kafka.cluster.Partition
import kafka.log.remote.{RemoteLogManager, RemoteLogReadResult}
import kafka.server.QuotaFactory.UnboundedQuota
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.easymock.{EasyMock, EasyMockSupport}
import org.junit.Assert.assertEquals
import org.junit.Test
import kafka.log.remote.MockRemoteLogManager

import scala.collection.JavaConverters._
import scala.collection.Seq

class DelayedRemoteFetchTest extends EasyMockSupport {
  val tp = new TopicPartition("test", 0)
  val tp1 = new TopicPartition("t1", 0)
  var isRemoteFetchExecuted = false

  @Test
  def testRemoteFetch: Unit = {
    var replied = false

    def responseCallback(r: Seq[(TopicPartition, FetchPartitionData)]): Unit = {
      replied = true
      assert(r(0)._1.equals(tp1))

      assert(r(1)._1.equals(tp))
      assertEquals(None, r(1)._2.error)
      assertEquals(2, r(1)._2.records.records.asScala.size)
      assertEquals(102, r(1)._2.records.records.iterator.next.offset)
      assertEquals(2000, r(1)._2.highWatermark)
    }

    RemoteFetch(false, responseCallback)
    assert(replied)
    assert(isRemoteFetchExecuted)
  }

  @Test
  def testRemoteFetchTimeout: Unit = {
    var replied = false

    def responseCallback(r: Seq[(TopicPartition, FetchPartitionData)]): Unit = {
      replied = true
      assert(r(0)._1.equals(tp1))

      assert(r(1)._1.equals(tp))
      assertEquals(None, r(1)._2.error)
      assertEquals(0, r(1)._2.records.records.asScala.size)
      assertEquals(2000, r(1)._2.highWatermark)
    }

    RemoteFetch(true, responseCallback)
    assert(replied)
    assert(!isRemoteFetchExecuted)
  }

  def RemoteFetch(timeout: Boolean, responseCallback: (Seq[(TopicPartition, FetchPartitionData)]) => Unit): Unit = {
    val rlm = new MockRemoteLogManager(5, 20)
    val fetchInfo = new PartitionData(100, 0, 1000, Optional.of(1))
    val remoteFetchInfo = RemoteStorageFetchInfo(1000, true, tp, fetchInfo)

    val fetchPartitionStatus = FetchPartitionStatus(new LogOffsetMetadata(fetchInfo.fetchOffset), fetchInfo)

    val fetchPartitionStatus1 = FetchPartitionStatus(new LogOffsetMetadata(messageOffset = 50L, segmentBaseOffset = 0L,
      relativePositionInSegment = 250), new PartitionData(50, 0, 1, Optional.empty()))

    val fetchMetadata = FetchMetadata(fetchMinBytes = 1,
      fetchMaxBytes = 1000,
      hardMaxBytesLimit = true,
      fetchOnlyLeader = true,
      fetchIsolation = FetchLogEnd,
      isFromFollower = false,
      replicaId = 1,
      fetchPartitionStatus = List((tp1, fetchPartitionStatus1), (tp, fetchPartitionStatus)))

    val localReadResults: Seq[(TopicPartition, LogReadResult)] = List(
      (tp1, new LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
        highWatermark = -1L,
        leaderLogStartOffset = -1L,
        leaderLogEndOffset = -1L,
        followerLogStartOffset = -1L,
        fetchTimeMs = -1L,
        readSize = 0,
        lastStableOffset = None,
        exception = Some(new Exception()))),
      (tp, new LogReadResult(
        FetchDataInfo(LogOffsetMetadata(fetchInfo.fetchOffset), MemoryRecords.EMPTY, delayedRemoteStorageFetch = Some(remoteFetchInfo)),
        highWatermark = 2000,
        leaderLogStartOffset = 0,
        leaderLogEndOffset = 2000,
        followerLogStartOffset = 0,
        fetchTimeMs = 0,
        readSize = 1000,
        lastStableOffset = Some(1000),
        exception = None))
    )

    val partition: Partition = EasyMock.createMock(classOf[Partition])
    val replicaManager: ReplicaManager = EasyMock.createMock(classOf[ReplicaManager])
    EasyMock.expect(replicaManager.getPartitionOrException(
      EasyMock.anyObject[TopicPartition], EasyMock.anyBoolean()))
      .andReturn(partition).anyTimes()
    EasyMock.replay(replicaManager)

    val remoteFetchPurgatory = DelayedOperationPurgatory[DelayedRemoteFetch](purgatoryName = "RemoteFetch", brokerId = 1, purgeInterval = 50)
    val key = new TopicPartitionOperationKey(tp.topic(), tp.partition())
    val remoteFetchResult = new CompletableFuture[RemoteLogReadResult]
    var remoteFetchTask: RemoteLogManager#AsyncReadTask = null

    val remoteFetch = new DelayedRemoteFetch(remoteFetchTask = remoteFetchTask,
      remoteFetchResult = remoteFetchResult,
      remoteFetchInfo = localReadResults(1)._2.info.delayedRemoteStorageFetch.get,
      delayMs = 500,
      fetchMetadata = fetchMetadata,
      localReadResults = localReadResults,
      replicaManager = replicaManager,
      UnboundedQuota,
      responseCallback = responseCallback)

    assertEquals(false, remoteFetchPurgatory.tryCompleteElseWatch(remoteFetch, Seq(key)))

    if (timeout)
      rlm.pause()

    remoteFetchTask = rlm.asyncRead(remoteFetchInfo, (result: RemoteLogReadResult) => {
      isRemoteFetchExecuted = true
      remoteFetchResult.complete(result)
      remoteFetchPurgatory.checkAndComplete(key)
    })

    Thread.sleep(100)

    if (timeout) {
      assertEquals(false, remoteFetch.isCompleted)
      Thread.sleep(500)
    }

    assertEquals(true, remoteFetch.isCompleted)
  }
}


