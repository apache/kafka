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
package kafka.cluster

import kafka.log.UnifiedLog
import kafka.server.metadata.{KRaftMetadataCache, ZkMetadataCache}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.NotLeaderOrFollowerException
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.LogOffsetMetadata
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito.{mock, when}

object ReplicaTest {
  val BrokerId: Int = 0
  val Partition: TopicPartition = new TopicPartition("foo", 0)
  val ReplicaLagTimeMaxMs: Long = 30000
}

class ReplicaTest {
  import ReplicaTest._

  val time = new MockTime()
  var replica: Replica = _

  @BeforeEach
  def setup(): Unit = {
    val metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.getAliveBrokerEpoch(BrokerId)).thenReturn(Option(1L))
    replica = new Replica(BrokerId, Partition, metadataCache)
  }

  private def assertReplicaState(
    logStartOffset: Long,
    logEndOffset: Long,
    lastCaughtUpTimeMs: Long,
    lastFetchLeaderLogEndOffset: Long,
    lastFetchTimeMs: Long,
    brokerEpoch: Option[Long] = Option[Long](1L)
  ): Unit = {
    val replicaState = replica.stateSnapshot
    assertEquals(logStartOffset, replicaState.logStartOffset,
      "Unexpected Log Start Offset")
    assertEquals(logEndOffset, replicaState.logEndOffset,
      "Unexpected Log End Offset")
    assertEquals(lastCaughtUpTimeMs, replicaState.lastCaughtUpTimeMs,
      "Unexpected Last Caught Up Time")
    assertEquals(lastFetchLeaderLogEndOffset, replicaState.lastFetchLeaderLogEndOffset,
      "Unexpected Last Fetch Leader Log End Offset")
    assertEquals(lastFetchTimeMs, replicaState.lastFetchTimeMs,
      "Unexpected Last Fetch Time")
    assertEquals(brokerEpoch, replicaState.brokerEpoch,
      "Broker Epoch Mismatch")
  }

  def assertReplicaStateDoesNotChange(
    op: => Unit
  ): Unit = {
    val previousState = replica.stateSnapshot

    op

    assertReplicaState(
      logStartOffset = previousState.logStartOffset,
      logEndOffset = previousState.logEndOffset,
      lastCaughtUpTimeMs = previousState.lastCaughtUpTimeMs,
      lastFetchLeaderLogEndOffset = previousState.lastFetchLeaderLogEndOffset,
      lastFetchTimeMs = previousState.lastFetchTimeMs
    )
  }

  private def updateFetchState(
    followerFetchOffset: Long,
    followerStartOffset: Long,
    leaderEndOffset: Long
  ): Long = {
    val currentTimeMs = time.milliseconds()
    replica.updateFetchStateOrThrow(
      followerFetchOffsetMetadata = new LogOffsetMetadata(followerFetchOffset),
      followerStartOffset = followerStartOffset,
      followerFetchTimeMs = currentTimeMs,
      leaderEndOffset = leaderEndOffset,
      brokerEpoch = 1L
    )
    currentTimeMs
  }

  private def resetReplicaState(
    leaderEndOffset: Long,
    isNewLeader: Boolean,
    isFollowerInSync: Boolean
  ): Long = {
    val currentTimeMs = time.milliseconds()
    replica.resetReplicaState(
      currentTimeMs = currentTimeMs,
      leaderEndOffset = leaderEndOffset,
      isNewLeader = isNewLeader,
      isFollowerInSync = isFollowerInSync
    )
    currentTimeMs
  }

  private def isCaughtUp(
    leaderEndOffset: Long
  ): Boolean = {
    replica.stateSnapshot.isCaughtUp(
      leaderEndOffset = leaderEndOffset,
      currentTimeMs = time.milliseconds(),
      replicaMaxLagMs = ReplicaLagTimeMaxMs
    )
  }

  @Test
  def testInitialState(): Unit = {
    assertReplicaState(
      logStartOffset = UnifiedLog.UnknownOffset,
      logEndOffset = UnifiedLog.UnknownOffset,
      lastCaughtUpTimeMs = 0L,
      lastFetchLeaderLogEndOffset = 0L,
      lastFetchTimeMs = 0L,
      brokerEpoch = Option.empty
    )
  }

  @Test
  def testUpdateFetchState(): Unit = {
    val fetchTimeMs1 = updateFetchState(
      followerFetchOffset = 5L,
      followerStartOffset = 1L,
      leaderEndOffset = 10L
    )

    assertReplicaState(
      logStartOffset = 1L,
      logEndOffset = 5L,
      lastCaughtUpTimeMs = 0L,
      lastFetchLeaderLogEndOffset = 10L,
      lastFetchTimeMs = fetchTimeMs1
    )

    val fetchTimeMs2 = updateFetchState(
      followerFetchOffset = 10L,
      followerStartOffset = 2L,
      leaderEndOffset = 15L
    )

    assertReplicaState(
      logStartOffset = 2L,
      logEndOffset = 10L,
      lastCaughtUpTimeMs = fetchTimeMs1,
      lastFetchLeaderLogEndOffset = 15L,
      lastFetchTimeMs = fetchTimeMs2
    )

    val fetchTimeMs3 = updateFetchState(
      followerFetchOffset = 15L,
      followerStartOffset = 3L,
      leaderEndOffset = 15L
    )

    assertReplicaState(
      logStartOffset = 3L,
      logEndOffset = 15L,
      lastCaughtUpTimeMs = fetchTimeMs3,
      lastFetchLeaderLogEndOffset = 15L,
      lastFetchTimeMs = fetchTimeMs3
    )
  }

  @Test
  def testResetReplicaStateWhenLeaderIsReelectedAndReplicaIsInSync(): Unit = {
    updateFetchState(
      followerFetchOffset = 10L,
      followerStartOffset = 1L,
      leaderEndOffset = 10L
    )

    val resetTimeMs1 = resetReplicaState(
      leaderEndOffset = 11L,
      isNewLeader = false,
      isFollowerInSync = true
    )

    assertReplicaState(
      logStartOffset = 1L,
      logEndOffset = 10L,
      lastCaughtUpTimeMs = resetTimeMs1,
      lastFetchLeaderLogEndOffset = 11L,
      lastFetchTimeMs = resetTimeMs1
    )
  }

  @Test
  def testResetReplicaStateWhenLeaderIsReelectedAndReplicaIsNotInSync(): Unit = {
    updateFetchState(
      followerFetchOffset = 10L,
      followerStartOffset = 1L,
      leaderEndOffset = 10L
    )

    resetReplicaState(
      leaderEndOffset = 11L,
      isNewLeader = false,
      isFollowerInSync = false
    )

    assertReplicaState(
      logStartOffset = 1L,
      logEndOffset = 10L,
      lastCaughtUpTimeMs = 0L,
      lastFetchLeaderLogEndOffset = 11L,
      lastFetchTimeMs = 0L
    )
  }

  @Test
  def testResetReplicaStateWhenNewLeaderIsElectedAndReplicaIsInSync(): Unit = {
    updateFetchState(
      followerFetchOffset = 10L,
      followerStartOffset = 1L,
      leaderEndOffset = 10L
    )

    val resetTimeMs1 = resetReplicaState(
      leaderEndOffset = 11L,
      isNewLeader = true,
      isFollowerInSync = true
    )

    assertReplicaState(
      logStartOffset = UnifiedLog.UnknownOffset,
      logEndOffset = UnifiedLog.UnknownOffset,
      lastCaughtUpTimeMs = resetTimeMs1,
      lastFetchLeaderLogEndOffset = UnifiedLog.UnknownOffset,
      lastFetchTimeMs = 0L,
      brokerEpoch = Option.empty
    )
  }

  @Test
  def testResetReplicaStateWhenNewLeaderIsElectedAndReplicaIsNotInSync(): Unit = {
    updateFetchState(
      followerFetchOffset = 10L,
      followerStartOffset = 1L,
      leaderEndOffset = 10L
    )

    resetReplicaState(
      leaderEndOffset = 11L,
      isNewLeader = true,
      isFollowerInSync = false
    )

    assertReplicaState(
      logStartOffset = UnifiedLog.UnknownOffset,
      logEndOffset = UnifiedLog.UnknownOffset,
      lastCaughtUpTimeMs = 0L,
      lastFetchLeaderLogEndOffset = UnifiedLog.UnknownOffset,
      lastFetchTimeMs = 0L,
      brokerEpoch = Option.empty
    )
  }

  @Test
  def testIsCaughtUpWhenReplicaIsCaughtUpToLogEnd(): Unit = {
    assertFalse(isCaughtUp(leaderEndOffset = 10L))

    updateFetchState(
      followerFetchOffset = 10L,
      followerStartOffset = 1L,
      leaderEndOffset = 10L
    )

    assertTrue(isCaughtUp(leaderEndOffset = 10L))

    time.sleep(ReplicaLagTimeMaxMs + 1)

    assertTrue(isCaughtUp(leaderEndOffset = 10L))
  }

  @Test
  def testIsCaughtUpWhenReplicaIsNotCaughtUpToLogEnd(): Unit = {
    assertFalse(isCaughtUp(leaderEndOffset = 10L))

    updateFetchState(
      followerFetchOffset = 5L,
      followerStartOffset = 1L,
      leaderEndOffset = 10L
    )

    assertFalse(isCaughtUp(leaderEndOffset = 10L))

    updateFetchState(
      followerFetchOffset = 10L,
      followerStartOffset = 1L,
      leaderEndOffset = 15L
    )

    assertTrue(isCaughtUp(leaderEndOffset = 16L))

    time.sleep(ReplicaLagTimeMaxMs + 1)

    assertFalse(isCaughtUp(leaderEndOffset = 16L))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testFenceStaleUpdates(isKraft: Boolean): Unit = {
    val metadataCache = if (isKraft) {
      val kRaftMetadataCache = mock(classOf[KRaftMetadataCache])
      when(kRaftMetadataCache.getAliveBrokerEpoch(BrokerId)).thenReturn(Option(2L))
      kRaftMetadataCache
    } else {
      mock(classOf[ZkMetadataCache])
    }

    val replica = new Replica(BrokerId, Partition, metadataCache)
    replica.updateFetchStateOrThrow(
      followerFetchOffsetMetadata = new LogOffsetMetadata(5L),
      followerStartOffset = 1L,
      followerFetchTimeMs = 1,
      leaderEndOffset = 10L,
      brokerEpoch = 2L
    )
    if (isKraft) {
      assertThrows(classOf[NotLeaderOrFollowerException], () => replica.updateFetchStateOrThrow(
        followerFetchOffsetMetadata = new LogOffsetMetadata(5L),
        followerStartOffset = 2L,
        followerFetchTimeMs = 3,
        leaderEndOffset = 10L,
        brokerEpoch = 1L
      ))
    } else {
      // No exception to expect under ZK mode.
      replica.updateFetchStateOrThrow(
        followerFetchOffsetMetadata = new LogOffsetMetadata(5L),
        followerStartOffset = 2L,
        followerFetchTimeMs = 3,
        leaderEndOffset = 10L,
        brokerEpoch = 1L
      )
    }
    replica.updateFetchStateOrThrow(
      followerFetchOffsetMetadata = new LogOffsetMetadata(5L),
      followerStartOffset = 2L,
      followerFetchTimeMs = 4,
      leaderEndOffset = 10L,
      brokerEpoch = -1L
    )
  }
}
