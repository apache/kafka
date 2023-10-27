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
import kafka.log.UnifiedLog
import kafka.log.remote.RemoteLogManager
import org.apache.kafka.common.errors.FencedLeaderEpochException
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record._
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import kafka.server.FetcherThreadTestUtils.{initialFetchState, mkBatch}
import org.apache.kafka.server.log.remote.storage.{RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageManager}
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogConfig, LogOffsetMetadata, ProducerStateManager, ProducerStateManagerConfig}
import org.apache.kafka.storage.internals.log.RemoteIndexCache.remoteLeaderEpochIndexFileName
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyLong}
import org.mockito.Mockito.{doNothing, doReturn, mock, when}
import unit.kafka.server.MockTierStateMachineWithRlm

import java.io.{File, FileInputStream}
import java.util.{Collections, Optional}
import scala.collection.Map

class ReplicaFetcherTierStateMachineTest {

  val truncateOnFetch = true
  val topicIds = Map("topic1" -> Uuid.randomUuid(), "topic2" -> Uuid.randomUuid())
  val version = ApiKeys.FETCH.latestVersion()
  private val failedPartitions = new FailedPartitions
  private val brokerId = 1
  private val producerStateManagerConfig = new ProducerStateManagerConfig(kafka.server.Defaults.ProducerIdExpirationMs, true)
  private val time = new MockTime
  private val producerId = 1L
  private var stateManager: ProducerStateManager = null
  private var logDir: File = null
  private var tpDirForRemoteSnapshotFile: File = null

  @Test
  def testFollowerFetchMovedToTieredStore(): Unit = {
    val partition = new TopicPartition("topic", 0)

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = PartitionState(replicaLog, leaderEpoch = 5, highWatermark = 0L, rlmEnabled = true)

    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 3L, leaderEpoch = 5)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 5, leaderEpoch = 5, new SimpleRecord("f".getBytes)),
      mkBatch(baseOffset = 6, leaderEpoch = 5, new SimpleRecord("g".getBytes)),
      mkBatch(baseOffset = 7, leaderEpoch = 5, new SimpleRecord("h".getBytes)),
      mkBatch(baseOffset = 8, leaderEpoch = 5, new SimpleRecord("i".getBytes)))

    val leaderState = PartitionState(leaderLog, leaderEpoch = 5, highWatermark = 8L, rlmEnabled = true)
    // Overriding the log start offset to zero for mocking the scenario of segment 0-4 moved to remote store.
    leaderState.logStartOffset = 0
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    assertEquals(3L, replicaState.logEndOffset)
    val expectedState = if (truncateOnFetch) Option(Fetching) else Option(Truncating)
    assertEquals(expectedState, fetcher.fetchState(partition).map(_.state))

    fetcher.doWork()
    // verify that the offset moved to tiered store error triggered and respective states are truncated to expected.
    assertEquals(0L, replicaState.logStartOffset)
    assertEquals(5L, replicaState.localLogStartOffset)
    assertEquals(5L, replicaState.highWatermark)
    assertEquals(5L, replicaState.logEndOffset)

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 5) fetcher.doWork()
    assertEquals(4, replicaState.log.size)
    assertEquals(0L, replicaState.logStartOffset)
    assertEquals(5L, replicaState.localLogStartOffset)
    assertEquals(8L, replicaState.highWatermark)
    assertEquals(9L, replicaState.logEndOffset)
  }

  @Test
  def testProducerStatesWhenFollowerFetchMovedToTieredStore(): Unit = {
    val partition = new TopicPartition("topic", 0)

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = PartitionState(replicaLog, leaderEpoch = 5, highWatermark = 0L, rlmEnabled = true)

    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
    val mockReplicaMgr = mock(classOf[ReplicaManager])
    val mockTierStateMachine = new MockTierStateMachineWithRlm(mockLeaderEndpoint, mockReplicaMgr)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    mockBuildRemoteLogAuxState(mockReplicaMgr, partition)

    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 3L, leaderEpoch = 5)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 5, leaderEpoch = 5, new SimpleRecord("f".getBytes)),
      mkBatch(baseOffset = 6, leaderEpoch = 5, new SimpleRecord("g".getBytes)),
      mkBatch(baseOffset = 7, leaderEpoch = 5, new SimpleRecord("h".getBytes)),
      mkBatch(baseOffset = 8, leaderEpoch = 5, new SimpleRecord("i".getBytes)))

    val leaderState = PartitionState(leaderLog, leaderEpoch = 5, highWatermark = 8L, rlmEnabled = true)
    // Overriding the log start offset to zero for mocking the scenario of segment 0-4 moved to remote store.
    leaderState.logStartOffset = 0
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()

    assertEquals(1, stateManager.activeProducers().size())
    assertEquals(7L, stateManager.latestSnapshotOffset().getAsLong)
    assertEquals(7L, stateManager.mapEndOffset())
  }

  @Test
  def testFencedOffsetResetAfterMovedToRemoteTier(): Unit = {
    val partition = new TopicPartition("topic", 0)
    var isErrorHandled = false
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint) {
      override def start(topicPartition: TopicPartition, currentFetchState: PartitionFetchState, fetchPartitionData: FetchResponseData.PartitionData): PartitionFetchState = {
        isErrorHandled = true
        throw new FencedLeaderEpochException(s"Epoch ${currentFetchState.currentLeaderEpoch} is fenced")
      }
    }
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, failedPartitions = failedPartitions)

    val replicaLog = Seq(
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))
    val replicaState = PartitionState(replicaLog, leaderEpoch = 5, highWatermark = 2L, rlmEnabled = true)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), fetchOffset = 0L, leaderEpoch = 5)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 5, leaderEpoch = 5, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 6, leaderEpoch = 5, new SimpleRecord("c".getBytes)))
    val leaderState = PartitionState(leaderLog, leaderEpoch = 5, highWatermark = 6L, rlmEnabled = true)
    // Overriding the log start offset to zero for mocking the scenario of segment 0-4 moved to remote store.
    leaderState.logStartOffset = 0
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    // After the offset moved to tiered storage error, we get a fenced error and remove the partition and mark as failed
    fetcher.doWork()
    assertEquals(3, replicaState.logEndOffset)
    assertTrue(isErrorHandled)
    assertTrue(fetcher.fetchState(partition).isEmpty)
    assertTrue(failedPartitions.contains(partition))
  }

  private def mockBuildRemoteLogAuxState(mockReplicaMgr: ReplicaManager, topicPartition: TopicPartition): Unit = {
    val mockUnifiedLog = mock(classOf[UnifiedLog])
    doReturn(mockUnifiedLog).when(mockReplicaMgr).localLogOrException(any(classOf[TopicPartition]))
    doReturn(true).when(mockUnifiedLog).remoteStorageSystemEnable
    val mockLogConfig = mock(classOf[LogConfig])
    doReturn(mockLogConfig).when(mockUnifiedLog).config
    doReturn(true).when(mockLogConfig).remoteStorageEnable()

    val mockRemoteLogManager = mock(classOf[RemoteLogManager])
    doReturn(Option.apply(mockRemoteLogManager)).when(mockReplicaMgr).remoteLogManager

    val remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
      RemoteLogSegmentId.generateNew(new TopicIdPartition(Uuid.randomUuid(), topicPartition)),
        5L, 6L, -1L, brokerId, -1L, 1024,
      Optional.empty, RemoteLogSegmentState.COPY_SEGMENT_FINISHED, Collections.singletonMap(5, 5L))
    doReturn(Optional.of(remoteLogSegmentMetadata)).when(mockRemoteLogManager).fetchRemoteLogSegmentMetadata(any(classOf[TopicPartition]), ArgumentMatchers.eq(5), ArgumentMatchers.eq(4L))

    val mockPartition = mock(classOf[Partition])
    doReturn(mockPartition).when(mockReplicaMgr).getPartitionOrException(any(classOf[TopicPartition]))
    doNothing().when(mockPartition).truncateFullyAndStartAt(anyLong(), anyBoolean(), any())
    doReturn(true).when(mockUnifiedLog).maybeIncrementLogStartOffset(anyLong(), any())

    val mockRemoteStorageManager = mock(classOf[RemoteStorageManager])
    doReturn(mockRemoteStorageManager).when(mockRemoteLogManager).storageManager()

    val idPartition = new TopicIdPartition(Uuid.randomUuid(), topicPartition)
    logDir = JTestUtils.tempDirectory(s"kafka-${this.getClass.getSimpleName}")
    val tpDir = JTestUtils.tempDirectory(logDir.toPath, idPartition.toString)
    doReturn(Option.empty).when(mockUnifiedLog).leaderEpochCache
    doReturn(tpDir).when(mockUnifiedLog).dir

    stateManager = new ProducerStateManager(topicPartition, tpDir, 5 * 60 * 1000, producerStateManagerConfig, time)
    tpDirForRemoteSnapshotFile = JTestUtils.tempDirectory(JTestUtils.tempDirectory(s"remote-kafka-${this.getClass.getSimpleName}").toPath, idPartition.toString)
    val stateManagerForRemoteSnapshotFile = new ProducerStateManager(topicPartition, tpDirForRemoteSnapshotFile, 5 * 60 * 1000, producerStateManagerConfig, time)
    doReturn(stateManager).when(mockUnifiedLog).producerStateManager
    val remoteSnapshotFile = prepareLocalAndRemoteSnapshotFile(stateManager, stateManagerForRemoteSnapshotFile)

    when(mockRemoteStorageManager.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), any(classOf[IndexType])))
      .thenAnswer(ans => {
        val indexType = ans.getArgument[IndexType](1)
        indexType match {
          case IndexType.LEADER_EPOCH => new FileInputStream(new File(remoteLeaderEpochIndexFileName(remoteLogSegmentMetadata)))
          case IndexType.PRODUCER_SNAPSHOT => new FileInputStream(remoteSnapshotFile)
          case IndexType.OFFSET => // not access here
          case IndexType.TIMESTAMP => // not access here
          case IndexType.TRANSACTION => // not access here
        }
      })
  }

  private def prepareLocalAndRemoteSnapshotFile(stateManager: ProducerStateManager, stateManagerForRemoteSnapshotFile: ProducerStateManager): File = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 3, 3L)
    append(stateManager, producerId, epoch, 4, 4L)
    stateManager.takeSnapshot()
    append(stateManagerForRemoteSnapshotFile, producerId, epoch, 3, 3L)
    append(stateManagerForRemoteSnapshotFile, producerId, epoch, 4, 4L)
    stateManagerForRemoteSnapshotFile.takeSnapshot()
    append(stateManagerForRemoteSnapshotFile, producerId, epoch, 5, 5L)
    append(stateManagerForRemoteSnapshotFile, producerId, epoch, 6, 6L)
    stateManagerForRemoteSnapshotFile.takeSnapshot()
    stateManagerForRemoteSnapshotFile.fetchSnapshot(5L).get()
  }

  private def append(stateManager: ProducerStateManager,
                     producerId: Long,
                     producerEpoch: Short,
                     seq: Int,
                     offset: Long,
                     timestamp: Long = time.milliseconds(),
                     isTransactional: Boolean = false,
                     origin : AppendOrigin = AppendOrigin.CLIENT): Unit = {
    val producerAppendInfo = stateManager.prepareUpdate(producerId, origin)
    producerAppendInfo.appendDataBatch(producerEpoch, seq, seq, timestamp,
      new LogOffsetMetadata(offset), offset, isTransactional)
    stateManager.update(producerAppendInfo)
    stateManager.updateMapEndOffset(offset + 1)
  }
}
