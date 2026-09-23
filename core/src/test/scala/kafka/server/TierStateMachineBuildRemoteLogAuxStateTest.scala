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

import kafka.utils.{Logging, TestUtils}

import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.metadata.{FeatureLevelRecord, PartitionRecord, TopicRecord}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.record.internal.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.KRaftMetadataCache
import org.apache.kafka.server.LeaderEndPoint
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion, OffsetAndEpoch}
import org.apache.kafka.server.log.remote.storage.{RemoteLogManager, RemoteLogSegmentMetadata, RemoteStorageException, RemoteStorageManager}
import org.apache.kafka.server.partition.AlterPartitionManager
import org.apache.kafka.server.quota.QuotaFactory
import org.apache.kafka.server.quota.QuotaFactory.QuotaManagers
import org.apache.kafka.server.util.{MockScheduler, MockTime}
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile
import org.apache.kafka.storage.internals.log.{EpochEntry, LogConfig, LogDirFailureChannel, UnifiedLog}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.function.Executable
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito.{mock, when}

import java.io.File
import java.util.{Optional, Properties}
import scala.collection.Map
import scala.jdk.CollectionConverters._

/**
 * Tests that a failure while building the remote log auxiliary state does not leave the follower's
 * local log and its leader epoch cache in an inconsistent state (KAFKA-17249).
 */
class TierStateMachineBuildRemoteLogAuxStateTest extends Logging {

  private val time = new MockTime
  private val topicId = Uuid.randomUuid()
  private val topic = "test"
  private val partitionId = 0
  private val topicIdPartition = new TopicIdPartition(topicId, partitionId, topic)
  private val topicPartition = topicIdPartition.topicPartition()
  private val brokerId = 0

  // The leader has tiered everything below leaderLocalLogStartOffset and expired its log up to leaderLogStartOffset.
  private val leaderEpoch = 5
  private val leaderLogStartOffset = 10L
  private val leaderLocalLogStartOffset = 100L
  private val remoteEndOffset = leaderLocalLogStartOffset - 1

  private var replicaManager: ReplicaManager = _
  private var quotaManager: QuotaManagers = _
  private var remoteLogManager: RemoteLogManager = _
  private var remoteStorageManager: RemoteStorageManager = _
  private var tierStateMachine: TierStateMachine = _

  @BeforeEach
  def setUp(): Unit = {
    val props = TestUtils.createBrokerConfig(brokerId)
    val config = KafkaConfig.fromProps(props)

    val logProps = new Properties()
    logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    val defaultLogConfig = LogConfig.fromProps(Map.empty[String, Object].asJava, logProps)
    val logManager = TestUtils.createLogManager(
      config.logDirs.asScala.map(new File(_)),
      defaultConfig = defaultLogConfig,
      remoteStorageSystemEnable = true
    )

    remoteLogManager = mock(classOf[RemoteLogManager])
    remoteStorageManager = mock(classOf[RemoteStorageManager])
    when(remoteLogManager.storageManager()).thenReturn(remoteStorageManager)
    when(remoteLogManager.isPartitionReady(any())).thenReturn(true)

    val metrics = new Metrics
    quotaManager = QuotaFactory.instantiate(config, metrics, time, "", "")
    replicaManager = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = logManager,
      quotaManagers = quotaManager,
      metadataCache = new KRaftMetadataCache(config.brokerId, () => KRaftVersion.KRAFT_VERSION_0),
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = mock(classOf[AlterPartitionManager]),
      remoteLogManager = Some(remoteLogManager),
      // remote storage metrics must be registered, TierStateMachine.start() marks them
      brokerTopicStats = new BrokerTopicStats(true)
    )

    val delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build()
    delta.replay(new FeatureLevelRecord()
      .setName(MetadataVersion.FEATURE_NAME)
      .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel()))
    delta.replay(new TopicRecord().setName(topic).setTopicId(topicId))
    delta.replay(new PartitionRecord()
      .setPartitionId(partitionId)
      .setTopicId(topicId)
      .setReplicas(java.util.List.of[Integer](brokerId))
      .setIsr(java.util.List.of[Integer](brokerId))
      .setLeader(brokerId)
      .setLeaderEpoch(0)
      .setPartitionEpoch(0))
    replicaManager.applyDelta(delta.topicsDelta(), delta.apply(MetadataProvenance.EMPTY))

    tierStateMachine = new TierStateMachine(mock(classOf[LeaderEndPoint]), replicaManager, false)
  }

  @AfterEach
  def tearDown(): Unit = {
    Utils.swallow(this.logger.underlying, () => replicaManager.shutdown(checkpointHW = false))
    Utils.swallow(this.logger.underlying, () => quotaManager.shutdown())
  }

  /** A failure to read the leader epoch checkpoint from remote storage must leave the local log unchanged. */
  @Test
  def testLocalStateIsUnchangedWhenLeaderEpochCheckpointCannotBeReadFromRemote(): Unit = {
    val log = seedFollowerLog()
    val epochEntriesBefore = log.leaderEpochCache.epochEntries()
    val logStartOffsetBefore = log.logStartOffset
    val localLogStartOffsetBefore = log.localLogStartOffset()
    assertFalse(epochEntriesBefore.isEmpty, "precondition: the follower has leader epoch entries")

    val segmentMetadata = mock(classOf[RemoteLogSegmentMetadata])
    when(segmentMetadata.endOffset()).thenReturn(remoteEndOffset)
    when(remoteLogManager.fetchRemoteLogSegmentMetadata(any(), anyInt(), anyLong()))
      .thenReturn(Optional.of(segmentMetadata))
    when(remoteStorageManager.fetchIndex(any(), any()))
      .thenThrow(new RemoteStorageException("Simulated failure while fetching the leader epoch index"))

    // Epoch 0 lets the tier state machine skip asking the leader for the end offset of the previous epoch.
    assertThrows(classOf[RemoteStorageException], () => tierStateMachine.start(
      topicPartition,
      Optional.of(topicId),
      leaderEpoch,
      new OffsetAndEpoch(leaderLocalLogStartOffset, 0),
      leaderLogStartOffset))

    assertAll("the local log must be left as it was before the failed attempt", Seq[Executable](
      () => assertEquals(logStartOffsetBefore, log.logStartOffset,
        "logStartOffset must not move when the remote log aux state could not be built"),
      () => assertEquals(localLogStartOffsetBefore, log.localLogStartOffset(),
        "localLogStartOffset must not move when the remote log aux state could not be built"),
      () => assertEquals(epochEntriesBefore, log.leaderEpochCache.epochEntries(),
        "the leader epoch cache must not be cleared when the remote log aux state could not be built"),
      () => assertEquals(epochEntriesBefore, readLeaderEpochCheckpointFromDisk(log),
        "the leader epoch checkpoint on disk must not be cleared when the remote log aux state could not be built")
    ).asJava)
  }

  private def readLeaderEpochCheckpointFromDisk(log: UnifiedLog): java.util.List[EpochEntry] =
    new LeaderEpochCheckpointFile(LeaderEpochCheckpointFile.newFile(log.dir()), new LogDirFailureChannel(1)).read()

  private def seedFollowerLog(): UnifiedLog = {
    val log = replicaManager.localLogOrException(topicPartition)
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes), new SimpleRecord("c".getBytes)), 0)
    log
  }
}
