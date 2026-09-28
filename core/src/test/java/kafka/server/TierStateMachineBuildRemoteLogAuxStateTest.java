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
package kafka.server;

import kafka.server.builders.LogManagerBuilder;
import kafka.server.builders.ReplicaManagerBuilder;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.metadata.KRaftMetadataCache;
import org.apache.kafka.metadata.MockConfigRepository;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.LeaderEndPoint;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.log.remote.storage.RemoteLogManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;
import org.apache.kafka.server.partition.AlterPartitionManager;
import org.apache.kafka.server.quota.QuotaFactory;
import org.apache.kafka.server.quota.QuotaFactory.QuotaManagers;
import org.apache.kafka.server.util.MockScheduler;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogManager;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that a failure while building the remote log auxiliary state does not leave the follower's
 * local log and its leader epoch cache in an inconsistent state (KAFKA-17249).
 */
public class TierStateMachineBuildRemoteLogAuxStateTest {

    private static final int BROKER_ID = 0;
    // The leader has tiered everything below LEADER_LOCAL_LOG_START_OFFSET and expired its log up to LEADER_LOG_START_OFFSET.
    private static final int LEADER_EPOCH = 5;
    private static final long LEADER_LOG_START_OFFSET = 10L;
    private static final long LEADER_LOCAL_LOG_START_OFFSET = 100L;
    // Leader epoch checkpoint of the last remote segment: version 0, one entry, epoch 0 starting at LEADER_LOG_START_OFFSET.
    private static final byte[] REMOTE_LEADER_EPOCH_CHECKPOINT =
        ("0\n1\n0 " + LEADER_LOG_START_OFFSET + "\n").getBytes(StandardCharsets.UTF_8);

    private final MockTime time = new MockTime();
    private final Uuid topicId = Uuid.randomUuid();
    private final TopicPartition topicPartition = new TopicPartition("test", 0);
    private final RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
    private final RemoteStorageManager remoteStorageManager = mock(RemoteStorageManager.class);

    private QuotaManagers quotaManagers;
    private ReplicaManager replicaManager;
    private TierStateMachine tierStateMachine;

    @BeforeEach
    public void setUp() throws IOException {
        KafkaConfig config = KafkaConfig.fromProps(brokerProperties());

        Properties logProps = new Properties();
        logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
        MockScheduler scheduler = new MockScheduler(time);
        LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(config.logDirs().size());
        // remote storage metrics must be registered, TierStateMachine.start() marks them
        BrokerTopicStats brokerTopicStats = new BrokerTopicStats(true);
        LogManager logManager = new LogManagerBuilder()
            .setLogDirs(config.logDirs().stream().map(File::new).toList())
            .setInitialOfflineDirs(List.of())
            .setConfigRepository(new MockConfigRepository())
            .setInitialDefaultConfig(new LogConfig(logProps))
            .setCleanerConfig(new CleanerConfig(false))
            .setProducerStateManagerConfig(60000, false)
            .setScheduler(scheduler)
            .setBrokerTopicStats(brokerTopicStats)
            .setLogDirFailureChannel(logDirFailureChannel)
            .setTime(time)
            .setRemoteStorageSystemEnable(true)
            .build();

        when(remoteLogManager.storageManager()).thenReturn(remoteStorageManager);
        when(remoteLogManager.isPartitionReady(any())).thenReturn(true);

        Metrics metrics = new Metrics();
        quotaManagers = QuotaFactory.instantiate(config, metrics, time, "", "");
        replicaManager = new ReplicaManagerBuilder()
            .setConfig(config)
            .setMetrics(metrics)
            .setTime(time)
            .setScheduler(scheduler)
            .setLogManager(logManager)
            .setRemoteLogManager(remoteLogManager)
            .setQuotaManagers(quotaManagers)
            .setMetadataCache(new KRaftMetadataCache(BROKER_ID, () -> KRaftVersion.KRAFT_VERSION_0))
            .setLogDirFailureChannel(logDirFailureChannel)
            .setAlterPartitionManager(mock(AlterPartitionManager.class))
            .setBrokerTopicStats(brokerTopicStats)
            .build();

        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new FeatureLevelRecord()
            .setName(MetadataVersion.FEATURE_NAME)
            .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel()));
        delta.replay(new TopicRecord().setName(topicPartition.topic()).setTopicId(topicId));
        delta.replay(new PartitionRecord()
            .setPartitionId(topicPartition.partition())
            .setTopicId(topicId)
            .setReplicas(List.of(BROKER_ID))
            .setIsr(List.of(BROKER_ID))
            .setLeader(BROKER_ID)
            .setLeaderEpoch(0)
            .setPartitionEpoch(0));
        replicaManager.applyDelta(delta.topicsDelta(), delta.apply(MetadataProvenance.EMPTY));

        tierStateMachine = new TierStateMachine(mock(LeaderEndPoint.class), replicaManager, false);
    }

    @AfterEach
    public void tearDown() {
        replicaManager.shutdown(false);
        quotaManagers.shutdown();
    }

    /**
     * A failure to read the leader epoch checkpoint or the producer snapshot from remote storage must leave the local
     * log unchanged.
     */
    @ParameterizedTest
    @EnumSource(value = IndexType.class, names = {"LEADER_EPOCH", "PRODUCER_SNAPSHOT"})
    public void testLocalStateIsUnchangedWhenRemoteIndexCannotBeRead(IndexType failingIndex) throws Exception {
        UnifiedLog log = seedFollowerLog();
        List<EpochEntry> epochEntriesBefore = log.leaderEpochCache().epochEntries();
        long logStartOffsetBefore = log.logStartOffset();
        long localLogStartOffsetBefore = log.localLogStartOffset();
        assertFalse(epochEntriesBefore.isEmpty(), "precondition: the follower has leader epoch entries");

        RemoteLogSegmentMetadata segmentMetadata = mock(RemoteLogSegmentMetadata.class);
        when(segmentMetadata.endOffset()).thenReturn(LEADER_LOCAL_LOG_START_OFFSET - 1);
        when(remoteLogManager.fetchRemoteLogSegmentMetadata(any(), anyInt(), anyLong()))
            .thenReturn(Optional.of(segmentMetadata));
        when(remoteStorageManager.fetchIndex(any(), any())).thenAnswer(invocation -> {
            IndexType indexType = invocation.getArgument(1);
            if (indexType == failingIndex) {
                throw new RemoteStorageException("Simulated failure while fetching the " + indexType + " index");
            }
            if (indexType == IndexType.LEADER_EPOCH) {
                return new ByteArrayInputStream(REMOTE_LEADER_EPOCH_CHECKPOINT);
            }
            throw new IllegalArgumentException("Unexpected remote index fetch: " + indexType);
        });

        // Epoch 0 lets the tier state machine skip asking the leader for the end offset of the previous epoch.
        assertThrows(RemoteStorageException.class, () -> tierStateMachine.start(
            topicPartition,
            Optional.of(topicId),
            LEADER_EPOCH,
            new OffsetAndEpoch(LEADER_LOCAL_LOG_START_OFFSET, 0),
            LEADER_LOG_START_OFFSET));

        assertAll("the local log must be left as it was before the failed attempt",
            () -> assertEquals(logStartOffsetBefore, log.logStartOffset(),
                "logStartOffset must not move when the remote log aux state could not be built"),
            () -> assertEquals(localLogStartOffsetBefore, log.localLogStartOffset(),
                "localLogStartOffset must not move when the remote log aux state could not be built"),
            () -> assertEquals(epochEntriesBefore, log.leaderEpochCache().epochEntries(),
                "the leader epoch cache must not change when the remote log aux state could not be built"),
            () -> assertEquals(epochEntriesBefore, readLeaderEpochCheckpointFromDisk(log),
                "the leader epoch checkpoint on disk must not change when the remote log aux state could not be built"));
    }

    private Properties brokerProperties() {
        Properties props = new Properties();
        props.put(KRaftConfigs.NODE_ID_CONFIG, String.valueOf(BROKER_ID));
        props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker");
        props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER");
        props.put(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(ServerLogConfigs.LOG_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        return props;
    }

    private UnifiedLog seedFollowerLog() throws IOException {
        UnifiedLog log = replicaManager.localLogOrException(topicPartition);
        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()), new SimpleRecord("c".getBytes())), 0);
        return log;
    }

    private static List<EpochEntry> readLeaderEpochCheckpointFromDisk(UnifiedLog log) throws IOException {
        return new LeaderEpochCheckpointFile(LeaderEpochCheckpointFile.newFile(log.dir()), new LogDirFailureChannel(1)).read();
    }
}
