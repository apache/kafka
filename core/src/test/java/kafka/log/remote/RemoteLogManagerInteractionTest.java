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
package kafka.log.remote;

import kafka.log.LogTestUtils;
import kafka.log.UnifiedLog;
import kafka.log.remote.quota.InMemoryRemoteLogMetadataManager;
import kafka.log.remote.quota.RLMQuotaManager;
import kafka.server.BrokerTopicStats;
import kafka.server.KafkaConfig;
import kafka.server.RequestLocal;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.LogStartOffsetIncrementReason;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import scala.Option;

import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteLogManagerInteractionTest {
    private static final Random RANDOM = new Random(0);

    private KafkaConfig config;
    private final String clusterId = "dummyId";
    private final int retentionSize = 10_000_000;
    private final File logDir = TestUtils.tempDirectory("kafka-");
    private final File partitionDir = kafka.utils.TestUtils.randomPartitionLogDir(logDir);
    private BrokerTopicStats brokerTopicStats;
    private final MockTime mockTime = new MockTime();
    private Uuid topicId;
    private UnifiedLog log;
    private final Metrics metrics = new Metrics();

    private final RemoteStorageManager remoteStorageManager = Mockito.mock(RemoteStorageManager.class);
    private final InMemoryRemoteLogMetadataManager remoteLogMetadataManager = new InMemoryRemoteLogMetadataManager();
    private final RLMQuotaManager rlmCopyQuotaManager = mock(RLMQuotaManager.class);
    private RemoteLogManager remoteLogManager;

    private final String remoteLogStorageTestProp = "remote.log.storage.test";
    private final String remoteLogStorageTestVal = "storage.test";

    private TopicIdPartition topicIdPartition;


    @BeforeEach
    void setUp() throws Exception {
        Properties props = kafka.utils.TestUtils.createDummyBrokerConfig();
        props.setProperty(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true");
        props.setProperty(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP, "100");
        appendRLMConfig(props);
        config = KafkaConfig.fromProps(props);
        topicId = Uuid.randomUuid();
        topicIdPartition = new TopicIdPartition(topicId, new TopicPartition("test", 0));
        brokerTopicStats = new BrokerTopicStats(KafkaConfig.fromProps(props).remoteLogManagerConfig().isRemoteStorageSystemEnabled());

        HashMap<String, Object> properties = new HashMap<>();
        properties.put("retention.bytes", retentionSize);
        properties.put("local.retention.bytes", "1");
        properties.put("remote.storage.enable", "true");

        LogConfig logConfig = new LogConfig(properties);
        log = LogTestUtils.createLog(
            partitionDir, logConfig, brokerTopicStats, mockTime.scheduler, mockTime, 0L, 0L, 5 * 60 * 1000,
            new ProducerStateManagerConfig(86400000, false), 600000, true,
            Option.apply(topicId), true, new ConcurrentHashMap<>(), true, null, LogOffsetsListener.NO_OP_OFFSETS_LISTENER
        );

        remoteLogManager = new RemoteLogManager(config.remoteLogManagerConfig(), 1, logDir.toString(), clusterId, mockTime,
            tp -> Optional.of(log),
            (topicPartition, offset) -> log.maybeIncrementLogStartOffset(offset, LogStartOffsetIncrementReason.SegmentDeletion),
            brokerTopicStats, metrics) {
            public RemoteStorageManager createRemoteStorageManager() {
                return remoteStorageManager;
            }

            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }

            public RLMQuotaManager createRLMCopyQuotaManager() {
                return rlmCopyQuotaManager;
            }

            public Duration quotaTimeout() {
                return Duration.ofMillis(100);
            }

            @Override
            long findLogStartOffset(TopicIdPartition topicIdPartition, UnifiedLog log) {
                return 0L;
            }
        };
        remoteLogMetadataManager.initialise(topicIdPartition);
        remoteLogManager.startup();
    }

    @AfterEach
    void cleanup() {
        log.close();
    }

    private void appendRLMConfig(Properties props) {
        props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true);
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, NoOpRemoteStorageManager.class.getName());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, NoOpRemoteLogMetadataManager.class.getName());
        props.put(DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + remoteLogStorageTestProp, remoteLogStorageTestVal);
    }

    /**
     * Builds a segment of the given size and append it to the log, then roll the segment after.
     * Each segment has some overhead of around 70bytes which impact log retention policy.
     *
     * @param size size of the segment to create
     * @param count number of messages to create in the segment
     */
    private void buildSegment(int size, int count) {
        byte[] bytes = new byte[size / count];
        for (int i = 0; i < count; i++) {
            RANDOM.nextBytes(bytes);
            MemoryRecords records = MemoryRecords.withRecords(
                RecordBatch.MAGIC_VALUE_V2, 0, Compression.NONE, TimestampType.CREATE_TIME,
                new SimpleRecord(bytes)
            );
            log.appendAsLeader(records, 0, AppendOrigin.CLIENT,
                MetadataVersion.latestTesting(), RequestLocal.NoCaching(), VerificationGuard.SENTINEL
            );
        }
        log.roll(Option.empty());
    }

    /**
     * Builds a segment of the given size with a default of 100 messages and append it to the log
     * then roll the segment after.
     * Each segment has some overhead of around 70bytes which impact log retention policy.
     *
     * @param size size of the segment to create
     */
    private void buildSegment(int size) {
        buildSegment(size, 100);
    }

    @Test
    void nonActiveSegmentsAreCorrectlyWritten() throws RemoteStorageException {
        RemoteLogManager.RLMTask task = remoteLogManager.new RLMCopyTask(topicIdPartition, 128);

        buildSegment(retentionSize);
        buildSegment(retentionSize);
        buildSegment(retentionSize);
        log.maybeUpdateHighWatermark(log.logEndOffset());

        task.run();

        verify(remoteStorageManager, times(3)).copyLogSegmentData(any(), any());
    }

    @Test
    void failedSegmentAreRetried() throws RemoteStorageException {
        RemoteLogManager.RLMTask task = remoteLogManager.new RLMCopyTask(topicIdPartition, 128);

        buildSegment(retentionSize);
        buildSegment(retentionSize);
        buildSegment(retentionSize);
        log.maybeUpdateHighWatermark(log.logEndOffset());

        when(remoteStorageManager.copyLogSegmentData(any(), any()))
            .thenThrow(new RemoteStorageException(""))
            .thenReturn(Optional.empty());

        task.run();
        task.run();

        verify(remoteStorageManager, times(4)).copyLogSegmentData(any(), any());
    }

    @Test
    void failedSegmentsAreNotAccountedInSizeForRetention() throws RemoteStorageException {
        // We're testing the scenario where the first segment is correctly pushed then the next segment fails to get
        // uploaded multiple times before succeeding again.
        // At the end we should see no call to delete as retention.bytes was never breached
        RemoteLogManager.RLMTask copyTask = remoteLogManager.new RLMCopyTask(topicIdPartition, 128);
        RemoteLogManager.RLMTask cleanupTask = remoteLogManager.new RLMExpirationTask(topicIdPartition);

        buildSegment(retentionSize /  3);
        log.maybeUpdateHighWatermark(log.logEndOffset());
        when(remoteStorageManager.copyLogSegmentData(any(), any())).thenReturn(Optional.empty());
        copyTask.run();

        buildSegment(retentionSize /  3);
        log.maybeUpdateHighWatermark(log.logEndOffset());
        when(remoteStorageManager.copyLogSegmentData(any(), any())).thenThrow(new RemoteStorageException(""));
        for (int i = 0; i < 4; i++) {
            copyTask.run();
        }
        doReturn(Optional.empty()).when(remoteStorageManager).copyLogSegmentData(any(), any());
        copyTask.run();

        buildSegment(retentionSize /  3);
        log.maybeUpdateHighWatermark(log.logEndOffset());
        copyTask.run();
        cleanupTask.run();

        // total is 1 (success) + 4 (failures) + 2x1 (success)
        verify(remoteStorageManager, times(7)).copyLogSegmentData(any(), any());
        verify(remoteStorageManager, never()).deleteLogSegmentData(
            argThat(segment -> segment.state().equals(RemoteLogSegmentState.COPY_SEGMENT_FINISHED))
        );
        verify(remoteStorageManager, times(4)).deleteLogSegmentData(
            argThat(segment -> segment.state().equals(RemoteLogSegmentState.COPY_SEGMENT_STARTED))
        );
    }

    @Test
    void failedSegmentsAreNotAccountedInSizeDuringDeletion() throws RemoteStorageException {
        RemoteLogManager.RLMTask copyTask = remoteLogManager.new RLMCopyTask(topicIdPartition, 128);
        RemoteLogManager.RLMTask cleanupTask = remoteLogManager.new RLMExpirationTask(topicIdPartition);

        buildSegment(retentionSize - 100);
        log.maybeUpdateHighWatermark(log.logEndOffset());
        when(remoteStorageManager.copyLogSegmentData(any(), any())).thenThrow(new RemoteStorageException(""));
        for (int i = 0; i < 4; i++) {
            copyTask.run();
        }
        doReturn(Optional.empty()).when(remoteStorageManager).copyLogSegmentData(any(), any());
        copyTask.run();

        buildSegment(retentionSize - 100);
        log.maybeUpdateHighWatermark(log.logEndOffset());
        copyTask.run();
        cleanupTask.run();

        // total is 1 (success) + 4 (failures) + 1 (success)
        verify(remoteStorageManager, times(6)).copyLogSegmentData(any(), any());

        // The first segment should now be deleted
        verify(remoteStorageManager, times(5)).deleteLogSegmentData(
            argThat(segment -> segment.startOffset() == 0)
        );
        verify(remoteStorageManager, never()).deleteLogSegmentData(argThat(segment -> segment.startOffset() == 1));
    }

    @Test
    void failedDeletionsAreRetried() throws RemoteStorageException {
        RemoteLogManager.RLMTask copyTask = remoteLogManager.new RLMCopyTask(topicIdPartition, 128);
        RemoteLogManager.RLMTask cleanupTask = remoteLogManager.new RLMExpirationTask(topicIdPartition);

        buildSegment(retentionSize);
        buildSegment(retentionSize);
        log.maybeUpdateHighWatermark(log.logEndOffset());
        copyTask.run();

        when(remoteStorageManager.copyLogSegmentData(any(), any())).thenReturn(Optional.empty());
        doThrow(new RemoteStorageException("")).when(remoteStorageManager).deleteLogSegmentData(any());
        cleanupTask.run();
        verify(remoteStorageManager, times(1)).deleteLogSegmentData(any());

        clearInvocations(remoteStorageManager);
        doNothing().when(remoteStorageManager).deleteLogSegmentData(any());
        cleanupTask.run();
        cleanupTask.run();
        verify(remoteStorageManager, times(1)).deleteLogSegmentData(any());
    }
}
