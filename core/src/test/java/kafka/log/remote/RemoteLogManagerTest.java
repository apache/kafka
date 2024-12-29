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

import kafka.cluster.Partition;
import kafka.log.UnifiedLog;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RemoteLogInputStream;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.api.Flaky;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.common.StopPartition;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.log.remote.quota.RLMQuotaManager;
import org.apache.kafka.server.log.remote.quota.RLMQuotaManagerConfig;
import org.apache.kafka.server.log.remote.storage.ClassLoaderAwareRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.MockScheduler;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LazyIndex;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.OffsetIndex;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;
import org.apache.kafka.storage.internals.log.TimeIndex;
import org.apache.kafka.storage.internals.log.TransactionIndex;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.opentest4j.AssertionFailedError;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

import static kafka.log.remote.RemoteLogManager.isRemoteSegmentWithinLeaderEpochs;
import static org.apache.kafka.common.record.TimestampType.CREATE_TIME;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_CONSUMER_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_PRODUCER_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_NUM;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_SIZE_SECONDS;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_NUM;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_SIZE_SECONDS;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics.REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics.REMOTE_LOG_READER_FETCH_RATE_AND_TIME_METRIC;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics.REMOTE_STORAGE_THREAD_POOL_METRICS;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RemoteLogManagerTest {
    private final Time time = new MockTime();
    private final int brokerId = 0;
    private final String logDir = TestUtils.tempDirectory("kafka-").toString();
    private final String clusterId = "dummyId";
    private final String remoteLogStorageTestProp = "remote.log.storage.test";
    private final String remoteLogStorageTestVal = "storage.test";
    private final String remoteLogMetadataTestProp = "remote.log.metadata.test";
    private final String remoteLogMetadataTestVal = "metadata.test";
    private final String remoteLogMetadataCommonClientTestProp = REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "common.client.test";
    private final String remoteLogMetadataCommonClientTestVal = "common.test";
    private final String remoteLogMetadataProducerTestProp = REMOTE_LOG_METADATA_PRODUCER_PREFIX + "producer.test";
    private final String remoteLogMetadataProducerTestVal = "producer.test";
    private final String remoteLogMetadataConsumerTestProp = REMOTE_LOG_METADATA_CONSUMER_PREFIX + "consumer.test";
    private final String remoteLogMetadataConsumerTestVal = "consumer.test";
    private final String remoteLogMetadataTopicPartitionsNum = "1";
    private final long quotaExceededThrottleTime = 1000L;
    private final long quotaAvailableThrottleTime = 0L;

    private final RemoteStorageManager remoteStorageManager = mock(RemoteStorageManager.class);
    private final RemoteLogMetadataManager remoteLogMetadataManager = mock(RemoteLogMetadataManager.class);
    private final RLMQuotaManager rlmCopyQuotaManager = mock(RLMQuotaManager.class);
    private KafkaConfig config;

    private BrokerTopicStats brokerTopicStats = null;
    private final Metrics metrics = new Metrics(time);
    private RemoteLogManager remoteLogManager = null;

    private final TopicIdPartition leaderTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("Leader", 0));
    private final String leaderTopic = "Leader";
    private final TopicIdPartition followerTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("Follower", 0));
    private final Map<String, Uuid> topicIds = new HashMap<>();
    private final TopicPartition tp = new TopicPartition("TestTopic", 5);
    private final EpochEntry epochEntry0 = new EpochEntry(0, 0);
    private final EpochEntry epochEntry1 = new EpochEntry(1, 100);
    private final EpochEntry epochEntry2 = new EpochEntry(2, 200);
    private final List<EpochEntry> totalEpochEntries = Arrays.asList(epochEntry0, epochEntry1, epochEntry2);
    private LeaderEpochCheckpointFile checkpoint;
    private final AtomicLong currentLogStartOffset = new AtomicLong(0L);

    private UnifiedLog mockLog = mock(UnifiedLog.class);

    private final MockScheduler scheduler = new MockScheduler(time);
    private final Properties brokerConfig = kafka.utils.TestUtils.createDummyBrokerConfig();

    @BeforeEach
    void setUp() throws Exception {
        checkpoint = new LeaderEpochCheckpointFile(TestUtils.tempFile(), new LogDirFailureChannel(1));
        topicIds.put(leaderTopicIdPartition.topicPartition().topic(), leaderTopicIdPartition.topicId());
        topicIds.put(followerTopicIdPartition.topicPartition().topic(), followerTopicIdPartition.topicId());
        Properties props = brokerConfig;
        props.setProperty(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true");
        props.setProperty(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP, "100");
        appendRLMConfig(props);
        config = KafkaConfig.fromProps(props);
        brokerTopicStats = new BrokerTopicStats(config.remoteLogManagerConfig().isRemoteStorageSystemEnabled());

        remoteLogManager = new RemoteLogManager(config.remoteLogManagerConfig(), brokerId, logDir, clusterId, time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> currentLogStartOffset.set(offset),
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
        doReturn(true).when(remoteLogMetadataManager).isReady(any(TopicIdPartition.class));
    }

    @AfterEach
    void tearDown() {
        if (remoteLogManager != null) {
            remoteLogManager.close();
            remoteLogManager = null;
        }
        kafka.utils.TestUtils.clearYammerMetrics();
    }

    @Test
    void testGetLeaderEpochCheckpoint() {
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        assertEquals(totalEpochEntries, remoteLogManager.getLeaderEpochEntries(mockLog, 0, 300));

        List<EpochEntry> epochEntries = remoteLogManager.getLeaderEpochEntries(mockLog, 100, 200);
        assertEquals(1, epochEntries.size());
        assertEquals(epochEntry1, epochEntries.get(0));
    }

    @Test
    void testFindHighestRemoteOffsetOnEmptyRemoteStorage() throws  RemoteStorageException {
        List<EpochEntry> totalEpochEntries = Arrays.asList(
                new EpochEntry(0, 0),
                new EpochEntry(1, 500)
        );
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        TopicIdPartition tpId = new TopicIdPartition(Uuid.randomUuid(), tp);
        OffsetAndEpoch offsetAndEpoch = remoteLogManager.findHighestRemoteOffset(tpId, mockLog);
        assertEquals(new OffsetAndEpoch(-1L, -1), offsetAndEpoch);
    }

    @Test
    void testFindHighestRemoteOffset() throws RemoteStorageException {
        List<EpochEntry> totalEpochEntries = Arrays.asList(
                new EpochEntry(0, 0),
                new EpochEntry(1, 500)
        );
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        TopicIdPartition tpId = new TopicIdPartition(Uuid.randomUuid(), tp);
        when(remoteLogMetadataManager.highestOffsetForEpoch(eq(tpId), anyInt())).thenAnswer(ans -> {
            Integer epoch = ans.getArgument(1, Integer.class);
            if (epoch == 0) {
                return Optional.of(200L);
            } else {
                return Optional.empty();
            }
        });
        OffsetAndEpoch offsetAndEpoch = remoteLogManager.findHighestRemoteOffset(tpId, mockLog);
        assertEquals(new OffsetAndEpoch(200L, 0), offsetAndEpoch);
    }

    @Test
    void testFindHighestRemoteOffsetWithUncleanLeaderElection() throws RemoteStorageException {
        List<EpochEntry> totalEpochEntries = Arrays.asList(
                new EpochEntry(0, 0),
                new EpochEntry(1, 150),
                new EpochEntry(2, 300)
        );
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        TopicIdPartition tpId = new TopicIdPartition(Uuid.randomUuid(), tp);
        when(remoteLogMetadataManager.highestOffsetForEpoch(eq(tpId), anyInt())).thenAnswer(ans -> {
            Integer epoch = ans.getArgument(1, Integer.class);
            if (epoch == 0) {
                return Optional.of(200L);
            } else {
                return Optional.empty();
            }
        });
        OffsetAndEpoch offsetAndEpoch = remoteLogManager.findHighestRemoteOffset(tpId, mockLog);
        assertEquals(new OffsetAndEpoch(149L, 0), offsetAndEpoch);
    }

    @Test
    void testRemoteLogMetadataManagerWithUserDefinedConfigs() {
        String key = "key";
        String configPrefix = "config.prefix";
        Properties props = new Properties();
        props.putAll(brokerConfig);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP, configPrefix);
        props.put(configPrefix + key, "world");
        props.put("remote.log.metadata.y", "z");
        appendRLMConfig(props);
        KafkaConfig config = KafkaConfig.fromProps(props);

        Map<String, Object> metadataMangerConfig = config.remoteLogManagerConfig().remoteLogMetadataManagerProps();
        assertEquals(props.get(configPrefix + key), metadataMangerConfig.get(key));
        assertFalse(metadataMangerConfig.containsKey("remote.log.metadata.y"));
    }

    @Test
    void testRemoteStorageManagerWithUserDefinedConfigs() {
        String key = "key";
        String configPrefix = "config.prefix";
        Properties props = new Properties();
        props.putAll(brokerConfig);
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP, configPrefix);
        props.put(configPrefix + key, "world");
        props.put("remote.storage.manager.y", "z");
        appendRLMConfig(props);
        KafkaConfig config = KafkaConfig.fromProps(props);

        Map<String, Object> remoteStorageManagerConfig = config.remoteLogManagerConfig().remoteStorageManagerProps();
        assertEquals(props.get(configPrefix + key), remoteStorageManagerConfig.get(key));
        assertFalse(remoteStorageManagerConfig.containsKey("remote.storage.manager.y"));
    }

    @Test
    void testRemoteLogMetadataManagerWithEndpointConfig() {
        String host = "localhost";
        int port = 1234;
        String securityProtocol = "PLAINTEXT";
        Endpoint endPoint = new Endpoint(securityProtocol, SecurityProtocol.PLAINTEXT, host, port);
        remoteLogManager.onEndPointCreated(endPoint);
        remoteLogManager.startup();

        ArgumentCaptor<Map<String, Object>> capture = ArgumentCaptor.forClass(Map.class);
        verify(remoteLogMetadataManager, times(1)).configure(capture.capture());
        assertEquals(host + ":" + port, capture.getValue().get(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "bootstrap.servers"));
        assertEquals(securityProtocol, capture.getValue().get(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "security.protocol"));
        assertEquals(clusterId, capture.getValue().get("cluster.id"));
        assertEquals(brokerId, capture.getValue().get(ServerConfigs.BROKER_ID_CONFIG));
    }

    @Test
    void testRemoteLogMetadataManagerWithEndpointConfigOverridden() throws IOException {
        Properties props = new Properties();
        props.putAll(brokerConfig);
        // override common security.protocol by adding "RLMM prefix" and "remote log metadata common client prefix"
        props.put(DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX + REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "security.protocol", "SSL");
        appendRLMConfig(props);
        KafkaConfig config = KafkaConfig.fromProps(props);
        try (RemoteLogManager remoteLogManager = new RemoteLogManager(
                config.remoteLogManagerConfig(),
                brokerId,
                logDir,
                clusterId,
                time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> { },
                brokerTopicStats,
                metrics) {
            public RemoteStorageManager createRemoteStorageManager() {
                return remoteStorageManager;
            }
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }
        }) {

            String host = "localhost";
            int port = 1234;
            String securityProtocol = "PLAINTEXT";
            Endpoint endpoint = new Endpoint(securityProtocol, SecurityProtocol.PLAINTEXT, host, port);
            remoteLogManager.onEndPointCreated(endpoint);
            remoteLogManager.startup();

            ArgumentCaptor<Map<String, Object>> capture = ArgumentCaptor.forClass(Map.class);
            verify(remoteLogMetadataManager, times(1)).configure(capture.capture());
            assertEquals(host + ":" + port, capture.getValue().get(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "bootstrap.servers"));
            // should be overridden as SSL
            assertEquals("SSL", capture.getValue().get(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "security.protocol"));
            assertEquals(clusterId, capture.getValue().get("cluster.id"));
            assertEquals(brokerId, capture.getValue().get(ServerConfigs.BROKER_ID_CONFIG));
        }
    }

    @Test
    void testStartup() {
        remoteLogManager.startup();
        ArgumentCaptor<Map<String, Object>> capture = ArgumentCaptor.forClass(Map.class);
        verify(remoteStorageManager, times(1)).configure(capture.capture());
        assertEquals(brokerId, capture.getValue().get("broker.id"));
        assertEquals(remoteLogStorageTestVal, capture.getValue().get(remoteLogStorageTestProp));

        verify(remoteLogMetadataManager, times(1)).configure(capture.capture());
        assertEquals(brokerId, capture.getValue().get("broker.id"));
        assertEquals(logDir, capture.getValue().get("log.dir"));

        // verify the configs starting with "remote.log.metadata", "remote.log.metadata.common.client."
        // "remote.log.metadata.producer.", and "remote.log.metadata.consumer." are correctly passed in
        assertEquals(remoteLogMetadataTopicPartitionsNum, capture.getValue().get(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP));
        assertEquals(remoteLogMetadataTestVal, capture.getValue().get(remoteLogMetadataTestProp));
        assertEquals(remoteLogMetadataConsumerTestVal, capture.getValue().get(remoteLogMetadataConsumerTestProp));
        assertEquals(remoteLogMetadataProducerTestVal, capture.getValue().get(remoteLogMetadataProducerTestProp));
        assertEquals(remoteLogMetadataCommonClientTestVal, capture.getValue().get(remoteLogMetadataCommonClientTestProp));
    }

    // This test creates 2 log segments, 1st one has start offset of 0, 2nd one (and active one) has start offset of 150.
    // The leader epochs are [0->0, 1->100, 2->200]. We are verifying:
    // 1. There's only 1 segment copied to remote storage
    // 2. The segment got copied to remote storage is the old segment, not the active one
    // 3. The log segment metadata stored into remoteLogMetadataManager is what we expected, both before and after copying the log segments
    // 4. The log segment got copied to remote storage has the expected metadata
    // 5. The highest remote offset is updated to the expected value
    @Test
    void testCopyLogSegmentsToRemoteShouldCopyExpectedLogSegment() throws Exception {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;
        long lso = 250L;
        long leo = 300L;
        assertCopyExpectedLogSegmentsToRemote(oldSegmentStartOffset, nextSegmentStartOffset, lso, leo);
    }

    /**
     * The following values will be equal when the active segment gets rotated to passive and there are no new messages:
     * last-stable-offset = high-water-mark = log-end-offset = base-offset-of-active-segment.
     * This test asserts that the active log segment that was rotated after log.roll.ms are copied to remote storage.
     */
    @Test
    void testCopyLogSegmentToRemoteForStaleTopic() throws Exception {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;
        long lso = 150L;
        long leo = 150L;
        assertCopyExpectedLogSegmentsToRemote(oldSegmentStartOffset, nextSegmentStartOffset, lso, leo);
    }

    private void assertCopyExpectedLogSegmentsToRemote(long oldSegmentStartOffset,
                                                       long nextSegmentStartOffset,
                                                       long lastStableOffset,
                                                       long logEndOffset) throws Exception {
        long oldSegmentEndOffset = nextSegmentStartOffset - 1;
        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt())).thenReturn(Optional.of(-1L));

        File tempFile = TestUtils.tempFile();
        File mockProducerSnapshotIndex = TestUtils.tempFile();
        File tempDir = TestUtils.tempDirectory();
        // create 2 log segments, with 0 and 150 as log start offset
        LogSegment oldSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(oldSegment.baseOffset()).thenReturn(oldSegmentStartOffset);
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);
        verify(oldSegment, times(0)).readNextOffset();
        verify(activeSegment, times(0)).readNextOffset();

        FileRecords fileRecords = mock(FileRecords.class);
        when(oldSegment.log()).thenReturn(fileRecords);
        when(fileRecords.file()).thenReturn(tempFile);
        when(fileRecords.sizeInBytes()).thenReturn(10);
        when(oldSegment.readNextOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(CollectionConverters.asScala(Arrays.asList(oldSegment, activeSegment)));

        ProducerStateManager mockStateManager = mock(ProducerStateManager.class);
        when(mockLog.producerStateManager()).thenReturn(mockStateManager);
        when(mockStateManager.fetchSnapshot(anyLong())).thenReturn(Optional.of(mockProducerSnapshotIndex));
        when(mockLog.lastStableOffset()).thenReturn(lastStableOffset);
        when(mockLog.logEndOffset()).thenReturn(logEndOffset);

        OffsetIndex idx = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1000).get();
        TimeIndex timeIdx = LazyIndex.forTime(LogFileUtils.timeIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1500).get();
        File txnFile = UnifiedLog.transactionIndexFile(tempDir, oldSegmentStartOffset, "");
        txnFile.createNewFile();
        TransactionIndex txnIndex = new TransactionIndex(oldSegmentStartOffset, txnFile);
        when(oldSegment.timeIndex()).thenReturn(timeIdx);
        when(oldSegment.offsetIndex()).thenReturn(idx);
        when(oldSegment.txnIndex()).thenReturn(txnIndex);

        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class))).thenReturn(dummyFuture);
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class))).thenReturn(dummyFuture);
        when(remoteStorageManager.copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class)))
                .thenReturn(Optional.empty());
        when(rlmCopyQuotaManager.getThrottleTimeMs()).thenReturn(quotaAvailableThrottleTime);

        // Verify the metrics for remote writes and for failures is zero before attempt to copy log segment
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyBytesRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Verify aggregate metrics
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyBytesRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());

        RemoteLogManager.RLMCopyTask task = remoteLogManager.new RLMCopyTask(leaderTopicIdPartition, 128);
        task.copyLogSegmentsToRemote(mockLog);

        // verify remoteLogMetadataManager did add the expected RemoteLogSegmentMetadata
        ArgumentCaptor<RemoteLogSegmentMetadata> remoteLogSegmentMetadataArg = ArgumentCaptor.forClass(RemoteLogSegmentMetadata.class);
        verify(remoteLogMetadataManager).addRemoteLogSegmentMetadata(remoteLogSegmentMetadataArg.capture());
        // The old segment should only contain leader epoch [0->0, 1->100] since its offset range is [0, 149]
        Map<Integer, Long> expectedLeaderEpochs = new TreeMap<>();
        expectedLeaderEpochs.put(epochEntry0.epoch, epochEntry0.startOffset);
        expectedLeaderEpochs.put(epochEntry1.epoch, epochEntry1.startOffset);
        verifyRemoteLogSegmentMetadata(remoteLogSegmentMetadataArg.getValue(), oldSegmentStartOffset, oldSegmentEndOffset, expectedLeaderEpochs);

        // verify copyLogSegmentData is passing the RemoteLogSegmentMetadata we created above
        // and verify the logSegmentData passed is expected
        ArgumentCaptor<RemoteLogSegmentMetadata> remoteLogSegmentMetadataArg2 = ArgumentCaptor.forClass(RemoteLogSegmentMetadata.class);
        ArgumentCaptor<LogSegmentData> logSegmentDataArg = ArgumentCaptor.forClass(LogSegmentData.class);
        verify(remoteStorageManager, times(1)).copyLogSegmentData(remoteLogSegmentMetadataArg2.capture(), logSegmentDataArg.capture());
        assertEquals(remoteLogSegmentMetadataArg.getValue(), remoteLogSegmentMetadataArg2.getValue());
        // The old segment should only contain leader epoch [0->0, 1->100] since its offset range is [0, 149]
        verifyLogSegmentData(logSegmentDataArg.getValue(), idx, timeIdx, txnIndex, tempFile, mockProducerSnapshotIndex,
            Arrays.asList(epochEntry0, epochEntry1));

        // verify remoteLogMetadataManager did add the expected RemoteLogSegmentMetadataUpdate
        ArgumentCaptor<RemoteLogSegmentMetadataUpdate> remoteLogSegmentMetadataUpdateArg = ArgumentCaptor.forClass(RemoteLogSegmentMetadataUpdate.class);
        verify(remoteLogMetadataManager, times(1)).updateRemoteLogSegmentMetadata(remoteLogSegmentMetadataUpdateArg.capture());
        verifyRemoteLogSegmentMetadataUpdate(remoteLogSegmentMetadataUpdateArg.getValue());

        // verify the highest remote offset is updated to the expected value
        ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
        verify(mockLog, times(2)).updateHighestOffsetInRemoteStorage(argument.capture());
        assertEquals(oldSegmentEndOffset, argument.getValue());

        // Verify the metric for remote writes is updated correctly
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(10, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyBytesRate().count());
        // Verify we did not report any failure for remote writes
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Verify aggregate metrics
        assertEquals(1, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(10, brokerTopicStats.allTopicsStats().remoteCopyBytesRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());
    }

    // We are verifying that if the size of a piece of custom metadata is bigger than the configured limit,
    // the copy task should be cancelled and there should be an attempt to delete the just copied segment.
    @Test
    void testCustomMetadataSizeExceedsLimit() throws Exception {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;
        long lastStableOffset = 150L;
        long logEndOffset = 150L;

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt())).thenReturn(Optional.of(-1L));

        File tempFile = TestUtils.tempFile();
        File mockProducerSnapshotIndex = TestUtils.tempFile();
        File tempDir = TestUtils.tempDirectory();
        // create 2 log segments, with 0 and 150 as log start offset
        LogSegment oldSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(oldSegment.baseOffset()).thenReturn(oldSegmentStartOffset);
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);
        verify(oldSegment, times(0)).readNextOffset();
        verify(activeSegment, times(0)).readNextOffset();

        FileRecords fileRecords = mock(FileRecords.class);
        when(oldSegment.log()).thenReturn(fileRecords);
        when(fileRecords.file()).thenReturn(tempFile);
        when(fileRecords.sizeInBytes()).thenReturn(10);
        when(oldSegment.readNextOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(CollectionConverters.asScala(Arrays.asList(oldSegment, activeSegment)));

        ProducerStateManager mockStateManager = mock(ProducerStateManager.class);
        when(mockLog.producerStateManager()).thenReturn(mockStateManager);
        when(mockStateManager.fetchSnapshot(anyLong())).thenReturn(Optional.of(mockProducerSnapshotIndex));
        when(mockLog.lastStableOffset()).thenReturn(lastStableOffset);
        when(mockLog.logEndOffset()).thenReturn(logEndOffset);

        OffsetIndex idx = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1000).get();
        TimeIndex timeIdx = LazyIndex.forTime(LogFileUtils.timeIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1500).get();
        File txnFile = UnifiedLog.transactionIndexFile(tempDir, oldSegmentStartOffset, "");
        txnFile.createNewFile();
        TransactionIndex txnIndex = new TransactionIndex(oldSegmentStartOffset, txnFile);
        when(oldSegment.timeIndex()).thenReturn(timeIdx);
        when(oldSegment.offsetIndex()).thenReturn(idx);
        when(oldSegment.txnIndex()).thenReturn(txnIndex);

        int customMetadataSizeLimit = 128;
        CustomMetadata customMetadata = new CustomMetadata(new byte[customMetadataSizeLimit * 2]);

        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class))).thenReturn(dummyFuture);
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class))).thenReturn(dummyFuture);
        when(remoteStorageManager.copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class)))
                .thenReturn(Optional.of(customMetadata));
        when(rlmCopyQuotaManager.getThrottleTimeMs()).thenReturn(quotaAvailableThrottleTime);

        RemoteLogManager.RLMCopyTask task = remoteLogManager.new RLMCopyTask(leaderTopicIdPartition, customMetadataSizeLimit);
        task.copyLogSegmentsToRemote(mockLog);

        ArgumentCaptor<RemoteLogSegmentMetadata> remoteLogSegmentMetadataArg = ArgumentCaptor.forClass(RemoteLogSegmentMetadata.class);
        verify(remoteLogMetadataManager).addRemoteLogSegmentMetadata(remoteLogSegmentMetadataArg.capture());

        // Check we attempt to delete the segment data providing the custom metadata back.
        RemoteLogSegmentMetadataUpdate expectedMetadataUpdate = new RemoteLogSegmentMetadataUpdate(
                remoteLogSegmentMetadataArg.getValue().remoteLogSegmentId(), time.milliseconds(),
                Optional.of(customMetadata), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId);
        RemoteLogSegmentMetadata expectedDeleteMetadata = remoteLogSegmentMetadataArg.getValue().createWithUpdates(expectedMetadataUpdate);
        verify(remoteStorageManager, times(1)).deleteLogSegmentData(eq(expectedDeleteMetadata));

        // Check the task is cancelled in the end.
        assertTrue(task.isCancelled());

        // The metadata update should be posted.
        verify(remoteLogMetadataManager, times(2)).updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class));

        // Verify the metrics
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyBytesRate().count());
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Verify aggregate metrics
        assertEquals(1, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyBytesRate().count());
        assertEquals(1, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());
    }

    @Test
    void testFailedCopyShouldDeleteTheDanglingSegment() throws Exception {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;
        long lastStableOffset = 150L;
        long logEndOffset = 150L;

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt())).thenReturn(Optional.of(-1L));

        File tempFile = TestUtils.tempFile();
        File mockProducerSnapshotIndex = TestUtils.tempFile();
        File tempDir = TestUtils.tempDirectory();
        // create 2 log segments, with 0 and 150 as log start offset
        LogSegment oldSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(oldSegment.baseOffset()).thenReturn(oldSegmentStartOffset);
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);
        verify(oldSegment, times(0)).readNextOffset();
        verify(activeSegment, times(0)).readNextOffset();

        FileRecords fileRecords = mock(FileRecords.class);
        when(oldSegment.log()).thenReturn(fileRecords);
        when(fileRecords.file()).thenReturn(tempFile);
        when(fileRecords.sizeInBytes()).thenReturn(10);
        when(oldSegment.readNextOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(CollectionConverters.asScala(Arrays.asList(oldSegment, activeSegment)));

        ProducerStateManager mockStateManager = mock(ProducerStateManager.class);
        when(mockLog.producerStateManager()).thenReturn(mockStateManager);
        when(mockStateManager.fetchSnapshot(anyLong())).thenReturn(Optional.of(mockProducerSnapshotIndex));
        when(mockLog.lastStableOffset()).thenReturn(lastStableOffset);
        when(mockLog.logEndOffset()).thenReturn(logEndOffset);

        OffsetIndex idx = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1000).get();
        TimeIndex timeIdx = LazyIndex.forTime(LogFileUtils.timeIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1500).get();
        File txnFile = UnifiedLog.transactionIndexFile(tempDir, oldSegmentStartOffset, "");
        txnFile.createNewFile();
        TransactionIndex txnIndex = new TransactionIndex(oldSegmentStartOffset, txnFile);
        when(oldSegment.timeIndex()).thenReturn(timeIdx);
        when(oldSegment.offsetIndex()).thenReturn(idx);
        when(oldSegment.txnIndex()).thenReturn(txnIndex);

        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class))).thenReturn(dummyFuture);
        when(rlmCopyQuotaManager.getThrottleTimeMs()).thenReturn(quotaAvailableThrottleTime);
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class))).thenReturn(dummyFuture);

        // throw exception when copyLogSegmentData
        when(remoteStorageManager.copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class)))
                .thenThrow(new RemoteStorageException("test"));
        RemoteLogManager.RLMCopyTask task = remoteLogManager.new RLMCopyTask(leaderTopicIdPartition, 128);
        task.copyLogSegmentsToRemote(mockLog);

        ArgumentCaptor<RemoteLogSegmentMetadata> remoteLogSegmentMetadataArg = ArgumentCaptor.forClass(RemoteLogSegmentMetadata.class);
        verify(remoteLogMetadataManager).addRemoteLogSegmentMetadata(remoteLogSegmentMetadataArg.capture());
        // verify the segment is deleted
        verify(remoteStorageManager, times(1)).deleteLogSegmentData(eq(remoteLogSegmentMetadataArg.getValue()));

        // verify deletion state update
        verify(remoteLogMetadataManager, times(2)).updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class));

        // Verify the metrics
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyBytesRate().count());
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Verify aggregate metrics
        assertEquals(1, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyBytesRate().count());
        assertEquals(1, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());
    }

    @Test
    void testLeadershipChangesWithoutRemoteLogManagerConfiguring() {
        assertThrows(KafkaException.class, () -> {
            remoteLogManager.onLeadershipChange(
                Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);
        }, "RemoteLogManager is not configured when remote storage system is enabled");
    }

    @Test
    void testRemoteLogManagerTasksAvgIdlePercentAndMetadataCountMetrics() throws Exception {
        remoteLogManager.startup();
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;
        int segmentCount = 3;
        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());
        when(mockLog.parentDir()).thenReturn("dir1");

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt())).thenReturn(Optional.of(0L));

        File tempFile = TestUtils.tempFile();
        File mockProducerSnapshotIndex = TestUtils.tempFile();
        File tempDir = TestUtils.tempDirectory();
        // create 2 log segments, with 0 and 150 as log start offset
        LogSegment oldSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(oldSegment.baseOffset()).thenReturn(oldSegmentStartOffset);
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);

        FileRecords fileRecords = mock(FileRecords.class);
        when(oldSegment.log()).thenReturn(fileRecords);
        when(fileRecords.file()).thenReturn(tempFile);
        when(fileRecords.sizeInBytes()).thenReturn(10);
        when(oldSegment.readNextOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(CollectionConverters.asScala(Arrays.asList(oldSegment, activeSegment)));

        ProducerStateManager mockStateManager = mock(ProducerStateManager.class);
        when(mockLog.producerStateManager()).thenReturn(mockStateManager);
        when(mockStateManager.fetchSnapshot(anyLong())).thenReturn(Optional.of(mockProducerSnapshotIndex));
        when(mockLog.lastStableOffset()).thenReturn(250L);
        when(mockLog.logEndOffset()).thenReturn(500L);
        Map<String, Long> logProps = new HashMap<>();
        logProps.put("retention.bytes", 100L);
        logProps.put("retention.ms", -1L);
        LogConfig logConfig = new LogConfig(logProps);
        when(mockLog.config()).thenReturn(logConfig);

        OffsetIndex idx = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1000).get();
        TimeIndex timeIdx = LazyIndex.forTime(LogFileUtils.timeIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1500).get();
        File txnFile = UnifiedLog.transactionIndexFile(tempDir, oldSegmentStartOffset, "");
        txnFile.createNewFile();
        TransactionIndex txnIndex = new TransactionIndex(oldSegmentStartOffset, txnFile);
        when(oldSegment.timeIndex()).thenReturn(timeIdx);
        when(oldSegment.offsetIndex()).thenReturn(idx);
        when(oldSegment.txnIndex()).thenReturn(txnIndex);

        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class))).thenReturn(dummyFuture);
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class))).thenReturn(dummyFuture);

        CountDownLatch copyLogSegmentLatch = new CountDownLatch(1);
        doAnswer(ans -> {
            // waiting for verification
            copyLogSegmentLatch.await(5000, TimeUnit.MILLISECONDS);
            return Optional.empty();
        }).when(remoteStorageManager).copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class));

        CountDownLatch remoteLogMetadataCountLatch = new CountDownLatch(1);
        doAnswer(ans -> {
            remoteLogMetadataCountLatch.await(5000, TimeUnit.MILLISECONDS);
            return null;
        }).when(remoteStorageManager).deleteLogSegmentData(any(RemoteLogSegmentMetadata.class));

        Partition mockLeaderPartition = mockPartition(leaderTopicIdPartition);
        Partition mockFollowerPartition = mockPartition(followerTopicIdPartition);
        List<RemoteLogSegmentMetadata> list = listRemoteLogSegmentMetadata(leaderTopicIdPartition, segmentCount, 100, 1024, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        // return the metadataList 3 times, then return empty list to simulate all segments are deleted
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition)).thenReturn(list.iterator()).thenReturn(Collections.emptyIterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 0)).thenReturn(list.iterator()).thenReturn(list.iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 1)).thenReturn(list.iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 2)).thenReturn(list.iterator());

        // before running tasks, the remote log manager tasks should be all idle and the remote log metadata count should be 0
        assertEquals(1.0, (double) yammerMetricValue("RemoteLogManagerTasksAvgIdlePercent"));
        assertEquals(0, safeLongYammerMetricValue("RemoteLogMetadataCount,topic=" + leaderTopic));
        assertEquals(0, safeLongYammerMetricValue("RemoteLogSizeBytes,topic=" + leaderTopic));
        assertEquals(0, safeLongYammerMetricValue("RemoteLogMetadataCount"));
        assertEquals(0, safeLongYammerMetricValue("RemoteLogSizeBytes"));
        remoteLogManager.onLeadershipChange(Collections.singleton(mockLeaderPartition), Collections.singleton(mockFollowerPartition), topicIds);
        assertTrue((double) yammerMetricValue("RemoteLogManagerTasksAvgIdlePercent") < 1.0);

        copyLogSegmentLatch.countDown();

        // Now, the `RemoteLogMetadataCount` should set to the expected value
        TestUtils.waitForCondition(() -> safeLongYammerMetricValue("RemoteLogMetadataCount,topic=" + leaderTopic) == segmentCount &&
                        safeLongYammerMetricValue("RemoteLogMetadataCount") == segmentCount,
                "Didn't show the expected RemoteLogMetadataCount metric value.");

        TestUtils.waitForCondition(
                () -> 3072 == safeLongYammerMetricValue("RemoteLogSizeBytes,topic=" + leaderTopic) &&
                        3072 == safeLongYammerMetricValue("RemoteLogSizeBytes"),
                String.format("Expected to find 3072 for RemoteLogSizeBytes metric value, but found %d for 'Leader' topic and %d for all topic",
                        safeLongYammerMetricValue("RemoteLogSizeBytes,topic=" + leaderTopic),
                        safeLongYammerMetricValue("RemoteLogSizeBytes")));

        remoteLogMetadataCountLatch.countDown();

        TestUtils.waitForCondition(() -> safeLongYammerMetricValue("RemoteLogMetadataCount,topic=" + leaderTopic) == 0 &&
                        safeLongYammerMetricValue("RemoteLogMetadataCount") == 0,
                "Didn't reset to 0 for RemoteLogMetadataCount metric value when no remote log metadata.");

        TestUtils.waitForCondition(
                () -> 0 == safeLongYammerMetricValue("RemoteLogSizeBytes,topic=" + leaderTopic) &&
                        0 == safeLongYammerMetricValue("RemoteLogSizeBytes"),
                String.format("Didn't reset to 0 for RemoteLogSizeBytes metric value when no remote log metadata - found %d for 'Leader' topic and %d for all topic.",
                        safeLongYammerMetricValue("RemoteLogSizeBytes,topic=" + leaderTopic),
                        safeLongYammerMetricValue("RemoteLogSizeBytes")));
    }

    @Test
    void testRemoteLogTaskUpdateRemoteLogSegmentMetadataAfterLogDirChanged() throws Exception {
        remoteLogManager.startup();
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;
        int segmentCount = 3;
        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());
        when(mockLog.parentDir()).thenReturn("dir1");

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt()))
                .thenReturn(Optional.of(0L))
                .thenReturn(Optional.of(nextSegmentStartOffset - 1));

        File tempFile = TestUtils.tempFile();
        File mockProducerSnapshotIndex = TestUtils.tempFile();
        File tempDir = TestUtils.tempDirectory();
        // create 2 log segments, with 0 and 150 as log start offset
        LogSegment oldSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(oldSegment.baseOffset()).thenReturn(oldSegmentStartOffset);
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);

        FileRecords fileRecords = mock(FileRecords.class);
        when(oldSegment.log()).thenReturn(fileRecords);
        when(fileRecords.file()).thenReturn(tempFile);
        when(fileRecords.sizeInBytes()).thenReturn(10);
        when(oldSegment.readNextOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(CollectionConverters.asScala(Arrays.asList(oldSegment, activeSegment)));

        ProducerStateManager mockStateManager = mock(ProducerStateManager.class);
        when(mockLog.producerStateManager()).thenReturn(mockStateManager);
        when(mockStateManager.fetchSnapshot(anyLong())).thenReturn(Optional.of(mockProducerSnapshotIndex));
        when(mockLog.lastStableOffset()).thenReturn(250L);
        when(mockLog.logEndOffset()).thenReturn(500L);
        Map<String, Long> logProps = new HashMap<>();
        logProps.put("retention.bytes", 100L);
        logProps.put("retention.ms", -1L);
        LogConfig logConfig = new LogConfig(logProps);
        when(mockLog.config()).thenReturn(logConfig);

        OffsetIndex idx = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1000).get();
        TimeIndex timeIdx = LazyIndex.forTime(LogFileUtils.timeIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1500).get();
        File txnFile = UnifiedLog.transactionIndexFile(tempDir, oldSegmentStartOffset, "");
        txnFile.createNewFile();
        TransactionIndex txnIndex = new TransactionIndex(oldSegmentStartOffset, txnFile);
        when(oldSegment.timeIndex()).thenReturn(timeIdx);
        when(oldSegment.offsetIndex()).thenReturn(idx);
        when(oldSegment.txnIndex()).thenReturn(txnIndex);

        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class))).thenReturn(dummyFuture);
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class))).thenReturn(dummyFuture);

        CountDownLatch copyLogSegmentLatch = new CountDownLatch(1);
        doAnswer(ans -> {
            // waiting for verification
            copyLogSegmentLatch.await(5000, TimeUnit.MILLISECONDS);
            return Optional.empty();
        }).when(remoteStorageManager).copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class));
        when(rlmCopyQuotaManager.getThrottleTimeMs()).thenReturn(quotaAvailableThrottleTime);

        Partition mockLeaderPartition = mockPartition(leaderTopicIdPartition);
        List<RemoteLogSegmentMetadata> metadataList = listRemoteLogSegmentMetadata(leaderTopicIdPartition, segmentCount, 100, 1024, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition)).thenReturn(metadataList.iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 0)).thenReturn(metadataList.iterator()).thenReturn(metadataList.iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 1)).thenReturn(metadataList.iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 2)).thenReturn(metadataList.iterator());

        // leadership change to log in dir1
        remoteLogManager.onLeadershipChange(Collections.singleton(mockLeaderPartition), Collections.emptySet(), topicIds);

        TestUtils.waitForCondition(() -> {
            ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
            verify(mockLog, times(1)).updateHighestOffsetInRemoteStorage(argument.capture());
            return 0L == argument.getValue();
        }, "Timed out waiting for updateHighestOffsetInRemoteStorage(0) get invoked for dir1 log");

        UnifiedLog oldMockLog = mockLog;
        Mockito.clearInvocations(oldMockLog);
        // simulate altering log dir completes, and the new partition leader changes to the same broker in different log dir (dir2)
        mockLog = mock(UnifiedLog.class);
        when(mockLog.parentDir()).thenReturn("dir2");
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(mockLog.config()).thenReturn(logConfig);
        when(mockLog.logEndOffset()).thenReturn(500L);

        remoteLogManager.onLeadershipChange(Collections.singleton(mockLeaderPartition), Collections.emptySet(), topicIds);

        // after copyLogSegment completes for log (in dir1), updateHighestOffsetInRemoteStorage will be triggered with new offset
        // even though the leader replica has changed to log in dir2
        copyLogSegmentLatch.countDown();
        TestUtils.waitForCondition(() -> {
            ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
            verify(oldMockLog, times(1)).updateHighestOffsetInRemoteStorage(argument.capture());
            return nextSegmentStartOffset - 1 == argument.getValue();
        }, "Timed out waiting for updateHighestOffsetInRemoteStorage(149) get invoked for dir1 log");

        // On the next run of RLMTask, the log in dir2 will be picked and start by updateHighestOffsetInRemoteStorage to the expected offset
        TestUtils.waitForCondition(() -> {
            ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
            verify(mockLog, times(1)).updateHighestOffsetInRemoteStorage(argument.capture());
            return nextSegmentStartOffset - 1 == argument.getValue();
        }, "Timed out waiting for updateHighestOffsetInRemoteStorage(149) get invoked for dir2 log");

    }

    @Test
    void testRemoteLogManagerRemoteMetrics() throws Exception {
        remoteLogManager.startup();
        long oldestSegmentStartOffset = 0L;
        long olderSegmentStartOffset = 75L;
        long nextSegmentStartOffset = 150L;
        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());
        when(mockLog.parentDir()).thenReturn("dir1");

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt())).thenReturn(Optional.of(0L));

        File tempFile = TestUtils.tempFile();
        File mockProducerSnapshotIndex = TestUtils.tempFile();
        File tempDir = TestUtils.tempDirectory();
        // create 3 log segments, with 0, 75 and 150 as log start offset
        LogSegment oldestSegment = mock(LogSegment.class);
        LogSegment olderSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(oldestSegment.baseOffset()).thenReturn(oldestSegmentStartOffset);
        when(olderSegment.baseOffset()).thenReturn(olderSegmentStartOffset);
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);

        FileRecords oldestFileRecords = mock(FileRecords.class);
        when(oldestSegment.log()).thenReturn(oldestFileRecords);
        when(oldestFileRecords.file()).thenReturn(tempFile);
        when(oldestFileRecords.sizeInBytes()).thenReturn(10);
        when(oldestSegment.readNextOffset()).thenReturn(olderSegmentStartOffset);

        FileRecords olderFileRecords = mock(FileRecords.class);
        when(olderSegment.log()).thenReturn(olderFileRecords);
        when(olderFileRecords.file()).thenReturn(tempFile);
        when(olderFileRecords.sizeInBytes()).thenReturn(10);
        when(olderSegment.readNextOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldestSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(CollectionConverters.asScala(Arrays.asList(oldestSegment, olderSegment, activeSegment)));

        ProducerStateManager mockStateManager = mock(ProducerStateManager.class);
        when(mockLog.producerStateManager()).thenReturn(mockStateManager);
        when(mockStateManager.fetchSnapshot(anyLong())).thenReturn(Optional.of(mockProducerSnapshotIndex));
        when(mockLog.lastStableOffset()).thenReturn(250L);
        Map<String, Long> logProps = new HashMap<>();
        logProps.put("retention.bytes", 1000000L);
        logProps.put("retention.ms", -1L);
        LogConfig logConfig = new LogConfig(logProps);
        when(mockLog.config()).thenReturn(logConfig);

        OffsetIndex oldestIdx = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(tempDir, oldestSegmentStartOffset, ""), oldestSegmentStartOffset, 1000).get();
        TimeIndex oldestTimeIdx = LazyIndex.forTime(LogFileUtils.timeIndexFile(tempDir, oldestSegmentStartOffset, ""), oldestSegmentStartOffset, 1500).get();
        File oldestTxnFile = UnifiedLog.transactionIndexFile(tempDir, oldestSegmentStartOffset, "");
        oldestTxnFile.createNewFile();
        TransactionIndex oldestTxnIndex = new TransactionIndex(oldestSegmentStartOffset, oldestTxnFile);
        when(oldestSegment.timeIndex()).thenReturn(oldestTimeIdx);
        when(oldestSegment.offsetIndex()).thenReturn(oldestIdx);
        when(oldestSegment.txnIndex()).thenReturn(oldestTxnIndex);

        OffsetIndex olderIdx = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(tempDir, olderSegmentStartOffset, ""), olderSegmentStartOffset, 1000).get();
        TimeIndex olderTimeIdx = LazyIndex.forTime(LogFileUtils.timeIndexFile(tempDir, olderSegmentStartOffset, ""), olderSegmentStartOffset, 1500).get();
        File olderTxnFile = UnifiedLog.transactionIndexFile(tempDir, olderSegmentStartOffset, "");
        oldestTxnFile.createNewFile();
        TransactionIndex olderTxnIndex = new TransactionIndex(olderSegmentStartOffset, olderTxnFile);
        when(olderSegment.timeIndex()).thenReturn(olderTimeIdx);
        when(olderSegment.offsetIndex()).thenReturn(olderIdx);
        when(olderSegment.txnIndex()).thenReturn(olderTxnIndex);

        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class))).thenReturn(dummyFuture);
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class))).thenReturn(dummyFuture);
        Iterator<RemoteLogSegmentMetadata> iterator = listRemoteLogSegmentMetadata(leaderTopicIdPartition, 5, 100, 1024, RemoteLogSegmentState.COPY_SEGMENT_FINISHED).iterator();
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition)).thenReturn(iterator);
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 2)).thenReturn(iterator);
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 1)).thenReturn(iterator);

        CountDownLatch remoteLogSizeComputationTimeLatch = new CountDownLatch(1);
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 0)).thenAnswer(ans -> {
            // advance the mock timer 1000ms to add value for RemoteLogSizeComputationTime metric
            time.sleep(1000);
            return iterator;
        }).thenAnswer(ans -> {
            // wait for verifying RemoteLogSizeComputationTime metric value.
            remoteLogSizeComputationTimeLatch.await(5000, TimeUnit.MILLISECONDS);
            return Collections.emptyIterator();
        });

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(ans -> Optional.empty()).doAnswer(ans -> {
            // waiting for verification
            latch.await(5000, TimeUnit.MILLISECONDS);
            return Optional.empty();
        }).when(remoteStorageManager).copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class));
        when(rlmCopyQuotaManager.getThrottleTimeMs()).thenReturn(quotaAvailableThrottleTime);

        Partition mockLeaderPartition = mockPartition(leaderTopicIdPartition);

        // This method is called by both Copy and Expiration task. On the first call, both tasks should see 175 bytes as
        // the local log segments size
        when(mockLog.onlyLocalLogSegmentsSize()).thenReturn(175L, 175L, 100L);
        when(activeSegment.size()).thenReturn(100);
        when(mockLog.onlyLocalLogSegmentsCount()).thenReturn(2L).thenReturn(1L);

        // before running tasks, the metric should not be registered
        assertThrows(NoSuchElementException.class, () -> yammerMetricValue("RemoteCopyLagBytes,topic=" + leaderTopic));
        assertThrows(NoSuchElementException.class, () -> yammerMetricValue("RemoteCopyLagSegments,topic=" + leaderTopic));
        assertThrows(NoSuchElementException.class, () -> yammerMetricValue("RemoteLogSizeComputationTime,topic=" + leaderTopic));
        // all topic metrics should be 0
        assertEquals(0L, yammerMetricValue("RemoteCopyLagBytes"));
        assertEquals(0L, yammerMetricValue("RemoteCopyLagSegments"));
        assertEquals(0L, yammerMetricValue("RemoteLogSizeComputationTime"));

        remoteLogManager.onLeadershipChange(Collections.singleton(mockLeaderPartition), Collections.emptySet(), topicIds);
        TestUtils.waitForCondition(
                () -> 75 == safeLongYammerMetricValue("RemoteCopyLagBytes") && 75 == safeLongYammerMetricValue("RemoteCopyLagBytes,topic=" + leaderTopic),
                String.format("Expected to find 75 for RemoteCopyLagBytes metric value, but found %d for topic 'Leader' and %d for all topics.",
                        safeLongYammerMetricValue("RemoteCopyLagBytes,topic=" + leaderTopic),
                        safeLongYammerMetricValue("RemoteCopyLagBytes")));
        TestUtils.waitForCondition(
                () -> 1 == safeLongYammerMetricValue("RemoteCopyLagSegments") && 1 == safeLongYammerMetricValue("RemoteCopyLagSegments,topic=" + leaderTopic),
                String.format("Expected to find 1 for RemoteCopyLagSegments metric value, but found %d for topic 'Leader' and %d for all topics.",
                        safeLongYammerMetricValue("RemoteCopyLagSegments,topic=" + leaderTopic),
                        safeLongYammerMetricValue("RemoteCopyLagSegments")));
        // unlock copyLogSegmentData
        latch.countDown();

        TestUtils.waitForCondition(
                () -> safeLongYammerMetricValue("RemoteLogSizeComputationTime") >= 1000 && safeLongYammerMetricValue("RemoteLogSizeComputationTime,topic=" + leaderTopic) >= 1000,
                String.format("Expected to find 1000 for RemoteLogSizeComputationTime metric value, but found %d for topic 'Leader' and %d for all topics.",
                        safeLongYammerMetricValue("RemoteLogSizeComputationTime,topic=" + leaderTopic),
                        safeLongYammerMetricValue("RemoteLogSizeComputationTime")));
        remoteLogSizeComputationTimeLatch.countDown();
        
        TestUtils.waitForCondition(
                () -> 0 == safeLongYammerMetricValue("RemoteCopyLagBytes") && 0 == safeLongYammerMetricValue("RemoteCopyLagBytes,topic=" + leaderTopic),
                String.format("Expected to find 0 for RemoteCopyLagBytes metric value, but found %d for topic 'Leader' and %d for all topics.",
                        safeLongYammerMetricValue("RemoteCopyLagBytes,topic=" + leaderTopic),
                        safeLongYammerMetricValue("RemoteCopyLagBytes")));
        TestUtils.waitForCondition(
                () -> 0 == safeLongYammerMetricValue("RemoteCopyLagSegments") && 0 == safeLongYammerMetricValue("RemoteCopyLagSegments,topic=" + leaderTopic),
                String.format("Expected to find 0 for RemoteCopyLagSegments metric value, but found %d for topic 'Leader' and %d for all topics.",
                        safeLongYammerMetricValue("RemoteCopyLagSegments,topic=" + leaderTopic),
                        safeLongYammerMetricValue("RemoteCopyLagSegments")));
    }

    private Object yammerMetricValue(String name) {
        Gauge gauge = (Gauge) KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
                .filter(e -> e.getKey().getMBeanName().endsWith(name))
                .findFirst()
                .get()
                .getValue();
        return gauge.value();
    }

    private long safeLongYammerMetricValue(String name) {
        try {
            return (long) yammerMetricValue(name);
        } catch (NoSuchElementException ex) {
            return 0L;
        }
    }

    @Test
    void testMetricsUpdateOnCopyLogSegmentsFailure() throws Exception {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt())).thenReturn(Optional.of(0L));

        File tempFile = TestUtils.tempFile();
        File mockProducerSnapshotIndex = TestUtils.tempFile();
        File tempDir = TestUtils.tempDirectory();
        // create 2 log segments, with 0 and 150 as log start offset
        LogSegment oldSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(oldSegment.baseOffset()).thenReturn(oldSegmentStartOffset);
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);

        FileRecords fileRecords = mock(FileRecords.class);
        when(oldSegment.log()).thenReturn(fileRecords);
        when(fileRecords.file()).thenReturn(tempFile);
        when(fileRecords.sizeInBytes()).thenReturn(10);
        when(oldSegment.readNextOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(CollectionConverters.asScala(Arrays.asList(oldSegment, activeSegment)));

        ProducerStateManager mockStateManager = mock(ProducerStateManager.class);
        when(mockLog.producerStateManager()).thenReturn(mockStateManager);
        when(mockStateManager.fetchSnapshot(anyLong())).thenReturn(Optional.of(mockProducerSnapshotIndex));
        when(mockLog.lastStableOffset()).thenReturn(250L);

        OffsetIndex idx = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1000).get();
        TimeIndex timeIdx = LazyIndex.forTime(LogFileUtils.timeIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1500).get();
        File txnFile = UnifiedLog.transactionIndexFile(tempDir, oldSegmentStartOffset, "");
        txnFile.createNewFile();
        TransactionIndex txnIndex = new TransactionIndex(oldSegmentStartOffset, txnFile);
        when(oldSegment.timeIndex()).thenReturn(timeIdx);
        when(oldSegment.offsetIndex()).thenReturn(idx);
        when(oldSegment.txnIndex()).thenReturn(txnIndex);

        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class))).thenReturn(dummyFuture);
        doThrow(new RuntimeException()).when(remoteStorageManager).copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class));
        when(rlmCopyQuotaManager.getThrottleTimeMs()).thenReturn(quotaAvailableThrottleTime);

        // Verify the metrics for remote write requests/failures is zero before attempt to copy log segment
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Verify aggregate metrics
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());
        RemoteLogManager.RLMCopyTask task = remoteLogManager.new RLMCopyTask(leaderTopicIdPartition, 128);
        task.copyLogSegmentsToRemote(mockLog);

        // Verify we attempted to copy log segment metadata to remote storage
        verify(remoteStorageManager, times(1)).copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class));

        // Verify we should not have updated the highest offset because of write failure
        verify(mockLog).updateHighestOffsetInRemoteStorage(anyLong());
        // Verify the metric for remote write requests/failures was updated.
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Verify aggregate metrics
        assertEquals(1, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(1, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());
    }

    @Test
    void testRLMTaskDoesNotUploadSegmentsWhenRemoteLogMetadataManagerIsNotInitialized() throws Exception {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        // Throw a retryable exception so indicate that the remote log metadata manager is not initialized yet
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt()))
            .thenThrow(new ReplicaNotAvailableException("Remote log metadata cache is not initialized for partition: " + leaderTopicIdPartition));

        // create 2 log segments, with 0 and 150 as log start offset
        LogSegment oldSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(oldSegment.baseOffset()).thenReturn(oldSegmentStartOffset);
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(CollectionConverters.asScala(Arrays.asList(oldSegment, activeSegment)));
        when(mockLog.lastStableOffset()).thenReturn(250L);

        // Ensure the metrics for remote write requests/failures is zero before attempt to copy log segment
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Ensure aggregate metrics
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());

        RemoteLogManager.RLMCopyTask task = remoteLogManager.new RLMCopyTask(leaderTopicIdPartition, 128);
        task.run();

        // verify the remoteLogMetadataManager never add any metadata and remoteStorageManager never copy log segments
        verify(remoteLogMetadataManager, never()).addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class));
        verify(remoteStorageManager, never()).copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class));
        verify(remoteLogMetadataManager, never()).updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class));
        verify(mockLog, never()).updateHighestOffsetInRemoteStorage(anyLong());

        // Verify the metric for remote write requests/failures was not updated.
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Verify aggregate metrics
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());
    }

    private void verifyRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                                long oldSegmentStartOffset,
                                                long oldSegmentEndOffset,
                                                Map<Integer, Long> expectedLeaderEpochs) {
        assertEquals(leaderTopicIdPartition, remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition());
        assertEquals(oldSegmentStartOffset, remoteLogSegmentMetadata.startOffset());
        assertEquals(oldSegmentEndOffset, remoteLogSegmentMetadata.endOffset());

        NavigableMap<Integer, Long> leaderEpochs = remoteLogSegmentMetadata.segmentLeaderEpochs();
        assertEquals(expectedLeaderEpochs.size(), leaderEpochs.size());
        Iterator<Map.Entry<Integer, Long>> leaderEpochEntries = expectedLeaderEpochs.entrySet().iterator();
        assertEquals(leaderEpochEntries.next(), leaderEpochs.firstEntry());
        assertEquals(leaderEpochEntries.next(), leaderEpochs.lastEntry());

        assertEquals(brokerId, remoteLogSegmentMetadata.brokerId());
        assertEquals(RemoteLogSegmentState.COPY_SEGMENT_STARTED, remoteLogSegmentMetadata.state());
    }

    private void verifyRemoteLogSegmentMetadataUpdate(RemoteLogSegmentMetadataUpdate remoteLogSegmentMetadataUpdate) {
        assertEquals(leaderTopicIdPartition, remoteLogSegmentMetadataUpdate.remoteLogSegmentId().topicIdPartition());
        assertEquals(brokerId, remoteLogSegmentMetadataUpdate.brokerId());

        assertEquals(RemoteLogSegmentState.COPY_SEGMENT_FINISHED, remoteLogSegmentMetadataUpdate.state());
    }

    private void verifyLogSegmentData(LogSegmentData logSegmentData,
                                      OffsetIndex idx,
                                      TimeIndex timeIdx,
                                      TransactionIndex txnIndex,
                                      File tempFile,
                                      File mockProducerSnapshotIndex,
                                      List<EpochEntry> expectedLeaderEpoch) throws IOException {
        assertEquals(idx.file().getAbsolutePath(), logSegmentData.offsetIndex().toAbsolutePath().toString());
        assertEquals(timeIdx.file().getAbsolutePath(), logSegmentData.timeIndex().toAbsolutePath().toString());
        assertEquals(txnIndex.file().getPath(), logSegmentData.transactionIndex().get().toAbsolutePath().toString());
        assertEquals(tempFile.getAbsolutePath(), logSegmentData.logSegment().toAbsolutePath().toString());
        assertEquals(mockProducerSnapshotIndex.getAbsolutePath(), logSegmentData.producerSnapshotIndex().toAbsolutePath().toString());

        assertEquals(RemoteLogManager.epochEntriesAsByteBuffer(expectedLeaderEpoch), logSegmentData.leaderEpochIndex());
    }

    @Test
    void testGetClassLoaderAwareRemoteStorageManager() throws Exception {
        ClassLoaderAwareRemoteStorageManager rsmManager = mock(ClassLoaderAwareRemoteStorageManager.class);
        try (RemoteLogManager remoteLogManager =
            new RemoteLogManager(config.remoteLogManagerConfig(), brokerId, logDir, clusterId, time,
                    t -> Optional.empty(),
                    (topicPartition, offset) -> { },
                    brokerTopicStats, metrics) {
                public RemoteStorageManager createRemoteStorageManager() {
                    return rsmManager;
                }
            }
        ) {
            assertEquals(rsmManager, remoteLogManager.storageManager());
        }
    }

    private void verifyInCache(TopicIdPartition... topicIdPartitions) {
        Arrays.stream(topicIdPartitions).forEach(topicIdPartition ->
            assertDoesNotThrow(() -> remoteLogManager.fetchRemoteLogSegmentMetadata(topicIdPartition.topicPartition(), 0, 0L))
        );
    }

    private void verifyNotInCache(TopicIdPartition... topicIdPartitions) {
        Arrays.stream(topicIdPartitions).forEach(topicIdPartition ->
            assertThrows(KafkaException.class, () ->
                remoteLogManager.fetchRemoteLogSegmentMetadata(topicIdPartition.topicPartition(), 0, 0L))
        );
    }

    @Test
    void testTopicIdCacheUpdates() throws RemoteStorageException {
        remoteLogManager.startup();
        Partition mockLeaderPartition = mockPartition(leaderTopicIdPartition);
        Partition mockFollowerPartition = mockPartition(followerTopicIdPartition);

        when(remoteLogMetadataManager.remoteLogSegmentMetadata(any(TopicIdPartition.class), anyInt(), anyLong()))
            .thenReturn(Optional.empty());
        verifyNotInCache(followerTopicIdPartition, leaderTopicIdPartition);
        // Load topicId cache
        remoteLogManager.onLeadershipChange(Collections.singleton(mockLeaderPartition), Collections.singleton(mockFollowerPartition), topicIds);
        verify(remoteLogMetadataManager, times(1))
            .onPartitionLeadershipChanges(Collections.singleton(leaderTopicIdPartition), Collections.singleton(followerTopicIdPartition));
        verifyInCache(followerTopicIdPartition, leaderTopicIdPartition);

        // Evicts from topicId cache
        remoteLogManager.stopPartitions(Collections.singleton(new StopPartition(leaderTopicIdPartition.topicPartition(), true, true, true)), (tp, ex) -> { });
        verifyNotInCache(leaderTopicIdPartition);
        verifyInCache(followerTopicIdPartition);

        // Evicts from topicId cache
        remoteLogManager.stopPartitions(Collections.singleton(new StopPartition(followerTopicIdPartition.topicPartition(), true, true, true)), (tp, ex) -> { });
        verifyNotInCache(leaderTopicIdPartition, followerTopicIdPartition);
    }

    @Test
    void testFetchRemoteLogSegmentMetadata() throws RemoteStorageException {
        remoteLogManager.startup();
        remoteLogManager.onLeadershipChange(
            Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);
        remoteLogManager.fetchRemoteLogSegmentMetadata(leaderTopicIdPartition.topicPartition(), 10, 100L);
        remoteLogManager.fetchRemoteLogSegmentMetadata(followerTopicIdPartition.topicPartition(), 20, 200L);

        verify(remoteLogMetadataManager)
            .remoteLogSegmentMetadata(eq(leaderTopicIdPartition), anyInt(), anyLong());
        verify(remoteLogMetadataManager)
            .remoteLogSegmentMetadata(eq(followerTopicIdPartition), anyInt(), anyLong());
    }

    @Test
    public void testFetchNextSegmentWithTxnIndex() throws RemoteStorageException {
        remoteLogManager.startup();
        remoteLogManager.onLeadershipChange(
            Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);
        remoteLogManager.fetchNextSegmentWithTxnIndex(leaderTopicIdPartition.topicPartition(), 10, 100L);
        remoteLogManager.fetchNextSegmentWithTxnIndex(followerTopicIdPartition.topicPartition(), 20, 200L);

        verify(remoteLogMetadataManager)
            .nextSegmentWithTxnIndex(eq(leaderTopicIdPartition), anyInt(), anyLong());
        verify(remoteLogMetadataManager)
                .nextSegmentWithTxnIndex(eq(followerTopicIdPartition), anyInt(), anyLong());
    }

    @Test
    public void testFindNextSegmentWithTxnIndex() throws RemoteStorageException {
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt()))
                .thenReturn(Optional.of(0L));
        when(remoteLogMetadataManager.nextSegmentWithTxnIndex(any(TopicIdPartition.class), anyInt(), anyLong()))
                .thenAnswer(ans -> {
                    TopicIdPartition topicIdPartition = ans.getArgument(0);
                    int leaderEpoch = ans.getArgument(1);
                    long offset = ans.getArgument(2);
                    RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
                    Map<Integer, Long> leaderEpochs = new TreeMap<>();
                    leaderEpochs.put(leaderEpoch, offset);
                    RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(segmentId,
                            offset, offset + 100, time.milliseconds(), 0, time.milliseconds(), 1024, leaderEpochs, true);
                    return Optional.of(metadata);
                });

        remoteLogManager.startup();
        remoteLogManager.onLeadershipChange(
                Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);

        // For offset-10, epoch is 0.
        remoteLogManager.findNextSegmentWithTxnIndex(leaderTopicIdPartition.topicPartition(), 10, cache);
        verify(remoteLogMetadataManager)
                .nextSegmentWithTxnIndex(eq(leaderTopicIdPartition), eq(0), eq(10L));
    }

    @Test
    public void testFindNextSegmentWithTxnIndexTraversesNextEpoch() throws RemoteStorageException {
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt()))
                .thenReturn(Optional.of(0L));
        when(remoteLogMetadataManager.nextSegmentWithTxnIndex(any(TopicIdPartition.class), anyInt(), anyLong()))
                .thenAnswer(ans -> {
                    TopicIdPartition topicIdPartition = ans.getArgument(0);
                    int leaderEpoch = ans.getArgument(1);
                    long offset = ans.getArgument(2);
                    Optional<RemoteLogSegmentMetadata> metadataOpt = Optional.empty();
                    if (leaderEpoch == 2) {
                        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
                        Map<Integer, Long> leaderEpochs = new TreeMap<>();
                        leaderEpochs.put(leaderEpoch, offset);
                        RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(segmentId,
                                offset, offset + 100, time.milliseconds(), 0, time.milliseconds(), 1024, leaderEpochs, true);
                        metadataOpt = Optional.of(metadata);
                    }
                    return metadataOpt;
                });

        remoteLogManager.startup();
        remoteLogManager.onLeadershipChange(
                Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);

        // For offset-10, epoch is 0.
        //  1. For epoch 0 and 1, it returns empty and
        //  2. For epoch 2, it returns the segment metadata.
        remoteLogManager.findNextSegmentWithTxnIndex(leaderTopicIdPartition.topicPartition(), 10, cache);
        verify(remoteLogMetadataManager)
            .nextSegmentWithTxnIndex(eq(leaderTopicIdPartition), eq(0), eq(10L));
        verify(remoteLogMetadataManager)
                .nextSegmentWithTxnIndex(eq(leaderTopicIdPartition), eq(1), eq(100L));
        verify(remoteLogMetadataManager)
                .nextSegmentWithTxnIndex(eq(leaderTopicIdPartition), eq(2), eq(200L));
    }

    @Test
    void testOnLeadershipChangeWillInvokeHandleLeaderOrFollowerPartitions() {
        remoteLogManager.startup();
        RemoteLogManager spyRemoteLogManager = spy(remoteLogManager);
        spyRemoteLogManager.onLeadershipChange(
            Collections.emptySet(), Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);
        verify(spyRemoteLogManager).doHandleFollowerPartition(eq(followerTopicIdPartition));

        Mockito.reset(spyRemoteLogManager);

        spyRemoteLogManager.onLeadershipChange(
            Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.emptySet(), topicIds);
        verify(spyRemoteLogManager).doHandleLeaderPartition(eq(leaderTopicIdPartition), eq(false));
    }

    private MemoryRecords records(long timestamp,
                                  long initialOffset,
                                  int partitionLeaderEpoch) {
        return MemoryRecords.withRecords(initialOffset, Compression.NONE, partitionLeaderEpoch,
            new SimpleRecord(timestamp - 1, "first message".getBytes()),
            new SimpleRecord(timestamp + 1, "second message".getBytes()),
            new SimpleRecord(timestamp + 2, "third message".getBytes())
            );
    }

    @Test
    void testFindOffsetByTimestamp() throws IOException, RemoteStorageException {
        remoteLogManager.startup();
        TopicPartition tp = leaderTopicIdPartition.topicPartition();

        long ts = time.milliseconds();
        long startOffset = 120;
        int targetLeaderEpoch = 10;

        TreeMap<Integer, Long> validSegmentEpochs = new TreeMap<>();
        validSegmentEpochs.put(targetLeaderEpoch, startOffset);

        LeaderEpochFileCache leaderEpochFileCache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        leaderEpochFileCache.assign(4, 99L);
        leaderEpochFileCache.assign(5, 99L);
        leaderEpochFileCache.assign(targetLeaderEpoch, startOffset);
        leaderEpochFileCache.assign(12, 500L);

        doTestFindOffsetByTimestamp(ts, startOffset, targetLeaderEpoch, validSegmentEpochs, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

        // Fetching message for timestamp `ts` will return the message with startOffset+1, and `ts+1` as there are no
        // messages starting with the startOffset and with `ts`.
        Optional<FileRecords.TimestampAndOffset> maybeTimestampAndOffset1 = remoteLogManager.findOffsetByTimestamp(tp, ts, startOffset, leaderEpochFileCache);
        assertEquals(Optional.of(new FileRecords.TimestampAndOffset(ts + 1, startOffset + 1, Optional.of(targetLeaderEpoch))), maybeTimestampAndOffset1);

        // Fetching message for `ts+2` will return the message with startOffset+2 and its timestamp value is `ts+2`.
        Optional<FileRecords.TimestampAndOffset> maybeTimestampAndOffset2 = remoteLogManager.findOffsetByTimestamp(tp, ts + 2, startOffset, leaderEpochFileCache);
        assertEquals(Optional.of(new FileRecords.TimestampAndOffset(ts + 2, startOffset + 2, Optional.of(targetLeaderEpoch))), maybeTimestampAndOffset2);

        // Fetching message for `ts+3` will return None as there are no records with timestamp >= ts+3.
        Optional<FileRecords.TimestampAndOffset>  maybeTimestampAndOffset3 = remoteLogManager.findOffsetByTimestamp(tp, ts + 3, startOffset, leaderEpochFileCache);
        assertEquals(Optional.empty(), maybeTimestampAndOffset3);
    }

    @Test
    void testFindOffsetByTimestampWithInvalidEpochSegments() throws IOException, RemoteStorageException {
        remoteLogManager.startup();
        TopicPartition tp = leaderTopicIdPartition.topicPartition();

        long ts = time.milliseconds();
        long startOffset = 120;
        int targetLeaderEpoch = 10;

        TreeMap<Integer, Long> validSegmentEpochs = new TreeMap<>();
        validSegmentEpochs.put(targetLeaderEpoch - 1, startOffset - 1); // invalid epochs not aligning with leader epoch cache
        validSegmentEpochs.put(targetLeaderEpoch, startOffset);

        LeaderEpochFileCache leaderEpochFileCache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        leaderEpochFileCache.assign(4, 99L);
        leaderEpochFileCache.assign(5, 99L);
        leaderEpochFileCache.assign(targetLeaderEpoch, startOffset);
        leaderEpochFileCache.assign(12, 500L);

        doTestFindOffsetByTimestamp(ts, startOffset, targetLeaderEpoch, validSegmentEpochs, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

        // Fetch offsets for this segment returns empty as the segment epochs are not with in the leader epoch cache.
        Optional<FileRecords.TimestampAndOffset> maybeTimestampAndOffset1 = remoteLogManager.findOffsetByTimestamp(tp, ts, startOffset, leaderEpochFileCache);
        assertEquals(Optional.empty(), maybeTimestampAndOffset1);

        Optional<FileRecords.TimestampAndOffset> maybeTimestampAndOffset2 = remoteLogManager.findOffsetByTimestamp(tp, ts + 2, startOffset, leaderEpochFileCache);
        assertEquals(Optional.empty(), maybeTimestampAndOffset2);

        Optional<FileRecords.TimestampAndOffset> maybeTimestampAndOffset3 = remoteLogManager.findOffsetByTimestamp(tp, ts + 3, startOffset, leaderEpochFileCache);
        assertEquals(Optional.empty(), maybeTimestampAndOffset3);
    }

    @Test
    void testFindOffsetByTimestampWithSegmentNotReady() throws IOException, RemoteStorageException {
        remoteLogManager.startup();
        TopicPartition tp = leaderTopicIdPartition.topicPartition();

        long ts = time.milliseconds();
        long startOffset = 120;
        int targetLeaderEpoch = 10;

        TreeMap<Integer, Long> validSegmentEpochs = new TreeMap<>();
        validSegmentEpochs.put(targetLeaderEpoch, startOffset);

        LeaderEpochFileCache leaderEpochFileCache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        leaderEpochFileCache.assign(4, 99L);
        leaderEpochFileCache.assign(5, 99L);
        leaderEpochFileCache.assign(targetLeaderEpoch, startOffset);
        leaderEpochFileCache.assign(12, 500L);

        doTestFindOffsetByTimestamp(ts, startOffset, targetLeaderEpoch, validSegmentEpochs, RemoteLogSegmentState.COPY_SEGMENT_STARTED);

        Optional<FileRecords.TimestampAndOffset> maybeTimestampAndOffset = remoteLogManager.findOffsetByTimestamp(tp, ts, startOffset, leaderEpochFileCache);
        assertEquals(Optional.empty(), maybeTimestampAndOffset);
    }

    private void doTestFindOffsetByTimestamp(long ts, long startOffset, int targetLeaderEpoch,
                                             TreeMap<Integer, Long> validSegmentEpochs,
                                             RemoteLogSegmentState state) throws IOException, RemoteStorageException {
        TopicPartition tp = leaderTopicIdPartition.topicPartition();
        RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid());

        RemoteLogSegmentMetadata segmentMetadata = mock(RemoteLogSegmentMetadata.class);
        when(segmentMetadata.remoteLogSegmentId()).thenReturn(remoteLogSegmentId);
        when(segmentMetadata.maxTimestampMs()).thenReturn(ts + 2);
        when(segmentMetadata.startOffset()).thenReturn(startOffset);
        when(segmentMetadata.endOffset()).thenReturn(startOffset + 2);
        when(segmentMetadata.segmentLeaderEpochs()).thenReturn(validSegmentEpochs);
        when(segmentMetadata.state()).thenReturn(state);

        File tpDir = new File(logDir, tp.toString());
        Files.createDirectory(tpDir.toPath());
        File txnIdxFile = new File(tpDir, "txn-index" + UnifiedLog.TxnIndexFileSuffix());
        txnIdxFile.createNewFile();
        when(remoteStorageManager.fetchIndex(any(RemoteLogSegmentMetadata.class), any(IndexType.class)))
                .thenAnswer(ans -> {
                    RemoteLogSegmentMetadata metadata = ans.getArgument(0);
                    IndexType indexType = ans.getArgument(1);
                    int maxEntries = (int) (metadata.endOffset() - metadata.startOffset());
                    OffsetIndex offsetIdx = new OffsetIndex(new File(tpDir, metadata.startOffset() + UnifiedLog.IndexFileSuffix()),
                            metadata.startOffset(), maxEntries * 8);
                    TimeIndex timeIdx = new TimeIndex(new File(tpDir, metadata.startOffset() + UnifiedLog.TimeIndexFileSuffix()),
                            metadata.startOffset(), maxEntries * 12);
                    switch (indexType) {
                        case OFFSET:
                            return Files.newInputStream(offsetIdx.file().toPath());
                        case TIMESTAMP:
                            return Files.newInputStream(timeIdx.file().toPath());
                        case TRANSACTION:
                            return Files.newInputStream(txnIdxFile.toPath());
                    }
                    return null;
                });

        when(remoteLogMetadataManager.listRemoteLogSegments(eq(leaderTopicIdPartition), anyInt()))
                .thenAnswer(ans -> {
                    int leaderEpoch = ans.<Integer>getArgument(1);
                    if (leaderEpoch == targetLeaderEpoch)
                        return Collections.singleton(segmentMetadata).iterator();
                    else
                        return Collections.emptyIterator();
                });

        // 3 messages are added with offset, and timestamp as below
        // startOffset   , ts-1
        // startOffset+1 , ts+1
        // startOffset+2 , ts+2
        when(remoteStorageManager.fetchLogSegment(segmentMetadata, 0))
                .thenAnswer(a -> new ByteArrayInputStream(records(ts, startOffset, targetLeaderEpoch).buffer().array()));

        when(mockLog.logEndOffset()).thenReturn(600L);

        remoteLogManager.onLeadershipChange(Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.emptySet(), topicIds);
    }

    @Flaky("KAFKA-17779")
    @Test
    void testFetchOffsetByTimestampWithTieredStorageDoesNotFetchIndexWhenExistsLocally() throws Exception {
        TopicPartition tp = new TopicPartition("sample", 0);
        TopicIdPartition tpId = new TopicIdPartition(Uuid.randomUuid(), tp);
        Map<String, Uuid> topicIds = Collections.singletonMap(tp.topic(), tpId.topicId());

        List<EpochEntry> epochEntries = new ArrayList<>();
        epochEntries.add(new EpochEntry(0, 0L));
        epochEntries.add(new EpochEntry(1, 20L));
        epochEntries.add(new EpochEntry(3, 50L));
        epochEntries.add(new EpochEntry(4, 100L));
        epochEntries.add(new EpochEntry(5, 200L));
        checkpoint.write(epochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        long timestamp = time.milliseconds();
        RemoteLogSegmentMetadata metadata0 = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(tpId, Uuid.randomUuid()),
                0, 99, timestamp, brokerId, timestamp, 1024, Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, truncateAndGetLeaderEpochs(epochEntries, 0L, 99L));
        RemoteLogSegmentMetadata metadata1 = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(tpId, Uuid.randomUuid()),
                100, 199, timestamp + 1, brokerId, timestamp + 1, 1024, Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, truncateAndGetLeaderEpochs(epochEntries, 100L, 199L));
        // Note that the metadata2 is in COPY_SEGMENT_STARTED state
        RemoteLogSegmentMetadata metadata2 = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(tpId, Uuid.randomUuid()),
                100, 299, timestamp + 2, brokerId, timestamp + 2, 1024, Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_STARTED, truncateAndGetLeaderEpochs(epochEntries, 200L, 299L));

        when(remoteLogMetadataManager.listRemoteLogSegments(eq(tpId), anyInt()))
            .thenAnswer(ans -> {
                int epoch = ans.getArgument(1);
                if (epoch < 4) {
                    return Collections.singletonList(metadata0).iterator();
                } else if (epoch == 4) {
                    return Arrays.asList(metadata1, metadata2).iterator();
                } else {
                    throw new IllegalArgumentException("Unexpected call!");
                }
            });
        // Different (timestamp, offset) is chosen for remote and local read result to assert the behaviour
        // 9999 -> refers to read from local, 999 -> refers to read from remote
        FileRecords.TimestampAndOffset expectedLocalResult = new FileRecords.TimestampAndOffset(timestamp + 9999, 9999, Optional.of(Integer.MAX_VALUE));
        FileRecords.TimestampAndOffset expectedRemoteResult = new FileRecords.TimestampAndOffset(timestamp + 999, 999, Optional.of(Integer.MAX_VALUE));
        Partition mockFollowerPartition = mockPartition(tpId);

        LogSegment logSegmentBaseOffset50 = mockLogSegment(50L, timestamp, null);
        LogSegment logSegmentBaseOffset100 = mockLogSegment(100L, timestamp + 1, expectedLocalResult);
        LogSegment logSegmentBaseOffset101 = mockLogSegment(101L, timestamp + 1, expectedLocalResult);

        // Constants representing the states of local log segments
        final int twoSegmentsBaseOffsets50and100 = 0;
        final int oneSegmentBaseOffset100 = 1;
        final int oneSegmentBaseOffset101 = 2;

        AtomicInteger localLogOffsetState = new AtomicInteger(twoSegmentsBaseOffsets50and100);

        when(mockLog.logSegments()).thenAnswer(invocation -> {
            if (localLogOffsetState.get() == twoSegmentsBaseOffsets50and100) {
                return Arrays.asList(logSegmentBaseOffset50, logSegmentBaseOffset100);
            } else if (localLogOffsetState.get() == oneSegmentBaseOffset100) {
                return Collections.singletonList(logSegmentBaseOffset100);
            } else if (localLogOffsetState.get() == oneSegmentBaseOffset101) {
                return Collections.singletonList(logSegmentBaseOffset101);
            } else {
                throw new IllegalStateException("Unexpected localLogOffsetState");
            }
        });

        when(mockLog.logEndOffset()).thenReturn(300L);
        remoteLogManager = new RemoteLogManager(config.remoteLogManagerConfig(), brokerId, logDir, clusterId, time,
                partition -> Optional.of(mockLog),
                (topicPartition, offset) -> currentLogStartOffset.set(offset),
                brokerTopicStats, metrics) {
            @Override
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }
            @Override
            Optional<FileRecords.TimestampAndOffset> lookupTimestamp(RemoteLogSegmentMetadata rlsMetadata, long timestamp, long startingOffset) {
                return Optional.of(expectedRemoteResult);
            }
        };
        remoteLogManager.startup();
        remoteLogManager.onLeadershipChange(Collections.emptySet(), Collections.singleton(mockFollowerPartition), topicIds);

        // Read the offset from the remote storage, since the local-log starts from offset 50L and the message with `timestamp` does not exist in the local log
        assertEquals(Optional.of(expectedRemoteResult), remoteLogManager.findOffsetByTimestamp(tp, timestamp, 0L, cache));
        // Short-circuits the read from the remote storage since the local-log starts from offset 50L and
        // the message with (timestamp + 1) exists in the segment with base_offset: 100 which is available locally.
        assertEquals(Optional.of(expectedLocalResult), remoteLogManager.findOffsetByTimestamp(tp, timestamp + 1, 0L, cache));

        // Move the local-log start offset to 100L, still the read from the remote storage should be short-circuited
        // as the message with (timestamp + 1) exists in the local log
        localLogOffsetState.set(oneSegmentBaseOffset100);
        assertEquals(Optional.of(expectedLocalResult), remoteLogManager.findOffsetByTimestamp(tp, timestamp + 1, 0L, cache));

        // Move the local log start offset to 101L, now message with (timestamp + 1) does not exist in the local log and
        // the indexes needs to be fetched from the remote storage
        localLogOffsetState.set(oneSegmentBaseOffset101);
        assertEquals(Optional.of(expectedRemoteResult), remoteLogManager.findOffsetByTimestamp(tp, timestamp + 1, 0L, cache));
    }

    private LogSegment mockLogSegment(long baseOffset,
                                      long largestTimestamp,
                                      FileRecords.TimestampAndOffset timestampAndOffset) throws IOException {
        LogSegment logSegment = mock(LogSegment.class);
        when(logSegment.baseOffset()).thenReturn(baseOffset);
        when(logSegment.largestTimestamp()).thenReturn(largestTimestamp);
        if (timestampAndOffset != null) {
            when(logSegment.findOffsetByTimestamp(anyLong(), anyLong()))
                    .thenReturn(Optional.of(timestampAndOffset));
        }
        return logSegment;
    }

    @Test
    void testIdempotentClose() throws IOException {
        remoteLogManager.close();
        remoteLogManager.close();
        InOrder inorder = inOrder(remoteStorageManager, remoteLogMetadataManager);
        inorder.verify(remoteStorageManager, times(1)).close();
        inorder.verify(remoteLogMetadataManager, times(1)).close();
    }

    @Test
    public void testRemoveMetricsOnClose() throws IOException {
        try (MockedConstruction<KafkaMetricsGroup> mockMetricsGroupCtor = mockConstruction(KafkaMetricsGroup.class)) {
            RemoteLogManager remoteLogManager = new RemoteLogManager(config.remoteLogManagerConfig(), brokerId, logDir, clusterId,
                    time, tp -> Optional.of(mockLog), (topicPartition, offset) -> {
            }, brokerTopicStats, metrics) {
                public RemoteStorageManager createRemoteStorageManager() {
                    return remoteStorageManager;
                }

                public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                    return remoteLogMetadataManager;
                }
            };
            // Close RemoteLogManager so that metrics are removed
            remoteLogManager.close();

            KafkaMetricsGroup mockRlmMetricsGroup = mockMetricsGroupCtor.constructed().get(0);
            KafkaMetricsGroup mockThreadPoolMetricsGroup = mockMetricsGroupCtor.constructed().get(1);

            List<MetricName> remoteLogManagerMetricNames = Arrays.asList(
                    REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC,
                    REMOTE_LOG_READER_FETCH_RATE_AND_TIME_METRIC);
            Set<String> remoteStorageThreadPoolMetricNames = REMOTE_STORAGE_THREAD_POOL_METRICS;

            verify(mockRlmMetricsGroup, times(1)).newGauge(any(MetricName.class), any());
            verify(mockRlmMetricsGroup, times(1)).newTimer(any(MetricName.class), any(), any());
            // Verify that the RemoteLogManager metrics are removed
            remoteLogManagerMetricNames.forEach(metricName -> verify(mockRlmMetricsGroup).removeMetric(metricName));

            verify(mockThreadPoolMetricsGroup, times(remoteStorageThreadPoolMetricNames.size())).newGauge(anyString(), any());
            // Verify that the RemoteStorageThreadPool metrics are removed
            remoteStorageThreadPoolMetricNames.forEach(metricName -> verify(mockThreadPoolMetricsGroup).removeMetric(metricName));

            verifyNoMoreInteractions(mockRlmMetricsGroup);
            verifyNoMoreInteractions(mockThreadPoolMetricsGroup);
        }
    }

    private static RemoteLogSegmentMetadata createRemoteLogSegmentMetadata(long startOffset, long endOffset, Map<Integer, Long> segmentEpochs) {
        return new RemoteLogSegmentMetadata(
                new RemoteLogSegmentId(new TopicIdPartition(Uuid.randomUuid(),
                        new TopicPartition("topic", 0)), Uuid.randomUuid()),
                startOffset, endOffset,
                100000L,
                1,
                100000L,
                1000,
                Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED, segmentEpochs);
    }

    @Test
    public void testBuildFilteredLeaderEpochMap() {
        TreeMap<Integer, Long> leaderEpochToStartOffset = new TreeMap<>();
        leaderEpochToStartOffset.put(0, 0L);
        leaderEpochToStartOffset.put(1, 0L);
        leaderEpochToStartOffset.put(2, 0L);
        leaderEpochToStartOffset.put(3, 30L);
        leaderEpochToStartOffset.put(4, 40L);
        leaderEpochToStartOffset.put(5, 60L);
        leaderEpochToStartOffset.put(6, 60L);
        leaderEpochToStartOffset.put(7, 70L);
        leaderEpochToStartOffset.put(8, 70L);

        TreeMap<Integer, Long> expectedLeaderEpochs = new TreeMap<>();
        expectedLeaderEpochs.put(2, 0L);
        expectedLeaderEpochs.put(3, 30L);
        expectedLeaderEpochs.put(4, 40L);
        expectedLeaderEpochs.put(6, 60L);
        expectedLeaderEpochs.put(8, 70L);

        NavigableMap<Integer, Long> refinedLeaderEpochMap = RemoteLogManager.buildFilteredLeaderEpochMap(leaderEpochToStartOffset);
        assertEquals(expectedLeaderEpochs, refinedLeaderEpochMap);
    }

    @Test
    public void testRemoteSegmentWithinLeaderEpochs() {
        // Test whether a remote segment is within the leader epochs
        final long logEndOffset = 90L;

        TreeMap<Integer, Long> leaderEpochToStartOffset = new TreeMap<>();
        leaderEpochToStartOffset.put(0, 0L);
        leaderEpochToStartOffset.put(1, 10L);
        leaderEpochToStartOffset.put(2, 20L);
        leaderEpochToStartOffset.put(3, 30L);
        leaderEpochToStartOffset.put(4, 40L);
        leaderEpochToStartOffset.put(5, 50L);
        leaderEpochToStartOffset.put(7, 70L);

        // Test whether a remote segment's epochs/offsets(multiple) are within the range of leader epochs
        TreeMap<Integer, Long> segmentEpochs1 = new TreeMap<>();
        segmentEpochs1.put(1, 15L);
        segmentEpochs1.put(2, 20L);
        segmentEpochs1.put(3, 30L);

        assertTrue(isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                15,
                35,
                segmentEpochs1), logEndOffset, leaderEpochToStartOffset));

        // Test whether a remote segment's epochs/offsets(single) are within the range of leader epochs
        TreeMap<Integer, Long> segmentEpochs2 = new TreeMap<>();
        segmentEpochs2.put(1, 15L);
        assertTrue(isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                15,
                19,
                segmentEpochs2), logEndOffset, leaderEpochToStartOffset));

        // Test whether a remote segment's start offset is same as the offset of the respective leader epoch entry.
        TreeMap<Integer, Long> segmentEpochs3 = new TreeMap<>();
        segmentEpochs3.put(0, 0L); // same as leader epoch's start offset
        assertTrue(isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                0,
                5,
                segmentEpochs3), logEndOffset, leaderEpochToStartOffset));

        // Test whether a remote segment's start offset is same as the offset of the respective leader epoch entry.
        TreeMap<Integer, Long> segmentEpochs4 = new TreeMap<>();
        segmentEpochs4.put(7, 70L); // same as leader epoch's start offset
        assertTrue(isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                70,
                75,
                segmentEpochs4), logEndOffset, leaderEpochToStartOffset));

        // Test whether a remote segment's end offset is same as the end offset of the respective leader epoch entry.
        TreeMap<Integer, Long> segmentEpochs5 = new TreeMap<>();
        segmentEpochs5.put(1, 15L);
        segmentEpochs5.put(2, 20L);

        assertTrue(isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                15,
                29, // same as end offset for epoch 2 in leaderEpochToStartOffset
                segmentEpochs5), logEndOffset, leaderEpochToStartOffset));

        // Test whether any of the epoch's is not with in the leader epoch chain.
        TreeMap<Integer, Long> segmentEpochs6 = new TreeMap<>();
        segmentEpochs6.put(5, 55L);
        segmentEpochs6.put(6, 60L); // epoch 6 exists here but it is missing in leaderEpochToStartOffset
        segmentEpochs6.put(7, 70L);

        assertFalse(isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                55,
                85,
                segmentEpochs6), logEndOffset, leaderEpochToStartOffset));

        // Test whether an epoch existing in remote segment does not exist in leader epoch chain.
        TreeMap<Integer, Long> segmentEpochs7 = new TreeMap<>();
        segmentEpochs7.put(1, 15L);
        segmentEpochs7.put(2, 20L); // epoch 3 is missing here which exists in leaderEpochToStartOffset
        segmentEpochs7.put(4, 40L);

        assertFalse(isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                15,
                45,
                segmentEpochs7), logEndOffset, leaderEpochToStartOffset));

        // Test a remote segment having larger end offset than the log end offset
        assertFalse(isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                15,
                95, // larger than log end offset
                leaderEpochToStartOffset), logEndOffset, leaderEpochToStartOffset));

        assertFalse(isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                15,
                90, // equal to the log end offset
                leaderEpochToStartOffset), logEndOffset, leaderEpochToStartOffset));

        // Test whether a segment's first offset is earlier to the respective epoch's start offset
        TreeMap<Integer, Long> segmentEpochs9 = new TreeMap<>();
        segmentEpochs9.put(1, 5L);
        segmentEpochs9.put(2, 20L);

        assertFalse(isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                5, // earlier to epoch 1's start offset
                25,
                segmentEpochs9), logEndOffset, leaderEpochToStartOffset));

        // Test whether a segment's last offset is more than the respective epoch's end offset
        TreeMap<Integer, Long> segmentEpochs10 = new TreeMap<>();
        segmentEpochs10.put(1, 15L);
        segmentEpochs10.put(2, 20L);
        assertFalse(isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                15,
                35, // more than epoch 2's end offset
                segmentEpochs10), logEndOffset, leaderEpochToStartOffset));
    }

    @Test
    public void testRemoteSegmentWithinLeaderEpochsForOverlappingSegments() {
        NavigableMap<Integer, Long> leaderEpochCache = new TreeMap<>();
        leaderEpochCache.put(7, 51L);
        leaderEpochCache.put(9, 100L);

        TreeMap<Integer, Long> segment1Epochs = new TreeMap<>();
        segment1Epochs.put(5, 14L);
        segment1Epochs.put(7, 15L);
        segment1Epochs.put(9, 100L);
        RemoteLogSegmentMetadata segment1 = createRemoteLogSegmentMetadata(14, 150, segment1Epochs);
        assertTrue(isRemoteSegmentWithinLeaderEpochs(segment1, 210, leaderEpochCache));

        // segment2Epochs are not within the leaderEpochCache
        TreeMap<Integer, Long> segment2Epochs = new TreeMap<>();
        segment2Epochs.put(2, 5L);
        segment2Epochs.put(3, 6L);
        RemoteLogSegmentMetadata segment2 = createRemoteLogSegmentMetadata(2, 7, segment2Epochs);
        assertFalse(isRemoteSegmentWithinLeaderEpochs(segment2, 210, leaderEpochCache));

        // segment3Epochs are not within the leaderEpochCache
        TreeMap<Integer, Long> segment3Epochs = new TreeMap<>();
        segment3Epochs.put(7, 15L);
        segment3Epochs.put(9, 100L);
        segment3Epochs.put(10, 200L);
        RemoteLogSegmentMetadata segment3 = createRemoteLogSegmentMetadata(15, 250, segment3Epochs);
        assertFalse(isRemoteSegmentWithinLeaderEpochs(segment3, 210, leaderEpochCache));

        // segment4Epochs are not within the leaderEpochCache
        TreeMap<Integer, Long> segment4Epochs = new TreeMap<>();
        segment4Epochs.put(8, 75L);
        RemoteLogSegmentMetadata segment4 = createRemoteLogSegmentMetadata(75, 100, segment4Epochs);
        assertFalse(isRemoteSegmentWithinLeaderEpochs(segment4, 210, leaderEpochCache));

        // segment5Epochs does not match with the leaderEpochCache
        TreeMap<Integer, Long> segment5Epochs = new TreeMap<>();
        segment5Epochs.put(7, 15L);
        segment5Epochs.put(9, 101L);
        RemoteLogSegmentMetadata segment5 = createRemoteLogSegmentMetadata(15, 150, segment5Epochs);
        assertFalse(isRemoteSegmentWithinLeaderEpochs(segment5, 210, leaderEpochCache));

        // segment6Epochs does not match with the leaderEpochCache
        TreeMap<Integer, Long> segment6Epochs = new TreeMap<>();
        segment6Epochs.put(9, 99L);
        RemoteLogSegmentMetadata segment6 = createRemoteLogSegmentMetadata(99, 150, segment6Epochs);
        assertFalse(isRemoteSegmentWithinLeaderEpochs(segment6, 210, leaderEpochCache));
    }

    @Test
    public void testCandidateLogSegmentsSkipsActiveSegment() {
        UnifiedLog log = mock(UnifiedLog.class);
        LogSegment segment1 = mock(LogSegment.class);
        LogSegment segment2 = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(segment1.baseOffset()).thenReturn(5L);
        when(segment2.baseOffset()).thenReturn(10L);
        when(activeSegment.baseOffset()).thenReturn(15L);

        when(log.logSegments(5L, Long.MAX_VALUE))
                .thenReturn(CollectionConverters.asScala(Arrays.asList(segment1, segment2, activeSegment)));

        RemoteLogManager.RLMCopyTask task = remoteLogManager.new RLMCopyTask(leaderTopicIdPartition, 128);
        List<RemoteLogManager.EnrichedLogSegment> expected =
                Arrays.asList(
                        new RemoteLogManager.EnrichedLogSegment(segment1, 10L),
                        new RemoteLogManager.EnrichedLogSegment(segment2, 15L)
                );
        List<RemoteLogManager.EnrichedLogSegment> actual = task.candidateLogSegments(log, 5L, 20L);
        assertEquals(expected, actual);
    }

    @Test
    public void testCandidateLogSegmentsSkipsSegmentsAfterLastStableOffset() {
        UnifiedLog log = mock(UnifiedLog.class);
        LogSegment segment1 = mock(LogSegment.class);
        LogSegment segment2 = mock(LogSegment.class);
        LogSegment segment3 = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(segment1.baseOffset()).thenReturn(5L);
        when(segment2.baseOffset()).thenReturn(10L);
        when(segment3.baseOffset()).thenReturn(15L);
        when(activeSegment.baseOffset()).thenReturn(20L);

        when(log.logSegments(5L, Long.MAX_VALUE))
                .thenReturn(CollectionConverters.asScala(Arrays.asList(segment1, segment2, segment3, activeSegment)));

        RemoteLogManager.RLMCopyTask task = remoteLogManager.new RLMCopyTask(leaderTopicIdPartition, 128);
        List<RemoteLogManager.EnrichedLogSegment> expected =
                Arrays.asList(
                        new RemoteLogManager.EnrichedLogSegment(segment1, 10L),
                        new RemoteLogManager.EnrichedLogSegment(segment2, 15L)
                );
        List<RemoteLogManager.EnrichedLogSegment> actual = task.candidateLogSegments(log, 5L, 15L);
        assertEquals(expected, actual);
    }

    @Test
    public void testRemoteSizeData() {
        Supplier<RemoteLogManager.RetentionSizeData>[] invalidRetentionSizeData =
            new Supplier[]{
                () -> new RemoteLogManager.RetentionSizeData(10, 0),
                () -> new RemoteLogManager.RetentionSizeData(10, -1),
                () -> new RemoteLogManager.RetentionSizeData(-1, 10),
                () -> new RemoteLogManager.RetentionSizeData(-1, -1),
                () -> new RemoteLogManager.RetentionSizeData(-1, 0)
            };

        for (Supplier<RemoteLogManager.RetentionSizeData> invalidRetentionSizeDataEntry : invalidRetentionSizeData) {
            assertThrows(IllegalArgumentException.class, invalidRetentionSizeDataEntry::get);
        }
    }

    @Test
    public void testRemoteSizeTime() {
        Supplier<RemoteLogManager.RetentionTimeData>[] invalidRetentionTimeData =
            new Supplier[] {
                () -> new RemoteLogManager.RetentionTimeData(-1, 10),
                () -> new RemoteLogManager.RetentionTimeData(10, -1),
            };

        for (Supplier<RemoteLogManager.RetentionTimeData> invalidRetentionTimeDataEntry : invalidRetentionTimeData) {
            assertThrows(IllegalArgumentException.class, invalidRetentionTimeDataEntry::get);
        }
    }

    @Test
    public void testStopPartitionsWithoutDeletion() throws RemoteStorageException {
        remoteLogManager.startup();
        BiConsumer<TopicPartition, Throwable> errorHandler = (topicPartition, throwable) -> fail("shouldn't be called");
        Set<StopPartition> partitions = new HashSet<>();
        partitions.add(new StopPartition(leaderTopicIdPartition.topicPartition(), true, false, false));
        partitions.add(new StopPartition(followerTopicIdPartition.topicPartition(), true, false, false));
        remoteLogManager.onLeadershipChange(Collections.singleton(mockPartition(leaderTopicIdPartition)),
                Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);
        assertNotNull(remoteLogManager.leaderCopyTask(leaderTopicIdPartition));
        assertNotNull(remoteLogManager.leaderExpirationTask(leaderTopicIdPartition));
        assertNotNull(remoteLogManager.followerTask(followerTopicIdPartition));

        remoteLogManager.stopPartitions(partitions, errorHandler);
        assertNull(remoteLogManager.leaderCopyTask(leaderTopicIdPartition));
        assertNull(remoteLogManager.leaderExpirationTask(leaderTopicIdPartition));
        assertNull(remoteLogManager.followerTask(followerTopicIdPartition));
        verify(remoteLogMetadataManager, times(1)).onStopPartitions(any());
        verify(remoteStorageManager, times(0)).deleteLogSegmentData(any());
        verify(remoteLogMetadataManager, times(0)).updateRemoteLogSegmentMetadata(any());
    }

    @Test
    public void testStopPartitionsWithDeletion() throws RemoteStorageException {
        remoteLogManager.startup();
        BiConsumer<TopicPartition, Throwable> errorHandler =
                (topicPartition, ex) -> fail("shouldn't be called: " + ex);
        Set<StopPartition> partitions = new HashSet<>();
        partitions.add(new StopPartition(leaderTopicIdPartition.topicPartition(), true, true, true));
        partitions.add(new StopPartition(followerTopicIdPartition.topicPartition(), true, true, true));

        when(remoteLogMetadataManager.listRemoteLogSegments(eq(leaderTopicIdPartition)))
                .thenReturn(listRemoteLogSegmentMetadata(leaderTopicIdPartition, 5, 100, 1024, RemoteLogSegmentState.DELETE_SEGMENT_FINISHED).iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(eq(followerTopicIdPartition)))
                .thenReturn(listRemoteLogSegmentMetadata(followerTopicIdPartition, 3, 100, 1024, RemoteLogSegmentState.DELETE_SEGMENT_FINISHED).iterator());
        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any()))
                .thenReturn(dummyFuture);

        remoteLogManager.onLeadershipChange(Collections.singleton(mockPartition(leaderTopicIdPartition)),
                Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);
        assertNotNull(remoteLogManager.leaderCopyTask(leaderTopicIdPartition));
        assertNotNull(remoteLogManager.leaderExpirationTask(leaderTopicIdPartition));
        assertNotNull(remoteLogManager.followerTask(followerTopicIdPartition));

        remoteLogManager.stopPartitions(partitions, errorHandler);
        assertNull(remoteLogManager.leaderCopyTask(leaderTopicIdPartition));
        assertNull(remoteLogManager.leaderExpirationTask(leaderTopicIdPartition));
        assertNull(remoteLogManager.followerTask(followerTopicIdPartition));
        verify(remoteLogMetadataManager, times(1)).onStopPartitions(any());
        verify(remoteStorageManager, times(8)).deleteLogSegmentData(any());
        verify(remoteLogMetadataManager, times(16)).updateRemoteLogSegmentMetadata(any());
    }

    /**
     * This test asserts that the newly elected leader for a partition is able to find the log-start-offset.
     * Note that the case tested here is that the previous leader deleted the log segments up-to offset 500. And, the
     * log-start-offset didn't propagate to the replicas before the leader-election.
     */
    @Test
    public void testFindLogStartOffset() throws RemoteStorageException, IOException {
        List<EpochEntry> epochEntries = new ArrayList<>();
        epochEntries.add(new EpochEntry(0, 0L));
        epochEntries.add(new EpochEntry(1, 250L));
        epochEntries.add(new EpochEntry(2, 550L));
        checkpoint.write(epochEntries);

        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        long timestamp = time.milliseconds();
        int segmentSize = 1024;
        List<RemoteLogSegmentMetadata> segmentMetadataList = Arrays.asList(
                new RemoteLogSegmentMetadata(new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid()),
                        500, 539, timestamp, brokerId, timestamp, segmentSize, truncateAndGetLeaderEpochs(epochEntries, 500L, 539L)),
                new RemoteLogSegmentMetadata(new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid()),
                        540, 700, timestamp, brokerId, timestamp, segmentSize, truncateAndGetLeaderEpochs(epochEntries, 540L, 700L))
                );
        when(remoteLogMetadataManager.listRemoteLogSegments(eq(leaderTopicIdPartition), anyInt()))
                .thenAnswer(invocation -> {
                    int epoch = invocation.getArgument(1);
                    if (epoch == 1)
                        return segmentMetadataList.iterator();
                    else
                        return Collections.emptyIterator();
                });
        try (RemoteLogManager remoteLogManager = new RemoteLogManager(config.remoteLogManagerConfig(), brokerId, logDir, clusterId, time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> { },
                brokerTopicStats, metrics) {
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }
        }) {
            assertEquals(500L, remoteLogManager.findLogStartOffset(leaderTopicIdPartition, mockLog));
        }
    }

    @Test
    public void testFindLogStartOffsetFallbackToLocalLogStartOffsetWhenRemoteIsEmpty() throws RemoteStorageException, IOException {
        List<EpochEntry> epochEntries = new ArrayList<>();
        epochEntries.add(new EpochEntry(1, 250L));
        epochEntries.add(new EpochEntry(2, 550L));
        checkpoint.write(epochEntries);

        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(mockLog.localLogStartOffset()).thenReturn(250L);
        when(remoteLogMetadataManager.listRemoteLogSegments(eq(leaderTopicIdPartition), anyInt()))
                .thenReturn(Collections.emptyIterator());

        try (RemoteLogManager remoteLogManager = new RemoteLogManager(config.remoteLogManagerConfig(), brokerId, logDir, clusterId, time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> { },
                brokerTopicStats, metrics) {
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }
        }) {
            assertEquals(250L, remoteLogManager.findLogStartOffset(leaderTopicIdPartition, mockLog));
        }
    }

    @Test
    public void testLogStartOffsetUpdatedOnStartup() throws RemoteStorageException, IOException, InterruptedException {
        List<EpochEntry> epochEntries = new ArrayList<>();
        epochEntries.add(new EpochEntry(1, 250L));
        epochEntries.add(new EpochEntry(2, 550L));
        checkpoint.write(epochEntries);

        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        RemoteLogSegmentMetadata metadata = mock(RemoteLogSegmentMetadata.class);
        when(metadata.startOffset()).thenReturn(600L);
        when(remoteLogMetadataManager.listRemoteLogSegments(eq(leaderTopicIdPartition), anyInt()))
                .thenAnswer(invocation -> {
                    int epoch = invocation.getArgument(1);
                    if (epoch == 2)
                        return Collections.singletonList(metadata).iterator();
                    else
                        return Collections.emptyIterator();
                });

        AtomicLong logStartOffset = new AtomicLong(0);
        try (RemoteLogManager remoteLogManager = new RemoteLogManager(config.remoteLogManagerConfig(), brokerId, logDir, clusterId, time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) ->  logStartOffset.set(offset),
                brokerTopicStats, metrics) {
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }
        }) {
            RemoteLogManager.RLMCopyTask task = remoteLogManager.new RLMCopyTask(leaderTopicIdPartition, 128);
            task.copyLogSegmentsToRemote(mockLog);
            assertEquals(600L, logStartOffset.get());
        }
    }

    @Test
    public void testDeletionSkippedForSegmentsBeingCopied() throws RemoteStorageException, IOException, InterruptedException, ExecutionException {
        RemoteLogMetadataManager remoteLogMetadataManager = new NoOpRemoteLogMetadataManager() {
            List<RemoteLogSegmentMetadata> metadataList = new ArrayList<>();

            @Override
            public synchronized CompletableFuture<Void> addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
                metadataList.add(remoteLogSegmentMetadata);
                return CompletableFuture.runAsync(() -> { });
            }

            @Override
            public synchronized CompletableFuture<Void> updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate remoteLogSegmentMetadataUpdate) {
                metadataList = metadataList.stream()
                        .map(m -> {
                            if (m.remoteLogSegmentId().equals(remoteLogSegmentMetadataUpdate.remoteLogSegmentId())) {
                                return m.createWithUpdates(remoteLogSegmentMetadataUpdate);
                            }
                            return m;
                        })
                        .collect(Collectors.toList());
                return CompletableFuture.runAsync(() -> { });
            }

            @Override
            public Optional<Long> highestOffsetForEpoch(TopicIdPartition topicIdPartition, int leaderEpoch) {
                return Optional.of(-1L);
            }

            @Override
            public synchronized Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition) {
                return metadataList.iterator();
            }

            @Override
            public synchronized Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition, int leaderEpoch) {
                return metadataList.iterator();
            }
        };

        remoteLogManager = new RemoteLogManager(config.remoteLogManagerConfig(), brokerId, logDir, clusterId, time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> currentLogStartOffset.set(offset),
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

        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;
        long lastStableOffset = 150L;
        long logEndOffset = 150L;

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());

        // leader epoch preparation
        checkpoint.write(Collections.singletonList(epochEntry0));
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        // create 2 log segments, with 0 and 150 as log start offset
        LogSegment oldSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);
        when(oldSegment.baseOffset()).thenReturn(oldSegmentStartOffset);
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);

        File tempFile = TestUtils.tempFile();
        FileRecords fileRecords = mock(FileRecords.class);
        when(fileRecords.file()).thenReturn(tempFile);
        when(fileRecords.sizeInBytes()).thenReturn(10);

        when(oldSegment.log()).thenReturn(fileRecords);
        when(oldSegment.readNextOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(CollectionConverters.asScala(Arrays.asList(oldSegment, activeSegment)));

        File mockProducerSnapshotIndex = TestUtils.tempFile();
        ProducerStateManager mockStateManager = mock(ProducerStateManager.class);
        when(mockLog.producerStateManager()).thenReturn(mockStateManager);
        when(mockStateManager.fetchSnapshot(anyLong())).thenReturn(Optional.of(mockProducerSnapshotIndex));
        when(mockLog.lastStableOffset()).thenReturn(lastStableOffset);
        when(mockLog.logEndOffset()).thenReturn(logEndOffset);

        File tempDir = TestUtils.tempDirectory();
        OffsetIndex idx = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1000).get();
        TimeIndex timeIdx = LazyIndex.forTime(LogFileUtils.timeIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1500).get();
        File txnFile = UnifiedLog.transactionIndexFile(tempDir, oldSegmentStartOffset, "");
        txnFile.createNewFile();
        TransactionIndex txnIndex = new TransactionIndex(oldSegmentStartOffset, txnFile);
        when(oldSegment.timeIndex()).thenReturn(timeIdx);
        when(oldSegment.offsetIndex()).thenReturn(idx);
        when(oldSegment.txnIndex()).thenReturn(txnIndex);

        CountDownLatch copyLogSegmentLatch = new CountDownLatch(1);
        CountDownLatch copySegmentDataLatch = new CountDownLatch(1);
        doAnswer(ans -> {
            // unblock the expiration thread
            copySegmentDataLatch.countDown();
            // introduce a delay in copying segment data
            copyLogSegmentLatch.await(5000, TimeUnit.MILLISECONDS);
            return Optional.empty();
        }).when(remoteStorageManager).copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class));
        when(rlmCopyQuotaManager.getThrottleTimeMs()).thenReturn(quotaAvailableThrottleTime);

        // Set up expiration behaviour
        Map<String, Long> logProps = new HashMap<>();
        logProps.put("retention.bytes", 0L);
        logProps.put("retention.ms", -1L);
        LogConfig mockLogConfig = new LogConfig(logProps);
        when(mockLog.config()).thenReturn(mockLogConfig);

        RemoteLogManager.RLMCopyTask copyTask = remoteLogManager.new RLMCopyTask(leaderTopicIdPartition, 128);
        Thread copyThread  = new Thread(() -> {
            try {
                copyTask.copyLogSegmentsToRemote(mockLog);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        RemoteLogManager.RLMExpirationTask expirationTask = remoteLogManager.new RLMExpirationTask(leaderTopicIdPartition);
        Thread expirationThread = new Thread(() -> {
            try {
                // wait until copy thread has started copying segment data
                copySegmentDataLatch.await();
                expirationTask.cleanupExpiredRemoteLogSegments();
                copyLogSegmentLatch.countDown();
            } catch (RemoteStorageException | ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        copyThread.start();
        expirationThread.start();

        copyThread.join(10_000);
        expirationThread.join(1_000);

        // Verify no segments were deleted
        verify(remoteStorageManager, times(0)).deleteLogSegmentData(any(RemoteLogSegmentMetadata.class));

        // Run expiration task again and verify the copied segment was deleted
        RemoteLogSegmentMetadata remoteLogSegmentMetadata = remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition).next();
        expirationTask.cleanupExpiredRemoteLogSegments();
        verify(remoteStorageManager, times(1)).deleteLogSegmentData(remoteLogSegmentMetadata);
    }

    @ParameterizedTest(name = "testDeletionOnRetentionBreachedSegments retentionSize={0} retentionMs={1}")
    @CsvSource(value = {"0, -1", "-1, 0"})
    public void testDeletionOnRetentionBreachedSegments(long retentionSize,
                                                        long retentionMs)
            throws RemoteStorageException, ExecutionException, InterruptedException {
        Map<String, Long> logProps = new HashMap<>();
        logProps.put("retention.bytes", retentionSize);
        logProps.put("retention.ms", retentionMs);
        LogConfig mockLogConfig = new LogConfig(logProps);
        when(mockLog.config()).thenReturn(mockLogConfig);

        List<EpochEntry> epochEntries = Collections.singletonList(epochEntry0);
        checkpoint.write(epochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());
        when(mockLog.logEndOffset()).thenReturn(200L);

        List<RemoteLogSegmentMetadata> metadataList =
                listRemoteLogSegmentMetadata(leaderTopicIdPartition, 2, 100, 1024, epochEntries, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition))
                .thenReturn(metadataList.iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 0))
                .thenAnswer(ans -> metadataList.iterator());
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class)))
                .thenReturn(CompletableFuture.runAsync(() -> { }));

        // Verify the metrics for remote deletes and for failures is zero before attempt to delete segments
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteDeleteRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteDeleteRequestRate().count());
        // Verify aggregate metrics
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteDeleteRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteDeleteRequestRate().count());


        RemoteLogManager.RLMExpirationTask task = remoteLogManager.new RLMExpirationTask(leaderTopicIdPartition);
        task.cleanupExpiredRemoteLogSegments();

        assertEquals(200L, currentLogStartOffset.get());
        verify(remoteStorageManager).deleteLogSegmentData(metadataList.get(0));
        verify(remoteStorageManager).deleteLogSegmentData(metadataList.get(1));

        // Verify the metric for remote delete is updated correctly
        assertEquals(2, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteDeleteRequestRate().count());
        // Verify we did not report any failure for remote deletes
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteDeleteRequestRate().count());
        // Verify aggregate metrics
        assertEquals(2, brokerTopicStats.allTopicsStats().remoteDeleteRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteDeleteRequestRate().count());
    }

    @ParameterizedTest(name = "testDeletionOnOverlappingRetentionBreachedSegments retentionSize={0} retentionMs={1}")
    @CsvSource(value = {"0, -1", "-1, 0"})
    public void testDeletionOnOverlappingRetentionBreachedSegments(long retentionSize,
                                                                   long retentionMs)
            throws RemoteStorageException, ExecutionException, InterruptedException {
        Map<String, Long> logProps = new HashMap<>();
        logProps.put("retention.bytes", retentionSize);
        logProps.put("retention.ms", retentionMs);
        LogConfig mockLogConfig = new LogConfig(logProps);
        when(mockLog.config()).thenReturn(mockLogConfig);

        List<EpochEntry> epochEntries = Collections.singletonList(epochEntry0);
        checkpoint.write(epochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());
        when(mockLog.logEndOffset()).thenReturn(200L);

        RemoteLogSegmentMetadata metadata1 = listRemoteLogSegmentMetadata(leaderTopicIdPartition, 1, 100, 1024,
                epochEntries, RemoteLogSegmentState.COPY_SEGMENT_FINISHED)
                .get(0);
        // overlapping segment
        RemoteLogSegmentMetadata metadata2 = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid()),
                metadata1.startOffset(), metadata1.endOffset() + 5, metadata1.maxTimestampMs(),
                metadata1.brokerId() + 1, metadata1.eventTimestampMs(), metadata1.segmentSizeInBytes() + 128,
                metadata1.customMetadata(), metadata1.state(), metadata1.segmentLeaderEpochs());

        // When there are overlapping/duplicate segments, the RemoteLogMetadataManager#listRemoteLogSegments
        // returns the segments in order of (valid ++ unreferenced) segments:
        // (eg) B0 uploaded segment S0 with offsets 0-100 and B1 uploaded segment S1 with offsets 0-200.
        //      We will mark the segment S0 as duplicate and add it to unreferencedSegmentIds.
        //      The order of segments returned by listRemoteLogSegments will be S1, S0.
        // While computing the next-log-start-offset, taking the max of deleted segment's end-offset + 1.
        List<RemoteLogSegmentMetadata> metadataList = new ArrayList<>();
        metadataList.add(metadata2);
        metadataList.add(metadata1);

        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition))
                .thenReturn(metadataList.iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 0))
                .thenAnswer(ans -> metadataList.iterator());
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class)))
                .thenReturn(CompletableFuture.runAsync(() -> { }));

        // Verify the metrics for remote deletes and for failures is zero before attempt to delete segments
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteDeleteRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteDeleteRequestRate().count());
        // Verify aggregate metrics
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteDeleteRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteDeleteRequestRate().count());

        RemoteLogManager.RLMExpirationTask task = remoteLogManager.new RLMExpirationTask(leaderTopicIdPartition);
        task.cleanupExpiredRemoteLogSegments();

        assertEquals(metadata2.endOffset() + 1, currentLogStartOffset.get());
        verify(remoteStorageManager).deleteLogSegmentData(metadataList.get(0));
        verify(remoteStorageManager).deleteLogSegmentData(metadataList.get(1));

        // Verify the metric for remote delete is updated correctly
        assertEquals(2, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteDeleteRequestRate().count());
        // Verify we did not report any failure for remote deletes
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteDeleteRequestRate().count());
        // Verify aggregate metrics
        assertEquals(2, brokerTopicStats.allTopicsStats().remoteDeleteRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteDeleteRequestRate().count());
    }

    @ParameterizedTest(name = "testRemoteDeleteLagsOnRetentionBreachedSegments retentionSize={0} retentionMs={1}")
    @CsvSource(value = {"0, -1", "-1, 0"})
    public void testRemoteDeleteLagsOnRetentionBreachedSegments(long retentionSize,
                                                                long retentionMs)
            throws RemoteStorageException, ExecutionException, InterruptedException {
        Map<String, Long> logProps = new HashMap<>();
        logProps.put("retention.bytes", retentionSize);
        logProps.put("retention.ms", retentionMs);
        LogConfig mockLogConfig = new LogConfig(logProps);
        when(mockLog.config()).thenReturn(mockLogConfig);

        List<EpochEntry> epochEntries = Collections.singletonList(epochEntry0);
        checkpoint.write(epochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());
        when(mockLog.logEndOffset()).thenReturn(200L);

        List<RemoteLogSegmentMetadata> metadataList =
                listRemoteLogSegmentMetadata(leaderTopicIdPartition, 2, 100, 1024, epochEntries, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition))
                .thenReturn(metadataList.iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 0))
                .thenAnswer(ans -> metadataList.iterator());
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class)))
                .thenReturn(CompletableFuture.runAsync(() -> { }));

        doAnswer(ans -> {
            verifyRemoteDeleteMetrics(2048L, 2L);
            return Optional.empty();
        }).doAnswer(ans -> {
            verifyRemoteDeleteMetrics(1024L, 1L);
            return Optional.empty();
        }).when(remoteStorageManager).deleteLogSegmentData(any(RemoteLogSegmentMetadata.class));

        RemoteLogManager.RLMExpirationTask task = remoteLogManager.new RLMExpirationTask(leaderTopicIdPartition);

        verifyRemoteDeleteMetrics(0L, 0L);

        task.cleanupExpiredRemoteLogSegments();

        assertEquals(200L, currentLogStartOffset.get());
        verify(remoteStorageManager).deleteLogSegmentData(metadataList.get(0));
        verify(remoteStorageManager).deleteLogSegmentData(metadataList.get(1));
    }

    @Test
    public void testRemoteLogSizeRetentionShouldFilterOutCopySegmentStartState()
            throws RemoteStorageException, ExecutionException, InterruptedException {
        int segmentSize = 1024;
        Map<String, Long> logProps = new HashMap<>();
        // set the retention.bytes to 10 segment size
        logProps.put("retention.bytes", segmentSize * 10L);
        logProps.put("retention.ms", -1L);
        LogConfig mockLogConfig = new LogConfig(logProps);
        when(mockLog.config()).thenReturn(mockLogConfig);

        List<EpochEntry> epochEntries = Collections.singletonList(epochEntry0);
        checkpoint.write(epochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());
        when(mockLog.logEndOffset()).thenReturn(2000L);

        // creating remote log metadata list:
        // s1. One segment with "COPY_SEGMENT_STARTED" state to simulate the segment was failing on copying to remote storage (dangling).
        //     it should be ignored for both remote log size calculation, but get deleted in the 1st run.
        // s2. One segment with "DELETE_SEGMENT_FINISHED" state to simulate the remoteLogMetadataManager doesn't filter it out and returned.
        //     We should filter it out when calculating remote storage log size and deletion
        // s3. One segment with "DELETE_SEGMENT_STARTED" state to simulate the segment was failing on deleting remote log (dangling).
        //     We should NOT count it when calculating remote storage log size and we should retry deletion.
        // s4. Another segment with "COPY_SEGMENT_STARTED" state to simulate the segment is copying to remote storage.
        //     The segment state will change to "COPY_SEGMENT_FINISHED" state before checking deletion.
        //     In the 1st run, this segment should be skipped when calculating remote storage size.
        //     In the 2nd run, we should count it in when calculating remote storage size.
        // s5. 11 segments with "COPY_SEGMENT_FINISHED" state. These are expected to be counted in when calculating remote storage log size
        //
        // Expected results (retention.size is 10240 (10 segments)):
        // In the 1st run, the total remote storage size should be 1024 * 11 (s5) and 2 segments (s1, s3) will be deleted because they are dangling segments.
        // Note: segments being copied are filtered out by the expiration logic, so s1 may be the result of an old failed copy cleanup where we weren't updating the state.
        // In the 2nd run, the total remote storage size should be 1024 * 12 (s4, s5) and 2 segments (s4, s5[0]) will be deleted because of retention size breach.
        RemoteLogSegmentMetadata s1 = createRemoteLogSegmentMetadata(new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid()),
                0, 99, segmentSize, epochEntries, RemoteLogSegmentState.COPY_SEGMENT_STARTED);
        RemoteLogSegmentMetadata s2 = createRemoteLogSegmentMetadata(new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid()),
                0, 99, segmentSize, epochEntries, RemoteLogSegmentState.DELETE_SEGMENT_FINISHED);
        RemoteLogSegmentMetadata s3 = createRemoteLogSegmentMetadata(new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid()),
                0, 99, segmentSize, epochEntries, RemoteLogSegmentState.DELETE_SEGMENT_STARTED);
        RemoteLogSegmentMetadata s4CopyStarted = createRemoteLogSegmentMetadata(new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid()),
                200, 299, segmentSize, epochEntries, RemoteLogSegmentState.COPY_SEGMENT_STARTED);
        RemoteLogSegmentMetadata s4CopyFinished = createRemoteLogSegmentMetadata(s4CopyStarted.remoteLogSegmentId(),
                s4CopyStarted.startOffset(), s4CopyStarted.endOffset(), segmentSize, epochEntries, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        List<RemoteLogSegmentMetadata> s5 =
                listRemoteLogSegmentMetadata(leaderTopicIdPartition, 11, 100, 1024, epochEntries, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

        List<RemoteLogSegmentMetadata> metadataList = new LinkedList<>();
        metadataList.addAll(Arrays.asList(s1, s2, s3, s4CopyStarted));
        metadataList.addAll(s5);

        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition))
                .thenReturn(metadataList.iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 0))
                .thenReturn(metadataList.iterator()).thenReturn(metadataList.iterator());
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class)))
                .thenReturn(CompletableFuture.runAsync(() -> { }));
        doNothing().when(remoteStorageManager).deleteLogSegmentData(any(RemoteLogSegmentMetadata.class));

        // RUN 1
        RemoteLogManager.RLMExpirationTask task = remoteLogManager.new RLMExpirationTask(leaderTopicIdPartition);
        task.cleanupExpiredRemoteLogSegments();
        verify(remoteStorageManager, times(2)).deleteLogSegmentData(any(RemoteLogSegmentMetadata.class));
        verify(remoteStorageManager).deleteLogSegmentData(s1);
        // make sure the s2 segment with "DELETE_SEGMENT_FINISHED" state is not invoking "deleteLogSegmentData"
        verify(remoteStorageManager, never()).deleteLogSegmentData(s2);
        verify(remoteStorageManager).deleteLogSegmentData(s3);

        clearInvocations(remoteStorageManager);

        // RUN 2
        // update the metadata list to remove deleted s1, s3, and set the state in s4 to COPY_SEGMENT_FINISHED
        List<RemoteLogSegmentMetadata> updatedMetadataList = new LinkedList<>();
        updatedMetadataList.addAll(Arrays.asList(s2, s4CopyFinished));
        updatedMetadataList.addAll(s5);
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition))
                .thenReturn(updatedMetadataList.iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 0))
                .thenAnswer(ans -> updatedMetadataList.iterator());

        doNothing().when(remoteStorageManager).deleteLogSegmentData(any(RemoteLogSegmentMetadata.class));
        task.cleanupExpiredRemoteLogSegments();

        // make sure 2 segments got deleted
        verify(remoteStorageManager, times(2)).deleteLogSegmentData(any(RemoteLogSegmentMetadata.class));
        verify(remoteStorageManager).deleteLogSegmentData(s4CopyFinished);
        verify(remoteStorageManager).deleteLogSegmentData(s5.get(0));
    }

    @Test
    public void testDeleteRetentionMsBeingCancelledBeforeSecondDelete() throws RemoteStorageException, ExecutionException, InterruptedException {
        RemoteLogManager.RLMExpirationTask leaderTask = remoteLogManager.new RLMExpirationTask(leaderTopicIdPartition);

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());
        when(mockLog.logEndOffset()).thenReturn(200L);

        List<EpochEntry> epochEntries = Collections.singletonList(epochEntry0);

        List<RemoteLogSegmentMetadata> metadataList =
                listRemoteLogSegmentMetadata(leaderTopicIdPartition, 2, 100, 1024, epochEntries, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition))
                .thenReturn(metadataList.iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 0))
                .thenAnswer(ans -> metadataList.iterator());

        checkpoint.write(epochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        Map<String, Long> logProps = new HashMap<>();
        logProps.put("retention.bytes", -1L);
        logProps.put("retention.ms", 0L);
        LogConfig mockLogConfig = new LogConfig(logProps);
        when(mockLog.config()).thenReturn(mockLogConfig);

        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class)))
                .thenAnswer(answer -> {
                    // cancel the task so that we don't delete the second segment
                    leaderTask.cancel();
                    return CompletableFuture.runAsync(() -> {
                    });
                });

        leaderTask.cleanupExpiredRemoteLogSegments();

        assertEquals(200L, currentLogStartOffset.get());
        verify(remoteStorageManager).deleteLogSegmentData(metadataList.get(0));
        verify(remoteStorageManager, never()).deleteLogSegmentData(metadataList.get(1));

        // test that the 2nd log segment will be deleted by the new leader
        RemoteLogManager.RLMExpirationTask newLeaderTask = remoteLogManager.new RLMExpirationTask(followerTopicIdPartition);

        Iterator<RemoteLogSegmentMetadata> firstIterator = metadataList.iterator();
        firstIterator.next();
        Iterator<RemoteLogSegmentMetadata> secondIterator = metadataList.iterator();
        secondIterator.next();
        Iterator<RemoteLogSegmentMetadata> thirdIterator = metadataList.iterator();
        thirdIterator.next();

        when(remoteLogMetadataManager.listRemoteLogSegments(followerTopicIdPartition))
                .thenReturn(firstIterator);
        when(remoteLogMetadataManager.listRemoteLogSegments(followerTopicIdPartition, 0))
                .thenReturn(secondIterator)
                .thenReturn(thirdIterator);

        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class)))
                .thenAnswer(answer -> CompletableFuture.runAsync(() -> { }));

        newLeaderTask.cleanupExpiredRemoteLogSegments();

        assertEquals(200L, currentLogStartOffset.get());
        verify(remoteStorageManager).deleteLogSegmentData(metadataList.get(0));
        verify(remoteStorageManager).deleteLogSegmentData(metadataList.get(1));
    }

    @Test
    public void testDeleteRetentionMsBiggerThanTimeMs() throws RemoteStorageException, ExecutionException, InterruptedException {
        // add 1 month to the current time to avoid flaky test
        LogConfig mockLogConfig = new LogConfig(Map.of("retention.ms", time.milliseconds() + 24 * 30 * 60 * 60 * 1000L));
        when(mockLog.config()).thenReturn(mockLogConfig);

        RemoteLogManager.RLMExpirationTask leaderTask = remoteLogManager.new RLMExpirationTask(leaderTopicIdPartition);

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());
        when(mockLog.logEndOffset()).thenReturn(200L);

        List<EpochEntry> epochEntries = Collections.singletonList(epochEntry0);

        List<RemoteLogSegmentMetadata> metadataList =
            listRemoteLogSegmentMetadata(leaderTopicIdPartition, 2, 100, 1024, epochEntries, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition))
            .thenReturn(metadataList.iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 0))
            .thenAnswer(ans -> metadataList.iterator());

        checkpoint.write(epochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        assertDoesNotThrow(leaderTask::cleanupExpiredRemoteLogSegments);

        verify(remoteStorageManager, never()).deleteLogSegmentData(any());
    }

    @ParameterizedTest(name = "testFailedDeleteExpiredSegments retentionSize={0} retentionMs={1}")
    @CsvSource(value = {"0, -1", "-1, 0"})
    public void testFailedDeleteExpiredSegments(long retentionSize,
                                                long retentionMs) throws RemoteStorageException, ExecutionException, InterruptedException {
        Map<String, Long> logProps = new HashMap<>();
        logProps.put("retention.bytes", retentionSize);
        logProps.put("retention.ms", retentionMs);
        LogConfig mockLogConfig = new LogConfig(logProps);
        when(mockLog.config()).thenReturn(mockLogConfig);

        List<EpochEntry> epochEntries = Collections.singletonList(epochEntry0);
        checkpoint.write(epochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());
        when(mockLog.logEndOffset()).thenReturn(200L);

        List<RemoteLogSegmentMetadata> metadataList =
                listRemoteLogSegmentMetadata(leaderTopicIdPartition, 1, 100, 1024, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition))
                .thenReturn(metadataList.iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 0))
                .thenAnswer(ans -> metadataList.iterator());
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class)))
                .thenReturn(CompletableFuture.runAsync(() -> { }));

        // Verify the metrics for remote deletes and for failures is zero before attempt to delete segments
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteDeleteRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteDeleteRequestRate().count());
        // Verify aggregate metrics
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteDeleteRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteDeleteRequestRate().count());

        RemoteLogManager.RLMExpirationTask task = remoteLogManager.new RLMExpirationTask(leaderTopicIdPartition);
        doThrow(new RemoteStorageException("Failed to delete segment")).when(remoteStorageManager).deleteLogSegmentData(any());
        assertThrows(RemoteStorageException.class, task::cleanupExpiredRemoteLogSegments);

        assertEquals(100L, currentLogStartOffset.get());
        verify(remoteStorageManager).deleteLogSegmentData(metadataList.get(0));

        // Verify the metric for remote delete is updated correctly
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteDeleteRequestRate().count());
        // Verify we reported 1 failure for remote deletes
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteDeleteRequestRate().count());
        // Verify aggregate metrics
        assertEquals(1, brokerTopicStats.allTopicsStats().remoteDeleteRequestRate().count());
        assertEquals(1, brokerTopicStats.allTopicsStats().failedRemoteDeleteRequestRate().count());

        // make sure we'll retry the deletion in next run
        doNothing().when(remoteStorageManager).deleteLogSegmentData(any());
        task.cleanupExpiredRemoteLogSegments();
        verify(remoteStorageManager).deleteLogSegmentData(metadataList.get(0));
    }

    @ParameterizedTest(name = "testDeleteLogSegmentDueToRetentionSizeBreach segmentCount={0} deletableSegmentCount={1}")
    @CsvSource(value = {"50, 0", "50, 1", "50, 23", "50, 50"})
    public void testDeleteLogSegmentDueToRetentionSizeBreach(int segmentCount,
                                                             int deletableSegmentCount)
            throws RemoteStorageException, ExecutionException, InterruptedException {
        int recordsPerSegment = 100;
        int segmentSize = 1024;
        List<EpochEntry> epochEntries = Arrays.asList(
                new EpochEntry(0, 0L),
                new EpochEntry(1, 20L),
                new EpochEntry(3, 50L),
                new EpochEntry(4, 100L)
        );
        checkpoint.write(epochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        int currentLeaderEpoch = epochEntries.get(epochEntries.size() - 1).epoch;

        long localLogSegmentsSize = 512L;
        long retentionSize = ((long) segmentCount - deletableSegmentCount) * segmentSize + localLogSegmentsSize;
        Map<String, Long> logProps = new HashMap<>();
        logProps.put("retention.bytes", retentionSize);
        logProps.put("retention.ms", -1L);
        LogConfig mockLogConfig = new LogConfig(logProps);
        when(mockLog.config()).thenReturn(mockLogConfig);
        when(mockLog.topicPartition()).thenReturn(tp);

        long localLogStartOffset = (long) segmentCount * recordsPerSegment;
        long logEndOffset = ((long) segmentCount * recordsPerSegment) + 1;
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(mockLog.localLogStartOffset()).thenReturn(localLogStartOffset);
        when(mockLog.logEndOffset()).thenReturn(logEndOffset);
        when(mockLog.onlyLocalLogSegmentsSize()).thenReturn(localLogSegmentsSize);

        List<RemoteLogSegmentMetadata> segmentMetadataList = listRemoteLogSegmentMetadata(
                leaderTopicIdPartition, segmentCount, recordsPerSegment, segmentSize, epochEntries, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        verifyDeleteLogSegment(segmentMetadataList, deletableSegmentCount, currentLeaderEpoch);
    }

    @ParameterizedTest(name = "testDeleteLogSegmentDueToRetentionTimeBreach segmentCount={0} deletableSegmentCount={1}")
    @CsvSource(value = {"50, 0", "50, 1", "50, 23", "50, 50"})
    public void testDeleteLogSegmentDueToRetentionTimeBreach(int segmentCount,
                                                             int deletableSegmentCount)
            throws RemoteStorageException, ExecutionException, InterruptedException {
        int recordsPerSegment = 100;
        int segmentSize = 1024;
        List<EpochEntry> epochEntries = Arrays.asList(
                new EpochEntry(0, 0L),
                new EpochEntry(1, 20L),
                new EpochEntry(3, 50L),
                new EpochEntry(4, 100L)
        );
        checkpoint.write(epochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        int currentLeaderEpoch = epochEntries.get(epochEntries.size() - 1).epoch;

        long localLogSegmentsSize = 512L;
        long retentionSize = -1L;
        Map<String, Long> logProps = new HashMap<>();
        logProps.put("retention.bytes", retentionSize);
        logProps.put("retention.ms", 1L);
        LogConfig mockLogConfig = new LogConfig(logProps);
        when(mockLog.config()).thenReturn(mockLogConfig);
        when(mockLog.topicPartition()).thenReturn(tp);

        long localLogStartOffset = (long) segmentCount * recordsPerSegment;
        long logEndOffset = ((long) segmentCount * recordsPerSegment) + 1;
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(mockLog.localLogStartOffset()).thenReturn(localLogStartOffset);
        when(mockLog.logEndOffset()).thenReturn(logEndOffset);
        when(mockLog.onlyLocalLogSegmentsSize()).thenReturn(localLogSegmentsSize);

        List<RemoteLogSegmentMetadata> segmentMetadataList = listRemoteLogSegmentMetadataByTime(
                leaderTopicIdPartition, segmentCount, deletableSegmentCount, recordsPerSegment, segmentSize, epochEntries, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        verifyDeleteLogSegment(segmentMetadataList, deletableSegmentCount, currentLeaderEpoch);
    }

    private void verifyRemoteDeleteMetrics(long remoteDeleteLagBytes, long remoteDeleteLagSegments) {
        assertEquals(remoteDeleteLagBytes, safeLongYammerMetricValue("RemoteDeleteLagBytes"),
                String.format("Expected to find %d for RemoteDeleteLagBytes metric value, but found %d",
                        remoteDeleteLagBytes, safeLongYammerMetricValue("RemoteDeleteLagBytes")));
        assertEquals(remoteDeleteLagSegments, safeLongYammerMetricValue("RemoteDeleteLagSegments"),
                String.format("Expected to find %d for RemoteDeleteLagSegments metric value, but found %d",
                        remoteDeleteLagSegments, safeLongYammerMetricValue("RemoteDeleteLagSegments")));
        assertEquals(remoteDeleteLagBytes, safeLongYammerMetricValue("RemoteDeleteLagBytes,topic=" + leaderTopic),
                String.format("Expected to find %d for RemoteDeleteLagBytes for 'Leader' topic metric value, but found %d",
                        remoteDeleteLagBytes, safeLongYammerMetricValue("RemoteDeleteLagBytes,topic=" + leaderTopic)));
        assertEquals(remoteDeleteLagSegments, safeLongYammerMetricValue("RemoteDeleteLagSegments,topic=" + leaderTopic),
                String.format("Expected to find %d for RemoteDeleteLagSegments for 'Leader' topic metric value, but found %d",
                        remoteDeleteLagSegments, safeLongYammerMetricValue("RemoteDeleteLagSegments,topic=" + leaderTopic)));
    }

    private void verifyDeleteLogSegment(List<RemoteLogSegmentMetadata> segmentMetadataList,
                                        int deletableSegmentCount,
                                        int currentLeaderEpoch)
            throws RemoteStorageException, ExecutionException, InterruptedException {
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition))
                .thenReturn(segmentMetadataList.iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(eq(leaderTopicIdPartition), anyInt()))
                .thenAnswer(invocation -> {
                    int leaderEpoch = invocation.getArgument(1);
                    return segmentMetadataList.stream()
                            .filter(segmentMetadata -> segmentMetadata.segmentLeaderEpochs().containsKey(leaderEpoch))
                            .iterator();
                });
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class)))
                .thenAnswer(answer -> CompletableFuture.runAsync(() -> { }));
        RemoteLogManager.RLMExpirationTask task = remoteLogManager.new RLMExpirationTask(leaderTopicIdPartition);
        task.cleanupExpiredRemoteLogSegments();

        ArgumentCaptor<RemoteLogSegmentMetadata> deletedMetadataCapture = ArgumentCaptor.forClass(RemoteLogSegmentMetadata.class);
        verify(remoteStorageManager, times(deletableSegmentCount)).deleteLogSegmentData(deletedMetadataCapture.capture());
        if (deletableSegmentCount > 0) {
            List<RemoteLogSegmentMetadata> deletedMetadataList = deletedMetadataCapture.getAllValues();
            RemoteLogSegmentMetadata expectedEndMetadata = segmentMetadataList.get(deletableSegmentCount - 1);
            assertEquals(segmentMetadataList.get(0), deletedMetadataList.get(0));
            assertEquals(expectedEndMetadata, deletedMetadataList.get(deletedMetadataList.size() - 1));
            assertEquals(currentLogStartOffset.get(), expectedEndMetadata.endOffset() + 1);
        }
    }


    @Test
    public void testDeleteRetentionMsOnExpiredSegment() throws RemoteStorageException, IOException {
        AtomicLong logStartOffset = new AtomicLong(0);
        try (RemoteLogManager remoteLogManager = new RemoteLogManager(config.remoteLogManagerConfig(), brokerId, logDir, clusterId, time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> logStartOffset.set(offset),
                brokerTopicStats, metrics) {
            public RemoteStorageManager createRemoteStorageManager() {
                return remoteStorageManager;
            }
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }
        }) {
            RemoteLogManager.RLMExpirationTask task = remoteLogManager.new RLMExpirationTask(leaderTopicIdPartition);

            when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());
            when(mockLog.logEndOffset()).thenReturn(200L);

            List<EpochEntry> epochEntries = Collections.singletonList(epochEntry0);

            List<RemoteLogSegmentMetadata> remoteLogSegmentMetadatas = listRemoteLogSegmentMetadata(
                    leaderTopicIdPartition, 2, 100, 1024, epochEntries, RemoteLogSegmentState.DELETE_SEGMENT_FINISHED);

            when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition))
                    .thenReturn(remoteLogSegmentMetadatas.iterator());
            when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 0))
                    .thenReturn(remoteLogSegmentMetadatas.iterator())
                    .thenReturn(remoteLogSegmentMetadatas.iterator());

            checkpoint.write(epochEntries);
            LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
            when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

            Map<String, Long> logProps = new HashMap<>();
            logProps.put("retention.bytes", -1L);
            logProps.put("retention.ms", 0L);
            LogConfig mockLogConfig = new LogConfig(logProps);
            when(mockLog.config()).thenReturn(mockLogConfig);

            when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class)))
                    .thenAnswer(answer -> CompletableFuture.runAsync(() -> { }));

            task.cleanupExpiredRemoteLogSegments();

            verifyNoMoreInteractions(remoteStorageManager);
            assertEquals(0L, logStartOffset.get());
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private List<RemoteLogSegmentMetadata> listRemoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                        int segmentCount,
                                                                        int recordsPerSegment,
                                                                        int segmentSize,
                                                                        RemoteLogSegmentState state) {
        return listRemoteLogSegmentMetadata(topicIdPartition, segmentCount, recordsPerSegment, segmentSize, Collections.emptyList(), state);
    }

    private List<RemoteLogSegmentMetadata> listRemoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                        int segmentCount,
                                                                        int recordsPerSegment,
                                                                        int segmentSize,
                                                                        List<EpochEntry> epochEntries,
                                                                        RemoteLogSegmentState state) {
        return listRemoteLogSegmentMetadataByTime(
                topicIdPartition, segmentCount, 0, recordsPerSegment, segmentSize, epochEntries, state);
    }

    private List<RemoteLogSegmentMetadata> listRemoteLogSegmentMetadataByTime(TopicIdPartition topicIdPartition,
                                                                              int segmentCount,
                                                                              int deletableSegmentCount,
                                                                              int recordsPerSegment,
                                                                              int segmentSize,
                                                                              List<EpochEntry> epochEntries,
                                                                              RemoteLogSegmentState state) {
        List<RemoteLogSegmentMetadata> segmentMetadataList = new ArrayList<>();
        for (int idx = 0; idx < segmentCount; idx++) {
            long timestamp = time.milliseconds();
            if (idx < deletableSegmentCount) {
                timestamp = time.milliseconds() - 1;
            }
            long startOffset = (long) idx * recordsPerSegment;
            long endOffset = startOffset + recordsPerSegment - 1;
            List<EpochEntry> localTotalEpochEntries = epochEntries.isEmpty() ? totalEpochEntries : epochEntries;
            Map<Integer, Long> segmentLeaderEpochs = truncateAndGetLeaderEpochs(localTotalEpochEntries, startOffset, endOffset);
            RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(
                    new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
                    startOffset,
                    endOffset,
                    timestamp,
                    brokerId,
                    timestamp,
                    segmentSize,
                    Optional.empty(),
                    state,
                    segmentLeaderEpochs
            );
            segmentMetadataList.add(metadata);
        }
        return segmentMetadataList;
    }

    private RemoteLogSegmentMetadata createRemoteLogSegmentMetadata(RemoteLogSegmentId segmentID,
                                                                    long startOffset,
                                                                    long endOffset,
                                                                    int segmentSize,
                                                                    List<EpochEntry> epochEntries,
                                                                    RemoteLogSegmentState state) {
        return new RemoteLogSegmentMetadata(
                segmentID,
                startOffset,
                endOffset,
                time.milliseconds(),
                brokerId,
                time.milliseconds(),
                segmentSize,
                Optional.empty(),
                state,
                truncateAndGetLeaderEpochs(epochEntries, startOffset, endOffset));
    }

    private Map<Integer, Long> truncateAndGetLeaderEpochs(List<EpochEntry> entries,
                                                          Long startOffset,
                                                          Long endOffset) {
        LeaderEpochCheckpointFile myCheckpoint;
        try {
            myCheckpoint = new LeaderEpochCheckpointFile(
                    TestUtils.tempFile(), new LogDirFailureChannel(1));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        myCheckpoint.write(entries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(null, myCheckpoint, scheduler);
        cache.truncateFromStartAsyncFlush(startOffset);
        cache.truncateFromEndAsyncFlush(endOffset);
        return myCheckpoint.read().stream().collect(Collectors.toMap(e -> e.epoch, e -> e.startOffset));
    }

    @Test
    public void testReadForMissingFirstBatchInRemote() throws RemoteStorageException, IOException {
        FileInputStream fileInputStream = mock(FileInputStream.class);
        ClassLoaderAwareRemoteStorageManager rsmManager = mock(ClassLoaderAwareRemoteStorageManager.class);
        RemoteLogSegmentMetadata segmentMetadata = mock(RemoteLogSegmentMetadata.class);
        LeaderEpochFileCache cache = mock(LeaderEpochFileCache.class);
        when(cache.epochForOffset(anyLong())).thenReturn(OptionalInt.of(1));

        when(remoteStorageManager.fetchLogSegment(any(RemoteLogSegmentMetadata.class), anyInt()))
                .thenAnswer(a -> fileInputStream);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        int fetchOffset = 0;
        int fetchMaxBytes = 10;

        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                Uuid.randomUuid(), fetchOffset, 0, fetchMaxBytes, Optional.empty()
        );

        RemoteStorageFetchInfo fetchInfo = new RemoteStorageFetchInfo(
                0, false, tp, partitionData, FetchIsolation.TXN_COMMITTED, false
        );

        try (RemoteLogManager remoteLogManager = new RemoteLogManager(
                config.remoteLogManagerConfig(),
                brokerId,
                logDir,
                clusterId,
                time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> { },
                brokerTopicStats,
                metrics) {
            public RemoteStorageManager createRemoteStorageManager() {
                return rsmManager;
            }
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }

            public Optional<RemoteLogSegmentMetadata> fetchRemoteLogSegmentMetadata(TopicPartition topicPartition,
                                                                                    int epochForOffset, long offset) {
                return Optional.of(segmentMetadata);
            }

            public Optional<RemoteLogSegmentMetadata> findNextSegmentMetadata(RemoteLogSegmentMetadata segmentMetadata,
                                                                              Option<LeaderEpochFileCache> leaderEpochFileCacheOption) {
                return Optional.empty();
            }

            int lookupPositionForOffset(RemoteLogSegmentMetadata remoteLogSegmentMetadata, long offset) {
                return 1;
            }

            // This is the key scenario that we are testing here
            EnrichedRecordBatch findFirstBatch(RemoteLogInputStream remoteLogInputStream, long offset) {
                return new EnrichedRecordBatch(null, 0);
            }
        }) {
            FetchDataInfo fetchDataInfo = remoteLogManager.read(fetchInfo);
            assertEquals(fetchOffset, fetchDataInfo.fetchOffsetMetadata.messageOffset);
            assertFalse(fetchDataInfo.firstEntryIncomplete);
            assertEquals(MemoryRecords.EMPTY, fetchDataInfo.records);
            // FetchIsolation is TXN_COMMITTED
            assertTrue(fetchDataInfo.abortedTransactions.isPresent());
            assertTrue(fetchDataInfo.abortedTransactions.get().isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReadForFirstBatchMoreThanMaxFetchBytes(boolean minOneMessage) throws RemoteStorageException, IOException {
        FileInputStream fileInputStream = mock(FileInputStream.class);
        ClassLoaderAwareRemoteStorageManager rsmManager = mock(ClassLoaderAwareRemoteStorageManager.class);
        RemoteLogSegmentMetadata segmentMetadata = mock(RemoteLogSegmentMetadata.class);
        LeaderEpochFileCache cache = mock(LeaderEpochFileCache.class);
        when(cache.epochForOffset(anyLong())).thenReturn(OptionalInt.of(1));

        when(remoteStorageManager.fetchLogSegment(any(RemoteLogSegmentMetadata.class), anyInt()))
                .thenAnswer(a -> fileInputStream);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        int fetchOffset = 0;
        int fetchMaxBytes = 10;
        int recordBatchSizeInBytes = fetchMaxBytes + 1;
        RecordBatch firstBatch = mock(RecordBatch.class);
        ArgumentCaptor<ByteBuffer> capture = ArgumentCaptor.forClass(ByteBuffer.class);

        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                Uuid.randomUuid(), fetchOffset, 0, fetchMaxBytes, Optional.empty()
        );

        RemoteStorageFetchInfo fetchInfo = new RemoteStorageFetchInfo(
                0, minOneMessage, tp, partitionData, FetchIsolation.HIGH_WATERMARK, false
        );

        try (RemoteLogManager remoteLogManager = new RemoteLogManager(
                config.remoteLogManagerConfig(),
                brokerId,
                logDir,
                clusterId,
                time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> { },
                brokerTopicStats,
                metrics) {
            public RemoteStorageManager createRemoteStorageManager() {
                return rsmManager;
            }
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }

            public Optional<RemoteLogSegmentMetadata> fetchRemoteLogSegmentMetadata(TopicPartition topicPartition,
                                                                                    int epochForOffset, long offset) {
                return Optional.of(segmentMetadata);
            }

            int lookupPositionForOffset(RemoteLogSegmentMetadata remoteLogSegmentMetadata, long offset) {
                return 1;
            }

            EnrichedRecordBatch findFirstBatch(RemoteLogInputStream remoteLogInputStream, long offset) {
                when(firstBatch.sizeInBytes()).thenReturn(recordBatchSizeInBytes);
                doNothing().when(firstBatch).writeTo(capture.capture());
                return new EnrichedRecordBatch(firstBatch, 0);
            }
        }) {
            FetchDataInfo fetchDataInfo = remoteLogManager.read(fetchInfo);
            // Common assertions
            assertEquals(fetchOffset, fetchDataInfo.fetchOffsetMetadata.messageOffset);
            assertFalse(fetchDataInfo.firstEntryIncomplete);
            // FetchIsolation is HIGH_WATERMARK
            assertEquals(Optional.empty(), fetchDataInfo.abortedTransactions);


            if (minOneMessage) {
                // Verify that the byte buffer has capacity equal to the size of the first batch
                assertEquals(recordBatchSizeInBytes, capture.getValue().capacity());
            } else {
                // Verify that the first batch is never written to the buffer
                verify(firstBatch, never()).writeTo(any(ByteBuffer.class));
                assertEquals(MemoryRecords.EMPTY, fetchDataInfo.records);
            }
        }
    }

    @Test
    public void testReadForFirstBatchInLogCompaction() throws RemoteStorageException, IOException {
        FileInputStream fileInputStream = mock(FileInputStream.class);
        RemoteLogInputStream remoteLogInputStream = mock(RemoteLogInputStream.class);
        ClassLoaderAwareRemoteStorageManager rsmManager = mock(ClassLoaderAwareRemoteStorageManager.class);
        RemoteLogSegmentMetadata segmentMetadata = mock(RemoteLogSegmentMetadata.class);
        LeaderEpochFileCache cache = mock(LeaderEpochFileCache.class);
        when(cache.epochForOffset(anyLong())).thenReturn(OptionalInt.of(1));
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));

        int fetchOffset = 0;
        int fetchMaxBytes = 10;
        int recordBatchSizeInBytes = fetchMaxBytes + 1;
        RecordBatch firstBatch = mock(RecordBatch.class);
        ArgumentCaptor<ByteBuffer> capture = ArgumentCaptor.forClass(ByteBuffer.class);

        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                Uuid.randomUuid(), fetchOffset, 0, fetchMaxBytes, Optional.empty()
        );

        when(rsmManager.fetchLogSegment(any(), anyInt())).thenReturn(fileInputStream);
        when(segmentMetadata.topicIdPartition()).thenReturn(new TopicIdPartition(Uuid.randomUuid(), tp));
        // Fetching first time  FirstBatch return null because of log compaction.
        // Fetching second time  FirstBatch return data.
        when(remoteLogInputStream.nextBatch()).thenReturn(null, firstBatch);
        // Return last offset greater than the requested offset.
        when(firstBatch.lastOffset()).thenReturn(2L);
        when(firstBatch.sizeInBytes()).thenReturn(recordBatchSizeInBytes);
        doNothing().when(firstBatch).writeTo(capture.capture());
        RemoteStorageFetchInfo fetchInfo = new RemoteStorageFetchInfo(
                0, true, tp, partitionData, FetchIsolation.HIGH_WATERMARK, false
        );


        try (RemoteLogManager remoteLogManager = new RemoteLogManager(
                config.remoteLogManagerConfig(),
                brokerId,
                logDir,
                clusterId,
                time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> {
                },
                brokerTopicStats,
                metrics) {
            public RemoteStorageManager createRemoteStorageManager() {
                return rsmManager;
            }

            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }

            public Optional<RemoteLogSegmentMetadata> fetchRemoteLogSegmentMetadata(TopicPartition topicPartition,
                                                                                    int epochForOffset, long offset) {
                return Optional.of(segmentMetadata);
            }
            public RemoteLogInputStream getRemoteLogInputStream(InputStream in) {
                return remoteLogInputStream;
            }

            int lookupPositionForOffset(RemoteLogSegmentMetadata remoteLogSegmentMetadata, long offset) {
                return 1;
            }
        }) {
            FetchDataInfo fetchDataInfo = remoteLogManager.read(fetchInfo);
            // Common assertions
            assertEquals(fetchOffset, fetchDataInfo.fetchOffsetMetadata.messageOffset);
            assertFalse(fetchDataInfo.firstEntryIncomplete);
            // FetchIsolation is HIGH_WATERMARK
            assertEquals(Optional.empty(), fetchDataInfo.abortedTransactions);
            // Verify that the byte buffer has capacity equal to the size of the first batch
            assertEquals(recordBatchSizeInBytes, capture.getValue().capacity());

        }
    }

    @Test
    public void testCopyQuotaManagerConfig() {
        Properties defaultProps = new Properties();
        defaultProps.putAll(brokerConfig);
        appendRLMConfig(defaultProps);
        KafkaConfig defaultRlmConfig = KafkaConfig.fromProps(defaultProps);
        RLMQuotaManagerConfig defaultConfig = RemoteLogManager.copyQuotaManagerConfig(defaultRlmConfig.remoteLogManagerConfig());
        assertEquals(DEFAULT_REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND, defaultConfig.quotaBytesPerSecond());
        assertEquals(DEFAULT_REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_NUM, defaultConfig.numQuotaSamples());
        assertEquals(DEFAULT_REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_SIZE_SECONDS, defaultConfig.quotaWindowSizeSeconds());

        Properties customProps = new Properties();
        customProps.putAll(brokerConfig);
        customProps.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP, 100);
        customProps.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_NUM_PROP, 31);
        customProps.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_SIZE_SECONDS_PROP, 1);
        appendRLMConfig(customProps);
        KafkaConfig config = KafkaConfig.fromProps(customProps);

        RLMQuotaManagerConfig rlmCopyQuotaManagerConfig = RemoteLogManager.copyQuotaManagerConfig(config.remoteLogManagerConfig());
        assertEquals(100L, rlmCopyQuotaManagerConfig.quotaBytesPerSecond());
        assertEquals(31, rlmCopyQuotaManagerConfig.numQuotaSamples());
        assertEquals(1, rlmCopyQuotaManagerConfig.quotaWindowSizeSeconds());
    }

    @Test
    public void testFetchQuotaManagerConfig() {
        Properties defaultProps = new Properties();
        defaultProps.putAll(brokerConfig);
        appendRLMConfig(defaultProps);
        KafkaConfig defaultRlmConfig = KafkaConfig.fromProps(defaultProps);

        RLMQuotaManagerConfig defaultConfig = RemoteLogManager.fetchQuotaManagerConfig(defaultRlmConfig.remoteLogManagerConfig());
        assertEquals(DEFAULT_REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND, defaultConfig.quotaBytesPerSecond());
        assertEquals(DEFAULT_REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_NUM, defaultConfig.numQuotaSamples());
        assertEquals(DEFAULT_REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_SIZE_SECONDS, defaultConfig.quotaWindowSizeSeconds());

        Properties customProps = new Properties();
        customProps.putAll(brokerConfig);
        customProps.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP, 100);
        customProps.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_NUM_PROP, 31);
        customProps.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_SIZE_SECONDS_PROP, 1);
        appendRLMConfig(customProps);
        KafkaConfig rlmConfig = KafkaConfig.fromProps(customProps);
        RLMQuotaManagerConfig rlmFetchQuotaManagerConfig = RemoteLogManager.fetchQuotaManagerConfig(rlmConfig.remoteLogManagerConfig());
        assertEquals(100L, rlmFetchQuotaManagerConfig.quotaBytesPerSecond());
        assertEquals(31, rlmFetchQuotaManagerConfig.numQuotaSamples());
        assertEquals(1, rlmFetchQuotaManagerConfig.quotaWindowSizeSeconds());
    }

    @Test
    public void testEpochEntriesAsByteBuffer() throws Exception {
        int expectedEpoch = 0;
        long expectedStartOffset = 1L;
        int expectedVersion = 0;
        List<EpochEntry> epochs = Arrays.asList(new EpochEntry(expectedEpoch, expectedStartOffset));
        ByteBuffer buffer = RemoteLogManager.epochEntriesAsByteBuffer(epochs);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer.array()), StandardCharsets.UTF_8));

        assertEquals(String.valueOf(expectedVersion), bufferedReader.readLine());
        assertEquals(String.valueOf(epochs.size()), bufferedReader.readLine());
        assertEquals(expectedEpoch + " " + expectedStartOffset, bufferedReader.readLine());
    }


    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCopyQuota(boolean quotaExceeded) throws Exception {
        RemoteLogManager.RLMCopyTask task = setupRLMTask(quotaExceeded);

        if (quotaExceeded) {
            // Verify that the copy operation times out, since no segments can be copied due to quota being exceeded
            assertThrows(AssertionFailedError.class, () -> assertTimeoutPreemptively(Duration.ofMillis(200), () -> task.copyLogSegmentsToRemote(mockLog)));

            Map<org.apache.kafka.common.MetricName, KafkaMetric> allMetrics = metrics.metrics();
            KafkaMetric avgMetric = allMetrics.get(metrics.metricName("remote-copy-throttle-time-avg", "RemoteLogManager"));
            KafkaMetric maxMetric = allMetrics.get(metrics.metricName("remote-copy-throttle-time-max", "RemoteLogManager"));
            assertEquals(quotaExceededThrottleTime, ((Double) avgMetric.metricValue()).longValue());
            assertEquals(quotaExceededThrottleTime, ((Double) maxMetric.metricValue()).longValue());

            // Verify the highest offset in remote storage is updated only once
            ArgumentCaptor<Long> capture = ArgumentCaptor.forClass(Long.class);
            verify(mockLog, times(1)).updateHighestOffsetInRemoteStorage(capture.capture());
            // Verify the highest offset in remote storage was -1L before the copy started
            assertEquals(-1L, capture.getValue());
        } else {
            // Verify the copy operation completes within the timeout, since it does not need to wait for quota availability
            assertTimeoutPreemptively(Duration.ofMillis(100), () -> task.copyLogSegmentsToRemote(mockLog));

            // Verify quota check was performed
            verify(rlmCopyQuotaManager, times(1)).getThrottleTimeMs();
            // Verify bytes to copy was recorded with the quota manager
            verify(rlmCopyQuotaManager, times(1)).record(10);

            Map<org.apache.kafka.common.MetricName, KafkaMetric> allMetrics = metrics.metrics();
            KafkaMetric avgMetric = allMetrics.get(metrics.metricName("remote-copy-throttle-time-avg", "RemoteLogManager"));
            KafkaMetric maxMetric = allMetrics.get(metrics.metricName("remote-copy-throttle-time-max", "RemoteLogManager"));
            assertEquals(Double.NaN, avgMetric.metricValue());
            assertEquals(Double.NaN, maxMetric.metricValue());

            // Verify the highest offset in remote storage is updated
            ArgumentCaptor<Long> capture = ArgumentCaptor.forClass(Long.class);
            verify(mockLog, times(2)).updateHighestOffsetInRemoteStorage(capture.capture());
            List<Long> capturedValues = capture.getAllValues();
            // Verify the highest offset in remote storage was -1L before the copy
            assertEquals(-1L, capturedValues.get(0).longValue());
            // Verify it was updated to 149L after the copy
            assertEquals(149L, capturedValues.get(1).longValue());
        }
    }

    @Test
    public void testRLMShutdownDuringQuotaExceededScenario() throws Exception {
        remoteLogManager.startup();
        setupRLMTask(true);
        remoteLogManager.onLeadershipChange(
            Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.emptySet(), topicIds);
        // Ensure the copy operation is waiting for quota to be available
        TestUtils.waitForCondition(() -> {
            verify(rlmCopyQuotaManager, atLeast(1)).getThrottleTimeMs();
            return true;
        }, "Quota exceeded check did not happen");
        // Verify RLM is able to shut down
        assertTimeoutPreemptively(Duration.ofMillis(100), () -> remoteLogManager.close());
    }

    // helper method to set up a RemoteLogManager.RLMTask for testing copy quota behaviour
    private RemoteLogManager.RLMCopyTask setupRLMTask(boolean quotaExceeded) throws RemoteStorageException, IOException {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(mockLog.parentDir()).thenReturn("dir1");
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt())).thenReturn(Optional.of(0L));

        // create 2 log segments, with 0 and 150 as log start offset
        LogSegment oldSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        File tempFile = TestUtils.tempFile();
        FileRecords fileRecords = mock(FileRecords.class);
        when(fileRecords.file()).thenReturn(tempFile);
        when(fileRecords.sizeInBytes()).thenReturn(10);

        // Set up the segment that is eligible for copy
        when(oldSegment.log()).thenReturn(fileRecords);
        when(oldSegment.baseOffset()).thenReturn(oldSegmentStartOffset);
        when(oldSegment.readNextOffset()).thenReturn(nextSegmentStartOffset);

        // set up the active segment
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(CollectionConverters.asScala(Arrays.asList(oldSegment, activeSegment)));

        File mockProducerSnapshotIndex = TestUtils.tempFile();
        ProducerStateManager mockStateManager = mock(ProducerStateManager.class);
        when(mockStateManager.fetchSnapshot(anyLong())).thenReturn(Optional.of(mockProducerSnapshotIndex));

        when(mockLog.producerStateManager()).thenReturn(mockStateManager);
        when(mockLog.lastStableOffset()).thenReturn(250L);

        File tempDir = TestUtils.tempDirectory();
        OffsetIndex idx = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1000).get();
        TimeIndex timeIdx = LazyIndex.forTime(LogFileUtils.timeIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1500).get();
        File txnFile = UnifiedLog.transactionIndexFile(tempDir, oldSegmentStartOffset, "");
        txnFile.createNewFile();
        TransactionIndex txnIndex = new TransactionIndex(oldSegmentStartOffset, txnFile);
        when(oldSegment.timeIndex()).thenReturn(timeIdx);
        when(oldSegment.offsetIndex()).thenReturn(idx);
        when(oldSegment.txnIndex()).thenReturn(txnIndex);

        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class))).thenReturn(dummyFuture);
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class))).thenReturn(dummyFuture);
        when(remoteStorageManager.copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class))).thenReturn(Optional.empty());

        when(rlmCopyQuotaManager.getThrottleTimeMs()).thenReturn(quotaExceeded ? 1000L : 0L);
        doNothing().when(rlmCopyQuotaManager).record(anyInt());

        return remoteLogManager.new RLMCopyTask(leaderTopicIdPartition, 128);
    }

    @Test
    public void testCopyThrottling() throws Exception {
        long oldestSegmentStartOffset = 0L;

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt())).thenReturn(Optional.of(0L));

        // create 3 log segments
        LogSegment segmentToCopy = mock(LogSegment.class);
        LogSegment segmentToThrottle = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        File tempFile = TestUtils.tempFile();
        FileRecords fileRecords = mock(FileRecords.class);
        when(fileRecords.file()).thenReturn(tempFile);
        when(fileRecords.sizeInBytes()).thenReturn(10);

        // set up the segment that will be copied
        when(segmentToCopy.log()).thenReturn(fileRecords);
        when(segmentToCopy.baseOffset()).thenReturn(oldestSegmentStartOffset);
        when(segmentToCopy.readNextOffset()).thenReturn(100L);

        // set up the segment that will not be copied because of hitting quota
        when(segmentToThrottle.log()).thenReturn(fileRecords);
        when(segmentToThrottle.baseOffset()).thenReturn(100L);
        when(segmentToThrottle.readNextOffset()).thenReturn(150L);

        // set up the active segment
        when(activeSegment.log()).thenReturn(fileRecords);
        when(activeSegment.baseOffset()).thenReturn(150L);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldestSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(CollectionConverters.asScala(Arrays.asList(segmentToCopy, segmentToThrottle, activeSegment)));

        File mockProducerSnapshotIndex = TestUtils.tempFile();
        ProducerStateManager mockStateManager = mock(ProducerStateManager.class);
        when(mockStateManager.fetchSnapshot(anyLong())).thenReturn(Optional.of(mockProducerSnapshotIndex));

        when(mockLog.producerStateManager()).thenReturn(mockStateManager);
        when(mockLog.lastStableOffset()).thenReturn(250L);

        File tempDir = TestUtils.tempDirectory();
        OffsetIndex idx = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(tempDir, oldestSegmentStartOffset, ""), oldestSegmentStartOffset, 1000).get();
        TimeIndex timeIdx = LazyIndex.forTime(LogFileUtils.timeIndexFile(tempDir, oldestSegmentStartOffset, ""), oldestSegmentStartOffset, 1500).get();
        File txnFile = UnifiedLog.transactionIndexFile(tempDir, oldestSegmentStartOffset, "");
        txnFile.createNewFile();
        TransactionIndex txnIndex = new TransactionIndex(oldestSegmentStartOffset, txnFile);
        when(segmentToCopy.timeIndex()).thenReturn(timeIdx);
        when(segmentToCopy.offsetIndex()).thenReturn(idx);
        when(segmentToCopy.txnIndex()).thenReturn(txnIndex);

        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class))).thenReturn(dummyFuture);
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class))).thenReturn(dummyFuture);
        when(remoteStorageManager.copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class))).thenReturn(Optional.empty());

        // After the first call, getThrottleTimeMs should return non-zero throttle time
        when(rlmCopyQuotaManager.getThrottleTimeMs()).thenReturn(0L, 1000L);
        doNothing().when(rlmCopyQuotaManager).record(anyInt());

        RemoteLogManager.RLMCopyTask task = remoteLogManager.new RLMCopyTask(leaderTopicIdPartition, 128);

        // Verify that the copy operation times out, since the second segment cannot be copied due to quota being exceeded
        assertThrows(AssertionFailedError.class, () -> assertTimeoutPreemptively(Duration.ofMillis(200), () -> task.copyLogSegmentsToRemote(mockLog)));

        // Verify the highest offset in remote storage is updated corresponding to the only segment that was copied
        ArgumentCaptor<Long> capture = ArgumentCaptor.forClass(Long.class);
        verify(mockLog, times(2)).updateHighestOffsetInRemoteStorage(capture.capture());
        List<Long> capturedValues = capture.getAllValues();
        // Verify the highest offset in remote storage was -1L before the copy
        assertEquals(-1L, capturedValues.get(0).longValue());
        // Verify it was updated to 99L after the copy
        assertEquals(99L, capturedValues.get(1).longValue());
    }

    @Test
    public void testTierLagResetsToZeroOnBecomingFollower() {
        remoteLogManager.startup();
        remoteLogManager.onLeadershipChange(
                Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.emptySet(), topicIds);
        RemoteLogManager.RLMCopyTask rlmTask = (RemoteLogManager.RLMCopyTask) remoteLogManager.rlmCopyTask(leaderTopicIdPartition);
        assertNotNull(rlmTask);
        rlmTask.recordLagStats(1024, 2);
        assertEquals(1024, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyLagBytes());
        assertEquals(2, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyLagSegments());
        // The same node becomes follower now which was the previous leader
        remoteLogManager.onLeadershipChange(Collections.emptySet(),
                Collections.singleton(mockPartition(leaderTopicIdPartition)), topicIds);
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyLagBytes());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyLagSegments());

        // If the old task emits the tier-lag stats, then it should be discarded
        rlmTask.recordLagStats(2048, 4);
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyLagBytes());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyLagSegments());
    }

    @Test
    public void testRemoteReadFetchDataInfo() throws RemoteStorageException, IOException {
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint, scheduler);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(remoteLogMetadataManager.remoteLogSegmentMetadata(eq(leaderTopicIdPartition), anyInt(), anyLong()))
                .thenAnswer(ans -> {
                    long offset = ans.getArgument(2);
                    RemoteLogSegmentId segmentId = new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid());
                    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata(segmentId,
                            offset - 10, offset + 99, 1024, totalEpochEntries, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
                    return Optional.of(segmentMetadata);
                });

        File segmentFile = tempFile();
        appendRecordsToFile(segmentFile, 100, 3);
        FileInputStream fileInputStream = new FileInputStream(segmentFile);
        when(remoteStorageManager.fetchLogSegment(any(RemoteLogSegmentMetadata.class), anyInt()))
                .thenReturn(fileInputStream);

        RemoteLogManager remoteLogManager = new RemoteLogManager(config.remoteLogManagerConfig(), brokerId, logDir, clusterId, time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> currentLogStartOffset.set(offset),
                brokerTopicStats, metrics) {
            public RemoteStorageManager createRemoteStorageManager() {
                return remoteStorageManager;
            }
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }
            int lookupPositionForOffset(RemoteLogSegmentMetadata remoteLogSegmentMetadata, long offset) {
                return 0;
            }
        };
        remoteLogManager.startup();
        remoteLogManager.onLeadershipChange(
                Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.emptySet(), topicIds);

        long fetchOffset = 10;
        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                Uuid.randomUuid(), fetchOffset, 0, 100, Optional.empty());
        RemoteStorageFetchInfo remoteStorageFetchInfo = new RemoteStorageFetchInfo(
                1048576, true, leaderTopicIdPartition.topicPartition(),
                partitionData, FetchIsolation.HIGH_WATERMARK, false);
        FetchDataInfo fetchDataInfo = remoteLogManager.read(remoteStorageFetchInfo);
        // firstBatch baseOffset may not be equal to the fetchOffset
        assertEquals(9, fetchDataInfo.fetchOffsetMetadata.messageOffset);
        assertEquals(273, fetchDataInfo.fetchOffsetMetadata.relativePositionInSegment);
    }

    @Test
    public void testRLMOpsWhenMetadataIsNotReady() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        when(remoteLogMetadataManager.isReady(any(TopicIdPartition.class)))
                .thenAnswer(ans -> {
                    latch.countDown();
                    return false;
                });
        remoteLogManager.startup();
        remoteLogManager.onLeadershipChange(
                Collections.singleton(mockPartition(leaderTopicIdPartition)),
                Collections.singleton(mockPartition(followerTopicIdPartition)),
                topicIds
        );
        assertNotNull(remoteLogManager.rlmCopyTask(leaderTopicIdPartition));
        assertNotNull(remoteLogManager.leaderExpirationTask(leaderTopicIdPartition));
        assertNotNull(remoteLogManager.followerTask(followerTopicIdPartition));

        // Once the partitions are assigned to the broker either as leader (or) follower in RLM#onLeadershipChange,
        // then it should have called the `isReady` method for each of the partitions. Otherwise, the test will fail.
        latch.await(5, TimeUnit.SECONDS);
        verify(remoteLogMetadataManager).configure(anyMap());
        verify(remoteLogMetadataManager).onPartitionLeadershipChanges(anySet(), anySet());
        verify(remoteLogMetadataManager, atLeastOnce()).isReady(eq(leaderTopicIdPartition));
        verify(remoteLogMetadataManager, atLeastOnce()).isReady(eq(followerTopicIdPartition));
        verifyNoMoreInteractions(remoteLogMetadataManager);
        verify(remoteStorageManager).configure(anyMap());
        verifyNoMoreInteractions(remoteStorageManager);
    }

    private void appendRecordsToFile(File file, int nRecords, int nRecordsPerBatch) throws IOException {
        byte magic = RecordBatch.CURRENT_MAGIC_VALUE;
        Compression compression = Compression.NONE;
        long offset = 0;
        List<SimpleRecord> records = new ArrayList<>();
        try (FileRecords fileRecords = FileRecords.open(file)) {
            for (long counter = 1; counter < nRecords + 1; counter++) {
                records.add(new SimpleRecord("foo".getBytes()));
                if (counter % nRecordsPerBatch == 0) {
                    fileRecords.append(MemoryRecords.withRecords(magic, offset, compression, CREATE_TIME,
                            records.toArray(new SimpleRecord[0])));
                    offset += records.size();
                    records.clear();
                }
            }
            fileRecords.flush();
        }
    }

    private Partition mockPartition(TopicIdPartition topicIdPartition) {
        TopicPartition tp = topicIdPartition.topicPartition();
        Partition partition = mock(Partition.class);
        UnifiedLog log = mock(UnifiedLog.class);
        when(partition.topicPartition()).thenReturn(tp);
        when(partition.topic()).thenReturn(tp.topic());
        when(log.remoteLogEnabled()).thenReturn(true);
        when(partition.log()).thenReturn(Option.apply(log));
        when(log.config()).thenReturn(new LogConfig(new Properties()));
        return partition;
    }

    private void appendRLMConfig(Properties props) {
        props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true);
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, NoOpRemoteStorageManager.class.getName());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, NoOpRemoteLogMetadataManager.class.getName());
        props.put(DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + remoteLogStorageTestProp, remoteLogStorageTestVal);
        // adding configs with "remote log metadata manager config prefix"
        props.put(DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX + REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP, remoteLogMetadataTopicPartitionsNum);
        props.put(DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX + remoteLogMetadataTestProp, remoteLogMetadataTestVal);
        props.put(DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX + remoteLogMetadataCommonClientTestProp, remoteLogMetadataCommonClientTestVal);
        props.put(DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX + remoteLogMetadataConsumerTestProp, remoteLogMetadataConsumerTestVal);
        props.put(DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX + remoteLogMetadataProducerTestProp, remoteLogMetadataProducerTestVal);
    }

}
