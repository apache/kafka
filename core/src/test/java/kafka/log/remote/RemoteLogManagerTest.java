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

import com.yammer.metrics.core.Gauge;
import kafka.cluster.EndPoint;
import kafka.cluster.Partition;
import kafka.log.UnifiedLog;
import kafka.server.BrokerTopicStats;
import kafka.server.KafkaConfig;
import kafka.server.StopPartition;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RemoteLogInputStream;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.OffsetAndEpoch;
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
import org.apache.kafka.storage.internals.checkpoint.InMemoryLeaderEpochCheckpoint;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpoint;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.FetchIsolation;
import org.apache.kafka.storage.internals.log.LazyIndex;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.OffsetIndex;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;
import org.apache.kafka.storage.internals.log.TimeIndex;
import org.apache.kafka.storage.internals.log.TransactionIndex;
import org.apache.kafka.test.TestUtils;
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
import scala.Option;
import scala.collection.JavaConverters;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_CONSUMER_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_PRODUCER_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics.REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics.REMOTE_STORAGE_THREAD_POOL_METRICS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
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

import static kafka.log.remote.RemoteLogManager.isRemoteSegmentWithinLeaderEpochs;

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

    private final RemoteStorageManager remoteStorageManager = mock(RemoteStorageManager.class);
    private final RemoteLogMetadataManager remoteLogMetadataManager = mock(RemoteLogMetadataManager.class);
    private RemoteLogManagerConfig remoteLogManagerConfig = null;

    private BrokerTopicStats brokerTopicStats = null;
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
    private final LeaderEpochCheckpoint checkpoint = new LeaderEpochCheckpoint() {
        List<EpochEntry> epochs = Collections.emptyList();

        @Override
        public void write(Collection<EpochEntry> epochs, boolean ignored) {
            this.epochs = new ArrayList<>(epochs);
        }

        @Override
        public List<EpochEntry> read() {
            return epochs;
        }
    };
    private final AtomicLong currentLogStartOffset = new AtomicLong(0L);

    private final UnifiedLog mockLog = mock(UnifiedLog.class);

    @BeforeEach
    void setUp() throws Exception {
        topicIds.put(leaderTopicIdPartition.topicPartition().topic(), leaderTopicIdPartition.topicId());
        topicIds.put(followerTopicIdPartition.topicPartition().topic(), followerTopicIdPartition.topicId());
        Properties props = kafka.utils.TestUtils.createDummyBrokerConfig();
        props.setProperty(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true");
        props.setProperty(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP, "100");
        remoteLogManagerConfig = createRLMConfig(props);
        brokerTopicStats = new BrokerTopicStats(Optional.of(KafkaConfig.fromProps(props)));

        remoteLogManager = new RemoteLogManager(remoteLogManagerConfig, brokerId, logDir, clusterId, time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> currentLogStartOffset.set(offset),
                brokerTopicStats) {
            public RemoteStorageManager createRemoteStorageManager() {
                return remoteStorageManager;
            }
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }
            @Override
            long findLogStartOffset(TopicIdPartition topicIdPartition, UnifiedLog log) {
                return 0L;
            }
        };
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
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        InMemoryLeaderEpochCheckpoint inMemoryCheckpoint = remoteLogManager.getLeaderEpochCheckpoint(mockLog, 0, 300);
        assertEquals(totalEpochEntries, inMemoryCheckpoint.read());

        InMemoryLeaderEpochCheckpoint inMemoryCheckpoint2 = remoteLogManager.getLeaderEpochCheckpoint(mockLog, 100, 200);
        List<EpochEntry> epochEntries = inMemoryCheckpoint2.read();
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
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint);
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
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint);
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
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint);
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
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP, configPrefix);
        props.put(configPrefix + key, "world");
        props.put("remote.log.metadata.y", "z");

        Map<String, Object> metadataMangerConfig = createRLMConfig(props).remoteLogMetadataManagerProps();
        assertEquals(props.get(configPrefix + key), metadataMangerConfig.get(key));
        assertFalse(metadataMangerConfig.containsKey("remote.log.metadata.y"));
    }

    @Test
    void testRemoteStorageManagerWithUserDefinedConfigs() {
        String key = "key";
        String configPrefix = "config.prefix";
        Properties props = new Properties();
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP, configPrefix);
        props.put(configPrefix + key, "world");
        props.put("remote.storage.manager.y", "z");

        Map<String, Object> remoteStorageManagerConfig = createRLMConfig(props).remoteStorageManagerProps();
        assertEquals(props.get(configPrefix + key), remoteStorageManagerConfig.get(key));
        assertFalse(remoteStorageManagerConfig.containsKey("remote.storage.manager.y"));
    }

    @Test
    void testRemoteLogMetadataManagerWithEndpointConfig() {
        String host = "localhost";
        String port = "1234";
        String securityProtocol = "PLAINTEXT";
        EndPoint endPoint = new EndPoint(host, Integer.parseInt(port), new ListenerName(securityProtocol),
                SecurityProtocol.PLAINTEXT);
        remoteLogManager.onEndPointCreated(endPoint);
        remoteLogManager.startup();

        ArgumentCaptor<Map<String, Object>> capture = ArgumentCaptor.forClass(Map.class);
        verify(remoteLogMetadataManager, times(1)).configure(capture.capture());
        assertEquals(host + ":" + port, capture.getValue().get(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "bootstrap.servers"));
        assertEquals(securityProtocol, capture.getValue().get(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "security.protocol"));
        assertEquals(clusterId, capture.getValue().get("cluster.id"));
        assertEquals(brokerId, capture.getValue().get(KafkaConfig.BrokerIdProp()));
    }

    @Test
    void testRemoteLogMetadataManagerWithEndpointConfigOverridden() throws IOException {
        Properties props = new Properties();
        // override common security.protocol by adding "RLMM prefix" and "remote log metadata common client prefix"
        props.put(DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX + REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "security.protocol", "SSL");
        try (RemoteLogManager remoteLogManager = new RemoteLogManager(
                createRLMConfig(props),
                brokerId,
                logDir,
                clusterId,
                time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> { },
                brokerTopicStats) {
            public RemoteStorageManager createRemoteStorageManager() {
                return remoteStorageManager;
            }
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }
        }) {

            String host = "localhost";
            String port = "1234";
            String securityProtocol = "PLAINTEXT";
            EndPoint endPoint = new EndPoint(host, Integer.parseInt(port), new ListenerName(securityProtocol),
                    SecurityProtocol.PLAINTEXT);
            remoteLogManager.onEndPointCreated(endPoint);
            remoteLogManager.startup();

            ArgumentCaptor<Map<String, Object>> capture = ArgumentCaptor.forClass(Map.class);
            verify(remoteLogMetadataManager, times(1)).configure(capture.capture());
            assertEquals(host + ":" + port, capture.getValue().get(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "bootstrap.servers"));
            // should be overridden as SSL
            assertEquals("SSL", capture.getValue().get(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "security.protocol"));
            assertEquals(clusterId, capture.getValue().get("cluster.id"));
            assertEquals(brokerId, capture.getValue().get(KafkaConfig.BrokerIdProp()));
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
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint);
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
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(oldSegment, activeSegment)));

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

        // Verify the metrics for remote writes and for failures is zero before attempt to copy log segment
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyBytesRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Verify aggregate metrics
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyBytesRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());

        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
        task.convertToLeader(2);
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
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint);
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
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(oldSegment, activeSegment)));

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
        when(remoteStorageManager.copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class)))
                .thenReturn(Optional.of(customMetadata));

        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, customMetadataSizeLimit);
        task.convertToLeader(2);
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

        // The metadata update should not be posted.
        verify(remoteLogMetadataManager, never()).updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class));

        // Verify the metric for remote writes are not updated.
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyBytesRate().count());
        // Verify we did not report any failure for remote writes
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Verify aggregate metrics
        assertEquals(1, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyBytesRate().count());
        assertEquals(1, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());
    }

    @Test
    void testRemoteLogManagerTasksAvgIdlePercentAndMetadataCountMetrics() throws Exception {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;
        int segmentCount = 3;
        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint);
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
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(oldSegment, activeSegment)));

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
        // return 3 metadata and then return 0 to simulate all segments are deleted
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
    void testRemoteLogManagerRemoteMetrics() throws Exception {
        long oldestSegmentStartOffset = 0L;
        long olderSegmentStartOffset = 75L;
        long nextSegmentStartOffset = 150L;
        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint);
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
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(oldestSegment, olderSegment, activeSegment)));

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

        Partition mockLeaderPartition = mockPartition(leaderTopicIdPartition);

        when(mockLog.onlyLocalLogSegmentsSize()).thenReturn(175L, 100L);
        when(activeSegment.size()).thenReturn(100);
        when(mockLog.onlyLocalLogSegmentsCount()).thenReturn(1L);

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
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint);
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
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(oldSegment, activeSegment)));

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

        // Verify the metrics for remote write requests/failures is zero before attempt to copy log segment
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Verify aggregate metrics
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());
        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
        task.convertToLeader(2);
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
    void testCopyLogSegmentsToRemoteShouldNotCopySegmentForFollower() throws Exception {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt())).thenReturn(Optional.of(0L));

        // create 2 log segments, with 0 and 150 as log start offset
        LogSegment oldSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(oldSegment.baseOffset()).thenReturn(oldSegmentStartOffset);
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(oldSegment, activeSegment)));
        when(mockLog.lastStableOffset()).thenReturn(250L);

        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
        task.convertToFollower();
        task.copyLogSegmentsToRemote(mockLog);

        // verify the remoteLogMetadataManager never add any metadata and remoteStorageManager never copy log segments
        verify(remoteLogMetadataManager, never()).addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class));
        verify(remoteStorageManager, never()).copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class));
        verify(remoteLogMetadataManager, never()).updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class));
        verify(mockLog).updateHighestOffsetInRemoteStorage(anyLong());
    }

    @Test
    void testRLMTaskDoesNotUploadSegmentsWhenRemoteLogMetadataManagerIsNotInitialized() throws Exception {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint);
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
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(oldSegment, activeSegment)));
        when(mockLog.lastStableOffset()).thenReturn(250L);

        // Ensure the metrics for remote write requests/failures is zero before attempt to copy log segment
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Ensure aggregate metrics
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());

        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
        task.convertToLeader(0);
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

        InMemoryLeaderEpochCheckpoint inMemoryLeaderEpochCheckpoint = new InMemoryLeaderEpochCheckpoint();
        inMemoryLeaderEpochCheckpoint.write(expectedLeaderEpoch);
        assertEquals(inMemoryLeaderEpochCheckpoint.readAsByteBuffer(), logSegmentData.leaderEpochIndex());
    }

    @Test
    void testGetClassLoaderAwareRemoteStorageManager() throws Exception {
        ClassLoaderAwareRemoteStorageManager rsmManager = mock(ClassLoaderAwareRemoteStorageManager.class);
        try (RemoteLogManager remoteLogManager =
            new RemoteLogManager(remoteLogManagerConfig, brokerId, logDir, clusterId, time,
                    t -> Optional.empty(),
                    (topicPartition, offset) -> { },
                    brokerTopicStats) {
                public RemoteStorageManager createRemoteStorageManager() {
                    return rsmManager;
                }
            }
        ) {
            assertEquals(rsmManager, remoteLogManager.storageManager());
        }
    }

    private void verifyInCache(TopicIdPartition... topicIdPartitions) {
        Arrays.stream(topicIdPartitions).forEach(topicIdPartition -> {
            assertDoesNotThrow(() -> remoteLogManager.fetchRemoteLogSegmentMetadata(topicIdPartition.topicPartition(), 0, 0L));
        });
    }

    private void verifyNotInCache(TopicIdPartition... topicIdPartitions) {
        Arrays.stream(topicIdPartitions).forEach(topicIdPartition -> {
            assertThrows(KafkaException.class, () ->
                remoteLogManager.fetchRemoteLogSegmentMetadata(topicIdPartition.topicPartition(), 0, 0L));
        });
    }

    @Test
    void testTopicIdCacheUpdates() throws RemoteStorageException {
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
        remoteLogManager.stopPartitions(Collections.singleton(new StopPartition(leaderTopicIdPartition.topicPartition(), true, true)), (tp, ex) -> { });
        verifyNotInCache(leaderTopicIdPartition);
        verifyInCache(followerTopicIdPartition);

        // Evicts from topicId cache
        remoteLogManager.stopPartitions(Collections.singleton(new StopPartition(followerTopicIdPartition.topicPartition(), true, true)), (tp, ex) -> { });
        verifyNotInCache(leaderTopicIdPartition, followerTopicIdPartition);
    }

    @Test
    void testFetchRemoteLogSegmentMetadata() throws RemoteStorageException {
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
    void testOnLeadershipChangeWillInvokeHandleLeaderOrFollowerPartitions() {
        RemoteLogManager spyRemoteLogManager = spy(remoteLogManager);
        spyRemoteLogManager.onLeadershipChange(
            Collections.emptySet(), Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);
        verify(spyRemoteLogManager).doHandleLeaderOrFollowerPartitions(eq(followerTopicIdPartition), any(java.util.function.Consumer.class));

        Mockito.reset(spyRemoteLogManager);

        spyRemoteLogManager.onLeadershipChange(
            Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.emptySet(), topicIds);
        verify(spyRemoteLogManager).doHandleLeaderOrFollowerPartitions(eq(leaderTopicIdPartition), any(java.util.function.Consumer.class));
    }

    private MemoryRecords records(long timestamp,
                                  long initialOffset,
                                  int partitionLeaderEpoch) {
        return MemoryRecords.withRecords(initialOffset, CompressionType.NONE, partitionLeaderEpoch,
            new SimpleRecord(timestamp - 1, "first message".getBytes()),
            new SimpleRecord(timestamp + 1, "second message".getBytes()),
            new SimpleRecord(timestamp + 2, "third message".getBytes())
            );
    }

    @Test
    void testRLMTaskShouldSetLeaderEpochCorrectly() {
        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
        assertFalse(task.isLeader());
        task.convertToLeader(1);
        assertTrue(task.isLeader());
        task.convertToFollower();
        assertFalse(task.isLeader());
    }

    @Test
    void testFindOffsetByTimestamp() throws IOException, RemoteStorageException {
        TopicPartition tp = leaderTopicIdPartition.topicPartition();

        long ts = time.milliseconds();
        long startOffset = 120;
        int targetLeaderEpoch = 10;

        TreeMap<Integer, Long> validSegmentEpochs = new TreeMap<>();
        validSegmentEpochs.put(targetLeaderEpoch, startOffset);

        LeaderEpochFileCache leaderEpochFileCache = new LeaderEpochFileCache(tp, checkpoint);
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
        TopicPartition tp = leaderTopicIdPartition.topicPartition();

        long ts = time.milliseconds();
        long startOffset = 120;
        int targetLeaderEpoch = 10;

        TreeMap<Integer, Long> validSegmentEpochs = new TreeMap<>();
        validSegmentEpochs.put(targetLeaderEpoch - 1, startOffset - 1); // invalid epochs not aligning with leader epoch cache
        validSegmentEpochs.put(targetLeaderEpoch, startOffset);

        LeaderEpochFileCache leaderEpochFileCache = new LeaderEpochFileCache(tp, checkpoint);
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
        TopicPartition tp = leaderTopicIdPartition.topicPartition();

        long ts = time.milliseconds();
        long startOffset = 120;
        int targetLeaderEpoch = 10;

        TreeMap<Integer, Long> validSegmentEpochs = new TreeMap<>();
        validSegmentEpochs.put(targetLeaderEpoch, startOffset);

        LeaderEpochFileCache leaderEpochFileCache = new LeaderEpochFileCache(tp, checkpoint);
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
        MockedConstruction<KafkaMetricsGroup> mockMetricsGroupCtor = mockConstruction(KafkaMetricsGroup.class);
        try {
            RemoteLogManager remoteLogManager = new RemoteLogManager(remoteLogManagerConfig, brokerId, logDir, clusterId,
                time, tp -> Optional.of(mockLog), (topicPartition, offset) -> { }, brokerTopicStats) {
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

            List<String> remoteLogManagerMetricNames = Collections.singletonList(REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC.getName());
            Set<String> remoteStorageThreadPoolMetricNames = REMOTE_STORAGE_THREAD_POOL_METRICS;

            verify(mockRlmMetricsGroup, times(remoteLogManagerMetricNames.size())).newGauge(anyString(), any());
            // Verify that the RemoteLogManager metrics are removed
            remoteLogManagerMetricNames.forEach(metricName -> verify(mockRlmMetricsGroup).removeMetric(metricName));

            verify(mockThreadPoolMetricsGroup, times(remoteStorageThreadPoolMetricNames.size())).newGauge(anyString(), any());
            // Verify that the RemoteStorageThreadPool metrics are removed
            remoteStorageThreadPoolMetricNames.forEach(metricName -> verify(mockThreadPoolMetricsGroup).removeMetric(metricName));

            verifyNoMoreInteractions(mockRlmMetricsGroup);
            verifyNoMoreInteractions(mockThreadPoolMetricsGroup);
        } finally {
            mockMetricsGroupCtor.close();
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
                .thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(segment1, segment2, activeSegment)));

        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
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
                .thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(segment1, segment2, segment3, activeSegment)));

        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
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
        BiConsumer<TopicPartition, Throwable> errorHandler = (topicPartition, throwable) -> fail("shouldn't be called");
        Set<StopPartition> partitions = new HashSet<>();
        partitions.add(new StopPartition(leaderTopicIdPartition.topicPartition(), true, false));
        partitions.add(new StopPartition(followerTopicIdPartition.topicPartition(), true, false));
        remoteLogManager.onLeadershipChange(Collections.singleton(mockPartition(leaderTopicIdPartition)),
                Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);
        assertNotNull(remoteLogManager.task(leaderTopicIdPartition));
        assertNotNull(remoteLogManager.task(followerTopicIdPartition));

        remoteLogManager.stopPartitions(partitions, errorHandler);
        assertNull(remoteLogManager.task(leaderTopicIdPartition));
        assertNull(remoteLogManager.task(followerTopicIdPartition));
        verify(remoteLogMetadataManager, times(1)).onStopPartitions(any());
        verify(remoteStorageManager, times(0)).deleteLogSegmentData(any());
        verify(remoteLogMetadataManager, times(0)).updateRemoteLogSegmentMetadata(any());
    }

    @Test
    public void testStopPartitionsWithDeletion() throws RemoteStorageException {
        BiConsumer<TopicPartition, Throwable> errorHandler =
                (topicPartition, ex) -> fail("shouldn't be called: " + ex);
        Set<StopPartition> partitions = new HashSet<>();
        partitions.add(new StopPartition(leaderTopicIdPartition.topicPartition(), true, true));
        partitions.add(new StopPartition(followerTopicIdPartition.topicPartition(), true, true));
        remoteLogManager.onLeadershipChange(Collections.singleton(mockPartition(leaderTopicIdPartition)),
                Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);
        assertNotNull(remoteLogManager.task(leaderTopicIdPartition));
        assertNotNull(remoteLogManager.task(followerTopicIdPartition));

        when(remoteLogMetadataManager.listRemoteLogSegments(eq(leaderTopicIdPartition)))
                .thenReturn(listRemoteLogSegmentMetadata(leaderTopicIdPartition, 5, 100, 1024, RemoteLogSegmentState.DELETE_SEGMENT_FINISHED).iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(eq(followerTopicIdPartition)))
                .thenReturn(listRemoteLogSegmentMetadata(followerTopicIdPartition, 3, 100, 1024, RemoteLogSegmentState.DELETE_SEGMENT_FINISHED).iterator());
        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any()))
                .thenReturn(dummyFuture);

        remoteLogManager.stopPartitions(partitions, errorHandler);
        assertNull(remoteLogManager.task(leaderTopicIdPartition));
        assertNull(remoteLogManager.task(followerTopicIdPartition));
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

        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint);
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
        try (RemoteLogManager remoteLogManager = new RemoteLogManager(remoteLogManagerConfig, brokerId, logDir, clusterId, time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> { },
                brokerTopicStats) {
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

        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(mockLog.localLogStartOffset()).thenReturn(250L);
        when(remoteLogMetadataManager.listRemoteLogSegments(eq(leaderTopicIdPartition), anyInt()))
                .thenReturn(Collections.emptyIterator());

        try (RemoteLogManager remoteLogManager = new RemoteLogManager(remoteLogManagerConfig, brokerId, logDir, clusterId, time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> { },
                brokerTopicStats) {
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

        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint);
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
        try (RemoteLogManager remoteLogManager = new RemoteLogManager(remoteLogManagerConfig, brokerId, logDir, clusterId, time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) ->  logStartOffset.set(offset),
                brokerTopicStats) {
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }
        }) {
            RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
            task.convertToLeader(4);
            task.copyLogSegmentsToRemote(mockLog);
            assertEquals(600L, logStartOffset.get());
        }
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
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint);
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


        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
        task.convertToLeader(0);
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
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint);
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
            assertEquals(2048, safeLongYammerMetricValue("RemoteDeleteLagBytes"),
                String.format("Expected to find 2048 for RemoteDeleteLagBytes metric value, but found %d", safeLongYammerMetricValue("RemoteDeleteLagBytes")));
            assertEquals(2048, safeLongYammerMetricValue("RemoteDeleteLagBytes,topic=" + leaderTopic),
                    String.format("Expected to find 2048 for RemoteDeleteLagBytes for 'Leader' topic metric value, but found %d", safeLongYammerMetricValue("RemoteDeleteLagBytes,topic=" + leaderTopic)));
            assertEquals(2, safeLongYammerMetricValue("RemoteDeleteLagSegments"),
                String.format("Expected to find 2 for RemoteDeleteLagSegments metric value, but found %d", safeLongYammerMetricValue("RemoteDeleteLagSegments")));
            assertEquals(2, safeLongYammerMetricValue("RemoteDeleteLagSegments,topic=" + leaderTopic),
                    String.format("Expected to find 2 for RemoteDeleteLagSegments for 'Leader' topic metric value, but found %d", safeLongYammerMetricValue("RemoteDeleteLagSegments,topic=" + leaderTopic)));
            return Optional.empty();
        }).doAnswer(ans -> {
            assertEquals(1024, safeLongYammerMetricValue("RemoteDeleteLagBytes"),
                String.format("Expected to find 1024 for RemoteDeleteLagBytes metric value, but found %d", safeLongYammerMetricValue("RemoteDeleteLagBytes")));
            assertEquals(1, safeLongYammerMetricValue("RemoteDeleteLagSegments,topic=" + leaderTopic),
                String.format("Expected to find 1 for RemoteDeleteLagSegments for 'Leader' topic metric value, but found %d", safeLongYammerMetricValue("RemoteDeleteLagSegments,topic=" + leaderTopic)));
            assertEquals(1024, safeLongYammerMetricValue("RemoteDeleteLagBytes"),
                    String.format("Expected to find 1024 for RemoteDeleteLagBytes metric value, but found %d", safeLongYammerMetricValue("RemoteDeleteLagBytes")));
            assertEquals(1, safeLongYammerMetricValue("RemoteDeleteLagSegments,topic=" + leaderTopic),
                    String.format("Expected to find 1 for RemoteDeleteLagSegments for 'Leader' topic metric value, but found %d", safeLongYammerMetricValue("RemoteDeleteLagSegments,topic=" + leaderTopic)));
            return Optional.empty();
        }).when(remoteStorageManager).deleteLogSegmentData(any(RemoteLogSegmentMetadata.class));

        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);

        assertEquals(0L, yammerMetricValue("RemoteDeleteLagBytes"));
        assertEquals(0L, yammerMetricValue("RemoteDeleteLagSegments"));

        assertEquals(0L, safeLongYammerMetricValue("RemoteDeleteLagBytes,topic=" + leaderTopic));
        assertEquals(0L, safeLongYammerMetricValue("RemoteDeleteLagSegments,topic=" + leaderTopic));

        task.convertToLeader(0);
        task.cleanupExpiredRemoteLogSegments();

        assertEquals(200L, currentLogStartOffset.get());
        verify(remoteStorageManager).deleteLogSegmentData(metadataList.get(0));
        verify(remoteStorageManager).deleteLogSegmentData(metadataList.get(1));
    }

    @Test
    public void testDeleteRetentionMsBeingCancelledBeforeSecondDelete() throws RemoteStorageException, ExecutionException, InterruptedException {
        RemoteLogManager.RLMTask leaderTask = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
        leaderTask.convertToLeader(0);

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
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint);
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
        RemoteLogManager.RLMTask newLeaderTask = remoteLogManager.new RLMTask(followerTopicIdPartition, 128);
        newLeaderTask.convertToLeader(1);

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
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint);
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

        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
        task.convertToLeader(0);
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
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint);
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
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint);
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
        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
        task.convertToLeader(currentLeaderEpoch);
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
        try (RemoteLogManager remoteLogManager = new RemoteLogManager(remoteLogManagerConfig, brokerId, logDir, clusterId, time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> logStartOffset.set(offset),
                brokerTopicStats) {
            public RemoteStorageManager createRemoteStorageManager() {
                return remoteStorageManager;
            }
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }
        }) {
            RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
            task.convertToLeader(0);

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
            LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint);
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
                    segmentLeaderEpochs);
            segmentMetadataList.add(metadata);
        }
        return segmentMetadataList;
    }

    private Map<Integer, Long> truncateAndGetLeaderEpochs(List<EpochEntry> entries,
                                                          Long startOffset,
                                                          Long endOffset) {
        InMemoryLeaderEpochCheckpoint myCheckpoint = new InMemoryLeaderEpochCheckpoint();
        myCheckpoint.write(entries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(null, myCheckpoint);
        cache.truncateFromStart(startOffset);
        cache.truncateFromEnd(endOffset);
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
                remoteLogManagerConfig,
                brokerId,
                logDir,
                clusterId,
                time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> { },
                brokerTopicStats) {
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
            RecordBatch findFirstBatch(RemoteLogInputStream remoteLogInputStream, long offset) {
                return null;
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
                remoteLogManagerConfig,
                brokerId,
                logDir,
                clusterId,
                time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> { },
                brokerTopicStats) {
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

            RecordBatch findFirstBatch(RemoteLogInputStream remoteLogInputStream, long offset) {
                when(firstBatch.sizeInBytes()).thenReturn(recordBatchSizeInBytes);
                doNothing().when(firstBatch).writeTo(capture.capture());
                return firstBatch;
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
                remoteLogManagerConfig,
                brokerId,
                logDir,
                clusterId,
                time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> {
                },
                brokerTopicStats) {
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

    private Partition mockPartition(TopicIdPartition topicIdPartition) {
        TopicPartition tp = topicIdPartition.topicPartition();
        Partition partition = mock(Partition.class);
        UnifiedLog log = mock(UnifiedLog.class);
        when(partition.topicPartition()).thenReturn(tp);
        when(partition.topic()).thenReturn(tp.topic());
        when(log.remoteLogEnabled()).thenReturn(true);
        when(partition.log()).thenReturn(Option.apply(log));
        return partition;
    }

    private RemoteLogManagerConfig createRLMConfig(Properties props) {
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

        AbstractConfig config = new AbstractConfig(RemoteLogManagerConfig.CONFIG_DEF, props);
        return new RemoteLogManagerConfig(config);
    }

}
