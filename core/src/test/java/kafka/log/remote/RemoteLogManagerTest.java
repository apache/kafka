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
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.server.log.remote.storage.ClassLoaderAwareRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpoint;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.OffsetIndex;
import org.apache.kafka.storage.internals.log.TimeIndex;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager;
import scala.Option;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteLogManagerTest {
    Time time = new MockTime();
    int brokerId = 0;
    String logDir = TestUtils.tempDirectory("kafka-").toString();

    RemoteStorageManager remoteStorageManager = mock(RemoteStorageManager.class);
    RemoteLogMetadataManager remoteLogMetadataManager = mock(RemoteLogMetadataManager.class);
    RemoteLogManagerConfig remoteLogManagerConfig = null;
    RemoteLogManager remoteLogManager = null;

    TopicIdPartition leaderTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("Leader", 0));
    TopicIdPartition followerTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("Follower", 0));
    Map<String, Uuid> topicIds = new HashMap<>();
    TopicPartition tp = new TopicPartition("TestTopic", 5);
    EpochEntry epochEntry0 = new EpochEntry(0, 0);
    EpochEntry epochEntry1 = new EpochEntry(1, 100);
    EpochEntry epochEntry2 = new EpochEntry(2, 200);
    List<EpochEntry> totalEpochEntries = Arrays.asList(epochEntry0, epochEntry1, epochEntry2);
    LeaderEpochCheckpoint checkpoint = new LeaderEpochCheckpoint() {
        List<EpochEntry> epochs = Collections.emptyList();
        @Override
        public void write(Collection<EpochEntry> epochs) {
            this.epochs = new ArrayList<>(epochs);
        }

        @Override
        public List<EpochEntry> read() {
            return epochs;
        }
    };

    UnifiedLog mockLog = mock(UnifiedLog.class);

    @BeforeEach
    void setUp() throws Exception {
        topicIds.put(leaderTopicIdPartition.topicPartition().topic(), leaderTopicIdPartition.topicId());
        topicIds.put(followerTopicIdPartition.topicPartition().topic(), followerTopicIdPartition.topicId());
        Properties props = new Properties();
        remoteLogManagerConfig = createRLMConfig(props);
        remoteLogManager = new RemoteLogManager(remoteLogManagerConfig, brokerId, logDir, time, tp -> Optional.of(mockLog)) {
            public RemoteStorageManager createRemoteStorageManager() {
                return remoteStorageManager;
            }
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }
        };
    }

    @Test
    void testGetLeaderEpochCheckpoint() {
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        RemoteLogManager.InMemoryLeaderEpochCheckpoint inMemoryCheckpoint = remoteLogManager.getLeaderEpochCheckpoint(mockLog, 0, 300);
        assertEquals(totalEpochEntries, inMemoryCheckpoint.read());

        RemoteLogManager.InMemoryLeaderEpochCheckpoint inMemoryCheckpoint2 = remoteLogManager.getLeaderEpochCheckpoint(mockLog, 100, 200);
        List<EpochEntry> epochEntries = inMemoryCheckpoint2.read();
        assertEquals(1, epochEntries.size());
        assertEquals(epochEntry1, epochEntries.get(0));
    }

    @Test
    void testFindHighestRemoteOffset() throws RemoteStorageException {
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        TopicIdPartition tpId = new TopicIdPartition(Uuid.randomUuid(), tp);
        long offset = remoteLogManager.findHighestRemoteOffset(tpId);
        assertEquals(-1, offset);

        when(remoteLogMetadataManager.highestOffsetForEpoch(tpId, 2)).thenReturn(Optional.of(200L));
        long offset2 = remoteLogManager.findHighestRemoteOffset(tpId);
        assertEquals(200, offset2);
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
    void testStartup() {
        remoteLogManager.startup();
        ArgumentCaptor<Map<String, Object>> capture = ArgumentCaptor.forClass(Map.class);
        verify(remoteStorageManager, times(1)).configure(capture.capture());
        assertEquals(brokerId, capture.getValue().get("broker.id"));

        verify(remoteLogMetadataManager, times(1)).configure(capture.capture());
        assertEquals(brokerId, capture.getValue().get("broker.id"));
        assertEquals(logDir, capture.getValue().get("log.dir"));
    }

    @Test
    void testGetClassLoaderAwareRemoteStorageManager() throws Exception {
        ClassLoaderAwareRemoteStorageManager rsmManager = mock(ClassLoaderAwareRemoteStorageManager.class);
        RemoteLogManager remoteLogManager =
            new RemoteLogManager(remoteLogManagerConfig, brokerId, logDir, time, t -> Optional.empty()) {
                public RemoteStorageManager createRemoteStorageManager() {
                    return rsmManager;
                }
            };
        assertEquals(rsmManager, remoteLogManager.storageManager());
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
        remoteLogManager.stopPartitions(leaderTopicIdPartition.topicPartition(), true);
        verifyNotInCache(leaderTopicIdPartition);
        verifyInCache(followerTopicIdPartition);

        // Evicts from topicId cache
        remoteLogManager.stopPartitions(followerTopicIdPartition.topicPartition(), true);
        verifyNotInCache(leaderTopicIdPartition, followerTopicIdPartition);
    }

    @Test
    void testFetchRemoteLogSegmentMetadata() throws RemoteStorageException {
        remoteLogManager.onLeadershipChange(
            Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);
        remoteLogManager.fetchRemoteLogSegmentMetadata(leaderTopicIdPartition.topicPartition(), 10, 100L);
        remoteLogManager.fetchRemoteLogSegmentMetadata(followerTopicIdPartition.topicPartition(), 20, 200L);

        verify(remoteLogMetadataManager)
            .remoteLogSegmentMetadata(ArgumentMatchers.eq(leaderTopicIdPartition), anyInt(), anyLong());
        verify(remoteLogMetadataManager)
            .remoteLogSegmentMetadata(ArgumentMatchers.eq(followerTopicIdPartition), anyInt(), anyLong());
    }

    @Test
    void testOnLeadershipChangeWillStartScheduledThread() {
        assertEquals(1.0, remoteLogManager.rlmScheduledThreadPool().getIdlePercent());
        remoteLogManager.onLeadershipChange(
            Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.emptySet(), topicIds);

        assertEquals(0.9, remoteLogManager.rlmScheduledThreadPool().getIdlePercent());

        remoteLogManager.onLeadershipChange(
            Collections.emptySet(), Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);

        assertEquals(0.8, remoteLogManager.rlmScheduledThreadPool().getIdlePercent());
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
        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition);
        assertFalse(task.isLeader());
        task.convertToLeader(1);
        assertTrue(task.isLeader());
        task.convertToFollower();
        assertFalse(task.isLeader());
    }

    @Test
    void testFindOffsetByTimestamp() throws IOException, RemoteStorageException {
        TopicPartition tp = leaderTopicIdPartition.topicPartition();
        RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid());
        long ts = time.milliseconds();
        long startOffset = 120;
        int targetLeaderEpoch = 10;

        RemoteLogSegmentMetadata segmentMetadata = mock(RemoteLogSegmentMetadata.class);
        when(segmentMetadata.remoteLogSegmentId()).thenReturn(remoteLogSegmentId);
        when(segmentMetadata.maxTimestampMs()).thenReturn(ts + 2);
        when(segmentMetadata.startOffset()).thenReturn(startOffset);
        when(segmentMetadata.endOffset()).thenReturn(startOffset + 2);

        File tpDir = new File(logDir, tp.toString());
        Files.createDirectory(tpDir.toPath());
        File txnIdxFile = new File(tpDir, "txn-index" + UnifiedLog.TxnIndexFileSuffix());
        txnIdxFile.createNewFile();
        when(remoteStorageManager.fetchIndex(any(RemoteLogSegmentMetadata.class), any(IndexType.class)))
            .thenAnswer(ans -> {
                RemoteLogSegmentMetadata metadata = ans.<RemoteLogSegmentMetadata>getArgument(0);
                IndexType indexType = ans.<IndexType>getArgument(1);
                int maxEntries = (int) (metadata.endOffset() - metadata.startOffset());
                OffsetIndex offsetIdx = new OffsetIndex(new File(tpDir, String.valueOf(metadata.startOffset()) + UnifiedLog.IndexFileSuffix()),
                    metadata.startOffset(), maxEntries * 8);
                TimeIndex timeIdx = new TimeIndex(new File(tpDir, String.valueOf(metadata.startOffset()) + UnifiedLog.TimeIndexFileSuffix()),
                    metadata.startOffset(), maxEntries * 12);
                switch (indexType) {
                    case OFFSET:
                        return new FileInputStream(offsetIdx.file());
                    case TIMESTAMP:
                        return new FileInputStream(timeIdx.file());
                    case TRANSACTION:
                        return new FileInputStream(txnIdxFile);
                }
                return null;
            });

        when(remoteLogMetadataManager.listRemoteLogSegments(ArgumentMatchers.eq(leaderTopicIdPartition), anyInt()))
            .thenAnswer(ans -> {
                int leaderEpoch = ans.<Integer>getArgument(1);
                if (leaderEpoch == targetLeaderEpoch)
                    return Collections.singleton(segmentMetadata).iterator();
                else
                    return Collections.emptyList().iterator();
            });



        // 3 messages are added with offset, and timestamp as below
        // startOffset   , ts-1
        // startOffset+1 , ts+1
        // startOffset+2 , ts+2
        when(remoteStorageManager.fetchLogSegment(segmentMetadata, 0))
            .thenAnswer(a -> new ByteArrayInputStream(records(ts, startOffset, targetLeaderEpoch).buffer().array()));

        LeaderEpochFileCache leaderEpochFileCache = new LeaderEpochFileCache(tp, checkpoint);
        leaderEpochFileCache.assign(5, 99L);
        leaderEpochFileCache.assign(targetLeaderEpoch, startOffset);
        leaderEpochFileCache.assign(12, 500L);

        remoteLogManager.onLeadershipChange(Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.emptySet(), topicIds);
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
    void testIdempotentClose() throws IOException {
        remoteLogManager.close();
        remoteLogManager.close();
        InOrder inorder = inOrder(remoteStorageManager, remoteLogMetadataManager);
        inorder.verify(remoteStorageManager, times(1)).close();
        inorder.verify(remoteLogMetadataManager, times(1)).close();
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
        AbstractConfig config = new AbstractConfig(RemoteLogManagerConfig.CONFIG_DEF, props);
        return new RemoteLogManagerConfig(config);
    }

}
