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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.record.ArbitraryMemoryRecords;
import org.apache.kafka.common.record.InvalidMemoryRecordsProvider;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.raft.KafkaRaftClient;
import org.apache.kafka.raft.LogAppendInfo;
import org.apache.kafka.raft.LogOffsetMetadata;
import org.apache.kafka.raft.MetadataLogConfig;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.raft.RaftLog;
import org.apache.kafka.raft.SegmentPosition;
import org.apache.kafka.raft.ValidOffsetAndEpoch;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.snapshot.FileRawSnapshotWriter;
import org.apache.kafka.snapshot.RawSnapshotWriter;
import org.apache.kafka.snapshot.Snapshots;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogStartOffsetIncrementReason;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.test.TestUtils;

import net.jqwik.api.AfterFailureMode;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.test.TestUtils.assertOptional;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaRaftLogTest {

    private static final MetadataLogConfig DEFAULT_METADATA_LOG_CONFIG = createMetadataLogConfig(
            100 * 1024,
            10 * 1000,
            100 * 1024,
            60 * 1000,
            KafkaRaftClient.MAX_BATCH_SIZE_BYTES,
            KafkaRaftClient.MAX_FETCH_SIZE_BYTES
    );

    private final MockTime mockTime = new MockTime();
    private File tempDir;

    @BeforeEach
    public void setUp() {
        tempDir = TestUtils.tempDirectory();
    }

    @AfterEach
    public void tearDown() throws IOException {
        Utils.delete(tempDir);
    }

    @Test
    public void testConfig() throws IOException {
        Properties props = new Properties();
        props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker");
        props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, "1@localhost:9093");
        props.put(KRaftConfigs.NODE_ID_CONFIG, String.valueOf(2));
        props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL");
        props.put(MetadataLogConfig.METADATA_LOG_SEGMENT_BYTES_CONFIG, String.valueOf(10240));
        props.put(MetadataLogConfig.METADATA_LOG_SEGMENT_MILLIS_CONFIG, String.valueOf(10 * 1024));
        assertThrows(ConfigException.class, () -> {
            AbstractConfig kafkaConfig = new AbstractConfig(MetadataLogConfig.CONFIG_DEF, props);
            MetadataLogConfig metadataConfig = new MetadataLogConfig(kafkaConfig);
            buildMetadataLog(tempDir, mockTime, metadataConfig);
        });

        props.put(MetadataLogConfig.METADATA_LOG_SEGMENT_BYTES_CONFIG, String.valueOf(10 * 1024 * 1024));
        AbstractConfig kafkaConfig = new AbstractConfig(MetadataLogConfig.CONFIG_DEF, props);
        MetadataLogConfig metadataConfig = new MetadataLogConfig(kafkaConfig);
        buildMetadataLog(tempDir, mockTime, metadataConfig);
    }

    @Test
    public void testUnexpectedAppendOffset() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        SimpleRecord recordFoo = new SimpleRecord("foo".getBytes());
        int currentEpoch = 3;
        long initialOffset = log.endOffset().offset();

        log.appendAsLeader(
                MemoryRecords.withRecords(initialOffset, Compression.NONE, currentEpoch, recordFoo),
                currentEpoch
        );

        // Throw exception for out of order records
        assertThrows(
                RuntimeException.class,
                () -> log.appendAsLeader(MemoryRecords.withRecords(initialOffset, Compression.NONE, currentEpoch, recordFoo), currentEpoch)
        );

        assertThrows(
                RuntimeException.class,
                () -> log.appendAsFollower(MemoryRecords.withRecords(initialOffset, Compression.NONE, currentEpoch, recordFoo), currentEpoch)
        );
    }

    @Test
    public void testEmptyAppendNotAllowed() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        assertThrows(IllegalArgumentException.class, () -> log.appendAsFollower(MemoryRecords.EMPTY, 1));
        assertThrows(IllegalArgumentException.class, () -> log.appendAsLeader(MemoryRecords.EMPTY, 1));
    }

    @ParameterizedTest
    @ArgumentsSource(InvalidMemoryRecordsProvider.class)
    public void testInvalidMemoryRecords(MemoryRecords records, Optional<Class<Exception>> expectedException) throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
        long previousEndOffset = log.endOffset().offset();

        Executable action = () -> log.appendAsFollower(records, Integer.MAX_VALUE);
        if (expectedException.isPresent()) {
            assertThrows(expectedException.get(), action);
        } else {
            assertThrows(CorruptRecordException.class, action);
        }

        assertEquals(previousEndOffset, log.endOffset().offset());
    }

    @Property(tries = 100, afterFailure = AfterFailureMode.SAMPLE_ONLY)
    public void testRandomRecords(@ForAll(supplier = ArbitraryMemoryRecords.class) MemoryRecords records) throws IOException {
        File tempDir = TestUtils.tempDirectory();
        try {
            KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
            long previousEndOffset = log.endOffset().offset();

            assertThrows(
                    CorruptRecordException.class,
                    () -> log.appendAsFollower(records, Integer.MAX_VALUE)
            );

            assertEquals(previousEndOffset, log.endOffset().offset());
        } finally {
            Utils.delete(tempDir);
        }
    }

    @Test
    public void testInvalidLeaderEpoch() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
        long previousEndOffset = log.endOffset().offset();
        int epoch = log.lastFetchedEpoch() + 1;
        int numberOfRecords = 10;

        SimpleRecord[] simpleRecords = new SimpleRecord[numberOfRecords];
        for (int i = 0; i < numberOfRecords; i++) {
            simpleRecords[i] = new SimpleRecord(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }
        MemoryRecords batchWithValidEpoch = MemoryRecords.withRecords(
                previousEndOffset,
                Compression.NONE,
                epoch,
                simpleRecords
        );
        MemoryRecords batchWithInvalidEpoch = MemoryRecords.withRecords(
                previousEndOffset + numberOfRecords,
                Compression.NONE,
                epoch + 1,
                simpleRecords
        );

        ByteBuffer buffer = ByteBuffer.allocate(batchWithValidEpoch.sizeInBytes() + batchWithInvalidEpoch.sizeInBytes());
        buffer.put(batchWithValidEpoch.buffer());
        buffer.put(batchWithInvalidEpoch.buffer());
        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        log.appendAsFollower(records, epoch);

        // Check that only the first batch was appended
        assertEquals(previousEndOffset + numberOfRecords, log.endOffset().offset());
        // Check that the last fetched epoch matches the first batch
        assertEquals(epoch, log.lastFetchedEpoch());
    }

    @Test
    public void testCreateSnapshot() throws IOException {
        int numberOfRecords = 10;
        int epoch = 1;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(numberOfRecords, epoch);
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        append(log, numberOfRecords, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords));
        createNewSnapshot(log, snapshotId);

        assertEquals(0, log.readSnapshot(snapshotId).get().sizeInBytes());
    }

    @Test
    public void testCreateSnapshotFromEndOffset() throws IOException {
        int numberOfRecords = 10;
        int firstEpoch = 1;
        int secondEpoch = 3;
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        append(log, numberOfRecords, firstEpoch);
        append(log, numberOfRecords, secondEpoch);
        log.updateHighWatermark(new LogOffsetMetadata(2 * numberOfRecords));

        // Test finding the first epoch
        log.createNewSnapshot(new OffsetAndEpoch(numberOfRecords, firstEpoch)).get().close();

        // Test finding the second epoch
        log.createNewSnapshot(new OffsetAndEpoch(2 * numberOfRecords, secondEpoch)).get().close();
    }

    @Test
    public void testCreateSnapshotInMiddleOfBatch() throws IOException {
        int numberOfRecords = 10;
        int epoch = 1;
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        append(log, numberOfRecords, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords));

        assertThrows(
                IllegalArgumentException.class,
                () -> log.createNewSnapshot(new OffsetAndEpoch(numberOfRecords - 1, epoch))
        );
    }

    @Test
    public void testCreateSnapshotLaterThanHighWatermark() throws IOException {
        int numberOfRecords = 10;
        int epoch = 1;
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        append(log, numberOfRecords, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords));

        assertThrows(
                IllegalArgumentException.class,
                () -> log.createNewSnapshot(new OffsetAndEpoch(numberOfRecords + 1, epoch))
        );
    }

    @Test
    public void testCreateSnapshotMuchLaterEpoch() throws IOException {
        int numberOfRecords = 10;
        int epoch = 1;
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        append(log, numberOfRecords, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords));

        assertThrows(
                IllegalArgumentException.class,
                () -> log.createNewSnapshot(new OffsetAndEpoch(numberOfRecords, epoch + 1))
        );
    }

    @Test
    public void testHighWatermarkOffsetMetadata() throws IOException {
        int numberOfRecords = 10;
        int epoch = 1;
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        append(log, numberOfRecords, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords));

        LogOffsetMetadata highWatermarkMetadata = log.highWatermark();
        assertEquals(numberOfRecords, highWatermarkMetadata.offset());
        assertTrue(highWatermarkMetadata.metadata().isPresent());

        SegmentPosition segmentPosition = (SegmentPosition) highWatermarkMetadata.metadata().get();
        assertEquals(0, segmentPosition.baseOffset());
        assertTrue(segmentPosition.relativePosition() > 0);
    }

    @Test
    public void testCreateSnapshotBeforeLogStartOffset() throws IOException {
        int numberOfRecords = 10;
        int epoch = 1;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(numberOfRecords - 4, epoch);
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        for (int i = 1; i <= numberOfRecords; i++) {
            append(log, 1, epoch);
        }
        log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords));
        createNewSnapshot(log, snapshotId);

        // Simulate log cleanup that advances the LSO
        log.log().maybeIncrementLogStartOffset(snapshotId.offset() - 1, LogStartOffsetIncrementReason.SegmentDeletion);

        assertEquals(Optional.empty(), log.createNewSnapshot(new OffsetAndEpoch(snapshotId.offset() - 2, snapshotId.epoch())));
    }

    @Test
    public void testCreateSnapshotDivergingEpoch() throws IOException {
        int numberOfRecords = 10;
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(numberOfRecords, epoch);
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        append(log, numberOfRecords, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords));

        assertThrows(
                IllegalArgumentException.class,
                () -> log.createNewSnapshot(new OffsetAndEpoch(snapshotId.offset(), snapshotId.epoch() - 1))
        );
    }

    @Test
    public void testCreateSnapshotOlderEpoch() throws IOException {
        int numberOfRecords = 10;
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(numberOfRecords, epoch);
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        append(log, numberOfRecords, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords));
        createNewSnapshot(log, snapshotId);

        assertThrows(
                IllegalArgumentException.class,
                () -> log.createNewSnapshot(new OffsetAndEpoch(snapshotId.offset(), snapshotId.epoch() - 1))
        );
    }

    @Test
    public void testCreateSnapshotWithMissingEpoch() throws IOException {
        int firstBatchRecords = 5;
        int firstEpoch = 1;
        int missingEpoch = firstEpoch + 1;
        int secondBatchRecords = 5;
        int secondEpoch = missingEpoch + 1;

        int numberOfRecords = firstBatchRecords + secondBatchRecords;
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        append(log, firstBatchRecords, firstEpoch);
        append(log, secondBatchRecords, secondEpoch);
        log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords));

        assertThrows(
                IllegalArgumentException.class,
                () -> log.createNewSnapshot(new OffsetAndEpoch(1, missingEpoch))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> log.createNewSnapshot(new OffsetAndEpoch(firstBatchRecords, missingEpoch))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> log.createNewSnapshot(new OffsetAndEpoch(secondBatchRecords, missingEpoch))
        );
    }

    @Test
    public void testCreateExistingSnapshot() throws IOException {
        int numberOfRecords = 10;
        int epoch = 1;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(numberOfRecords, epoch);
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        append(log, numberOfRecords, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords));
        createNewSnapshot(log, snapshotId);

        assertEquals(Optional.empty(), log.createNewSnapshot(snapshotId),
                "Creating an existing snapshot should not do anything");
    }

    @Test
    public void testTopicId() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
        assertEquals(Uuid.METADATA_TOPIC_ID, log.topicId());
    }

    @Test
    public void testReadMissingSnapshot() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
        assertEquals(Optional.empty(), log.readSnapshot(new OffsetAndEpoch(10, 0)));
    }

    @Test
    public void testReadMissingSnapshotFile() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(10, 0);
        log.onSnapshotFrozen(offsetAndEpoch);
        assertEquals(Optional.empty(), log.readSnapshot(offsetAndEpoch));
    }

    @Test
    public void testDeleteNonExistentSnapshot() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
        int offset = 10;
        int epoch = 0;

        append(log, offset, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(offset));

        assertFalse(log.deleteBeforeSnapshot(new OffsetAndEpoch(2L, epoch)));
        assertEquals(0, log.startOffset());
        assertEquals(epoch, log.lastFetchedEpoch());
        assertEquals(offset, log.endOffset().offset());
        assertEquals(offset, log.highWatermark().offset());
    }

    @Test
    public void testTruncateFullyToLatestSnapshot() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
        int numberOfRecords = 10;
        int epoch = 0;
        OffsetAndEpoch sameEpochSnapshotId = new OffsetAndEpoch(2 * numberOfRecords, epoch);

        append(log, numberOfRecords, epoch);
        createNewSnapshotUnchecked(log, sameEpochSnapshotId);

        assertTrue(log.truncateToLatestSnapshot());
        assertEquals(sameEpochSnapshotId.offset(), log.startOffset());
        assertEquals(sameEpochSnapshotId.epoch(), log.lastFetchedEpoch());
        assertEquals(sameEpochSnapshotId.offset(), log.endOffset().offset());
        assertEquals(sameEpochSnapshotId.offset(), log.highWatermark().offset());

        OffsetAndEpoch greaterEpochSnapshotId = new OffsetAndEpoch(3 * numberOfRecords, epoch + 1);

        append(log, numberOfRecords, epoch);
        createNewSnapshotUnchecked(log, greaterEpochSnapshotId);

        assertTrue(log.truncateToLatestSnapshot());
        assertEquals(greaterEpochSnapshotId.offset(), log.startOffset());
        assertEquals(greaterEpochSnapshotId.epoch(), log.lastFetchedEpoch());
        assertEquals(greaterEpochSnapshotId.offset(), log.endOffset().offset());
        assertEquals(greaterEpochSnapshotId.offset(), log.highWatermark().offset());
    }

    @Test
    public void testTruncateWillRemoveOlderSnapshot() throws IOException {
        MetadataLogAndDir metadataLogAndDir = buildMetadataLogAndDir(tempDir, mockTime);
        KafkaRaftLog log = metadataLogAndDir.log;
        MetadataLogConfig config = metadataLogAndDir.config;
        Path logDir = metadataLogAndDir.path;

        int numberOfRecords = 10;
        int epoch = 1;

        append(log, 1, epoch - 1);
        OffsetAndEpoch oldSnapshotId1 = new OffsetAndEpoch(1, epoch - 1);
        createNewSnapshotUnchecked(log, oldSnapshotId1);

        append(log, 1, epoch);
        OffsetAndEpoch oldSnapshotId2 = new OffsetAndEpoch(2, epoch);
        createNewSnapshotUnchecked(log, oldSnapshotId2);

        append(log, numberOfRecords - 2, epoch);
        OffsetAndEpoch oldSnapshotId3 = new OffsetAndEpoch(numberOfRecords, epoch);
        createNewSnapshotUnchecked(log, oldSnapshotId3);

        OffsetAndEpoch greaterSnapshotId = new OffsetAndEpoch(3 * numberOfRecords, epoch);
        createNewSnapshotUnchecked(log, greaterSnapshotId);

        assertNotEquals(log.earliestSnapshotId(), log.latestSnapshotId());
        assertTrue(log.truncateToLatestSnapshot());
        assertEquals(log.earliestSnapshotId(), log.latestSnapshotId());
        log.close();

        mockTime.sleep(config.internalDeleteDelayMillis());
        // Assert that the log dir doesn't contain any older snapshots
        Files.walk(logDir, 1)
                .map(Snapshots::parse)
                .filter(Optional::isPresent)
                .forEach(path -> assertFalse(path.get().snapshotId().offset() < log.startOffset()));
    }

    @Test
    public void testStartupWithInvalidSnapshotState() throws IOException {
        // Initialize an empty log at offset 100.
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
        log.log().truncateFullyAndStartAt(100, Optional.empty());
        log.close();

        File metadataDir = metadataLogDir(tempDir);
        assertTrue(metadataDir.exists());

        // Initialization should fail unless we have a snapshot at an offset
        // greater than or equal to 100.
        assertThrows(IllegalStateException.class, () -> buildMetadataLog(tempDir, mockTime));

        // Snapshots at offsets less than 100 are not sufficient.
        writeEmptySnapshot(metadataDir, new OffsetAndEpoch(50, 1));
        assertThrows(IllegalStateException.class, () -> buildMetadataLog(tempDir, mockTime));

        // Snapshot at offset 100 should be fine.
        writeEmptySnapshot(metadataDir, new OffsetAndEpoch(100, 1));
        log = buildMetadataLog(tempDir, mockTime);
        log.log().truncateFullyAndStartAt(200, Optional.empty());
        log.close();

        // Snapshots at higher offsets are also fine. In this case, the
        // log start offset should advance to the first snapshot offset.
        writeEmptySnapshot(metadataDir, new OffsetAndEpoch(500, 1));
        log = buildMetadataLog(tempDir, mockTime);
        assertEquals(500, log.log().logStartOffset());
    }

    @Test
    public void testSnapshotDeletionWithInvalidSnapshotState() throws IOException {
        // Initialize an empty log at offset 100.
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
        log.log().truncateFullyAndStartAt(100, Optional.empty());
        log.close();

        File metadataDir = metadataLogDir(tempDir);
        assertTrue(metadataDir.exists());

        // We have one deleted snapshot at an offset matching the start offset.
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100, 1);
        writeEmptySnapshot(metadataDir, snapshotId);

        Path deletedPath = Snapshots.markForDelete(metadataDir.toPath(), snapshotId);
        assertTrue(deletedPath.toFile().exists());

        // Initialization should still fail.
        assertThrows(IllegalStateException.class, () -> buildMetadataLog(tempDir, mockTime));

        // The snapshot marked for deletion should still exist.
        assertTrue(deletedPath.toFile().exists());
    }

    private File metadataLogDir(File logDir) {
        return new File(
                logDir.getAbsolutePath(),
                UnifiedLog.logDirName(Topic.CLUSTER_METADATA_TOPIC_PARTITION)
        );
    }

    private void writeEmptySnapshot(File metadataDir, OffsetAndEpoch snapshotId) {
        try (FileRawSnapshotWriter writer = FileRawSnapshotWriter.create(metadataDir.toPath(), snapshotId)) {
            writer.freeze();
        }
    }

    @Test
    public void testDoesntTruncateFully() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
        int numberOfRecords = 10;
        int epoch = 1;

        append(log, numberOfRecords, epoch);

        OffsetAndEpoch olderEpochSnapshotId = new OffsetAndEpoch(numberOfRecords, epoch - 1);
        createNewSnapshotUnchecked(log, olderEpochSnapshotId);
        assertFalse(log.truncateToLatestSnapshot());

        append(log, numberOfRecords, epoch);

        OffsetAndEpoch olderOffsetSnapshotId = new OffsetAndEpoch(numberOfRecords, epoch);
        createNewSnapshotUnchecked(log, olderOffsetSnapshotId);

        assertFalse(log.truncateToLatestSnapshot());
    }

    @Test
    public void testCleanupPartialSnapshots() throws IOException {
        MetadataLogAndDir metadataLogAndDir = buildMetadataLogAndDir(tempDir, mockTime);
        KafkaRaftLog log = metadataLogAndDir.log;
        Path logDir = metadataLogAndDir.path;

        int numberOfRecords = 10;
        int epoch = 1;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(1, epoch);

        append(log, numberOfRecords, epoch);
        createNewSnapshotUnchecked(log, snapshotId);
        log.close();

        // Create a few partial snapshots
        Snapshots.createTempFile(logDir, new OffsetAndEpoch(0, epoch - 1));
        Snapshots.createTempFile(logDir, new OffsetAndEpoch(1, epoch));
        Snapshots.createTempFile(logDir, new OffsetAndEpoch(2, epoch + 1));

        KafkaRaftLog secondLog = buildMetadataLog(tempDir, mockTime);

        assertEquals(snapshotId, secondLog.latestSnapshotId().get());
        assertEquals(0, log.startOffset());
        assertEquals(epoch, log.lastFetchedEpoch());
        assertEquals(numberOfRecords, log.endOffset().offset());
        assertEquals(0, secondLog.highWatermark().offset());

        // Assert that the log dir doesn't contain any partial snapshots
        Files.walk(logDir, 1)
                .map(Snapshots::parse)
                .filter(Optional::isPresent)
                .forEach(path -> assertFalse(path.get().partial()));
    }

    @Test
    public void testCleanupOlderSnapshots() throws IOException {
        MetadataLogAndDir metadataLogAndDir = buildMetadataLogAndDir(tempDir, mockTime);
        KafkaRaftLog log = metadataLogAndDir.log;
        MetadataLogConfig config = metadataLogAndDir.config;
        Path logDir = metadataLogAndDir.path;
        int numberOfRecords = 10;
        int epoch = 1;

        append(log, 1, epoch - 1);
        OffsetAndEpoch oldSnapshotId1 = new OffsetAndEpoch(1, epoch - 1);
        createNewSnapshotUnchecked(log, oldSnapshotId1);

        append(log, 1, epoch);
        OffsetAndEpoch oldSnapshotId2 = new OffsetAndEpoch(2, epoch);
        createNewSnapshotUnchecked(log, oldSnapshotId2);

        append(log, numberOfRecords - 2, epoch);
        OffsetAndEpoch oldSnapshotId3 = new OffsetAndEpoch(numberOfRecords, epoch);
        createNewSnapshotUnchecked(log, oldSnapshotId3);

        OffsetAndEpoch greaterSnapshotId = new OffsetAndEpoch(3 * numberOfRecords, epoch);
        append(log, numberOfRecords, epoch);
        createNewSnapshotUnchecked(log, greaterSnapshotId);

        log.close();

        KafkaRaftLog secondLog = buildMetadataLog(tempDir, mockTime);

        assertEquals(greaterSnapshotId, secondLog.latestSnapshotId().get());
        assertEquals(3 * numberOfRecords, secondLog.startOffset());
        assertEquals(epoch, secondLog.lastFetchedEpoch());
        mockTime.sleep(config.internalDeleteDelayMillis());

        // Assert that the log dir doesn't contain any older snapshots
        Files.walk(logDir, 1)
                .map(Snapshots::parse)
                .filter(Optional::isPresent)
                .forEach(path -> assertFalse(path.get().snapshotId().offset() < log.startOffset()));
    }

    @Test
    public void testCreateRaftLogTruncatesFully() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
        int numberOfRecords = 10;
        int epoch = 1;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(numberOfRecords + 1, epoch + 1);

        append(log, numberOfRecords, epoch);
        createNewSnapshotUnchecked(log, snapshotId);

        log.close();

        KafkaRaftLog secondLog = buildMetadataLog(tempDir, mockTime);

        assertEquals(snapshotId, secondLog.latestSnapshotId().get());
        assertEquals(snapshotId.offset(), secondLog.startOffset());
        assertEquals(snapshotId.epoch(), secondLog.lastFetchedEpoch());
        assertEquals(snapshotId.offset(), secondLog.endOffset().offset());
        assertEquals(snapshotId.offset(), secondLog.highWatermark().offset());
    }

    @Test
    public void testMaxBatchSize() throws IOException {
        int leaderEpoch = 5;
        int maxBatchSizeInBytes = 16384;
        int recordSize = 64;
        MetadataLogConfig config = createMetadataLogConfig(
                DEFAULT_METADATA_LOG_CONFIG.logSegmentBytes(),
                DEFAULT_METADATA_LOG_CONFIG.logSegmentMillis(),
                DEFAULT_METADATA_LOG_CONFIG.retentionMaxBytes(),
                DEFAULT_METADATA_LOG_CONFIG.retentionMillis(),
                maxBatchSizeInBytes,
                KafkaRaftClient.MAX_FETCH_SIZE_BYTES
        );
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime, config);

        MemoryRecords oversizeBatch = buildFullBatch(leaderEpoch, recordSize, maxBatchSizeInBytes + recordSize);
        assertThrows(RecordTooLargeException.class, () -> log.appendAsLeader(oversizeBatch, leaderEpoch));

        MemoryRecords undersizedBatch = buildFullBatch(leaderEpoch, recordSize, maxBatchSizeInBytes);
        LogAppendInfo appendInfo = log.appendAsLeader(undersizedBatch, leaderEpoch);
        assertEquals(0L, appendInfo.firstOffset());
    }

    @Test
    public void testTruncateBelowHighWatermark() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
        int numRecords = 10;
        int epoch = 5;

        append(log, numRecords, epoch);
        assertEquals(numRecords, log.endOffset().offset());

        log.updateHighWatermark(new LogOffsetMetadata(numRecords));
        assertEquals(numRecords, log.highWatermark().offset());

        assertThrows(IllegalArgumentException.class, () -> log.truncateTo(5L));
        assertEquals(numRecords, log.highWatermark().offset());
    }

    private MemoryRecords buildFullBatch(
            int leaderEpoch,
            int recordSize,
            int maxBatchSizeInBytes
    ) {
        ByteBuffer buffer = ByteBuffer.allocate(maxBatchSizeInBytes);
        BatchBuilder<byte[]> batchBuilder = new BatchBuilder<>(
                buffer,
                new ByteArraySerde(),
                Compression.NONE,
                0L,
                mockTime.milliseconds(),
                leaderEpoch,
                maxBatchSizeInBytes
        );

        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        List<byte[]> records = Collections.singletonList(new byte[recordSize]);
        while (batchBuilder.bytesNeeded(records, serializationCache).isEmpty()) {
            batchBuilder.appendRecord(records.get(0), serializationCache);
        }

        return batchBuilder.build();
    }

    @Test
    public void testValidateEpochGreaterThanLastKnownEpoch() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
        int numberOfRecords = 1;
        int epoch = 1;

        append(log, numberOfRecords, epoch);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(numberOfRecords, epoch + 1);
        assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, resultOffsetAndEpoch.kind());
        assertEquals(new OffsetAndEpoch(log.endOffset().offset(), epoch), resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testValidateEpochLessThanOldestSnapshotEpoch() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
        int numberOfRecords = 10;
        int epoch = 1;

        append(log, numberOfRecords, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords));

        OffsetAndEpoch snapshotId = new OffsetAndEpoch(numberOfRecords, epoch);
        createNewSnapshot(log, snapshotId);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(numberOfRecords, epoch - 1);
        assertEquals(ValidOffsetAndEpoch.Kind.SNAPSHOT, resultOffsetAndEpoch.kind());
        assertEquals(snapshotId, resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testValidateOffsetLessThanOldestSnapshotOffset() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);
        int offset = 2;
        int epoch = 1;

        append(log, offset, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(offset));

        OffsetAndEpoch snapshotId = new OffsetAndEpoch(offset, epoch);
        createNewSnapshot(log, snapshotId);

        // Simulate log cleaning advancing the LSO
        log.log().maybeIncrementLogStartOffset(offset, LogStartOffsetIncrementReason.SegmentDeletion);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(offset - 1, epoch);
        assertEquals(ValidOffsetAndEpoch.Kind.SNAPSHOT, resultOffsetAndEpoch.kind());
        assertEquals(snapshotId, resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testValidateOffsetEqualToOldestSnapshotOffset() throws IOException {
        int offset = 2;
        int epoch = 1;
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        append(log, offset, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(offset));

        OffsetAndEpoch snapshotId = new OffsetAndEpoch(offset, epoch);
        createNewSnapshot(log, snapshotId);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(offset, epoch);
        assertEquals(ValidOffsetAndEpoch.Kind.VALID, resultOffsetAndEpoch.kind());
        assertEquals(snapshotId, resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testValidateUnknownEpochLessThanLastKnownGreaterThanOldestSnapshot() throws IOException {
        long offset = 10;
        int numOfRecords = 5;
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        log.updateHighWatermark(new LogOffsetMetadata(offset));
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(offset, 1);
        createNewSnapshotUnchecked(log, snapshotId);
        log.truncateToLatestSnapshot();

        append(log, numOfRecords, 1);
        append(log, numOfRecords, 2);
        append(log, numOfRecords, 4);

        // offset is not equal to the oldest snapshot's offset
        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(100, 3);
        assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, resultOffsetAndEpoch.kind());
        assertEquals(new OffsetAndEpoch(20, 2), resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testValidateEpochLessThanFirstEpochInLog() throws IOException {
        long offset = 10;
        int numOfRecords = 5;
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        log.updateHighWatermark(new LogOffsetMetadata(offset));
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(offset, 1);
        createNewSnapshotUnchecked(log, snapshotId);
        log.truncateToLatestSnapshot();

        append(log, numOfRecords, 3);

        // offset is not equal to the oldest snapshot's offset
        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(100, 2);
        assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, resultOffsetAndEpoch.kind());
        assertEquals(snapshotId, resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testValidateOffsetGreatThanEndOffset() throws IOException {
        int numberOfRecords = 1;
        int epoch = 1;
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        append(log, numberOfRecords, epoch);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(numberOfRecords + 1, epoch);
        assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, resultOffsetAndEpoch.kind());
        assertEquals(new OffsetAndEpoch(log.endOffset().offset(), epoch), resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testValidateOffsetLessThanLEO() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        int numberOfRecords = 10;
        int epoch = 1;

        append(log, numberOfRecords, epoch);
        append(log, numberOfRecords, epoch + 1);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(11, epoch);
        assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, resultOffsetAndEpoch.kind());
        assertEquals(new OffsetAndEpoch(10, epoch), resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testValidateValidEpochAndOffset() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime);

        int numberOfRecords = 5;
        int epoch = 1;

        append(log, numberOfRecords, epoch);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(numberOfRecords - 1, epoch);
        assertEquals(ValidOffsetAndEpoch.Kind.VALID, resultOffsetAndEpoch.kind());
        assertEquals(new OffsetAndEpoch(numberOfRecords - 1, epoch), resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testAdvanceLogStartOffsetAfterCleaning() throws IOException {
        MetadataLogConfig config = createMetadataLogConfig(
                512,
                10 * 1000,
                256,
                60 * 1000,
                512,
                DEFAULT_METADATA_LOG_CONFIG.internalMaxFetchSizeInBytes()
        );
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime, config);

        // Generate some segments
        for (int i = 0; i < 100; i++) {
            append(log, 47, 1); // An odd number of records to avoid offset alignment
        }
        assertFalse(log.maybeClean(), "Should not clean since HW was still 0");

        log.updateHighWatermark(new LogOffsetMetadata(4000));
        assertFalse(log.maybeClean(), "Should not clean since no snapshots exist");

        OffsetAndEpoch snapshotId1 = new OffsetAndEpoch(1000, 1);
        createNewSnapshotUnchecked(log, snapshotId1);

        OffsetAndEpoch snapshotId2 = new OffsetAndEpoch(2000, 1);
        createNewSnapshotUnchecked(log, snapshotId2);

        long lsoBefore = log.startOffset();
        assertTrue(log.maybeClean(), "Expected to clean since there was at least one snapshot");
        long lsoAfter = log.startOffset();
        assertTrue(lsoAfter > lsoBefore, "Log Start Offset should have increased after cleaning");
        assertEquals(lsoAfter, snapshotId2.offset(), "Expected the Log Start Offset to be less than or equal to the snapshot offset");
    }

    @Test
    public void testDeleteSnapshots() throws IOException {
        // Generate some logs and a few snapshots, set retention low and verify that cleaning occurs
        MetadataLogConfig config = createMetadataLogConfig(
                1024,
                10 * 1000,
                1024,
                60 * 1000,
                100,
                DEFAULT_METADATA_LOG_CONFIG.internalMaxFetchSizeInBytes()
        );
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime, config);

        for (int i = 0; i < 1000; i++) {
            append(log, 1, 1);
        }
        log.updateHighWatermark(new LogOffsetMetadata(1001));

        for (int offset : List.of(100, 200, 300, 400, 500, 600)) {
            OffsetAndEpoch snapshotId = new OffsetAndEpoch(offset, 1);
            createNewSnapshotUnchecked(log, snapshotId);
        }

        assertEquals(6, log.snapshotCount());
        assertTrue(log.maybeClean());
        assertEquals(1, log.snapshotCount(), "Expected only one snapshot after cleaning");
        assertOptional(log.latestSnapshotId(), snapshotId ->
            assertEquals(600, snapshotId.offset())
        );
        assertEquals(600, log.startOffset());
    }

    @Test
    public void testSoftRetentionLimit() throws IOException {
        // Set retention equal to the segment size and generate slightly more than one segment of logs
        MetadataLogConfig config = createMetadataLogConfig(
                10240,
                10 * 1000,
                10240,
                60 * 1000,
                100,
                DEFAULT_METADATA_LOG_CONFIG.internalMaxFetchSizeInBytes()
        );
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime, config);

        for (int i = 0; i < 2000; i++) {
            append(log, 1, 1);
        }
        log.updateHighWatermark(new LogOffsetMetadata(2000));

        // Then generate two snapshots
        OffsetAndEpoch snapshotId1 = new OffsetAndEpoch(1000, 1);
        RawSnapshotWriter snapshot = log.createNewSnapshotUnchecked(snapshotId1).get();
        append(snapshot, 500);
        snapshot.freeze();
        snapshot.close();

        // Then generate a snapshot
        OffsetAndEpoch snapshotId2 = new OffsetAndEpoch(2000, 1);
        snapshot = log.createNewSnapshotUnchecked(snapshotId2).get();
        append(snapshot, 500);
        snapshot.freeze();
        snapshot.close();

        // Cleaning should occur, but resulting size will not be under retention limit since we have to keep one snapshot
        assertTrue(log.maybeClean());
        assertEquals(1, log.snapshotCount(), "Expected one snapshot after cleaning");
        assertOptional(log.latestSnapshotId(), snapshotId -> {
            assertEquals(2000, snapshotId.offset(), "Unexpected offset for latest snapshot");
            assertOptional(log.readSnapshot(snapshotId), reader ->
                assertTrue(reader.sizeInBytes() + log.log().size() > config.retentionMaxBytes())
            );
        });
    }

    @Test
    public void testSegmentMsConfigIsSetInMetadataLog() throws IOException {
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime, DEFAULT_METADATA_LOG_CONFIG);
        assertEquals(DEFAULT_METADATA_LOG_CONFIG.logSegmentMillis(), log.log().config().segmentMs);
    }

    @Test
    public void testSegmentsLessThanLatestSnapshot() throws IOException {
        MetadataLogConfig config = createMetadataLogConfig(
                10240,
                10 * 1000,
                10240,
                60 * 1000,
                200,
                DEFAULT_METADATA_LOG_CONFIG.internalMaxFetchSizeInBytes()
        );
        KafkaRaftLog log = buildMetadataLog(tempDir, mockTime, config);

        // Generate enough data to cause a segment roll
        for (int i = 0; i < 2000; i++) {
            append(log, 10, 1);
        }
        log.updateHighWatermark(new LogOffsetMetadata(log.endOffset().offset()));

        // The cleanup code requires that there are at least two snapshots
        // Generate first snapshots that includes the first segment by using the base offset of the second segment
        OffsetAndEpoch snapshotId1 = new OffsetAndEpoch(
                log.log().logSegments().get(1).baseOffset(),
                1
        );
        createNewSnapshotUnchecked(log, snapshotId1);

        // Generate second snapshots that includes the second segment by using the base offset of the third segment
        OffsetAndEpoch snapshotId2 = new OffsetAndEpoch(
                log.log().logSegments().get(2).baseOffset(),
                1
        );
        createNewSnapshotUnchecked(log, snapshotId2);

        // Sleep long enough to trigger a possible segment delete because of the default retention
        long defaultLogRetentionMs = LogConfig.DEFAULT_RETENTION_MS * 2;
        mockTime.sleep(defaultLogRetentionMs);

        assertTrue(log.maybeClean());
        assertEquals(1, log.snapshotCount());
        assertTrue(log.startOffset() > 0, log.startOffset() + " must be greater than 0");
        long latestSnapshotOffset = log.latestSnapshotId().get().offset();
        assertTrue(
                latestSnapshotOffset >= log.startOffset(),
                "latest snapshot offset (" + latestSnapshotOffset + " must be >= log start offset (" + log.startOffset() + ")"
        );
    }

    private static MetadataLogConfig createMetadataLogConfig(
            int internalLogSegmentBytes,
            long logSegmentMillis,
            long retentionMaxBytes,
            long retentionMillis,
            int internalMaxBatchSizeInBytes, //: Int = KafkaRaftClient.MAX_BATCH_SIZE_BYTES,
            int internalMaxFetchSizeInBytes //: Int = KafkaRaftClient.MAX_FETCH_SIZE_BYTES,
    ) {
        Map<String, ?> config = Map.of(
                MetadataLogConfig.INTERNAL_METADATA_LOG_SEGMENT_BYTES_CONFIG, internalLogSegmentBytes,
                MetadataLogConfig.METADATA_LOG_SEGMENT_MILLIS_CONFIG, logSegmentMillis,
                MetadataLogConfig.METADATA_MAX_RETENTION_BYTES_CONFIG, retentionMaxBytes,
                MetadataLogConfig.METADATA_MAX_RETENTION_MILLIS_CONFIG, retentionMillis,
                MetadataLogConfig.INTERNAL_METADATA_MAX_BATCH_SIZE_IN_BYTES_CONFIG, internalMaxBatchSizeInBytes,
                MetadataLogConfig.INTERNAL_METADATA_MAX_FETCH_SIZE_IN_BYTES_CONFIG, internalMaxFetchSizeInBytes,
                MetadataLogConfig.INTERNAL_METADATA_DELETE_DELAY_MILLIS_CONFIG, ServerLogConfigs.LOG_DELETE_DELAY_MS_DEFAULT
                );
        return new MetadataLogConfig(new AbstractConfig(MetadataLogConfig.CONFIG_DEF, config, false));
    }

    private static class ByteArraySerde implements RecordSerde<byte[]> {
        @Override
        public int recordSize(byte[] data, ObjectSerializationCache serializationCache) {
            return data.length;
        }
        @Override
        public void write(byte[] data, ObjectSerializationCache serializationCache, Writable out) {
            out.writeByteArray(data);
        }

        @Override
        public byte[] read(org.apache.kafka.common.protocol.Readable input, int size) {
            return input.readArray(size);
        }
    }

    private record MetadataLogAndDir(Path path, KafkaRaftLog log, MetadataLogConfig config) { }

    private static MetadataLogAndDir buildMetadataLogAndDir(File tempDir, MockTime time) throws IOException {
        return buildMetadataLogAndDir(tempDir, time, DEFAULT_METADATA_LOG_CONFIG);
    }

    private static MetadataLogAndDir buildMetadataLogAndDir(File tempDir, MockTime time, MetadataLogConfig metadataLogConfig) throws IOException {
        File logDir = createLogDirectory(
                tempDir,
                UnifiedLog.logDirName(Topic.CLUSTER_METADATA_TOPIC_PARTITION)
        );

        KafkaRaftLog metadataLog = KafkaRaftLog.createLog(
                Topic.CLUSTER_METADATA_TOPIC_PARTITION,
                Uuid.METADATA_TOPIC_ID,
                logDir,
                time,
                time.scheduler,
                metadataLogConfig,
                1
        );

        return new MetadataLogAndDir(logDir.toPath(), metadataLog, metadataLogConfig);
    }

    private static KafkaRaftLog buildMetadataLog(File tempDir, MockTime time) throws IOException {
        return buildMetadataLog(tempDir, time, DEFAULT_METADATA_LOG_CONFIG);
    }

    private static KafkaRaftLog buildMetadataLog(File tempDir, MockTime time, MetadataLogConfig metadataLogConfig) throws IOException {
        MetadataLogAndDir metadataLogAndDir = buildMetadataLogAndDir(tempDir, time, metadataLogConfig);
        return metadataLogAndDir.log;
    }

    private void createNewSnapshot(KafkaRaftLog log, OffsetAndEpoch snapshotId) {
        RawSnapshotWriter snapshot = log.createNewSnapshot(snapshotId).get();
        snapshot.freeze();
        snapshot.close();
    }

    private static void createNewSnapshotUnchecked(KafkaRaftLog log, OffsetAndEpoch snapshotId) {
        RawSnapshotWriter snapshot = log.createNewSnapshotUnchecked(snapshotId).get();
        snapshot.freeze();
        snapshot.close();
    }

    private static void append(RaftLog log, int numberOfRecords, int epoch) {
        SimpleRecord[] records = new SimpleRecord[numberOfRecords];
        for (int i = 0; i < numberOfRecords; i++) {
            records[i] = new SimpleRecord(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }
        log.appendAsLeader(
            MemoryRecords.withRecords(
                    log.endOffset().offset(),
                    Compression.NONE,
                    epoch,
                    records
            ),
            epoch
        );
    }

    private static void append(RawSnapshotWriter snapshotWriter, int numberOfRecords) {
        SimpleRecord[] records = new SimpleRecord[numberOfRecords];
        for (int i = 0; i < numberOfRecords; i++) {
            records[i] = new SimpleRecord(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }
        snapshotWriter.append(MemoryRecords.withRecords(
                0,
                Compression.NONE,
                0,
                records
        ));
    }

    private static File createLogDirectory(File logDir, String logDirName) throws IOException {
        String logDirPath = logDir.getAbsolutePath();
        File dir = new File(logDirPath, logDirName);
        if (!Files.exists(dir.toPath())) {
            Files.createDirectories(dir.toPath());
        }
        return dir;
    }

}
