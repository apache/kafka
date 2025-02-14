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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.storage.internals.log.LogFileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static java.lang.String.format;
import static java.nio.ByteBuffer.wrap;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageSnapshot.takeSnapshot;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.LEADER_EPOCH_CHECKPOINT;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.SEGMENT;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.LEADER_EPOCH;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.OFFSET;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TIMESTAMP;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TRANSACTION;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class LocalTieredStorageTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalTieredStorageTest.class);

    private final LocalLogSegments localLogSegments = new LocalLogSegments();
    private final TopicPartition topicPartition = new TopicPartition("my-topic", 1);
    private final TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), topicPartition);

    private LocalTieredStorage tieredStorage;
    private Verifier remoteStorageVerifier;
    private String storageDir;

    private void init(Map<String, Object> extraConfig, String testName) {
        tieredStorage = new LocalTieredStorage();
        remoteStorageVerifier = new Verifier(tieredStorage, topicIdPartition);
        storageDir = generateStorageId(testName);

        Map<String, Object> config = new HashMap<>();
        config.put(LocalTieredStorage.STORAGE_DIR_CONFIG, storageDir);
        config.put(LocalTieredStorage.DELETE_ON_CLOSE_CONFIG, "true");
        config.put(LocalTieredStorage.BROKER_ID, 1);
        config.putAll(extraConfig);

        tieredStorage.configure(config);
    }

    @BeforeEach
    public void before(TestInfo testInfo) {
        init(Collections.emptyMap(), testInfo.getDisplayName());
    }

    @AfterEach
    public void after() throws IOException {
        tieredStorage.clear();
        localLogSegments.deleteAll();
        Files.deleteIfExists(Paths.get(storageDir));
    }

    @Test
    public void copyEmptyLogSegment() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();
        final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(id);
        tieredStorage.copyLogSegmentData(metadata, segment);

        remoteStorageVerifier.verifyContainsLogSegmentFiles(metadata);
    }

    @Test
    public void copyDataFromLogSegment() throws RemoteStorageException {
        final byte[] data = new byte[]{0, 1, 2};
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(id);
        final LogSegmentData segment = localLogSegments.nextSegment(data);

        tieredStorage.copyLogSegmentData(metadata, segment);

        remoteStorageVerifier.verifyRemoteLogSegmentMatchesLocal(metadata, segment);
    }

    @Test
    public void fetchLogSegment() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment(new byte[]{0, 1, 2});

        tieredStorage.copyLogSegmentData(newRemoteLogSegmentMetadata(id), segment);

        remoteStorageVerifier.verifyFetchedLogSegment(id, 0, new byte[]{0, 1, 2});
        //FIXME: Fetch at arbitrary index does not work as proper support for records need to be added.
    }

    @Test
    public void fetchOffsetIndex() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        tieredStorage.copyLogSegmentData(newRemoteLogSegmentMetadata(id), segment);

        remoteStorageVerifier.verifyFetchedOffsetIndex(id, LocalLogSegments.OFFSET_FILE_BYTES);
    }

    @Test
    public void fetchTimeIndex() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        tieredStorage.copyLogSegmentData(newRemoteLogSegmentMetadata(id), segment);

        remoteStorageVerifier.verifyFetchedTimeIndex(id, LocalLogSegments.TIME_FILE_BYTES);
    }

    @Test
    public void fetchTransactionIndex() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        tieredStorage.copyLogSegmentData(newRemoteLogSegmentMetadata(id), segment);

        remoteStorageVerifier.verifyFetchedTransactionIndex(id, LocalLogSegments.TXN_FILE_BYTES);
    }

    @Test
    public void fetchLeaderEpochCheckpoint() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        tieredStorage.copyLogSegmentData(newRemoteLogSegmentMetadata(id), segment);

        remoteStorageVerifier.verifyLeaderEpochCheckpoint(id, LocalLogSegments.LEADER_EPOCH_CHECKPOINT_FILE_BYTES);
    }

    @Test
    public void fetchProducerSnapshot() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        tieredStorage.copyLogSegmentData(newRemoteLogSegmentMetadata(id), segment);

        remoteStorageVerifier.verifyProducerSnapshot(id, LocalLogSegments.PRODUCER_SNAPSHOT_FILE_BYTES);
    }

    @Test
    public void deleteLogSegment() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(id);
        final LogSegmentData segment = localLogSegments.nextSegment();

        tieredStorage.copyLogSegmentData(newRemoteLogSegmentMetadata(id), segment);
        remoteStorageVerifier.verifyContainsLogSegmentFiles(metadata);

        tieredStorage.deleteLogSegmentData(newRemoteLogSegmentMetadata(id));
        remoteStorageVerifier.verifyLogSegmentFilesAbsent(metadata);
    }

    @Test
    public void deletePartition() throws RemoteStorageException {
        int segmentCount = 10;
        List<RemoteLogSegmentMetadata> segmentMetadatas = new ArrayList<>();
        for (int i = 0; i < segmentCount; i++) {
            final RemoteLogSegmentId id = newRemoteLogSegmentId();
            final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(id);
            final LogSegmentData segment = localLogSegments.nextSegment();
            tieredStorage.copyLogSegmentData(metadata, segment);
            remoteStorageVerifier.verifyContainsLogSegmentFiles(metadata);
            segmentMetadatas.add(metadata);
        }
        tieredStorage.deletePartition(topicIdPartition);
        remoteStorageVerifier.assertFileDoesNotExist(remoteStorageVerifier.expectedPartitionPath());
        for (RemoteLogSegmentMetadata segmentMetadata: segmentMetadatas) {
            remoteStorageVerifier.verifyLogSegmentFilesAbsent(segmentMetadata);
        }
    }

    @Test
    public void deleteLogSegmentWithoutOptionalFiles() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(id);
        final LogSegmentData segment = localLogSegments.nextSegment();
        segment.transactionIndex().get().toFile().delete();

        tieredStorage.copyLogSegmentData(metadata, segment);
        remoteStorageVerifier.verifyContainsLogSegmentFiles(metadata, path -> {
            String fileName = path.getFileName().toString();
            if (!fileName.contains(LogFileUtils.TXN_INDEX_FILE_SUFFIX)) {
                remoteStorageVerifier.assertFileExists(path);
            }
        });

        tieredStorage.deleteLogSegmentData(newRemoteLogSegmentMetadata(id));
        remoteStorageVerifier.verifyLogSegmentFilesAbsent(metadata);
    }

    @Test
    public void segmentsAreNotDeletedIfDeleteApiIsDisabled(TestInfo testInfo) throws RemoteStorageException {
        init(Collections.singletonMap(LocalTieredStorage.ENABLE_DELETE_API_CONFIG, "false"), testInfo.getDisplayName());

        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();
        final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(id);
        tieredStorage.copyLogSegmentData(newRemoteLogSegmentMetadata(id), segment);
        remoteStorageVerifier.verifyContainsLogSegmentFiles(metadata);

        tieredStorage.deleteLogSegmentData(newRemoteLogSegmentMetadata(id));
        remoteStorageVerifier.verifyContainsLogSegmentFiles(metadata);
    }

    @Test
    public void traverseSingleOffloadedRecord() throws RemoteStorageException {
        final byte[] bytes = new byte[]{0, 1, 2};

        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment(bytes);

        tieredStorage.copyLogSegmentData(newRemoteLogSegmentMetadata(id), segment);

        tieredStorage.traverse(new LocalTieredStorageTraverser() {
            @Override
            public void visitTopicIdPartition(TopicIdPartition topicIdPartition) {
                assertEquals(LocalTieredStorageTest.this.topicPartition, topicIdPartition.topicPartition());
            }

            @Override
            public void visitSegment(RemoteLogSegmentFileset fileset) {
                assertEquals(id, fileset.getRemoteLogSegmentId());

                try {
                    final FileRecords records = FileRecords.open(fileset.getFile(SEGMENT));
                    final Iterator<Record> it = records.records().iterator();

                    assertEquals(wrap(bytes), it.next().value());

                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        });
    }

    @Test
    public void traverseMultipleOffloadedRecordsInOneSegment() throws RemoteStorageException, IOException {
        final byte[] record1 = new byte[]{0, 1, 2};
        final byte[] record2 = new byte[]{3, 4, 5};
        final RemoteLogSegmentId id = newRemoteLogSegmentId();

        tieredStorage.copyLogSegmentData(newRemoteLogSegmentMetadata(id), localLogSegments.nextSegment(record1, record2));

        final LocalTieredStorageSnapshot snapshot = takeSnapshot(tieredStorage);

        assertEquals(Collections.singletonList(topicPartition), snapshot.getTopicPartitions());
        assertEquals(asList(wrap(record1), wrap(record2)), extractRecordsValue(snapshot, id));
    }

    @Test
    public void traverseMultipleOffloadedRecordsInTwoSegments() throws RemoteStorageException, IOException {
        final byte[] record1a = new byte[]{0, 1, 2};
        final byte[] record2a = new byte[]{3, 4, 5};
        final byte[] record1b = new byte[]{6, 7, 8};
        final byte[] record2b = new byte[]{9, 10, 11};

        final RemoteLogSegmentId idA = newRemoteLogSegmentId();
        final RemoteLogSegmentId idB = newRemoteLogSegmentId();

        tieredStorage.copyLogSegmentData(newRemoteLogSegmentMetadata(idA), localLogSegments.nextSegment(record1a, record2a));
        tieredStorage.copyLogSegmentData(newRemoteLogSegmentMetadata(idB), localLogSegments.nextSegment(record1b, record2b));

        final LocalTieredStorageSnapshot snapshot = takeSnapshot(tieredStorage);

        final Map<RemoteLogSegmentId, List<ByteBuffer>> expected = new HashMap<>();
        expected.put(idA, asList(wrap(record1a), wrap(record2a)));
        expected.put(idB, asList(wrap(record1b), wrap(record2b)));

        final Map<RemoteLogSegmentId, List<ByteBuffer>> actual = new HashMap<>();
        actual.put(idA, extractRecordsValue(snapshot, idA));
        actual.put(idB, extractRecordsValue(snapshot, idB));

        assertEquals(Collections.singletonList(topicPartition), snapshot.getTopicPartitions());
        assertEquals(expected, actual);
    }

    @Test
    public void fetchThrowsIfDataDoesNotExist() {
        final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(newRemoteLogSegmentId());

        assertThrows(RemoteResourceNotFoundException.class,
            () -> tieredStorage.fetchLogSegment(metadata, 0, metadata.segmentSizeInBytes()));
        assertThrows(RemoteResourceNotFoundException.class, () -> tieredStorage.fetchIndex(metadata, OFFSET));
        assertThrows(RemoteResourceNotFoundException.class, () -> tieredStorage.fetchIndex(metadata, TIMESTAMP));
        assertThrows(RemoteResourceNotFoundException.class, () -> tieredStorage.fetchIndex(metadata, LEADER_EPOCH));
        assertThrows(RemoteResourceNotFoundException.class, () -> tieredStorage.fetchIndex(metadata, PRODUCER_SNAPSHOT));
        assertThrows(RemoteResourceNotFoundException.class, () -> tieredStorage.fetchIndex(metadata, TRANSACTION));
    }

    @Test
    public void assertStartAndEndPositionConsistency() {
        final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(newRemoteLogSegmentId());

        assertThrows(IllegalArgumentException.class, () -> tieredStorage.fetchLogSegment(metadata, -1, Integer.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> tieredStorage.fetchLogSegment(metadata, 1, -1));
        assertThrows(IllegalArgumentException.class, () -> tieredStorage.fetchLogSegment(metadata, 2, 1));
    }

    private RemoteLogSegmentMetadata newRemoteLogSegmentMetadata(final RemoteLogSegmentId id) {
        return new RemoteLogSegmentMetadata(id, 0, 0, -1L, -1, 1000L,
                1024, Collections.singletonMap(0, 0L));
    }

    private RemoteLogSegmentId newRemoteLogSegmentId() {
        return new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
    }

    private static List<ByteBuffer> extractRecordsValue(
            final LocalTieredStorageSnapshot snapshot,
            final RemoteLogSegmentId id) throws IOException {

        final FileRecords records = FileRecords.open(snapshot.getFile(id, SEGMENT));
        final List<ByteBuffer> buffers = new ArrayList<>();

        for (Record record: records.records()) {
            buffers.add(record.value());
        }

        return buffers;
    }

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss");

    private String generateStorageId(String testName) {
        return format("kafka-tiered-storage/%s-%s-%s",
                getClass().getSimpleName(), testName, DATE_TIME_FORMATTER.format(LocalDateTime.now()));
    }

    public static final class Verifier {
        private final LocalTieredStorage remoteStorage;
        private final TopicIdPartition topicIdPartition;

        public Verifier(final LocalTieredStorage remoteStorage, final TopicIdPartition topicIdPartition) {
            this.remoteStorage = requireNonNull(remoteStorage);
            this.topicIdPartition = requireNonNull(topicIdPartition);
        }

        private List<Path> expectedPaths(final RemoteLogSegmentMetadata metadata) {
            final String rootPath = getStorageRootDirectory();
            TopicPartition tp = topicIdPartition.topicPartition();
            final String topicPartitionSubpath = format("%s-%d-%s", tp.topic(), tp.partition(),
                    topicIdPartition.topicId());
            final String uuid = metadata.remoteLogSegmentId().id().toString();
            final String startOffset = LogFileUtils.filenamePrefixFromOffset(metadata.startOffset());

            return Arrays.asList(
                    Paths.get(rootPath, topicPartitionSubpath, startOffset + "-" + uuid + LogFileUtils.LOG_FILE_SUFFIX),
                    Paths.get(rootPath, topicPartitionSubpath, startOffset + "-" + uuid + LogFileUtils.INDEX_FILE_SUFFIX),
                    Paths.get(rootPath, topicPartitionSubpath, startOffset + "-" + uuid + LogFileUtils.TIME_INDEX_FILE_SUFFIX),
                    Paths.get(rootPath, topicPartitionSubpath, startOffset + "-" + uuid + LogFileUtils.TXN_INDEX_FILE_SUFFIX),
                    Paths.get(rootPath, topicPartitionSubpath, startOffset + "-" + uuid + LEADER_EPOCH_CHECKPOINT.getSuffix()),
                    Paths.get(rootPath, topicPartitionSubpath, startOffset + "-" + uuid + LogFileUtils.PRODUCER_SNAPSHOT_FILE_SUFFIX)
            );
        }

        public Path expectedPartitionPath() {
            final String rootPath = getStorageRootDirectory();
            TopicPartition tp = topicIdPartition.topicPartition();
            final String topicPartitionSubpath = format("%s-%d-%s", topicIdPartition.topicId(),
                    tp.partition(), tp.topic());
            return Paths.get(rootPath, topicPartitionSubpath);
        }

        public void verifyContainsLogSegmentFiles(final RemoteLogSegmentMetadata metadata, final Consumer<Path> action) {
            expectedPaths(metadata).forEach(action);
        }

        /**
         * Verify the remote storage contains remote log segment and associated files for the provided {@code id}.
         *
         * @param metadata The metadata of the remote log segment and associated resources (e.g. offset and time indexes).
         */
        public void verifyContainsLogSegmentFiles(final RemoteLogSegmentMetadata metadata) {
            expectedPaths(metadata).forEach(this::assertFileExists);
        }

        /**
         * Verify the remote storage does NOT contain remote log segment and associated files for the provided {@code id}.
         *
         * @param metadata The metadata of the remote log segment and associated resources (e.g. offset and time indexes).
         */
        public void verifyLogSegmentFilesAbsent(final RemoteLogSegmentMetadata metadata) {
            expectedPaths(metadata).forEach(this::assertFileDoesNotExist);
        }

        /**
         * Compare the content of the remote segment with the provided {@link LogSegmentData}.
         * This method does not fetch from the remote storage.
         *
         * @param metadata The metadata of the remote log segment and associated resources (e.g. offset and time indexes).
         * @param seg The segment stored on Kafka's local storage.
         */
        public void verifyRemoteLogSegmentMatchesLocal(final RemoteLogSegmentMetadata metadata, final LogSegmentData seg) {
            final Path remoteSegmentPath = expectedPaths(metadata).get(0);
            assertFileDataEquals(remoteSegmentPath, seg.logSegment());
        }

        /**
         * Verifies the content of the remote segment matches with the {@code expected} array.
         *
         * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
         * @param startPosition The position in the segment to fetch from.
         * @param expected The expected content.
         */
        public void verifyFetchedLogSegment(final RemoteLogSegmentId id, final int startPosition, final byte[] expected) {
            try {
                final InputStream in = remoteStorage.fetchLogSegment(newMetadata(id), startPosition);
                final ByteBuffer buffer = ByteBuffer.wrap(readFully(in));
                Iterator<Record> records = MemoryRecords.readableRecords(buffer).records().iterator();

                assertTrue(records.hasNext());
                assertEquals(ByteBuffer.wrap(expected), records.next().value());

            } catch (RemoteStorageException | IOException e) {
                throw new AssertionError(e);
            }
        }

        /**
         * Verifies the content of the remote offset index matches with the {@code expected} array.
         *
         * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
         * @param expected The expected content.
         */
        public void verifyFetchedOffsetIndex(final RemoteLogSegmentId id, final byte[] expected) {
            verifyFileContents(metadata -> remoteStorage.fetchIndex(metadata, OFFSET), id, expected);
        }

        /**
         * Verifies the content of the remote time index matches with the {@code expected} array.
         *
         * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
         * @param expected The expected content.
         */
        public void verifyFetchedTimeIndex(final RemoteLogSegmentId id, final byte[] expected) {
            verifyFileContents(metadata -> remoteStorage.fetchIndex(metadata, TIMESTAMP), id, expected);
        }

        /**
         * Verifies the content of the remote transaction index matches with the {@code expected} array.
         *
         * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
         * @param expected The expected content.
         */
        public void verifyFetchedTransactionIndex(final RemoteLogSegmentId id, final byte[] expected) {
            verifyFileContents(metadata -> remoteStorage.fetchIndex(metadata, TRANSACTION), id, expected);
        }

        /**
         * Verifies the content of the remote leader epoch checkpoint matches with the {@code expected} array.
         *
         * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
         * @param expected The expected content.
         */
        public void verifyLeaderEpochCheckpoint(final RemoteLogSegmentId id, final byte[] expected) {
            verifyFileContents(metadata -> remoteStorage.fetchIndex(metadata, LEADER_EPOCH), id, expected);
        }

        /**
         * Verifies the content of the remote producer snapshot matches with the {@code expected} array.
         *
         * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
         * @param expected The expected content.
         */
        public void verifyProducerSnapshot(final RemoteLogSegmentId id, final byte[] expected) {
            verifyFileContents(metadata -> remoteStorage.fetchIndex(metadata, PRODUCER_SNAPSHOT), id, expected);
        }

        private void verifyFileContents(final Function<RemoteLogSegmentMetadata, InputStream> actual,
                                        final RemoteLogSegmentId id,
                                        final byte[] expected) {
            try {
                final InputStream in = actual.apply(newMetadata(id));
                assertArrayEquals(expected, readFully(in));
            } catch (RemoteStorageException | IOException e) {
                throw new AssertionError(e);
            }
        }

        private RemoteLogSegmentMetadata newMetadata(final RemoteLogSegmentId id) {
            return new RemoteLogSegmentMetadata(id, 0, 0, -1L, -1, 1000,
                    1024, Collections.singletonMap(0, 0L));
        }

        private String getStorageRootDirectory() {
            try {
                return remoteStorage.getStorageDirectoryRoot();

            } catch (RemoteStorageException e) {
                throw new RuntimeException(e);
            }
        }

        private void assertFileExists(final Path path) {
            assertTrue(path.toFile().exists(), format("File %s does not exist", path));
        }

        private void assertFileDoesNotExist(final Path path) {
            assertFalse(path.toFile().exists(), format("File %s should not exist", path));
        }

        private void assertFileDataEquals(final Path path1, final Path path2) {
            try {
                assertFileExists(path1);
                assertArrayEquals(Files.readAllBytes(path1), Files.readAllBytes(path2));

            } catch (final IOException e) {
                throw new AssertionError(e);
            }
        }

        private byte[] readFully(final InputStream in) throws IOException {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final byte[] buffer = new byte[1024];
            int len;

            while ((len = in.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }

            return out.toByteArray();
        }
    }

    private interface Function<A, B> {
        B apply(A a) throws RemoteStorageException;
    }

    private static final class LocalLogSegments {
        private static final byte[] OFFSET_FILE_BYTES = "offset".getBytes();
        private static final byte[] TIME_FILE_BYTES = "time".getBytes();
        private static final byte[] TXN_FILE_BYTES = "txn".getBytes();
        private static final byte[] PRODUCER_SNAPSHOT_FILE_BYTES = "pid".getBytes();
        private static final byte[] LEADER_EPOCH_CHECKPOINT_FILE_BYTES = "0\n2\n0 0\n2 12".getBytes();

        private final Path segmentPath = Paths.get("local-segments");
        private long baseOffset = 0;

        LocalLogSegments() {
            if (Files.notExists(segmentPath)) {
                try {
                    Files.createDirectories(segmentPath);
                } catch (final IOException ex) {
                    LOGGER.error("Failed to create directory: {}", segmentPath, ex);
                }
            }
        }

        LogSegmentData nextSegment() {
            return nextSegment(new byte[0]);
        }

        LogSegmentData nextSegment(final byte[]... data) {
            final String offset = LogFileUtils.filenamePrefixFromOffset(baseOffset);

            try {
                final FileChannel channel = FileChannel.open(
                        segmentPath.resolve(offset + LogFileUtils.LOG_FILE_SUFFIX),
                        StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

                final ByteBuffer buffer = ByteBuffer.allocate(128);
                final byte magic = RecordBatch.MAGIC_VALUE_V2;

                MemoryRecordsBuilder builder = MemoryRecords.builder(
                        buffer, magic, Compression.NONE, TimestampType.CREATE_TIME, baseOffset);

                for (byte[] value : data) {
                    builder.append(System.currentTimeMillis(), null, value);
                }

                builder.build().writeFullyTo(channel);
                channel.force(true);

                final Path segment = segmentPath.resolve(offset + LogFileUtils.LOG_FILE_SUFFIX);
                final Path offsetIdx = segmentPath.resolve(offset + LogFileUtils.INDEX_FILE_SUFFIX);
                final Path timeIdx = segmentPath.resolve(offset + LogFileUtils.TIME_INDEX_FILE_SUFFIX);
                final Path txnIdx = segmentPath.resolve(offset + LogFileUtils.TXN_INDEX_FILE_SUFFIX);
                final Path producerIdSnapshot = segmentPath.resolve(offset + LogFileUtils.PRODUCER_SNAPSHOT_FILE_SUFFIX);

                Files.write(offsetIdx, OFFSET_FILE_BYTES);
                Files.write(timeIdx, TIME_FILE_BYTES);
                Files.write(txnIdx, TXN_FILE_BYTES);
                Files.write(producerIdSnapshot, PRODUCER_SNAPSHOT_FILE_BYTES);

                baseOffset += data.length;
                return new LogSegmentData(segment, offsetIdx, timeIdx, Optional.of(txnIdx),
                        producerIdSnapshot, ByteBuffer.wrap(LEADER_EPOCH_CHECKPOINT_FILE_BYTES));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        void deleteAll() throws IOException {
            List<Path> paths = Files.list(segmentPath).toList();
            for (final Path path : paths) {
                Files.delete(path);
            }
            Files.delete(segmentPath);
        }
    }
}
