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
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.lang.String.format;
import static java.nio.ByteBuffer.wrap;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.common.log.remote.storage.LocalTieredStorageSnapshot.takeSnapshot;
import static org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.SEGMENT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public final class LocalTieredStorageTest {
    @Rule
    public final TestName testName = new TestName();

    private final LocalLogSegments localLogSegments = new LocalLogSegments();
    private final TopicPartition topicPartition = new TopicPartition("my-topic", 1);

    private LocalTieredStorage tieredStorage;
    private Verifier remoteStorageVerifier;

    private void init(Map<String, Object> extraConfig) {
        tieredStorage = new LocalTieredStorage();
        remoteStorageVerifier = new Verifier(tieredStorage, topicPartition);

        Map<String, Object> config = new HashMap<>();
        config.put(LocalTieredStorage.STORAGE_DIR_PROP, generateStorageId());
        config.put(LocalTieredStorage.DELETE_ON_CLOSE_PROP, "true");
        config.put(LocalTieredStorage.BROKER_ID, 1);
        config.putAll(extraConfig);

        tieredStorage.configure(config);
    }

    @Before
    public void before() {
        init(Collections.emptyMap());
    }

    @After
    public void after() {
        tieredStorage.clear();
        localLogSegments.deleteAll();
    }

    @Test
    public void copyEmptyLogSegment() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        tieredStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyContainsLogSegmentFiles(id, segment);
    }

    @Test
    public void copyDataFromLogSegment() throws RemoteStorageException {
        final byte[] data = new byte[]{0, 1, 2};
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment(data);

        tieredStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyRemoteLogSegmentMatchesLocal(id, segment);
    }

    @Test
    public void fetchLogSegment() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment(new byte[]{0, 1, 2});

        tieredStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyFetchedLogSegment(id, 0, new byte[]{0, 1, 2});
        //FIXME: Fetch at arbitrary index does not work as proper support for records need to be added.
    }

    @Test
    public void fetchOffsetIndex() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        tieredStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyFetchedOffsetIndex(id, LocalLogSegments.OFFSET_FILE_BYTES);
    }

    @Test
    public void fetchTimeIndex() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        tieredStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyFetchedTimeIndex(id, LocalLogSegments.TIME_FILE_BYTES);
    }

    @Test
    public void deleteLogSegment() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        tieredStorage.copyLogSegment(id, segment);
        remoteStorageVerifier.verifyContainsLogSegmentFiles(id, segment);

        tieredStorage.deleteLogSegment(newRemoteLogSegmentMetadata(id));
        remoteStorageVerifier.verifyLogSegmentFilesAbsent(id, segment);
    }

    @Test
    public void segmentsAreNotDeletedIfDeleteApiIsDisabled() throws RemoteStorageException {
        init(Collections.singletonMap(LocalTieredStorage.ENABLE_DELETE_API_PROP, "false"));

        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        tieredStorage.copyLogSegment(id, segment);
        remoteStorageVerifier.verifyContainsLogSegmentFiles(id, segment);

        tieredStorage.deleteLogSegment(newRemoteLogSegmentMetadata(id));
        remoteStorageVerifier.verifyContainsLogSegmentFiles(id, segment);
    }

    @Test
    public void traverseSingleOffloadedRecord() throws RemoteStorageException {
        final byte[] bytes = new byte[]{0, 1, 2};

        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment(bytes);

        tieredStorage.copyLogSegment(id, segment);

        tieredStorage.traverse(new LocalTieredStorageTraverser() {
            @Override
            public void visitTopicPartition(TopicPartition topicPartition) {
                assertEquals(LocalTieredStorageTest.this.topicPartition, topicPartition);
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

        tieredStorage.copyLogSegment(id, localLogSegments.nextSegment(record1, record2));

        final LocalTieredStorageSnapshot snapshot = takeSnapshot(tieredStorage);

        assertEquals(asList(topicPartition), snapshot.getTopicPartitions());
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

        tieredStorage.copyLogSegment(idA, localLogSegments.nextSegment(record1a, record2a));
        tieredStorage.copyLogSegment(idB, localLogSegments.nextSegment(record1b, record2b));

        final LocalTieredStorageSnapshot snapshot = takeSnapshot(tieredStorage);

        final Map<RemoteLogSegmentId, List<ByteBuffer>> expected = new HashMap<>();
        expected.put(idA, asList(wrap(record1a), wrap(record2a)));
        expected.put(idB, asList(wrap(record1b), wrap(record2b)));

        final Map<RemoteLogSegmentId, List<ByteBuffer>> actual = new HashMap<>();
        actual.put(idA, extractRecordsValue(snapshot, idA));
        actual.put(idB, extractRecordsValue(snapshot, idB));

        assertEquals(asList(topicPartition), snapshot.getTopicPartitions());
        assertEquals(expected, actual);
    }

    @Test
    public void fetchThrowsIfDataDoesNotExist() {
        final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(newRemoteLogSegmentId());

        assertThrows(RemoteResourceNotFoundException.class,
            () -> tieredStorage.fetchLogSegmentData(metadata, 0L, null));
        assertThrows(RemoteResourceNotFoundException.class, () -> tieredStorage.fetchOffsetIndex(metadata));
        assertThrows(RemoteResourceNotFoundException.class, () -> tieredStorage.fetchTimestampIndex(metadata));
    }

    @Test
    public void assertStartAndEndPositionConsistency() {
        final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(newRemoteLogSegmentId());

        assertThrows(IllegalArgumentException.class, () -> tieredStorage.fetchLogSegmentData(metadata, -1L, null));
        assertThrows(IllegalArgumentException.class, () -> tieredStorage.fetchLogSegmentData(metadata, 1L, -1L));
        assertThrows(IllegalArgumentException.class, () -> tieredStorage.fetchLogSegmentData(metadata, 2L, 1L));
    }

    private RemoteLogSegmentMetadata newRemoteLogSegmentMetadata(final RemoteLogSegmentId id) {
        return new RemoteLogSegmentMetadata(id, 0, 0, -1L, -1, 1000);
    }

    private RemoteLogSegmentId newRemoteLogSegmentId() {
        return new RemoteLogSegmentId(topicPartition, UUID.randomUUID());
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

    private String generateStorageId() {
        return format("%s-%s-%s",
                getClass().getSimpleName(), testName.getMethodName(), DATE_TIME_FORMATTER.format(LocalDateTime.now()));
    }

    public final class Verifier {
        private final LocalTieredStorage remoteStorage;
        private final TopicPartition topicPartition;

        public Verifier(final LocalTieredStorage remoteStorage, final TopicPartition topicPartition) {
            this.remoteStorage = requireNonNull(remoteStorage);
            this.topicPartition = requireNonNull(topicPartition);
        }

        private List<String> expectedPaths(final RemoteLogSegmentId id, final int brokerId) {
            final String rootPath = getStorageRootDirectory();
            final String topicPartitionSubpath = format("%s-%d", topicPartition.topic(), topicPartition.partition());
            final String uuid = id.id().toString();

            return Arrays.asList(
                    Paths.get(rootPath, topicPartitionSubpath, uuid + "-" + brokerId + "-segment").toString(),
                    Paths.get(rootPath, topicPartitionSubpath, uuid + "-" + brokerId + "-offset_index").toString(),
                    Paths.get(rootPath, topicPartitionSubpath, uuid + "-" + brokerId + "-time_index").toString()
            );
        }

        /**
         * Verify the remote storage contains remote log segment and associated files for the provided {@code id}.
         *
         * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
         * @param segment The segment stored on Kafka's local storage.
         */
        public void verifyContainsLogSegmentFiles(final RemoteLogSegmentId id, final LogSegmentData segment) {
            expectedPaths(id, remoteStorage.getBrokerId()).forEach(this::assertFileExists);
        }

        /**
         * Verify the remote storage does NOT contain remote log segment and associated files for the provided {@code id}.
         *
         * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
         * @param segment The segment stored on Kafka's local storage.
         */
        public void verifyLogSegmentFilesAbsent(final RemoteLogSegmentId id, final LogSegmentData segment) {
            expectedPaths(id, remoteStorage.getBrokerId()).forEach(this::assertFileDoesNotExist);
        }

        /**
         * Compare the content of the remote segment with the provided {@code data} array.
         * This method does not fetch from the remote storage.
         *
         * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
         * @param seg The segment stored on Kafka's local storage.
         */
        public void verifyRemoteLogSegmentMatchesLocal(final RemoteLogSegmentId id, final LogSegmentData seg) {
            final String remoteSegmentPath = expectedPaths(id, remoteStorage.getBrokerId()).get(0);
            assertFileDataEquals(remoteSegmentPath, seg.logSegment().getAbsolutePath());
        }

        /**
         * Verifies the content of the remote segment matches with the {@code expected} array.
         *
         * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
         * @param startPosition The position in the segment to fetch from.
         * @param expected The expected content.
         */
        public void verifyFetchedLogSegment(final RemoteLogSegmentId id, final long startPosition, final byte[] expected) {
            try {
                final InputStream in = remoteStorage.fetchLogSegmentData(newMetadata(id), startPosition, null);
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
            try {
                final InputStream in = remoteStorage.fetchOffsetIndex(newMetadata(id));
                assertArrayEquals(expected, readFully(in));

            } catch (RemoteStorageException | IOException e) {
                throw new AssertionError(e);
            }
        }

        /**
         * Verifies the content of the remote time index matches with the {@code expected} array.
         *
         * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
         * @param expected The expected content.
         */
        public void verifyFetchedTimeIndex(final RemoteLogSegmentId id, final byte[] expected) {
            try {
                final InputStream in = remoteStorage.fetchTimestampIndex(newMetadata(id));
                assertArrayEquals(expected, readFully(in));

            } catch (RemoteStorageException | IOException e) {
                throw new AssertionError(e);
            }
        }

        private RemoteLogSegmentMetadata newMetadata(final RemoteLogSegmentId id) {
            return new RemoteLogSegmentMetadata(id, 0, 0, -1L, -1, 1000);
        }

        private String getStorageRootDirectory() {
            try {
                return remoteStorage.getStorageDirectoryRoot();

            } catch (RemoteStorageException e) {
                throw new RuntimeException(e);
            }
        }

        private void assertFileExists(final String path) {
            if (!Paths.get(path).toFile().exists()) {
                throw new AssertionError(format("File %s does not exist", path));
            }
        }

        private void assertFileDoesNotExist(final String path) {
            if (Paths.get(path).toFile().exists()) {
                throw new AssertionError(format("File %s should not exist", path));
            }
        }

        private void assertFileDataEquals(final String path1, final String path2) {
            try {
                assertFileExists(path1);
                assertArrayEquals(Files.readAllBytes(Paths.get(path1)), Files.readAllBytes(Paths.get(path2)));

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

    private static final class LocalLogSegments {
        private static final byte[] OFFSET_FILE_BYTES = "offset".getBytes();
        private static final byte[] TIME_FILE_BYTES = "time".getBytes();

        private static final NumberFormat OFFSET_FORMAT = NumberFormat.getInstance();

        static {
            OFFSET_FORMAT.setMaximumIntegerDigits(20);
            OFFSET_FORMAT.setMaximumFractionDigits(0);
            OFFSET_FORMAT.setGroupingUsed(false);
        }

        private final File segmentDir = new File("local-segments");
        private long baseOffset = 0;

        LocalLogSegments() {
            if (!segmentDir.exists()) {
                segmentDir.mkdir();
            }
        }

        LogSegmentData nextSegment() {
            return nextSegment(new byte[0]);
        }

        LogSegmentData nextSegment(final byte[]... data) {
            final String offset = OFFSET_FORMAT.format(baseOffset);

            try {
                final FileChannel channel = FileChannel.open(
                        Paths.get(segmentDir.getAbsolutePath(), offset + ".log"),
                        StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

                final ByteBuffer buffer = ByteBuffer.allocate(128);
                final byte magic = RecordBatch.MAGIC_VALUE_V2;

                MemoryRecordsBuilder builder = MemoryRecords.builder
                        (buffer, magic, CompressionType.NONE, TimestampType.CREATE_TIME, baseOffset);

                for (byte[] value : data) {
                    builder.append(System.currentTimeMillis(), null, value);
                }

                builder.build().writeFullyTo(channel);
                channel.force(true);

                final File segment = new File(segmentDir, offset + ".log");
                final File offsetIndex = new File(segmentDir, offset + ".index");
                final File timeIndex = new File(segmentDir, offset + ".time");
                final File txnIndex = new File(segmentDir, offset + ".txn");
                final File producerIdSnapshotIndex = new File(segmentDir, offset + ".pid");

                Files.write(offsetIndex.toPath(), OFFSET_FILE_BYTES);
                Files.write(timeIndex.toPath(), TIME_FILE_BYTES);

                baseOffset += data.length;
                //todo-tier pass leaderepoch state
                return new LogSegmentData(segment, offsetIndex, timeIndex, txnIndex, producerIdSnapshotIndex, producerIdSnapshotIndex);

            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        void deleteAll() {
            Arrays.stream(segmentDir.listFiles()).forEach(File::delete);
            segmentDir.delete();
        }
    }
}
