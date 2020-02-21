/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.log.storage;

import org.apache.kafka.common.*;
import org.apache.kafka.common.log.remote.storage.*;
import org.junit.*;
import org.junit.rules.*;

import java.io.*;
import java.nio.file.*;
import java.text.*;
import java.time.*;
import java.time.format.*;
import java.util.*;

import static java.lang.String.*;
import static java.util.Optional.*;
import static org.apache.kafka.common.log.storage.LocalRemoteStorageManager.*;
import static org.junit.Assert.*;

public final class LocalRemoteStorageManagerTest {
    @Rule
    public final TestName testName = new TestName();

    private final LocalLogSegments localLogSegments = new LocalLogSegments();
    private final TopicPartition topicPartition = new TopicPartition("my-topic", 1);

    private LocalRemoteStorageManager remoteStorage;
    private LocalRemoteStorageVerifier remoteStorageVerifier;

    @Before
    public void before() {
        remoteStorage = new LocalRemoteStorageManager();
        remoteStorageVerifier = new LocalRemoteStorageVerifier(remoteStorage, topicPartition);

        Map<String, Object> config = new HashMap<>();
        config.put(STORAGE_ID_PROP, generateStorageId());
        config.put(DELETE_ON_CLOSE_PROP, true);

        remoteStorage.configure(config);
    }

    @After
    public void after() {
        remoteStorage.close();
        localLogSegments.deleteAll();
    }

    @Test
    public void copyEmptyLogSegment() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        remoteStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyContainsLogSegmentFiles(id, segment);
    }

    @Test
    public void copyDataFromLogSegment() throws RemoteStorageException {
        final byte[] data = new byte[] { 0, 1, 2 };
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment(data);

        remoteStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyLogSegmentDataEquals(id, segment, data);
    }

    @Test
    public void fetchLogSegment() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment(new byte[] { 0, 1, 2 });

        remoteStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyFetchedLogSegment(id, 0, new byte[] { 0, 1, 2 });
        remoteStorageVerifier.verifyFetchedLogSegment(id, 1, new byte[] { 1, 2 });
        remoteStorageVerifier.verifyFetchedLogSegment(id, 2, new byte[] { 2 });
    }

    @Test
    public void fetchOffsetIndex() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        remoteStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyFetchedOffsetIndex(id, LocalLogSegments.OFFSET_FILE_BYTES);
    }

    @Test
    public void fetchTimeIndex() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        remoteStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyFetchedTimeIndex(id, LocalLogSegments.TIME_FILE_BYTES);
    }

    @Test
    public void deleteLogSegment() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        remoteStorage.copyLogSegment(id, segment);
        remoteStorageVerifier.verifyContainsLogSegmentFiles(id, segment);

        remoteStorage.deleteLogSegment(newRemoteLogSegmentMetadata(id));
        remoteStorageVerifier.verifyLogSegmentFilesAbsent(id, segment);
    }

    @Test
    public void fetchThrowsIfDataDoesNotExist() {
        final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(newRemoteLogSegmentId());

        assertThrows(RemoteResourceNotFoundException.class, () -> remoteStorage.fetchLogSegmentData(metadata, 0L, empty()));
        assertThrows(RemoteResourceNotFoundException.class, () -> remoteStorage.fetchOffsetIndex(metadata));
        assertThrows(RemoteResourceNotFoundException.class, () -> remoteStorage.fetchTimestampIndex(metadata));
    }

    @Test
    public void assertStartAndEndPositionConsistency() {
        final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(newRemoteLogSegmentId());

        assertThrows(IllegalArgumentException.class, () -> remoteStorage.fetchLogSegmentData(metadata, -1L, empty()));
        assertThrows(IllegalArgumentException.class, () -> remoteStorage.fetchLogSegmentData(metadata, 1L, of(-1L)));
        assertThrows(IllegalArgumentException.class, () -> remoteStorage.fetchLogSegmentData(metadata, 2L, of(1L)));
    }

    private RemoteLogSegmentMetadata newRemoteLogSegmentMetadata(final RemoteLogSegmentId id) {
        return new RemoteLogSegmentMetadata(id, 0, 0, -1, new byte[0]);
    }

    private RemoteLogSegmentId newRemoteLogSegmentId() {
        return new RemoteLogSegmentId(topicPartition, UUID.randomUUID());
    }

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss");

    private String generateStorageId() {
        return format("%s-%s-%s",
                getClass().getSimpleName(), testName.getMethodName(), DATE_TIME_FORMATTER.format(LocalDateTime.now()));
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

        LogSegmentData nextSegment(final byte[] data) {
            final String offset = OFFSET_FORMAT.format(baseOffset);

            final File segment = new File(segmentDir, offset+ ".log");
            final File offsetIndex = new File(segmentDir, offset + ".index");
            final File timeIndex = new File(segmentDir, offset + ".time");

            try {
                Files.write(segment.toPath(), data);
                Files.write(offsetIndex.toPath(), OFFSET_FILE_BYTES);
                Files.write(timeIndex.toPath(), TIME_FILE_BYTES);

            } catch (IOException e) {
                throw new AssertionError(e);
            }

            ++baseOffset;

            return new LogSegmentData(segment, offsetIndex, timeIndex);
        }

        void deleteAll() {
            Arrays.stream(segmentDir.listFiles()).forEach(File::delete);
            segmentDir.delete();
        }
    }
}
