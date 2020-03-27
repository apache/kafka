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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static java.lang.String.format;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class LocalRemoteStorageManagerTest {
    @Rule
    public final TestName testName = new TestName();

    private final LocalLogSegments localLogSegments = new LocalLogSegments();
    private final TopicPartition topicPartition = new TopicPartition("my-topic", 1);

    private LocalRemoteStorageManager remoteStorage;
    private LocalRemoteStorageVerifier remoteStorageVerifier;

    private void init(Map<String, Object> extraConfig) {
        remoteStorage = new LocalRemoteStorageManager();
        remoteStorageVerifier = new LocalRemoteStorageVerifier(remoteStorage, topicPartition);

        Map<String, Object> config = new HashMap<>();
        config.put(LocalRemoteStorageManager.STORAGE_ID_PROP, generateStorageId());
        config.put(LocalRemoteStorageManager.STORAGE_ID_PROP, "true");
        config.putAll(extraConfig);

        remoteStorage.configure(config);
    }

    @Before
    public void before() {
        init(Collections.emptyMap());
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
        final byte[] data = new byte[]{0, 1, 2};
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment(data);

        remoteStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyLogSegmentDataEquals(id, segment, data);
    }

    @Test
    public void fetchLogSegment() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment(new byte[]{0, 1, 2});

        remoteStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyFetchedLogSegment(id, 0, new byte[]{0, 1, 2});
        remoteStorageVerifier.verifyFetchedLogSegment(id, 1, new byte[]{1, 2});
        remoteStorageVerifier.verifyFetchedLogSegment(id, 2, new byte[]{2});
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
    public void segmentsAreNotDeletedIfDeleteApiIsDisabled() throws RemoteStorageException {
        init(Collections.singletonMap(LocalRemoteStorageManager.STORAGE_ID_PROP, "false"));

        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        remoteStorage.copyLogSegment(id, segment);
        remoteStorageVerifier.verifyContainsLogSegmentFiles(id, segment);

        remoteStorage.deleteLogSegment(newRemoteLogSegmentMetadata(id));
        remoteStorageVerifier.verifyContainsLogSegmentFiles(id, segment);
    }

    @Test
    public void fetchThrowsIfDataDoesNotExist() {
        final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(newRemoteLogSegmentId());

        assertThrows(RemoteResourceNotFoundException.class,
            () -> remoteStorage.fetchLogSegmentData(metadata, 0L, null));
        assertThrows(RemoteResourceNotFoundException.class, () -> remoteStorage.fetchOffsetIndex(metadata));
        assertThrows(RemoteResourceNotFoundException.class, () -> remoteStorage.fetchTimestampIndex(metadata));
    }

    @Test
    public void assertStartAndEndPositionConsistency() {
        final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(newRemoteLogSegmentId());

        assertThrows(IllegalArgumentException.class, () -> remoteStorage.fetchLogSegmentData(metadata, -1L, null));
        assertThrows(IllegalArgumentException.class, () -> remoteStorage.fetchLogSegmentData(metadata, 1L, -1L));
        assertThrows(IllegalArgumentException.class, () -> remoteStorage.fetchLogSegmentData(metadata, 2L, 1L));
    }

    private RemoteLogSegmentMetadata newRemoteLogSegmentMetadata(final RemoteLogSegmentId id) {
        return new RemoteLogSegmentMetadata(id, 0, 0, -1L, -1, new byte[0]);
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

            final File segment = new File(segmentDir, offset + ".log");
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
