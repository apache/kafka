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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InmemoryRemoteStorageManagerTest {
    private static final Logger log = LoggerFactory.getLogger(InmemoryRemoteStorageManagerTest.class);

    private static final TopicPartition TP = new TopicPartition("foo", 1);
    private static final File DIR = TestUtils.tempDirectory("inmem-rsm-");
    private static final Random RANDOM = new Random();

    @Test
    public void testCopyLogSegment() throws Exception {
        InmemoryRemoteStorageManager rsm = new InmemoryRemoteStorageManager();
        RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
        LogSegmentData logSegmentData = createLogSegmentData();
        // Copy all the segment data.
        rsm.copyLogSegmentData(segmentMetadata, logSegmentData);

        // Check that the segment data exists in in-memory RSM.
        boolean containsSegment = rsm.containsKey(InmemoryRemoteStorageManager.generateKeyForSegment(segmentMetadata));
        assertTrue(containsSegment);

        // Check that the indexes exist in in-memory RSM.
        for (RemoteStorageManager.IndexType indexType : RemoteStorageManager.IndexType.values()) {
            boolean containsIndex = rsm.containsKey(InmemoryRemoteStorageManager.generateKeyForIndex(segmentMetadata, indexType));
            assertTrue(containsIndex);
        }
    }

    private RemoteLogSegmentMetadata createRemoteLogSegmentMetadata() {
        TopicIdPartition topicPartition = new TopicIdPartition(Uuid.randomUuid(), TP);
        RemoteLogSegmentId id = new RemoteLogSegmentId(topicPartition, Uuid.randomUuid());
        return new RemoteLogSegmentMetadata(id, 100L, 200L, System.currentTimeMillis(), 0,
                System.currentTimeMillis(), 100, Collections.singletonMap(1, 100L));
    }

    @Test
    public void testFetchLogSegmentIndexes() throws Exception {
        InmemoryRemoteStorageManager rsm = new InmemoryRemoteStorageManager();
        RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
        int segSize = 100;
        LogSegmentData logSegmentData = createLogSegmentData(segSize);

        // Copy the segment
        rsm.copyLogSegmentData(segmentMetadata, logSegmentData);

        // Check segment data exists for the copied segment.
        try (InputStream segmentStream = rsm.fetchLogSegment(segmentMetadata, 0)) {
            checkContentSame(segmentStream, logSegmentData.logSegment());
        }

        HashMap<RemoteStorageManager.IndexType, Path> expectedIndexToPaths = new HashMap<>();
        expectedIndexToPaths.put(RemoteStorageManager.IndexType.OFFSET, logSegmentData.offsetIndex());
        expectedIndexToPaths.put(RemoteStorageManager.IndexType.TIMESTAMP, logSegmentData.timeIndex());
        expectedIndexToPaths.put(RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT, logSegmentData.producerSnapshotIndex());

        logSegmentData.transactionIndex().ifPresent(txnIndex -> expectedIndexToPaths.put(RemoteStorageManager.IndexType.TRANSACTION, txnIndex));

        // Check all segment indexes exist for the copied segment.
        for (Map.Entry<RemoteStorageManager.IndexType, Path> entry : expectedIndexToPaths.entrySet()) {
            RemoteStorageManager.IndexType indexType = entry.getKey();
            Path indexPath = entry.getValue();
            log.debug("Fetching index type: {}, indexPath: {}", indexType, indexPath);

            try (InputStream offsetIndexStream = rsm.fetchIndex(segmentMetadata, indexType)) {
                checkContentSame(offsetIndexStream, indexPath);
            }
        }

        try (InputStream leaderEpochIndexStream = rsm.fetchIndex(segmentMetadata, RemoteStorageManager.IndexType.LEADER_EPOCH)) {
            ByteBuffer leaderEpochIndex = logSegmentData.leaderEpochIndex();
            assertEquals(leaderEpochIndex,
                    readAsByteBuffer(leaderEpochIndexStream, leaderEpochIndex.array().length));
        }
    }

    @Test
    public void testFetchSegmentsForRange() throws Exception {
        InmemoryRemoteStorageManager rsm = new InmemoryRemoteStorageManager();
        RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
        int segSize = 100;
        LogSegmentData logSegmentData = createLogSegmentData(segSize);
        Path path = logSegmentData.logSegment();

        // Copy the segment
        rsm.copyLogSegmentData(segmentMetadata, logSegmentData);

        // 1. Fetch segment for startPos at 0
        doTestFetchForRange(rsm, segmentMetadata, path, 0, 40);

        // 2. Fetch segment for start and end positions as start and end of the segment.
        doTestFetchForRange(rsm, segmentMetadata, path, 0, segSize);

        // 3. Fetch segment for endPos at the end of segment.
        doTestFetchForRange(rsm, segmentMetadata, path, 90, segSize - 90);

        // 4. Fetch segment only for the start position.
        doTestFetchForRange(rsm, segmentMetadata, path, 0, 1);

        // 5. Fetch segment only for the end position.
        doTestFetchForRange(rsm, segmentMetadata, path, segSize - 1, 1);

        // 6. Fetch for any range other than boundaries.
        doTestFetchForRange(rsm, segmentMetadata, path, 3, 90);
    }

    private void doTestFetchForRange(InmemoryRemoteStorageManager rsm, RemoteLogSegmentMetadata rlsm, Path path,
                                     int startPos, int len) throws Exception {
        // Read from the segment for the expected range.
        ByteBuffer expectedSegRangeBytes = ByteBuffer.allocate(len);
        try (SeekableByteChannel seekableByteChannel = Files.newByteChannel(path)) {
            seekableByteChannel.position(startPos).read(expectedSegRangeBytes);
        }
        expectedSegRangeBytes.rewind();

        // Fetch from in-memory RSM for the same range
        ByteBuffer fetchedSegRangeBytes = ByteBuffer.allocate(len);
        try (InputStream segmentRangeStream = rsm.fetchLogSegment(rlsm, startPos, startPos + len - 1)) {
            Utils.readFully(segmentRangeStream, fetchedSegRangeBytes);
        }
        fetchedSegRangeBytes.rewind();
        assertEquals(expectedSegRangeBytes, fetchedSegRangeBytes);
    }

    @Test
    public void testFetchInvalidRange() throws Exception {
        InmemoryRemoteStorageManager rsm = new InmemoryRemoteStorageManager();
        RemoteLogSegmentMetadata remoteLogSegmentMetadata = createRemoteLogSegmentMetadata();
        int segSize = 100;
        LogSegmentData logSegmentData = createLogSegmentData(segSize);

        // Copy the segment
        rsm.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData);

        // Check fetch segments with invalid ranges like startPos < endPos
        assertThrows(Exception.class, () -> rsm.fetchLogSegment(remoteLogSegmentMetadata, 2, 1));

        // Check fetch segments with invalid ranges like startPos or endPos as negative.
        assertThrows(Exception.class, () -> rsm.fetchLogSegment(remoteLogSegmentMetadata, -1, 0));
        assertThrows(Exception.class, () -> rsm.fetchLogSegment(remoteLogSegmentMetadata, -2, -1));
    }

    @Test
    public void testDeleteSegment() throws Exception {
        InmemoryRemoteStorageManager rsm = new InmemoryRemoteStorageManager();
        RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
        LogSegmentData logSegmentData = createLogSegmentData();

        // Copy a log segment.
        rsm.copyLogSegmentData(segmentMetadata, logSegmentData);

        // Check that the copied segment exists in rsm and it is same.
        try (InputStream segmentStream = rsm.fetchLogSegment(segmentMetadata, 0)) {
            checkContentSame(segmentStream, logSegmentData.logSegment());
        }

        // Delete segment and check that it does not exist in RSM.
        rsm.deleteLogSegmentData(segmentMetadata);

        // Check that the segment data does not exist.
        assertThrows(RemoteResourceNotFoundException.class, () -> rsm.fetchLogSegment(segmentMetadata, 0));

        // Check that the segment data does not exist for range.
        assertThrows(RemoteResourceNotFoundException.class, () -> rsm.fetchLogSegment(segmentMetadata, 0, 1));

        // Check that all the indexes are not found.
        for (RemoteStorageManager.IndexType indexType : RemoteStorageManager.IndexType.values()) {
            assertThrows(RemoteResourceNotFoundException.class, () -> rsm.fetchIndex(segmentMetadata, indexType));
        }
    }

    private void checkContentSame(InputStream segmentStream, Path path) throws IOException {
        byte[] segmentBytes = Files.readAllBytes(path);
        ByteBuffer byteBuffer = readAsByteBuffer(segmentStream, segmentBytes.length);
        assertEquals(ByteBuffer.wrap(segmentBytes), byteBuffer);
    }

    private ByteBuffer readAsByteBuffer(InputStream segmentStream,
                                        int len) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[len]);
        Utils.readFully(segmentStream, byteBuffer);
        byteBuffer.rewind();
        return byteBuffer;
    }

    private LogSegmentData createLogSegmentData() throws Exception {
        return createLogSegmentData(100);
    }

    private LogSegmentData createLogSegmentData(int segSize) throws Exception {
        int prefix = Math.abs(RANDOM.nextInt());
        Path segment = new File(DIR, prefix + ".seg").toPath();
        Files.write(segment, TestUtils.randomBytes(segSize));

        Path offsetIndex = new File(DIR, prefix + ".oi").toPath();
        Files.write(offsetIndex, TestUtils.randomBytes(10));

        Path timeIndex = new File(DIR, prefix + ".ti").toPath();
        Files.write(timeIndex, TestUtils.randomBytes(10));

        Path txnIndex = new File(DIR, prefix + ".txni").toPath();
        Files.write(txnIndex, TestUtils.randomBytes(10));

        Path producerSnapshotIndex = new File(DIR, prefix + ".psi").toPath();
        Files.write(producerSnapshotIndex, TestUtils.randomBytes(10));

        ByteBuffer leaderEpochIndex = ByteBuffer.wrap(TestUtils.randomBytes(10));
        return new LogSegmentData(segment, offsetIndex, timeIndex, Optional.of(txnIndex), producerSnapshotIndex, leaderEpochIndex);
    }
}
