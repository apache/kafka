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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Random;

public class InmemoryRemoteStorageManagerTest {

    private static final TopicPartition TP = new TopicPartition("foo", 1);
    private static final File DIR = TestUtils.tempDirectory("inmem-rsm-");

    @Test
    public void testCopyLogSegment() throws Exception {
        InmemoryRemoteStorageManager rsm = new InmemoryRemoteStorageManager();
        RemoteLogSegmentMetadata rlsm = createRemoteLogSegmentMetadata();
        LogSegmentData logSegmentData = createLogSegmentData();
        // Copy all the segment data.
        rsm.copyLogSegmentData(rlsm, logSegmentData);

        // Check that the segment data exists in inmemory RSM.
        boolean containsSegment = rsm.containsKey(InmemoryRemoteStorageManager.generateKeyForSegment(rlsm));
        Assertions.assertTrue(containsSegment);

        // Check that the indexes exist in inmemory RSM.
        for (RemoteStorageManager.IndexType indexType : RemoteStorageManager.IndexType.values()) {
            boolean containsIndex = rsm.containsKey(InmemoryRemoteStorageManager.generateKeyForIndex(rlsm, indexType));
            Assertions.assertTrue(containsIndex);
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
        RemoteLogSegmentMetadata rlsm = createRemoteLogSegmentMetadata();
        int segSize = 100;
        LogSegmentData logSegmentData = createLogSegmentData(segSize);

        // Copy the segment
        rsm.copyLogSegmentData(rlsm, logSegmentData);

        // Check segment data exists for the copied segment.
        try (InputStream segmentStream = rsm.fetchLogSegment(rlsm, 0)) {
            checkContentSame(segmentStream, logSegmentData.logSegment().toPath());
        }

        // Check all segment indexes exist for the copied segment.
        try (InputStream offsetIndexStream = rsm.fetchIndex(rlsm, RemoteStorageManager.IndexType.Offset)) {
            checkContentSame(offsetIndexStream, logSegmentData.offsetIndex().toPath());
        }

        try (InputStream timestampIndexStream = rsm.fetchIndex(rlsm, RemoteStorageManager.IndexType.Timestamp)) {
            checkContentSame(timestampIndexStream, logSegmentData.timeIndex().toPath());
        }

        try (InputStream txnIndexStream = rsm.fetchIndex(rlsm, RemoteStorageManager.IndexType.Transaction)) {
            checkContentSame(txnIndexStream, logSegmentData.txnIndex().toPath());
        }

        try (InputStream producerSnapshotStream = rsm
                .fetchIndex(rlsm, RemoteStorageManager.IndexType.ProducerSnapshot)) {
            checkContentSame(producerSnapshotStream, logSegmentData.producerSnapshotIndex().toPath());
        }

        try (InputStream leaderEpochIndexStream = rsm.fetchIndex(rlsm, RemoteStorageManager.IndexType.LeaderEpoch)) {
            ByteBuffer leaderEpochIndex = logSegmentData.leaderEpochIndex();
            Assertions.assertEquals(leaderEpochIndex,
                    readAsByteBuffer(leaderEpochIndexStream, leaderEpochIndex.array().length));
        }
    }

    @Test
    public void testFetchSegmentsForRange() throws Exception {
        InmemoryRemoteStorageManager rsm = new InmemoryRemoteStorageManager();
        RemoteLogSegmentMetadata rlsm = createRemoteLogSegmentMetadata();
        int segSize = 100;
        LogSegmentData logSegmentData = createLogSegmentData(segSize);
        Path path = logSegmentData.logSegment().toPath();

        // Copy the segment
        rsm.copyLogSegmentData(rlsm, logSegmentData);

        // 1. Fetch segment for startPos at 0
        doTestFetchForRange(rsm, rlsm, path, 0, 40);

        // 2. Fetch segment for start and end positions as start and end of the segment.
        doTestFetchForRange(rsm, rlsm, path, 0, segSize);

        // 3. Fetch segment for endPos at the end of segment.
        doTestFetchForRange(rsm, rlsm, path, 90, segSize - 90);

        // 4. Fetch segment only for the start position.
        doTestFetchForRange(rsm, rlsm, path, 0, 1);

        // 5. Fetch segment only for the end position.
        doTestFetchForRange(rsm, rlsm, path, segSize - 1, 1);

        // 6. Fetch for any range other than boundaries.
        doTestFetchForRange(rsm, rlsm, path, 3, 90);
    }

    private void doTestFetchForRange(InmemoryRemoteStorageManager rsm, RemoteLogSegmentMetadata rlsm, Path path,
                                     int startPos, int len) throws Exception {
        // Read from the segment for the expected range.
        ByteBuffer expectedSegRangeBytes = ByteBuffer.allocate(len);
        try (SeekableByteChannel seekableByteChannel = Files.newByteChannel(path)) {
            seekableByteChannel.position(startPos).read(expectedSegRangeBytes);
        }
        expectedSegRangeBytes.rewind();

        // Fetch from inmemory RSM for the same range
        ByteBuffer fetchedSegRangeBytes = ByteBuffer.allocate(len);
        try (InputStream segmentRangeStream = rsm.fetchLogSegment(rlsm, startPos, startPos + len - 1)) {
            Utils.readFully(segmentRangeStream, fetchedSegRangeBytes);
        }
        fetchedSegRangeBytes.rewind();
        Assertions.assertEquals(expectedSegRangeBytes, fetchedSegRangeBytes);
    }

    @Test
    public void testFetchInvalidRange() throws Exception {
        InmemoryRemoteStorageManager rsm = new InmemoryRemoteStorageManager();
        RemoteLogSegmentMetadata rlsm = createRemoteLogSegmentMetadata();
        int segSize = 100;
        LogSegmentData logSegmentData = createLogSegmentData(segSize);

        // Copy the segment
        rsm.copyLogSegmentData(rlsm, logSegmentData);

        // Check fetch segments with invalid ranges like startPos < endPos
        Assertions.assertThrows(Exception.class, () -> rsm.fetchLogSegment(rlsm, 2, 1));

        // Check fetch segments with invalid ranges like startPos or endPos as negative.
        Assertions.assertThrows(Exception.class, () -> rsm.fetchLogSegment(rlsm, -1, 0));
        Assertions.assertThrows(Exception.class, () -> rsm.fetchLogSegment(rlsm, -2, -1));
    }

    @Test
    public void testDeleteSegment() throws Exception {
        InmemoryRemoteStorageManager rsm = new InmemoryRemoteStorageManager();
        RemoteLogSegmentMetadata rlsm = createRemoteLogSegmentMetadata();
        LogSegmentData logSegmentData = createLogSegmentData();

        // Copy a log segment.
        rsm.copyLogSegmentData(rlsm, logSegmentData);

        // Check that the copied segment exists in rsm and it is same.
        try (InputStream segmentStream = rsm.fetchLogSegment(rlsm, 0)) {
            checkContentSame(segmentStream, logSegmentData.logSegment().toPath());
        }

        // Delete segment and check that it does not exist in RSM.
        rsm.deleteLogSegmentData(rlsm);

        // Check that the segment data does not exist.
        Assertions.assertThrows(RemoteResourceNotFoundException.class, () -> rsm.fetchLogSegment(rlsm, 0));

        // Check that the segment data does not exist for range.
        Assertions.assertThrows(RemoteResourceNotFoundException.class, () -> rsm.fetchLogSegment(rlsm, 0, 1));

        // Check that all the indexes are not found.
        for (RemoteStorageManager.IndexType indexType : RemoteStorageManager.IndexType.values()) {
            Assertions.assertThrows(RemoteResourceNotFoundException.class, () -> rsm.fetchIndex(rlsm, indexType));
        }
    }

    private void checkContentSame(InputStream segmentStream, Path path) throws IOException {
        byte[] segmentBytes = Files.readAllBytes(path);
        ByteBuffer byteBuffer = readAsByteBuffer(segmentStream, segmentBytes.length);
        Assertions.assertEquals(ByteBuffer.wrap(segmentBytes), byteBuffer);
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
        int prefix = Math.abs(new Random().nextInt());
        File segment = new File(DIR, prefix + ".seg");
        Files.write(segment.toPath(), TestUtils.randomBytes(segSize));

        File offsetIndex = new File(DIR, prefix + ".oi");
        Files.write(offsetIndex.toPath(), TestUtils.randomBytes(10));

        File timeIndex = new File(DIR, prefix + ".ti");
        Files.write(timeIndex.toPath(), TestUtils.randomBytes(10));

        File txnIndex = new File(DIR, prefix + ".txni");
        Files.write(txnIndex.toPath(), TestUtils.randomBytes(10));

        File producerSnapshotIndex = new File(DIR, prefix + ".psi");
        Files.write(producerSnapshotIndex.toPath(), TestUtils.randomBytes(10));

        ByteBuffer leaderEpochIndex = ByteBuffer.wrap(TestUtils.randomBytes(10));
        return new LogSegmentData(segment, offsetIndex, timeIndex, txnIndex, producerSnapshotIndex, leaderEpochIndex);
    }
}
