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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class RemoteLogMetadataCacheTest {
    private static final Logger log = LoggerFactory.getLogger(RemoteLogMetadataCacheTest.class);

    private static final TopicIdPartition TP0 = new TopicIdPartition(Uuid.randomUuid(),
            new TopicPartition("foo", 0));
    private static final int SEG_SIZE = 1024 * 1024;
    private static final int BROKER_ID_0 = 0;
    private static final int BROKER_ID_1 = 1;

    private final Time time = new MockTime(1);

    @Test
    public void testSegmentsLifeCycleInCache() throws Exception {
        RemoteLogMetadataCache cache = new RemoteLogMetadataCache();
        // Create remote log segment metadata and add them to RemoteLogMetadataCache.

        // segment 0
        // offsets: [0-100]
        // leader epochs (0,0), (1,20), (2,80)
        Map<Integer, Long> segment0LeaderEpochs = new HashMap<>();
        segment0LeaderEpochs.put(0, 0L);
        segment0LeaderEpochs.put(1, 20L);
        segment0LeaderEpochs.put(2, 80L);
        RemoteLogSegmentId segment0Id = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segment0Metadata = new RemoteLogSegmentMetadata(segment0Id, 0L, 100L,
                -1L, BROKER_ID_0, time.milliseconds(), SEG_SIZE, segment0LeaderEpochs);
        cache.addCopyInProgressSegment(segment0Metadata);

        // We should not get this as the segment is still getting copied and it is not yet considered successful until
        // it reaches RemoteLogSegmentState.COPY_SEGMENT_FINISHED.
        Assertions.assertFalse(cache.remoteLogSegmentMetadata(40, 1).isPresent());

        // Check that these leader epochs are to considered for highest offsets as they are still getting copied and
        // they did nto reach COPY_SEGMENT_FINISHED state.
        Stream.of(0, 1, 2).forEach(epoch -> Assertions.assertFalse(cache.highestOffsetForEpoch(epoch).isPresent()));

        RemoteLogSegmentMetadataUpdate segment0Update = new RemoteLogSegmentMetadataUpdate(
                segment0Id, time.milliseconds(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, BROKER_ID_1);
        cache.updateRemoteLogSegmentMetadata(segment0Update);
        RemoteLogSegmentMetadata expectedSegment0Metadata = segment0Metadata.createWithUpdates(segment0Update);

        // segment 1
        // offsets: [101 - 200]
        // no changes in leadership with in this segment
        // leader epochs (2, 101)
        Map<Integer, Long> segment1LeaderEpochs = Collections.singletonMap(2, 101L);
        RemoteLogSegmentMetadata segment1Metadata = createSegmentUpdateWithState(cache, segment1LeaderEpochs, 101L, 200L,
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

        // segment 2
        // offsets: [201 - 300]
        // moved to epoch 3 in between
        // leader epochs (2, 201), (3, 240)
        Map<Integer, Long> segment2LeaderEpochs = new HashMap<>();
        segment2LeaderEpochs.put(2, 201L);
        segment2LeaderEpochs.put(3, 240L);
        RemoteLogSegmentMetadata segment2Metadata = createSegmentUpdateWithState(cache, segment2LeaderEpochs, 201L, 300L,
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

        // segment 3
        // offsets: [250 - 400]
        // leader epochs (3, 250), (4, 370)
        Map<Integer, Long> segment3LeaderEpochs = new HashMap<>();
        segment3LeaderEpochs.put(3, 250L);
        segment3LeaderEpochs.put(4, 370L);
        RemoteLogSegmentMetadata segment3Metadata = createSegmentUpdateWithState(cache, segment3LeaderEpochs, 250L, 400L,
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

        //////////////////////////////////////////////////////////////////////////////////////////
        // Four segments are added with different boundaries and leader epochs.
        // Search for cache.remoteLogSegmentMetadata(leaderEpoch, offset)  for different
        // epochs and offsets
        //////////////////////////////////////////////////////////////////////////////////////////

        HashMap<EpochOffset, RemoteLogSegmentMetadata> expectedEpochOffsetToSegmentMetadata = new HashMap<>();
        // Existing metadata entries.
        expectedEpochOffsetToSegmentMetadata.put(new EpochOffset(1, 40), expectedSegment0Metadata);
        expectedEpochOffsetToSegmentMetadata.put(new EpochOffset(2, 110), segment1Metadata);
        expectedEpochOffsetToSegmentMetadata.put(new EpochOffset(3, 240), segment2Metadata);
        expectedEpochOffsetToSegmentMetadata.put(new EpochOffset(3, 250), segment3Metadata);
        expectedEpochOffsetToSegmentMetadata.put(new EpochOffset(4, 375), segment3Metadata);

        // Non existing metadata entries.
        // Search for offset 110, epoch 1, and it should not exist.
        expectedEpochOffsetToSegmentMetadata.put(new EpochOffset(1, 110), null);
        // Search for non existing offset 401, epoch 4.
        expectedEpochOffsetToSegmentMetadata.put(new EpochOffset(4, 401), null);
        // Search for non existing epoch 5.
        expectedEpochOffsetToSegmentMetadata.put(new EpochOffset(5, 301), null);

        for (Map.Entry<EpochOffset, RemoteLogSegmentMetadata> entry : expectedEpochOffsetToSegmentMetadata.entrySet()) {
            EpochOffset epochOffset = entry.getKey();
            Optional<RemoteLogSegmentMetadata> segmentMetadata = cache.remoteLogSegmentMetadata(epochOffset.epoch, epochOffset.offset);
            RemoteLogSegmentMetadata expectedSegmentMetadata = entry.getValue();
            log.debug("Searching for {} , result: {}, expected: {} ", epochOffset, segmentMetadata,
                    expectedSegmentMetadata);
            if (expectedSegmentMetadata != null) {
                Assertions.assertEquals(Optional.of(expectedSegmentMetadata), segmentMetadata);
            } else {
                Assertions.assertFalse(segmentMetadata.isPresent());
            }
        }

        // Update segment with state as DELETE_SEGMENT_STARTED.
        // It should not be available when we search for that segment.
        cache.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(expectedSegment0Metadata.remoteLogSegmentId(),
                time.milliseconds(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, BROKER_ID_1));
        Assertions.assertFalse(cache.remoteLogSegmentMetadata(0, 10).isPresent());

        // Update segment with state as DELETE_SEGMENT_FINISHED.
        // It should not be available when we search for that segment.
        cache.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(expectedSegment0Metadata.remoteLogSegmentId(),
                time.milliseconds(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, BROKER_ID_1));
        Assertions.assertFalse(cache.remoteLogSegmentMetadata(0, 10).isPresent());

        //////////////////////////////////////////////////////////////////////////////////////////
        //  Search for cache.highestLogOffset(leaderEpoch) for all the leader epochs
        //////////////////////////////////////////////////////////////////////////////////////////

        Map<Integer, Long> expectedEpochToHighestOffset = new HashMap<>();
        expectedEpochToHighestOffset.put(0, 19L);
        expectedEpochToHighestOffset.put(1, 79L);
        expectedEpochToHighestOffset.put(2, 239L);
        expectedEpochToHighestOffset.put(3, 369L);
        expectedEpochToHighestOffset.put(4, 400L);

        for (Map.Entry<Integer, Long> entry : expectedEpochToHighestOffset.entrySet()) {
            Integer epoch = entry.getKey();
            Long expectedOffset = entry.getValue();
            Optional<Long> offset = cache.highestOffsetForEpoch(epoch);
            log.debug("Fetching highest offset for epoch: {} , returned: {} , expected: {}", epoch, offset, expectedOffset);
            Assertions.assertEquals(Optional.of(expectedOffset), offset);
        }

        // Search for non existing leader epoch
        Optional<Long> highestOffsetForEpoch5 = cache.highestOffsetForEpoch(5);
        Assertions.assertFalse(highestOffsetForEpoch5.isPresent());
    }

    private RemoteLogSegmentMetadata createSegmentUpdateWithState(RemoteLogMetadataCache cache,
                                                                  Map<Integer, Long> segmentLeaderEpochs,
                                                                  long startOffset,
                                                                  long endOffset,
                                                                  RemoteLogSegmentState state)
            throws RemoteResourceNotFoundException {
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId, startOffset, endOffset, -1L,
                BROKER_ID_0, time.milliseconds(), SEG_SIZE, segmentLeaderEpochs);
        cache.addCopyInProgressSegment(segmentMetadata);

        RemoteLogSegmentMetadataUpdate segMetadataUpdate = new RemoteLogSegmentMetadataUpdate(segmentId,
                time.milliseconds(), state, BROKER_ID_1);
        cache.updateRemoteLogSegmentMetadata(segMetadataUpdate);

        return segmentMetadata.createWithUpdates(segMetadataUpdate);
    }

    @Test
    public void testCacheSegmentWithCopySegmentStartedState() throws Exception {
        RemoteLogMetadataCache cache = new RemoteLogMetadataCache();

        // Create a segment with state COPY_SEGMENT_STARTED, and check for searching that segment and listing the
        // segments.
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId, 0L, 50L, -1L, BROKER_ID_0,
                time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        cache.addCopyInProgressSegment(segmentMetadata);

        // This segment should not be available as the state is not reached to COPY_SEGMENT_FINISHED.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset0Epoch0 = cache.remoteLogSegmentMetadata(0, 0);
        Assertions.assertFalse(segMetadataForOffset0Epoch0.isPresent());

        // cache.listRemoteLogSegments APIs should contain the above segment.
        checkListSegments(cache, 0, segmentMetadata);
    }

    @Test
    public void testCacheSegmentWithCopySegmentFinishedState() throws Exception {
        RemoteLogMetadataCache cache = new RemoteLogMetadataCache();

        // Create a segment and move it to state COPY_SEGMENT_FINISHED. and check for searching that segment and
        // listing the segments.
        RemoteLogSegmentMetadata segmentMetadata = createSegmentUpdateWithState(cache, Collections.singletonMap(0, 101L),
                101L, 200L, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

        // Search should return the above segment.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset150 = cache.remoteLogSegmentMetadata(0, 150);
        Assertions.assertEquals(Optional.of(segmentMetadata), segMetadataForOffset150);

        // cache.listRemoteLogSegments should contain the above segments.
        checkListSegments(cache, 0, segmentMetadata);
    }

    @Test
    public void testCacheSegmentWithDeleteSegmentStartedState() throws Exception {
        RemoteLogMetadataCache cache = new RemoteLogMetadataCache();

        // Create a segment and move it to state DELETE_SEGMENT_STARTED, and check for searching that segment and
        // listing the segments.
        RemoteLogSegmentMetadata segmentMetadata = createSegmentUpdateWithState(cache, Collections.singletonMap(0, 201L),
                201L, 300L, RemoteLogSegmentState.DELETE_SEGMENT_STARTED);

        // Search should not return the above segment as their leader epoch state is cleared.
        Optional<RemoteLogSegmentMetadata> segmentMetadataForOffset250Epoch0 = cache.remoteLogSegmentMetadata(0, 250);
        Assertions.assertFalse(segmentMetadataForOffset250Epoch0.isPresent());

        checkListSegments(cache, 0, segmentMetadata);
    }

    @Test
    public void testCacheSegmentsWithDeleteSegmentFinishedState() throws Exception {
        RemoteLogMetadataCache cache = new RemoteLogMetadataCache();

        // Create a segment and move it to state DELETE_SEGMENT_FINISHED, and check for searching that segment and
        // listing the segments.
        RemoteLogSegmentMetadata segmentMetadata = createSegmentUpdateWithState(cache, Collections.singletonMap(0, 301L),
                301L, 400L, RemoteLogSegmentState.DELETE_SEGMENT_STARTED);

        // Search should not return the above segment as their leader epoch state is cleared.
        Assertions.assertFalse(cache.remoteLogSegmentMetadata(0, 350).isPresent());

        RemoteLogSegmentMetadataUpdate segmentMetadataUpdate = new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(),
                time.milliseconds(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, BROKER_ID_1);
        cache.updateRemoteLogSegmentMetadata(segmentMetadataUpdate);

        // listRemoteLogSegments(0) and listRemoteLogSegments() should not contain the above segment.
        Assertions.assertFalse(cache.listRemoteLogSegments(0).hasNext());
        Assertions.assertFalse(cache.listAllRemoteLogSegments().hasNext());
    }

    @Test
    public void testCacheListSegments() throws Exception {
        RemoteLogMetadataCache cache = new RemoteLogMetadataCache();

        // Create a few segments and add them to the cache.
        RemoteLogSegmentMetadata segment0 = createSegmentUpdateWithState(cache, Collections.singletonMap(0, 0L), 0, 100,
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        RemoteLogSegmentMetadata segment1 = createSegmentUpdateWithState(cache, Collections.singletonMap(0, 101L), 101, 200,
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        Map<Integer, Long> segment2LeaderEpochs = new HashMap<>();
        segment2LeaderEpochs.put(0, 201L);
        segment2LeaderEpochs.put(1, 301L);
        RemoteLogSegmentMetadata segment2 = createSegmentUpdateWithState(cache, segment2LeaderEpochs, 201, 400,
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

        // listRemoteLogSegments(0) and listAllRemoteLogSegments() should contain all the above segments.
        List<RemoteLogSegmentMetadata> expectedSegmentsForEpoch0 = Arrays.asList(segment0, segment1, segment2);
        Assertions.assertTrue(TestUtils.sameElementsWithOrder(cache.listRemoteLogSegments(0),
                expectedSegmentsForEpoch0.iterator()));
        Assertions.assertTrue(TestUtils.sameElementsWithoutOrder(cache.listAllRemoteLogSegments(),
                expectedSegmentsForEpoch0.iterator()));

        // listRemoteLogSegments(1) should contain only segment2.
        List<RemoteLogSegmentMetadata> expectedSegmentsForEpoch1 = Collections.singletonList(segment2);
        Assertions.assertTrue(TestUtils.sameElementsWithOrder(cache.listRemoteLogSegments(1),
                expectedSegmentsForEpoch1.iterator()));
    }

    @Test
    public void testAPIsWithInvalidArgs() {
        RemoteLogMetadataCache cache = new RemoteLogMetadataCache();

        Assertions.assertThrows(NullPointerException.class, () -> cache.addCopyInProgressSegment(null));
        Assertions.assertThrows(NullPointerException.class, () -> cache.updateRemoteLogSegmentMetadata(null));

        // Check for invalid state updates to addCopyInProgressSegment method.
        for (RemoteLogSegmentState state : RemoteLogSegmentState.values()) {
            if (state != RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
                RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(
                        new RemoteLogSegmentId(TP0, Uuid.randomUuid()), 0, 100L,
                        -1L, BROKER_ID_0, time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
                RemoteLogSegmentMetadata updatedMetadata = segmentMetadata
                        .createWithUpdates(new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(),
                                time.milliseconds(), state, BROKER_ID_1));
                Assertions.assertThrows(IllegalArgumentException.class, () ->
                        cache.addCopyInProgressSegment(updatedMetadata));
            }
        }

        // Check for updating non existing segment-id.
        Assertions.assertThrows(RemoteResourceNotFoundException.class, () -> {
            RemoteLogSegmentId nonExistingId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
            cache.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(nonExistingId,
                    time.milliseconds(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, BROKER_ID_1));
        });

        // Check for invalid state transition.
        Assertions.assertThrows(IllegalStateException.class, () -> {
            RemoteLogSegmentMetadata segmentMetadata = createSegmentUpdateWithState(cache, Collections.singletonMap(0, 0L), 0,
                    100, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
            cache.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(),
                    time.milliseconds(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, BROKER_ID_1));
        });
    }

    private void checkListSegments(RemoteLogMetadataCache cache,
                                   int leaderEpoch,
                                   RemoteLogSegmentMetadata expectedSegment)
            throws RemoteResourceNotFoundException {
        // cache.listRemoteLogSegments(leaderEpoch) should contain the above segment.
        Iterator<RemoteLogSegmentMetadata> segmentsIter = cache.listRemoteLogSegments(leaderEpoch);
        Assertions.assertTrue(segmentsIter.hasNext() && Objects.equals(segmentsIter.next(), expectedSegment));

        // cache.listAllRemoteLogSegments() should contain the above segment.
        Iterator<RemoteLogSegmentMetadata> allSegmentsIter = cache.listAllRemoteLogSegments();
        Assertions.assertTrue(allSegmentsIter.hasNext() && Objects.equals(allSegmentsIter.next(), expectedSegment));
    }

    private static class EpochOffset {
        final int epoch;
        final long offset;

        private EpochOffset(int epoch, long offset) {
            this.epoch = epoch;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            EpochOffset that = (EpochOffset) o;
            return epoch == that.epoch && offset == that.offset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(epoch, offset);
        }

        @Override
        public String toString() {
            return "EpochOffset{" +
                   "epoch=" + epoch +
                   ", offset=" + offset +
                   '}';
        }
    }
}