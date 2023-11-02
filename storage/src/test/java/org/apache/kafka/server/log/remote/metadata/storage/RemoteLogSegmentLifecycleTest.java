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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class RemoteLogSegmentLifecycleTest {
    private static final Logger log = LoggerFactory.getLogger(RemoteLogSegmentLifecycleTest.class);

    private static final int SEG_SIZE = 1024 * 1024;
    private static final int BROKER_ID_0 = 0;
    private static final int BROKER_ID_1 = 1;

    private final TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
    private final Time time = new MockTime(1);

    @ParameterizedTest(name = "remoteLogSegmentLifecycleManager = {0}")
    @MethodSource("remoteLogSegmentLifecycleManagers")
    public void testRemoteLogSegmentLifeCycle(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager) throws Exception {
        try {
            remoteLogSegmentLifecycleManager.initialize(topicIdPartition);

            // segment 0
            // offsets: [0-100]
            // leader epochs (0,0), (1,20), (2,80)
            Map<Integer, Long> segment0LeaderEpochs = new HashMap<>();
            segment0LeaderEpochs.put(0, 0L);
            segment0LeaderEpochs.put(1, 20L);
            segment0LeaderEpochs.put(2, 80L);
            RemoteLogSegmentId segment0Id = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
            RemoteLogSegmentMetadata segment0Metadata = new RemoteLogSegmentMetadata(segment0Id, 0L, 100L,
                                                                                     -1L, BROKER_ID_0, time.milliseconds(), SEG_SIZE,
                                                                                     segment0LeaderEpochs);
            remoteLogSegmentLifecycleManager.addRemoteLogSegmentMetadata(segment0Metadata);

            // We should not get this as the segment is still getting copied and it is not yet considered successful until
            // it reaches RemoteLogSegmentState.COPY_SEGMENT_FINISHED.
            Assertions.assertFalse(remoteLogSegmentLifecycleManager.remoteLogSegmentMetadata(40, 1).isPresent());

            // Check that these leader epochs are not to be considered for highestOffsetForEpoch API as they are still getting copied.
            Stream.of(0, 1, 2).forEach(epoch -> {
                try {
                    Assertions.assertFalse(remoteLogSegmentLifecycleManager.highestOffsetForEpoch(epoch).isPresent());
                } catch (RemoteStorageException e) {
                    Assertions.fail(e);
                }
            });

            RemoteLogSegmentMetadataUpdate segment0Update = new RemoteLogSegmentMetadataUpdate(
                    segment0Id, time.milliseconds(), Optional.empty(),
                    RemoteLogSegmentState.COPY_SEGMENT_FINISHED, BROKER_ID_1);
            remoteLogSegmentLifecycleManager.updateRemoteLogSegmentMetadata(segment0Update);
            RemoteLogSegmentMetadata expectedSegment0Metadata = segment0Metadata.createWithUpdates(segment0Update);

            // segment 1
            // offsets: [101 - 200]
            // no changes in leadership with in this segment
            // leader epochs (2, 101)
            Map<Integer, Long> segment1LeaderEpochs = Collections.singletonMap(2, 101L);
            RemoteLogSegmentMetadata segment1Metadata = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager, segment1LeaderEpochs, 101L,
                                                                                     200L,
                                                                                     RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

            // segment 2
            // offsets: [201 - 300]
            // moved to epoch 3 in between
            // leader epochs (2, 201), (3, 240)
            Map<Integer, Long> segment2LeaderEpochs = new HashMap<>();
            segment2LeaderEpochs.put(2, 201L);
            segment2LeaderEpochs.put(3, 240L);
            RemoteLogSegmentMetadata segment2Metadata = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager, segment2LeaderEpochs, 201L,
                                                                                     300L,
                                                                                     RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

            // segment 3
            // offsets: [250 - 400]
            // leader epochs (3, 250), (4, 370)
            Map<Integer, Long> segment3LeaderEpochs = new HashMap<>();
            segment3LeaderEpochs.put(3, 250L);
            segment3LeaderEpochs.put(4, 370L);
            RemoteLogSegmentMetadata segment3Metadata = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager, segment3LeaderEpochs, 250L,
                                                                                     400L,
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
                Optional<RemoteLogSegmentMetadata> segmentMetadata = remoteLogSegmentLifecycleManager
                        .remoteLogSegmentMetadata(epochOffset.epoch, epochOffset.offset);
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
            remoteLogSegmentLifecycleManager
                    .updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(expectedSegment0Metadata.remoteLogSegmentId(),
                                                                                       time.milliseconds(),
                                                                                       Optional.empty(),
                                                                                       RemoteLogSegmentState.DELETE_SEGMENT_STARTED,
                                                                                       BROKER_ID_1));
            Assertions.assertFalse(remoteLogSegmentLifecycleManager.remoteLogSegmentMetadata(0, 10).isPresent());

            // Update segment with state as DELETE_SEGMENT_FINISHED.
            // It should not be available when we search for that segment.
            remoteLogSegmentLifecycleManager
                    .updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(expectedSegment0Metadata.remoteLogSegmentId(),
                                                                                       time.milliseconds(),
                                                                                       Optional.empty(),
                                                                                       RemoteLogSegmentState.DELETE_SEGMENT_FINISHED,
                                                                                       BROKER_ID_1));
            Assertions.assertFalse(remoteLogSegmentLifecycleManager.remoteLogSegmentMetadata(0, 10).isPresent());

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
                Optional<Long> offset = remoteLogSegmentLifecycleManager.highestOffsetForEpoch(epoch);
                log.debug("Fetching highest offset for epoch: {} , returned: {} , expected: {}", epoch, offset, expectedOffset);
                Assertions.assertEquals(Optional.of(expectedOffset), offset);
            }

            // Search for non existing leader epoch
            Optional<Long> highestOffsetForEpoch5 = remoteLogSegmentLifecycleManager.highestOffsetForEpoch(5);
            Assertions.assertFalse(highestOffsetForEpoch5.isPresent());
        } finally {
            Utils.closeQuietly(remoteLogSegmentLifecycleManager, "RemoteLogSegmentLifecycleManager");
        }
    }

    private RemoteLogSegmentMetadata createSegmentUpdateWithState(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager,
                                                                  Map<Integer, Long> segmentLeaderEpochs,
                                                                  long startOffset,
                                                                  long endOffset,
                                                                  RemoteLogSegmentState state)
            throws RemoteStorageException {
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId, startOffset, endOffset, -1L, BROKER_ID_0,
                                                                                time.milliseconds(), SEG_SIZE, segmentLeaderEpochs);
        remoteLogSegmentLifecycleManager.addRemoteLogSegmentMetadata(segmentMetadata);

        RemoteLogSegmentMetadataUpdate segMetadataUpdate = new RemoteLogSegmentMetadataUpdate(segmentId, time.milliseconds(),
                Optional.empty(),
                state, BROKER_ID_1);
        remoteLogSegmentLifecycleManager.updateRemoteLogSegmentMetadata(segMetadataUpdate);

        return segmentMetadata.createWithUpdates(segMetadataUpdate);
    }

    private static class EpochOffset {
        final int epoch;
        final long offset;

        private EpochOffset(int epoch,
                            long offset) {
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

    private static Collection<Arguments> remoteLogSegmentLifecycleManagers() {
        return Arrays.asList(Arguments.of(new RemoteLogMetadataCacheWrapper()),
                             Arguments.of(new TopicBasedRemoteLogMetadataManagerWrapper()));
    }

    private void checkListSegments(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager,
                                   int leaderEpoch,
                                   RemoteLogSegmentMetadata expectedSegment)
            throws RemoteStorageException {
        // cache.listRemoteLogSegments(leaderEpoch) should contain the above segment.
        Iterator<RemoteLogSegmentMetadata> segmentsIter = remoteLogSegmentLifecycleManager.listRemoteLogSegments(leaderEpoch);
        Assertions.assertTrue(segmentsIter.hasNext());
        Assertions.assertEquals(expectedSegment, segmentsIter.next());

        // cache.listAllRemoteLogSegments() should contain the above segment.
        Iterator<RemoteLogSegmentMetadata> allSegmentsIter = remoteLogSegmentLifecycleManager.listAllRemoteLogSegments();
        Assertions.assertTrue(allSegmentsIter.hasNext());
        Assertions.assertEquals(expectedSegment, allSegmentsIter.next());
    }

    @ParameterizedTest(name = "remoteLogSegmentLifecycleManager = {0}")
    @MethodSource("remoteLogSegmentLifecycleManagers")
    public void testCacheSegmentWithCopySegmentStartedState(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager) throws Exception {

        try {
            remoteLogSegmentLifecycleManager.initialize(topicIdPartition);

            // Create a segment with state COPY_SEGMENT_STARTED, and check for searching that segment and listing the
            // segments.
            RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
            RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId, 0L, 50L, -1L, BROKER_ID_0,
                                                                                    time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
            remoteLogSegmentLifecycleManager.addRemoteLogSegmentMetadata(segmentMetadata);

            // This segment should not be available as the state is not reached to COPY_SEGMENT_FINISHED.
            Optional<RemoteLogSegmentMetadata> segMetadataForOffset0Epoch0 = remoteLogSegmentLifecycleManager.remoteLogSegmentMetadata(0, 0);
            Assertions.assertFalse(segMetadataForOffset0Epoch0.isPresent());

            // cache.listRemoteLogSegments APIs should contain the above segment.
            checkListSegments(remoteLogSegmentLifecycleManager, 0, segmentMetadata);
        } finally {
            Utils.closeQuietly(remoteLogSegmentLifecycleManager, "RemoteLogSegmentLifecycleManager");
        }
    }

    @ParameterizedTest(name = "remoteLogSegmentLifecycleManager = {0}")
    @MethodSource("remoteLogSegmentLifecycleManagers")
    public void testCacheSegmentWithCopySegmentFinishedState(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager) throws Exception {
        try {
            remoteLogSegmentLifecycleManager.initialize(topicIdPartition);

            // Create a segment and move it to state COPY_SEGMENT_FINISHED. and check for searching that segment and
            // listing the segments.
            RemoteLogSegmentMetadata segmentMetadata = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager,
                                                                                    Collections.singletonMap(0, 101L),
                                                                                    101L, 200L, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

            // Search should return the above segment.
            Optional<RemoteLogSegmentMetadata> segMetadataForOffset150 = remoteLogSegmentLifecycleManager.remoteLogSegmentMetadata(0, 150);
            Assertions.assertEquals(Optional.of(segmentMetadata), segMetadataForOffset150);

            // cache.listRemoteLogSegments should contain the above segments.
            checkListSegments(remoteLogSegmentLifecycleManager, 0, segmentMetadata);
        } finally {
            Utils.closeQuietly(remoteLogSegmentLifecycleManager, "RemoteLogSegmentLifecycleManager");
        }
    }

    @ParameterizedTest(name = "remoteLogSegmentLifecycleManager = {0}")
    @MethodSource("remoteLogSegmentLifecycleManagers")
    public void testCacheSegmentWithDeleteSegmentStartedState(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager) throws Exception {
        try {
            remoteLogSegmentLifecycleManager.initialize(topicIdPartition);

            // Create a segment and move it to state DELETE_SEGMENT_STARTED, and check for searching that segment and
            // listing the segments.
            RemoteLogSegmentMetadata segmentMetadata = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager,
                                                                                    Collections.singletonMap(0, 201L),
                                                                                    201L, 300L, RemoteLogSegmentState.DELETE_SEGMENT_STARTED);

            // Search should not return the above segment as their leader epoch state is cleared.
            Optional<RemoteLogSegmentMetadata> segmentMetadataForOffset250Epoch0 = remoteLogSegmentLifecycleManager.remoteLogSegmentMetadata(0, 250);
            Assertions.assertFalse(segmentMetadataForOffset250Epoch0.isPresent());

            checkListSegments(remoteLogSegmentLifecycleManager, 0, segmentMetadata);
        } finally {
            Utils.closeQuietly(remoteLogSegmentLifecycleManager, "RemoteLogSegmentLifecycleManager");
        }
    }

    @ParameterizedTest(name = "remoteLogSegmentLifecycleManager = {0}")
    @MethodSource("remoteLogSegmentLifecycleManagers")
    public void testCacheSegmentsWithDeleteSegmentFinishedState(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager) throws Exception {
        try {
            remoteLogSegmentLifecycleManager.initialize(topicIdPartition);

            // Create a segment and move it to state DELETE_SEGMENT_FINISHED, and check for searching that segment and
            // listing the segments.
            RemoteLogSegmentMetadata segmentMetadata = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager,
                                                                                    Collections.singletonMap(0, 301L),
                                                                                    301L, 400L, RemoteLogSegmentState.DELETE_SEGMENT_STARTED);

            // Search should not return the above segment as their leader epoch state is cleared.
            Assertions.assertFalse(remoteLogSegmentLifecycleManager.remoteLogSegmentMetadata(0, 350).isPresent());

            RemoteLogSegmentMetadataUpdate segmentMetadataUpdate = new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(),
                                                                                                      time.milliseconds(),
                                                                                                      Optional.empty(),
                                                                                                      RemoteLogSegmentState.DELETE_SEGMENT_FINISHED,
                                                                                                      BROKER_ID_1);
            remoteLogSegmentLifecycleManager.updateRemoteLogSegmentMetadata(segmentMetadataUpdate);

            // listRemoteLogSegments(0) and listRemoteLogSegments() should not contain the above segment.
            Assertions.assertFalse(remoteLogSegmentLifecycleManager.listRemoteLogSegments(0).hasNext());
            Assertions.assertFalse(remoteLogSegmentLifecycleManager.listAllRemoteLogSegments().hasNext());
        } finally {
            Utils.closeQuietly(remoteLogSegmentLifecycleManager, "RemoteLogSegmentLifecycleManager");
        }
    }

    @ParameterizedTest(name = "remoteLogSegmentLifecycleManager = {0}")
    @MethodSource("remoteLogSegmentLifecycleManagers")
    public void testCacheListSegments(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager) throws Exception {
        try {
            remoteLogSegmentLifecycleManager.initialize(topicIdPartition);

            // Create a few segments and add them to the cache.
            RemoteLogSegmentMetadata segment0 = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager, Collections.singletonMap(0, 0L), 0,
                                                                             100,
                                                                             RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
            RemoteLogSegmentMetadata segment1 = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager, Collections.singletonMap(0, 101L), 101,
                                                                             200,
                                                                             RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
            Map<Integer, Long> segment2LeaderEpochs = new HashMap<>();
            segment2LeaderEpochs.put(0, 201L);
            segment2LeaderEpochs.put(1, 301L);
            RemoteLogSegmentMetadata segment2 = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager, segment2LeaderEpochs, 201, 400,
                                                                             RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

            // listRemoteLogSegments(0) and listAllRemoteLogSegments() should contain all the above segments.
            List<RemoteLogSegmentMetadata> expectedSegmentsForEpoch0 = Arrays.asList(segment0, segment1, segment2);
            Assertions.assertTrue(TestUtils.sameElementsWithOrder(remoteLogSegmentLifecycleManager.listRemoteLogSegments(0),
                                                                  expectedSegmentsForEpoch0.iterator()));
            Assertions.assertTrue(TestUtils.sameElementsWithoutOrder(remoteLogSegmentLifecycleManager.listAllRemoteLogSegments(),
                                                                     expectedSegmentsForEpoch0.iterator()));

            // listRemoteLogSegments(1) should contain only segment2.
            List<RemoteLogSegmentMetadata> expectedSegmentsForEpoch1 = Collections.singletonList(segment2);
            Assertions.assertTrue(TestUtils.sameElementsWithOrder(remoteLogSegmentLifecycleManager.listRemoteLogSegments(1),
                                                                  expectedSegmentsForEpoch1.iterator()));
        } finally {
            Utils.closeQuietly(remoteLogSegmentLifecycleManager, "RemoteLogSegmentLifecycleManager");
        }
    }

    /**
     * This is a wrapper with {@link TopicBasedRemoteLogMetadataManager} implementing {@link RemoteLogSegmentLifecycleManager}.
     * This is passed to {@link #testRemoteLogSegmentLifeCycle(RemoteLogSegmentLifecycleManager)} to test
     * {@code RemoteLogMetadataCache} for several lifecycle operations.
     * <p>
     * This starts a Kafka cluster with {@link #initialize(Set, boolean)} )} with {@link #brokerCount()} no of servers. It also
     * creates the remote log metadata topic required for {@code TopicBasedRemoteLogMetadataManager}. This cluster will
     * be stopped by invoking {@link #close()}.
     */
    static class TopicBasedRemoteLogMetadataManagerWrapper extends TopicBasedRemoteLogMetadataManagerHarness implements RemoteLogSegmentLifecycleManager {

        private TopicIdPartition topicIdPartition;

        @Override
        public synchronized void initialize(TopicIdPartition topicIdPartition) {
            this.topicIdPartition = topicIdPartition;
            super.initialize(Collections.singleton(topicIdPartition), true);
        }

        @Override
        public void addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata segmentMetadata) throws RemoteStorageException {
            try {
                // Wait until the segment is added successfully.
                remoteLogMetadataManager().addRemoteLogSegmentMetadata(segmentMetadata).get();
            } catch (Exception e) {
                throw new RemoteStorageException(e);
            }
        }

        @Override
        public void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate segmentMetadataUpdate) throws RemoteStorageException {
            try {
                // Wait until the segment is updated successfully.
                remoteLogMetadataManager().updateRemoteLogSegmentMetadata(segmentMetadataUpdate).get();
            } catch (Exception e) {
                throw new RemoteStorageException(e);
            }
        }

        @Override
        public Optional<Long> highestOffsetForEpoch(int leaderEpoch) throws RemoteStorageException {
            return remoteLogMetadataManager().highestOffsetForEpoch(topicIdPartition, leaderEpoch);
        }

        @Override
        public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(int leaderEpoch,
                                                                           long offset) throws RemoteStorageException {
            return remoteLogMetadataManager().remoteLogSegmentMetadata(topicIdPartition, leaderEpoch, offset);
        }

        @Override
        public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(int leaderEpoch) throws RemoteStorageException {
            return remoteLogMetadataManager().listRemoteLogSegments(topicIdPartition, leaderEpoch);
        }

        @Override
        public Iterator<RemoteLogSegmentMetadata> listAllRemoteLogSegments() throws RemoteStorageException {
            return remoteLogMetadataManager().listRemoteLogSegments(topicIdPartition);
        }

        @Override
        public void close() throws IOException {
            super.close();
        }

    }

    /**
     * This is a wrapper with {@link RemoteLogMetadataCache} implementing {@link RemoteLogSegmentLifecycleManager}.
     * This is passed to {@link #testRemoteLogSegmentLifeCycle(RemoteLogSegmentLifecycleManager)} to test
     * {@code RemoteLogMetadataCache} for several lifecycle operations.
     */
    static class RemoteLogMetadataCacheWrapper implements RemoteLogSegmentLifecycleManager {

        private final RemoteLogMetadataCache metadataCache = new RemoteLogMetadataCache();

        @Override
        public void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate segmentMetadataUpdate) throws RemoteStorageException {
            metadataCache.updateRemoteLogSegmentMetadata(segmentMetadataUpdate);
        }

        @Override
        public Optional<Long> highestOffsetForEpoch(int epoch) {
            return metadataCache.highestOffsetForEpoch(epoch);
        }

        @Override
        public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(int leaderEpoch, long offset) {
            return metadataCache.remoteLogSegmentMetadata(leaderEpoch, offset);
        }

        @Override
        public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(int leaderEpoch) throws RemoteResourceNotFoundException {
            return metadataCache.listRemoteLogSegments(leaderEpoch);
        }

        @Override
        public Iterator<RemoteLogSegmentMetadata> listAllRemoteLogSegments() {
            return metadataCache.listAllRemoteLogSegments();
        }

        @Override
        public void addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata segmentMetadata) {
            metadataCache.addCopyInProgressSegment(segmentMetadata);
        }
    }
}
