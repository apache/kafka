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
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState.COPY_SEGMENT_FINISHED;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState.DELETE_SEGMENT_FINISHED;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState.DELETE_SEGMENT_STARTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ClusterTestDefaults(brokers = 3)
@ExtendWith(value = ClusterTestExtensions.class)
public class RemoteLogSegmentLifecycleTest {

    private final int segSize = 1048576;
    private final int brokerId0 = 0;
    private final int brokerId1 = 1;
    private final Uuid topicId = Uuid.randomUuid();
    private final TopicPartition tp = new TopicPartition("foo", 0);
    private final TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, tp);
    private final Time time = Time.SYSTEM;
    private final RemotePartitionMetadataStore spyRemotePartitionMetadataStore = spy(new RemotePartitionMetadataStore());
    private final ClusterInstance clusterInstance;

    RemoteLogSegmentLifecycleTest(ClusterInstance clusterInstance) {     // Constructor injections
        this.clusterInstance = clusterInstance;
    }

    private RemoteLogMetadataManager createTopicBasedRemoteLogMetadataManager() {
        return RemoteLogMetadataManagerTestUtils.builder()
                .bootstrapServers(clusterInstance.bootstrapServers())
                .startConsumerThread(true)
                .remotePartitionMetadataStore(() -> spyRemotePartitionMetadataStore)
                .build();
    }

    @ClusterTest
    public void testRemoteLogSegmentLifeCycle() throws Exception {
        try (RemoteLogMetadataManager metadataManager = createTopicBasedRemoteLogMetadataManager()) {
            metadataManager.onPartitionLeadershipChanges(Collections.singleton(topicIdPartition), Collections.emptySet());

            // segment 0
            // offsets: [0-100]
            // leader epochs (0,0), (1,20), (2,80)
            Map<Integer, Long> leaderEpochSegment0 = new HashMap<>();
            leaderEpochSegment0.put(0, 0L);
            leaderEpochSegment0.put(1, 20L);
            leaderEpochSegment0.put(2, 80L);
            RemoteLogSegmentId segmentId0 = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
            RemoteLogSegmentMetadata metadataSegment0 = new RemoteLogSegmentMetadata(segmentId0, 0L,
                    100L, -1L, brokerId0, time.milliseconds(), segSize, leaderEpochSegment0);
            metadataManager.addRemoteLogSegmentMetadata(metadataSegment0).get();
            verify(spyRemotePartitionMetadataStore).handleRemoteLogSegmentMetadata(metadataSegment0);

            // We should not get this as the segment is still getting copied and it is not yet considered successful until
            // it reaches COPY_SEGMENT_FINISHED.
            assertFalse(metadataManager.remoteLogSegmentMetadata(topicIdPartition, 1, 40).isPresent());

            // Check that these leader epochs are not to be considered for highestOffsetForEpoch API as they are still getting copied.
            for (int leaderEpoch = 0; leaderEpoch <= 2; leaderEpoch++) {
                assertFalse(metadataManager.highestOffsetForEpoch(topicIdPartition, leaderEpoch).isPresent());
            }

            RemoteLogSegmentMetadataUpdate metadataUpdateSegment0 = new RemoteLogSegmentMetadataUpdate(
                    segmentId0, time.milliseconds(), Optional.empty(),
                    COPY_SEGMENT_FINISHED, brokerId1);
            metadataManager.updateRemoteLogSegmentMetadata(metadataUpdateSegment0).get();
            verify(spyRemotePartitionMetadataStore).handleRemoteLogSegmentMetadataUpdate(metadataUpdateSegment0);
            metadataSegment0 = metadataSegment0.createWithUpdates(metadataUpdateSegment0);

            // segment 1
            // offsets: [101 - 200]
            // no changes in leadership with in this segment
            // leader epochs (2, 101)
            Map<Integer, Long> leaderEpochSegment1 = Collections.singletonMap(2, 101L);
            RemoteLogSegmentMetadata metadataSegment1 = upsertSegmentState(metadataManager, leaderEpochSegment1,
                    101L, 200L, COPY_SEGMENT_FINISHED);

            // segment 2
            // offsets: [201 - 300]
            // moved to epoch 3 in between
            // leader epochs (2, 201), (3, 240)
            Map<Integer, Long> leaderEpochSegment2 = new HashMap<>();
            leaderEpochSegment2.put(2, 201L);
            leaderEpochSegment2.put(3, 240L);
            RemoteLogSegmentMetadata metadataSegment2 = upsertSegmentState(metadataManager, leaderEpochSegment2,
                    201L, 300L, COPY_SEGMENT_FINISHED);

            // segment 3
            // offsets: [250 - 400]
            // leader epochs (3, 250), (4, 370)
            Map<Integer, Long> leaderEpochSegment3 = new HashMap<>();
            leaderEpochSegment3.put(3, 250L);
            leaderEpochSegment3.put(4, 370L);
            RemoteLogSegmentMetadata metadataSegment3 = upsertSegmentState(metadataManager, leaderEpochSegment3,
                    250L, 400L, COPY_SEGMENT_FINISHED);

            //////////////////////////////////////////////////////////////////////////////////////////
            // Four segments are added with different boundaries and leader epochs.
            // Search for cache.remoteLogSegmentMetadata(leaderEpoch, offset)  for different
            // epochs and offsets
            //////////////////////////////////////////////////////////////////////////////////////////

            Map<EpochEntry, RemoteLogSegmentMetadata> expectedEpochEntryToMetadata = new HashMap<>();
            // Existing metadata entries.
            expectedEpochEntryToMetadata.put(new EpochEntry(1, 40), metadataSegment0);
            expectedEpochEntryToMetadata.put(new EpochEntry(2, 110), metadataSegment1);
            expectedEpochEntryToMetadata.put(new EpochEntry(3, 240), metadataSegment2);
            expectedEpochEntryToMetadata.put(new EpochEntry(3, 250), metadataSegment3);
            expectedEpochEntryToMetadata.put(new EpochEntry(4, 375), metadataSegment3);

            // Non existing metadata entries.
            // Search for offset 110, epoch 1, and it should not exist.
            expectedEpochEntryToMetadata.put(new EpochEntry(1, 110), null);
            // Search for non existing offset 401, epoch 4.
            expectedEpochEntryToMetadata.put(new EpochEntry(4, 401), null);
            // Search for non existing epoch 5.
            expectedEpochEntryToMetadata.put(new EpochEntry(5, 301), null);

            for (Map.Entry<EpochEntry, RemoteLogSegmentMetadata> entry : expectedEpochEntryToMetadata.entrySet()) {
                EpochEntry epochEntry = entry.getKey();
                Optional<RemoteLogSegmentMetadata> actualMetadataOpt = metadataManager
                        .remoteLogSegmentMetadata(topicIdPartition, epochEntry.epoch, epochEntry.startOffset);
                RemoteLogSegmentMetadata expectedSegmentMetadata = entry.getValue();
                if (expectedSegmentMetadata != null) {
                    assertEquals(Optional.of(expectedSegmentMetadata), actualMetadataOpt);
                } else {
                    assertFalse(actualMetadataOpt.isPresent());
                }
            }

            // Update segment with state as DELETE_SEGMENT_STARTED.
            // It should not be available when we search for that segment.
            RemoteLogSegmentMetadataUpdate metadataDeleteStartedSegment0 =
                    new RemoteLogSegmentMetadataUpdate(metadataSegment0.remoteLogSegmentId(), time.milliseconds(),
                            Optional.empty(), DELETE_SEGMENT_STARTED, brokerId1);
            metadataManager.updateRemoteLogSegmentMetadata(metadataDeleteStartedSegment0).get();
            assertFalse(metadataManager.remoteLogSegmentMetadata(topicIdPartition, 0, 10).isPresent());

            // Update segment with state as DELETE_SEGMENT_FINISHED.
            // It should not be available when we search for that segment.
            RemoteLogSegmentMetadataUpdate metadataDeleteFinishedSegment0 =
                    new RemoteLogSegmentMetadataUpdate(metadataSegment0.remoteLogSegmentId(), time.milliseconds(),
                            Optional.empty(), DELETE_SEGMENT_FINISHED, brokerId1);
            metadataManager.updateRemoteLogSegmentMetadata(metadataDeleteFinishedSegment0).get();
            assertFalse(metadataManager.remoteLogSegmentMetadata(topicIdPartition, 0, 10).isPresent());

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
                Optional<Long> offset = metadataManager.highestOffsetForEpoch(topicIdPartition, epoch);
                assertEquals(Optional.of(expectedOffset), offset);
            }

            // Search for non existing leader epoch
            Optional<Long> highestOffsetForEpoch5 = metadataManager.highestOffsetForEpoch(topicIdPartition, 5);
            assertFalse(highestOffsetForEpoch5.isPresent());
        }
    }

    private RemoteLogSegmentMetadata upsertSegmentState(RemoteLogMetadataManager metadataManager,
                                                        Map<Integer, Long> segmentLeaderEpochs,
                                                        long startOffset,
                                                        long endOffset,
                                                        RemoteLogSegmentState state)
            throws RemoteStorageException, ExecutionException, InterruptedException {
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId, startOffset, endOffset,
                -1L, brokerId0, time.milliseconds(), segSize, segmentLeaderEpochs);
        metadataManager.addRemoteLogSegmentMetadata(segmentMetadata).get();
        verify(spyRemotePartitionMetadataStore).handleRemoteLogSegmentMetadata(segmentMetadata);

        RemoteLogSegmentMetadataUpdate segMetadataUpdate = new RemoteLogSegmentMetadataUpdate(segmentId,
                time.milliseconds(), Optional.empty(), state, brokerId1);
        metadataManager.updateRemoteLogSegmentMetadata(segMetadataUpdate).get();
        verify(spyRemotePartitionMetadataStore).handleRemoteLogSegmentMetadataUpdate(segMetadataUpdate);
        return segmentMetadata.createWithUpdates(segMetadataUpdate);
    }

    private void checkListSegments(RemoteLogMetadataManager metadataManager,
                                   int leaderEpoch,
                                   RemoteLogSegmentMetadata expectedMetadata)
            throws RemoteStorageException {
        // cache.listRemoteLogSegments(leaderEpoch) should contain the above segment.
        Iterator<RemoteLogSegmentMetadata> metadataIter =
                metadataManager.listRemoteLogSegments(topicIdPartition, leaderEpoch);
        assertTrue(metadataIter.hasNext());
        assertEquals(expectedMetadata, metadataIter.next());

        // cache.listAllRemoteLogSegments() should contain the above segment.
        Iterator<RemoteLogSegmentMetadata> allMetadataIter = metadataManager.listRemoteLogSegments(topicIdPartition);
        assertTrue(allMetadataIter.hasNext());
        assertEquals(expectedMetadata, allMetadataIter.next());
    }

    @ClusterTest
    public void testCacheSegmentWithCopySegmentStartedState() throws Exception {
        try (RemoteLogMetadataManager metadataManager = createTopicBasedRemoteLogMetadataManager()) {
            metadataManager.onPartitionLeadershipChanges(Collections.singleton(topicIdPartition), Collections.emptySet());
            // Create a segment with state COPY_SEGMENT_STARTED, and check for searching that segment and listing the
            // segments.
            RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
            RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId, 0L, 50L,
                    -1L, brokerId0, time.milliseconds(), segSize, Collections.singletonMap(0, 0L));
            metadataManager.addRemoteLogSegmentMetadata(segmentMetadata).get();
            verify(spyRemotePartitionMetadataStore).handleRemoteLogSegmentMetadata(segmentMetadata);

            // This segment should not be available as the state is not reached to COPY_SEGMENT_FINISHED.
            Optional<RemoteLogSegmentMetadata> segMetadataForOffset0Epoch0 =
                    metadataManager.remoteLogSegmentMetadata(topicIdPartition, 0, 0);
            assertFalse(segMetadataForOffset0Epoch0.isPresent());

            // cache.listRemoteLogSegments APIs should contain the above segment.
            checkListSegments(metadataManager, 0, segmentMetadata);
        }
    }

    @ClusterTest
    public void testCacheSegmentWithCopySegmentFinishedState() throws Exception {
        try (RemoteLogMetadataManager metadataManager = createTopicBasedRemoteLogMetadataManager()) {
            metadataManager.onPartitionLeadershipChanges(Collections.singleton(topicIdPartition), Collections.emptySet());
            // Create a segment and move it to state COPY_SEGMENT_FINISHED. and check for searching that segment and
            // listing the segments.
            RemoteLogSegmentMetadata segmentMetadata = upsertSegmentState(
                    metadataManager, Collections.singletonMap(0, 101L), 101L, 200L, COPY_SEGMENT_FINISHED);

            // Search should return the above segment.
            Optional<RemoteLogSegmentMetadata> segMetadataForOffset150 =
                    metadataManager.remoteLogSegmentMetadata(topicIdPartition, 0, 150);
            assertEquals(Optional.of(segmentMetadata), segMetadataForOffset150);

            // cache.listRemoteLogSegments should contain the above segments.
            checkListSegments(metadataManager, 0, segmentMetadata);
        }
    }

    @ClusterTest
    public void testCacheSegmentWithDeleteSegmentStartedState() throws Exception {
        try (RemoteLogMetadataManager metadataManager = createTopicBasedRemoteLogMetadataManager()) {
            metadataManager.onPartitionLeadershipChanges(Collections.singleton(topicIdPartition), Collections.emptySet());
            // Create a segment and move it to state DELETE_SEGMENT_STARTED, and check for searching that segment and
            // listing the segments.
            RemoteLogSegmentMetadata segmentMetadata = upsertSegmentState(
                    metadataManager, Collections.singletonMap(0, 201L), 201L, 300L, DELETE_SEGMENT_STARTED);

            // Search should not return the above segment as their leader epoch state is cleared.
            Optional<RemoteLogSegmentMetadata> segmentMetadataForOffset250Epoch0 =
                    metadataManager.remoteLogSegmentMetadata(topicIdPartition, 0, 250);
            assertFalse(segmentMetadataForOffset250Epoch0.isPresent());
            checkListSegments(metadataManager, 0, segmentMetadata);
        }
    }

    @ClusterTest
    public void testCacheSegmentsWithDeleteSegmentFinishedState() throws Exception {
        try (RemoteLogMetadataManager metadataManager = createTopicBasedRemoteLogMetadataManager()) {
            metadataManager.onPartitionLeadershipChanges(Collections.singleton(topicIdPartition), Collections.emptySet());
            // Create a segment and move it to state DELETE_SEGMENT_FINISHED, and check for searching that segment and
            // listing the segments.
            RemoteLogSegmentMetadata segmentMetadata = upsertSegmentState(
                    metadataManager, Collections.singletonMap(0, 301L), 301L, 400L, DELETE_SEGMENT_STARTED);

            // Search should not return the above segment as their leader epoch state is cleared.
            assertFalse(metadataManager.remoteLogSegmentMetadata(topicIdPartition, 0, 350).isPresent());

            RemoteLogSegmentMetadataUpdate segmentMetadataUpdate = new RemoteLogSegmentMetadataUpdate(
                    segmentMetadata.remoteLogSegmentId(), time.milliseconds(), Optional.empty(),
                    DELETE_SEGMENT_FINISHED, brokerId1);
            metadataManager.updateRemoteLogSegmentMetadata(segmentMetadataUpdate).get();
            verify(spyRemotePartitionMetadataStore).handleRemoteLogSegmentMetadataUpdate(segmentMetadataUpdate);

            // listRemoteLogSegments(0) and listRemoteLogSegments() should not contain the above segment.
            assertFalse(metadataManager.listRemoteLogSegments(topicIdPartition, 0).hasNext());
            assertFalse(metadataManager.listRemoteLogSegments(topicIdPartition).hasNext());
        }
    }

    @ClusterTest
    public void testCacheListSegments() throws Exception {
        try (RemoteLogMetadataManager metadataManager = createTopicBasedRemoteLogMetadataManager()) {
            metadataManager.onPartitionLeadershipChanges(Collections.singleton(topicIdPartition), Collections.emptySet());
            // Create a few segments and add them to the cache.
            RemoteLogSegmentMetadata segment0 = upsertSegmentState(metadataManager, Collections.singletonMap(0, 0L),
                    0, 100, COPY_SEGMENT_FINISHED);
            RemoteLogSegmentMetadata segment1 = upsertSegmentState(metadataManager, Collections.singletonMap(0, 101L),
                    101, 200, COPY_SEGMENT_FINISHED);
            Map<Integer, Long> leaderEpochSegment2 = new HashMap<>();
            leaderEpochSegment2.put(0, 201L);
            leaderEpochSegment2.put(1, 301L);
            RemoteLogSegmentMetadata segment2 = upsertSegmentState(metadataManager, leaderEpochSegment2,
                    201, 400, COPY_SEGMENT_FINISHED);

            // listRemoteLogSegments(0) and listAllRemoteLogSegments() should contain all the above segments.
            List<RemoteLogSegmentMetadata> expectedSegmentsForEpoch0 = Arrays.asList(segment0, segment1, segment2);
            assertTrue(TestUtils.sameElementsWithOrder(
                    expectedSegmentsForEpoch0.iterator(), metadataManager.listRemoteLogSegments(topicIdPartition, 0)));
            assertTrue(TestUtils.sameElementsWithoutOrder(
                    expectedSegmentsForEpoch0.iterator(), metadataManager.listRemoteLogSegments(topicIdPartition)));

            // listRemoteLogSegments(1) should contain only segment2.
            List<RemoteLogSegmentMetadata> expectedSegmentsForEpoch1 = Collections.singletonList(segment2);
            assertTrue(TestUtils.sameElementsWithOrder(
                    expectedSegmentsForEpoch1.iterator(),  metadataManager.listRemoteLogSegments(topicIdPartition, 1)));
        }
    }
}
