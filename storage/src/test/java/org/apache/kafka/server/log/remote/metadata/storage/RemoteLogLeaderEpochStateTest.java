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
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RemoteLogLeaderEpochStateTest {

    TopicPartition tp = new TopicPartition("topic", 0);
    TopicIdPartition tpId = new TopicIdPartition(Uuid.randomUuid(), tp);
    RemoteLogLeaderEpochState epochState = new RemoteLogLeaderEpochState();

    @Test
    void testListAllRemoteLogSegmentsOnEmpty() throws RemoteResourceNotFoundException {
        assertFalse(epochState.listAllRemoteLogSegments(Collections.emptyMap()).hasNext());
    }

    @Test
    void testListAllRemoteLogSegmentsShouldThrowErrorForUnknownSegmentId() {
        RemoteLogSegmentId segmentId1 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        epochState.handleSegmentWithCopySegmentFinishedState(0L, segmentId1, 10L);
        assertThrows(RemoteResourceNotFoundException.class,
            () -> epochState.listAllRemoteLogSegments(Collections.singletonMap(segmentId2, null)));
    }

    @Test
    void testListAllRemoteLogSegmentsShouldReturnSortedSegments() throws RemoteResourceNotFoundException {
        Map<RemoteLogSegmentId, RemoteLogSegmentMetadata> segmentIdToMetadataMap = new HashMap<>();

        // copy started but never finished so marked as unreferenced
        RemoteLogSegmentId segmentId1 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        epochState.handleSegmentWithCopySegmentStartedState(segmentId1);
        segmentIdToMetadataMap.put(segmentId1, createRemoteLogSegmentMetadata(segmentId1, 0L));

        // copy finished successfully
        RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        epochState.handleSegmentWithCopySegmentFinishedState(5L, segmentId2, 10L);
        segmentIdToMetadataMap.put(segmentId2, createRemoteLogSegmentMetadata(segmentId2, 5L));

        // copy finished successfully, but overwritten by the next segment upload so marked as unreferenced.
        RemoteLogSegmentId segmentId3 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        epochState.handleSegmentWithCopySegmentFinishedState(11L, segmentId3, 100L);
        segmentIdToMetadataMap.put(segmentId3, createRemoteLogSegmentMetadata(segmentId3, 11L));

        // copy finished successfully
        RemoteLogSegmentId segmentId4 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        epochState.handleSegmentWithCopySegmentFinishedState(9L, segmentId4, 150L);
        segmentIdToMetadataMap.put(segmentId4, createRemoteLogSegmentMetadata(segmentId4, 9L));

        // segments should be sorted by start-offset
        List<RemoteLogSegmentId> expectedList = Arrays.asList(segmentId1, segmentId2, segmentId4, segmentId3);
        List<RemoteLogSegmentId> actualList = new ArrayList<>();
        epochState.listAllRemoteLogSegments(segmentIdToMetadataMap)
                .forEachRemaining(metadata -> actualList.add(metadata.remoteLogSegmentId()));
        assertEquals(expectedList, actualList);
    }

    @Test
    void handleSegmentWithCopySegmentStartedState() {
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        epochState.handleSegmentWithCopySegmentStartedState(segmentId);
        assertEquals(1, epochState.unreferencedSegmentIds().size());
        assertTrue(epochState.unreferencedSegmentIds().contains(segmentId));
    }

    @Test
    void handleSegmentWithCopySegmentFinishedState() {
        RemoteLogSegmentId segmentId1 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        epochState.handleSegmentWithCopySegmentFinishedState(10L, segmentId1, 100L);
        epochState.handleSegmentWithCopySegmentFinishedState(101L, segmentId2, 200L);

        assertEquals(2, epochState.referencedSegmentIds().size());
        assertEquals(segmentId1, epochState.floorEntry(90L));
        assertEquals(segmentId2, epochState.floorEntry(150L));
        assertTrue(epochState.unreferencedSegmentIds().isEmpty());
        assertEquals(200L, epochState.highestLogOffset());
    }

    @Test
    void handleSegmentWithCopySegmentFinishedStateForOverlappingSegments() {
        RemoteLogSegmentId segmentId1 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        epochState.handleSegmentWithCopySegmentFinishedState(10L, segmentId1, 100L);
        epochState.handleSegmentWithCopySegmentFinishedState(5L, segmentId2, 150L);

        assertEquals(1, epochState.referencedSegmentIds().size());
        assertEquals(segmentId2, epochState.floorEntry(11L));
        assertEquals(1, epochState.unreferencedSegmentIds().size());
        assertTrue(epochState.unreferencedSegmentIds().contains(segmentId1));
        assertEquals(150L, epochState.highestLogOffset());
    }

    @Test
    void handleSegmentWithCopySegmentFinishedStateForMultipleOverlappingSegments() {
        RemoteLogSegmentId segmentId1 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        RemoteLogSegmentId segmentId3 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        RemoteLogSegmentId segmentId4 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());

        epochState.handleSegmentWithCopySegmentFinishedState(10L, segmentId1, 100L);
        epochState.handleSegmentWithCopySegmentFinishedState(5L, segmentId2, 150L);
        epochState.handleSegmentWithCopySegmentFinishedState(148L, segmentId3, 155L);
        epochState.handleSegmentWithCopySegmentFinishedState(4L, segmentId4, 200L);

        assertEquals(1, epochState.referencedSegmentIds().size());
        assertEquals(segmentId4, epochState.floorEntry(11L));
        assertEquals(3, epochState.unreferencedSegmentIds().size());
        assertTrue(epochState.unreferencedSegmentIds().containsAll(Arrays.asList(segmentId1, segmentId2, segmentId3)));
        assertEquals(200L, epochState.highestLogOffset());
    }

    @Test
    void handleSegmentWithCopySegmentFinishedStateForDuplicateSegments() {
        RemoteLogSegmentId segmentId1 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        epochState.handleSegmentWithCopySegmentFinishedState(10L, segmentId1, 100L);
        epochState.handleSegmentWithCopySegmentFinishedState(10L, segmentId2, 100L);

        assertEquals(segmentId2, epochState.floorEntry(11L));
        assertEquals(1, epochState.unreferencedSegmentIds().size());
        assertTrue(epochState.unreferencedSegmentIds().contains(segmentId1));
        assertEquals(100L, epochState.highestLogOffset());
    }

    @Test
    void handleSegmentWithCopySegmentFinishedStateForSegmentsWithSameStartOffset() {
        RemoteLogSegmentId segmentId1 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        epochState.handleSegmentWithCopySegmentFinishedState(10L, segmentId1, 100L);
        epochState.handleSegmentWithCopySegmentFinishedState(10L, segmentId2, 150L);

        assertEquals(segmentId2, epochState.floorEntry(11L));
        assertEquals(segmentId2, epochState.floorEntry(111L));
        assertEquals(1, epochState.unreferencedSegmentIds().size());
        assertEquals(150L, epochState.highestLogOffset());
    }

    @Test
    void handleSegmentWithDeleteSegmentStartedState() {
        RemoteLogSegmentId segmentId1 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        epochState.handleSegmentWithCopySegmentFinishedState(10L, segmentId1, 100L);
        epochState.handleSegmentWithCopySegmentFinishedState(101L, segmentId2, 200L);
        assertEquals(2, epochState.referencedSegmentIds().size());

        epochState.handleSegmentWithDeleteSegmentStartedState(10L, segmentId1);
        epochState.handleSegmentWithDeleteSegmentStartedState(101L, segmentId2);
        assertTrue(epochState.referencedSegmentIds().isEmpty());
        assertEquals(2, epochState.unreferencedSegmentIds().size());
        assertTrue(epochState.unreferencedSegmentIds().containsAll(Arrays.asList(segmentId1, segmentId2)));
    }

    @Test
    void handleSegmentWithDeleteSegmentFinishedState() {
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(tpId, Uuid.randomUuid());
        epochState.handleSegmentWithCopySegmentStartedState(segmentId);
        assertEquals(1, epochState.unreferencedSegmentIds().size());

        epochState.handleSegmentWithDeleteSegmentFinishedState(segmentId);
        assertTrue(epochState.unreferencedSegmentIds().isEmpty());
    }

    private RemoteLogSegmentMetadata createRemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId,
                                                                    long startOffset) {
        RemoteLogSegmentMetadata metadata = mock(RemoteLogSegmentMetadata.class);
        when(metadata.remoteLogSegmentId()).thenReturn(remoteLogSegmentId);
        when(metadata.startOffset()).thenReturn(startOffset);
        return metadata;
    }
}