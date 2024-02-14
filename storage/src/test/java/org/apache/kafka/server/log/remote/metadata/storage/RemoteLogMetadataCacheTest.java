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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RemoteLogMetadataCacheTest {

    private final TopicPartition tp0 = new TopicPartition("foo", 0);
    private final TopicIdPartition tpId0 = new TopicIdPartition(Uuid.randomUuid(), tp0);
    private final int segmentSize = 1048576;
    private final int brokerId0 = 0;
    private final int brokerId1 = 1;
    private final Time time = new MockTime(1);
    private final RemoteLogMetadataCache cache = new RemoteLogMetadataCache();

    @Test
    public void testCacheAddMetadataOnInvalidArgs() {
        cache.markInitialized();
        assertThrows(NullPointerException.class, () -> cache.addCopyInProgressSegment(null));
        // Check for invalid state updates to addCopyInProgressSegment method.
        for (RemoteLogSegmentState state : RemoteLogSegmentState.values()) {
            if (state != RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
                RemoteLogSegmentId segmentId = new RemoteLogSegmentId(tpId0, Uuid.randomUuid());
                RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId, 0, 100L,
                        -1L, brokerId0, time.milliseconds(), segmentSize, Collections.singletonMap(0, 0L));
                RemoteLogSegmentMetadata updatedMetadata = segmentMetadata.createWithUpdates(
                        new RemoteLogSegmentMetadataUpdate(segmentId, time.milliseconds(), Optional.empty(),
                                state, brokerId1));
                assertThrows(IllegalArgumentException.class, () -> cache.addCopyInProgressSegment(updatedMetadata));
            }
        }
    }

    @ParameterizedTest(name = "isInitialized={0}")
    @ValueSource(booleans = {true, false})
    public void testCacheUpdateMetadataOnInvalidArgs(boolean isInitialized) {
        if (isInitialized) {
            cache.markInitialized();
        }
        assertThrows(NullPointerException.class, () -> cache.updateRemoteLogSegmentMetadata(null));
        for (RemoteLogSegmentState state : RemoteLogSegmentState.values()) {
            if (state != RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
                RemoteLogSegmentId segmentId = new RemoteLogSegmentId(tpId0, Uuid.randomUuid());
                RemoteLogSegmentMetadataUpdate updatedMetadata = new RemoteLogSegmentMetadataUpdate(
                        segmentId, time.milliseconds(), Optional.empty(), state, brokerId1);
                try {
                    cache.updateRemoteLogSegmentMetadata(updatedMetadata);
                    if (isInitialized) {
                        fail("Should throw RemoteResourceNotFoundException when cache is initialized");
                    }
                } catch (RemoteResourceNotFoundException ex) {
                    if (!isInitialized) {
                        fail("Should not throw RemoteResourceNotFoundException when cache is not initialized");
                    }
                }
            }
        }
    }

    @Test
    public void testDropEventOnInvalidStateTransition() throws RemoteResourceNotFoundException {
        cache.markInitialized();
        int leaderEpoch = 5;
        long offset = 10L;
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(tpId0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId, offset, 100L,
                -1L, brokerId0, time.milliseconds(), segmentSize, Collections.singletonMap(leaderEpoch, offset));
        cache.addCopyInProgressSegment(segmentMetadata);

        // invalid-transition-1. COPY_SEGMENT_STARTED -> DELETE_SEGMENT_FINISHED
        RemoteLogSegmentMetadataUpdate updatedMetadata = new RemoteLogSegmentMetadataUpdate(segmentId, time.milliseconds(),
                Optional.empty(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, brokerId1);
        updateAndVerifyCacheContents(updatedMetadata, RemoteLogSegmentState.COPY_SEGMENT_STARTED, leaderEpoch);

        // valid-transition-2: COPY_SEGMENT_STARTED -> COPY_SEGMENT_FINISHED
        updatedMetadata = new RemoteLogSegmentMetadataUpdate(segmentId, time.milliseconds(),
                Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId1);
        updateAndVerifyCacheContents(updatedMetadata, RemoteLogSegmentState.COPY_SEGMENT_FINISHED, leaderEpoch);

        // invalid-transition-3: COPY_SEGMENT_FINISHED -> DELETE_SEGMENT_FINISHED
        updatedMetadata = new RemoteLogSegmentMetadataUpdate(segmentId, time.milliseconds(),
                Optional.empty(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, brokerId1);
        updateAndVerifyCacheContents(updatedMetadata, RemoteLogSegmentState.COPY_SEGMENT_FINISHED, leaderEpoch);

        // invalid-transition-4: COPY_SEGMENT_FINISHED -> COPY_SEGMENT_STARTED
        updatedMetadata = new RemoteLogSegmentMetadataUpdate(segmentId, time.milliseconds(),
                Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_STARTED, brokerId1);
        updateAndVerifyCacheContents(updatedMetadata, RemoteLogSegmentState.COPY_SEGMENT_FINISHED, leaderEpoch);

        // valid-transition-5: COPY_SEGMENT_FINISHED -> DELETE_SEGMENT_STARTED
        updatedMetadata = new RemoteLogSegmentMetadataUpdate(segmentId, time.milliseconds(),
                Optional.empty(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, brokerId1);
        updateAndVerifyCacheContents(updatedMetadata, RemoteLogSegmentState.DELETE_SEGMENT_STARTED, leaderEpoch);

        // invalid-transition-6: DELETE_SEGMENT_STARTED -> COPY_SEGMENT_FINISHED
        updatedMetadata = new RemoteLogSegmentMetadataUpdate(segmentId, time.milliseconds(),
                Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId1);
        updateAndVerifyCacheContents(updatedMetadata, RemoteLogSegmentState.DELETE_SEGMENT_STARTED, leaderEpoch);

        // invalid-transition-7: DELETE_SEGMENT_STARTED -> COPY_SEGMENT_STARTED
        updatedMetadata = new RemoteLogSegmentMetadataUpdate(segmentId, time.milliseconds(),
                Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_STARTED, brokerId1);
        updateAndVerifyCacheContents(updatedMetadata, RemoteLogSegmentState.DELETE_SEGMENT_STARTED, leaderEpoch);

        // valid-transition-8: DELETE_SEGMENT_STARTED -> DELETE_SEGMENT_FINISHED
        updatedMetadata = new RemoteLogSegmentMetadataUpdate(segmentId, time.milliseconds(),
                Optional.empty(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, brokerId1);
        updateAndVerifyCacheContents(updatedMetadata, RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, leaderEpoch);
    }

    private void updateAndVerifyCacheContents(RemoteLogSegmentMetadataUpdate updatedMetadata,
                                              RemoteLogSegmentState expectedSegmentState,
                                              int leaderEpoch) throws RemoteResourceNotFoundException {
        cache.updateRemoteLogSegmentMetadata(updatedMetadata);
        List<RemoteLogSegmentMetadata> metadataList = new ArrayList<>();
        cache.listRemoteLogSegments(leaderEpoch).forEachRemaining(metadataList::add);
        if (expectedSegmentState != RemoteLogSegmentState.DELETE_SEGMENT_FINISHED) {
            assertEquals(1, metadataList.size());
            assertEquals(expectedSegmentState, metadataList.get(0).state());
        } else {
            assertTrue(metadataList.isEmpty());
        }
    }
}