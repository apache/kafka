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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

public class RemoteLogMetadataCacheTest {

    private static final TopicIdPartition TP0 = new TopicIdPartition(Uuid.randomUuid(),
            new TopicPartition("foo", 0));
    private static final int SEG_SIZE = 1024 * 1024;
    private static final int BROKER_ID_0 = 0;
    private static final int BROKER_ID_1 = 1;

    private final Time time = new MockTime(1);

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

}