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
import org.junit.jupiter.api.Test;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RemoteLogSegmentMetadataTest {
    private static final TopicIdPartition TP0 = new TopicIdPartition(Uuid.randomUuid(),
            new TopicPartition("foo", 0));

    @Test
    void createWithUpdates() {
        int brokerId = 0;
        int eventTimestamp = 0;
        int brokerIdFinished = 1;
        int timestampFinished = 1;
        long startOffset = 0L;
        long endOffset = 100L;
        int segmentSize = 123;
        long maxTimestamp = -1L;

        Map<Integer, Long> segmentLeaderEpochs = new HashMap<>();
        segmentLeaderEpochs.put(0, 0L);
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId, startOffset, endOffset,
                maxTimestamp, brokerId, eventTimestamp, segmentSize,
                segmentLeaderEpochs);

        CustomMetadata customMetadata = new CustomMetadata(new byte[]{0, 1, 2, 3});
        RemoteLogSegmentMetadataUpdate segmentMetadataUpdate = new RemoteLogSegmentMetadataUpdate(
                segmentId, timestampFinished, Optional.of(customMetadata), RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                brokerIdFinished);
        RemoteLogSegmentMetadata updatedMetadata = segmentMetadata.createWithUpdates(segmentMetadataUpdate);

        RemoteLogSegmentMetadata expectedUpdatedMetadata = new RemoteLogSegmentMetadata(
                segmentId, startOffset, endOffset,
                maxTimestamp, brokerIdFinished, timestampFinished, segmentSize, Optional.of(customMetadata),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                segmentLeaderEpochs
        );
        assertEquals(expectedUpdatedMetadata, updatedMetadata);

        // Check that the original metadata have not changed.
        assertEquals(segmentId, segmentMetadata.remoteLogSegmentId());
        assertEquals(startOffset, segmentMetadata.startOffset());
        assertEquals(endOffset, segmentMetadata.endOffset());
        assertEquals(maxTimestamp, segmentMetadata.maxTimestampMs());
        assertEquals(brokerId, segmentMetadata.brokerId());
        assertEquals(eventTimestamp, segmentMetadata.eventTimestampMs());
        assertEquals(segmentSize, segmentMetadata.segmentSizeInBytes());
        assertEquals(segmentLeaderEpochs, segmentMetadata.segmentLeaderEpochs());
    }
}
