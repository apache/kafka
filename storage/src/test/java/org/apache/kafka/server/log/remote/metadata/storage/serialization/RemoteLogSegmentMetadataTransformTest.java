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
package org.apache.kafka.server.log.remote.metadata.storage.serialization;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RemoteLogSegmentMetadataTransformTest {
    @ParameterizedTest
    @MethodSource("parameters")
    void testToAndFromMessage(Optional<CustomMetadata> customMetadata) {
        Map<Integer, Long> segmentLeaderEpochs = new HashMap<>();
        segmentLeaderEpochs.put(0, 0L);
        RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(
                new RemoteLogSegmentId(new TopicIdPartition(Uuid.randomUuid(), 0, "topic"), Uuid.randomUuid()),
                0L, 100L, -1L, 0, 0, 1234,
                customMetadata,
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                segmentLeaderEpochs
        );

        RemoteLogSegmentMetadataTransform transform = new RemoteLogSegmentMetadataTransform();
        ApiMessageAndVersion message = transform.toApiMessageAndVersion(metadata);
        assertEquals(metadata, transform.fromApiMessageAndVersion(message));
    }

    private static Stream<Object> parameters() {
        return Stream.of(
                Optional.of(new CustomMetadata(new byte[]{0, 1, 2, 3})),
                Optional.of(new CustomMetadata(new byte[0])),
                Optional.empty()
        );
    }
}