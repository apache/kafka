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
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogSegmentMetadataTransform;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogSegmentMetadataUpdateTransform;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemotePartitionDeleteMetadataTransform;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

public class RemoteLogMetadataTransformTest {
    private static final TopicIdPartition TP0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
    private final Time time = new MockTime(1);

    @Test
    public void testRemoteLogSegmentMetadataTransform() {
        RemoteLogSegmentMetadataTransform metadataTransform = new RemoteLogSegmentMetadataTransform();

        RemoteLogSegmentMetadata metadata = createRemoteLogSegmentMetadata();
        ApiMessageAndVersion apiMessageAndVersion = metadataTransform.toApiMessageAndVersion(metadata);
        RemoteLogSegmentMetadata remoteLogSegmentMetadataFromRecord = metadataTransform
                .fromApiMessageAndVersion(apiMessageAndVersion);

        Assertions.assertEquals(metadata, remoteLogSegmentMetadataFromRecord);
    }

    @Test
    public void testRemoteLogSegmentMetadataUpdateTransform() {
        RemoteLogSegmentMetadataUpdateTransform metadataUpdateTransform = new RemoteLogSegmentMetadataUpdateTransform();

        RemoteLogSegmentMetadataUpdate metadataUpdate =
                new RemoteLogSegmentMetadataUpdate(new RemoteLogSegmentId(TP0, Uuid.randomUuid()), time.milliseconds(),
                                                   Optional.of(new CustomMetadata(new byte[]{0, 1, 2, 3})),
                                                   RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 1);
        ApiMessageAndVersion apiMessageAndVersion = metadataUpdateTransform.toApiMessageAndVersion(metadataUpdate);
        RemoteLogSegmentMetadataUpdate metadataUpdateFromRecord = metadataUpdateTransform.fromApiMessageAndVersion(apiMessageAndVersion);

        Assertions.assertEquals(metadataUpdate, metadataUpdateFromRecord);
    }

    private RemoteLogSegmentMetadata createRemoteLogSegmentMetadata() {
        RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        return new RemoteLogSegmentMetadata(remoteLogSegmentId, 0L, 100L, -1L, 1,
                                            time.milliseconds(), 1024, Collections.singletonMap(0, 0L));
    }

    @Test
    public void testRemoteLogPartitionMetadataTransform() {
        RemotePartitionDeleteMetadataTransform transform = new RemotePartitionDeleteMetadataTransform();

        RemotePartitionDeleteMetadata partitionDeleteMetadata
                = new RemotePartitionDeleteMetadata(TP0, RemotePartitionDeleteState.DELETE_PARTITION_STARTED, time.milliseconds(), 1);
        ApiMessageAndVersion apiMessageAndVersion = transform.toApiMessageAndVersion(partitionDeleteMetadata);
        RemotePartitionDeleteMetadata partitionDeleteMetadataFromRecord = transform.fromApiMessageAndVersion(apiMessageAndVersion);

        Assertions.assertEquals(partitionDeleteMetadata, partitionDeleteMetadataFromRecord);
    }
}
