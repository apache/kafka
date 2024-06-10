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
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteState;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class RemoteLogMetadataSerdeTest {

    public static final String TOPIC = "foo";
    private static final TopicIdPartition TP0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(TOPIC, 0));
    private final Time time = new MockTime(1);

    @Test
    public void testRemoteLogSegmentMetadataSerde() {
        RemoteLogSegmentMetadata remoteLogSegmentMetadata = createRemoteLogSegmentMetadata();

        doTestRemoteLogMetadataSerde(remoteLogSegmentMetadata);
    }

    @Test
    public void testRemoteLogSegmentMetadataUpdateSerde() {
        RemoteLogSegmentMetadataUpdate remoteLogSegmentMetadataUpdate = createRemoteLogSegmentMetadataUpdate();

        doTestRemoteLogMetadataSerde(remoteLogSegmentMetadataUpdate);
    }

    @Test
    public void testRemotePartitionDeleteMetadataSerde() {
        RemotePartitionDeleteMetadata remotePartitionDeleteMetadata = createRemotePartitionDeleteMetadata();

        doTestRemoteLogMetadataSerde(remotePartitionDeleteMetadata);
    }

    private RemoteLogSegmentMetadata createRemoteLogSegmentMetadata() {
        Map<Integer, Long> segLeaderEpochs = new HashMap<>();
        segLeaderEpochs.put(0, 0L);
        segLeaderEpochs.put(1, 20L);
        segLeaderEpochs.put(2, 80L);
        RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        return new RemoteLogSegmentMetadata(remoteLogSegmentId, 0L, 100L, -1L, 1,
                                            time.milliseconds(), 1024,
                                            Optional.of(new CustomMetadata(new byte[] {0, 1, 2, 3})),
                                            RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                                            segLeaderEpochs
        );
    }

    private RemoteLogSegmentMetadataUpdate createRemoteLogSegmentMetadataUpdate() {
        RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        return new RemoteLogSegmentMetadataUpdate(remoteLogSegmentId, time.milliseconds(),
                                                  Optional.of(new CustomMetadata(new byte[] {0, 1, 2, 3})),
                                                  RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 2);
    }

    private RemotePartitionDeleteMetadata createRemotePartitionDeleteMetadata() {
        return new RemotePartitionDeleteMetadata(TP0, RemotePartitionDeleteState.DELETE_PARTITION_MARKED,
                                                 time.milliseconds(), 0);
    }

    private void doTestRemoteLogMetadataSerde(RemoteLogMetadata remoteLogMetadata) {
        // Serialize metadata and get the bytes.
        RemoteLogMetadataSerde serializer = new RemoteLogMetadataSerde();
        byte[] metadataBytes = serializer.serialize(remoteLogMetadata);

        // Deserialize the bytes and check the RemoteLogMetadata object is as expected.
        // Created another RemoteLogMetadataSerde instance to depict the real usecase of serializer and deserializer having their own instances.
        RemoteLogMetadataSerde deserializer = new RemoteLogMetadataSerde();
        RemoteLogMetadata deserializedRemoteLogMetadata = deserializer.deserialize(metadataBytes);
        Assertions.assertEquals(remoteLogMetadata, deserializedRemoteLogMetadata);
    }

    @Test
    public void testInvalidRemoteStorageMetadata() {
        // Serializing receives an exception as it does not have the expected RemoteLogMetadata registered in serdes.
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new RemoteLogMetadataSerde().serialize(new InvalidRemoteLogMetadata(1, time.milliseconds())));
    }

    private static class InvalidRemoteLogMetadata extends RemoteLogMetadata {
        public InvalidRemoteLogMetadata(int brokerId, long eventTimestampMs) {
            super(brokerId, eventTimestampMs);
        }

        @Override
        public TopicIdPartition topicIdPartition() {
            throw new UnsupportedOperationException();
        }
    }

}