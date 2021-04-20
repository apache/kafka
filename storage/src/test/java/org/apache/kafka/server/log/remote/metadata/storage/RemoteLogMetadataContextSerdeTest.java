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
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataContextSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataContextSerde.REMOTE_LOG_SEGMENT_METADATA_API_KEY;
import static org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataContextSerde.REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY;
import static org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataContextSerde.REMOTE_PARTITION_DELETE_API_KEY;

public class RemoteLogMetadataContextSerdeTest {

    public static final String TOPIC = "foo";
    private static final TopicIdPartition TP0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(TOPIC, 0));
    private final Time time = new MockTime(1);

    @Test
    public void testRemoteLogSegmentMetadataSerde() {
        // Create RemoteLogMetadataContext for RemoteLogSegmentMetadata
        // Deserialize the bytes and get RemoteLogSegmentMetadata and check it is as expected.
        RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();

        RemoteLogMetadataContext remoteLogMetadataContext = new RemoteLogMetadataContext(
                REMOTE_LOG_SEGMENT_METADATA_API_KEY, segmentMetadata);
        doTestRemoteLogContextSerde(remoteLogMetadataContext);
    }

    @Test
    public void testRemoteLogSegmentMetadataSerdeWithWrongApiKey() {
        RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
        // Set the wrong API key for the given RemoteLogSegmentMetadata on RemoteLogMetadataContext and expect an
        // exception is thrown while serializing.
        short[] wrongApiKeys;
        wrongApiKeys = new short[]{REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY, REMOTE_PARTITION_DELETE_API_KEY};

        doTestWrongAPIKeySerialize(segmentMetadata, wrongApiKeys);
    }

    @Test
    public void testRemoteLogSegmentMetadataUpdateSerde() {
        // Create RemoteLogMetadataContext for RemoteLogSegmentMetadataUpdate
        RemoteLogSegmentMetadataUpdate segmentMetadataUpdate = createRemoteLogSegmentMetadataUpdate();

        RemoteLogMetadataContext remoteLogMetadataContext = new RemoteLogMetadataContext(
                REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY, segmentMetadataUpdate);

        doTestRemoteLogContextSerde(remoteLogMetadataContext);
    }

    @Test
    public void testRemoteLogSegmentMetadataUpdateSerdeWithWrongApiKey() {
        RemoteLogSegmentMetadataUpdate segmentMetadataUpdate = createRemoteLogSegmentMetadataUpdate();
        // Set the wrong API key for the given RemoteLogSegmentMetadataUpdate on RemoteLogMetadataContext and expect an
        // exception is thrown while serializing.
        short[] wrongApiKeys = {REMOTE_LOG_SEGMENT_METADATA_API_KEY, REMOTE_PARTITION_DELETE_API_KEY};

        doTestWrongAPIKeySerialize(segmentMetadataUpdate, wrongApiKeys);
    }

    @Test
    public void testRemotePartitionDeleteMetadataSerde() {
        // Create RemoteLogMetadataContext for RemotePartitionDeleteMetadata
        RemotePartitionDeleteMetadata remotePartitionDeleteMetadata = createRemotePartitionDeleteMetadata();

        RemoteLogMetadataContext remoteLogMetadataContext = new RemoteLogMetadataContext(
                REMOTE_PARTITION_DELETE_API_KEY,
                remotePartitionDeleteMetadata);

        doTestRemoteLogContextSerde(remoteLogMetadataContext);
    }

    @Test
    public void testRemotePartitionDeleteMetadataSerdeWithWrongApiKey() {
        RemotePartitionDeleteMetadata remotePartitionDeleteMetadata = createRemotePartitionDeleteMetadata();
        // Set the wrong API key for the given RemotePartitionDeleteMetadata on RemoteLogMetadataContext and expect an
        // exception is thrown while serializing.
        short[] wrongApiKeys = {REMOTE_LOG_SEGMENT_METADATA_API_KEY, REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY};

        doTestWrongAPIKeySerialize(remotePartitionDeleteMetadata, wrongApiKeys);
    }

    private RemoteLogSegmentMetadata createRemoteLogSegmentMetadata() {
        Map<Integer, Long> segLeaderEpochs = new HashMap<>();
        segLeaderEpochs.put(0, 0L);
        segLeaderEpochs.put(1, 20L);
        segLeaderEpochs.put(2, 80L);
        RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        return new RemoteLogSegmentMetadata(remoteLogSegmentId, 0L, 100L, -1L, 2,
                                            time.milliseconds(), 1024, segLeaderEpochs);
    }

    private RemoteLogSegmentMetadataUpdate createRemoteLogSegmentMetadataUpdate() {
        RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        return new RemoteLogSegmentMetadataUpdate(remoteLogSegmentId, time.milliseconds(),
                                                  RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 0);
    }

    private RemotePartitionDeleteMetadata createRemotePartitionDeleteMetadata() {
        return new RemotePartitionDeleteMetadata(TP0, RemotePartitionDeleteState.DELETE_PARTITION_MARKED,
                                                 time.milliseconds(), 0);
    }

    private void doTestRemoteLogContextSerde(RemoteLogMetadataContext remoteLogMetadataContext) {
        RemoteLogMetadataContextSerde.RemoteLogMetadataContextSerializer serializer = new RemoteLogMetadataContextSerde.RemoteLogMetadataContextSerializer();

        // Serialize context and get the bytes.
        byte[] contextSerBytes = serializer.serialize(TOPIC, remoteLogMetadataContext);

        // Deserialize the bytes and check the RemoteLogMetadataContext object is as expected.
        RemoteLogMetadataContextSerde.RemoteLogMetadataContextDeserializer deserializer = new RemoteLogMetadataContextSerde.RemoteLogMetadataContextDeserializer();
        RemoteLogMetadataContext deserializedRlmContext = deserializer.deserialize(TOPIC, contextSerBytes);
        Assertions.assertEquals(remoteLogMetadataContext, deserializedRlmContext);
    }

    private void doTestWrongAPIKeySerialize(Object object, short[] wrongApiKeys) {
        for (short wrongApiKey : wrongApiKeys) {
            RemoteLogMetadataContext remoteLogMetadataContext = new RemoteLogMetadataContext(wrongApiKey, object);

            // Serializing receives an exception as it does not have the expected payload for the given apiKey.
            Assertions.assertThrows(ClassCastException.class,
                () -> new RemoteLogMetadataContextSerde.RemoteLogMetadataContextSerializer().serialize(TOPIC, remoteLogMetadataContext));
        }
    }

}