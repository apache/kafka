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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.serialization.BytesApiMessageSerde;
import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogSegmentMetadataSnapshot;
import org.apache.kafka.server.log.remote.metadata.storage.generated.MetadataRecordType;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataRecord;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataSnapshotRecord;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataUpdateRecord;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemotePartitionDeleteMetadataRecord;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

/**
 * This class provides serialization and deserialization for {@link RemoteLogMetadata}. This is the root serde
 * for the messages that are stored in internal remote log metadata topic.
 */
public class RemoteLogMetadataSerde {
    private static final short REMOTE_LOG_SEGMENT_METADATA_API_KEY = new RemoteLogSegmentMetadataRecord().apiKey();
    private static final short REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY = new RemoteLogSegmentMetadataUpdateRecord().apiKey();
    private static final short REMOTE_PARTITION_DELETE_API_KEY = new RemotePartitionDeleteMetadataRecord().apiKey();
    private static final short REMOTE_LOG_SEGMENT_METADATA_SNAPSHOT_API_KEY = new RemoteLogSegmentMetadataSnapshotRecord().apiKey();

    private final Map<String, Short> remoteLogStorageClassToApiKey;
    private final Map<Short, RemoteLogMetadataTransform> keyToTransform;
    private final BytesApiMessageSerde bytesApiMessageSerde;

    public RemoteLogMetadataSerde() {
        remoteLogStorageClassToApiKey = createRemoteLogStorageClassToApiKeyMap();
        keyToTransform = createRemoteLogMetadataTransforms();
        bytesApiMessageSerde = new BytesApiMessageSerde() {
            @Override
            public ApiMessage apiMessageFor(short apiKey) {
                return newApiMessage(apiKey);
            }
        };
    }

    protected ApiMessage newApiMessage(short apiKey) {
        return MetadataRecordType.fromId(apiKey).newMetadataRecord();
    }

    protected final Map<Short, RemoteLogMetadataTransform> createRemoteLogMetadataTransforms() {
        Map<Short, RemoteLogMetadataTransform> map = new HashMap<>();
        map.put(REMOTE_LOG_SEGMENT_METADATA_API_KEY, new RemoteLogSegmentMetadataTransform());
        map.put(REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY, new RemoteLogSegmentMetadataUpdateTransform());
        map.put(REMOTE_PARTITION_DELETE_API_KEY, new RemotePartitionDeleteMetadataTransform());
        map.put(REMOTE_LOG_SEGMENT_METADATA_SNAPSHOT_API_KEY, new RemoteLogSegmentMetadataSnapshotTransform());
        return map;
    }

    protected final Map<String, Short> createRemoteLogStorageClassToApiKeyMap() {
        Map<String, Short> map = new HashMap<>();
        map.put(RemoteLogSegmentMetadata.class.getName(), REMOTE_LOG_SEGMENT_METADATA_API_KEY);
        map.put(RemoteLogSegmentMetadataUpdate.class.getName(), REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY);
        map.put(RemotePartitionDeleteMetadata.class.getName(), REMOTE_PARTITION_DELETE_API_KEY);
        map.put(RemoteLogSegmentMetadataSnapshot.class.getName(), REMOTE_LOG_SEGMENT_METADATA_SNAPSHOT_API_KEY);
        return map;
    }

    public byte[] serialize(RemoteLogMetadata remoteLogMetadata) {
        Short apiKey = remoteLogStorageClassToApiKey.get(remoteLogMetadata.getClass().getName());
        if (apiKey == null) {
            throw new IllegalArgumentException("ApiKey for given RemoteStorageMetadata class: " + remoteLogMetadata.getClass()
                                                       + " does not exist.");
        }

        @SuppressWarnings("unchecked")
        ApiMessageAndVersion apiMessageAndVersion = remoteLogMetadataTransform(apiKey).toApiMessageAndVersion(remoteLogMetadata);

        return bytesApiMessageSerde.serialize(apiMessageAndVersion);
    }

    public RemoteLogMetadata deserialize(byte[] data) {
        ApiMessageAndVersion apiMessageAndVersion = bytesApiMessageSerde.deserialize(data);

        return remoteLogMetadataTransform(apiMessageAndVersion.message().apiKey()).fromApiMessageAndVersion(apiMessageAndVersion);
    }

    private RemoteLogMetadataTransform remoteLogMetadataTransform(short apiKey) {
        RemoteLogMetadataTransform metadataTransform = keyToTransform.get(apiKey);
        if (metadataTransform == null) {
            throw new IllegalArgumentException("RemoteLogMetadataTransform for apikey: " + apiKey + " does not exist.");
        }

        return metadataTransform;
    }

    public static class RemoteLogMetadataFormatter implements MessageFormatter {
        private final RemoteLogMetadataSerde remoteLogMetadataSerde = new RemoteLogMetadataSerde();

        @Override
        public void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output) {
            // The key is expected to be null.
            output.printf("partition: %d, offset: %d, value: %s%n",
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    remoteLogMetadataSerde.deserialize(consumerRecord.value()).toString());
        }
    }
}
