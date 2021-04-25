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

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataRecord;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataUpdateRecord;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemotePartitionDeleteMetadataRecord;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;

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

    private static final Map<String, Short> REMOTE_LOG_STORAGE_CLASS_TO_API_KEY = createRemoteLogStorageClassToApiKeyMap();
    private static final Map<Short, RemoteLogMetadataTransform> KEY_TO_TRANSFORM = createRemoteLogMetadataTransforms();

    private static final BytesApiMessageSerde BYTES_API_MESSAGE_SERDE = new BytesApiMessageSerde() {
        @Override
        public ApiMessage apiMessageFor(short apiKey) {
            if (apiKey == REMOTE_LOG_SEGMENT_METADATA_API_KEY) {
                return new RemoteLogSegmentMetadataRecord();
            } else if (apiKey == REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY) {
                return new RemoteLogSegmentMetadataUpdateRecord();
            } else if (apiKey == REMOTE_PARTITION_DELETE_API_KEY) {
                return new RemotePartitionDeleteMetadataRecord();
            } else {
                throw new IllegalArgumentException("apiKey: " + apiKey + "is not supported.");
            }
        }
    };

    private static Map<Short, RemoteLogMetadataTransform> createRemoteLogMetadataTransforms() {
        Map<Short, RemoteLogMetadataTransform> map = new HashMap<>();
        map.put(REMOTE_LOG_SEGMENT_METADATA_API_KEY, new RemoteLogSegmentMetadataTransform());
        map.put(REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY, new RemoteLogSegmentMetadataUpdateTransform());
        map.put(REMOTE_PARTITION_DELETE_API_KEY, new RemotePartitionDeleteMetadataTransform());
        return map;
    }

    private static Map<String, Short> createRemoteLogStorageClassToApiKeyMap() {
        Map<String, Short> map = new HashMap<>();
        map.put(RemoteLogSegmentMetadata.class.getName(), REMOTE_LOG_SEGMENT_METADATA_API_KEY);
        map.put(RemoteLogSegmentMetadataUpdate.class.getName(), REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY);
        map.put(RemotePartitionDeleteMetadata.class.getName(), REMOTE_PARTITION_DELETE_API_KEY);
        return map;
    }

    public byte[] serialize(RemoteLogMetadata remoteLogMetadata) {
        Short apiKey = REMOTE_LOG_STORAGE_CLASS_TO_API_KEY.get(remoteLogMetadata.getClass().getName());
        if (apiKey == null) {
            throw new IllegalArgumentException("ApiKey for given RemoteStorageMetadata class: " + remoteLogMetadata.getClass()
                                                       + " does not exist.");
        }

        @SuppressWarnings("unchecked")
        ApiMessageAndVersion apiMessageAndVersion = remoteLogMetadataTransform(apiKey).toApiMessageAndVersion(remoteLogMetadata);

        return BYTES_API_MESSAGE_SERDE.serialize(apiMessageAndVersion);
    }

    public RemoteLogMetadata deserialize(byte[] data) {
        ApiMessageAndVersion apiMessageAndVersion = BYTES_API_MESSAGE_SERDE.deserialize(data);

        return remoteLogMetadataTransform(apiMessageAndVersion.message().apiKey()).fromApiMessageAndVersion(apiMessageAndVersion);
    }

    private RemoteLogMetadataTransform remoteLogMetadataTransform(short apiKey) {
        RemoteLogMetadataTransform metadataTransform = KEY_TO_TRANSFORM.get(apiKey);
        if (metadataTransform == null) {
            throw new IllegalArgumentException("RemoteLogMetadataTransform for apikey: " + apiKey + " does not exist.");
        }

        return metadataTransform;
    }

}