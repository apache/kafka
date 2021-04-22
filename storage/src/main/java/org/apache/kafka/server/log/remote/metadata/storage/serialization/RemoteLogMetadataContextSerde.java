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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataContext;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataRecord;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataUpdateRecord;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemotePartitionDeleteMetadataRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * This class provides serialization and deserialization for {@link RemoteLogMetadataContext}. This is the root serde
 * for the messages that are stored in internal remote log metadata topic.
 */
public class RemoteLogMetadataContextSerde {
    public static final short REMOTE_LOG_SEGMENT_METADATA_API_KEY = new RemoteLogSegmentMetadataRecord().apiKey();
    public static final short REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY = new RemoteLogSegmentMetadataUpdateRecord().apiKey();
    public static final short REMOTE_PARTITION_DELETE_API_KEY = new RemotePartitionDeleteMetadataRecord().apiKey();

    private static final Map<Short, RemoteLogMetadataTransform> KEY_TO_TRANSFORM = createRemoteLogMetadataTransforms();

    private static final AbstractMetadataMessageSerde METADATA_MESSAGE_SERDE = new AbstractMetadataMessageSerde() {
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

    public byte[] serialize(RemoteLogMetadataContext remoteLogMetadataContext) {
        RemoteLogMetadataTransform metadataTransform = KEY_TO_TRANSFORM.get(remoteLogMetadataContext.apiKey());
        if (metadataTransform == null) {
            throw new IllegalArgumentException(
                    "RemoteLogMetadataTransform for apiKey: " + remoteLogMetadataContext.apiKey() + " does not exist.");
        }

        @SuppressWarnings("unchecked")
        ApiMessageAndVersion apiMessageAndVersion = metadataTransform.toApiMessageAndVersion(remoteLogMetadataContext.payload());

        return METADATA_MESSAGE_SERDE.serialize(apiMessageAndVersion);
    }

    public RemoteLogMetadataContext deserialize(byte[] data) {
        ApiMessageAndVersion apiMessageAndVersion = METADATA_MESSAGE_SERDE.deserialize(data);

        short apiKey = apiMessageAndVersion.message().apiKey();
        RemoteLogMetadataTransform metadataTransform = KEY_TO_TRANSFORM.get(apiKey);
        if (metadataTransform == null) {
            throw new IllegalArgumentException(
                    "RemoteLogMetadataTransform for apikey: " + apiKey + " does not exist.");
        }

        Object deserializedObj = metadataTransform.fromApiMessageAndVersion(apiMessageAndVersion);
        return new RemoteLogMetadataContext(apiKey, deserializedObj);
    }

    public static class RemoteLogMetadataContextSerializer implements Serializer<RemoteLogMetadataContext> {
        private final RemoteLogMetadataContextSerde serde = new RemoteLogMetadataContextSerde();

        @Override
        public byte[] serialize(String topic, RemoteLogMetadataContext remoteLogMetadataContext) {
            return serde.serialize(remoteLogMetadataContext);
        }
    }

    public static class RemoteLogMetadataContextDeserializer implements Deserializer<RemoteLogMetadataContext> {

        private final RemoteLogMetadataContextSerde serde = new RemoteLogMetadataContextSerde();

        @Override
        public RemoteLogMetadataContext deserialize(String topic, byte[] data) {
            return serde.deserialize(data);
        }
    }
}