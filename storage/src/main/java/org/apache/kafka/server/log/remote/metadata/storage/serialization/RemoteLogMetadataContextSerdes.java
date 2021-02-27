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

import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataContext;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataRecord;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataUpdateRecord;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemotePartitionDeleteMetadataRecord;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * This class provides serialization and deserialization for {@link RemoteLogMetadataContext}. This is the root serdes
 * for the messages that are stored in internal remote log metadata topic.
 */
public class RemoteLogMetadataContextSerdes implements Serde<RemoteLogMetadataContext> {

    public static final byte REMOTE_LOG_SEGMENT_METADATA_API_KEY = (byte) new RemoteLogSegmentMetadataRecord().apiKey();
    public static final byte REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY = (byte) new RemoteLogSegmentMetadataUpdateRecord().apiKey();
    public static final byte REMOTE_PARTITION_DELETE_API_KEY = (byte) new RemotePartitionDeleteMetadataRecord().apiKey();

    private final Map<Byte, RemoteLogMetadataSerdes> keyToSerdes;
    private final Deserializer<RemoteLogMetadataContext> rootDeserializer;
    private final Serializer<RemoteLogMetadataContext> rootSerializer;

    public RemoteLogMetadataContextSerdes() {
        keyToSerdes = createInternalSerde();
        rootSerializer = (topic, data) -> serialize(data);
        rootDeserializer = (topic, data) -> deserialize(data);
    }

    private Map<Byte, RemoteLogMetadataSerdes> createInternalSerde() {
        Map<Byte, RemoteLogMetadataSerdes> map = new HashMap<>();
        map.put(REMOTE_LOG_SEGMENT_METADATA_API_KEY, new RemoteLogSegmentMetadataSerdes());
        map.put(REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY, new RemoteLogSegmentMetadataUpdateSerdes());
        map.put(REMOTE_PARTITION_DELETE_API_KEY, new RemotePartitionDeleteMetadataSerdes());
        return map;
    }

    private byte[] serialize(RemoteLogMetadataContext remoteLogMetadataContext) {
        RemoteLogMetadataSerdes serdes = keyToSerdes.get(remoteLogMetadataContext.apiKey());
        if (serdes == null) {
            throw new IllegalArgumentException("Serializer for apikey: " + remoteLogMetadataContext.apiKey() +
                                               " does not exist.");
        }

        @SuppressWarnings("unchecked")
        Message message = serdes.serialize(remoteLogMetadataContext.payload());

        return transformToBytes(message, remoteLogMetadataContext.apiKey(), remoteLogMetadataContext.version());
    }

    private RemoteLogMetadataContext deserialize(byte[] data) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        byte apiKey = byteBuffer.get();
        byte version = byteBuffer.get();
        RemoteLogMetadataSerdes serdes = keyToSerdes.get(apiKey);
        if (serdes == null) {
            throw new IllegalArgumentException("Deserializer for apikey: " + apiKey + " does not exist.");
        }

        Object deserializedObj = serdes.deserialize(version, byteBuffer);
        return new RemoteLogMetadataContext(apiKey, version, deserializedObj);
    }

    private byte[] transformToBytes(Message message, byte apiKey, byte apiVersion) {
        ObjectSerializationCache cache = new ObjectSerializationCache();

        // Add header containing apiKey and apiVersion,
        // headerSize is 1 byte for apiKey and 1 byte for apiVersion
        int headerSize = 1 + 1;
        int messageSize = message.size(cache, apiVersion);
        ByteBufferAccessor writable = new ByteBufferAccessor(ByteBuffer.allocate(headerSize + messageSize));

        // Write apiKey and apiVersion
        writable.writeByte(apiKey);
        writable.writeByte(apiVersion);

        // Write the message
        message.write(writable, cache, apiVersion);

        return writable.buffer().array();
    }

    @Override
    public Serializer<RemoteLogMetadataContext> serializer() {
        return rootSerializer;
    }

    @Override
    public Deserializer<RemoteLogMetadataContext> deserializer() {
        return rootDeserializer;
    }
}
