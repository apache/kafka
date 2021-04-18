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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataContext;
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

    private static final Map<Byte, RemoteLogMetadataSerdes> KEY_TO_SERDES = createInternalSerde();

    private final Deserializer<RemoteLogMetadataContext> rootDeserializer;
    private final Serializer<RemoteLogMetadataContext> rootSerializer;

    public RemoteLogMetadataContextSerdes() {
        rootSerializer = (topic, data) -> serialize(data);
        rootDeserializer = (topic, data) -> deserialize(data);
    }

    private static Map<Byte, RemoteLogMetadataSerdes> createInternalSerde() {
        Map<Byte, RemoteLogMetadataSerdes> map = new HashMap<>();
        map.put(REMOTE_LOG_SEGMENT_METADATA_API_KEY, new RemoteLogSegmentMetadataSerdes());
        map.put(REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY, new RemoteLogSegmentMetadataUpdateSerdes());
        map.put(REMOTE_PARTITION_DELETE_API_KEY, new RemotePartitionDeleteMetadataSerdes());
        return map;
    }

    private byte[] serialize(RemoteLogMetadataContext remoteLogMetadataContext) {
        RemoteLogMetadataSerdes serdes = KEY_TO_SERDES.get(remoteLogMetadataContext.apiKey());
        if (serdes == null) {
            throw new IllegalArgumentException("Serializer for apiKey: " + remoteLogMetadataContext.apiKey() +
                                               " does not exist.");
        }

        @SuppressWarnings("unchecked")
        ByteBuffer message = serdes.serialize(remoteLogMetadataContext.version(), remoteLogMetadataContext.payload());

        return toBytes(message, remoteLogMetadataContext.apiKey(), remoteLogMetadataContext.version());
    }

    private RemoteLogMetadataContext deserialize(byte[] data) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        byte apiKey = byteBuffer.get();
        byte version = byteBuffer.get();
        RemoteLogMetadataSerdes serdes = KEY_TO_SERDES.get(apiKey);
        if (serdes == null) {
            throw new IllegalArgumentException("Deserializer for apikey: " + apiKey + " does not exist.");
        }

        Object deserializedObj = serdes.deserialize(version, byteBuffer);
        return new RemoteLogMetadataContext(apiKey, version, deserializedObj);
    }

    private byte[] toBytes(ByteBuffer message, byte apiKey, byte apiVersion) {
        // Add header containing apiKey and apiVersion,
        // headerSize is 1 byte for apiKey and 1 byte for apiVersion
        int headerSize = 1 + 1;
        int messageSize = message.capacity();
        ByteBuffer byteBuffer = ByteBuffer.allocate(headerSize + messageSize);

        // Write apiKey and apiVersion
        byteBuffer.put(apiKey);
        byteBuffer.put(apiVersion);

        // Write the message
        byteBuffer.put(message);

        return byteBuffer.array();
    }

    @Override
    public Serializer<RemoteLogMetadataContext> serializer() {
        return rootSerializer;
    }

    @Override
    public Deserializer<RemoteLogMetadataContext> deserializer() {
        return rootDeserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        rootSerializer.configure(configs, isKey);
        rootDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        rootSerializer.close();
        rootDeserializer.close();
    }
}
