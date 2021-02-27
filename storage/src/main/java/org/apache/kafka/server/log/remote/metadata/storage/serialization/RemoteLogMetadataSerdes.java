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

import org.apache.kafka.common.protocol.Message;

import java.nio.ByteBuffer;

/**
 * The default implementation of {@link org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager} stores all
 * the remote log metadata in an internal topic. Those messages can be {@link org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata},
 * {@link org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate}, or {@link org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata}.
 * These messages are written in Kafka's protocol message format as mentioned in
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage#KIP405:KafkaTieredStorage-MessageFormat">KIP-405</a>
 * <p>
 * This interface is about serializing and deserializing these messages that are stored in remote log metadata internal
 * topic. There are respective implementations for the mentioned message types.
 * <p>
 * @param <T> metadata type to be serialized/deserialized.
 *
 * @see RemoteLogSegmentMetadataSerdes
 * @see RemoteLogSegmentMetadataUpdateSerdes
 * @see RemotePartitionDeleteMetadataSerdes
 */
public interface RemoteLogMetadataSerdes<T> {

    /**
     * Returns the message serialized for the given {@code metadata} object.
     *
     * @param metadata object to be serialized.
     */
    Message serialize(T metadata);

    /**
     * Return the deserialized object for the given payload and the version.
     *
     * @param version version of the message payload.
     * @param data    data payload.
     */
    T deserialize(byte version, ByteBuffer data);

}
