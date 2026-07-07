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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataKey;

import java.nio.ByteBuffer;

/**
 * Serde for {@link RemoteLogSegmentMetadataKey}.
 * <p>
 * Serializes to a compact 37-byte binary representation:
 * <ul>
 *   <li>8 bytes – topicId most-significant bits</li>
 *   <li>8 bytes – topicId least-significant bits</li>
 *   <li>4 bytes – partition (big-endian int)</li>
 *   <li>8 bytes – segmentId most-significant bits</li>
 *   <li>8 bytes – segmentId least-significant bits</li>
 *   <li>1 byte  – stateId</li>
 * </ul>
 */
public class RemoteLogSegmentMetadataKeySerde implements Serde<RemoteLogSegmentMetadataKey> {

    static final int SERIALIZED_SIZE = 8 + 8 + 4 + 8 + 8 + 1; // 37 bytes

    @Override
    public Serializer<RemoteLogSegmentMetadataKey> serializer() {
        return (topic, key) -> {
            if (key == null) return null;
            ByteBuffer buf = ByteBuffer.allocate(SERIALIZED_SIZE);
            buf.putLong(key.topicId().getMostSignificantBits());
            buf.putLong(key.topicId().getLeastSignificantBits());
            buf.putInt(key.partition());
            buf.putLong(key.segmentId().getMostSignificantBits());
            buf.putLong(key.segmentId().getLeastSignificantBits());
            buf.put(key.stateId());
            return buf.array();
        };
    }

    @Override
    public Deserializer<RemoteLogSegmentMetadataKey> deserializer() {
        return (topic, data) -> {
            if (data == null) return null;
            if (data.length != SERIALIZED_SIZE) {
                throw new IllegalArgumentException(
                        "Expected " + SERIALIZED_SIZE + " bytes but got " + data.length);
            }
            ByteBuffer buf = ByteBuffer.wrap(data);
            Uuid topicId = new Uuid(buf.getLong(), buf.getLong());
            int partition = buf.getInt();
            Uuid segmentId = new Uuid(buf.getLong(), buf.getLong());
            byte stateId = buf.get();
            return new RemoteLogSegmentMetadataKey(topicId, partition, segmentId, stateId);
        };
    }
}
