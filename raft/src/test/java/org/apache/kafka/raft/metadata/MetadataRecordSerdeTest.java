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
package org.apache.kafka.raft.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MetadataRecordSerdeTest {

    @Test
    public void testSerde() {
        TopicRecord topicRecord = new TopicRecord()
            .setName("foo")
            .setTopicId(Uuid.randomUuid());

        MetadataRecordSerde serde = new MetadataRecordSerde();

        for (short version = TopicRecord.LOWEST_SUPPORTED_VERSION; version <= TopicRecord.HIGHEST_SUPPORTED_VERSION; version++) {
            ApiMessageAndVersion messageAndVersion = new ApiMessageAndVersion(topicRecord, version);

            ObjectSerializationCache cache = new ObjectSerializationCache();
            int size = serde.recordSize(messageAndVersion, cache);

            ByteBuffer buffer = ByteBuffer.allocate(size);
            ByteBufferAccessor bufferAccessor = new ByteBufferAccessor(buffer);

            serde.write(messageAndVersion, cache, bufferAccessor);
            buffer.flip();

            assertEquals(size, buffer.remaining());
            ApiMessageAndVersion readMessageAndVersion = serde.read(bufferAccessor, size);
            assertEquals(messageAndVersion, readMessageAndVersion);
        }
    }

    @Test
    public void testDeserializeWithUnhandledFrameVersion() {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        ByteUtils.writeUnsignedVarint(15, buffer);
        buffer.flip();

        MetadataRecordSerde serde = new MetadataRecordSerde();
        assertThrows(SerializationException.class,
            () -> serde.read(new ByteBufferAccessor(buffer), 16));
    }

}