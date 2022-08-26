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
package org.apache.kafka.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.serialization.MetadataParseException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        assertStartsWith("Could not deserialize metadata record due to unknown frame version",
                assertThrows(MetadataParseException.class,
                        () -> serde.read(new ByteBufferAccessor(buffer), 16)).getMessage());
    }

    /**
     * Test attempting to parse an event which has a malformed frame version type varint.
     */
    @Test
    public void testParsingMalformedFrameVersionVarint() {
        MetadataRecordSerde serde = new MetadataRecordSerde();
        ByteBuffer buffer = ByteBuffer.allocate(64);
        buffer.clear();
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.position(0);
        buffer.limit(64);
        assertStartsWith("Error while reading frame version",
                assertThrows(MetadataParseException.class,
                        () -> serde.read(new ByteBufferAccessor(buffer), buffer.remaining())).getMessage());
    }

    /**
     * Test attempting to parse an event which has a malformed message type varint.
     */
    @Test
    public void testParsingMalformedMessageTypeVarint() {
        MetadataRecordSerde serde = new MetadataRecordSerde();
        ByteBuffer buffer = ByteBuffer.allocate(64);
        buffer.clear();
        buffer.put((byte) 0x01);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.position(0);
        buffer.limit(64);
        assertStartsWith("Error while reading type",
                assertThrows(MetadataParseException.class,
                        () -> serde.read(new ByteBufferAccessor(buffer), buffer.remaining())).getMessage());
    }

    /**
     * Test attempting to parse an event which has a malformed message version varint.
     */
    @Test
    public void testParsingMalformedMessageVersionVarint() {
        MetadataRecordSerde serde = new MetadataRecordSerde();
        ByteBuffer buffer = ByteBuffer.allocate(64);
        buffer.clear();
        buffer.put((byte) 0x01);
        buffer.put((byte) 0x08);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.position(0);
        buffer.limit(64);
        assertStartsWith("Error while reading version",
                assertThrows(MetadataParseException.class,
                        () -> serde.read(new ByteBufferAccessor(buffer), buffer.remaining())).getMessage());
    }

    /**
     * Test attempting to parse an event which has a version > Short.MAX_VALUE
     */
    @Test
    public void testParsingVersionTooLarge() {
        MetadataRecordSerde serde = new MetadataRecordSerde();
        ByteBuffer buffer = ByteBuffer.allocate(64);
        buffer.clear();
        buffer.put((byte) 0x01); // frame version
        buffer.put((byte) 0x08); // apiKey
        buffer.put((byte) 0xff); // api version
        buffer.put((byte) 0xff); // api version
        buffer.put((byte) 0xff); // api version
        buffer.put((byte) 0x7f); // api version end
        buffer.put((byte) 0x80);
        buffer.position(0);
        buffer.limit(64);
        assertStartsWith("Value for version was too large",
                assertThrows(MetadataParseException.class,
                        () -> serde.read(new ByteBufferAccessor(buffer), buffer.remaining())).getMessage());
    }

    /**
     * Test attempting to parse an event which has a unsupported version
     */
    @Test
    public void testParsingUnsupportedApiKey() {
        MetadataRecordSerde serde = new MetadataRecordSerde();
        ByteBuffer buffer = ByteBuffer.allocate(64);
        buffer.put((byte) 0x01); // frame version
        buffer.put((byte) 0xff); // apiKey
        buffer.put((byte) 0x7f); // apiKey
        buffer.put((byte) 0x00); // api version
        buffer.put((byte) 0x80);
        buffer.position(0);
        buffer.limit(64);
        assertStartsWith("Unknown metadata id ",
                assertThrows(MetadataParseException.class,
                        () -> serde.read(new ByteBufferAccessor(buffer), buffer.remaining())).getCause().getMessage());
    }

    /**
     * Test attempting to parse an event which has a malformed message body.
     */
    @Test
    public void testParsingMalformedMessage() {
        MetadataRecordSerde serde = new MetadataRecordSerde();
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.put((byte) 0x01); // frame version
        buffer.put((byte) 0x00); // apiKey
        buffer.put((byte) 0x00); // apiVersion
        buffer.put((byte) 0x80); // malformed data
        buffer.position(0);
        buffer.limit(4);
        assertStartsWith("Failed to deserialize record with type",
                assertThrows(MetadataParseException.class,
                        () -> serde.read(new ByteBufferAccessor(buffer), buffer.remaining())).getMessage());
    }

    /**
     * Test attempting to parse an event which has a malformed message version varint.
     */
    @Test
    public void testParsingRecordWithGarbageAtEnd() {
        MetadataRecordSerde serde = new MetadataRecordSerde();
        RegisterBrokerRecord message = new RegisterBrokerRecord().setBrokerId(1).setBrokerEpoch(2);

        ObjectSerializationCache cache = new ObjectSerializationCache();
        ApiMessageAndVersion messageAndVersion = new ApiMessageAndVersion(message, (short) 0);
        int size = serde.recordSize(messageAndVersion, cache);
        ByteBuffer buffer = ByteBuffer.allocate(size + 1);

        serde.write(messageAndVersion, cache, new ByteBufferAccessor(buffer));
        buffer.clear();
        assertStartsWith("Found 1 byte(s) of garbage after",
                assertThrows(MetadataParseException.class,
                        () -> serde.read(new ByteBufferAccessor(buffer), size + 1)).getMessage());
    }

    private static void assertStartsWith(String prefix, String str) {
        assertTrue(str.startsWith(prefix),
                "Expected string '" + str + "' to start with '" + prefix + "'");
    }

}
