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

import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
public class MetadataParserTest {
    private static final Logger log =
        LoggerFactory.getLogger(MetadataParserTest.class);

    /**
     * Test some serialization / deserialization round trips.
     */
    @Test
    public void testRoundTrips() {
        testRoundTrip(new RegisterBrokerRecord().setBrokerId(1).setBrokerEpoch(2), (short) 0);
        testRoundTrip(new ConfigRecord().setName("my.config.value").
            setResourceName("foo").setResourceType((byte) 0).setValue("bar"), (short) 0);
    }

    private static void testRoundTrip(ApiMessage message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = MetadataParser.size(message, version, cache);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        MetadataParser.write(message, version, cache, buffer);
        buffer.flip();
        ApiMessage message2 = MetadataParser.read(buffer.duplicate());
        assertEquals(message, message2);
        assertEquals(message2, message);

        ObjectSerializationCache cache2 = new ObjectSerializationCache();
        int size2 = MetadataParser.size(message2, version, cache2);
        assertEquals(size, size2);
        ByteBuffer buffer2 = ByteBuffer.allocate(size);
        MetadataParser.write(message2, version, cache2, buffer2);
        buffer2.flip();
        assertEquals(buffer.duplicate(), buffer2.duplicate());
    }

    /**
     * Test attempting to serialize a message which is too big to be serialized.
     */
    @Test
    public void testMaxSerializedEventSizeCheck() {
        List<Integer> longReplicaList =
            new ArrayList<>(MetadataParser.MAX_SERIALIZED_EVENT_SIZE / Integer.BYTES);
        for (int i = 0; i < MetadataParser.MAX_SERIALIZED_EVENT_SIZE / Integer.BYTES; i++) {
            longReplicaList.add(i);
        }
        PartitionRecord partitionRecord = new PartitionRecord().
            setReplicas(longReplicaList);
        ObjectSerializationCache cache = new ObjectSerializationCache();
        assertEquals("Event size would be 33554478, but the maximum serialized event " +
            "size is 33554432", assertThrows(RuntimeException.class, () -> {
                MetadataParser.size(partitionRecord, (short) 0, cache);
            }).getMessage());
    }

    /**
     * Test attemping to parse an event which has a malformed message type varint.
     */
    @Test
    public void testParsingMalformedMessageTypeVarint() {
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
        assertStartsWith("Failed to read variable-length type number",
            assertThrows(MetadataParseException.class, () -> {
                MetadataParser.read(buffer);
            }).getMessage());
    }

    /**
     * Test attemping to parse an event which has a malformed message version varint.
     */
    @Test
    public void testParsingMalformedMessageVersionVarint() {
        ByteBuffer buffer = ByteBuffer.allocate(64);
        buffer.clear();
        buffer.put((byte) 0x00);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.put((byte) 0x80);
        buffer.position(0);
        buffer.limit(64);
        assertStartsWith("Failed to read variable-length version number",
            assertThrows(MetadataParseException.class, () -> {
                MetadataParser.read(buffer);
            }).getMessage());
    }

    /**
     * Test attemping to parse an event which has a malformed message version varint.
     */
    @Test
    public void testParsingRecordWithGarbageAtEnd() {
        RegisterBrokerRecord message = new RegisterBrokerRecord().setBrokerId(1).setBrokerEpoch(2);
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = MetadataParser.size(message, (short) 0, cache);
        ByteBuffer buffer = ByteBuffer.allocate(size + 1);
        MetadataParser.write(message, (short) 0, cache, buffer);
        buffer.clear();
        assertStartsWith("Found 1 byte(s) of garbage after",
            assertThrows(MetadataParseException.class, () -> {
                MetadataParser.read(buffer);
            }).getMessage());
    }

    private static void assertStartsWith(String prefix, String str) {
        assertTrue(str.startsWith(prefix),
            "Expected string '" + str + "' to start with '" + prefix + "'");
    }
}
