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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RequestHeaderTest {

    @Test
    public void testSerdeControlledShutdownV0() {
        // Verify that version 0 of controlled shutdown does not include the clientId field
        short apiVersion = 0;
        int correlationId = 2342;
        ByteBuffer rawBuffer = ByteBuffer.allocate(32);
        rawBuffer.putShort(ApiKeys.CONTROLLED_SHUTDOWN.id);
        rawBuffer.putShort(apiVersion);
        rawBuffer.putInt(correlationId);
        rawBuffer.flip();

        RequestHeader deserialized = RequestHeader.parse(rawBuffer);
        assertEquals(ApiKeys.CONTROLLED_SHUTDOWN, deserialized.apiKey());
        assertEquals(0, deserialized.apiVersion());
        assertEquals(correlationId, deserialized.correlationId());
        assertEquals("", deserialized.clientId());
        assertEquals(0, deserialized.headerVersion());

        ByteBuffer serializedBuffer = RequestTestUtils.serializeRequestHeader(deserialized);

        assertEquals(ApiKeys.CONTROLLED_SHUTDOWN.id, serializedBuffer.getShort(0));
        assertEquals(0, serializedBuffer.getShort(2));
        assertEquals(correlationId, serializedBuffer.getInt(4));
        assertEquals(8, serializedBuffer.limit());
    }

    @Test
    public void testRequestHeaderV1() {
        short apiVersion = 1;
        RequestHeader header = new RequestHeader(ApiKeys.FIND_COORDINATOR, apiVersion, "", 10);
        assertEquals(1, header.headerVersion());

        ByteBuffer buffer = RequestTestUtils.serializeRequestHeader(header);
        assertEquals(10, buffer.remaining());
        RequestHeader deserialized = RequestHeader.parse(buffer);
        assertEquals(header, deserialized);
    }

    @Test
    public void testRequestHeaderV2() {
        short apiVersion = 2;
        RequestHeader header = new RequestHeader(ApiKeys.CREATE_DELEGATION_TOKEN, apiVersion, "", 10);
        assertEquals(2, header.headerVersion());

        ByteBuffer buffer = RequestTestUtils.serializeRequestHeader(header);
        assertEquals(11, buffer.remaining());
        RequestHeader deserialized = RequestHeader.parse(buffer);
        assertEquals(header, deserialized);
    }

    @Test
    public void parseHeaderFromBufferWithNonZeroPosition() {
        ByteBuffer buffer = ByteBuffer.allocate(64);
        buffer.position(10);

        RequestHeader header = new RequestHeader(ApiKeys.FIND_COORDINATOR, (short) 1, "", 10);
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        // size must be called before write to avoid an NPE with the current implementation
        header.size(serializationCache);
        header.write(buffer, serializationCache);
        int limit = buffer.position();
        buffer.position(10);
        buffer.limit(limit);

        RequestHeader parsed = RequestHeader.parse(buffer);
        assertEquals(header, parsed);
    }
}
