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
import org.apache.kafka.common.protocol.types.Struct;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RequestHeaderTest {

    @Test
    public void testSerdeControlledShutdownV0() {
        // Verify that version 0 of controlled shutdown does not include the clientId field

        int correlationId = 2342;
        ByteBuffer rawBuffer = ByteBuffer.allocate(32);
        rawBuffer.putShort(ApiKeys.CONTROLLED_SHUTDOWN_KEY.id);
        rawBuffer.putShort((short) 0);
        rawBuffer.putInt(correlationId);
        rawBuffer.flip();

        RequestHeader deserialized = RequestHeader.parse(rawBuffer);
        assertEquals(ApiKeys.CONTROLLED_SHUTDOWN_KEY.id, deserialized.apiKey());
        assertEquals(0, deserialized.apiVersion());
        assertEquals(correlationId, deserialized.correlationId());
        assertNull(deserialized.clientId());

        Struct serialized = deserialized.toStruct();
        ByteBuffer serializedBuffer = ByteBuffer.allocate(serialized.sizeOf());
        serialized.writeTo(serializedBuffer);
        serializedBuffer.flip();

        assertEquals(ApiKeys.CONTROLLED_SHUTDOWN_KEY.id, serializedBuffer.getShort(0));
        assertEquals(0, serializedBuffer.getShort(2));
        assertEquals(correlationId, serializedBuffer.getInt(4));
    }

    @Test
    public void testSerde() {
        short apiKey = ApiKeys.FETCH.id;
        short version = 1;
        String clientId = "foo";
        int correlationId = 2342;

        RequestHeader header = new RequestHeader(apiKey, version, clientId, correlationId);
        Struct struct = header.toStruct();

        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(buffer);
        buffer.flip();

        assertEquals(header, RequestHeader.parse(buffer));
    }

}
