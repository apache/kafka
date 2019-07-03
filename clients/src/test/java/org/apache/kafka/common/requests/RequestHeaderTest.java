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

import static org.apache.kafka.test.TestUtils.toBuffer;
import static org.junit.Assert.assertEquals;

public class RequestHeaderTest {

    @Test
    public void testSerdeControlledShutdownV0() {
        // Verify that version 0 of controlled shutdown does not include the clientId field

        int correlationId = 2342;
        ByteBuffer rawBuffer = ByteBuffer.allocate(32);
        rawBuffer.putShort(ApiKeys.CONTROLLED_SHUTDOWN.id);
        rawBuffer.putShort((short) 0);
        rawBuffer.putInt(correlationId);
        rawBuffer.flip();

        RequestHeader deserialized = RequestHeader.parse(rawBuffer);
        assertEquals(ApiKeys.CONTROLLED_SHUTDOWN, deserialized.apiKey());
        assertEquals(0, deserialized.apiVersion());
        assertEquals(correlationId, deserialized.correlationId());
        assertEquals("", deserialized.clientId());

        Struct serialized = deserialized.toStruct();
        ByteBuffer serializedBuffer = toBuffer(serialized);

        assertEquals(ApiKeys.CONTROLLED_SHUTDOWN.id, serializedBuffer.getShort(0));
        assertEquals(0, serializedBuffer.getShort(2));
        assertEquals(correlationId, serializedBuffer.getInt(4));
        assertEquals(8, serializedBuffer.limit());
    }

    @Test
    public void testRequestHeader() {
        RequestHeader header = new RequestHeader(ApiKeys.FIND_COORDINATOR, (short) 1, "", 10);
        ByteBuffer buffer = toBuffer(header.toStruct());
        RequestHeader deserialized = RequestHeader.parse(buffer);
        assertEquals(header, deserialized);
    }

    @Test
    public void testRequestHeaderWithNullClientId() {
        RequestHeader header = new RequestHeader(ApiKeys.FIND_COORDINATOR, (short) 1, null, 10);
        Struct headerStruct = header.toStruct();
        ByteBuffer buffer = toBuffer(headerStruct);
        RequestHeader deserialized = RequestHeader.parse(buffer);
        assertEquals(header.apiKey(), deserialized.apiKey());
        assertEquals(header.apiVersion(), deserialized.apiVersion());
        assertEquals(header.correlationId(), deserialized.correlationId());
        assertEquals("", deserialized.clientId()); // null defaults to ""
    }

}
