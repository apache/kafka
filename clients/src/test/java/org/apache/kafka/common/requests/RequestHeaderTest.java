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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.utils.internals.ByteUtils;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class RequestHeaderTest {

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
    public void testRequestHeaderV3() {
        // OffsetDelete v1 uses the v3 request header.
        short apiVersion = 1;
        RequestHeader header = new RequestHeader(ApiKeys.OFFSET_DELETE, apiVersion, "", 10);
        assertEquals(3, header.headerVersion());

        // The client instance ID is tagged, so a v3 header which leaves it unset is the size of a v2 header.
        ByteBuffer buffer = RequestTestUtils.serializeRequestHeader(header);
        assertEquals(11, buffer.remaining());
        RequestHeader deserialized = RequestHeader.parse(buffer);
        assertEquals(header, deserialized);
        assertEquals(Uuid.ZERO_UUID, deserialized.data().clientInstanceId());
    }

    @Test
    public void testRequestHeaderV3WithClientInstanceId() {
        Uuid clientInstanceId = Uuid.randomUuid();
        RequestHeader header = new RequestHeader(ApiKeys.OFFSET_DELETE, (short) 1, "", 10, clientInstanceId);
        assertEquals(3, header.headerVersion());
        assertEquals(clientInstanceId, header.clientInstanceId());

        // The 10 bytes of header fields, plus the tagged field's count, tag, size and 16-byte UUID.
        ByteBuffer buffer = RequestTestUtils.serializeRequestHeader(header);
        assertEquals(29, buffer.remaining());
        RequestHeader deserialized = RequestHeader.parse(buffer);
        assertEquals(header, deserialized);
        assertEquals(clientInstanceId, deserialized.clientInstanceId());
    }

    @Test
    public void testClientInstanceIdIsDroppedBelowTheV3Header() {
        // OffsetDelete v0 uses the v1 request header, which has no ClientInstanceId field, so the header
        // leaves the ID unset as its default, ZERO_UUID.
        RequestHeader header = new RequestHeader(ApiKeys.OFFSET_DELETE, (short) 0, "", 10, Uuid.randomUuid());
        assertEquals(1, header.headerVersion());
        assertEquals(Uuid.ZERO_UUID, header.clientInstanceId());

        ByteBuffer buffer = RequestTestUtils.serializeRequestHeader(header);
        assertEquals(10, buffer.remaining());
        assertEquals(header, RequestHeader.parse(buffer));
    }

    @Test
    public void testNullClientInstanceIdLeavesTheV3HeaderUnset() {
        RequestHeader header = new RequestHeader(ApiKeys.OFFSET_DELETE, (short) 1, "", 10, null);
        assertEquals(3, header.headerVersion());
        assertEquals(Uuid.ZERO_UUID, header.clientInstanceId());
        assertEquals(11, RequestTestUtils.serializeRequestHeader(header).remaining());
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

    @Test
    public void parseHeaderWithNullClientId() {
        RequestHeaderData headerData = new RequestHeaderData().
            setClientId(null).
            setCorrelationId(123).
            setRequestApiKey(ApiKeys.FIND_COORDINATOR.id).
            setRequestApiVersion((short) 10);
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(headerData.size(serializationCache, (short) 2));
        headerData.write(new ByteBufferAccessor(buffer), serializationCache, (short) 2);
        buffer.flip();
        RequestHeader parsed = RequestHeader.parse(buffer);
        assertEquals("", parsed.clientId());
        assertEquals(123, parsed.correlationId());
        assertEquals(ApiKeys.FIND_COORDINATOR, parsed.apiKey());
        assertEquals((short) 10, parsed.apiVersion());
    }

    @Test
    public void testHugeDeclaredTaggedFieldCountIsRejected() {
        RequestHeaderData headerData = new RequestHeaderData().
            setClientId("client").
            setCorrelationId(123).
            setRequestApiKey(ApiKeys.FIND_COORDINATOR.id).
            setRequestApiVersion((short) 10);
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        ByteBuffer prefix = ByteBuffer.allocate(headerData.size(serializationCache, (short) 2));
        headerData.write(new ByteBufferAccessor(prefix), serializationCache, (short) 2);
        prefix.flip();
        byte[] prefixBytes = new byte[prefix.remaining() - 1];
        prefix.get(prefixBytes);
        assertEquals((byte) 0, prefix.get(), "expected an empty tagged-fields section to replace");

        int declaredCount = 20_000_000;
        ByteBuffer countBuf = ByteBuffer.allocate(8);
        ByteUtils.writeUnsignedVarint(declaredCount, countBuf);
        countBuf.flip();
        byte[] countBytes = new byte[countBuf.remaining()];
        countBuf.get(countBytes);

        // Real buffer backed by declaredCount padding bytes, so the pre-existing
        // count-vs-remaining-bytes guard alone would let this through and the new hard cap is
        // what has to catch it.
        ByteBuffer buffer = ByteBuffer.allocate(prefixBytes.length + countBytes.length + declaredCount);
        buffer.put(prefixBytes);
        buffer.put(countBytes);
        buffer.position(prefixBytes.length + countBytes.length + declaredCount);
        buffer.flip();

        InvalidRequestException e = assertThrows(InvalidRequestException.class, () -> RequestHeader.parse(buffer));
        assertTrue(e.getCause().getMessage().contains("exceeds the maximum allowed count"),
                "Expected a hard-cap rejection, but got: " + e.getCause().getMessage());
    }

    @Test
    public void verifySizeMethodsReturnSameValue() {
        // Create a dummy RequestHeaderData
        RequestHeaderData headerData = new RequestHeaderData().
            setClientId("hakuna-matata").
            setCorrelationId(123).
            setRequestApiKey(ApiKeys.FIND_COORDINATOR.id).
            setRequestApiVersion((short) 10);

        // Serialize RequestHeaderData to a buffer
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(headerData.size(serializationCache, (short) 2));
        headerData.write(new ByteBufferAccessor(buffer), serializationCache, (short) 2);
        buffer.flip();

        // actual call to generate the RequestHeader from buffer containing RequestHeaderData
        RequestHeader parsed = spy(RequestHeader.parse(buffer));

        // verify that the result of cached value of size is same as actual calculation of size
        int sizeCalculatedFromData = parsed.size(new ObjectSerializationCache());
        int sizeFromCache = parsed.size();
        assertEquals(sizeCalculatedFromData, sizeFromCache);

        // verify that size(ObjectSerializationCache) is only called once, i.e. during assertEquals call. This validates
        // that size() method does not calculate the size instead it uses the cached value
        verify(parsed).size(any(ObjectSerializationCache.class));
    }
}
