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
package org.apache.kafka.clients;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


public class ClientResponseTest {

    private ClientResponse clientResponseWithPool;
    private ClientResponse clientResponse;
    private ClientResponse clientResponseDisconnected;
    private MemoryPool memoryPool;

    @Before
    public void setup() {
        RequestHeader requestHeader = new RequestHeader(ApiKeys.FETCH, (short) 1, "someclient", 1);
        NetworkClientTest.TestCallbackHandler callbackHandler = new NetworkClientTest.TestCallbackHandler();
        FetchResponse<MemoryRecords> fetchResponse = new FetchResponse<>(Errors.NONE, new LinkedHashMap<>(), 0, 1);
        memoryPool = Mockito.mock(MemoryPool.class);

        clientResponseWithPool =
            new ClientResponse(requestHeader, callbackHandler, "node0", 100, 110, false, null, null, fetchResponse,
                memoryPool, ByteBuffer.allocate(1));
        clientResponse =
            new ClientResponse(requestHeader, callbackHandler, "node0", 100, 110, false, null, null, fetchResponse,
                MemoryPool.NONE, ByteBuffer.allocate(1));
        clientResponseDisconnected =
            new ClientResponse(requestHeader, callbackHandler, "node0", 100, 110, true, null, null, fetchResponse);
    }

    private void decrementRefCountBelowZero(ClientResponse clientResponse) {
        clientResponse.decRefCount();
    }

    private void incrementRefCountAfterBufferRelease(ClientResponse clientResponse) {
        clientResponse.incRefCount();
        clientResponse.decRefCount();

        // Buffer released now. Try incrementing the ref count again.
        clientResponse.incRefCount();
    }

    @Test
    public void testClientResponseBufferRelease() {
        Mockito.doNothing().when(memoryPool).release(Mockito.any());

        clientResponseWithPool.incRefCount();
        clientResponseWithPool.decRefCount();

        Mockito.verify(memoryPool, Mockito.times(1)).release(Mockito.any());
    }

    @Test
    public void testDecRefCountBelowZeroMemoryPoolNone() {
        try {
            decrementRefCountBelowZero(clientResponse);
        } catch (Exception e) {
            Assert.fail("Client response with MemoryPool.NONE should not throw.");
        }
    }

    @Test
    public void testDecRefCountBelowZeroDisconnected() {
        try {
            decrementRefCountBelowZero(clientResponseDisconnected);
        } catch (Exception e) {
            Assert.fail("Client response with disconnection should not throw.");
        }
    }

    @Test
    public void testDecRefCountBelowZeroShouldThrow() {
        try {
            decrementRefCountBelowZero(clientResponseWithPool);
            Assert.fail("Decrementing ref count below zero should throw.");
        } catch (Exception e) {
            Assert.assertEquals(IllegalStateException.class, e.getClass());
            Assert.assertEquals("Ref count decremented below zero. This should never happen.", e.getMessage());
        }
    }

    @Test
    public void testIncRefCountAfterBufferReleaseMemoryPoolNone() {
        try {
            incrementRefCountAfterBufferRelease(clientResponse);
        } catch (Exception e) {
            Assert.fail("Client response with MemoryPool.NONE should not throw.");
        }
    }

    @Test
    public void testIncRefCountAfterBufferReleaseDisconnected() {
        try {
            incrementRefCountAfterBufferRelease(clientResponseDisconnected);
        } catch (Exception e) {
            Assert.fail("Client response with disconnection should not throw.");
        }
    }

    @Test
    public void testIncRefCountAfterBufferReleaseShouldThrow() {
        try {
            incrementRefCountAfterBufferRelease(clientResponseWithPool);
            Assert.fail("Incrementing ref count after releasing pool should throw.");
        } catch (IllegalStateException e) {
            Assert.assertEquals("Ref count being incremented again after buffer release. This should never happen.",
                e.getMessage());
        }
    }
}
