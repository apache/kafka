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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A response from the server. Contains both the body of the response as well as the correlated request
 * metadata that was originally sent.
 */
public class ClientResponse {
    protected static final Logger log = LoggerFactory.getLogger(ClientResponse.class);
    protected final RequestHeader requestHeader;
    private final RequestCompletionHandler callback;
    private final String destination;
    private final long receivedTimeMs;
    private final long latencyMs;
    private final boolean disconnected;
    private final UnsupportedVersionException versionMismatch;
    private final AuthenticationException authenticationException;
    private final AbstractResponse responseBody;
    protected final MemoryPool memoryPool;
    protected final AtomicLong refCount;
    protected ByteBuffer responsePayload;
    private boolean bufferReleased = false;

    public ClientResponse(RequestHeader requestHeader,
        RequestCompletionHandler callback,
        String destination,
        long createdTimeMs,
        long receivedTimeMs,
        boolean disconnected,
        UnsupportedVersionException versionMismatch,
        AuthenticationException authenticationException,
        AbstractResponse responseBody,
        MemoryPool memoryPool,
        ByteBuffer responsePayload) {
        this.requestHeader = requestHeader;
        this.callback = callback;
        this.destination = destination;
        this.receivedTimeMs = receivedTimeMs;
        this.latencyMs = receivedTimeMs - createdTimeMs;
        this.disconnected = disconnected;
        this.versionMismatch = versionMismatch;
        this.authenticationException = authenticationException;
        this.responseBody = responseBody;
        this.memoryPool = memoryPool;
        this.responsePayload = responsePayload;
        this.refCount = new AtomicLong(0);
    }

    /**
     * @param requestHeader The header of the corresponding request
     * @param callback The callback to be invoked
     * @param createdTimeMs The unix timestamp when the corresponding request was created
     * @param destination The node the corresponding request was sent to
     * @param receivedTimeMs The unix timestamp when this response was received
     * @param disconnected Whether the client disconnected before fully reading a response
     * @param versionMismatch Whether there was a version mismatch that prevented sending the request.
     * @param responseBody The response contents (or null) if we disconnected, no response was expected,
     *                     or if there was a version mismatch.
     */
    public ClientResponse(RequestHeader requestHeader,
                          RequestCompletionHandler callback,
                          String destination,
                          long createdTimeMs,
                          long receivedTimeMs,
                          boolean disconnected,
                          UnsupportedVersionException versionMismatch,
                          AuthenticationException authenticationException,
                          AbstractResponse responseBody) {
        this(requestHeader, callback, destination, createdTimeMs, receivedTimeMs, disconnected, versionMismatch,
            authenticationException, responseBody, null, null);
    }

    public long receivedTimeMs() {
        return receivedTimeMs;
    }

    public boolean wasDisconnected() {
        return disconnected;
    }

    public UnsupportedVersionException versionMismatch() {
        return versionMismatch;
    }

    public AuthenticationException authenticationException() {
        return authenticationException;
    }

    public RequestHeader requestHeader() {
        return requestHeader;
    }

    public String destination() {
        return destination;
    }

    public AbstractResponse responseBody() {
        return responseBody;
    }

    public boolean hasResponse() {
        return responseBody != null;
    }

    public long requestLatencyMs() {
        return latencyMs;
    }

    private void releaseBuffer() {
        if (memoryPool != null && responsePayload != null) {
            if (log.isTraceEnabled()) {
                log.trace("ByteBuffer[{}] returned to memorypool. Ref Count: {}. RequestType: {}",
                    responsePayload.position(), refCount.get(), this.requestHeader.apiKey());
            }

            memoryPool.release(responsePayload);
            responsePayload = null;
            bufferReleased = true;
        }
    }

    private boolean usingMemoryPool() {
        return memoryPool != null && memoryPool != MemoryPool.NONE;
    }

    public void incRefCount() {
        if (!usingMemoryPool()) {
            return;
        }

        if (bufferReleased) {
            // If somebody tried to call incRefCount after buffer has been released. This shouldn't happen
            throw new IllegalStateException(
                "Ref count being incremented again after buffer release. This should never happen.");
        }
        refCount.incrementAndGet();
    }

    public void decRefCount() {
        if (!usingMemoryPool()) {
            return;
        }

        long value = refCount.decrementAndGet();
        if (value < 0) {
            // Oops! This seems to be a place where we shouldn't get to.
            // However, to save users from exceptions, who don't use pooling, don't throw an exception.
            throw new IllegalStateException("Ref count decremented below zero. This should never happen.");
        }

        if (value == 0) {
            releaseBuffer();
        }
    }

    public void onComplete() {
        if (callback != null)
            callback.onComplete(this);
    }

    @Override
    public String toString() {
        return "ClientResponse(receivedTimeMs=" + receivedTimeMs +
               ", latencyMs=" +
               latencyMs +
               ", disconnected=" +
               disconnected +
               ", requestHeader=" +
               requestHeader +
               ", responseBody=" +
               responseBody +
               ")";
    }
}
