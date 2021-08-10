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
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;


/**
 * This is a decorator for ClientResponse used to verify (at finalization time) that any underlying memory for this
 * response has been returned to the pool. To be used only as a debugging aide.
 */
public class ClientResponseWithFinalize extends ClientResponse {
    private final LogContext logContext;

    public ClientResponseWithFinalize(RequestHeader requestHeader, RequestCompletionHandler callback,
        String destination, long createdTimeMs, long receivedTimeMs, boolean disconnected,
        UnsupportedVersionException versionMismatch, AuthenticationException authenticationException,
        AbstractResponse responseBody, MemoryPool memoryPool, ByteBuffer responsePayload, LogContext logContext) {
        super(requestHeader, callback, destination, createdTimeMs, receivedTimeMs, disconnected, versionMismatch,
            authenticationException, responseBody, memoryPool, responsePayload);
        this.logContext = logContext;
    }

    private Logger getLogger() {
        if (logContext != null) {
            return logContext.logger(ClientResponseWithFinalize.class);
        }
        return log;
    }

    protected void checkAndForceBufferRelease() {
        if (memoryPool != null && responsePayload != null) {
            getLogger().error("ByteBuffer[{}] not released. Ref Count: {}. RequestType: {}", responsePayload.position(),
                refCount.get(), this.requestHeader.apiKey());
            memoryPool.release(responsePayload);
            responsePayload = null;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        checkAndForceBufferRelease();
    }
}
