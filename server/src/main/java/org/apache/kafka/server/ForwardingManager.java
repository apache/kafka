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
package org.apache.kafka.server;

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.network.Request;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface ForwardingManager {

    /**
     * Close the forwarding manager
     */
    void close();

    /**
     * Forward given request to the active controller.
     *
     * @param originalRequest     The request to forward.
     * @param responseCallback    A callback which takes in an `Optional&lt;AbstractResponse&gt;`.
     *                            We will call this function with Optional.of(x) after the controller responds with x.
     *                            Or, if the controller doesn't support the request version, we will complete
     *                            the callback with Optional.empty().
     */
    default void forwardRequest(
            Request originalRequest,
            Consumer<Optional<AbstractResponse>> responseCallback
    ) {
        ByteBuffer buffer = originalRequest.buffer().duplicate();
        buffer.flip();
        forwardRequest(originalRequest.context(),
                buffer,
                originalRequest.startTimeNanos(),
                originalRequest.body(AbstractRequest.class),
                originalRequest::toString,
                responseCallback);
    }

    /**
     * Forward given request to the active controller.
     *
     * @param originalRequest     The request to forward.
     * @param newRequestBody      The AbstractRequest we are sending.
     * @param responseCallback    A callback which takes in an `Optional&lt;AbstractResponse&gt;`.
     *                            We will call this function with Optional.of(x) after the controller responds with x.
     *                            Or, if the controller doesn't support the request version, we will complete
     *                            the callback with Optional.empty().
     */
    default void forwardRequest(
            Request originalRequest,
            AbstractRequest newRequestBody,
            Consumer<Optional<AbstractResponse>> responseCallback) {
        ByteBuffer buffer = newRequestBody.serializeWithHeader(originalRequest.header());
        forwardRequest(originalRequest.context(),
                buffer,
                originalRequest.startTimeNanos(),
                newRequestBody,
                originalRequest::toString,
                responseCallback);
    }

    /**
     * Forward given request to the active controller.
     *
     * @param requestContext      The request context of the original envelope request.
     * @param requestBufferCopy   The request buffer we want to send. This should not be the original
     *                            byte buffer from the envelope request, since we will be mutating
     *                            the position and limit fields. It should be a copy.
     * @param requestCreationNs   The request creation timestamp in nanoseconds.
     * @param requestBody         The AbstractRequest we are sending.
     * @param requestToString     A callback which can be invoked to produce a human-readable description
     *                            of the request.
     * @param responseCallback    A callback which takes in an `Optional&lt;AbstractResponse&gt;`.
     *                            We will call this function with Optional.of(x) after the controller responds with x.
     *                            Or, if the controller doesn't support the request version, we will complete
     *                            the callback with Optional.empty().
     */
    void forwardRequest(
            RequestContext requestContext,
            ByteBuffer requestBufferCopy,
            long requestCreationNs,
            AbstractRequest requestBody,
            Supplier<String> requestToString,
            Consumer<Optional<AbstractResponse>> responseCallback);

    /**
     * Return the NodeApiVersions for the controller node if available
     */
    Optional<NodeApiVersions> controllerApiVersions();
}
