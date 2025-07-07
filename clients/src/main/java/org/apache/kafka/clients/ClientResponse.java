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

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;

/**
 * A response from the server. Contains both the body of the response as well as the correlated request
 * metadata that was originally sent.
 */
public class ClientResponse {

    private final RequestHeader requestHeader;
    private final RequestCompletionHandler callback;
    private final String destination;
    private final long receivedTimeMs;
    private final long latencyMs;
    private final boolean disconnected;
    private final boolean timedOut;
    private final UnsupportedVersionException versionMismatch;
    private final AuthenticationException authenticationException;
    private final AbstractResponse responseBody;

    /**
     * @param requestHeader The header of the corresponding request
     * @param callback The callback to be invoked
     * @param destination The node the corresponding request was sent to
     * @param createdTimeMs The unix timestamp when the corresponding request was created
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
        this(requestHeader,
             callback,
             destination,
             createdTimeMs,
             receivedTimeMs,
             disconnected,
             false,
             versionMismatch,
             authenticationException,
             responseBody);
    }

    /**
     * @param requestHeader The header of the corresponding request
     * @param callback The callback to be invoked
     * @param destination The node the corresponding request was sent to
     * @param createdTimeMs The unix timestamp when the corresponding request was created
     * @param receivedTimeMs The unix timestamp when this response was received
     * @param disconnected Whether the client disconnected before fully reading a response
     * @param timedOut Whether the client was disconnected because of a timeout; when setting this
     *                 to <code>true</code>, <code>disconnected</code> must be <code>true</code>
     *                 or an {@link IllegalStateException} will be thrown
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
                          boolean timedOut,
                          UnsupportedVersionException versionMismatch,
                          AuthenticationException authenticationException,
                          AbstractResponse responseBody) {
        if (!disconnected && timedOut)
            throw new IllegalStateException("The client response can't be in the state of connected, yet timed out");

        this.requestHeader = requestHeader;
        this.callback = callback;
        this.destination = destination;
        this.receivedTimeMs = receivedTimeMs;
        this.latencyMs = receivedTimeMs - createdTimeMs;
        this.disconnected = disconnected;
        this.timedOut = timedOut;
        this.versionMismatch = versionMismatch;
        this.authenticationException = authenticationException;
        this.responseBody = responseBody;
    }

    public long receivedTimeMs() {
        return receivedTimeMs;
    }

    public boolean wasDisconnected() {
        return disconnected;
    }

    public boolean wasTimedOut() {
        return timedOut;
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
               ", timedOut=" +
               timedOut +
               ", requestHeader=" +
               requestHeader +
               ", responseBody=" +
               responseBody +
               ")";
    }

}
