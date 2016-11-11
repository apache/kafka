/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.requests.RequestHeader;

/**
 * A request being sent to the server. This holds both the network send as well as the client-level metadata.
 */
public final class ClientRequest {

    private final String destination;
    private final RequestHeader header;
    private final long createdTimeMs;
    private final boolean expectResponse;
    private final RequestCompletionHandler callback;
    private final boolean isInitiatedByNetworkClient;

    private Send send;
    private long sendTimeMs;

    /**
     * @param destination The brokerId to send the request to
     * @param createdTimeMs The unix timestamp in milliseconds for the time at which this request was created.
     * @param expectResponse Should we expect a response message or is this request complete once it is sent?
     * @param header The request's header
     * @param callback A callback to execute when the response has been received (or null if no callback is necessary)
     */
    public ClientRequest(String destination,
                         long createdTimeMs,
                         boolean expectResponse,
                         RequestHeader header,
                         RequestCompletionHandler callback) {
        this(destination, createdTimeMs, expectResponse, header, callback, false);
    }

    /**
     * @param destination The brokerId to send the request to
     * @param createdTimeMs The unix timestamp in milliseconds for the time at which this request was created.
     * @param expectResponse Should we expect a response message or is this request complete once it is sent?
     * @param header The request's header
     * @param callback A callback to execute when the response has been received (or null if no callback is necessary)
     * @param isInitiatedByNetworkClient Is request initiated by network client, if yes, its
     */
    public ClientRequest(String destination,
                         long createdTimeMs,
                         boolean expectResponse,
                         RequestHeader header,
                         RequestCompletionHandler callback,
                         boolean isInitiatedByNetworkClient) {
        this.destination = destination;
        this.createdTimeMs = createdTimeMs;
        this.callback = callback;
        this.header = header;
        this.expectResponse = expectResponse;
        this.isInitiatedByNetworkClient = isInitiatedByNetworkClient;
    }

    @Override
    public String toString() {
        return "ClientRequest(expectResponse=" + expectResponse +
            ", callback=" + callback +
            ", header=" + header +
            ", send=" + send +
            (isInitiatedByNetworkClient ? ", isInitiatedByNetworkClient" : "") +
            ", createdTimeMs=" + createdTimeMs +
            ", sendTimeMs=" + sendTimeMs +
            ")";
    }

    public boolean expectResponse() {
        return expectResponse;
    }

    public RequestHeader header() {
        return header;
    }

    public String destination() {
        return destination;
    }

    public Send send() {
        return send;
    }

    public boolean hasCallback() {
        return callback != null;
    }

    public RequestCompletionHandler callback() {
        return callback;
    }

    public long createdTimeMs() {
        return createdTimeMs;
    }

    public boolean isInitiatedByNetworkClient() {
        return isInitiatedByNetworkClient;
    }

    public long sendTimeMs() {
        return sendTimeMs;
    }

    public void setSend(Send send, long sendTimeMs) {
        this.send = send;
        this.sendTimeMs = sendTimeMs;
    }
}
