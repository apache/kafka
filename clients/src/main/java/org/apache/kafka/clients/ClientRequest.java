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

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;

/**
 * A request being sent to the server. This holds both the network send as well as the client-level metadata.
 */
public final class ClientRequest {

    private final String destination;
    private final RequestHeader header;
    private final AbstractRequest body;
    private final long createdTimeMs;
    private final boolean expectResponse;
    private final RequestCompletionHandler callback;

    /**
     * @param destination The brokerId to send the request to
     * @param createdTimeMs The unix timestamp in milliseconds for the time at which this request was created.
     * @param expectResponse Should we expect a response message or is this request complete once it is sent?
     * @param header The request's header
     * @param body The request's body
     * @param callback A callback to execute when the response has been received (or null if no callback is necessary)
     */
    public ClientRequest(String destination,
                         long createdTimeMs,
                         boolean expectResponse,
                         RequestHeader header,
                         AbstractRequest body,
                         RequestCompletionHandler callback) {
        this.destination = destination;
        this.createdTimeMs = createdTimeMs;
        this.callback = callback;
        this.header = header;
        this.body = body;
        this.expectResponse = expectResponse;
    }

    @Override
    public String toString() {
        return "ClientRequest(expectResponse=" + expectResponse +
            ", callback=" + callback +
            ", header=" + header +
            ", body=" + body +
            ", createdTimeMs=" + createdTimeMs +
            ")";
    }

    public boolean expectResponse() {
        return expectResponse;
    }

    public RequestHeader header() {
        return header;
    }

    public AbstractRequest body() {
        return body;
    }

    public String destination() {
        return destination;
    }

    public RequestCompletionHandler callback() {
        return callback;
    }

    public long createdTimeMs() {
        return createdTimeMs;
    }

}
