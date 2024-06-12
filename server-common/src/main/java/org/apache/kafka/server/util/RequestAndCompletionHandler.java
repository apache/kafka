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
package org.apache.kafka.server.util;

import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.AbstractRequest;

public final class RequestAndCompletionHandler {

    public final long creationTimeMs;
    public final Node destination;
    public final AbstractRequest.Builder<? extends AbstractRequest> request;
    public final RequestCompletionHandler handler;

    public RequestAndCompletionHandler(
        long creationTimeMs,
        Node destination,
        AbstractRequest.Builder<? extends AbstractRequest> request,
        RequestCompletionHandler handler
    ) {
        this.creationTimeMs = creationTimeMs;
        this.destination = destination;
        this.request = request;
        this.handler = handler;
    }

    @Override
    public String toString() {
        return "RequestAndCompletionHandler(" +
            "creationTimeMs=" + creationTimeMs +
            ", destination=" + destination +
            ", request=" + request +
            ", handler=" + handler +
            ')';
    }
}
