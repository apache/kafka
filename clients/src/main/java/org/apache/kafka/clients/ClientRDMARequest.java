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

import com.ibm.disni.verbs.IbvSendWR;


import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * A request being sent to the server. This holds both the network send as well as the client-level metadata.
 */
public final class ClientRDMARequest {

    private final String destination;
    private final int correlationId;
    private final RDMAWrBuilder builder;
    private final String clientId;
    public final long createdTimeNanos;
    private final boolean expectSendCompletion;
    private final boolean expectResponse;
    private final RDMARequestCompletionHandler callback;


    public ClientRDMARequest(String destination,
                             int correlationId,
                             RDMAWrBuilder builder,
                             String clientId,
                             long createdTimeNanos,
                             boolean expectSendCompletion,
                             boolean expectResponse,
                             RDMARequestCompletionHandler callback) {
        this.destination = destination;
        this.correlationId = correlationId;
        this.builder = builder;
        this.clientId = clientId;
        this.createdTimeNanos = createdTimeNanos;
        this.expectSendCompletion = expectSendCompletion;
        this.expectResponse = expectResponse;
        this.callback = callback;

    }

    @Override
    public String toString() {
        return "ClientRequest(expectResponse=" + expectResponse +
            ", callback=" + callback +
            ", destination=" + destination +
            ", correlationId=" + correlationId +
            ", clientId=" + clientId +
            ")";
    }

    public ByteBuffer getBuffer() {
        return builder.getTargetBuffer();
    }

    public boolean expectSendCompletion() {
        return expectSendCompletion;
    }

    public boolean expectResponse() {
        return expectResponse;
    }

    public String destination() {
        return destination;
    }

    public RDMARequestCompletionHandler callback() {
        return callback;
    }

    public int correlationId() {
        return correlationId;
    }

    public LinkedList<IbvSendWR> getWRs() {
        return builder.build();
    }

    public RDMAWrBuilder getBuilder() {
        return builder;
    }

}
