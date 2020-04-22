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
package org.apache.kafka.raft;

import org.apache.kafka.common.protocol.ApiMessage;

public abstract class RaftRequest implements RaftMessage {
    protected final int requestId;
    protected final ApiMessage data;
    protected final long createdTimeMs;

    public RaftRequest(int requestId, ApiMessage data, long createdTimeMs) {
        this.requestId = requestId;
        this.data = data;
        this.createdTimeMs = createdTimeMs;
    }

    @Override
    public int requestId() {
        return requestId;
    }

    @Override
    public ApiMessage data() {
        return data;
    }

    public long createdTimeMs() {
        return createdTimeMs;
    }

    public static class Inbound extends RaftRequest {
        public Inbound(int requestId, ApiMessage data, long createdTimeMs) {
            super(requestId, data, createdTimeMs);
        }

        @Override
        public String toString() {
            return "InboundRequest(" +
                    "requestId=" + requestId +
                    ", data=" + data +
                    ", createdTimeMs=" + createdTimeMs +
                    ')';
        }
    }

    public static class Outbound extends RaftRequest {
        private final int destinationId;

        public Outbound(int requestId, ApiMessage data, int destinationId, long createdTimeMs) {
            super(requestId, data, createdTimeMs);
            this.destinationId = destinationId;
        }

        public int destinationId() {
            return destinationId;
        }

        @Override
        public String toString() {
            return "OutboundRequest(" +
                    "requestId=" + requestId +
                    ", data=" + data +
                    ", createdTimeMs=" + createdTimeMs +
                    ", destinationId=" + destinationId +
                    ')';
        }
    }
}
