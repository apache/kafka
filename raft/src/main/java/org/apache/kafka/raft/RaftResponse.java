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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiMessage;

public abstract class RaftResponse implements RaftMessage {
    private final int correlationId;
    private final ApiMessage data;

    protected RaftResponse(int correlationId, ApiMessage data) {
        this.correlationId = correlationId;
        this.data = data;
    }

    @Override
    public int correlationId() {
        return correlationId;
    }

    @Override
    public ApiMessage data() {
        return data;
    }

    public static final class Inbound extends RaftResponse {
        private final Node source;

        public Inbound(int correlationId, ApiMessage data, Node source) {
            super(correlationId, data);
            this.source = source;
        }

        public Node source() {
            return source;
        }

        @Override
        public String toString() {
            return String.format(
                "InboundResponse(correlationId=%d, data=%s, source=%s)",
                correlationId(),
                data(),
                source
            );
        }
    }

    public static final class Outbound extends RaftResponse {
        public Outbound(int requestId, ApiMessage data) {
            super(requestId, data);
        }

        @Override
        public String toString() {
            return String.format(
                "OutboundResponse(correlationId=%d, data=%s)",
                correlationId(),
                data()
            );
        }
    }
}
