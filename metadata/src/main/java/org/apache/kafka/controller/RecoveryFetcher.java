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
package org.apache.kafka.controller;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.GetReplicaLogInfoResponse;

interface RecoveryFetcher {
    interface Receiver {
        void receive(Result response);
    }

    interface Sender {
        void enqueueRequest(Receiver receiver, Request request);
    }

    enum ResultStatus {
        HasResults,
        Timeout,
        AuthError,
        Disconnect,
    }

    class Request {
        public final AbstractRequest.Builder<?> builder;
        public final Node node;
        public final int retryCount;
        public final int requestId;

        public Request(AbstractRequest.Builder<?> builder,
                       Node node,
                       int retryCount,
                       int requestId) {
            this.builder = builder;
            this.node = node;
            this.retryCount = retryCount;
            this.requestId = requestId;
        }
    }

    class Result {
        final ResultStatus status;
        final GetReplicaLogInfoResponseData response;
        final Request previous;

        Result(ResultStatus status, GetReplicaLogInfoResponseData response, Request previous) {
            this.status = status;
            this.response = response;
            this.previous = previous;
        }

        static Result fromResponse(ClientResponse clientResponse, Request work) {
            ResultStatus status = ResultStatus.HasResults;
            if (clientResponse.hasResponse()) {
                GetReplicaLogInfoResponse response = (GetReplicaLogInfoResponse) clientResponse.responseBody();
                return new Result(status, response.data(), work);
            }
            if (clientResponse.wasTimedOut()) {
                status = ResultStatus.Timeout;
            } else if (clientResponse.wasDisconnected()) {
                status = ResultStatus.Disconnect;
            } else if (clientResponse.authenticationException() != null) {
                status = ResultStatus.AuthError;
            }
            return new Result(status, null, work);
        }

        public boolean hasResults() {
            return status == ResultStatus.HasResults;
        }
    }
}
