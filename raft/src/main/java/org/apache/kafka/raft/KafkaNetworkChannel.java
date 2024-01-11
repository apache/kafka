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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchSnapshotRequest;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaNetworkChannel implements NetworkChannel {

    static class SendThread extends InterBrokerSendThread {

        private final Queue<RequestAndCompletionHandler> queue = new ConcurrentLinkedQueue<>();

        public SendThread(String name, KafkaClient networkClient, int requestTimeoutMs, Time time, boolean isInterruptible) {
            super(name, networkClient, requestTimeoutMs, time, isInterruptible);
        }

        @Override
        public Collection<RequestAndCompletionHandler> generateRequests() {
            List<RequestAndCompletionHandler> list =  new ArrayList<>();
            while (true) {
                RequestAndCompletionHandler request = queue.poll();
                if (request == null) {
                    return list;
                } else {
                    list.add(request);
                }
            }
        }

        public void sendRequest(RequestAndCompletionHandler request) {
            queue.add(request);
            wakeup();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(KafkaNetworkChannel.class);

    private final SendThread requestThread;

    private final AtomicInteger correlationIdCounter = new AtomicInteger(0);
    private final Map<Integer, Node> endpoints = new HashMap<>();

    public KafkaNetworkChannel(Time time, KafkaClient client, int requestTimeoutMs, String threadNamePrefix) {
        this.requestThread = new SendThread(
            threadNamePrefix + "-outbound-request-thread",
            client,
            requestTimeoutMs,
            time,
            false
        );
    }

    @Override
    public int newCorrelationId() {
        return correlationIdCounter.getAndIncrement();
    }

    @Override
    public void send(RaftRequest.Outbound request) {
        Node node = endpoints.get(request.destinationId());
        if (node != null) {
            requestThread.sendRequest(new RequestAndCompletionHandler(
                request.createdTimeMs,
                node,
                buildRequest(request.data),
                response -> sendOnComplete(request, response)
            ));
        } else
            sendCompleteFuture(request, errorResponse(request.data, Errors.BROKER_NOT_AVAILABLE));
    }

    private void sendCompleteFuture(RaftRequest.Outbound request, ApiMessage message) {
        RaftResponse.Inbound response = new RaftResponse.Inbound(
                request.correlationId,
                message,
                request.destinationId()
        );
        request.completion.complete(response);
    }

    private void sendOnComplete(RaftRequest.Outbound request, ClientResponse clientResponse) {
        ApiMessage response;
        if (clientResponse.versionMismatch() != null) {
            log.error("Request {} failed due to unsupported version error", request, clientResponse.versionMismatch());
            response = errorResponse(request.data, Errors.UNSUPPORTED_VERSION);
        } else if (clientResponse.authenticationException() != null) {
            // For now we treat authentication errors as retriable. We use the
            // `NETWORK_EXCEPTION` error code for lack of a good alternative.
            // Note that `NodeToControllerChannelManager` will still log the
            // authentication errors so that users have a chance to fix the problem.
            log.error("Request {} failed due to authentication error", request, clientResponse.authenticationException());
            response = errorResponse(request.data, Errors.NETWORK_EXCEPTION);
        } else if (clientResponse.wasDisconnected()) {
            response = errorResponse(request.data, Errors.BROKER_NOT_AVAILABLE);
        } else {
            response = clientResponse.responseBody().data();
        }
        sendCompleteFuture(request, response);
    }

    private ApiMessage errorResponse(ApiMessage request, Errors error) {
        ApiKeys apiKey = ApiKeys.forId(request.apiKey());
        return RaftUtil.errorResponse(apiKey, error);
    }

    @Override
    public void updateEndpoint(int id, RaftConfig.InetAddressSpec spec) {
        Node node = new Node(id, spec.address.getHostString(), spec.address.getPort());
        endpoints.put(id, node);
    }

    public void start() {
        requestThread.start();
    }

    @Override
    public void close() throws InterruptedException {
        requestThread.shutdown();
    }

    // Visible for testing
    public void pollOnce() {
        requestThread.doWork();
    }

    static AbstractRequest.Builder<? extends AbstractRequest> buildRequest(ApiMessage requestData) {
        if (requestData instanceof VoteRequestData)
            return new VoteRequest.Builder((VoteRequestData) requestData);
        if (requestData instanceof BeginQuorumEpochRequestData)
            return new BeginQuorumEpochRequest.Builder((BeginQuorumEpochRequestData) requestData);
        if (requestData instanceof EndQuorumEpochRequestData)
            return new EndQuorumEpochRequest.Builder((EndQuorumEpochRequestData) requestData);
        if (requestData instanceof FetchRequestData)
            return new FetchRequest.SimpleBuilder((FetchRequestData) requestData);
        if (requestData instanceof FetchSnapshotRequestData)
            return new FetchSnapshotRequest.Builder((FetchSnapshotRequestData) requestData);
        throw new IllegalArgumentException("Unexpected type for requestData: " + requestData);
    }
}
