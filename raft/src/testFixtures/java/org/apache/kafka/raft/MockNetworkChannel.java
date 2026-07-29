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

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

public class MockNetworkChannel implements NetworkChannel {
    public static final ListenerName LISTENER_NAME = VoterSetTestUtil.DEFAULT_LISTENER_NAME;

    private final AtomicInteger correlationIdCounter;
    private final List<RequestEntry> sendQueue = new ArrayList<>();
    private final Map<Integer, CompletableFuture<RaftResponse.Inbound>> awaitingResponse = new HashMap<>();

    public MockNetworkChannel(AtomicInteger correlationIdCounter) {
        this.correlationIdCounter = correlationIdCounter;
    }

    public MockNetworkChannel() {
        this(new AtomicInteger(0));
    }

    @Override
    public int newCorrelationId() {
        return correlationIdCounter.getAndIncrement();
    }

    @Override
    public CompletionStage<RaftResponse.Inbound> send(RaftRequest.Outbound request) {
        var future = new CompletableFuture<RaftResponse.Inbound>();
        sendQueue.add(new RequestEntry(request, future));
        return future;
    }

    @Override
    public ListenerName listenerName() {
        return LISTENER_NAME;
    }

    @Override
    public void close() { }

    public List<RaftRequest.Outbound> drainSendQueue() {
        return drainSentRequests(Optional.empty());
    }

    public List<RaftRequest.Outbound> drainSentRequests(Optional<ApiKeys> apiKeyFilter) {
        var requests = new ArrayList<RaftRequest.Outbound>();
        var iterator = sendQueue.iterator();
        while (iterator.hasNext()) {
            var requestEntry = iterator.next();
            var request = requestEntry.request();
            var future = requestEntry.future();
            if (apiKeyFilter.isEmpty() || request.data().apiKey() == apiKeyFilter.get().id) {
                awaitingResponse.put(request.correlationId(), future);
                requests.add(request);
                iterator.remove();
            }
        }
        return requests;
    }

    public boolean hasSentRequests() {
        return !sendQueue.isEmpty();
    }

    public void mockReceive(RaftResponse.Inbound response) {
        var future = awaitingResponse.get(response.correlationId());
        if (future == null) {
            throw new IllegalStateException("Received response for a request which is not being awaited");
        }
        future.complete(response);
    }

    private record RequestEntry(
        RaftRequest.Outbound request,
        CompletableFuture<RaftResponse.Inbound> future
    ) { }
}
