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

import org.apache.kafka.common.protocol.ApiKeys;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class MockNetworkChannel implements NetworkChannel {
    private final AtomicInteger correlationIdCounter;
    private final List<RaftRequest.Outbound> sendQueue = new ArrayList<>();
    private final Map<Integer, RaftRequest.Outbound> awaitingResponse = new HashMap<>();
    private final Map<Integer, InetSocketAddress> addressCache = new HashMap<>();

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
    public void send(RaftRequest.Outbound request) {
        if (!addressCache.containsKey(request.destinationId())) {
            throw new IllegalArgumentException("Attempted to send to destination " +
                request.destinationId() + ", but its address is not yet known");
        }
        sendQueue.add(request);
    }

    @Override
    public void updateEndpoint(int id, InetSocketAddress address) {
        addressCache.put(id, address);
    }

    public List<RaftRequest.Outbound> drainSendQueue() {
        return drainSentRequests(Optional.empty());
    }

    public List<RaftRequest.Outbound> drainSentRequests(Optional<ApiKeys> apiKeyFilter) {
        List<RaftRequest.Outbound> requests = new ArrayList<>();
        Iterator<RaftRequest.Outbound> iterator = sendQueue.iterator();
        while (iterator.hasNext()) {
            RaftRequest.Outbound request = iterator.next();
            if (!apiKeyFilter.isPresent() || request.data().apiKey() == apiKeyFilter.get().id) {
                awaitingResponse.put(request.correlationId, request);
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
        RaftRequest.Outbound request = awaitingResponse.get(response.correlationId);
        if (request == null) {
            throw new IllegalStateException("Received response for a request which is not being awaited");
        }
        request.completion.complete(response);
    }

}
