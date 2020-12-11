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
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MockNetworkChannel implements NetworkChannel {
    private final AtomicInteger requestIdCounter;
    private final AtomicBoolean wakeupRequested = new AtomicBoolean(false);
    private final AtomicLong lastReceiveTimeout = new AtomicLong(-1);

    private List<RaftMessage> sendQueue = new ArrayList<>();
    private List<RaftMessage> receiveQueue = new ArrayList<>();
    private Map<Integer, InetSocketAddress> addressCache = new HashMap<>();

    public MockNetworkChannel(AtomicInteger requestIdCounter) {
        this.requestIdCounter = requestIdCounter;
    }

    public MockNetworkChannel() {
        this(new AtomicInteger(0));
    }

    @Override
    public int newCorrelationId() {
        return requestIdCounter.getAndIncrement();
    }

    @Override
    public void send(RaftMessage message) {
        if (message instanceof RaftRequest.Outbound) {
            RaftRequest.Outbound request = (RaftRequest.Outbound) message;
            if (!addressCache.containsKey(request.destinationId())) {
                throw new IllegalArgumentException("Attempted to send to destination " +
                    request.destinationId() + ", but its address is not yet known");
            }
        }
        sendQueue.add(message);
    }

    @Override
    public List<RaftMessage> receive(long timeoutMs) {
        wakeupRequested.set(false);
        lastReceiveTimeout.set(timeoutMs);
        List<RaftMessage> messages = receiveQueue;
        receiveQueue = new ArrayList<>();
        return messages;
    }

    OptionalLong lastReceiveTimeout() {
        long timeout = lastReceiveTimeout.get();
        if (timeout < 0) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(timeout);
        }
    }

    boolean wakeupRequested() {
        return wakeupRequested.get();
    }

    @Override
    public void wakeup() {
        wakeupRequested.set(true);
    }

    @Override
    public void updateEndpoint(int id, InetSocketAddress address) {
        addressCache.put(id, address);
    }

    public List<RaftMessage> drainSendQueue() {
        List<RaftMessage> messages = sendQueue;
        sendQueue = new ArrayList<>();
        return messages;
    }

    public List<RaftRequest.Outbound> drainSentRequests(ApiKeys apiKey) {
        List<RaftRequest.Outbound> requests = new ArrayList<>();
        Iterator<RaftMessage> iterator = sendQueue.iterator();
        while (iterator.hasNext()) {
            RaftMessage message = iterator.next();
            if (message instanceof RaftRequest.Outbound && message.data().apiKey() == apiKey.id) {
                RaftRequest.Outbound request = (RaftRequest.Outbound) message;
                requests.add(request);
                iterator.remove();
            }
        }
        return requests;
    }

    public List<RaftResponse.Outbound> drainSentResponses(ApiKeys apiKey) {
        List<RaftResponse.Outbound> responses = new ArrayList<>();
        Iterator<RaftMessage> iterator = sendQueue.iterator();
        while (iterator.hasNext()) {
            RaftMessage message = iterator.next();
            if (message instanceof RaftResponse.Outbound && message.data().apiKey() == apiKey.id) {
                RaftResponse.Outbound response = (RaftResponse.Outbound) message;
                responses.add(response);
                iterator.remove();
            }
        }
        return responses;
    }


    public boolean hasSentMessages() {
        return !sendQueue.isEmpty();
    }

    public void mockReceive(RaftMessage message) {
        receiveQueue.add(message);
    }

}
