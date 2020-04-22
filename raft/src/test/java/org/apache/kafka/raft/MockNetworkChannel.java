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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MockNetworkChannel implements NetworkChannel {
    private final AtomicInteger requestIdCounter;
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
    public int newRequestId() {
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
        List<RaftMessage> messages = receiveQueue;
        receiveQueue = new ArrayList<>();
        return messages;
    }

    @Override
    public void wakeup() {}

    @Override
    public void updateEndpoint(int id, InetSocketAddress address) {
        addressCache.put(id, address);
    }

    public List<RaftMessage> drainSendQueue() {
        List<RaftMessage> messages = sendQueue;
        sendQueue = new ArrayList<>();
        return messages;
    }

    public boolean hasSentMessages() {
        return !sendQueue.isEmpty();
    }

    public void mockReceive(RaftMessage message) {
        receiveQueue.add(message);
    }

    void clear() {
        sendQueue.clear();
        receiveQueue.clear();
        requestIdCounter.set(0);
    }

}
