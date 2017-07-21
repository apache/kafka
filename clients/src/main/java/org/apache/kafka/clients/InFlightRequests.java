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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The set of requests which have been sent or are being sent but haven't yet received a response
 */
final class InFlightRequests {

    private final int maxInFlightRequestsPerConnection;
    private final Map<String, Deque<NetworkClient.InFlightRequest>> requests = new HashMap<>();
    private Integer minTimeoutMs;

    public InFlightRequests(int maxInFlightRequestsPerConnection) {
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
    }

    /**
     * Add the given request to the queue for the connection it was directed to
     */
    public void add(NetworkClient.InFlightRequest request) {
        String destination = request.destination;
        Deque<NetworkClient.InFlightRequest> reqs = this.requests.get(destination);
        if (reqs == null) {
            reqs = new ArrayDeque<>();
            this.requests.put(destination, reqs);
        }
        if (minTimeoutMs != null)
            minTimeoutMs = Math.min(request.timeoutMs, minTimeoutMs);
        reqs.addFirst(request);
    }

    /**
     * Get the request queue for the given node
     */
    private Deque<NetworkClient.InFlightRequest> requestQueue(String node) {
        Deque<NetworkClient.InFlightRequest> reqs = requests.get(node);
        if (reqs == null || reqs.isEmpty())
            throw new IllegalStateException("There are no in-flight requests for node " + node);
        return reqs;
    }

    /**
     * Complete the oldest request (the one that that will be completed next) for the given node
     */
    public NetworkClient.InFlightRequest completeNext(String node) {
        NetworkClient.InFlightRequest request = requestQueue(node).pollLast();
        minTimeoutMs = null;
        return request;
    }

    /**
     * Get the last request we sent to the given node (but don't remove it from the queue)
     * @param node The node id
     */
    public NetworkClient.InFlightRequest lastSent(String node) {
        return requestQueue(node).peekFirst();
    }

    /**
     * Complete the last request that was sent to a particular node.
     * @param node The node the request was sent to
     * @return The request
     */
    public NetworkClient.InFlightRequest completeLastSent(String node) {
        NetworkClient.InFlightRequest request = requestQueue(node).pollFirst();
        minTimeoutMs = null;
        return request;
    }

    /**
     * Can we send more requests to this node?
     *
     * @param node Node in question
     * @return true iff we have no requests still being sent to the given node
     */
    public boolean canSendMore(String node) {
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null || queue.isEmpty() ||
               (queue.peekFirst().send.completed() && queue.size() < this.maxInFlightRequestsPerConnection);
    }

    /**
     * Return the number of in-flight requests directed at the given node
     * @param node The node
     * @return The request count.
     */
    public int count(String node) {
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null ? 0 : queue.size();
    }

    /**
     * Return true if there is no in-flight request directed at the given node and false otherwise
     */
    public boolean isEmpty(String node) {
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null || queue.isEmpty();
    }

    /**
     * Count all in-flight requests for all nodes
     */
    public int count() {
        int total = 0;
        for (Deque<NetworkClient.InFlightRequest> deque : this.requests.values())
            total += deque.size();
        return total;
    }

    /**
     * Return true if there is no in-flight request and false otherwise
     */
    public boolean isEmpty() {
        for (Deque<NetworkClient.InFlightRequest> deque : this.requests.values()) {
            if (!deque.isEmpty())
                return false;
        }
        return true;
    }

    /**
     * Clear out all the in-flight requests for the given node and return them
     *
     * @param node The node
     * @return All the in-flight requests for that node that have been removed
     */
    public Iterable<NetworkClient.InFlightRequest> clearAll(String node) {
        Deque<NetworkClient.InFlightRequest> reqs = requests.remove(node);
        minTimeoutMs = null;
        return (reqs == null) ? Collections.<NetworkClient.InFlightRequest>emptyList() : reqs;
    }

    /**
     * Returns a list of nodes with pending in-flight request, that need to be timed out
     *
     * @param now current time in milliseconds
     * @return list of nodes
     */
    public List<String> getNodesWithTimedOutRequests(long now) {
        List<String> nodeIds = new ArrayList<>();
        for (Map.Entry<String, Deque<NetworkClient.InFlightRequest>> nodeRequests : requests.entrySet()) {
            String nodeId = nodeRequests.getKey();
            Deque<NetworkClient.InFlightRequest> deque = nodeRequests.getValue();

            for (NetworkClient.InFlightRequest request : deque) {
                if (request.hasExpired(now)) {
                    nodeIds.add(nodeId);
                    break;
                }
            }
        }
        return nodeIds;
    }

    /**
     * Return the minimum request timeout of all in flight requests or null if there are none.
     */
    public Integer minTimeoutMs() {
        if (minTimeoutMs == null && !requests.isEmpty()) {
            int timeoutMs = Integer.MAX_VALUE;
            for (Deque<NetworkClient.InFlightRequest> nodeRequests : requests.values()) {
                for (NetworkClient.InFlightRequest request : nodeRequests)
                    timeoutMs = Math.min(request.timeoutMs, timeoutMs);
            }
            minTimeoutMs = timeoutMs;
        }
        return minTimeoutMs;
    }
}
