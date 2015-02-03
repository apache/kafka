/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Time;

/**
 * A mock network client for use testing code
 */
public class MockClient implements KafkaClient {

    private final Time time;
    private int correlation = 0;
    private final Set<Integer> ready = new HashSet<Integer>();
    private final Queue<ClientRequest> requests = new ArrayDeque<ClientRequest>();
    private final Queue<ClientResponse> responses = new ArrayDeque<ClientResponse>();

    public MockClient(Time time) {
        this.time = time;
    }

    @Override
    public boolean isReady(Node node, long now) {
        return ready.contains(node.id());
    }

    @Override
    public boolean ready(Node node, long now) {
        boolean found = isReady(node, now);
        ready.add(node.id());
        return found;
    }

    @Override
    public long connectionDelay(Node node, long now) {
        return 0;
    }

    public void disconnect(Integer node) {
        Iterator<ClientRequest> iter = requests.iterator();
        while (iter.hasNext()) {
            ClientRequest request = iter.next();
            if (request.request().destination() == node) {
                responses.add(new ClientResponse(request, time.milliseconds(), true, null));
                iter.remove();
            }
        }
        ready.remove(node);
    }

    @Override
    public void send(ClientRequest request) {
        this.requests.add(request);
    }

    @Override
    public List<ClientResponse> poll(long timeoutMs, long now) {
        for (ClientResponse response: this.responses)
            if (response.request().hasCallback()) 
                response.request().callback().onComplete(response);
        List<ClientResponse> copy = new ArrayList<ClientResponse>();
        this.responses.clear();
        return copy;
    }

    @Override
    public List<ClientResponse> completeAll(int node, long now) {
        return completeAll(now);
    }

    @Override
    public List<ClientResponse> completeAll(long now) {
        List<ClientResponse> responses = poll(0, now);
        if (requests.size() > 0)
            throw new IllegalStateException("Requests without responses remain.");
        return responses;
    }

    public Queue<ClientRequest> requests() {
        return this.requests;
    }

    public void respond(Struct body) {
        ClientRequest request = requests.remove();
        responses.add(new ClientResponse(request, time.milliseconds(), false, body));
    }

    @Override
    public int inFlightRequestCount() {
        return requests.size();
    }

    @Override
    public int inFlightRequestCount(int nodeId) {
        return requests.size();
    }

    @Override
    public RequestHeader nextRequestHeader(ApiKeys key) {
        return new RequestHeader(key.id, "mock", correlation++);
    }

    @Override
    public void wakeup() {
    }

    @Override
    public void close() {
    }

    @Override
    public Node leastLoadedNode(long now) {
        return null;
    }

}
