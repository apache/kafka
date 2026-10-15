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
package org.apache.kafka.server;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.util.MockTime;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Supplier;

public class MockNodeToControllerChannelManager implements NodeToControllerChannelManager {

    private final ConcurrentLinkedDeque<NodeToControllerQueueItem> unsentQueue = new ConcurrentLinkedDeque<>();
    private final MockClient client;
    private final MockTime time;
    private final Supplier<ControllerInformation> controllerNodeProvider;
    private final NodeApiVersions controllerApiVersions;
    private final int retryTimeoutMs = 60000;

    public MockNodeToControllerChannelManager(
            MockClient client,
            MockTime time,
            Supplier<ControllerInformation> controllerNodeProvider,
            NodeApiVersions controllerApiVersions
    ) {
        this.client = client;
        this.time = time;
        this.controllerNodeProvider = controllerNodeProvider;
        this.controllerApiVersions = controllerApiVersions;
        client.setNodeApiVersions(controllerApiVersions);
    }

    @Override
    public void start() { }

    @Override
    public void shutdown() { }

    @Override
    public void sendRequest(
        AbstractRequest.Builder<? extends AbstractRequest> request,
        ControllerRequestCompletionHandler callback
    ) {
        unsentQueue.add(new NodeToControllerQueueItem(
                time.milliseconds(),
                request,
                callback
        ));
    }

    @Override
    public Optional<NodeApiVersions> controllerApiVersions() {
        return Optional.of(controllerApiVersions);
    }

    @Override
    public long getTimeoutMs() {
        return retryTimeoutMs;
    }

    private void handleResponse(NodeToControllerQueueItem request, ClientResponse response) {
        if (response.authenticationException() != null || response.versionMismatch() != null) {
            request.callback().onComplete(response);
        } else if (response.wasDisconnected() || response.responseBody().errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
            unsentQueue.addFirst(request);
        } else {
            request.callback().onComplete(response);
        }
    }

    public void poll() {
        Iterator<NodeToControllerQueueItem> unsentIterator = unsentQueue.iterator();
        boolean canSend = true;

        while (canSend && unsentIterator.hasNext()) {
            NodeToControllerQueueItem queueItem = unsentIterator.next();
            long elapsedTimeMs = time.milliseconds() - queueItem.createdTimeMs();
            if (elapsedTimeMs >= retryTimeoutMs) {
                queueItem.callback().onTimeout();
                unsentIterator.remove();
            } else {
                Optional<Node> nodeOpt = controllerNodeProvider.get().node();
                if (nodeOpt.isPresent() && client.ready(nodeOpt.get(), time.milliseconds())) {
                    Node controller = nodeOpt.get();
                    ClientRequest clientRequest = client.newClientRequest(
                            controller.idString(),
                            queueItem.request(),
                            queueItem.createdTimeMs(),
                            true, // we expect response,
                            30000,
                            response -> handleResponse(queueItem, response)
                    );
                    client.send(clientRequest, time.milliseconds());
                    unsentIterator.remove();
                } else {
                    canSend = false;
                }
            }
        }
        client.poll(0L, time.milliseconds());
    }

    public ConcurrentLinkedDeque<NodeToControllerQueueItem> unsentQueue() {
        return unsentQueue;
    }
}
