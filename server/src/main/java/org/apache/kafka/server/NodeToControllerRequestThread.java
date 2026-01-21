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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Background thread that manages to send requests to the active controller.
 * <p>
 * Maintains a queue of pending requests and handles automatic retries on failures,
 * controller failover, and timeout detection. Requests are re-queued when the controller
 * changes or connections are lost.
 */
public class NodeToControllerRequestThread extends InterBrokerSendThread {
    private static final Logger log = LoggerFactory.getLogger(NodeToControllerRequestThread.class);

    private final LinkedBlockingDeque<NodeToControllerQueueItem> requestQueue = new LinkedBlockingDeque<>();
    private final AtomicReference<Node> activeController = new AtomicReference<>(null);


    private final Time time;
    private final long retryTimeoutMs;
    private final Supplier<ControllerInformation> controllerNodeProvider;
    private final ManualMetadataUpdater metadataUpdater;

    // Used for testing
    volatile boolean started = false;
    public void setStarted(boolean started) {
        this.started = started;
    }

    public NodeToControllerRequestThread(KafkaClient initialNetworkClient,
                                         ManualMetadataUpdater metadataUpdater,
                                         Supplier<ControllerInformation> controllerNodeProvider,
                                         AbstractConfig config,
                                         Time time,
                                         String threadName,
                                         Long retryTimeoutMs) {
        super(threadName, initialNetworkClient, Math.min(Integer.MAX_VALUE, (int) Math.min(config.getInt(ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG), retryTimeoutMs)), time, false);
        this.time = time;
        this.controllerNodeProvider = controllerNodeProvider;
        this.metadataUpdater = metadataUpdater;
        this.retryTimeoutMs = retryTimeoutMs;
    }

    public Optional<Node> activeControllerAddress() {
        return Optional.ofNullable(activeController.get());
    }

    private void updateControllerAddress(Node newActiveController) {
        activeController.set(newActiveController);
    }

    public void enqueue(NodeToControllerQueueItem request) {
        if (!started) {
            throw new IllegalStateException("Cannot enqueue a request if the request thread is not running");
        }
        requestQueue.add(request);
        if (activeControllerAddress().isPresent()) {
            wakeup();
        }
    }

    public int queueSize() {
        return requestQueue.size();
    }

    @Override
    public Collection<RequestAndCompletionHandler> generateRequests() {
        final long currentTimeMs = time.milliseconds();
        final Iterator<NodeToControllerQueueItem> requestIter = requestQueue.iterator();
        while (requestIter.hasNext()) {
            var request = requestIter.next();
            if (currentTimeMs - request.createdTimeMs() >= retryTimeoutMs) {
                requestIter.remove();
                request.callback().onTimeout();
            } else {
                Optional<Node> controllerAddress = activeControllerAddress();
                if (controllerAddress.isPresent()) {
                    requestIter.remove();
                    return List.of(new RequestAndCompletionHandler(
                            time.milliseconds(),
                            controllerAddress.get(),
                            request.request(),
                            response -> handleResponse(request, response)
                    ));
                }
            }
        }

        return List.of();
    }

    void handleResponse(NodeToControllerQueueItem queueItem, ClientResponse response) {
        log.debug("Request {} received {}", queueItem.request(), response);
        if (response.authenticationException() != null) {
            log.error("Request {} failed due to authentication error with controller. Disconnecting the " +
                            "connection to the stale controller {}",
                    queueItem.request(), activeControllerAddress().map(Node::idString).orElse("null"),
                    response.authenticationException()
            );
            maybeDisconnectAndUpdateController();
            queueItem.callback().onComplete(response);
        } else if (response.versionMismatch() != null) {
            log.error("Request {} failed due to unsupported version error", queueItem.request(),
                    response.versionMismatch());
            queueItem.callback().onComplete(response);
        } else if (response.wasDisconnected()) {
            updateControllerAddress(null);
            requestQueue.addFirst(queueItem);
        } else if (response.responseBody().errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
            log.debug("Request {} received NOT_CONTROLLER exception. Disconnecting the " +
                            "connection to the stale controller {}",
                    queueItem.request(),
                    activeControllerAddress().map(Node::idString).orElse("null"));
            maybeDisconnectAndUpdateController();
            requestQueue.addFirst(queueItem);
        } else {
            queueItem.callback().onComplete(response);
        }
    }

    private void maybeDisconnectAndUpdateController() {
        // just close the controller connection and wait for metadata cache update in doWork
        activeControllerAddress().ifPresent(controllerAddress -> {
            try {
                // We don't care if disconnect has an error, just log it and get a new network client
                networkClient.disconnect(controllerAddress.idString());
            } catch (Throwable t) {
                log.error("Had an error while disconnecting from NetworkClient.", t);
            }
            updateControllerAddress(null);
        });
    }

    @Override
    public void doWork() {
        if (activeControllerAddress().isPresent()) {
            super.pollOnce(Long.MAX_VALUE);
        } else {
            log.debug("Controller isn't cached, looking for local metadata changes");
            final ControllerInformation controllerInformation = controllerNodeProvider.get();
            Optional<Node> nodeOptional = controllerInformation.node();
            if (nodeOptional.isPresent()) {
                Node controllerNode = nodeOptional.get();
                log.info("Recorded new KRaft controller, from now on will use node {}", controllerNode);
                updateControllerAddress(controllerNode);
                metadataUpdater.setNodes(List.of(controllerNode));
            } else {
                // need to backoff to avoid tight loops
                log.debug("No controller provided, retrying after backoff");
                super.pollOnce(100);
            }
        }
    }

    @Override
    public void start() {
        super.start();
        started = true;
    }
}
