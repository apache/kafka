/*
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
 *
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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

public class NodeToControllerRequestThread extends InterBrokerSendThread {
    private static final Logger log = LoggerFactory.getLogger(NodeToControllerRequestThread.class);

    private final LinkedBlockingDeque<NodeToControllerQueueItem> requestQueue = new LinkedBlockingDeque<>();
    private final AtomicReference<Node> activeController = new AtomicReference<>(null);

    // Used to testing
    volatile boolean started = false;
    private final Time time;
    private long retryTimeoutMs;

    public NodeToControllerRequestThread(KafkaClient initialNetworkClient,
                                         ManualMetadataUpdater metadataUpdater,
                                         ControllerNodeProvider controllerNodeProvider,
                                         AbstractConfig config,
                                         Time time,
                                         String threadName,
                                         Long retryTimeoutMs) {
        super(threadName, initialNetworkClient, Math.min(Integer.MAX_VALUE, (int) Math.min(config.getLong(ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG), retryTimeoutMs)), time, false);
        this.time = time;

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
        final var currentTimeMs = time.milliseconds();
        final var requestIter = requestQueue.iterator();
        while (requestIter.hasNext()) {
            var request = requestIter.next();
            if (currentTimeMs - request.createdTimeMs() >= retryTimeoutMs) {
                requestIter.remove();
                request.callback().onTimeout();
            } else {
                var controllerAddress = activeControllerAddress();
                if (controllerAddress.isPresent()) {
                    requestIter.remove();
                    return Collections.singletonList(new RequestAndCompletionHandler(
                            time.milliseconds(),
                            controllerAddress.get(),
                            request.request(),
                            response -> handleResponse(request, response)
                    ));
                }
            }
        }

        return Collections.emptyList();
    }

    void handleResponse(NodeToControllerQueueItem queueItem, ClientResponse response) {
        log.debug("Request {} received {}", queueItem.request(), response);
        if(response.authenticationException() != null) {
            log.error("Request {} failed due to authentication error with controller. Disconnecting the " +
                    "connection to the stale controller {}",
                    queueItem.request(), activeControllerAddress().map(Node::idString).orElse("null"),
                    response.authenticationException()
            );
            maybeDisconnectAndUpdateController();
            queueItem.callback().onComplete(response);
        } else if (response.versionMismatch() != null ) {
            log.error("Request {} failed due to unsupported version error", queueItem.request(),
                    response.versionMismatch());
            queueItem.callback().onComplete(response);
        } else if (response.wasDisconnected()) {
            updateControllerAddress(null);
            try {
                requestQueue.putFirst(queueItem);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Thread interrupted while re-queuing request after disconnection", e);
            }
        } else if (response.responseBody().errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
            log.debug("Request {} received NOT_CONTROLLER exception. Disconnecting the " +
                            "connection to the stale controller {}",
                    queueItem.request(),
                    activeControllerAddress().map(Node::idString).orElse("null"));
            maybeDisconnectAndUpdateController();
            try {
                requestQueue.putFirst(queueItem);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Thread interrupted while re-queuing request after NOT_CONTROLLER", e);
            }
        } else {
            queueItem.callback().onComplete(response);
        }
    }

    private void maybeDisconnectAndUpdateController() {
    }
}
