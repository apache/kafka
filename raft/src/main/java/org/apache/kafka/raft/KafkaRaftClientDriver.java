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

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.server.util.EventExecutor;

import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;

/**
 * A single-threaded driver for {@link KafkaRaftClient}. Client APIs will only do useful work
 * as long as the driver is active. To start it, use {@link #start()}. To stop it, use
 * {@link #shutdown()}.
 *
 * <p>The driver uses an {@link EventExecutor} to schedule poll events. Each poll event calls
 * {@link KafkaRaftClient#poll()} and then re-submits itself for the next iteration, forming
 * a self-rescheduling loop.
 *
 * <p>Note that the driver is responsible for the lifecycle of the {@link KafkaRaftClient} instance.
 * Shutdown of the driver through {@link #shutdown()} ensures that the client itself is properly
 * shutdown and closed.
 *
 * @param <T> See {@link KafkaRaftClient}
 */
public class KafkaRaftClientDriver<T> {
    /**
     * Closed in {@link #shutdown()} after shutdown completes.
     */
    private final KafkaRaftClient<T> client;
    private final EventExecutor eventExecutor;
    private final Logger log;
    private final FaultHandler fatalFaultHandler;

    public KafkaRaftClientDriver(
        KafkaRaftClient<T> client,
        EventExecutor eventExecutor,
        FaultHandler fatalFaultHandler,
        LogContext logContext
    ) {
        this.client = client;
        this.eventExecutor = eventExecutor;
        this.fatalFaultHandler = fatalFaultHandler;
        this.log = logContext.logger(KafkaRaftClientDriver.class);
    }

    /**
     * Start the driver by submitting the first poll event to the event executor.
     */
    public void start() {
        schedulePoll();
    }

    private void schedulePoll() {
        try {
            eventExecutor.submit(this::doPoll);
        } catch (RejectedExecutionException e) {
            // Event executor has been shut down; stop polling
        }
    }

    private void doPoll() {
        try {
            client.poll();
        } catch (Throwable t) {
            fatalFaultHandler.handleFault("Unexpected error in raft IO thread", t);
            return;
        }
        if (client.isRunning()) {
            schedulePoll();
        }
    }

    /**
     * Shutdown the driver. This initiates a graceful shutdown of the {@link KafkaRaftClient},
     * waits for the event executor to drain all pending events, and then closes the client.
     */
    public void shutdown() throws InterruptedException {
        client.shutdown(5000).whenComplete((v, ex) -> {
            if (ex != null) {
                log.error("Graceful shutdown of RaftClient failed", ex);
            } else {
                log.info("Completed graceful shutdown of RaftClient");
            }
        });
        try {
            eventExecutor.shutdown().get();
        } catch (ExecutionException e) {
            log.error("Error while shutting down event executor", e);
        } finally {
            client.close();
        }
    }

    public boolean isRunning() {
        return client.isRunning();
    }

    public CompletableFuture<ApiMessage> handleRequest(
        RequestContext context,
        RequestHeader header,
        ApiMessage request,
        long createdTimeMs
    ) {
        RaftRequest.Inbound inboundRequest = new RaftRequest.Inbound(
            context.listenerName,
            header.correlationId(),
            header.apiVersion(),
            request,
            createdTimeMs
        );

        client.handle(inboundRequest);

        return inboundRequest.completion.thenApply(RaftMessage::data);
    }

    public KafkaRaftClient<T> client() {
        return client;
    }
}
