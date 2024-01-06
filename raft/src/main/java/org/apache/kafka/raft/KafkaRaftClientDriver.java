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
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.server.util.ShutdownableThread;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

/**
 * A single-threaded driver for {@link KafkaRaftClient}. Client APIs will only do useful work
 * as long as the driver's thread is active. To start the thread, use {@link #start()}. To
 * stop it, use {@link #shutdown()}.
 *
 * Note that the driver is responsible for the lifecycle of the {@link KafkaRaftClient} instance.
 * Shutdown of the driver through {@link #shutdown()} ensures that the client itself is properly
 * shutdown and closed.
 *
 * @param <T> See {@link KafkaRaftClient<T>}
 */
public class KafkaRaftClientDriver<T> extends ShutdownableThread {
    /**
     * Closed in {@link #shutdown()} after shutdown completes.
     */
    private final KafkaRaftClient<T> client;
    private final Logger log;
    private final FaultHandler fatalFaultHandler;

    public KafkaRaftClientDriver(
        KafkaRaftClient<T> client,
        String threadNamePrefix,
        FaultHandler fatalFaultHandler,
        LogContext logContext
    ) {
        super(threadNamePrefix + "-io-thread", false);
        this.client = client;
        this.fatalFaultHandler = fatalFaultHandler;
        this.log = logContext.logger(KafkaRaftClientDriver.class);
    }

    @Override
    public void doWork() {
        try {
            client.poll();
        } catch (Throwable t) {
            throw fatalFaultHandler.handleFault("Unexpected error in raft IO thread", t);
        }
    }

    @Override
    public boolean initiateShutdown() {
        if (super.initiateShutdown()) {
            client.shutdown(5000).whenComplete((na, exception) -> {
                if (exception != null) {
                    log.error("Graceful shutdown of RaftClient failed", exception);
                } else {
                    log.info("Completed graceful shutdown of RaftClient");
                }
            });
            return true;
        } else {
            return false;
        }
    }

    /**
     * Shutdown the thread. In addition to stopping any utilized threads, this will
     * close the {@link KafkaRaftClient} instance.
     */
    @Override
    public void shutdown() throws InterruptedException {
        try {
            super.shutdown();
        } finally {
            client.close();
        }
    }

    @Override
    public boolean isRunning() {
        return client.isRunning() && !isThreadFailed();
    }

    public CompletableFuture<ApiMessage> handleRequest(
        RequestHeader header,
        ApiMessage request,
        long createdTimeMs
    ) {
        RaftRequest.Inbound inboundRequest = new RaftRequest.Inbound(
            header.correlationId(),
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
