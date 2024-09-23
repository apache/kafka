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
package org.apache.kafka.connect.runtime.standalone;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.util.Callback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Thread that can be used to check for the readiness and liveness of a standalone herder.
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-1017%3A+Health+check+endpoint+for+Kafka+Connect">KIP-1017</a>
 */
class HealthCheckThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(HealthCheckThread.class);

    private final StandaloneHerder herder;
    private final Queue<Callback<Void>> callbacks;
    private volatile boolean running;

    public HealthCheckThread(StandaloneHerder herder) {
        this.herder = herder;
        this.callbacks = new LinkedList<>();
        this.running = true;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Callback<Void> callback;
                synchronized (this) {
                    while (running && this.callbacks.isEmpty())
                        wait();

                    if (!running)
                        break;

                    callback = callbacks.remove();
                }

                // For now, our only criteria for liveness (as opposed to readiness)
                // in standalone mode is that the worker isn't deadlocked
                synchronized (herder) {
                    try {
                        callback.onCompletion(null, null);
                    } catch (Throwable t) {
                        log.warn("Failed to complete health check callback", t);
                    }
                }
            } catch (Throwable t) {
                log.warn("Health check thread encountered unexpected error", t);
            }
        }

        Throwable shuttingDown = new ConnectException("The herder is shutting down");

        // Don't leave callbacks dangling, even if shutdown has already started
        while (!this.callbacks.isEmpty()) {
            try {
                callbacks.remove().onCompletion(shuttingDown, null);
            } catch (Throwable t) {
                log.warn("Failed to complete health check callback during shutdown", t);
            }
        }
    }

    /**
     * Check the health of the herder. This method may be called at any time after
     * the thread has been constructed, but callbacks will only be invoked after this
     * thread has been {@link #start() started}.
     *
     * @param callback callback to invoke after herder health has been verified or if
     *                 an error occurs that indicates herder is unhealthy; may not be null
     * @throws  IllegalStateException if invoked after {@link #shutDown()}
     */
    public void check(Callback<Void> callback) {
        if (callback == null) {
            log.warn("Ignoring null callback");
            return;
        }

        synchronized (this) {
            if (!running) {
                throw new IllegalStateException("Cannot check herder health after thread has been shut down");
            }

            this.callbacks.add(callback);
            notifyAll();
        }
    }

    /**
     * Trigger shutdown of the thread, stop accepting new callbacks via {@link #check(Callback)},
     * and await the termination of the thread.
     */
    public void shutDown() {
        synchronized (this) {
            this.running = false;
            notifyAll();
        }

        try {
            join();
        } catch (InterruptedException e) {
            log.warn(
                    "Interrupted during graceful shutdown; will interrupt health check thread "
                            + "and then return immediately without waiting for thread to terminate",
                    e
            );
            this.interrupt();
            // Preserve the interrupt status in case later operations block too
            Thread.currentThread().interrupt();
        }
    }

}
