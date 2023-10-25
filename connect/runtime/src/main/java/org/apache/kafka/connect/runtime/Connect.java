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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.connect.runtime.rest.ConnectRestServer;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class ties together all the components of a Kafka Connect process (herder, worker,
 * storage, command interface), managing their lifecycle.
 */
public class Connect {
    private static final Logger log = LoggerFactory.getLogger(Connect.class);

    private final Herder herder;
    private final ConnectRestServer rest;
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ShutdownHook shutdownHook;

    public Connect(Herder herder, ConnectRestServer rest) {
        log.debug("Kafka Connect instance created");
        this.herder = herder;
        this.rest = rest;
        shutdownHook = new ShutdownHook();
    }

    public void start() {
        try {
            log.info("Kafka Connect starting");
            Exit.addShutdownHook("connect-shutdown-hook", shutdownHook);

            herder.start();
            rest.initializeResources(herder);

            log.info("Kafka Connect started");
        } finally {
            startLatch.countDown();
        }
    }

    public void stop() {
        try {
            boolean wasShuttingDown = shutdown.getAndSet(true);
            if (!wasShuttingDown) {
                log.info("Kafka Connect stopping");

                rest.stop();
                herder.stop();

                log.info("Kafka Connect stopped");
            }
        } finally {
            stopLatch.countDown();
        }
    }

    public void awaitStop() {
        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            log.error("Interrupted waiting for Kafka Connect to shutdown");
        }
    }

    public boolean isRunning() {
        return herder.isRunning();
    }

    // Visible for testing
    public RestServer rest() {
        return rest;
    }

    private class ShutdownHook extends Thread {
        @Override
        public void run() {
            try {
                startLatch.await();
                Connect.this.stop();
            } catch (InterruptedException e) {
                log.error("Interrupted in shutdown hook while waiting for Kafka Connect startup to finish");
            }
        }
    }
}
