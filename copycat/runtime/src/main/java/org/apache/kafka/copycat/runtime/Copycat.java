/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.runtime;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.copycat.runtime.rest.RestServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class ties together all the components of a Copycat process (herder, worker,
 * storage, command interface), managing their lifecycle.
 */
@InterfaceStability.Unstable
public class Copycat {
    private static final Logger log = LoggerFactory.getLogger(Copycat.class);

    private final Worker worker;
    private final Herder herder;
    private final RestServer rest;
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ShutdownHook shutdownHook;

    public Copycat(Worker worker, Herder herder, RestServer rest) {
        log.debug("Copycat created");
        this.worker = worker;
        this.herder = herder;
        this.rest = rest;
        shutdownHook = new ShutdownHook();
    }

    public void start() {
        log.info("Copycat starting");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        worker.start();
        herder.start();
        rest.start(herder);

        log.info("Copycat started");

        startLatch.countDown();
    }

    public void stop() {
        boolean wasShuttingDown = shutdown.getAndSet(true);
        if (!wasShuttingDown) {
            log.info("Copycat stopping");

            rest.stop();
            herder.stop();
            worker.stop();

            log.info("Copycat stopped");
        }

        stopLatch.countDown();
    }

    public void awaitStop() {
        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            log.error("Interrupted waiting for Copycat to shutdown");
        }
    }

    private class ShutdownHook extends Thread {
        @Override
        public void run() {
            try {
                startLatch.await();
                Copycat.this.stop();
            } catch (InterruptedException e) {
                log.error("Interrupted in shutdown hook while waiting for copycat startup to finish");
            }
        }
    }
}
