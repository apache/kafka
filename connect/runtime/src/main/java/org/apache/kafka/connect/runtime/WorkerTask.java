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

package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles processing for an individual task. This interface only provides the basic methods
 * used by {@link Worker} to manage the tasks. Implementations combine a user-specified Task with
 * Kafka to create a data flow.
 */
abstract class WorkerTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(WorkerTask.class);

    protected final ConnectorTaskId id;
    private final AtomicBoolean stopping;
    private final AtomicBoolean running;
    private final CountDownLatch shutdownLatch;

    public WorkerTask(ConnectorTaskId id) {
        this.id = id;
        this.stopping = new AtomicBoolean(false);
        this.running = new AtomicBoolean(false);
        this.shutdownLatch = new CountDownLatch(1);
    }

    /**
     * Initialize the task for execution.
     * @param props initial configuration
     */
    public abstract void initialize(Map<String, String> props);

    /**
     * Stop this task from processing messages. This method does not block, it only triggers
     * shutdown. Use #{@link #awaitStop} to block until completion.
     */
    public void stop() {
        this.stopping.set(true);
    }

    /**
     * Wait for this task to finish stopping.
     *
     * @param timeoutMs
     * @return true if successful, false if the timeout was reached
     */
    public boolean awaitStop(long timeoutMs) {
        if (!running.get())
            return true;

        try {
            return shutdownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    protected abstract void execute();

    protected abstract void close();

    protected boolean isStopping() {
        return stopping.get();
    }

    private void doClose() {
        try {
            close();
        } catch (Throwable t) {
            log.error("Unhandled exception in task shutdown {}", id, t);
        } finally {
            running.set(false);
            shutdownLatch.countDown();
        }
    }

    @Override
    public void run() {
        if (!this.running.compareAndSet(false, true))
            throw new IllegalStateException("The task cannot be started while still running");

        try {
            if (stopping.get())
                return;

            execute();
        } catch (Throwable t) {
            log.error("Unhandled exception in task {}", id, t);
        } finally {
            doClose();
        }
    }

}
