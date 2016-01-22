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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class AbstractWorkerTask implements WorkerTask {
    private static final Logger log = LoggerFactory.getLogger(AbstractWorkerTask.class);

    protected final ConnectorTaskId id;
    private final AtomicBoolean stopping;
    private final AtomicBoolean running;
    private final CountDownLatch shutdownLatch;

    public AbstractWorkerTask(ConnectorTaskId id) {
        this.id = id;
        this.stopping = new AtomicBoolean(false);
        this.running = new AtomicBoolean(false);
        this.shutdownLatch = new CountDownLatch(1);
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
            return;

        try {
            execute();
        } catch (Throwable t) {
            log.error("Unhandled exception in task {}", id, t);
        } finally {
            doClose();
        }
    }

    @Override
    public void stop() {
        this.stopping.set(true);
    }

    @Override
    public boolean awaitStop(long timeoutMs) {
        if (!running.get())
            return true;

        try {
            return shutdownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

}
