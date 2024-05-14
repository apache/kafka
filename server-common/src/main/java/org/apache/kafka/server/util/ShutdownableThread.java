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

package org.apache.kafka.server.util;

import org.apache.kafka.common.internals.FatalExitError;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class ShutdownableThread extends Thread {

    public final String logPrefix;

    protected final Logger log;

    private final boolean isInterruptible;

    private final CountDownLatch shutdownInitiated = new CountDownLatch(1);
    private final CountDownLatch shutdownComplete = new CountDownLatch(1);

    private volatile boolean isStarted = false;

    public ShutdownableThread(String name) {
        this(name, true);
    }

    public ShutdownableThread(String name, boolean isInterruptible) {
        this(name, isInterruptible, "[" + name + "]: ");
    }

    @SuppressWarnings("this-escape")
    public ShutdownableThread(String name, boolean isInterruptible, String logPrefix) {
        super(name);
        this.isInterruptible = isInterruptible;
        this.logPrefix = logPrefix;
        log = new LogContext(logPrefix).logger(this.getClass());
        this.setDaemon(false);
    }

    public void shutdown() throws InterruptedException {
        initiateShutdown();
        awaitShutdown();
    }

    public boolean isShutdownInitiated() {
        return shutdownInitiated.getCount() == 0;
    }

    public boolean isShutdownComplete() {
        return shutdownComplete.getCount() == 0;
    }

    public boolean isStarted() {
        return isStarted;
    }

    /**
     * @return true if there has been an unexpected error and the thread shut down
     */
    // mind that run() might set both when we're shutting down the broker
    // but the return value of this function at that point wouldn't matter
    public boolean isThreadFailed() {
        return isShutdownComplete() && !isShutdownInitiated();
    }

    /**
     * @return true if the thread hasn't initiated shutdown already
     */
    public boolean initiateShutdown() {
        synchronized (this) {
            if (isRunning()) {
                log.info("Shutting down");
                shutdownInitiated.countDown();
                if (isInterruptible)
                    interrupt();
                return true;
            } else
                return false;
        }
    }

    /**
     * After calling initiateShutdown(), use this API to wait until the shutdown is complete.
     */
    public void awaitShutdown() throws InterruptedException {
        if (!isShutdownInitiated())
            throw new IllegalStateException("initiateShutdown() was not called before awaitShutdown()");
        else {
            if (isStarted)
                shutdownComplete.await();
            log.info("Shutdown completed");
        }
    }

    /**
     * Causes the current thread to wait until the shutdown is initiated,
     * or the specified waiting time elapses.
     *
     * @param timeout wait time in units.
     * @param unit    TimeUnit value for the wait time.
     */
    public void pause(long timeout, TimeUnit unit) throws InterruptedException {
        if (shutdownInitiated.await(timeout, unit))
            log.trace("shutdownInitiated latch count reached zero. Shutdown called.");
    }

    /**
     * This method is repeatedly invoked until the thread shuts down or this method throws an exception
     */
    public abstract void doWork();

    public void run() {
        isStarted = true;
        log.info("Starting");
        try {
            while (isRunning())
                doWork();
        } catch (FatalExitError e) {
            shutdownInitiated.countDown();
            shutdownComplete.countDown();
            log.info("Stopped");
            Exit.exit(e.statusCode());
        } catch (Throwable e) {
            if (isRunning())
                log.error("Error due to", e);
        } finally {
            shutdownComplete.countDown();
        }
        log.info("Stopped");
    }

    public boolean isRunning() {
        return !isShutdownInitiated();
    }

}
