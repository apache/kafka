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

/**
 * Handles processing for an individual task. This interface only provides the basic methods
 * used by {@link Worker} to manage the tasks. Implementations combine a user-specified Task with
 * Kafka to create a data flow.
 *
 * Note on locking: since the task runs in its own thread, special care must be taken to ensure
 * that state transitions are reported correctly, in particular since some state transitions are
 * asynchronous (e.g. pause/resume). For example, changing the state to paused could cause a race
 * if the task fails at the same time. To protect from these cases, we synchronize status updates
 * using the WorkerTask's monitor.
 */
abstract class WorkerTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(WorkerTask.class);

    protected final ConnectorTaskId id;
    private final TaskStatus.Listener statusListener;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private volatile TargetState targetState;
    private volatile boolean stopping;   // indicates whether the Worker has asked the task to stop
    private volatile boolean cancelled;  // indicates whether the Worker has cancelled the task (e.g. because of slow shutdown)

    public WorkerTask(ConnectorTaskId id,
                      TaskStatus.Listener statusListener,
                      TargetState initialState) {
        this.id = id;
        this.statusListener = statusListener;
        this.targetState = initialState;
        this.stopping = false;
        this.cancelled = false;
    }

    public ConnectorTaskId id() {
        return id;
    }

    /**
     * Initialize the task for execution.
     * @param taskConfig initial configuration
     */
    public abstract void initialize(TaskConfig taskConfig);


    private void triggerStop() {
        synchronized (this) {
            stopping = true;

            // wakeup any threads that are waiting for unpause
            this.notifyAll();
        }
    }

    /**
     * Stop this task from processing messages. This method does not block, it only triggers
     * shutdown. Use #{@link #awaitStop} to block until completion.
     */
    public void stop() {
        triggerStop();
    }

    /**
     * Cancel this task. This won't actually stop it, but it will prevent the state from being
     * updated when it eventually does shutdown.
     */
    public void cancel() {
        cancelled = true;
    }

    /**
     * Wait for this task to finish stopping.
     *
     * @param timeoutMs time in milliseconds to await stop
     * @return true if successful, false if the timeout was reached
     */
    public boolean awaitStop(long timeoutMs) {
        try {
            return shutdownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    protected abstract void execute();

    protected abstract void close();

    protected boolean isStopping() {
        return stopping;
    }

    private void doClose() {
        try {
            close();
        } catch (Throwable t) {
            log.error("Task {} threw an uncaught and unrecoverable exception during shutdown", id, t);
            throw t;
        }
    }

    private void doRun() throws InterruptedException {
        try {
            synchronized (this) {
                if (stopping)
                    return;

                if (targetState == TargetState.PAUSED) {
                    onPause();
                    if (!awaitUnpause()) return;
                }

                statusListener.onStartup(id);
            }

            execute();
        } catch (Throwable t) {
            log.error("Task {} threw an uncaught and unrecoverable exception", id, t);
            log.error("Task is being killed and will not recover until manually restarted");
            throw t;
        } finally {
            doClose();
        }
    }

    private void onShutdown() {
        synchronized (this) {
            triggerStop();

            // if we were cancelled, skip the status update since the task may have already been
            // started somewhere else
            if (!cancelled)
                statusListener.onShutdown(id);
        }
    }

    protected void onFailure(Throwable t) {
        synchronized (this) {
            triggerStop();

            // if we were cancelled, skip the status update since the task may have already been
            // started somewhere else
            if (!cancelled)
                statusListener.onFailure(id, t);
        }
    }

    protected synchronized void onPause() {
        statusListener.onPause(id);
    }

    protected synchronized void onResume() {
        statusListener.onResume(id);
    }

    @Override
    public void run() {
        try {
            doRun();
            onShutdown();
        } catch (Throwable t) {
            onFailure(t);

            if (t instanceof Error)
                throw (Error) t;
        } finally {
            shutdownLatch.countDown();
        }
    }

    public boolean shouldPause() {
        return this.targetState == TargetState.PAUSED;
    }

    /**
     * Await task resumption.
     * @return true if the task's target state is not paused, false if the task is shutdown before resumption
     * @throws InterruptedException
     */
    protected boolean awaitUnpause() throws InterruptedException {
        synchronized (this) {
            while (targetState == TargetState.PAUSED) {
                if (stopping)
                    return false;
                this.wait();
            }
            return true;
        }
    }

    public void transitionTo(TargetState state) {
        synchronized (this) {
            // ignore the state change if we are stopping
            if (stopping)
                return;

            this.targetState = state;
            this.notifyAll();
        }
    }

}
