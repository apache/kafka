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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Ensures blocking APIs can be woken up by the consumer.wakeup().
 */
public class WakeupTrigger {

    private final Logger log;
    private final AtomicReference<Wakeupable> pendingTask = new AtomicReference<>(null);

    public WakeupTrigger(final LogContext logContext) {
        this.log = logContext.logger(getClass());
    }

    /**
     * Wakeup a pending task.  If there isn't any pending task, return a WakeupFuture, so that the subsequent call
     * would know wakeup was previously called.
     * <p>
     * If there are active tasks, complete it with WakeupException, then unset pending task (return null here.
     * If the current task has already been woken-up, do nothing.
     */
    public void wakeup() {
        pendingTask.getAndUpdate(task -> {
            final Wakeupable ret;

            if (task == null) {
                log.debug("KIRK_DEBUG - wakeup - a");
                ret = new WakeupFuture();
            } else if (task instanceof ActiveFuture) {
                ActiveFuture active = (ActiveFuture) task;
                log.debug("KIRK_DEBUG - wakeup - b - before completeExceptionally, active: {}", active);
                active.future().completeExceptionally(new WakeupException());
                log.debug("KIRK_DEBUG - wakeup - c - after completeExceptionally, active: {}", active);
                ret = null;
            } else if (task instanceof FetchAction) {
                FetchAction fetchAction = (FetchAction) task;
                log.debug("KIRK_DEBUG - wakeup - d - before fetch wakeup, fetchAction: {}", fetchAction);
                fetchAction.fetchBuffer().wakeup();
                log.debug("KIRK_DEBUG - wakeup - e - after fetch wakeup, fetchAction: {}", fetchAction);
                ret = new WakeupFuture();
            } else {
                log.debug("KIRK_DEBUG - wakeup - f");
                ret = task;
            }

            log.debug("KIRK_DEBUG - wakeup - g - ret: {}", ret);
            return ret;
        });
    }

    /**
     *     If there is no pending task, set the pending task active.
     *     If wakeup was called before setting an active task, the current task will complete exceptionally with
     *     WakeupException right
     *     away.
     *     if there is an active task, throw exception.
     * @param currentTask
     * @param <T>
     * @return
     */
    public <T> CompletableFuture<T> setActiveTask(final CompletableFuture<T> currentTask) {
        Objects.requireNonNull(currentTask, "currentTask cannot be null");
        pendingTask.getAndUpdate(task -> {
            if (task == null) {
                ActiveFuture activeFuture = new ActiveFuture(currentTask);
                log.debug("KIRK_DEBUG - setActiveTask - activeFuture: {}", activeFuture);
                return activeFuture;
            } else if (task instanceof WakeupFuture) {
                log.debug("KIRK_DEBUG - setActiveTask - before completeExceptionally, currentTask: {}", currentTask);
                currentTask.completeExceptionally(new WakeupException());
                log.debug("KIRK_DEBUG - setActiveTask - after completeExceptionally, currentTask: {}", currentTask);
                return null;
            } else if (task instanceof DisabledWakeups) {
                log.debug("KIRK_DEBUG - setActiveTask - task: {}", task);
                return task;
            }

            log.debug("KIRK_DEBUG - setActiveTask - last task ({}) is still active!?", task);
            // last active state is still active
            throw new KafkaException("Last active task is still active");
        });
        return currentTask;
    }

    public void setFetchAction(final FetchBuffer fetchBuffer) {
        final AtomicBoolean throwWakeupException = new AtomicBoolean(false);
        pendingTask.getAndUpdate(task -> {
            if (task == null) {
                FetchAction fetchAction = new FetchAction(fetchBuffer);
                log.debug("KIRK_DEBUG - setFetchAction - fetchAction: {}", fetchAction);
                return fetchAction;
            } else if (task instanceof WakeupFuture) {
                log.debug("KIRK_DEBUG - setFetchAction - throwWakeupException before: {}", throwWakeupException);
                throwWakeupException.set(true);
                log.debug("KIRK_DEBUG - setFetchAction - throwWakeupException after: {}", throwWakeupException);
                return null;
            } else if (task instanceof DisabledWakeups) {
                log.debug("KIRK_DEBUG - setFetchAction - task: {}", task);
                return task;
            }

            log.debug("KIRK_DEBUG - setFetchAction - last task ({}) is still active!?", task);
            // last active state is still active
            throw new IllegalStateException("Last active task is still active");
        });
        if (throwWakeupException.get()) {
            throw new WakeupException();
        }
    }

    public void disableWakeups() {
        log.debug("KIRK_DEBUG - disableWakeups - before: {}", pendingTask.get());
        Wakeupable previousTask = pendingTask.getAndSet(new DisabledWakeups());
        log.debug("KIRK_DEBUG - disableWakeups - after: {}, previousTask: {}", pendingTask.get(), previousTask);
    }

    public void clearTask() {
        pendingTask.getAndUpdate(task -> {
            if (task == null) {
                log.debug("KIRK_DEBUG - clearTask - a - task: {}", task);
                return null;
            } else if (task instanceof ActiveFuture || task instanceof FetchAction) {
                log.debug("KIRK_DEBUG - clearTask - b - task: {}", task);
                return null;
            }

            log.debug("KIRK_DEBUG - clearTask - c - task: {}", task);
            return task;
        });
    }

    public void maybeTriggerWakeup() {
        final AtomicBoolean throwWakeupException = new AtomicBoolean(false);
        pendingTask.getAndUpdate(task -> {
            if (task == null) {
                log.debug("KIRK_DEBUG - maybeTriggerWakeup - a - task: {}", task);
                return null;
            } else if (task instanceof WakeupFuture) {
                log.debug("KIRK_DEBUG - maybeTriggerWakeup - b - task: {}", task);
                throwWakeupException.set(true);
                return null;
            } else {
                log.debug("KIRK_DEBUG - maybeTriggerWakeup - c - task: {}", task);
                return task;
            }
        });
        if (throwWakeupException.get()) {
            throw new WakeupException();
        }
    }

    Wakeupable getPendingTask() {
        return pendingTask.get();
    }

    interface Wakeupable { }

    // Set to block wakeups from happening and pending actions to be registered.
    static class DisabledWakeups implements Wakeupable {
        @Override
        public String toString() {
            return "DisabledWakeups{}";
        }
    }

    static class ActiveFuture implements Wakeupable {
        private final CompletableFuture<?> future;

        public ActiveFuture(final CompletableFuture<?> future) {
            this.future = future;
        }

        public CompletableFuture<?> future() {
            return future;
        }

        @Override
        public String toString() {
            return "ActiveFuture{" +
                    "future=" + future +
                    '}';
        }
    }

    static class WakeupFuture implements Wakeupable {

        @Override
        public String toString() {
            return "WakeupFuture{}";
        }
    }

    static class FetchAction implements Wakeupable {

        private final FetchBuffer fetchBuffer;

        public FetchAction(FetchBuffer fetchBuffer) {
            this.fetchBuffer = fetchBuffer;
        }

        public FetchBuffer fetchBuffer() {
            return fetchBuffer;
        }

        @Override
        public String toString() {
            return "FetchAction{" +
                    "fetchBuffer=" + fetchBuffer +
                    '}';
        }
    }
}
