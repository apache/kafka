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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Ensures blocking APIs can be woken up by the consumer.wakeup().
 */
public class WakeupTrigger {
    private final AtomicReference<Wakeupable> pendingTask = new AtomicReference<>(null);

    /**
     * Wakeup a pending task.  If there isn't any pending task, return a WakeupFuture, so that the subsequent call
     * would know wakeup was previously called.
     * <p>
     * If there are active tasks, complete it with WakeupException, then unset pending task (return null here.
     * If the current task has already been woken-up, do nothing.
     */
    public void wakeup() {
        pendingTask.getAndUpdate(task -> {
            if (task == null) {
                return new WakeupFuture();
            } else if (task instanceof ActiveFuture) {
                ActiveFuture active = (ActiveFuture) task;
                active.future().completeExceptionally(new WakeupException());
                return null;
            } else {
                return task;
            }
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
                return new ActiveFuture(currentTask);
            } else if (task instanceof WakeupFuture) {
                currentTask.completeExceptionally(new WakeupException());
                return null;
            }
            // last active state is still active
            throw new KafkaException("Last active task is still active");
        });
        return currentTask;
    }

    public void clearActiveTask() {
        pendingTask.getAndUpdate(task -> {
            if (task == null) {
                return null;
            } else if (task instanceof ActiveFuture) {
                return null;
            }
            return task;
        });
    }

    Wakeupable getPendingTask() {
        return pendingTask.get();
    }

    interface Wakeupable { }

    static class ActiveFuture implements Wakeupable {
        private final CompletableFuture<?> future;

        public ActiveFuture(final CompletableFuture<?> future) {
            this.future = future;
        }

        public CompletableFuture<?> future() {
            return future;
        }
    }

    static class WakeupFuture implements Wakeupable { }
}
