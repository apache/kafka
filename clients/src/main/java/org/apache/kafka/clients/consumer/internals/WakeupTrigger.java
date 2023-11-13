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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Ensures blocking APIs can be woken up by {@link Consumer#wakeup()}.
 */
class WakeupTrigger {

    private final static Object FAIL_NEXT_MARKER = new Object();
    private final AtomicReference<Object> activeTask = new AtomicReference<>(null);

    /**
     * Wakeup a pending task.
     *
     * <p/>
     *
     * There are three cases that can happen when this method is invoked:
     *
     * <ul>
     *     <li>
     *         If there is no <em>active</em> task from a previous call to {@code setActiveTask}, we set an
     *         internal indicator that the <em>next</em> attempt to start a long-running task (via a call to
     *         {@link #setActiveTask(CompletableFuture)}) will fail with a {@link WakeupException}.
     *     </li>
     *     <li>
     *         If there is an <em>active</em> task (i.e. there was a previous call to
     *         {@link #setActiveTask(CompletableFuture)} for a long-running task), fail it via
     *         {@link CompletableFuture#completeExceptionally(Throwable)} and then clear the <em>active</em> task.
     *     </li>
     *     <li>
     *         If there is already an pending wakeup from a previous call to {@link Consumer#wakeup()}, do nothing.
     *         We keep the internal state as is so that the future calls to {@link #setActiveTask(CompletableFuture)}
     *         will fail as expected.
     *     </li>
     * </ul>
     */
    void wakeup() {
        activeTask.getAndUpdate(existingTask -> {
            if (existingTask == null) {
                // If there isn't an existing task, return our marker, so that the subsequent call will
                // know wakeup was previously called.
                return FAIL_NEXT_MARKER;
            } else if (existingTask instanceof CompletableFuture<?>) {
                // If there is an existing "active" task, complete it with WakeupException.
                CompletableFuture<?> active = (CompletableFuture<?>) existingTask;
                active.completeExceptionally(new WakeupException());

                // We return a null here to effectively unset the "active" task.
                return null;
            } else {
                // This is the case where the existing task is the wakeup marker. So the user has apparently
                // called Consumer.wakeup() more than once.
                return existingTask;
            }
        });
    }

    /**
     * This method should be called before execution a blocking operation in the {@link Consumer}. This will
     * store an internal reference to the given <em>active</em> {@link CompletableFuture task} that can be
     * {@link CompletableFuture#completeExceptionally(Throwable) forcibly failed} if the user invokes the
     * {@link Consumer#wakeup()} call before or during its execution.
     *
     * <p/>
     *
     * There are three cases that can happen when this method is invoked:
     *
     * <ul>
     *     <li>
     *         If there is no <em>active</em> task from a previous call to {@code setActiveTask} <em>and</em> no
     *         previous calls to {@link #wakeup()}, set the given {@link CompletableFuture} as the
     *         <em>active</em> task.
     *     </li>
     *     <li>
     *         If there was a previous call to {@link #wakeup()}, the given {@link CompletableFuture task} will fail
     *         via {@link CompletableFuture#completeExceptionally(Throwable)} and the <em>active</em> task will be
     *         cleared.
     *     </li>
     *     <li>
     *         If there is already an <em>active</em> task from a previous call to {@code setActiveTask},
     *         a {@link KafkaException} will immediately be thrown.
     *     </li>
     * </ul>
     *
     * <p/>
     *
     * <em>Note</em>: the expected use of this method is to pair it with calls to {@link #clearActiveTask()}. That is,
     * callers should usually invoke this function at the beginning of a {@code try} clause to set the <em>active</em>
     * task and then invoke {@link #clearActiveTask()} in the {@code finally} clause to release it.
     *
     * @param task {@link CompletableFuture Task} to set as the <em>active</em> task if checks pass
     */
    void setActiveTask(final CompletableFuture<?> task) {
        Objects.requireNonNull(task, "task cannot be null");
        activeTask.getAndUpdate(existingTask -> {
            if (existingTask == null) {
                return task;
            } else if (existingTask == FAIL_NEXT_MARKER) {
                task.completeExceptionally(new WakeupException());
                return null;
            } else {
                // last active state is still active
                throw new KafkaException("Last active task is still active");
            }
        });
    }

    /**
     * This method should be called after execution of a blocking operation in the {@link Consumer}. This will
     * release the internal reference to the current <em>active</em> {@link CompletableFuture task}.
     *
     * <p/>
     *
     * <em>Note</em>: the expected use of this method is to pair it with calls to
     * {@link #setActiveTask(CompletableFuture)}. That is, callers should usually invoke the
     * {@link #setActiveTask(CompletableFuture)} method at the beginning of a {@code try} clause to set the
     * <em>active</em> task and then invoke this method in the {@code finally} clause to release it.
     */
    void clearActiveTask() {
        activeTask.getAndUpdate(existingTask -> {
            if (existingTask == null) {
                return null;
            } else if (existingTask instanceof CompletableFuture<?>) {
                return null;
            } else {
                return existingTask;
            }
        });
    }

    boolean hasPendingTask() {
        return activeTask.get() != null;
    }
}
