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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.StandbyUpdateListener;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class TaskAndAction {

    public enum Action {
        ADD,
        REMOVE
    }

    private final Task task;
    private final TaskId taskId;
    private final Action action;
    private final CompletableFuture<StateUpdater.RemovedTaskResult> futureForRemove;
    private final StandbyUpdateListener.SuspendReason suspendReason;

    private TaskAndAction(final Task task,
                          final TaskId taskId,
                          final Action action,
                          final CompletableFuture<StateUpdater.RemovedTaskResult> futureForRemove,
                          final StandbyUpdateListener.SuspendReason suspendReason) {
        this.task = task;
        this.taskId = taskId;
        this.action = action;
        this.futureForRemove = futureForRemove;
        this.suspendReason = suspendReason;
    }

    public static TaskAndAction createAddTask(final Task task) {
        Objects.requireNonNull(task, "Task to add is null!");
        return new TaskAndAction(task, null, Action.ADD, null, null);
    }

    public static TaskAndAction createRemoveTask(final TaskId taskId,
                                                 final CompletableFuture<StateUpdater.RemovedTaskResult> future,
                                                 final StandbyUpdateListener.SuspendReason suspendReason) {
        Objects.requireNonNull(taskId, "Task ID of task to remove is null!");
        Objects.requireNonNull(future, "Future for task to remove is null!");
        Objects.requireNonNull(suspendReason, "Suspend reason for task to remove is null!");
        return new TaskAndAction(null, taskId, Action.REMOVE, future, suspendReason);
    }

    public Task task() {
        if (action != Action.ADD) {
            throw new IllegalStateException("Action type " + action + " cannot have a task!");
        }
        return task;
    }

    public TaskId taskId() {
        if (action != Action.REMOVE) {
            throw new IllegalStateException("Action type " + action + " cannot have a task ID!");
        }
        return taskId;
    }

    public CompletableFuture<StateUpdater.RemovedTaskResult> futureForRemove() {
        if (action != Action.REMOVE) {
            throw new IllegalStateException("Action type " + action + " cannot have a future with a single result!");
        }
        return futureForRemove;
    }

    public StandbyUpdateListener.SuspendReason suspendReason() {
        if (action != Action.REMOVE) {
            throw new IllegalStateException("Action type " + action + " cannot have a suspend reason!");
        }
        return suspendReason;
    }

    public Action action() {
        return action;
    }
}
