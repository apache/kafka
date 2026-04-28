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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface TasksRegistry {

    Map<TaskId, Set<TopicPartition>> drainPendingActiveTasksForTopologies(final Set<String> currentTopologies);

    Map<TaskId, Set<TopicPartition>> drainPendingStandbyTasksForTopologies(final Set<String> currentTopologies);

    void addPendingActiveTasksToCreate(final Map<TaskId, Set<TopicPartition>> pendingTasks);

    void addPendingStandbyTasksToCreate(final Map<TaskId, Set<TopicPartition>> pendingTasks);

    void clearPendingTasksToCreate();

    Set<Task> drainPendingTasksToInit();

    Set<StreamTask> drainPendingActiveTasksToInit();

    Set<StandbyTask> drainPendingStandbyTasksToInit();

    Set<Task> pendingTasksToInit();

    void addPendingTasksToInit(final Collection<? extends Task> tasks);

    boolean hasPendingTasksToInit();

    Set<Task> pendingTasksToClose();

    void addPendingTasksToClose(final Collection<? extends Task> tasks);

    boolean hasPendingTasksToClose();

    void addActiveTasks(final Collection<StreamTask> tasks);

    void addStandbyTasks(final Collection<StandbyTask> tasks);

    void addTask(final Task task);

    void addActiveTask(final StreamTask task);

    void addStandbyTask(final StandbyTask task);

    void addFailedTask(final Task task);

    void removeTask(final Task taskToRemove);

    void replaceStandbyWithActive(final StreamTask activeTask);

    boolean updateActiveTaskInputPartitions(final StreamTask task, final Set<TopicPartition> topicPartitions);

    void clear();

    StreamTask activeInitializedTasksForInputPartition(final TopicPartition partition);

    Task initializedTask(final TaskId taskId);

    Collection<Task> initializedTasks(final Collection<TaskId> taskIds);

    Collection<TaskId> activeInitializedTaskIds();

    Collection<StreamTask> activeInitializedTasks();

    Collection<StandbyTask> standbyInitializedTasks();

    Set<Task> allInitializedTasks();

    Set<Task> allNonFailedInitializedTasks();

    Map<TaskId, Task> allInitializedTasksPerId();

    Set<TaskId> allInitializedTaskIds();

    boolean containsInitialized(final TaskId taskId);
}
