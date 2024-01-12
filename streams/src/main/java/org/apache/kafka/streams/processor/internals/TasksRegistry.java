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

    Set<TopicPartition> removePendingTaskToRecycle(final TaskId taskId);

    boolean hasPendingTasksToRecycle();

    void addPendingTaskToRecycle(final TaskId taskId, final Set<TopicPartition> inputPartitions);

    Set<TopicPartition> removePendingTaskToCloseReviveAndUpdateInputPartitions(final TaskId taskId);

    void addPendingTaskToCloseReviveAndUpdateInputPartitions(final TaskId taskId, final Set<TopicPartition> inputPartitions);

    Set<TopicPartition> removePendingTaskToUpdateInputPartitions(final TaskId taskId);

    void addPendingTaskToUpdateInputPartitions(final TaskId taskId, final Set<TopicPartition> inputPartitions);

    boolean removePendingTaskToAddBack(final TaskId taskId);

    void addPendingTaskToAddBack(final TaskId taskId);

    boolean removePendingTaskToCloseClean(final TaskId taskId);

    void addPendingTaskToCloseClean(final TaskId taskId);

    Set<Task> drainPendingTasksToInit();

    void addPendingTasksToInit(final Collection<Task> tasks);

    boolean hasPendingTasksToInit();

    boolean removePendingActiveTaskToSuspend(final TaskId taskId);

    void addPendingActiveTaskToSuspend(final TaskId taskId);

    void addActiveTasks(final Collection<Task> tasks);

    void addStandbyTasks(final Collection<Task> tasks);

    void addTask(final Task task);

    void removeTask(final Task taskToRemove);

    void replaceActiveWithStandby(final StandbyTask standbyTask);

    void replaceStandbyWithActive(final StreamTask activeTask);

    boolean updateActiveTaskInputPartitions(final Task task, final Set<TopicPartition> topicPartitions);

    void clear();

    Task activeTasksForInputPartition(final TopicPartition partition);

    Task task(final TaskId taskId);

    Collection<Task> tasks(final Collection<TaskId> taskIds);

    Collection<TaskId> activeTaskIds();

    Collection<Task> activeTasks();

    Set<Task> allTasks();

    Map<TaskId, Task> allTasksPerId();

    Set<TaskId> allTaskIds();

    boolean contains(final TaskId taskId);
}
