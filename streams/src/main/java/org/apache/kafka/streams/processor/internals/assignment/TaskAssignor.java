/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class TaskAssignor<C, T extends Comparable<T>> {

    private static final Logger log = LoggerFactory.getLogger(TaskAssignor.class);

    public static <C, T extends Comparable<T>> Map<C, ClientState<T>> assign(Map<C, ClientState<T>> states, Set<T> tasks, int numStandbyReplicas, String streamThreadId) {
        long seed = 0L;
        for (C client : states.keySet()) {
            seed += client.hashCode();
        }

        TaskAssignor<C, T> assignor = new TaskAssignor<>(states, tasks, seed);
        log.info("stream-thread [{}] Assigning tasks to clients: {}, prevAssignmentBalanced: {}, " +
            "prevClientsUnchanged: {}, tasks: {}, replicas: {}",
            streamThreadId, states, assignor.prevAssignmentBalanced, assignor.prevClientsUnchanged,
            tasks, numStandbyReplicas);

        assignor.assignTasks();
        if (numStandbyReplicas > 0)
            assignor.assignStandbyTasks(numStandbyReplicas);

        log.info("stream-thread [{}] Assigned with: {}", streamThreadId, assignor.states);
        return assignor.states;
    }

    private final Random rand;
    private final Map<C, ClientState<T>> states;
    private final Set<TaskPair<T>> taskPairs;
    private final int maxNumTaskPairs;
    private final ArrayList<T> tasks;
    private boolean prevAssignmentBalanced = true;
    private boolean prevClientsUnchanged = true;

    private TaskAssignor(Map<C, ClientState<T>> states, Set<T> tasks, long randomSeed) {
        this.rand = new Random(randomSeed);
        this.states = new HashMap<>();
        int avgNumTasks = tasks.size() / states.size();
        Set<T> existingTasks = new HashSet<>();
        for (Map.Entry<C, ClientState<T>> entry : states.entrySet()) {
            this.states.put(entry.getKey(), entry.getValue().copy());
            Set<T> oldTasks = entry.getValue().prevAssignedTasks;
            // make sure the previous assignment is balanced
            prevAssignmentBalanced = prevAssignmentBalanced &&
                oldTasks.size() < 2 * avgNumTasks && oldTasks.size() > avgNumTasks / 2;
            for (T task : oldTasks) {
                // Make sure there is no duplicates
                prevClientsUnchanged = prevClientsUnchanged && !existingTasks.contains(task);
            }
            existingTasks.addAll(oldTasks);
        }
        // Make sure the existing assignment didn't miss out any task
        prevClientsUnchanged = prevClientsUnchanged && existingTasks.equals(tasks);

        this.tasks = new ArrayList<>(tasks);

        int numTasks = tasks.size();
        this.maxNumTaskPairs = numTasks * (numTasks - 1) / 2;
        this.taskPairs = new HashSet<>(this.maxNumTaskPairs);
    }

    public void assignTasks() {
        assignTasks(true);
    }

    public void assignStandbyTasks(int numStandbyReplicas) {
        int numReplicas = Math.min(numStandbyReplicas, states.size() - 1);
        for (int i = 0; i < numReplicas; i++) {
            assignTasks(false);
        }
    }

    private void assignTasks(boolean active) {
        Collections.shuffle(this.tasks, rand);

        for (T task : tasks) {
            ClientState<T> state = findClientFor(task);

            if (state != null) {
                state.assign(task, active);
            } else {
                TaskAssignmentException ex = new TaskAssignmentException("failed to find an assignable client");
                log.error(ex.getMessage(), ex);
                throw ex;
            }
        }
    }

    private ClientState<T> findClientFor(T task) {
        boolean checkTaskPairs = taskPairs.size() < maxNumTaskPairs;

        ClientState<T> state = findClientByAdditionCost(task, checkTaskPairs);

        if (state == null && checkTaskPairs)
            state = findClientByAdditionCost(task, false);

        if (state != null)
            addTaskPairs(task, state);

        return state;
    }

    private ClientState<T> findClientByAdditionCost(T task, boolean checkTaskPairs) {
        ClientState<T> candidate = null;
        double candidateAdditionCost = 0d;

        for (ClientState<T> state : states.values()) {
            if (prevAssignmentBalanced && prevClientsUnchanged &&
                state.prevAssignedTasks.contains(task)) {
                return state;
            }
            if (!state.assignedTasks.contains(task)) {
                // if checkTaskPairs flag is on, skip this client if this task doesn't introduce a new task combination
                if (checkTaskPairs && !state.assignedTasks.isEmpty() && !hasNewTaskPair(task, state))
                    continue;

                double additionCost = computeAdditionCost(task, state);
                if (candidate == null ||
                        (additionCost < candidateAdditionCost ||
                            (additionCost == candidateAdditionCost && state.cost < candidate.cost))) {
                    candidate = state;
                    candidateAdditionCost = additionCost;
                }
            }
        }

        return candidate;
    }

    private void addTaskPairs(T task, ClientState<T> state) {
        for (T other : state.assignedTasks) {
            taskPairs.add(pair(task, other));
        }
    }

    private boolean hasNewTaskPair(T task, ClientState<T> state) {
        for (T other : state.assignedTasks) {
            if (!taskPairs.contains(pair(task, other)))
                return true;
        }
        return false;
    }

    private double computeAdditionCost(T task, ClientState<T> state) {
        double cost = Math.floor((double) state.assignedTasks.size() / state.capacity);

        if (state.prevAssignedTasks.contains(task)) {
            if (state.prevActiveTasks.contains(task)) {
                cost += ClientState.COST_ACTIVE;
            } else {
                cost += ClientState.COST_STANDBY;
            }
        } else {
            cost += ClientState.COST_LOAD;
        }

        return cost;
    }

    private TaskPair<T> pair(T task1, T task2) {
        if (task1.compareTo(task2) < 0) {
            return new TaskPair<>(task1, task2);
        } else {
            return new TaskPair<>(task2, task1);
        }
    }

    private static class TaskPair<T> {
        public final T task1;
        public final T task2;

        public TaskPair(T task1, T task2) {
            this.task1 = task1;
            this.task2 = task2;
        }

        @Override
        public int hashCode() {
            return task1.hashCode() ^ task2.hashCode();
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean equals(Object o) {
            if (o instanceof TaskPair) {
                TaskPair<T> other = (TaskPair<T>) o;
                return this.task1.equals(other.task1) && this.task2.equals(other.task2);
            }
            return false;
        }
    }

}
