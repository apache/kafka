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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

public class DefaultBalancedAssignor<ID extends Comparable<? super ID>> implements BalancedAssignor<ID> {

    @Override
    public Map<ID, List<TaskId>> assign(final SortedSet<ID> clients,
                                        final SortedSet<TaskId> tasks,
                                        final Map<ID, Integer> clientsToNumberOfStreamThreads,
                                        final int balanceFactor) {
        final Map<ID, List<TaskId>> assignment = new HashMap<>();
        clients.forEach(client -> assignment.put(client, new ArrayList<>()));
        distributeTasksEvenlyOverClients(assignment, clients, tasks);
        balanceTasksOverStreamThreads(assignment, clients, clientsToNumberOfStreamThreads, balanceFactor);
        return assignment;
    }

    private void distributeTasksEvenlyOverClients(final Map<ID, List<TaskId>> assignment,
                                                  final SortedSet<ID> clients,
                                                  final SortedSet<TaskId> tasks) {
        final LinkedList<TaskId> tasksToAssign = new LinkedList<>(tasks);
        while (!tasksToAssign.isEmpty()) {
            for (final ID client : clients) {
                final TaskId task = tasksToAssign.poll();

                if (task == null) {
                    break;
                }
                assignment.get(client).add(task);
            }
        }
    }

    private void balanceTasksOverStreamThreads(final Map<ID, List<TaskId>> assignment,
                                               final SortedSet<ID> clients,
                                               final Map<ID, Integer> clientsToNumberOfStreamThreads,
                                               final int balanceFactor) {
        boolean stop = false;
        while (!stop) {
            stop = true;
            for (final ID sourceClient : clients) {
                final List<TaskId> sourceTasks = assignment.get(sourceClient);
                for (final ID destinationClient : clients) {
                    if (sourceClient.equals(destinationClient)) {
                        continue;
                    }
                    final List<TaskId> destinationTasks = assignment.get(destinationClient);
                    final int assignedTasksPerStreamThreadAtDestination =
                        destinationTasks.size() / clientsToNumberOfStreamThreads.get(destinationClient);
                    final int assignedTasksPerStreamThreadAtSource =
                        sourceTasks.size() / clientsToNumberOfStreamThreads.get(sourceClient);
                    if (assignedTasksPerStreamThreadAtSource - assignedTasksPerStreamThreadAtDestination > balanceFactor) {
                        final Iterator<TaskId> sourceIterator = sourceTasks.iterator();
                        final TaskId taskToMove = sourceIterator.next();
                        sourceIterator.remove();
                        destinationTasks.add(taskToMove);
                        stop = false;
                    }
                }
            }
        }
    }
}
