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

import java.util.UUID;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import org.apache.kafka.streams.processor.TaskId;

public class DefaultBalancedAssignor implements BalancedAssignor {

    @Override
    public Map<UUID, List<TaskId>> assign(final SortedSet<UUID> clients,
                                          final SortedSet<TaskId> tasks,
                                          final Map<UUID, Integer> clientsToNumberOfStreamThreads,
                                          final int balanceFactor) {
        final Map<UUID, List<TaskId>> assignment = new HashMap<>();
        clients.forEach(client -> assignment.put(client, new ArrayList<>()));
        distributeTasksEvenlyOverClients(assignment, clients, tasks);
        balanceTasksOverStreamThreads(assignment, clients, clientsToNumberOfStreamThreads, balanceFactor);
        return assignment;
    }

    private void distributeTasksEvenlyOverClients(final Map<UUID, List<TaskId>> assignment,
                                                  final SortedSet<UUID> clients,
                                                  final SortedSet<TaskId> tasks) {
        final LinkedList<TaskId> tasksToAssign = new LinkedList<>(tasks);
        while (!tasksToAssign.isEmpty()) {
            for (final UUID client : clients) {
                final TaskId task = tasksToAssign.poll();

                if (task == null) {
                    break;
                }
                assignment.get(client).add(task);
            }
        }
    }

    private void balanceTasksOverStreamThreads(final Map<UUID, List<TaskId>> assignment,
                                               final SortedSet<UUID> clients,
                                               final Map<UUID, Integer> clientsToNumberOfStreamThreads,
                                               final int balanceFactor) {
        boolean stop = false;
        while (!stop) {
            stop = true;
            for (final UUID sourceClient : clients) {
                final List<TaskId> sourceTasks = assignment.get(sourceClient);
                for (final UUID destinationClient : clients) {
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
