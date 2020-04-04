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

import static org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo.UNKNOWN_OFFSET_SUM;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RankedClient implements Comparable<RankedClient> {
    private static final Logger log = LoggerFactory.getLogger(RankedClient.class);

    private final UUID clientId;
    private final long rank;

    RankedClient(final UUID clientId, final long rank) {
        this.clientId = clientId;
        this.rank = rank;
    }

    UUID clientId() {
        return clientId;
    }

    long rank() {
        return rank;
    }

    @Override
    public int compareTo(final RankedClient clientIdAndLag) {
        if (rank < clientIdAndLag.rank) {
            return -1;
        } else if (rank > clientIdAndLag.rank) {
            return 1;
        } else {
            return clientId.compareTo(clientIdAndLag.clientId);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RankedClient that = (RankedClient) o;
        return rank == that.rank && Objects.equals(clientId, that.clientId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, rank);
    }

    /**
     * Maps tasks to clients with caught-up states for the task.
     *
     * @param statefulTasksToRankedClients ranked clients map
     * @return map from tasks with caught-up clients to the list of client candidates
     */
    static Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients(final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedClients) {
        final Map<TaskId, SortedSet<UUID>> taskToCaughtUpClients = new HashMap<>();
        for (final SortedMap.Entry<TaskId, SortedSet<RankedClient>> taskToRankedClients : statefulTasksToRankedClients.entrySet()) {
            final SortedSet<RankedClient> rankedClients = taskToRankedClients.getValue();
            for (final RankedClient rankedClient : rankedClients) {
                if (rankedClient.rank() == Task.LATEST_OFFSET || rankedClient.rank() == 0) {
                    final TaskId taskId = taskToRankedClients.getKey();
                    taskToCaughtUpClients.computeIfAbsent(taskId, ignored -> new TreeSet<>()).add(rankedClient.clientId());
                } else {
                    break;
                }
            }
        }
        return taskToCaughtUpClients;
    }

    /**
     * Rankings are computed as follows, with lower being more caught up:
     *      Rank -1: active running task
     *      Rank 0: standby or restoring task whose overall lag is within the acceptableRecoveryLag bounds
     *      Rank 1: tasks whose lag is unknown, eg because it was not encoded in an older version subscription.
     *                 Since it may have been caught-up, we rank it higher than clients whom we know are not caught-up
     *                 to give it priority without classifying it as caught-up and risking violating high availability
     *      Rank 1+: all other tasks are ranked according to their actual total lag
     * @return Sorted set of all client candidates for each stateful task, ranked by their overall lag. Tasks are
     */
    static SortedMap<TaskId, SortedSet<RankedClient>> buildClientRankingsByTask(final Set<TaskId> statefulTasks,
                                                                                final Map<UUID, ClientState> clientStates,
                                                                                final long acceptableRecoveryLag) {
        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates = new TreeMap<>();

        for (final TaskId task : statefulTasks) {
            final SortedSet<RankedClient> rankedClientCandidates = new TreeSet<>();
            statefulTasksToRankedCandidates.put(task, rankedClientCandidates);

            for (final Map.Entry<UUID, ClientState> clientEntry : clientStates.entrySet()) {
                final UUID clientId = clientEntry.getKey();
                final long taskLag = clientEntry.getValue().lagFor(task);
                final long clientRank;
                if (taskLag == Task.LATEST_OFFSET) {
                    clientRank = Task.LATEST_OFFSET;
                } else if (taskLag == UNKNOWN_OFFSET_SUM) {
                    clientRank = 1L;
                } else if (taskLag <= acceptableRecoveryLag) {
                    clientRank = 0L;
                } else {
                    clientRank = taskLag;
                }
                rankedClientCandidates.add(new RankedClient(clientId, clientRank));
            }
        }
        log.trace("Computed statefulTasksToRankedCandidates map as {}", statefulTasksToRankedCandidates);

        return statefulTasksToRankedCandidates;
    }
}
