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
package org.apache.kafka.streams.processor.assignment;

import org.apache.kafka.streams.processor.TaskId;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;

/**
 * A simple container class for the assignor to return the desired placement of active and standby tasks on
 * KafkaStreams clients.
 */
public class KafkaStreamsAssignment {

    private final ProcessId processId;
    private final Map<TaskId, AssignedTask> tasks;
    private final Optional<Instant> followupRebalanceDeadline;

    /**
     * Construct an instance of KafkaStreamsAssignment with this processId and the given set of
     * assigned tasks. If you want this KafkaStreams client to request a followup rebalance, you
     * can set the followupRebalanceDeadline via the {@link #withFollowupRebalance(Instant)} API.
     *
     * @param processId the processId for the KafkaStreams client that should receive this assignment
     * @param assignment the set of tasks to be assigned to this KafkaStreams client
     *
     * @return a new KafkaStreamsAssignment object with the given processId and assignment
     */
    public static KafkaStreamsAssignment of(final ProcessId processId, final Set<AssignedTask> assignment) {
        final Map<TaskId, AssignedTask> tasks = assignment.stream().collect(Collectors.toMap(AssignedTask::id, Function.identity()));
        return new KafkaStreamsAssignment(processId, tasks, Optional.empty());
    }

    /**
     * This API can be used to request that a followup rebalance be triggered by the KafkaStreams client
     * receiving this assignment. The followup rebalance will be initiated after the provided deadline
     * has passed, although it will always wait until it has finished the current rebalance before
     * triggering a new one. This request will last until the new rebalance, and will be erased if a
     * new rebalance begins before the scheduled followup rebalance deadline has elapsed. The next
     * assignment must request the followup rebalance again if it still wants to schedule one for
     * the given instant, otherwise no additional rebalance will be triggered after that.
     *
     * @param rebalanceDeadline the instant after which this KafkaStreams client will trigger a followup rebalance
     *
     * @return a new KafkaStreamsAssignment object with the same processId and assignment but with the given rebalanceDeadline
     */
    public KafkaStreamsAssignment withFollowupRebalance(final Instant rebalanceDeadline) {
        return new KafkaStreamsAssignment(this.processId(), this.tasks(), Optional.of(rebalanceDeadline));
    }

    private KafkaStreamsAssignment(final ProcessId processId,
                                   final Map<TaskId, AssignedTask> tasks,
                                   final Optional<Instant> followupRebalanceDeadline) {
        this.processId = processId;
        this.tasks = tasks;
        this.followupRebalanceDeadline = followupRebalanceDeadline;
    }

    /**
     *
     * @return the {@code ProcessID} associated with this {@code KafkaStreamsAssignment}
     */
    public ProcessId processId() {
        return processId;
    }

    /**
     *
     * @return a read-only set of assigned tasks that are part of this {@code KafkaStreamsAssignment}
     */
    public Map<TaskId, AssignedTask> tasks() {
        return unmodifiableMap(tasks);
    }

    public void assignTask(final AssignedTask newTask) {
        tasks.put(newTask.id(), newTask);
    }

    public void removeTask(final AssignedTask removedTask) {
        tasks.remove(removedTask.id());
    }

    /**
     * @return the followup rebalance deadline in epoch time, after which this KafkaStreams
     * client will trigger a new rebalance.
     */
    public Optional<Instant> followupRebalanceDeadline() {
        return followupRebalanceDeadline;
    }

    @Override
    public String toString() {
        return String.format(
            "KafkaStreamsAssignment{%s, %s, %s}",
            processId,
            Arrays.toString(tasks.values().toArray(new AssignedTask[0])),
            followupRebalanceDeadline
        );
    }

    public static class AssignedTask {
        private final TaskId id;
        private final Type taskType;

        public AssignedTask(final TaskId id, final Type taskType) {
            this.id = id;
            this.taskType = taskType;
        }

        public enum Type {
            ACTIVE,
            STANDBY
        }

        /**
         *
         * @return the id of the {@code AssignedTask}
         */
        public TaskId id() {
            return id;
        }

        /**
         *
         * @return the type of the {@code AssignedTask}
         */
        public Type type() {
            return taskType;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = prime + this.id.hashCode();
            result = prime * result + this.type().hashCode();
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            final AssignedTask other = (AssignedTask) obj;
            return this.id.equals(other.id()) && this.taskType == other.taskType;
        }

        @Override
        public String toString() {
            return String.format("AssignedTask{%s, %s}", taskType, id);
        }
    }
}
