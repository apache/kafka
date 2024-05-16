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

import static java.util.Collections.unmodifiableSet;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.streams.processor.TaskId;

/**
 * A simple container class for the assignor to return the desired placement of active and standby tasks on
 * KafkaStreams clients.
 */
public class KafkaStreamsAssignment {

    private final ProcessId processId;
    private final Set<AssignedTask> assignment;
    private final Optional<Instant> followupRebalanceDeadline;

    private KafkaStreamsAssignment(final ProcessId processId,
                                   final Set<AssignedTask> assignment,
                                   final Optional<Instant> followupRebalanceDeadline) {
        this.processId = processId;
        this.assignment = unmodifiableSet(assignment);
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
     * @return a set of assigned tasks that are part of this {@code KafkaStreamsAssignment}
     */
    public Set<AssignedTask> assignment() {
        return assignment;
    }

    /**
     * @return the followup rebalance deadline in epoch time, after which this KafkaStreams
     * client will trigger a new rebalance.
     */
    public Optional<Instant> followupRebalanceDeadline() {
        return followupRebalanceDeadline;
    }

    public static KafkaStreamsAssignment of(final ProcessId processId, final Set<AssignedTask> assignment) {
        return new KafkaStreamsAssignment(processId, assignment, Optional.empty());
    }

    public KafkaStreamsAssignment withFollowupRebalance(final Instant rebalanceDeadline) {
        return new KafkaStreamsAssignment(this.processId(), this.assignment(), Optional.of(rebalanceDeadline));
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
    }
}
