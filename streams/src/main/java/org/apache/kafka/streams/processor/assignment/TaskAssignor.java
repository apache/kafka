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

import java.util.Collection;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupAssignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.TaskAssignmentException;

/**
 * A TaskAssignor is responsible for creating a TaskAssignment from a given
 * {@code ApplicationState}.
 * The implementation may also override the {@code onAssignmentComputed} callback for insight into
 * the result of the assignment result.
 */
public interface TaskAssignor extends Configurable {

    /**
     * NONE: no error detected
     * ACTIVE_TASK_ASSIGNED_MULTIPLE_TIMES: multiple KafkaStreams clients assigned with the same active task
     * ACTIVE_AND_STANDBY_TASK_ASSIGNED_TO_SAME_KAFKASTREAMS: active task and standby task assigned to the same KafkaStreams client
     * INVALID_STANDBY_TASK: stateless task assigned as a standby task
     * UNKNOWN_PROCESS_ID: unrecognized ProcessId not matching any of the participating consumers
     * UNKNOWN_TASK_ID: unrecognized TaskId not matching any of the tasks to be assigned
     */
    enum AssignmentError {
        NONE,
        ACTIVE_TASK_ASSIGNED_MULTIPLE_TIMES,
        ACTIVE_AND_STANDBY_TASK_ASSIGNED_TO_SAME_KAFKASTREAMS,
        INVALID_STANDBY_TASK,
        UNKNOWN_PROCESS_ID,
        UNKNOWN_TASK_ID
    }

    /**
     * @param applicationState the metadata for this Kafka Streams application
     *
     * @return the assignment of active and standby tasks to KafkaStreams clients
     *
     * @throws TaskAssignmentException If an error occurs during assignment, and you wish for the rebalance to be retried,
     *                                 you can throw this exception to keep the assignment unchanged and automatically
     *                                 schedule an immediate followup rebalance.
     */
    TaskAssignment assign(ApplicationState applicationState);

    /**
     * This callback can be used to observe the final assignment returned to the brokers and check for any errors that
     * were detected while processing the returned assignment. If any errors were found, the corresponding
     * will be returned and a StreamsException will be thrown after this callback returns. The StreamsException will
     * be thrown up to kill the StreamThread and can be handled as any other uncaught exception would if the application
     * has registered a {@link StreamsUncaughtExceptionHandler}.
     *
     * @param assignment    the final assignment returned to the kafka broker
     * @param subscription  the original subscription passed into the assignor
     * @param error         the corresponding error type if one was detected while processing the returned assignment,
     *                      or AssignmentError.NONE if the returned assignment was valid
     */
    default void onAssignmentComputed(GroupAssignment assignment, GroupSubscription subscription, AssignmentError error) {}

    /**
     * Wrapper class for the final assignment of active and standbys tasks to individual
     * KafkaStreams clients.
     */
    class TaskAssignment {
        private final Collection<KafkaStreamsAssignment> assignment;

        public TaskAssignment(final Collection<KafkaStreamsAssignment> assignment) {
            this.assignment = assignment;
        }

        /**
         * @return the assignment of tasks to kafka streams clients.
         */
        public Collection<KafkaStreamsAssignment> assignment() {
            return assignment;
        }
    }
}