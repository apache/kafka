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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.internals.StreamThread.State;
import org.apache.kafka.streams.processor.internals.assignment.AssignorError;
import org.slf4j.Logger;

import java.util.Collection;

public class StreamsRebalanceListener implements ConsumerRebalanceListener {

    private final Time time;
    private final TaskManager taskManager;
    private final StreamThread streamThread;
    private final Logger log;
    private final AtomicInteger assignmentErrorCode;

    StreamsRebalanceListener(final Time time,
                             final TaskManager taskManager,
                             final StreamThread streamThread,
                             final Logger log,
                             final AtomicInteger assignmentErrorCode) {
        this.time = time;
        this.taskManager = taskManager;
        this.streamThread = streamThread;
        this.log = log;
        this.assignmentErrorCode = assignmentErrorCode;
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        // NB: all task management is already handled by:
        // org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor.onAssignment
        if (assignmentErrorCode.get() == AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.code()) {
            log.error("Received error code {}", AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA);
            taskManager.handleRebalanceComplete();
            throw new MissingSourceTopicException("One or more source topics were missing during rebalance");
        } else if (assignmentErrorCode.get() == AssignorError.VERSION_PROBING.code()) {
            log.info("Received version probing code {}", AssignorError.VERSION_PROBING);
        }  else if (assignmentErrorCode.get() == AssignorError.ASSIGNMENT_ERROR.code()) {
            log.error("Received error code {}", AssignorError.ASSIGNMENT_ERROR);
            taskManager.handleRebalanceComplete();
            throw new TaskAssignmentException("Hit an unexpected exception during task assignment phase of rebalance");
        } else if (assignmentErrorCode.get() == AssignorError.SHUTDOWN_REQUESTED.code()) {
            log.error("A Kafka Streams client in this Kafka Streams application is requesting to shutdown the application");
            taskManager.handleRebalanceComplete();
            streamThread.shutdownToError();
            return;
        } else if (assignmentErrorCode.get() != AssignorError.NONE.code()) {
            log.error("Received unknown error code {}", assignmentErrorCode.get());
            throw new TaskAssignmentException("Hit an unrecognized exception during rebalance");
        }

        streamThread.setState(State.PARTITIONS_ASSIGNED);
        taskManager.handleRebalanceComplete();
    }

    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        log.debug("Current state {}: revoked partitions {} because of consumer rebalance.\n" +
                      "\tcurrently assigned active tasks: {}\n" +
                      "\tcurrently assigned standby tasks: {}\n",
                  streamThread.state(),
                  partitions,
                  taskManager.activeTaskIds(),
                  taskManager.standbyTaskIds());

        if (streamThread.setState(State.PARTITIONS_REVOKED) != null && !partitions.isEmpty()) {
            final long start = time.milliseconds();
            try {
                taskManager.handleRevocation(partitions);
            } finally {
                log.info("partition revocation took {} ms.", time.milliseconds() - start);
            }
        }
    }

    @Override
    public void onPartitionsLost(final Collection<TopicPartition> partitions) {
        log.info("at state {}: partitions {} lost due to missed rebalance.\n" +
                     "\tlost active tasks: {}\n" +
                     "\tlost assigned standby tasks: {}\n",
                 streamThread.state(),
                 partitions,
                 taskManager.activeTaskIds(),
                 taskManager.standbyTaskIds());

        final long start = time.milliseconds();
        try {
            // close all active tasks as lost but don't try to commit offsets as we no longer own them
            taskManager.handleLostAll();
        } finally {
            log.info("partitions lost took {} ms.", time.milliseconds() - start);
        }
    }
}
