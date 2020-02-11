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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.internals.StreamThread.State;
import org.apache.kafka.streams.processor.internals.assignment.AssignorError;
import org.slf4j.Logger;

import java.util.Collection;

public class StreamsRebalanceListener implements ConsumerRebalanceListener {

    private final Time time;
    private final TaskManager taskManager;
    private final StreamThread streamThread;
    private final Logger log;

    StreamsRebalanceListener(final Time time,
                             final TaskManager taskManager,
                             final StreamThread streamThread,
                             final Logger log) {
        this.time = time;
        this.taskManager = taskManager;
        this.streamThread = streamThread;
        this.log = log;
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        // NB: all task management is already handled by:
        // org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor.onAssignment
        if (streamThread.getAssignmentErrorCode() == AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.code()) {
            log.error("Received error code {} - shutdown", streamThread.getAssignmentErrorCode());
            streamThread.shutdown();
        } else {
            taskManager.handleRebalanceComplete();

            streamThread.setState(State.PARTITIONS_ASSIGNED);
        }
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
            } catch (final Throwable t) {
                log.error(
                    "Error caught during partition revocation, " +
                        "will abort the current process and re-throw at the end of rebalance: ",
                    t
                );
                streamThread.setRebalanceException(t);
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
        } catch (final Throwable t) {
            log.error(
                "Error caught during partitions lost, " +
                    "will abort the current process and re-throw at the end of rebalance: ",
                t
            );
            streamThread.setRebalanceException(t);
        } finally {
            log.info("partitions lost took {} ms.", time.milliseconds() - start);
        }
    }

}
