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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StreamThread.State;
import org.apache.kafka.streams.processor.internals.assignment.AssignorError;
import org.slf4j.Logger;

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
    public void onPartitionsAssigned(final Collection<TopicPartition> assignedPartitions) {
        log.debug("Current state {}: assigned partitions {} at the end of consumer rebalance.\n" +
                "\tpreviously assigned active tasks: {}\n" +
                "\tpreviously assigned standby tasks: {}\n",
            streamThread.state(),
            assignedPartitions,
            taskManager.previousActiveTaskIds(),
            taskManager.previousStandbyTaskIds());

        if (streamThread.getAssignmentErrorCode() == AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.code()) {
            log.error("Received error code {} - shutdown", streamThread.getAssignmentErrorCode());
            streamThread.shutdown();
            return;
        }

        final long start = time.milliseconds();
        List<TopicPartition> revokedStandbyPartitions = null;

        try {
            if (streamThread.setState(State.PARTITIONS_ASSIGNED) == null) {
                log.debug(
                    "Skipping task creation in rebalance because we are already in {} state.",
                    streamThread.state());
            } else {
                // Close non-reassigned tasks before initializing new ones as we may have suspended active
                // tasks that become standbys or vice versa
                revokedStandbyPartitions = taskManager.closeRevokedStandbyTasks();
                taskManager.closeRevokedSuspendedTasks();
                taskManager.createTasks(assignedPartitions);
            }
        } catch (final Throwable t) {
            log.error(
                "Error caught during partition assignment, " +
                    "will abort the current process and re-throw at the end of rebalance", t);
            streamThread.setRebalanceException(t);
        } finally {
            if (revokedStandbyPartitions != null) {
                streamThread.clearStandbyRecords(revokedStandbyPartitions);
            }
            log.info("partition assignment took {} ms.\n" +
                    "\tcurrently assigned active tasks: {}\n" +
                    "\tcurrently assigned standby tasks: {}\n" +
                    "\trevoked active tasks: {}\n" +
                    "\trevoked standby tasks: {}\n",
                time.milliseconds() - start,
                taskManager.activeTaskIds(),
                taskManager.standbyTaskIds(),
                taskManager.revokedActiveTaskIds(),
                taskManager.revokedStandbyTaskIds());
        }
    }

    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> revokedPartitions) {
        log.debug("Current state {}: revoked partitions {} because of consumer rebalance.\n" +
                "\tcurrently assigned active tasks: {}\n" +
                "\tcurrently assigned standby tasks: {}\n",
            streamThread.state(),
            revokedPartitions,
            taskManager.activeTaskIds(),
            taskManager.standbyTaskIds());

        Set<TaskId> suspendedTasks = new HashSet<>();
        if (streamThread.setState(State.PARTITIONS_REVOKED) != null && !revokedPartitions.isEmpty()) {
            final long start = time.milliseconds();
            try {
                // suspend only the active tasks, reassigned standby tasks will be closed in onPartitionsAssigned
                suspendedTasks = taskManager.suspendActiveTasksAndState(revokedPartitions);
            } catch (final Throwable t) {
                log.error(
                    "Error caught during partition revocation, " +
                        "will abort the current process and re-throw at the end of rebalance: ",
                    t
                );
                streamThread.setRebalanceException(t);
            } finally {
                log.info("partition revocation took {} ms.\n" +
                        "\tcurrent suspended active tasks: {}\n",
                    time.milliseconds() - start,
                    suspendedTasks);
            }
        }
    }

    @Override
    public void onPartitionsLost(final Collection<TopicPartition> lostPartitions) {
        log.info("at state {}: partitions {} lost due to missed rebalance.\n" +
                "\tlost active tasks: {}\n" +
                "\tlost assigned standby tasks: {}\n",
            streamThread.state(),
            lostPartitions,
            taskManager.activeTaskIds(),
            taskManager.standbyTaskIds());

        Set<TaskId> lostTasks = new HashSet<>();
        final long start = time.milliseconds();
        try {
            // close lost active tasks but don't try to commit offsets as we no longer own them
            lostTasks = taskManager.closeLostTasks(lostPartitions);
        } catch (final Throwable t) {
            log.error(
                "Error caught during partitions lost, " +
                    "will abort the current process and re-throw at the end of rebalance: ",
                t
            );
            streamThread.setRebalanceException(t);
        } finally {
            log.info("partitions lost took {} ms.\n" +
                    "\tsuspended lost active tasks: {}\n",
                time.milliseconds() - start,
                lostTasks);
        }
    }

}
