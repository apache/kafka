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

import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.HostInfo;

/**
 * A read-only metadata class representing the current state of each KafkaStreams client with at least one StreamThread participating in this rebalance
 */
public interface KafkaStreamsState {
    /**
     * @return the processId of the application instance running on this KafkaStreams client
     */
    ProcessId processId();

    /**
     * Returns the number of processing threads available to work on tasks for this KafkaStreams client,
     * which represents its overall capacity for work relative to other KafkaStreams clients.
     *
     * @return the number of processing threads on this KafkaStreams client
     */
    int numProcessingThreads();

    /**
     * @return the set of consumer client ids for this KafkaStreams client
     */
    SortedSet<String> consumerClientIds();

    /**
     * @return the set of all active tasks owned by consumers on this KafkaStreams client since the previous rebalance
     */
    SortedSet<TaskId> previousActiveTasks();

    /**
     * @return the set of all standby tasks owned by consumers on this KafkaStreams client since the previous rebalance
     */
    SortedSet<TaskId> previousStandbyTasks();

    /**
     * Returns the total lag across all logged stores in the task. Equal to the end offset sum if this client
     * did not have any state for this task on disk.
     *
     * @return end offset sum - offset sum
     *                    Task.LATEST_OFFSET if this was previously an active running task on this client
     *
     * @throws UnsupportedOperationException if the user did not request task lags be computed.
     */
    long lagFor(final TaskId task);

    /**
     * @return the previous tasks assigned to this consumer ordered by lag, filtered for any tasks that don't exist in this assignment
     *
     * @throws UnsupportedOperationException if the user did not request task lags be computed.
     */
    SortedSet<TaskId> prevTasksByLag(final String consumerClientId);

    /**
     * Returns a collection containing all (and only) stateful tasks in the topology by {@link TaskId},
     * mapped to its "offset lag sum". This is computed as the difference between the changelog end offset
     * and the current offset, summed across all logged state stores in the task.
     *
     * @return a map from all stateful tasks to their lag sum
     *
     * @throws UnsupportedOperationException if the user did not request task lags be computed.
     */
    Map<TaskId, Long> statefulTasksToLagSums();

    /**
     * The {@link HostInfo} of this KafkaStreams client, if set via the
     * {@link org.apache.kafka.streams.StreamsConfig#APPLICATION_SERVER_CONFIG application.server} config
     *
     * @return the host info for this KafkaStreams client if configured, else {@code Optional.empty()}
     */
    Optional<HostInfo> hostInfo();

    /**
     * The client tags for this KafkaStreams client, if set any have been via configs using the
     * {@link org.apache.kafka.streams.StreamsConfig#clientTagPrefix}
     * <p>
     * Can be used however you want, or passed in to enable the rack-aware standby task assignor.
     *
     * @return all the client tags found in this KafkaStreams client's {@link org.apache.kafka.streams.StreamsConfig}
     */
    Map<String, String> clientTags();
}