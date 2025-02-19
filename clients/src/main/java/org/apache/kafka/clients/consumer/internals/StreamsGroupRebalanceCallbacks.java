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
package org.apache.kafka.clients.consumer.internals;

import java.util.Optional;
import java.util.Set;

/**
 * Callbacks for handling Streams group rebalance events in Kafka Streams.
 */
public interface StreamsGroupRebalanceCallbacks {

    /**
     * Called when tasks are revoked from a stream thread.
     *
     * @param tasks The tasks to be revoked.
     * @return The exception thrown during the callback, if any.
     */
    Optional<Exception> onTasksRevoked(final Set<StreamsRebalanceData.TaskId> tasks);

    /**
     * Called when tasks are assigned from a stream thread.
     *
     * @param assignment The tasks assigned.
     * @return The exception thrown during the callback, if any.
     */
    Optional<Exception> onTasksAssigned(final StreamsRebalanceData.Assignment assignment);

    /**
     * Called when a stream thread loses all assigned tasks.
     *
     * @return The exception thrown during the callback, if any.
     */
    Optional<Exception> onAllTasksLost();
}
