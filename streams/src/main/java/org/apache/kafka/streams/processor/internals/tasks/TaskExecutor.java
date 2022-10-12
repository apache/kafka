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
package org.apache.kafka.streams.processor.internals.tasks;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.streams.processor.internals.ReadOnlyTask;
import org.apache.kafka.streams.processor.internals.StreamTask;

import java.time.Duration;

public interface TaskExecutor {

    /**
     * Starts the task processor.
     */
    void start();

    /**
     * Shuts down the task processor updater.
     *
     * @param timeout duration how long to wait until the state updater is shut down
     *
     * @throws
     *     org.apache.kafka.streams.errors.StreamsException if the state updater thread cannot shutdown within the timeout
     */
    void shutdown(final Duration timeout);

    /**
     * Get the current assigned processing task. The task returned is read-only and cannot be modified.
     *
     * @return the current processing task
     */
    ReadOnlyTask currentTask();

    /**
     * Unassign the current processing task from the task processor and give it back to the state manager.
     *
     * The paused task must be flushed since it may be committed or closed by the task manager next.
     *
     * This method does not block, instead a future is returned.
     */
    KafkaFuture<StreamTask> unassign();
}
