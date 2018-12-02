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
package org.apache.kafka.connect.source;

import org.apache.kafka.connect.connector.Task;

import java.util.List;
import java.util.Map;

/**
 * SourceTask is a Task that pulls records from another system for storage in Kafka.
 */
public abstract class SourceTask implements Task {

    protected SourceTaskContext context;

    /**
     * Initialize this SourceTask with the specified context object.
     */
    public void initialize(SourceTaskContext context) {
        this.context = context;
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     * @param props initial configuration
     */
    @Override
    public abstract void start(Map<String, String> props);

    /**
     * <p>
     * Poll this source task for new records. If no data is currently available, this method
     * should block but return control to the caller regularly (by returning {@code null}) in
     * order for the task to transition to the {@code PAUSED} state if requested to do so.
     * </p>
     * <p>
     * The task will be {@link #stop() stopped} on a separate thread, and when that happens
     * this method is expected to unblock, quickly finish up any remaining processing, and
     * return.
     * </p>
     *
     * @return a list of source records
     */
    public abstract List<SourceRecord> poll() throws InterruptedException;

    /**
     * <p>
     * Commit the offsets, up to the offsets that have been returned by {@link #poll()}. This
     * method should block until the commit is complete.
     * </p>
     * <p>
     * SourceTasks are not required to implement this functionality; Kafka Connect will record offsets
     * automatically. This hook is provided for systems that also need to store offsets internally
     * in their own system.
     * </p>
     */
    public void commit() throws InterruptedException {
        // This space intentionally left blank.
    }

    /**
     * Signal this SourceTask to stop. In SourceTasks, this method only needs to signal to the task that it should stop
     * trying to poll for new data and interrupt any outstanding poll() requests. It is not required that the task has
     * fully stopped. Note that this method necessarily may be invoked from a different thread than {@link #poll()} and
     * {@link #commit()}.
     *
     * For example, if a task uses a {@link java.nio.channels.Selector} to receive data over the network, this method
     * could set a flag that will force {@link #poll()} to exit immediately and invoke
     * {@link java.nio.channels.Selector#wakeup() wakeup()} to interrupt any ongoing requests.
     */
    @Override
    public abstract void stop();

    /**
     * <p>
     * Commit an individual {@link SourceRecord} when the callback from the producer client is received, or if a record is filtered by a transformation.
     * </p>
     * <p>
     * SourceTasks are not required to implement this functionality; Kafka Connect will record offsets
     * automatically. This hook is provided for systems that also need to store offsets internally
     * in their own system.
     * </p>
     *
     * @param record {@link SourceRecord} that was successfully sent via the producer.
     * @throws InterruptedException
     */
    public void commitRecord(SourceRecord record) throws InterruptedException {
        // This space intentionally left blank.
    }
}
