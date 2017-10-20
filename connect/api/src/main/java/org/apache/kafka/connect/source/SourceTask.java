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
     * Poll this SourceTask for new records. This method should block if no data is currently
     * available.
     *
     * @return a list of source records
     */
    public abstract List<SourceRecord> poll() throws InterruptedException;

    /**
     * See {@link #offsetsFlushedAndAcknowledged(List)}
     *
     * Deprecated. Use {@link #offsetsFlushedAndAcknowledged(List)} instead
     */
    @Deprecated
    public void commit() throws InterruptedException {
        // This space intentionally left blank.
    }

    /**
     * <p>
     * Notification that offsets on {@link SourceRecord}s returned by {@link #poll()} has just been flushed and
     * acknowledged. No additional flushing of offsets go on before returning from this method.
     * </p>
     * <p>
     * SourceTasks are not required to implement this method. This hook is provided for systems that
     * needs to react somehow to the fact that offsets flushed
     * </p>
     * <p>
     * By default this method will call {@link #commit()} for backwards compatibility
     * </p>
     *
     * @param offsetsFlushed The list of {@link SourceRecord}s that just had their offsets flushed. It may NOT include
     *                       records recently returned by {@link #poll()}
     * @throws InterruptedException
     */
    public void offsetsFlushedAndAcknowledged(List<SourceRecord> offsetsFlushed) throws InterruptedException {
        commit();
    }

    /**
     * Signal this SourceTask to stop. In SourceTasks, this method only needs to signal to the task that it should stop
     * trying to poll for new data and interrupt any outstanding poll() requests. It is not required that the task has
     * fully stopped. Note that this method necessarily may be invoked from a different thread than {@link #poll()} and
     * {@link #offsetsFlushedAndAcknowledged(List)}.
     *
     * For example, if a task uses a {@link java.nio.channels.Selector} to receive data over the network, this method
     * could set a flag that will force {@link #poll()} to exit immediately and invoke
     * {@link java.nio.channels.Selector#wakeup() wakeup()} to interrupt any ongoing requests.
     */
    public abstract void stop();

    /**
     * See {@link #recordSentAndAcknowledged(SourceRecord)}
     *
     * Deprecated. Use {@link #recordSentAndAcknowledged(SourceRecord)} instead
     */
    @Deprecated
    public void commitRecord(SourceRecord record) throws InterruptedException {
        // This space intentionally left blank.
    }

    /**
     * <p>
     * Notification that a {@link SourceRecord} returned by {@link #poll()} was just acknowledged by
     * the receiving Kafka, or that it was filtered by a transformation.
     * </p>
     * <p>
     * SourceTasks are not required to implement this method. This hook is provided for systems that
     * needs to react somehow to the fact that {@link SourceRecord} was successfully forwarded
     * </p>
     * <p>
     * By default this method will call {@link #commitRecord(SourceRecord)} for backwards compatibility
     * </p>
     *
     * @param record {@link SourceRecord} that was successfully sent and acknowledged via the producer.
     * @throws InterruptedException
     */
    public void recordSentAndAcknowledged(SourceRecord record) throws InterruptedException {
        commitRecord(record);
    }
}
