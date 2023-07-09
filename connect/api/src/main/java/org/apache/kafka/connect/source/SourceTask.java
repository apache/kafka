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

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.connector.Task;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * SourceTask is a Task that pulls records from another system for storage in Kafka.
 */
public abstract class SourceTask implements Task {

    /**
     * The configuration key that determines how source tasks will define transaction boundaries
     * when exactly-once support is enabled.
     */
    public static final String TRANSACTION_BOUNDARY_CONFIG = "transaction.boundary";

    /**
     * Represents the permitted values for the {@link #TRANSACTION_BOUNDARY_CONFIG} property.
     */
    public enum TransactionBoundary {
        /**
         * A new transaction will be started and committed for every batch of records returned by {@link #poll()}.
         */
        POLL,
        /**
         * Transactions will be started and committed on a user-defined time interval.
         */
        INTERVAL,
        /**
         * Transactions will be defined by the connector itself, via a {@link TransactionContext}.
         */
        CONNECTOR;

        /**
         * The default transaction boundary style that will be used for source connectors when no style is explicitly
         * configured.
         */
        public static final TransactionBoundary DEFAULT = POLL;

        /**
         * Parse a {@link TransactionBoundary} from the given string.
         * @param property the string to parse; should not be null
         * @return the {@link TransactionBoundary} whose name matches the given string
         * @throws IllegalArgumentException if there is no transaction boundary type with the given name
         */
        public static TransactionBoundary fromProperty(String property) {
            Objects.requireNonNull(property, "Value for transaction boundary property may not be null");
            return TransactionBoundary.valueOf(property.toUpperCase(Locale.ROOT).trim());
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

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
     * Poll this source task for new records. If no data is currently available, this method
     * should block but return control to the caller regularly (by returning {@code null}) in
     * order for the task to transition to the {@code PAUSED} state if requested to do so.
     * <p>
     * The task will be {@link #stop() stopped} on a separate thread, and when that happens
     * this method is expected to unblock, quickly finish up any remaining processing, and
     * return.
     *
     * @return a list of source records
     */
    public abstract List<SourceRecord> poll() throws InterruptedException;

    /**
     * <p>
     * Commit the offsets, up to the offsets that have been returned by {@link #poll()}. This
     * method should block until the commit is complete.
     * <p>
     * SourceTasks are not required to implement this functionality; Kafka Connect will record offsets
     * automatically. This hook is provided for systems that also need to store offsets internally
     * in their own system.
     */
    public void commit() throws InterruptedException {
        // This space intentionally left blank.
    }

    /**
     * Signal this SourceTask to stop. In SourceTasks, this method only needs to signal to the task that it should stop
     * trying to poll for new data and interrupt any outstanding poll() requests. It is not required that the task has
     * fully stopped. Note that this method necessarily may be invoked from a different thread than {@link #poll()} and
     * {@link #commit()}.
     * <p>
     * For example, if a task uses a {@link java.nio.channels.Selector} to receive data over the network, this method
     * could set a flag that will force {@link #poll()} to exit immediately and invoke
     * {@link java.nio.channels.Selector#wakeup() wakeup()} to interrupt any ongoing requests.
     */
    @Override
    public abstract void stop();

    /**
     * <p>
     * Commit an individual {@link SourceRecord} when the callback from the producer client is received. This method is
     * also called when a record is filtered by a transformation, and thus will never be ACK'd by a broker.
     * <p>
     * This is an alias for {@link #commitRecord(SourceRecord, RecordMetadata)} for backwards compatibility. The default
     * implementation of {@link #commitRecord(SourceRecord, RecordMetadata)} just calls this method. It is not necessary
     * to override both methods.
     * <p>
     * SourceTasks are not required to implement this functionality; Kafka Connect will record offsets
     * automatically. This hook is provided for systems that also need to store offsets internally
     * in their own system.
     *
     * @param record {@link SourceRecord} that was successfully sent via the producer or filtered by a transformation
     * @throws InterruptedException
     * @deprecated Use {@link #commitRecord(SourceRecord, RecordMetadata)} instead.
     */
    @Deprecated
    public void commitRecord(SourceRecord record) throws InterruptedException {
        // This space intentionally left blank.
    }

    /**
     * <p>
     * Commit an individual {@link SourceRecord} when the callback from the producer client is received. This method is
     * also called when a record is filtered by a transformation or when "errors.tolerance" is set to "all"
     * and thus will never be ACK'd by a broker.
     * In both cases {@code metadata} will be null.
     * <p>
     * SourceTasks are not required to implement this functionality; Kafka Connect will record offsets
     * automatically. This hook is provided for systems that also need to store offsets internally
     * in their own system.
     * <p>
     * The default implementation just calls {@link #commitRecord(SourceRecord)}, which is a nop by default. It is
     * not necessary to implement both methods.
     *
     * @param record {@link SourceRecord} that was successfully sent via the producer, filtered by a transformation, or dropped on producer exception
     * @param metadata {@link RecordMetadata} record metadata returned from the broker, or null if the record was filtered or if producer exceptions are ignored
     * @throws InterruptedException
     */
    public void commitRecord(SourceRecord record, RecordMetadata metadata)
            throws InterruptedException {
        // by default, just call other method for backwards compatibility
        commitRecord(record);
    }

    /**
     * Hook to update the offsets for source partitions before offsets are committed. Source tasks can use this
     * hook to update the offsets for any source partition which isn't part of the offsets about to be committed or update
     * the offsets for any source partition. If any source partition is dropped, or has been set to a null offset,
     * then it has no effect on the offsets committed.
     * @param offsets the offsets that were accumulated as per last poll or iteration. Could be empty, never null.
     *                Note that this could mean different things based upon the mode:
     *                At Least Once Mode: The offsets that are about to be committed based on the previous poll
     *                EOS: The offsets about to be committed based on the transaction boundary.
     *
     * @return An optional map of updated Source partitions and offsets.
     */
    public Optional<Map<Map<String, Object>, Map<String, Object>>> updateOffsets(Map<Map<String, Object>, Map<String, Object>> offsets) {
        return Optional.empty();
    }
}
