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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.kstream.internals.suppress.EagerBufferConfigImpl;
import org.apache.kafka.streams.kstream.internals.suppress.FinalResultsSuppressionBuilder;
import org.apache.kafka.streams.kstream.internals.suppress.StrictBufferConfigImpl;
import org.apache.kafka.streams.kstream.internals.suppress.SuppressedInternal;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

public interface Suppressed<K> extends NamedOperation<Suppressed<K>> {

    /**
     * Marker interface for a buffer configuration that is "strict" in the sense that it will strictly
     * enforce the time bound and never emit early.
     */
    interface StrictBufferConfig extends BufferConfig<StrictBufferConfig> {

    }

    /**
     * Marker interface for a buffer configuration that will strictly enforce size constraints
     * (bytes and/or number of records) on the buffer, so it is suitable for reducing duplicate
     * results downstream, but does not promise to eliminate them entirely.
     */
    interface EagerBufferConfig extends BufferConfig<EagerBufferConfig> {

    }

    interface BufferConfig<BC extends BufferConfig<BC>> {
        /**
         * Create a size-constrained buffer in terms of the maximum number of keys it will store.
         */
        static EagerBufferConfig maxRecords(final long recordLimit) {
            return new EagerBufferConfigImpl(recordLimit, Long.MAX_VALUE, Collections.emptyMap());
        }

        /**
         * Set a size constraint on the buffer in terms of the maximum number of keys it will store.
         */
        BC withMaxRecords(final long recordLimit);

        /**
         * Create a size-constrained buffer in terms of the maximum number of bytes it will use.
         */
        static EagerBufferConfig maxBytes(final long byteLimit) {
            return new EagerBufferConfigImpl(Long.MAX_VALUE, byteLimit, Collections.emptyMap());
        }

        /**
         * Set a size constraint on the buffer, the maximum number of bytes it will use.
         */
        BC withMaxBytes(final long byteLimit);

        /**
         * Create a buffer unconstrained by size (either keys or bytes).
         *
         * As a result, the buffer will consume as much memory as it needs, dictated by the time bound.
         *
         * If there isn't enough heap available to meet the demand, the application will encounter an
         * {@link OutOfMemoryError} and shut down (not guaranteed to be a graceful exit). Also, note that
         * JVM processes under extreme memory pressure may exhibit poor GC behavior.
         *
         * This is a convenient option if you doubt that your buffer will be that large, but also don't
         * wish to pick particular constraints, such as in testing.
         *
         * This buffer is "strict" in the sense that it will enforce the time bound or crash.
         * It will never emit early.
         */
        static StrictBufferConfig unbounded() {
            return new StrictBufferConfigImpl();
        }

        /**
         * Set the buffer to be unconstrained by size (either keys or bytes).
         *
         * As a result, the buffer will consume as much memory as it needs, dictated by the time bound.
         *
         * If there isn't enough heap available to meet the demand, the application will encounter an
         * {@link OutOfMemoryError} and shut down (not guaranteed to be a graceful exit). Also, note that
         * JVM processes under extreme memory pressure may exhibit poor GC behavior.
         *
         * This is a convenient option if you doubt that your buffer will be that large, but also don't
         * wish to pick particular constraints, such as in testing.
         *
         * This buffer is "strict" in the sense that it will enforce the time bound or crash.
         * It will never emit early.
         */
        StrictBufferConfig withNoBound();

        /**
         * Set the buffer to gracefully shut down the application when any of its constraints are violated
         *
         * This buffer is "strict" in the sense that it will enforce the time bound or shut down.
         * It will never emit early.
         */
        StrictBufferConfig shutDownWhenFull();

        /**
         * Set the buffer to just emit the oldest records when any of its constraints are violated.
         *
         * This buffer is "not strict" in the sense that it may emit early, so it is suitable for reducing
         * duplicate results downstream, but does not promise to eliminate them.
         */
        EagerBufferConfig emitEarlyWhenFull();

        /**
         * Disable the changelog for this suppression's internal buffer.
         * This will turn off fault-tolerance for the suppression, and will result in data loss in the event of a rebalance.
         * By default the changelog is enabled.
         * @return this
         */
        BC withLoggingDisabled();

        /**
         * Indicates that a changelog topic should be created containing the currently suppressed
         * records. Due to the short-lived nature of records in this topic it is likely more
         * compactable than changelog topics for KTables.
         *
         * @param config Configs that should be applied to the changelog. Note: Any unrecognized
         *               configs will be ignored.
         * @return this
         */
        BC withLoggingEnabled(final Map<String, String> config);
    }

    /**
     * Configure the suppression to emit only the "final results" from the window.
     *
     * By default all Streams operators emit results whenever new results are available.
     * This includes windowed operations.
     *
     * This configuration will instead emit just one result per key for each window, guaranteeing
     * to deliver only the final result. This option is suitable for use cases in which the business logic
     * requires a hard guarantee that only the final result is propagated. For example, sending alerts.
     *
     * To accomplish this, the operator will buffer events from the window until the window close (that is,
     * until the end-time passes, and additionally until the grace period expires). Since windowed operators
     * are required to reject out-of-order events for a window whose grace period is expired, there is an additional
     * guarantee that the final results emitted from this suppression will match any queryable state upstream.
     *
     * @param bufferConfig A configuration specifying how much space to use for buffering intermediate results.
     *                     This is required to be a "strict" config, since it would violate the "final results"
     *                     property to emit early and then issue an update later.
     * @return a "final results" mode suppression configuration
     */
    static Suppressed<Windowed> untilWindowCloses(final StrictBufferConfig bufferConfig) {
        return new FinalResultsSuppressionBuilder<>(null, bufferConfig);
    }

    /**
     * Configure the suppression to wait {@code timeToWaitForMoreEvents} amount of time after receiving a record
     * before emitting it further downstream. If another record for the same key arrives in the mean time, it replaces
     * the first record in the buffer but does <em>not</em> re-start the timer.
     *
     * @param timeToWaitForMoreEvents The amount of time to wait, per record, for new events.
     * @param bufferConfig A configuration specifying how much space to use for buffering intermediate results.
     * @param <K> The key type for the KTable to apply this suppression to.
     * @return a suppression configuration
     */
    static <K> Suppressed<K> untilTimeLimit(final Duration timeToWaitForMoreEvents, final BufferConfig bufferConfig) {
        return new SuppressedInternal<>(null, timeToWaitForMoreEvents, bufferConfig, null, false);
    }

    /**
     * Use the specified name for the suppression node in the topology.
     * <p>
     * This can be used to insert a suppression without changing the rest of the topology names
     * (and therefore not requiring an application reset).
     * <p>
     * Note however, that once a suppression has buffered some records, removing it from the topology would cause
     * the loss of those records.
     * <p>
     * A suppression can be "disabled" with the configuration {@code untilTimeLimit(Duration.ZERO, ...}.
     *
     * @param name The name to be used for the suppression node and changelog topic
     * @return The same configuration with the addition of the given {@code name}.
     */
    @Override
    Suppressed<K> withName(final String name);
}
