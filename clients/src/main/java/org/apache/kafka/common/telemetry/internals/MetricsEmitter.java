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
package org.apache.kafka.common.telemetry.internals;

import java.util.Collections;
import java.util.List;

import java.io.Closeable;

/**
 * An {@code MetricsEmitter} emits the values held by the {@link SinglePointMetric}, likely first converting them
 * to a format suitable for exposure, storage, or transmission. The telemetry reporter is likely
 * the entity that is familiar with the underlying method of making the metrics visible to the
 * broker. Thus, it is the primary place in the code where the implementation details are known.
 *
 * <p>
 *
 * Regarding threading, the {@link #init()} and {@link #close()} methods may be called from
 * different threads and so proper care should be taken by implementations of the
 * {@code MetricsCollector} interface to be thread-safe. However, the telemetry reporter must
 * ensure that the {@link #emitMetric(SinglePointMetric)} and {@link #emittedMetrics()} methods
 * should only be invoked in a synchronous manner.
 */
public interface MetricsEmitter extends Closeable {

    /**
     * Performs the necessary logic to determine if the metric is to be emitted. The telemetry
     * reporter should respect this and not just call the {@link #emitMetric(SinglePointMetric)} directly.
     *
     * @param metricKeyable Object from which to get the {@link MetricKey}
     * @return {@code true} if the metric should be emitted, {@code false} otherwise
     */
    boolean shouldEmitMetric(MetricKeyable metricKeyable);

    /**
     * Determines if the delta aggregation temporality metrics are to be emitted.
     *
     * @return {@code true} if the delta metric should be emitted, {@code false} otherwise
     */
    boolean shouldEmitDeltaMetrics();

    /**
     * Emits the metric in an implementation-specific fashion. Depending on the implementation,
     * calls made to this after {@link #close()} has been invoked will fail.
     *
     * @param metric {@code SinglePointMetric}
     * @return {@code true} if the metric was emitted, {@code false} otherwise
     */
    boolean emitMetric(SinglePointMetric metric);

    /**
     * Return emitted metrics. Implementation should decide if all emitted metrics should be returned
     * or should provide the delta from last invocation of this method. Depending on the implementation,
     * calls made to this after {@link #close()} has been invoked will fail.
     *
     * @return emitted metrics.
     */
    default List<SinglePointMetric> emittedMetrics() {
        return Collections.emptyList();
    }

    /**
     * Emits a metric if {@link MetricsEmitter#shouldEmitMetric(MetricKeyable)} returns <tt>true</tt>.
     * @param metric to emit
     * @return true if emit is successful, false otherwise
     */
    default boolean maybeEmitMetric(SinglePointMetric metric) {
        return shouldEmitMetric(metric) && emitMetric(metric);
    }

    /**
     * Allows the {@code MetricsEmitter} implementation to initialize itself. This method should be invoked
     * by the telemetry reporter before calls to {@link #emitMetric(SinglePointMetric)} are made.
     *
     * <p>
     *
     * The telemetry reporter should not invoke this method more than once.
     */
    default void init() {
        // Do nothing...
    }

    /**
     * Allows the {@code MetricsEmitter} implementation to stop itself and dispose of any resources. This
     * method should ideally be invoked only once by the telemetry reporter.
     *
     * <p>
     *
     * Calls to {@link #emitMetric(SinglePointMetric)} once this method has been invoked should be
     * expected to fail by the telemetry reporter; it should take caution to handle that case.
     */
    default void close() {
        // Do nothing...
    }

}
