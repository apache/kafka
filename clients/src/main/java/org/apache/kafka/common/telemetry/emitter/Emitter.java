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
package org.apache.kafka.common.telemetry.emitter;

import java.io.Closeable;
import org.apache.kafka.common.telemetry.metrics.MetricKey;
import org.apache.kafka.common.telemetry.metrics.MetricKeyable;
import org.apache.kafka.common.telemetry.metrics.Metric;

/**
 * An {@code Emitter} emits the values held by the {@link Metric}, likely first converting them
 * to a format suitable for exposure, storage, or transmission. The telemetry manager is likely
 * the entity that is familiar with the underlying method of making the metrics visible to the
 * outside world. Thus, it is the primary place in the code where the implementation details
 * are known. Care should be taken to limit the scope of these internal implementation details so
 * as to keep the code more flexible.
 *
 * <p/>
 *
 * An {@code Emitter} is stateless and the telemetry manager should assume that the object is
 * not thread safe and thus concurrent access to either the
 * {@link #shouldEmitMetric(MetricKeyable)} or {@link #emitMetric(Metric)} should be avoided.
 *
 * Regarding threading, the {@link #init()} and {@link #close()} methods may be called from
 * different threads and so proper care should be taken by implementations of the
 * {@code MetricsCollector} interface to be thread-safe. However, the telemetry manager must
 * ensure that the {@link #emitMetric(Metric)} method should only be invoked in a synchronous
 * manner.
 */
public interface Emitter extends Closeable {

    /**
     * Performs the necessary logic to determine if the metric is to be emitted. The telemetry
     * manager should respect this and not just call the {@link #emitMetric(Metric)} directly.
     *
     * @param metricKeyable Object from which to get the {@link MetricKey}
     * @return {@code true} if the metric should be emitted, {@code false} otherwise
     */
    boolean shouldEmitMetric(MetricKeyable metricKeyable);

    /**
     * Emits the metric in an implementation-specific fashion. Depending on the implementation,
     * calls made to this after {@link #close()} has been invoked will fail.
     *
     * @param metric {@link Metric}
     * @return {@code true} if the metric was emitted, {@code false} otherwise
     */

    boolean emitMetric(Metric metric);

    /**
     * Allows for a the {@code Emitter} implementation to initialize itself. This
     * method should be invoked by the telemetry manager before calls to {@link #emitMetric(Metric)}
     * are made. The telemetry manager should not invoke this method more than once.
     */
    default void init() {
        // Do nothing...
    }

    /**
     * Allows for a the {@code Emitter} implementation to stop itself and dispose of any
     * resources. This method should ideally be invoked only once by the telemetry manager, but
     * under some edge cases it might end up being called more than once.
     *
     * <p/>
     *
     * Calls to {@link #emitMetric(Metric)} once this method has been invoked should be expected to
     * fail by the telemetry manager; it should take caution to handle that case.
     */
    default void close() {
        // Do nothing...
    }

}
