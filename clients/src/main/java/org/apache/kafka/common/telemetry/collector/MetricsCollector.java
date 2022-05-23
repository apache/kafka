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
package org.apache.kafka.common.telemetry.collector;

import java.io.Closeable;
import org.apache.kafka.common.telemetry.emitter.Emitter;

/**
 * A {@code MetricsCollector} is responsible for scraping a source of metrics and forwarding
 * them to the given {@link Emitter}. For example, a given collector might be used to collect
 * system metrics, Kafka metrics, JVM metrics, or other metrics that are to be captured, exposed,
 * and/or forwarded.
 *
 * <p/>
 *
 * In general, a {@code MetricsCollector} implementation is closely managed by another entity
 * (that entity is colloquially referred to as the "telemetry manager") that will be in
 * charge of its lifecycle via the {@link #init()} and {@link #close()} methods. The telemetry
 * manager should ensure that the {@link #init()} method is invoked <i>once and only once</i>
 * before calls to {@link #collect(Emitter)} are made. Implementations of {@code MetricsCollector}
 * should allow for the corner-case that {@link #close()} is called before {@link #init()},
 * which might happen in the case of error on startup of the telemetry manager.
 *
 * <p/>
 *
 * Regarding threading, the {@link #init()} and {@link #close()} methods may be called from
 * different threads and so proper care should be taken by implementations of the
 * {@code MetricsCollector} interface to be thread-safe. However, the telemetry manager must
 * ensure that the {@link #collect(Emitter)} method should only be invoked in a synchronous
 * manner.
 *
 * @see Emitter
 */
public interface MetricsCollector extends Closeable {

    /**
     * The {@code collect} method is called by the telemetry manager to retrieve the value
     * of its desired set of metrics, and then forward those on to the provided
     * {@link Emitter}. The implementation may choose to collect all the metrics before forwarding
     * them to the {@code emitter}, or they may be forwarded as they are collected.
     *
     * <p/>
     *
     * In general, the implementation should try not to presume the characteristics of the
     * {@link Emitter} so as to keep a loose coupling.
     *
     * @param emitter {@link Emitter} to which the metric values will be passed once collected
     */
    void collect(Emitter emitter);

    /**
     * Allows for a the {@code MetricsCollector} implementation to initialize itself. This
     * method should be invoked by the telemetry manager before calls to {@link #collect(Emitter)}
     * are made. The telemetry manager should not invoke this method more than once.
     */
    default void init() {
        // Do nothing...
    }

    /**
     * Allows for a the {@code MetricsCollector} implementation to stop itself and dispose of any
     * resources. This method should ideally be invoked only once by the telemetry manager, but
     * under some edge cases it might end up being called more than once.
     *
     * <p/>
     *
     * Calls to {@link #collect(Emitter)} once this method has been invoked should be expected to
     * fail by the telemetry manager; it should take caution to handle that case.
     */
    default void close() {
        // Do nothing...
    }

}
