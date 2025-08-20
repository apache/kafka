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

/**
 * A {@code MetricsCollector} is responsible for scraping a source of metrics and forwarding
 * them to the given {@link MetricsEmitter}. For example, a given collector might be used to collect
 * system metrics, Kafka metrics, JVM metrics, or other metrics that are to be captured, exposed,
 * and/or forwarded.
 *
 * <p/>
 *
 * In general, a {@code MetricsCollector} implementation is closely managed by another entity
 * (that entity is colloquially referred to as the "telemetry reporter") that will be in
 * charge of its lifecycle via the {@link #start()} and {@link #stop()} methods. The telemetry
 * reporter should ensure that the {@link #start()} method is invoked <i>once and only once</i>
 * before calls to {@link #collect(MetricsEmitter)} are made. Implementations of {@code MetricsCollector}
 * should allow for the corner-case that {@link #stop()} is called before {@link #start()},
 * which might happen in the case of error on startup of the telemetry reporter.
 *
 * <p/>
 *
 * Regarding threading, the {@link #start()} and {@link #stop()} methods may be called from
 * different threads and so proper care should be taken by implementations of the
 * {@code MetricsCollector} interface to be thread-safe. However, the telemetry reporter must
 * ensure that the {@link #collect(MetricsEmitter)} method should only be invoked in a synchronous
 * manner.
 *
 * @see MetricsEmitter
 */
public interface MetricsCollector {

    /**
     * The {@code collect} method is called by the telemetry reporter to retrieve the value
     * of its desired set of metrics, and then forward those on to the provided
     * {@link MetricsEmitter}. The implementation may choose to collect all the metrics before forwarding
     * them to the {@code metricsEmitter}, or they may be forwarded as they are collected.
     *
     * <p>
     *
     * In general, the implementation should try not to presume the characteristics of the
     * {@link MetricsEmitter} so as to keep a loose coupling.
     *
     * @param metricsEmitter {@link MetricsEmitter} to which the metric values will be passed once collected
     */
    void collect(MetricsEmitter metricsEmitter);

    /**
     * Allows the {@code MetricsCollector} implementation to initialize itself. This method should
     * be invoked by the telemetry reporter before calls to {@link #collect(MetricsEmitter)} are made. The
     * telemetry reporter should not invoke this method more than once.
     */
    default void start() {
        // Do nothing...
    }

    /**
     * Allows the {@code MetricsCollector} implementation to stop itself and dispose of any resources.
     * This method should ideally be invoked only once by the telemetry reporter.
     *
     * <p>
     *
     * Calls to {@link #collect(MetricsEmitter)} once this method has been invoked should be expected to
     * fail by the telemetry reporter; it should take caution to handle that case.
     */
    default void stop() {
        // Do nothing...
    }
}
