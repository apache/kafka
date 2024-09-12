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

import org.apache.kafka.common.MetricName;

/**
 * {@code MetricNamingStrategy} provides a strategy pattern-based means of converting from an
 * implementation-specific metric name (e.g. Kafka {@link MetricName}) representing a
 * particular metric name and associated tags into a canonical {@link MetricKey}
 * (name and tags) representation.
 *
 * <p>
 *
 * Each strategy may define its own conventions for how the resulting metric should be named,
 * including things such conforming name and tags to use specific casing and separators for
 * different parts of the metric name.
 *
 * <p>
 *
 * In general, a {@code MetricNamingStrategy} implementation is closely managed by another entity,
 * referred to as the "telemetry reporter", as that reporter handles the conversion between different
 * representations of metric names and keys.
 *
 * <p>
 *
 * This class is primarily used by the telemetry reporter, {@link MetricsCollector}, and
 * {@link MetricsEmitter} layers.
 */
public interface MetricNamingStrategy<T> {

    /**
     * Converts the given metric name into a {@link MetricKey} representation.
     *
     * @param metricName Implementation-specific metric
     * @return {@link MetricKey}
     */
    MetricKey metricKey(T metricName);

    /**
     * Creates a derived {@link MetricKey} from an existing {@link MetricKey}.
     *
     * <p>
     *
     * Some metrics may include multiple components derived from the same underlying source
     * of data (e.g. a Meter that exposes multiple rates and a counter) in which case it may
     * be desirable to create a new metric key derived from the primary one, with a different
     * name for each component of the metric.
     *
     * <p>
     *
     * Some metrics may be derived from others by the collector itself. For example, a delta
     * metric might be created from a cumulative counter.
     *
     * <p>
     *
     * This method exists so each strategy can define its own convention for how to name
     * derived metrics keys.
     *
     * <p>
     *
     * The derived key should have the same tags as the input key, and its name new name
     * will typically be composed of the input key name and the component name.
     *
     * @param key Input {@link MetricKey} used to construct the derived key
     * @param derivedComponent Name to use for the derived component of the input metric
     * @return Derived {@link MetricKey} with a new metric name composed of the input key
     * name and the additional name
     */
    MetricKey derivedMetricKey(MetricKey key, String derivedComponent);
}
