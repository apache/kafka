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
package org.apache.kafka.common.metrics;

/**
 * A non-measurable gauge value that can be registered as a metric.
 * This is separate from {@link Measurable} to avoid breaking the public API.
 */
public interface Gauge<T> {

    /**
     * Returns the current value associated with this gauge.
     * <p>
     * In the future we may introduce a super-interface that is implemented
     * by {@link Measurable} as well with a default implementation that returns
     * {@link Measurable#measure(MetricConfig, long)} (for Java8 and above).
     * </p>
     * @param config The configuration for this metric
     * @param now The POSIX time in milliseconds the measurement is being taken
     */
    public T value(MetricConfig config, long now);

}
