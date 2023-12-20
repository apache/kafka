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
 * A gauge metric is an instantaneous reading of a particular value.
 */
@FunctionalInterface
public interface Gauge<T> extends MetricValueProvider<T> {

    /**
     * Returns the current value associated with this gauge.
     * @param config The configuration for this metric
     * @param now The POSIX time in milliseconds the measurement is being taken
     */
    T value(MetricConfig config, long now);

}
