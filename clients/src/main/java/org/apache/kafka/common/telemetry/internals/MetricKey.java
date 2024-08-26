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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Value object that contains the name and tags for a Metric.
 */
public class MetricKey implements MetricKeyable {

    private final String name;
    private final Map<String, String> tags;

    /**
     * Create a {@code MetricKey}
     *
     * @param name metric name. This should be the telemetry metric name of the metric (the final name
     *             under which this metric is emitted).
     */
    public MetricKey(String name) {
        this(name, null);
    }

    /**
     * Create a {@code MetricKey}
     *
     * @param name metric name. This should be the .converted. name of the metric (the final name
     *             under which this metric is emitted).
     * @param tags mapping of tag keys to values.
     */
    public MetricKey(String name, Map<String, String> tags) {
        this.name = Objects.requireNonNull(name);
        this.tags = tags != null ? Collections.unmodifiableMap(tags) : Collections.emptyMap();
    }

    public MetricKey(MetricName metricName) {
        this(metricName.name(), metricName.tags());
    }

    @Override
    public MetricKey key() {
        return this;
    }

    public String name() {
        return name;
    }

    public Map<String, String> tags() {
        return tags;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, tags);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MetricKey other = (MetricKey) obj;
        return this.name().equals(other.name()) && this.tags().equals(other.tags());
    }

    @Override
    public String toString() {
        return "MetricKey {name=" + name() + ", tags=" + tags() + "}";
    }

}
