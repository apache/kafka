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
package org.apache.kafka.common;

import java.util.Map;
import java.util.Objects;

/**
 * The <code>MetricName</code> class encapsulates a metric's name, logical group and its related attributes. It should be constructed using metrics.MetricName(...).
 * <p>
 * This class captures the following parameters:
 * <pre>
 *  <b>name</b> The name of the metric
 *  <b>group</b> logical group name of the metrics to which this metric belongs.
 *  <b>description</b> A human-readable description to include in the metric. This is optional.
 *  <b>tags</b> additional key/value attributes of the metric. This is optional.
 * </pre>
 * group, tags parameters can be used to create unique metric names while reporting in JMX or any custom reporting.
 * <p>
 * Ex: standard JMX MBean can be constructed like <b>domainName:type=group,key1=val1,key2=val2</b>
 * <p>
 *
 * Usage looks something like this:
 * <pre>{@code
 * // set up metrics:
 *
 * Map<String, String> metricTags = new LinkedHashMap<String, String>();
 * metricTags.put("client-id", "producer-1");
 * metricTags.put("topic", "topic");
 *
 * MetricConfig metricConfig = new MetricConfig().tags(metricTags);
 * Metrics metrics = new Metrics(metricConfig); // this is the global repository of metrics and sensors
 *
 * Sensor sensor = metrics.sensor("message-sizes");
 *
 * MetricName metricName = metrics.metricName("message-size-avg", "producer-metrics", "average message size");
 * sensor.add(metricName, new Avg());
 *
 * metricName = metrics.metricName("message-size-max", "producer-metrics");
 * sensor.add(metricName, new Max());
 *
 * metricName = metrics.metricName("message-size-min", "producer-metrics", "message minimum size", "client-id", "my-client", "topic", "my-topic");
 * sensor.add(metricName, new Min());
 *
 * // as messages are sent we record the sizes
 * sensor.record(messageSize);
 * }</pre>
 */
public final class MetricName {

    private final String name;
    private final String group;
    private final String description;
    private final Map<String, String> tags;
    private int hash = 0;

    /**
     * Please create MetricName by method {@link org.apache.kafka.common.metrics.Metrics#metricName(String, String, String, Map)}
     *
     * @param name        The name of the metric
     * @param group       logical group name of the metrics to which this metric belongs
     * @param description A human-readable description to include in the metric
     * @param tags        additional key/value attributes of the metric
     */
    public MetricName(String name, String group, String description, Map<String, String> tags) {
        this.name = Objects.requireNonNull(name);
        this.group = Objects.requireNonNull(group);
        this.description = Objects.requireNonNull(description);
        this.tags = Objects.requireNonNull(tags);
    }

    public String name() {
        return this.name;
    }

    public String group() {
        return this.group;
    }

    public Map<String, String> tags() {
        return this.tags;
    }

    public String description() {
        return this.description;
    }

    @Override
    public int hashCode() {
        if (hash != 0)
            return hash;
        final int prime = 31;
        int result = 1;
        result = prime * result + group.hashCode();
        result = prime * result + name.hashCode();
        result = prime * result + tags.hashCode();
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MetricName other = (MetricName) obj;
        return group.equals(other.group) && name.equals(other.name) && tags.equals(other.tags);
    }

    @Override
    public String toString() {
        return "MetricName [name=" + name + ", group=" + group + ", description="
                + description + ", tags=" + tags + "]";
    }
}
