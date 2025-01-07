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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A template for a MetricName. It contains a name, group, and description, as
 * well as all the tags that will be used to create the mBean name. Tag values
 * are omitted from the template, but are filled in at runtime with their
 * specified values. The order of the tags is maintained, if an ordered set
 * is provided, so that the mBean names can be compared and sorted lexicographically.
 */
public class MetricNameTemplate {
    private final String name;
    private final String group;
    private final String description;
    private final LinkedHashSet<String> tags;

    /**
     * Create a new template. Note that the order of the tags will be preserved if the supplied
     * {@code tagsNames} set has an order.
     *
     * @param name the name of the metric; may not be null
     * @param group the name of the group; may not be null
     * @param description the description of the metric; may not be null
     * @param tagsNames the set of metric tag names, which can/should be a set that maintains order; may not be null
     */
    public MetricNameTemplate(String name, String group, String description, Set<String> tagsNames) {
        this.name = Objects.requireNonNull(name);
        this.group = Objects.requireNonNull(group);
        this.description = Objects.requireNonNull(description);
        this.tags = new LinkedHashSet<>(Objects.requireNonNull(tagsNames));
    }

    /**
     * Create a new template. Note that the order of the tags will be preserved.
     *
     * @param name the name of the metric; may not be null
     * @param group the name of the group; may not be null
     * @param description the description of the metric; may not be null
     * @param tagsNames the names of the metric tags in the preferred order; none of the tag names should be null
     */
    public MetricNameTemplate(String name, String group, String description, String... tagsNames) {
        this(name, group, description, getTags(tagsNames));
    }

    private static LinkedHashSet<String> getTags(String... keys) {
        LinkedHashSet<String> tags = new LinkedHashSet<>();

        Collections.addAll(tags, keys);

        return tags;
    }

    /**
     * Get the name of the metric.
     *
     * @return the metric name; never null
     */
    public String name() {
        return this.name;
    }

    /**
     * Get the name of the group.
     *
     * @return the group name; never null
     */
    public String group() {
        return this.group;
    }

    /**
     * Get the description of the metric.
     *
     * @return the metric description; never null
     */
    public String description() {
        return this.description;
    }

    /**
     * Get the set of tag names for the metric.
     *
     * @return the ordered set of tag names; never null but possibly empty
     */
    public Set<String> tags() {
        return tags;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, group, tags);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        MetricNameTemplate other = (MetricNameTemplate) o;
        return Objects.equals(name, other.name) && Objects.equals(group, other.group) &&
                Objects.equals(tags, other.tags);
    }

    @Override
    public String toString() {
        return String.format("name=%s, group=%s, tags=%s", name, group, tags);
    }
}
