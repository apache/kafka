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

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.common.utils.Utils;

/**
 * A template for a MetricName. It contains a name, group, and description, as
 * well as all the tags that will be used to create the mBean name. Tag values
 * are omitted from the template, but are filled in at runtime with their
 * specified values.
 */
public class MetricNameTemplate {
    private final String name;
    private final String group;
    private final String description;
    private Set<String> tags;

    public MetricNameTemplate(String name, String group, String description, Set<String> tags) {
        this.name = Utils.notNull(name);
        this.group = Utils.notNull(group);
        this.description = Utils.notNull(description);
        this.tags = Utils.notNull(tags);
    }
    
    public MetricNameTemplate(String name, String group, String description, String... keys) {
        this(name, group, description, getTags(keys));
    }

    private static Set<String> getTags(String... keys) {
        Set<String> tags = new HashSet<String>();
        
        for (int i = 0; i < keys.length; i++)
            tags.add(keys[i]);

        return tags;
    }

    public String name() {
        return this.name;
    }

    public String group() {
        return this.group;
    }

    public String description() {
        return this.description;
    }

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
