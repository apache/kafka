/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

public class ConfigKeyInfo {

    private final String name;
    private final String type;
    private final boolean required;
    private final String defaultValue;
    private final String importance;
    private final String documentation;
    private final String group;
    private final int orderInGroup;
    private final String width;
    private final String displayName;
    private final List<String> dependents;

    @JsonCreator
    public ConfigKeyInfo(@JsonProperty("name") String name,
                         @JsonProperty("type") String type,
                         @JsonProperty("required") boolean required,
                         @JsonProperty("default_value") String defaultValue,
                         @JsonProperty("importance") String importance,
                         @JsonProperty("documentation") String documentation,
                         @JsonProperty("group") String group,
                         @JsonProperty("order_in_group") int orderInGroup,
                         @JsonProperty("width") String width,
                         @JsonProperty("display_name") String displayName,
                         @JsonProperty("dependents") List<String> dependents) {
        this.name = name;
        this.type = type;
        this.required = required;
        this.defaultValue = defaultValue;
        this.importance = importance;
        this.documentation = documentation;
        this.group = group;
        this.orderInGroup = orderInGroup;
        this.width = width;
        this.displayName = displayName;
        this.dependents = dependents;
    }

    @JsonProperty
    public String name() {
        return name;
    }

    @JsonProperty
    public String type() {
        return type;
    }

    @JsonProperty
    public boolean required() {
        return required;
    }

    @JsonProperty("default_value")
    public String defaultValue() {
        return defaultValue;
    }

    @JsonProperty
    public String documentation() {
        return documentation;
    }

    @JsonProperty
    public String group() {
        return group;
    }

    @JsonProperty("order")
    public int orderInGroup() {
        return orderInGroup;
    }

    @JsonProperty
    public String width() {
        return width;
    }

    @JsonProperty
    public String importance() {
        return importance;
    }

    @JsonProperty("display_name")
    public String displayName() {
        return displayName;
    }

    @JsonProperty
    public List<String> dependents() {
        return dependents;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConfigKeyInfo that = (ConfigKeyInfo) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(type, that.type) &&
               Objects.equals(required, that.required) &&
               Objects.equals(defaultValue, that.defaultValue) &&
               Objects.equals(importance, that.importance) &&
               Objects.equals(documentation, that.documentation) &&
               Objects.equals(group, that.group) &&
               Objects.equals(orderInGroup, that.orderInGroup) &&
               Objects.equals(width, that.width) &&
               Objects.equals(displayName, that.displayName) &&
               Objects.equals(dependents, that.dependents);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, required, defaultValue, importance, documentation, group, orderInGroup, width, displayName, dependents);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[")
            .append(name)
            .append(",")
            .append(type)
            .append(",")
            .append(required)
            .append(",")
            .append(defaultValue)
            .append(",")
            .append(importance)
            .append(",")
            .append(documentation)
            .append(",")
            .append(group)
            .append(",")
            .append(orderInGroup)
            .append(",")
            .append(width)
            .append(",")
            .append(displayName)
            .append(",")
            .append(dependents)
            .append("]");
        return sb.toString();
    }
}
