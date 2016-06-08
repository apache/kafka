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

public class ConfigValueInfo {
    private String name;
    private String value;
    private List<String> recommendedValues;
    private List<String> errors;
    private boolean visible;

    @JsonCreator
    public ConfigValueInfo(
        @JsonProperty("name") String name,
        @JsonProperty("value") String value,
        @JsonProperty("recommended_values") List<String> recommendedValues,
        @JsonProperty("errors") List<String> errors,
        @JsonProperty("visible") boolean visible) {
        this.name = name;
        this.value = value;
        this.recommendedValues = recommendedValues;
        this.errors = errors;
        this.visible = visible;
    }

    @JsonProperty
    public String name() {
        return name;
    }

    @JsonProperty
    public String value() {
        return value;
    }

    @JsonProperty("recommended_values")
    public List<String> recommendedValues() {
        return recommendedValues;
    }

    @JsonProperty
    public List<String> errors() {
        return errors;
    }

    @JsonProperty
    public boolean visible() {
        return visible;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConfigValueInfo that = (ConfigValueInfo) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(value, that.value) &&
               Objects.equals(recommendedValues, that.recommendedValues) &&
               Objects.equals(errors, that.errors) &&
               Objects.equals(visible, that.visible);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, recommendedValues, errors, visible);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[")
            .append(name)
            .append(",")
            .append(value)
            .append(",")
            .append(recommendedValues)
            .append(",")
            .append(errors)
            .append(",")
            .append(visible)
            .append("]");
        return sb.toString();
    }

}
