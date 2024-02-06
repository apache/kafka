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
package org.apache.kafka.common.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ConfigValue {

    private final String name;
    private Object value;
    private List<Object> recommendedValues;
    private final List<String> errorMessages;
    private boolean visible;

    public ConfigValue(String name) {
        this(name, null, new ArrayList<>(), new ArrayList<>());
    }

    public ConfigValue(String name, Object value, List<Object> recommendedValues, List<String> errorMessages) {
        this.name = name;
        this.value = value;
        this.recommendedValues = recommendedValues;
        this.errorMessages = errorMessages;
        this.visible = true;
    }

    public String name() {
        return name;
    }

    public Object value() {
        return value;
    }

    public List<Object> recommendedValues() {
        return recommendedValues;
    }

    public List<String> errorMessages() {
        return errorMessages;
    }

    public boolean visible() {
        return visible;
    }

    public void value(Object value) {
        this.value = value;
    }

    public void recommendedValues(List<Object> recommendedValues) {
        this.recommendedValues = recommendedValues;
    }

    public void addErrorMessage(String errorMessage) {
        this.errorMessages.add(errorMessage);
    }

    public void visible(boolean visible) {
        this.visible = visible;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConfigValue that = (ConfigValue) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(value, that.value) &&
               Objects.equals(recommendedValues, that.recommendedValues) &&
               Objects.equals(errorMessages, that.errorMessages) &&
               Objects.equals(visible, that.visible);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, recommendedValues, errorMessages, visible);
    }

    @Override
    public String toString() {
        return "[" +
                name + "," +
                value + "," +
                recommendedValues + "," +
                errorMessages + "," +
                visible +
                "]";
    }
}
