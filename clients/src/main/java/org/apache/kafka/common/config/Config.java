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

package org.apache.kafka.common.config;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class Config {

    private String name;
    private Object value;
    private List<Object> recommendedValues;
    private List<String> errorMessages;
    private boolean visible;

    public Config(String name) {
        this(name, null);
    }

    public Config(String name, Object value) {
        this(name, value, new LinkedList<Object>());
    }

    public Config(String name, Object value, List<Object> recommendedValues) {
        this(name, value, recommendedValues, new LinkedList<String>());
    }

    public Config(String name, Object value, List<Object> recommendedValues, List<String> errorMessages) {
        this(name, value, recommendedValues, errorMessages, true);
    }

    public Config(String name, Object value, List<Object> recommendedValues, List<String> errorMessages, boolean visible) {
        this.name = name;
        this.value = value;
        this.recommendedValues = recommendedValues;
        this.errorMessages = errorMessages;
        this.visible = visible;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    public List<Object> getRecommendedValues() {
        return recommendedValues;
    }

    public List<String> getErrorMessages() {
        return errorMessages;
    }

    public boolean isVisible() {
        return visible;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public void setRecommendedValues(List<Object> recommendedValues) {
        this.recommendedValues = recommendedValues;
    }

    public void addErrorMessage(String errorMessage) {
        this.errorMessages.add(errorMessage);
    }

    public void setVisible(boolean visible) {
        this.visible = visible;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Config that = (Config) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(value, that.value) &&
               Objects.equals(recommendedValues, that.recommendedValues) &&
               Objects.equals(errorMessages, that.errorMessages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, recommendedValues, errorMessages);
    }
}
