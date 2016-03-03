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

public class ConfigValueInfo {
    private String name;
    private Object value;
    private List<Object> recommendedValues;
    private List<String> errors;
    private boolean visible;

    @JsonCreator
    public ConfigValueInfo(
        @JsonProperty("name") String name,
        @JsonProperty("value") Object value,
        @JsonProperty("recommended_values") List<Object> recommendedValues,
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
    public Object value() {
        return value;
    }

    @JsonProperty("recommended_values")
    public List<Object> recommendedValues() {
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
}
