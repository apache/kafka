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

import java.util.Objects;

public class ConfigInfo {

    private ConfigKeyInfo configKey;
    private ConfigValueInfo configValue;

    @JsonCreator
    public ConfigInfo(
        @JsonProperty("definition") ConfigKeyInfo configKey,
        @JsonProperty("value") ConfigValueInfo configValue) {
        this.configKey = configKey;
        this.configValue = configValue;
    }

    @JsonProperty("definition")
    public ConfigKeyInfo configKey() {
        return configKey;
    }

    @JsonProperty("value")
    public ConfigValueInfo configValue() {
        return configValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConfigInfo that = (ConfigInfo) o;
        return Objects.equals(configKey, that.configKey) &&
               Objects.equals(configValue, that.configValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configKey, configValue);
    }

    @Override
    public String toString() {
        return "[" + configKey.toString() + "," + configValue.toString() + "]";
    }
}
