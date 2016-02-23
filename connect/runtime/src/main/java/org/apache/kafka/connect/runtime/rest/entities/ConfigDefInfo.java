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

public class ConfigDefInfo {
    private final String connectorType;
    private final List<ConfigKeyInfo> configs;

    @JsonCreator
    public ConfigDefInfo(
        @JsonProperty("connectorType") String connectorType,
        @JsonProperty("configs") List<ConfigKeyInfo> configs) {
        this.connectorType = connectorType;
        this.configs = configs;
    }

    @JsonProperty
    public String connectorType() {
        return connectorType;
    }

    @JsonProperty
    public List<ConfigKeyInfo> configs() {
        return configs;
    }
}
