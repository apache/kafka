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

package org.apache.kafka.connect.runtime.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ConfigKeyInfos {

    @JsonProperty("name")
    private final String name;

    @JsonProperty("groups")
    private final List<String> groups;

    @JsonProperty("configKeys")
    private final List<ConfigKeyInfo> configKeys;

    @JsonCreator
    public ConfigKeyInfos(@JsonProperty("name") String name,
                       @JsonProperty("groups") List<String> groups,
                       @JsonProperty("configKeys") List<ConfigKeyInfo> configKeys) {
        this.name = name;
        this.groups = groups;
        this.configKeys = configKeys;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConfigKeyInfos that = (ConfigKeyInfos) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(groups, that.groups) &&
            Objects.equals(configKeys, that.configKeys);
    }

    public List<ConfigKeyInfo> configKeys() {
        return Collections.unmodifiableList(configKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, groups, configKeys);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[")
            .append(name)
            .append(",")
            .append(groups)
            .append(",")
            .append(configKeys)
            .append("]");
        return sb.toString();
    }
}
