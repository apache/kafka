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

public class ConfigInfos {

    @JsonProperty("name")
    private final String name;

    @JsonProperty("error_count")
    private final int errorCount;

    @JsonProperty("groups")
    private final List<String> groups;

    @JsonProperty("configs")
    private final List<ConfigInfo> configs;

    @JsonCreator
    public ConfigInfos(@JsonProperty("name") String name,
                       @JsonProperty("error_count") int errorCount,
                       @JsonProperty("groups") List<String> groups,
                       @JsonProperty("configs") List<ConfigInfo> configs) {
        this.name = name;
        this.groups = groups;
        this.errorCount = errorCount;
        this.configs = configs;
    }

    @JsonProperty
    public String name() {
        return name;
    }

    @JsonProperty
    public List<String> groups() {
        return groups;
    }

    @JsonProperty("error_count")
    public int errorCount() {
        return errorCount;
    }

    @JsonProperty("configs")
    public List<ConfigInfo> values() {
        return configs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConfigInfos that = (ConfigInfos) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(errorCount, that.errorCount) &&
               Objects.equals(groups, that.groups) &&
               Objects.equals(configs, that.configs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, errorCount, groups, configs);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[")
            .append(name)
            .append(",")
            .append(errorCount)
            .append(",")
            .append(groups)
            .append(",")
            .append(configs)
            .append("]");
        return sb.toString();
    }

}
