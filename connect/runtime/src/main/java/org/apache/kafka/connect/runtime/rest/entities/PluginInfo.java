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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.PluginType;

import java.util.Objects;

public class PluginInfo {
    private final String className;
    private final PluginType type;
    private final String version;

    @JsonCreator
    public PluginInfo(
        @JsonProperty("class") String className,
        @JsonProperty("type") PluginType type,
        @JsonProperty("version") String version
    ) {
        this.className = className;
        this.type = type;
        this.version = version;
    }

    public PluginInfo(PluginDesc<?> plugin) {
        this(plugin.className(), plugin.type(), plugin.version());
    }

    @JsonProperty("class")
    public String className() {
        return className;
    }

    @JsonProperty("type")
    public String type() {
        return type.toString();
    }

    @JsonProperty("version")
    @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = NoVersionFilter.class)
    public String version() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PluginInfo that = (PluginInfo) o;
        return Objects.equals(className, that.className) &&
               Objects.equals(type, that.type) &&
               Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(className, type, version);
    }

    @Override
    public String toString() {
        return "PluginInfo{" + "className='" + className + '\'' +
                ", type=" + type.toString() +
                ", version='" + version + '\'' +
                '}';
    }

    public static final class NoVersionFilter {
        // This method is used by Jackson to filter the version field for plugins that don't have a version
        public boolean equals(Object obj) {
            return PluginDesc.UNDEFINED_VERSION.equals(obj);
        }

        // Dummy hashCode method to not fail compilation because of equals() method
        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }
}
