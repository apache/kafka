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

import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.PluginType;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public record PluginInfo(
    @JsonProperty("class") String className,
    @JsonProperty("type") PluginType type,
    @JsonProperty("version")
    @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = NoVersionFilter.class)
    String version
) {
    public PluginInfo(PluginDesc<?> plugin) {
        this(plugin.className(), plugin.type(), plugin.version());
    }

    public static final class NoVersionFilter {
        // Used by Jackson to filter out undefined versions
        @Override
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