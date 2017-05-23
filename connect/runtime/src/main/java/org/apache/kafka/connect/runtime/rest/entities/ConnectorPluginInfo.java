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
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;

import java.util.Objects;

public class ConnectorPluginInfo {
    private String className;
    private ConnectorType type;
    private String version;

    @JsonCreator
    public ConnectorPluginInfo(
        @JsonProperty("class") String className,
        @JsonProperty("type") ConnectorType type,
        @JsonProperty("version") String version
    ) {
        this.className = className;
        this.type = type;
        this.version = version;
    }

    public ConnectorPluginInfo(PluginDesc<Connector> plugin) {
        this(plugin.className(), ConnectorType.from(plugin.pluginClass()), plugin.version());
    }

    @JsonProperty("class")
    public String className() {
        return className;
    }

    @JsonProperty("type")
    public ConnectorType type() {
        return type;
    }

    @JsonProperty("version")
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
        ConnectorPluginInfo that = (ConnectorPluginInfo) o;
        return Objects.equals(className, that.className) &&
               type == that.type &&
               Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(className, type, version);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ConnectorPluginInfo{");
        sb.append("className='").append(className).append('\'');
        sb.append(", type=").append(type);
        sb.append(", version='").append(version).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
