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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectorPluginInfo {

    private static final Logger log = LoggerFactory.getLogger(ConnectorPluginInfo.class);

    private static final Map<Class<? extends Connector>, String>
        VERSIONS = new ConcurrentHashMap<>();

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

    public ConnectorPluginInfo(Class<? extends Connector> klass) {
        this(klass.getCanonicalName(), ConnectorType.from(klass), getVersion(klass));
    }

    private static String getVersion(Class<? extends Connector> klass) {
        if (!VERSIONS.containsKey(klass)) {
            synchronized (VERSIONS) {
                if (!VERSIONS.containsKey(klass)) {
                    try {
                        VERSIONS.put(klass, klass.newInstance().version());
                    } catch (
                        ExceptionInInitializerError
                            | InstantiationException
                            | IllegalAccessException
                            | SecurityException e
                    ) {
                        log.warn("Unable to instantiate connector", e);
                        VERSIONS.put(klass, "unknown");
                    }
                }
            }
        }
        return VERSIONS.get(klass);
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
