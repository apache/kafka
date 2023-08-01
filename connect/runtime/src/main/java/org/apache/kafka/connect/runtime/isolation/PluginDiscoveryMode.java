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
package org.apache.kafka.connect.runtime.isolation;

import java.util.Locale;

/**
 * Strategy to use to discover plugins usable on a Connect worker.
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-898%3A+Modernize+Connect+plugin+discovery">KIP-898</a>
 */
public enum PluginDiscoveryMode {

    /**
     * Scan for plugins reflectively. This corresponds to the legacy behavior of Connect prior to KIP-898.
     * <p>Note: the following plugins are still loaded using {@link java.util.ServiceLoader} in this mode:
     * <ul>
     *     <li>{@link org.apache.kafka.common.config.provider.ConfigProvider}</li>
     *     <li>{@link org.apache.kafka.connect.rest.ConnectRestExtension}</li>
     *     <li>{@link org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy}</li>
     * </ul>
     */
    ONLY_SCAN,
    /**
     * Scan for plugins reflectively and via {@link java.util.ServiceLoader}.
     * Emit warnings if one or more plugins is not available via {@link java.util.ServiceLoader}
     */
    HYBRID_WARN,
    /**
     * Scan for plugins reflectively and via {@link java.util.ServiceLoader}.
     * Fail worker during startup if one or more plugins is not available via {@link java.util.ServiceLoader}
     */
    HYBRID_FAIL,
    /**
     * Discover plugins via {@link java.util.ServiceLoader} only.
     * Plugins may not be usable if they are not available via {@link java.util.ServiceLoader}
     */
    SERVICE_LOAD;

    public boolean reflectivelyScan() {
        return this != SERVICE_LOAD;
    }

    public boolean serviceLoad() {
        return this != ONLY_SCAN;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
