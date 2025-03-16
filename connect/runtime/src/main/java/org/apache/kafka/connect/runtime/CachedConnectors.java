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

package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.isolation.VersionedPluginLoadingException;

import org.apache.maven.artifact.versioning.VersionRange;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CachedConnectors {

    private static final String LATEST_VERSION = "latest";

    private final Map<String, Map<String, Connector>> connectors;
    private final Map<String, Throwable> invalidConnectors;
    private final Map<String, Map<String, VersionedPluginLoadingException>> invalidVersions;
    private final Plugins plugins;

    public CachedConnectors(Plugins plugins) {
        this.plugins = plugins;
        this.connectors = new ConcurrentHashMap<>();
        this.invalidConnectors = new ConcurrentHashMap<>();
        this.invalidVersions = new ConcurrentHashMap<>();
    }

    private void validate(String connectorName, VersionRange range) throws ConnectException, VersionedPluginLoadingException {
        if (invalidConnectors.containsKey(connectorName)) {
            throw new ConnectException(invalidConnectors.get(connectorName));
        }

        String version = range == null ? LATEST_VERSION : range.toString();
        if (invalidVersions.containsKey(connectorName) && invalidVersions.get(connectorName).containsKey(version)) {
            throw new VersionedPluginLoadingException(invalidVersions.get(connectorName).get(version).getMessage());
        }
    }

    private Connector lookup(String connectorName, VersionRange range) throws Exception {
        String version = range == null ? LATEST_VERSION : range.toString();
        if (connectors.containsKey(connectorName) && connectors.get(connectorName).containsKey(version)) {
            return connectors.get(connectorName).get(version);
        }

        try {
            Connector connector = plugins.newConnector(connectorName, range);
            connectors.computeIfAbsent(connectorName, k -> new ConcurrentHashMap<>()).put(version, connector);
            return connector;
        } catch (VersionedPluginLoadingException e) {
            invalidVersions.computeIfAbsent(connectorName, k -> new ConcurrentHashMap<>()).put(version, e);
            throw e;
        } catch (Exception e) {
            invalidConnectors.put(connectorName, e);
            throw e;
        }
    }

    public Connector getConnector(String connectorName, VersionRange range) throws Exception {
        validate(connectorName, range);
        return lookup(connectorName, range);
    }
}
