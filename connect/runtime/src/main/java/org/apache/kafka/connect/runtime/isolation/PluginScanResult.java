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

import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class PluginScanResult {
    private final Collection<PluginDesc<SinkConnector>> sinkConnectors;
    private final Collection<PluginDesc<SourceConnector>> sourceConnectors;
    private final Collection<PluginDesc<Converter>> converters;
    private final Collection<PluginDesc<HeaderConverter>> headerConverters;
    private final Collection<PluginDesc<Transformation<?>>> transformations;
    private final Collection<PluginDesc<Predicate<?>>> predicates;
    private final Collection<PluginDesc<ConfigProvider>> configProviders;
    private final Collection<PluginDesc<ConnectRestExtension>> restExtensions;
    private final Collection<PluginDesc<ConnectorClientConfigOverridePolicy>> connectorClientConfigPolicies;

    private final List<Collection<?>> allPlugins;

    public PluginScanResult(
            Collection<PluginDesc<SinkConnector>> sinkConnectors,
            Collection<PluginDesc<SourceConnector>> sourceConnectors,
            Collection<PluginDesc<Converter>> converters,
            Collection<PluginDesc<HeaderConverter>> headerConverters,
            Collection<PluginDesc<Transformation<?>>> transformations,
            Collection<PluginDesc<Predicate<?>>> predicates,
            Collection<PluginDesc<ConfigProvider>> configProviders,
            Collection<PluginDesc<ConnectRestExtension>> restExtensions,
            Collection<PluginDesc<ConnectorClientConfigOverridePolicy>> connectorClientConfigPolicies
    ) {
        this.sinkConnectors = sinkConnectors;
        this.sourceConnectors = sourceConnectors;
        this.converters = converters;
        this.headerConverters = headerConverters;
        this.transformations = transformations;
        this.predicates = predicates;
        this.configProviders = configProviders;
        this.restExtensions = restExtensions;
        this.connectorClientConfigPolicies = connectorClientConfigPolicies;
        this.allPlugins =
            Arrays.asList(sinkConnectors, sourceConnectors, converters, headerConverters, transformations, configProviders,
                          connectorClientConfigPolicies);
    }

    public Collection<PluginDesc<SinkConnector>> sinkConnectors() {
        return sinkConnectors;
    }

    public Collection<PluginDesc<SourceConnector>> sourceConnectors() {
        return sourceConnectors;
    }

    public Collection<PluginDesc<Converter>> converters() {
        return converters;
    }

    public Collection<PluginDesc<HeaderConverter>> headerConverters() {
        return headerConverters;
    }

    public Collection<PluginDesc<Transformation<?>>> transformations() {
        return transformations;
    }

    public Collection<PluginDesc<Predicate<?>>> predicates() {
        return predicates;
    }

    public Collection<PluginDesc<ConfigProvider>> configProviders() {
        return configProviders;
    }

    public Collection<PluginDesc<ConnectRestExtension>> restExtensions() {
        return restExtensions;
    }

    public Collection<PluginDesc<ConnectorClientConfigOverridePolicy>> connectorClientConfigPolicies() {
        return connectorClientConfigPolicies;
    }

    public boolean isEmpty() {
        boolean isEmpty = true;
        for (Collection<?> plugins : allPlugins) {
            isEmpty = isEmpty && plugins.isEmpty();
        }
        return isEmpty;
    }
}
