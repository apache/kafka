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
import java.util.SortedSet;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;

public class PluginScanResult {
    private final SortedSet<PluginDesc<SinkConnector>> sinkConnectors;
    private final SortedSet<PluginDesc<SourceConnector>> sourceConnectors;
    private final SortedSet<PluginDesc<Converter>> converters;
    private final SortedSet<PluginDesc<HeaderConverter>> headerConverters;
    private final SortedSet<PluginDesc<Transformation<?>>> transformations;
    private final SortedSet<PluginDesc<Predicate<?>>> predicates;
    private final SortedSet<PluginDesc<ConfigProvider>> configProviders;
    private final SortedSet<PluginDesc<ConnectRestExtension>> restExtensions;
    private final SortedSet<PluginDesc<ConnectorClientConfigOverridePolicy>> connectorClientConfigPolicies;

    private final List<SortedSet<? extends PluginDesc<?>>> allPlugins;

    public PluginScanResult(
            SortedSet<PluginDesc<SinkConnector>> sinkConnectors,
            SortedSet<PluginDesc<SourceConnector>> sourceConnectors,
            SortedSet<PluginDesc<Converter>> converters,
            SortedSet<PluginDesc<HeaderConverter>> headerConverters,
            SortedSet<PluginDesc<Transformation<?>>> transformations,
            SortedSet<PluginDesc<Predicate<?>>> predicates,
            SortedSet<PluginDesc<ConfigProvider>> configProviders,
            SortedSet<PluginDesc<ConnectRestExtension>> restExtensions,
            SortedSet<PluginDesc<ConnectorClientConfigOverridePolicy>> connectorClientConfigPolicies
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
            Arrays.asList(sinkConnectors, sourceConnectors, converters, headerConverters, transformations, predicates,
                    configProviders, restExtensions, connectorClientConfigPolicies);
    }

    /**
     * Merge one or more {@link PluginScanResult results} into one result object
     */
    public PluginScanResult(List<PluginScanResult> results) {
        this(
                merge(results, PluginScanResult::sinkConnectors),
                merge(results, PluginScanResult::sourceConnectors),
                merge(results, PluginScanResult::converters),
                merge(results, PluginScanResult::headerConverters),
                merge(results, PluginScanResult::transformations),
                merge(results, PluginScanResult::predicates),
                merge(results, PluginScanResult::configProviders),
                merge(results, PluginScanResult::restExtensions),
                merge(results, PluginScanResult::connectorClientConfigPolicies)
        );
    }

    private static <R extends Comparable<?>> SortedSet<R> merge(List<PluginScanResult> results, Function<PluginScanResult, SortedSet<R>> accessor) {
        SortedSet<R> merged = new TreeSet<>();
        for (PluginScanResult element : results) {
            merged.addAll(accessor.apply(element));
        }
        return merged;
    }

    public SortedSet<PluginDesc<SinkConnector>> sinkConnectors() {
        return sinkConnectors;
    }

    public SortedSet<PluginDesc<SourceConnector>> sourceConnectors() {
        return sourceConnectors;
    }

    public SortedSet<PluginDesc<Converter>> converters() {
        return converters;
    }

    public SortedSet<PluginDesc<HeaderConverter>> headerConverters() {
        return headerConverters;
    }

    public SortedSet<PluginDesc<Transformation<?>>> transformations() {
        return transformations;
    }

    public SortedSet<PluginDesc<Predicate<?>>> predicates() {
        return predicates;
    }

    public SortedSet<PluginDesc<ConfigProvider>> configProviders() {
        return configProviders;
    }

    public SortedSet<PluginDesc<ConnectRestExtension>> restExtensions() {
        return restExtensions;
    }

    public SortedSet<PluginDesc<ConnectorClientConfigOverridePolicy>> connectorClientConfigPolicies() {
        return connectorClientConfigPolicies;
    }

    public void forEach(Consumer<PluginDesc<?>> consumer) {
        allPlugins.forEach(plugins -> plugins.forEach(consumer));
    }

    public boolean isEmpty() {
        boolean isEmpty = true;
        for (SortedSet<?> plugins : allPlugins) {
            isEmpty = isEmpty && plugins.isEmpty();
        }
        return isEmpty;
    }
}
