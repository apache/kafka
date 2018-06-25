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
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Collection;

public class PluginScanResult {
    private final Collection<PluginDesc<Connector>> connectors;
    private final Collection<PluginDesc<Converter>> converters;
    private final Collection<PluginDesc<HeaderConverter>> headerConverters;
    private final Collection<PluginDesc<Transformation>> transformations;
    private final Collection<PluginDesc<ConfigProvider>> configProviders;
    private final Collection<PluginDesc<ConnectRestExtension>> restExtensions;

    public PluginScanResult(
            Collection<PluginDesc<Connector>> connectors,
            Collection<PluginDesc<Converter>> converters,
            Collection<PluginDesc<HeaderConverter>> headerConverters,
            Collection<PluginDesc<Transformation>> transformations,
            Collection<PluginDesc<ConfigProvider>> configProviders,
            Collection<PluginDesc<ConnectRestExtension>> restExtensions
    ) {
        this.connectors = connectors;
        this.converters = converters;
        this.headerConverters = headerConverters;
        this.transformations = transformations;
        this.configProviders = configProviders;
        this.restExtensions = restExtensions;
    }

    public Collection<PluginDesc<Connector>> connectors() {
        return connectors;
    }

    public Collection<PluginDesc<Converter>> converters() {
        return converters;
    }

    public Collection<PluginDesc<HeaderConverter>> headerConverters() {
        return headerConverters;
    }

    public Collection<PluginDesc<Transformation>> transformations() {
        return transformations;
    }

    public Collection<PluginDesc<ConfigProvider>> configProviders() {
        return configProviders;
    }

    public Collection<PluginDesc<ConnectRestExtension>> restExtensions() {
        return restExtensions;
    }

    public boolean isEmpty() {
        return connectors().isEmpty()
               && converters().isEmpty()
               && headerConverters().isEmpty()
               && transformations().isEmpty()
               && configProviders().isEmpty()
               && restExtensions().isEmpty();
    }
}
