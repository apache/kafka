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

import java.util.ServiceLoader;
import java.util.SortedSet;

/**
 * A {@link PluginScanner} implementation which uses {@link ServiceLoader} to discover plugins.
 * <p>This implements the modern discovery strategy, which uses only service loading in order to discover plugins.
 * Specifically, a plugin appears in the scan result if all the following conditions are true:
 * <ul>
 *     <li>The class and direct dependencies can be loaded</li>
 *     <li>The class is concrete</li>
 *     <li>The class is public</li>
 *     <li>The class has a no-args constructor</li>
 *     <li>The no-args constructor is public</li>
 *     <li>Static initialization of the class completes without throwing an exception</li>
 *     <li>The no-args constructor completes without throwing an exception</li>
 *     <li>The class is a subclass of {@link SinkConnector}, {@link SourceConnector}, {@link Converter},
 *         {@link HeaderConverter}, {@link Transformation}, {@link Predicate}, {@link ConfigProvider},
 *         {@link ConnectRestExtension}, or {@link ConnectorClientConfigOverridePolicy}
 *     </li>
 *     <li>The class has a {@link ServiceLoader} compatible manifest file or module declaration</li>
 * </ul>
 * <p>Note: This scanner can only find plugins with corresponding {@link ServiceLoader} manifests, which are
 * not required to be packaged with plugins. This has the effect that some plugins discoverable by the
 * {@link ReflectionScanner} may not be visible with this implementation.
 */
public class ServiceLoaderScanner extends PluginScanner {

    @Override
    protected PluginScanResult scanPlugins(PluginSource source) {
        return new PluginScanResult(
                getServiceLoaderPluginDesc(PluginType.SINK, source),
                getServiceLoaderPluginDesc(PluginType.SOURCE, source),
                getServiceLoaderPluginDesc(PluginType.CONVERTER, source),
                getServiceLoaderPluginDesc(PluginType.HEADER_CONVERTER, source),
                getTransformationPluginDesc(source),
                getPredicatePluginDesc(source),
                getServiceLoaderPluginDesc(PluginType.CONFIGPROVIDER, source),
                getServiceLoaderPluginDesc(PluginType.REST_EXTENSION, source),
                getServiceLoaderPluginDesc(PluginType.CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY, source)
        );
    }

    @SuppressWarnings({"unchecked"})
    private SortedSet<PluginDesc<Predicate<?>>> getPredicatePluginDesc(PluginSource source) {
        return (SortedSet<PluginDesc<Predicate<?>>>) (SortedSet<?>) getServiceLoaderPluginDesc(PluginType.PREDICATE, source);
    }

    @SuppressWarnings({"unchecked"})
    private SortedSet<PluginDesc<Transformation<?>>> getTransformationPluginDesc(PluginSource source) {
        return (SortedSet<PluginDesc<Transformation<?>>>) (SortedSet<?>) getServiceLoaderPluginDesc(PluginType.TRANSFORMATION, source);
    }
}
