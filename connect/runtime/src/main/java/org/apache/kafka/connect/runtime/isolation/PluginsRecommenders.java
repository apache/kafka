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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PluginsRecommenders {

    private final Plugins plugins;
    private final ConverterPluginRecommender converterPluginRecommender;
    private final ConnectorPluginVersionRecommender connectorPluginVersionRecommender;
    private final HeaderConverterPluginRecommender headerConverterPluginRecommender;
    private final KeyConverterPluginVersionRecommender keyConverterPluginVersionRecommender;
    private final ValueConverterPluginVersionRecommender valueConverterPluginVersionRecommender;
    private final HeaderConverterPluginVersionRecommender headerConverterPluginVersionRecommender;

    public PluginsRecommenders() {
        this(null);
    }

    public PluginsRecommenders(Plugins plugins) {
        this.plugins = plugins;
        this.converterPluginRecommender = new ConverterPluginRecommender();
        this.connectorPluginVersionRecommender = new ConnectorPluginVersionRecommender();
        this.headerConverterPluginRecommender = new HeaderConverterPluginRecommender();
        this.keyConverterPluginVersionRecommender = new KeyConverterPluginVersionRecommender();
        this.valueConverterPluginVersionRecommender = new ValueConverterPluginVersionRecommender();
        this.headerConverterPluginVersionRecommender = new HeaderConverterPluginVersionRecommender();
    }

    public ConverterPluginRecommender converterPluginRecommender() {
        return converterPluginRecommender;
    }

    public ConnectorPluginVersionRecommender connectorPluginVersionRecommender() {
        return connectorPluginVersionRecommender;
    }

    public HeaderConverterPluginRecommender headerConverterPluginRecommender() {
        return headerConverterPluginRecommender;
    }

    public KeyConverterPluginVersionRecommender keyConverterPluginVersionRecommender() {
        return keyConverterPluginVersionRecommender;
    }

    public ValueConverterPluginVersionRecommender valueConverterPluginVersionRecommender() {
        return valueConverterPluginVersionRecommender;
    }

    public HeaderConverterPluginVersionRecommender headerConverterPluginVersionRecommender() {
        return headerConverterPluginVersionRecommender;
    }

    public TransformationPluginRecommender transformationPluginRecommender(String classOrAlias) {
        return new TransformationPluginRecommender(classOrAlias);
    }

    public PredicatePluginRecommender predicatePluginRecommender(String classOrAlias) {
        return new PredicatePluginRecommender(classOrAlias);
    }

    public class ConnectorPluginVersionRecommender implements ConfigDef.Recommender {

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            if (plugins == null) {
                return Collections.emptyList();
            }
            String connectorClassOrAlias = (String) parsedConfig.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
            if (connectorClassOrAlias == null) {
                //should never happen
                return Collections.emptyList();
            }
            List<Object> sourceConnectors = plugins.sourceConnectors(connectorClassOrAlias).stream()
                    .map(PluginDesc::version).distinct().collect(Collectors.toList());
            if (!sourceConnectors.isEmpty()) {
                return sourceConnectors;
            }
            return plugins.sinkConnectors(connectorClassOrAlias).stream()
                    .map(PluginDesc::version).distinct().collect(Collectors.toList());
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return parsedConfig.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG) != null;
        }

    }

    public class ConverterPluginRecommender implements ConfigDef.Recommender {

        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            if (plugins == null) {
                return Collections.emptyList();
            }
            return plugins.converters().stream()
                    .map(PluginDesc::pluginClass).distinct().collect(Collectors.toList());
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    public class HeaderConverterPluginRecommender implements ConfigDef.Recommender {

        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            if (plugins == null) {
                return Collections.emptyList();
            }
            return plugins.headerConverters().stream()
                    .map(PluginDesc::pluginClass).distinct().collect(Collectors.toList());
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    public abstract class ConverterPluginVersionRecommender implements ConfigDef.Recommender {

        protected Function<String, List<Object>> recommendations() {
            return converterClass -> plugins.converters(converterClass).stream()
                    .map(PluginDesc::version).distinct().collect(Collectors.toList());
        }

        protected abstract String converterConfig();

        @SuppressWarnings({"rawtypes"})
        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            if (plugins == null) {
                return Collections.emptyList();
            }
            if (parsedConfig.get(converterConfig()) == null) {
                return Collections.emptyList();
            }
            Class converterClass = (Class) parsedConfig.get(converterConfig());
            return recommendations().apply(converterClass.getName());
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return parsedConfig.get(converterConfig()) != null;
        }
    }

    public class KeyConverterPluginVersionRecommender extends ConverterPluginVersionRecommender {

        @Override
        protected String converterConfig() {
            return ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
        }

    }

    public class ValueConverterPluginVersionRecommender extends ConverterPluginVersionRecommender {

        @Override
        protected String converterConfig() {
            return ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
        }
    }

    public class HeaderConverterPluginVersionRecommender extends ConverterPluginVersionRecommender {

        @Override
        protected String converterConfig() {
            return ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG;
        }

        @Override
        protected Function<String, List<Object>> recommendations() {
            return converterClass -> plugins.headerConverters(converterClass).stream()
                    .map(PluginDesc::version).distinct().collect(Collectors.toList());
        }
    }

    // Recommender for transformation and predicate plugins
    public abstract class SMTPluginRecommender<T> implements ConfigDef.Recommender {

        protected abstract Function<String, Set<PluginDesc<T>>> plugins();

        protected final String classOrAliasConfig;

        public SMTPluginRecommender(String classOrAliasConfig) {
            this.classOrAliasConfig = classOrAliasConfig;
        }

        @Override
        @SuppressWarnings({"rawtypes"})
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            if (plugins == null) {
                return Collections.emptyList();
            }
            if (parsedConfig.get(classOrAliasConfig) == null) {
                return Collections.emptyList();
            }

            Class classOrAlias = (Class) parsedConfig.get(classOrAliasConfig);
            return plugins().apply(classOrAlias.getName())
                    .stream().map(PluginDesc::version).distinct().collect(Collectors.toList());
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    public class TransformationPluginRecommender extends SMTPluginRecommender<Transformation<?>> {

        public TransformationPluginRecommender(String classOrAliasConfig) {
            super(classOrAliasConfig);
        }

        @Override
        protected Function<String, Set<PluginDesc<Transformation<?>>>> plugins() {
            return plugins::transformations;
        }
    }

    public class PredicatePluginRecommender extends SMTPluginRecommender<Predicate<?>> {

        public PredicatePluginRecommender(String classOrAliasConfig) {
            super(classOrAliasConfig);
        }

        @Override
        protected Function<String, Set<PluginDesc<Predicate<?>>>> plugins() {
            return plugins::predicates;
        }
    }
}
