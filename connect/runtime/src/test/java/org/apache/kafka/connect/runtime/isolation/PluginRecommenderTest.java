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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.runtime.isolation.MultiVersionTest.DEFAULT_COMBINED_ARTIFACT_VERSIONS;
import static org.apache.kafka.connect.runtime.isolation.MultiVersionTest.DEFAULT_ISOLATED_ARTIFACTS;
import static org.apache.kafka.connect.runtime.isolation.MultiVersionTest.MULTI_VERSION_PLUGINS;

public class PluginRecommenderTest {

    private Set<String> allVersionsOff(String classOrAlias) {
        Set<String> versions = DEFAULT_ISOLATED_ARTIFACTS.values().stream()
            .flatMap(List::stream)
            .filter(b -> b.plugin().className().equals(classOrAlias))
            .map(VersionedPluginBuilder.BuildInfo::version)
            .collect(Collectors.toSet());
        Arrays.stream(VersionedPluginBuilder.VersionedTestPlugin.values()).filter(p -> p.className().equals(classOrAlias))
            .forEach(r -> versions.add(DEFAULT_COMBINED_ARTIFACT_VERSIONS.get(r)));
        return versions;
    }

    @Test
    public void TestConnectorVersionRecommenders() {
        PluginsRecommenders recommender = new PluginsRecommenders(MULTI_VERSION_PLUGINS);
        for (String connectorClass : Arrays.asList(
            VersionedPluginBuilder.VersionedTestPlugin.SINK_CONNECTOR.className(),
            VersionedPluginBuilder.VersionedTestPlugin.SOURCE_CONNECTOR.className())
        ) {
            Set<String> versions = recommender.connectorPluginVersionRecommender().validValues(
                ConnectorConfig.CONNECTOR_CLASS_CONFIG, Collections.singletonMap(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass)
            ).stream().map(Object::toString).collect(Collectors.toSet());
            Set<String> allVersions = allVersionsOff(connectorClass);
            Assertions.assertEquals(allVersions.size(), versions.size());
            allVersions.forEach(v -> Assertions.assertTrue(versions.contains(v), "Missing version " + v + " for connector " + connectorClass));
        }
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void TestConverterVersionRecommenders() throws ClassNotFoundException {
        PluginsRecommenders recommender = new PluginsRecommenders(MULTI_VERSION_PLUGINS);
        Map<String, Object> config = new HashMap<>();
        Class converterClass = MULTI_VERSION_PLUGINS.pluginClass(VersionedPluginBuilder.VersionedTestPlugin.CONVERTER.className());
        config.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, converterClass);
        config.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, converterClass);
        Set<String> allVersions = allVersionsOff(VersionedPluginBuilder.VersionedTestPlugin.CONVERTER.className());
        for (ConfigDef.Recommender r : Arrays.asList(recommender.keyConverterPluginVersionRecommender(), recommender.valueConverterPluginVersionRecommender())) {
            Set<String> versions = r.validValues(null, config).stream().map(Object::toString).collect(Collectors.toSet());
            Assertions.assertEquals(allVersions.size(), versions.size());
            allVersions.forEach(v -> Assertions.assertTrue(versions.contains(v), "Missing version " + v + " for converter"));
        }
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void TestHeaderConverterVersionRecommenders() throws ClassNotFoundException {
        PluginsRecommenders recommender = new PluginsRecommenders(MULTI_VERSION_PLUGINS);
        Map<String, Object> config = new HashMap<>();
        Class headerConverterClass = MULTI_VERSION_PLUGINS.pluginClass(VersionedPluginBuilder.VersionedTestPlugin.HEADER_CONVERTER.className());
        config.put(ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG, headerConverterClass);
        Set<String> versions = recommender.headerConverterPluginVersionRecommender().validValues(null, config).stream().map(Object::toString).collect(Collectors.toSet());
        Set<String> allVersions = allVersionsOff(VersionedPluginBuilder.VersionedTestPlugin.HEADER_CONVERTER.className());
        Assertions.assertEquals(allVersions.size(), versions.size());
        allVersions.forEach(v -> Assertions.assertTrue(versions.contains(v), "Missing version " + v + " for header converter"));
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void TestTransformationVersionRecommenders() throws ClassNotFoundException {
        PluginsRecommenders recommender = new PluginsRecommenders(MULTI_VERSION_PLUGINS);
        Class transformationClass = MULTI_VERSION_PLUGINS.pluginClass(VersionedPluginBuilder.VersionedTestPlugin.TRANSFORMATION.className());
        Set<String> versions = recommender.transformationPluginRecommender("transforms.t1.type")
            .validValues("transforms.t1.type", Collections.singletonMap("transforms.t1.type", transformationClass))
            .stream().map(Object::toString).collect(Collectors.toSet());
        Set<String> allVersions = allVersionsOff(VersionedPluginBuilder.VersionedTestPlugin.TRANSFORMATION.className());
        Assertions.assertEquals(allVersions.size(), versions.size());
        allVersions.forEach(v -> Assertions.assertTrue(versions.contains(v), "Missing version " + v + " for transformation"));
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void TestPredicateVersionRecommenders() throws ClassNotFoundException {
        PluginsRecommenders recommender = new PluginsRecommenders(MULTI_VERSION_PLUGINS);
        Class predicateClass = MULTI_VERSION_PLUGINS.pluginClass(VersionedPluginBuilder.VersionedTestPlugin.PREDICATE.className());
        Set<String> versions = recommender.predicatePluginRecommender("predicates.p1.type")
            .validValues("predicates.p1.type", Collections.singletonMap("predicates.p1.type", predicateClass))
            .stream().map(Object::toString).collect(Collectors.toSet());
        Set<String> allVersions = allVersionsOff(VersionedPluginBuilder.VersionedTestPlugin.PREDICATE.className());
        Assertions.assertEquals(allVersions.size(), versions.size());
        allVersions.forEach(v -> Assertions.assertTrue(versions.contains(v), "Missing version " + v + " for predicate"));
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void TestConverterPluginRecommender() {
        PluginsRecommenders recommender = new PluginsRecommenders(MULTI_VERSION_PLUGINS);
        Set<String> converters = recommender.converterPluginRecommender().validValues(null, null)
            .stream().map(c -> ((Class) c).getName()).collect(Collectors.toSet());
        Assertions.assertTrue(converters.contains(VersionedPluginBuilder.VersionedTestPlugin.CONVERTER.className()));
        // some sanity checks to ensure that other plugin types are not included
        Assertions.assertFalse(converters.contains(VersionedPluginBuilder.VersionedTestPlugin.SINK_CONNECTOR.className()));
        Assertions.assertFalse(converters.contains(VersionedPluginBuilder.VersionedTestPlugin.SOURCE_CONNECTOR.className()));
        Assertions.assertFalse(converters.contains(VersionedPluginBuilder.VersionedTestPlugin.HEADER_CONVERTER.className()));
        Assertions.assertFalse(converters.contains(VersionedPluginBuilder.VersionedTestPlugin.TRANSFORMATION.className()));
        Assertions.assertFalse(converters.contains(VersionedPluginBuilder.VersionedTestPlugin.PREDICATE.className()));
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void TestHeaderConverterPluginRecommender() {
        PluginsRecommenders recommender = new PluginsRecommenders(MULTI_VERSION_PLUGINS);
        Set<String> headerConverters = recommender.headerConverterPluginRecommender().validValues(null, null)
            .stream().map(c -> ((Class) c).getName()).collect(Collectors.toSet());
        Assertions.assertTrue(headerConverters.contains(VersionedPluginBuilder.VersionedTestPlugin.HEADER_CONVERTER.className()));
        // some sanity checks to ensure that other plugin types are not included
        Assertions.assertFalse(headerConverters.contains(VersionedPluginBuilder.VersionedTestPlugin.SINK_CONNECTOR.className()));
        Assertions.assertFalse(headerConverters.contains(VersionedPluginBuilder.VersionedTestPlugin.SOURCE_CONNECTOR.className()));
        Assertions.assertFalse(headerConverters.contains(VersionedPluginBuilder.VersionedTestPlugin.CONVERTER.className()));
        Assertions.assertFalse(headerConverters.contains(VersionedPluginBuilder.VersionedTestPlugin.TRANSFORMATION.className()));
        Assertions.assertFalse(headerConverters.contains(VersionedPluginBuilder.VersionedTestPlugin.PREDICATE.className()));
    }
}
