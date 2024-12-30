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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.apache.maven.artifact.versioning.VersionRange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class MultiVersionTest {

    private static Plugins setUpPlugins(Map<Path, List<VersionedPluginBuilder.BuildInfo>> artifacts, PluginDiscoveryMode mode) {
        String pluginPath = artifacts.keySet().stream().map(Path::toString).collect(Collectors.joining(","));
        Map<String, String> configs = new HashMap<>();
        configs.put(WorkerConfig.PLUGIN_PATH_CONFIG, pluginPath);
        configs.put(WorkerConfig.PLUGIN_DISCOVERY_CONFIG, mode.name());
        return new Plugins(configs);
    }

    private void assertPluginLoad(Map<Path, List<VersionedPluginBuilder.BuildInfo>> artifacts, PluginDiscoveryMode mode)
            throws InvalidVersionSpecificationException, ClassNotFoundException {

        Plugins plugins = setUpPlugins(artifacts, mode);

        for (Map.Entry<Path, List<VersionedPluginBuilder.BuildInfo>> entry : artifacts.entrySet()) {
            String pluginLocation = entry.getKey().toAbsolutePath().toString();

            for (VersionedPluginBuilder.BuildInfo buildInfo : entry.getValue()) {
                ClassLoader pluginLoader = plugins.pluginLoader(buildInfo.plugin().className(), PluginUtils.connectorVersionRequirement(buildInfo.version()));
                Assertions.assertInstanceOf(PluginClassLoader.class, pluginLoader);
                Assertions.assertTrue(((PluginClassLoader) pluginLoader).location().contains(pluginLocation));
                Object p = plugins.newPlugin(buildInfo.plugin().className(), PluginUtils.connectorVersionRequirement(buildInfo.version()));
                Assertions.assertInstanceOf(Versioned.class, p);
                Assertions.assertEquals(buildInfo.version(), ((Versioned) p).version());
            }
        }
    }

    private static final PluginType[] ALL_PLUGIN_TYPES = new PluginType[]{
        PluginType.SINK, PluginType.SOURCE, PluginType.CONVERTER,
        PluginType.HEADER_CONVERTER, PluginType.TRANSFORMATION, PluginType.PREDICATE
    };

    private void assertCorrectLatestPluginVersion(
            Map<Path, List<VersionedPluginBuilder.BuildInfo>> artifacts,
            PluginDiscoveryMode mode,
            String latestVersion
    ) {
        Plugins plugins = setUpPlugins(artifacts, mode);
        List<String> classes = artifacts.values().stream()
                .flatMap(List::stream)
                .map(VersionedPluginBuilder.BuildInfo::plugin)
                .map(VersionedPluginBuilder.VersionedTestPlugin::className)
                .distinct()
                .toList();
        for (String className : classes) {
            String version = plugins.latestVersion(className, ALL_PLUGIN_TYPES);
            Assertions.assertEquals(latestVersion, version);
        }
    }

    private static Map<Path, List<VersionedPluginBuilder.BuildInfo>> buildIsolatedArtifacts(
            String[] versions,
            VersionedPluginBuilder.VersionedTestPlugin[] pluginTypes
    ) throws IOException {
        Map<Path, List<VersionedPluginBuilder.BuildInfo>> artifacts = new HashMap<>();
        for (String v : versions) {
            for (VersionedPluginBuilder.VersionedTestPlugin pluginType: pluginTypes) {
                VersionedPluginBuilder builder = new VersionedPluginBuilder();
                builder.include(pluginType, v);
                artifacts.put(builder.build(pluginType + "-" + v), builder.buildInfos());
            }
        }
        return artifacts;
    }

    public static final String DEFAULT_ISOLATED_ARTIFACTS_LATEST_VERSION;
    public static final Set<String> DEFAULT_ISOLATED_ARTIFACTS_VERSIONS;
    public static final Map<Path, List<VersionedPluginBuilder.BuildInfo>> DEFAULT_ISOLATED_ARTIFACTS;
    public static final Map<Path, List<VersionedPluginBuilder.BuildInfo>> DEFAULT_COMBINED_ARTIFACT;
    public static final Plugins MULTI_VERSION_PLUGINS;
    public static final Map<VersionedPluginBuilder.VersionedTestPlugin, String> DEFAULT_COMBINED_ARTIFACT_VERSIONS;

    static {

        String[] defaultIsolatedArtifactsVersions = new String[]{"1.1.0", "2.3.0", "4.3.0"};
        DEFAULT_ISOLATED_ARTIFACTS_VERSIONS = new HashSet<>(Arrays.asList(defaultIsolatedArtifactsVersions));
        try {
            DEFAULT_ISOLATED_ARTIFACTS = buildIsolatedArtifacts(
                defaultIsolatedArtifactsVersions, VersionedPluginBuilder.VersionedTestPlugin.values()
            );
            DEFAULT_ISOLATED_ARTIFACTS_LATEST_VERSION = "4.3.0";
            DEFAULT_COMBINED_ARTIFACT_VERSIONS = new HashMap<>();

            VersionedPluginBuilder builder = new VersionedPluginBuilder();
            builder.include(VersionedPluginBuilder.VersionedTestPlugin.SOURCE_CONNECTOR,
                DEFAULT_COMBINED_ARTIFACT_VERSIONS.computeIfAbsent(VersionedPluginBuilder.VersionedTestPlugin.SOURCE_CONNECTOR, k -> "0.0.0"));
            builder.include(VersionedPluginBuilder.VersionedTestPlugin.SINK_CONNECTOR,
                DEFAULT_COMBINED_ARTIFACT_VERSIONS.computeIfAbsent(VersionedPluginBuilder.VersionedTestPlugin.SINK_CONNECTOR, k -> "0.1.0"));
            builder.include(VersionedPluginBuilder.VersionedTestPlugin.CONVERTER,
                DEFAULT_COMBINED_ARTIFACT_VERSIONS.computeIfAbsent(VersionedPluginBuilder.VersionedTestPlugin.CONVERTER, k -> "0.2.0"));
            builder.include(VersionedPluginBuilder.VersionedTestPlugin.HEADER_CONVERTER,
                DEFAULT_COMBINED_ARTIFACT_VERSIONS.computeIfAbsent(VersionedPluginBuilder.VersionedTestPlugin.HEADER_CONVERTER, k -> "0.3.0"));
            builder.include(VersionedPluginBuilder.VersionedTestPlugin.TRANSFORMATION,
                DEFAULT_COMBINED_ARTIFACT_VERSIONS.computeIfAbsent(VersionedPluginBuilder.VersionedTestPlugin.TRANSFORMATION, k -> "0.4.0"));
            builder.include(VersionedPluginBuilder.VersionedTestPlugin.PREDICATE,
                DEFAULT_COMBINED_ARTIFACT_VERSIONS.computeIfAbsent(VersionedPluginBuilder.VersionedTestPlugin.PREDICATE, k -> "0.5.0"));
            DEFAULT_COMBINED_ARTIFACT = Collections.singletonMap(builder.build("all_versioned_artifact"), builder.buildInfos());

            Map<Path, List<VersionedPluginBuilder.BuildInfo>> artifacts = new HashMap<>();
            artifacts.putAll(DEFAULT_COMBINED_ARTIFACT);
            artifacts.putAll(DEFAULT_ISOLATED_ARTIFACTS);
            MULTI_VERSION_PLUGINS = setUpPlugins(artifacts, PluginDiscoveryMode.SERVICE_LOAD);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestVersionedPluginLoaded() throws InvalidVersionSpecificationException, ClassNotFoundException {
        assertPluginLoad(DEFAULT_COMBINED_ARTIFACT, PluginDiscoveryMode.SERVICE_LOAD);
        assertPluginLoad(DEFAULT_COMBINED_ARTIFACT, PluginDiscoveryMode.ONLY_SCAN);
    }

    @Test
    public void TestMultipleIsolatedVersionedPluginLoading() throws InvalidVersionSpecificationException, ClassNotFoundException {
        assertPluginLoad(DEFAULT_ISOLATED_ARTIFACTS, PluginDiscoveryMode.SERVICE_LOAD);
        assertPluginLoad(DEFAULT_ISOLATED_ARTIFACTS, PluginDiscoveryMode.ONLY_SCAN);
    }

    @Test
    public void TestLatestVersion() {
        assertCorrectLatestPluginVersion(DEFAULT_ISOLATED_ARTIFACTS, PluginDiscoveryMode.SERVICE_LOAD, DEFAULT_ISOLATED_ARTIFACTS_LATEST_VERSION);
        assertCorrectLatestPluginVersion(DEFAULT_ISOLATED_ARTIFACTS, PluginDiscoveryMode.ONLY_SCAN, DEFAULT_ISOLATED_ARTIFACTS_LATEST_VERSION);
    }

    @Test
    public void TestBundledPluginLoading() throws InvalidVersionSpecificationException, ClassNotFoundException {

        Plugins plugins = MULTI_VERSION_PLUGINS;
        // get the connector loader of the combined artifact which includes all plugin types
        ClassLoader connectorLoader = plugins.pluginLoader(
            VersionedPluginBuilder.VersionedTestPlugin.SINK_CONNECTOR.className(),
            PluginUtils.connectorVersionRequirement("0.1.0")
        );
        Assertions.assertInstanceOf(PluginClassLoader.class, connectorLoader);

        List<VersionedPluginBuilder.VersionedTestPlugin> pluginTypes = List.of(
            VersionedPluginBuilder.VersionedTestPlugin.CONVERTER,
            VersionedPluginBuilder.VersionedTestPlugin.HEADER_CONVERTER,
            VersionedPluginBuilder.VersionedTestPlugin.TRANSFORMATION,
            VersionedPluginBuilder.VersionedTestPlugin.PREDICATE
        );
        // should match the version used in setUp for creating the combined artifact
        List<String> versions = Arrays.asList("0.2.0", "0.3.0", "0.4.0", "0.5.0");
        for (int i = 0; i < 4; i++) {
            String className = pluginTypes.get(i).className();
            // when using the connector loader, the version and plugin returned should be from the ones in the combined artifact
            String version = plugins.pluginVersion(className, connectorLoader, ALL_PLUGIN_TYPES);
            Assertions.assertEquals(versions.get(i), version);
            Object p = plugins.newPlugin(className, null, connectorLoader);
            Assertions.assertInstanceOf(Versioned.class, p);
            Assertions.assertEquals(versions.get(i), ((Versioned) p).version());

            String latestVersion = plugins.latestVersion(className, ALL_PLUGIN_TYPES);
            Assertions.assertEquals(DEFAULT_ISOLATED_ARTIFACTS_LATEST_VERSION, latestVersion);
        }
    }

    @Test
    public void TestCorrectVersionRange() throws IOException, InvalidVersionSpecificationException, ClassNotFoundException {
        Map<Path, List<VersionedPluginBuilder.BuildInfo>> artifacts = buildIsolatedArtifacts(
            new String[]{"1.0.0", "1.1.0", "1.1.2", "2.0.0", "2.0.2", "3.0.0", "4.0.0"},
            VersionedPluginBuilder.VersionedTestPlugin.values()
        );

        Plugins plugins = setUpPlugins(artifacts, PluginDiscoveryMode.SERVICE_LOAD);
        Map<VersionRange, String> requiredVersions = new HashMap<>();
        requiredVersions.put(PluginUtils.connectorVersionRequirement("latest"), "4.0.0");
        requiredVersions.put(PluginUtils.connectorVersionRequirement(null), "4.0.0");
        requiredVersions.put(PluginUtils.connectorVersionRequirement("1.0.0"), "1.0.0");
        requiredVersions.put(PluginUtils.connectorVersionRequirement("[2.0.2]"), "2.0.2");
        requiredVersions.put(PluginUtils.connectorVersionRequirement("[1.1.0,3.0.1]"), "3.0.0");
        requiredVersions.put(PluginUtils.connectorVersionRequirement("(,2.0.0)"), "1.1.2");
        requiredVersions.put(PluginUtils.connectorVersionRequirement("(,1.0.0]"), "1.0.0");
        requiredVersions.put(PluginUtils.connectorVersionRequirement("[2.0.0,)"), "4.0.0");
        requiredVersions.put(PluginUtils.connectorVersionRequirement("(,2.0.0],[2.0.3, 2.0.4)"), "2.0.0");
        requiredVersions.put(PluginUtils.connectorVersionRequirement("(2.0.0,3.0.0)"), "2.0.2");
        requiredVersions.put(PluginUtils.connectorVersionRequirement("(,1.1.0),[4.1.1,)"), "1.0.0");
        requiredVersions.put(PluginUtils.connectorVersionRequirement("[1.1.0,1.1.0]"), "1.1.0");
        requiredVersions.put(PluginUtils.connectorVersionRequirement("(,1.1.0),(2.0.0, 2.0.2]"), "2.0.2");
        requiredVersions.put(PluginUtils.connectorVersionRequirement("[1.1.0,1.1.3)"), "1.1.2");

        for (Map.Entry<VersionRange, String> entry : requiredVersions.entrySet()) {
            for (VersionedPluginBuilder.VersionedTestPlugin pluginType: VersionedPluginBuilder.VersionedTestPlugin.values()) {
                Object p = plugins.newPlugin(pluginType.className(), entry.getKey());
                Assertions.assertInstanceOf(Versioned.class, p);
                Assertions.assertEquals(entry.getValue(), ((Versioned) p).version(),
                    String.format("Provided Version Range %s for class %s should return plugin version %s instead of %s",
                        entry.getKey(), pluginType.className(), entry.getValue(), ((Versioned) p).version()));
            }
        }
    }

    @Test
    public void TestInvalidVersionRange() throws IOException, InvalidVersionSpecificationException {
        String[] validVersions = new String[]{"1.0.0", "1.1.0", "1.1.2", "2.0.0", "2.0.2", "3.0.0", "4.0.0"};
        Map<Path, List<VersionedPluginBuilder.BuildInfo>> artifacts = buildIsolatedArtifacts(
            validVersions,
            VersionedPluginBuilder.VersionedTestPlugin.values()
        );

        Plugins plugins = setUpPlugins(artifacts, PluginDiscoveryMode.SERVICE_LOAD);
        Set<VersionRange> invalidVersions = new HashSet<>();
        invalidVersions.add(PluginUtils.connectorVersionRequirement("0.9.0"));
        invalidVersions.add(PluginUtils.connectorVersionRequirement("[4.0.1,)"));
        invalidVersions.add(PluginUtils.connectorVersionRequirement("(4.0.0,)"));
        invalidVersions.add(PluginUtils.connectorVersionRequirement("[4.0.1]"));
        invalidVersions.add(PluginUtils.connectorVersionRequirement("(2.0.0, 2.0.1)"));
        invalidVersions.add(PluginUtils.connectorVersionRequirement("(,1.0.0)"));
        invalidVersions.add(PluginUtils.connectorVersionRequirement("(1.1.0, 1.1.2)"));
        invalidVersions.add(PluginUtils.connectorVersionRequirement("(1.1.0, 1.1.2),[1.1.3, 2.0.0)"));

        for (VersionRange versionRange : invalidVersions) {
            for (VersionedPluginBuilder.VersionedTestPlugin pluginType: VersionedPluginBuilder.VersionedTestPlugin.values()) {
                VersionedPluginLoadingException e = Assertions.assertThrows(VersionedPluginLoadingException.class, () -> {
                    plugins.newPlugin(pluginType.className(), versionRange);
                }, String.format("Provided Version Range %s for class %s should throw VersionedPluginLoadingException", versionRange, pluginType.className()));
                Assertions.assertEquals(e.availableVersions(), List.of(validVersions));
            }
        }
    }

    @Test
    public void TestVersionedConverter() {
        Plugins plugins = setUpPlugins(DEFAULT_ISOLATED_ARTIFACTS, PluginDiscoveryMode.SERVICE_LOAD);
        Map<String, String> converterConfig = new HashMap<>();
        converterConfig.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, VersionedPluginBuilder.VersionedTestPlugin.CONVERTER.className());
        converterConfig.put(WorkerConfig.KEY_CONVERTER_VERSION, "1.1.0");
        converterConfig.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, VersionedPluginBuilder.VersionedTestPlugin.CONVERTER.className());
        converterConfig.put(WorkerConfig.VALUE_CONVERTER_VERSION, "2.3.0");
        converterConfig.put(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG, VersionedPluginBuilder.VersionedTestPlugin.HEADER_CONVERTER.className());
        converterConfig.put(WorkerConfig.HEADER_CONVERTER_VERSION, "4.3.0");

        AbstractConfig config;
        try (LoaderSwap swap = plugins.safeLoaderSwapper().apply(plugins.delegatingLoader())) {
            config = new PluginsTest.TestableWorkerConfig(converterConfig);
        }

        Converter keyConverter = plugins.newConverter(config, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, WorkerConfig.KEY_CONVERTER_VERSION);
        Assertions.assertEquals(keyConverter.getClass().getName(), VersionedPluginBuilder.VersionedTestPlugin.CONVERTER.className());
        Assertions.assertInstanceOf(Versioned.class, keyConverter);
        Assertions.assertEquals("1.1.0", ((Versioned) keyConverter).version());

        Converter valueConverter = plugins.newConverter(config, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, WorkerConfig.VALUE_CONVERTER_VERSION);
        Assertions.assertEquals(valueConverter.getClass().getName(), VersionedPluginBuilder.VersionedTestPlugin.CONVERTER.className());
        Assertions.assertInstanceOf(Versioned.class, valueConverter);
        Assertions.assertEquals("2.3.0", ((Versioned) valueConverter).version());

        HeaderConverter headerConverter = plugins.newHeaderConverter(config, WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG, WorkerConfig.HEADER_CONVERTER_VERSION);
        Assertions.assertEquals(headerConverter.getClass().getName(), VersionedPluginBuilder.VersionedTestPlugin.HEADER_CONVERTER.className());
        Assertions.assertInstanceOf(Versioned.class, headerConverter);
        Assertions.assertEquals("4.3.0", ((Versioned) headerConverter).version());
    }
}
