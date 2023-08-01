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
package org.apache.kafka.tools;

import org.apache.kafka.connect.runtime.isolation.ClassLoaderFactory;
import org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader;
import org.apache.kafka.connect.runtime.isolation.PluginScanResult;
import org.apache.kafka.connect.runtime.isolation.PluginSource;
import org.apache.kafka.connect.runtime.isolation.PluginUtils;
import org.apache.kafka.connect.runtime.isolation.ReflectionScanner;
import org.apache.kafka.connect.runtime.isolation.ServiceLoaderScanner;
import org.apache.kafka.connect.runtime.isolation.TestPlugins;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ConnectPluginPathTest {

    private static final Logger log = LoggerFactory.getLogger(ConnectPluginPathTest.class);

    @TempDir
    public Path workspace;

    private enum PluginLocationType {
        CLASS_HIERARCHY,
        SINGLE_JAR,
        MULTI_JAR
    }

    private static class PluginLocation {
        private final Path path;

        private PluginLocation(Path path) {
            this.path = path;
        }

        @Override
        public String toString() {
            return path.toString();
        }
    }

    @BeforeAll
    public static void setUp() {
        // Work around a circular-dependency in TestPlugins.
        TestPlugins.pluginPath();
    }

    /**
     * Populate a writable disk path to be usable as a single plugin location.
     * The returned path will be usable as a single path.
     * @param path A non-existent path immediately within a writable directory, suggesting a location for this plugin.
     * @param type The format to which the on-disk plugin should conform
     * @param plugin The plugin which should be written to the specified path
     * @return The final usable path name to this location, in case it is different from the suggested input path.
     */
    private static PluginLocation setupLocation(Path path, PluginLocationType type, TestPlugins.TestPlugin plugin) {
        try {
            Path jarPath = TestPlugins.pluginPath(plugin).stream().findFirst().get();
            switch (type) {
                case CLASS_HIERARCHY: {
                    try (JarFile jarFile = new JarFile(jarPath.toFile())) {
                        jarFile.stream().forEach(jarEntry -> {
                            Path entryPath = path.resolve(jarEntry.getName());
                            try {
                                entryPath.getParent().toFile().mkdirs();
                                Files.copy(jarFile.getInputStream(jarEntry), entryPath, StandardCopyOption.REPLACE_EXISTING);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        });
                    }
                    return new PluginLocation(path);
                }
                case SINGLE_JAR: {
                    Path outputJar = path.resolveSibling(path.getFileName() + ".jar");
                    outputJar.getParent().toFile().mkdirs();
                    Files.copy(jarPath, outputJar, StandardCopyOption.REPLACE_EXISTING);
                    return new PluginLocation(outputJar);
                }
                case MULTI_JAR: {
                    Path outputJar = path.resolve(jarPath.getFileName());
                    outputJar.getParent().toFile().mkdirs();
                    Files.copy(jarPath, outputJar, StandardCopyOption.REPLACE_EXISTING);
                    return new PluginLocation(path);
                }
                default:
                    throw new IllegalArgumentException("Unknown PluginLocationType");
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class PluginPathElement {
        private final Path root;
        private final List<PluginLocation> locations;

        private PluginPathElement(Path root, List<PluginLocation> locations) {
            this.root = root;
            this.locations = locations;
        }

        @Override
        public String toString() {
            return root.toString();
        }
    }

    /**
     * Populate a writable disk path to be usable as single {@code plugin.path} element providing the specified plugins
     * @param path    A directory that should contain the populated plugins, will be created if it does not exist.
     * @param type    The format to which the on-disk plugins should conform
     * @param plugins The plugins which should be written to the specified path
     * @return The specific inner locations of the plugins that were written.
     */
    private PluginPathElement setupPluginPathElement(Path path, PluginLocationType type, TestPlugins.TestPlugin... plugins) {
        List<PluginLocation> locations = new ArrayList<>();
        for (int i = 0; i < plugins.length; i++) {
            TestPlugins.TestPlugin plugin = plugins[i];
            locations.add(setupLocation(path.resolve("plugin-" + i), type, plugin));
        }
        return new PluginPathElement(path, locations);
    }

    private static class WorkerConfig {
        private final Path configFile;
        private final List<PluginPathElement> pluginPathElements;

        private WorkerConfig(Path configFile, List<PluginPathElement> pluginPathElements) {
            this.configFile = configFile;
            this.pluginPathElements = pluginPathElements;
        }

        @Override
        public String toString() {
            return configFile.toString();
        }
    }

    /**
     * Populate a writable disk path
     * @param path
     * @param pluginPathElements
     * @return
     */
    private WorkerConfig setupWorkerConfig(Path path, PluginPathElement... pluginPathElements) {
        path.getParent().toFile().mkdirs();
        Properties properties = new Properties();
        String pluginPath = Arrays.stream(pluginPathElements)
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        properties.setProperty("plugin.path", pluginPath);
        try (OutputStream outputStream = Files.newOutputStream(path)) {
            properties.store(outputStream, "dummy worker properties file");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new WorkerConfig(path, Arrays.asList(pluginPathElements));
    }

    private static class CommandResult {
        public CommandResult(int returnCode, String out, String err, PluginScanResult reflective, PluginScanResult serviceLoading) {
            this.returnCode = returnCode;
            this.out = out;
            this.err = err;
            this.reflective = reflective;
            this.serviceLoading = serviceLoading;
        }

        int returnCode;
        String out;
        String err;
        PluginScanResult reflective;
        PluginScanResult serviceLoading;
    }

    private CommandResult runCommand(Object... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        try {
            int returnCode = ConnectPluginPath.mainNoExit(
                    Arrays.stream(args)
                            .map(Object::toString)
                            .collect(Collectors.toList())
                            .toArray(new String[]{}),
                    new PrintStream(out, true, "utf-8"),
                    new PrintStream(err, true, "utf-8"));
            Set<Path> pluginLocations = getPluginLocations(args);
            ClassLoader parent = ConnectPluginPath.class.getClassLoader();
            ClassLoaderFactory factory = new ClassLoaderFactory();
            try (DelegatingClassLoader delegatingClassLoader = factory.newDelegatingClassLoader(parent)) {
                Set<PluginSource> sources = PluginUtils.pluginSources(pluginLocations, delegatingClassLoader, factory);
                String stdout = new String(out.toByteArray(), StandardCharsets.UTF_8);
                String stderr = new String(err.toByteArray(), StandardCharsets.UTF_8);
                log.info("STDOUT:\n{}", stdout);
                log.info("STDERR:\n{}", stderr);
                return new CommandResult(
                        returnCode,
                        stdout,
                        stderr,
                        new ReflectionScanner().discoverPlugins(sources),
                        new ServiceLoaderScanner().discoverPlugins(sources)
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private static Set<Path> getPluginLocations(Object[] args) {
        return Arrays.stream(args)
                .flatMap(obj -> {
                    if (obj instanceof WorkerConfig) {
                        return ((WorkerConfig) obj).pluginPathElements.stream();
                    } else {
                        return Stream.of(obj);
                    }
                })
                .flatMap(obj -> {
                    if (obj instanceof PluginPathElement) {
                        return ((PluginPathElement) obj).locations.stream();
                    } else {
                        return Stream.of(obj);
                    }
                })
                .map(obj -> {
                    if (obj instanceof PluginLocation) {
                        return ((PluginLocation) obj).path;
                    } else {
                        return null;
                    }
                })

                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    @Test
    public void testNoArguments() {
        CommandResult res = runCommand();
        assertNotEquals(0, res.returnCode);
    }

    @Test
    public void testListNoArguments() {
        CommandResult res = runCommand(
                "list"
        );
        assertNotEquals(0, res.returnCode);
    }

    @ParameterizedTest
    @EnumSource
    public void testListOneLocation(PluginLocationType type) {
        CommandResult res = runCommand(
                "list",
                "--plugin-location",
                setupLocation(workspace.resolve("location-a"), type, TestPlugins.TestPlugin.NON_MIGRATED_MULTI_PLUGIN)
        );
        assertEquals(0, res.returnCode);
    }

    @ParameterizedTest
    @EnumSource
    public void testListMultipleLocations(PluginLocationType type) {
        CommandResult res = runCommand(
                "list",
                "--plugin-location",
                setupLocation(workspace.resolve("location-a"), type, TestPlugins.TestPlugin.NON_MIGRATED_MULTI_PLUGIN),
                "--plugin-location",
                setupLocation(workspace.resolve("location-b"), type, TestPlugins.TestPlugin.SAMPLING_CONFIGURABLE)
        );
        assertEquals(0, res.returnCode);
    }

    @ParameterizedTest
    @EnumSource
    public void testListOnePluginPath(PluginLocationType type) {
        CommandResult res = runCommand(
                "list",
                "--plugin-path",
                setupPluginPathElement(workspace.resolve("path-a"), type,
                        TestPlugins.TestPlugin.NON_MIGRATED_MULTI_PLUGIN, TestPlugins.TestPlugin.SAMPLING_CONFIGURABLE)
        );
        assertEquals(0, res.returnCode);
    }

    @ParameterizedTest
    @EnumSource
    public void testListMultiplePluginPaths(PluginLocationType type) {
        CommandResult res = runCommand(
                "list",
                "--plugin-path",
                setupPluginPathElement(workspace.resolve("path-a"), type,
                        TestPlugins.TestPlugin.NON_MIGRATED_MULTI_PLUGIN, TestPlugins.TestPlugin.SAMPLING_CONFIGURABLE),
                "--plugin-path",
                setupPluginPathElement(workspace.resolve("path-b"), type,
                        TestPlugins.TestPlugin.SAMPLING_HEADER_CONVERTER, TestPlugins.TestPlugin.ALIASED_STATIC_FIELD)
        );
        assertEquals(0, res.returnCode);
    }

    @ParameterizedTest
    @EnumSource
    public void testListOneWorkerConfig(PluginLocationType type) {
        CommandResult res = runCommand(
                "list",
                "--worker-config",
                setupWorkerConfig(workspace.resolve("worker.properties"),
                        setupPluginPathElement(workspace.resolve("path-a"), type,
                                TestPlugins.TestPlugin.NON_MIGRATED_MULTI_PLUGIN))
        );
        assertEquals(0, res.returnCode);
    }

    @ParameterizedTest
    @EnumSource
    public void testListMultipleWorkerConfigs(PluginLocationType type) {
        CommandResult res = runCommand(
                "list",
                "--worker-config",
                setupWorkerConfig(workspace.resolve("worker-a.properties"),
                        setupPluginPathElement(workspace.resolve("path-a"), type,
                                TestPlugins.TestPlugin.NON_MIGRATED_MULTI_PLUGIN)),
                "--worker-config",
                setupWorkerConfig(workspace.resolve("worker-b.properties"),
                        setupPluginPathElement(workspace.resolve("path-b"), type,
                                TestPlugins.TestPlugin.SERVICE_LOADER))
        );
        assertEquals(0, res.returnCode);
    }
}
