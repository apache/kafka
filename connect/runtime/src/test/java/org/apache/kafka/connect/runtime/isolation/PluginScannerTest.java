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

import org.apache.kafka.connect.storage.Converter;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class PluginScannerTest {

    @TempDir
    File pluginDir;

    static Stream<PluginScanner> parameters() {
        return Stream.of(new ReflectionScanner(), new ServiceLoaderScanner());
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testScanningEmptyPluginPath(PluginScanner scanner) {
        PluginScanResult result = scan(scanner, Collections.emptySet());
        assertTrue(result.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testScanningPluginClasses(PluginScanner scanner) {
        PluginScanResult result = scan(scanner, TestPlugins.pluginPath());
        Set<String> classes = new HashSet<>();
        result.forEach(pluginDesc -> classes.add(pluginDesc.className()));
        Set<String> expectedClasses = new HashSet<>(TestPlugins.pluginClasses());
        assertEquals(expectedClasses, classes);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testScanningInvalidUberJar(PluginScanner scanner) throws Exception {
        File newFile = new File(pluginDir, "invalid.jar");
        newFile.createNewFile();
        PluginScanResult result = scan(scanner, Collections.singleton(pluginDir.toPath()));
        assertTrue(result.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testScanningPluginDirContainsInvalidJarsOnly(PluginScanner scanner) throws Exception {
        File newFile = new File(pluginDir, "my-plugin");
        newFile.mkdir();
        newFile = new File(newFile, "invalid.jar");
        newFile.createNewFile();

        PluginScanResult result = scan(scanner, Collections.singleton(pluginDir.toPath()));
        assertTrue(result.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testScanningNoPlugins(PluginScanner scanner) {
        PluginScanResult result = scan(scanner, Collections.singleton(pluginDir.toPath()));
        assertTrue(result.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testScanningPluginDirEmpty(PluginScanner scanner) {
        File newFile = new File(pluginDir, "my-plugin");
        newFile.mkdir();

        PluginScanResult result = scan(scanner, Collections.singleton(pluginDir.toPath()));
        assertTrue(result.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testScanningMixOfValidAndInvalidPlugins(PluginScanner scanner) throws Exception {
        new File(pluginDir, "invalid.jar").createNewFile();
        File newFile = new File(pluginDir, "my-plugin");
        newFile.mkdir();
        newFile = new File(newFile, "invalid.jar");
        newFile.createNewFile();
        Path pluginPath = this.pluginDir.toPath();

        for (Path source : TestPlugins.pluginPath()) {
            Files.copy(source, pluginPath.resolve(source.getFileName()));
        }

        PluginScanResult result = scan(scanner, Collections.singleton(pluginDir.toPath()));
        Set<String> classes = new HashSet<>();
        result.forEach(pluginDesc -> classes.add(pluginDesc.className()));
        Set<String> expectedClasses = new HashSet<>(TestPlugins.pluginClasses());
        assertEquals(expectedClasses, classes);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testNonVersionedPluginHasUndefinedVersion(PluginScanner scanner) {
        PluginScanResult unversionedPluginsResult = scan(scanner,
                TestPlugins.pluginPath(TestPlugins.TestPlugin.SAMPLING_HEADER_CONVERTER));
        assertFalse(unversionedPluginsResult.isEmpty());
        unversionedPluginsResult.forEach(pluginDesc -> assertEquals(PluginDesc.UNDEFINED_VERSION, pluginDesc.version()));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testVersionedPluginsHasVersion(PluginScanner scanner) {
        PluginScanResult versionedPluginResult = scan(scanner,
                TestPlugins.pluginPath(TestPlugins.TestPlugin.READ_VERSION_FROM_RESOURCE_V1));
        assertFalse(versionedPluginResult.isEmpty());
        versionedPluginResult.forEach(pluginDesc -> assertEquals("1.0.0", pluginDesc.version()));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testClasspathPluginIsAlsoLoadedInIsolation(PluginScanner scanner) {
        Set<Path> isolatedClassPathPlugin = TestPlugins.pluginPath(TestPlugins.TestPlugin.CLASSPATH_CONVERTER);
        PluginScanResult result = scan(scanner, isolatedClassPathPlugin);
        Optional<PluginDesc<Converter>> pluginDesc = result.converters().stream()
            .filter(desc -> desc.className().equals(TestPlugins.TestPlugin.CLASSPATH_CONVERTER.className()))
            .findFirst();
        assertTrue(pluginDesc.isPresent());
        assertInstanceOf(PluginClassLoader.class, pluginDesc.get().loader());
    }

    private PluginScanResult scan(PluginScanner scanner, Set<Path> pluginLocations) {
        ClassLoaderFactory factory = new ClassLoaderFactory();
        Set<PluginSource> pluginSources = PluginUtils.pluginSources(pluginLocations, PluginScannerTest.class.getClassLoader(), factory);
        return scanner.discoverPlugins(pluginSources);
    }
}