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

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

@RunWith(Parameterized.class)
public class PluginScannerTest {

    private enum ScannerType { Reflection, ServiceLoader }

    @Rule
    public TemporaryFolder pluginDir = new TemporaryFolder();

    private final PluginScanner scanner;

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        List<Object[]> values = new ArrayList<>();
        for (ScannerType type : ScannerType.values()) {
            values.add(new Object[]{type});
        }
        return values;
    }

    public PluginScannerTest(ScannerType scannerType) {
        switch (scannerType) {
            case Reflection:
                this.scanner = new ReflectionScanner();
                break;
            case ServiceLoader:
                this.scanner = new ServiceLoaderScanner();
                break;
            default:
                throw new IllegalArgumentException("Unknown type " + scannerType);
        }
    }

    @BeforeClass
    public static void setUp() {
        // Work around a circular-dependency in TestPlugins.
        TestPlugins.pluginPath();
    }

    @Test
    public void testScanningEmptyPluginPath() {
        PluginScanResult result = scan(
                Collections.emptySet()
        );
        assertTrue(result.isEmpty());
    }

    @Test
    public void testScanningPluginClasses() {
        PluginScanResult result = scan(
                TestPlugins.pluginPath()
        );
        Set<String> classes = new HashSet<>();
        result.forEach(pluginDesc -> classes.add(pluginDesc.className()));
        Set<String> expectedClasses = new HashSet<>(TestPlugins.pluginClasses());
        assertEquals(expectedClasses, classes);
    }

    @Test
    public void testScanningInvalidUberJar() throws Exception {
        pluginDir.newFile("invalid.jar");

        PluginScanResult result = scan(
                Collections.singleton(pluginDir.getRoot().toPath().toAbsolutePath())
        );
        assertTrue(result.isEmpty());
    }

    @Test
    public void testScanningPluginDirContainsInvalidJarsOnly() throws Exception {
        pluginDir.newFolder("my-plugin");
        pluginDir.newFile("my-plugin/invalid.jar");

        PluginScanResult result = scan(
                Collections.singleton(pluginDir.getRoot().toPath().toAbsolutePath())
        );
        assertTrue(result.isEmpty());
    }

    @Test
    public void testScanningNoPlugins() {
        PluginScanResult result = scan(
                Collections.singleton(pluginDir.getRoot().toPath().toAbsolutePath())
        );
        assertTrue(result.isEmpty());
    }

    @Test
    public void testScanningPluginDirEmpty() throws Exception {
        pluginDir.newFolder("my-plugin");

        PluginScanResult result = scan(
                Collections.singleton(pluginDir.getRoot().toPath().toAbsolutePath())
        );
        assertTrue(result.isEmpty());
    }

    @Test
    public void testScanningMixOfValidAndInvalidPlugins() throws Exception {
        pluginDir.newFile("invalid.jar");
        pluginDir.newFolder("my-plugin");
        pluginDir.newFile("my-plugin/invalid.jar");
        Path pluginPath = this.pluginDir.getRoot().toPath();

        for (Path source : TestPlugins.pluginPath()) {
            Files.copy(source, pluginPath.resolve(source.getFileName()));
        }

        PluginScanResult result = scan(
                Collections.singleton(pluginDir.getRoot().toPath().toAbsolutePath())
        );
        Set<String> classes = new HashSet<>();
        result.forEach(pluginDesc -> classes.add(pluginDesc.className()));
        Set<String> expectedClasses = new HashSet<>(TestPlugins.pluginClasses());
        assertEquals(expectedClasses, classes);
    }

    @Test
    public void testNonVersionedPluginHasUndefinedVersion() {
        PluginScanResult unversionedPluginsResult = scan(TestPlugins.pluginPath(TestPlugins.TestPlugin.SAMPLING_HEADER_CONVERTER));
        assertFalse(unversionedPluginsResult.isEmpty());
        unversionedPluginsResult.forEach(pluginDesc -> assertEquals(PluginDesc.UNDEFINED_VERSION, pluginDesc.version()));
    }

    @Test
    public void testVersionedPluginsHasVersion() {
        PluginScanResult versionedPluginResult = scan(TestPlugins.pluginPath(TestPlugins.TestPlugin.READ_VERSION_FROM_RESOURCE_V1));
        assertFalse(versionedPluginResult.isEmpty());
        versionedPluginResult.forEach(pluginDesc -> assertEquals("1.0.0", pluginDesc.version()));

    }

    private PluginScanResult scan(Set<Path> pluginLocations) {
        ClassLoaderFactory factory = new ClassLoaderFactory();
        Set<PluginSource> pluginSources = PluginUtils.pluginSources(pluginLocations, PluginScannerTest.class.getClassLoader(), factory);
        return scanner.discoverPlugins(pluginSources);
    }

}
