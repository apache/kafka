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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class PluginScannerTest {

    @Rule
    public TemporaryFolder pluginDir = new TemporaryFolder();

    @Test
    public void testLoadingUnloadedPluginClass() {
        DelegatingClassLoader classLoader = initClassLoader(
                Collections.emptyList()
        );
        for (String pluginClassName : TestPlugins.pluginClasses()) {
            assertThrows(ClassNotFoundException.class, () -> classLoader.loadClass(pluginClassName));
        }
    }

    @Test
    public void testLoadingPluginClass() throws ClassNotFoundException {
        DelegatingClassLoader classLoader = initClassLoader(
                TestPlugins.pluginPath()
        );
        for (String pluginClassName : TestPlugins.pluginClasses()) {
            assertNotNull(classLoader.loadClass(pluginClassName));
            assertNotNull(classLoader.pluginClassLoader(pluginClassName));
        }
    }

    @Test
    public void testLoadingInvalidUberJar() throws Exception {
        pluginDir.newFile("invalid.jar");

        initClassLoader(
                Collections.singletonList(pluginDir.getRoot().toPath().toAbsolutePath())
        );
    }

    @Test
    public void testLoadingPluginDirContainsInvalidJarsOnly() throws Exception {
        pluginDir.newFolder("my-plugin");
        pluginDir.newFile("my-plugin/invalid.jar");

        initClassLoader(
                Collections.singletonList(pluginDir.getRoot().toPath().toAbsolutePath())
        );
    }

    @Test
    public void testLoadingNoPlugins() {
        initClassLoader(
                Collections.singletonList(pluginDir.getRoot().toPath().toAbsolutePath())
        );
    }

    @Test
    public void testLoadingPluginDirEmpty() throws Exception {
        pluginDir.newFolder("my-plugin");

        initClassLoader(
                Collections.singletonList(pluginDir.getRoot().toPath().toAbsolutePath())
        );
    }

    @Test
    public void testLoadingMixOfValidAndInvalidPlugins() throws Exception {
        pluginDir.newFile("invalid.jar");
        pluginDir.newFolder("my-plugin");
        pluginDir.newFile("my-plugin/invalid.jar");
        Path pluginPath = this.pluginDir.getRoot().toPath();

        for (Path source : TestPlugins.pluginPath()) {
            Files.copy(source, pluginPath.resolve(source.getFileName()));
        }

        DelegatingClassLoader classLoader = initClassLoader(
                Collections.singletonList(pluginDir.getRoot().toPath().toAbsolutePath())
        );
        for (String pluginClassName : TestPlugins.pluginClasses()) {
            assertNotNull(classLoader.loadClass(pluginClassName));
            assertNotNull(classLoader.pluginClassLoader(pluginClassName));
        }
    }

    private DelegatingClassLoader initClassLoader(List<Path> pluginLocations) {
        ClassLoaderFactory factory = new ClassLoaderFactory();
        DelegatingClassLoader classLoader = factory.newDelegatingClassLoader(DelegatingClassLoader.class.getClassLoader());
        Set<PluginSource> pluginSources = PluginUtils.pluginSources(pluginLocations, classLoader, factory);
        PluginScanResult scanResult = new ReflectionScanner().discoverPlugins(pluginSources);
        classLoader.installDiscoveredPlugins(scanResult);
        return classLoader;
    }

}
