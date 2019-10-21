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

import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DelegatingClassLoaderTest {

    @Test
    public void testWhiteListedManifestResources() {
        assertTrue(
            DelegatingClassLoader.serviceLoaderManifestForPlugin("META-INF/services/org.apache.kafka.connect.rest.ConnectRestExtension"));
        assertTrue(
            DelegatingClassLoader.serviceLoaderManifestForPlugin("META-INF/services/org.apache.kafka.common.config.provider.ConfigProvider"));
    }

    @Test
    public void testOtherResources() {
        assertFalse(
            DelegatingClassLoader.serviceLoaderManifestForPlugin("META-INF/services/org.apache.kafka.connect.transforms.Transformation"));
        assertFalse(DelegatingClassLoader.serviceLoaderManifestForPlugin("resource/version.properties"));
    }

    @Test(expected = ClassNotFoundException.class)
    public void testLoadingUnloadedPluginClass() throws ClassNotFoundException {
        TestPlugins.assertAvailable();
        DelegatingClassLoader classLoader = new DelegatingClassLoader(Collections.emptyList());
        classLoader.initLoaders();
        for (String pluginClassName : TestPlugins.pluginClasses()) {
            classLoader.loadClass(pluginClassName);
        }
    }

    @Test
    public void testLoadingPluginClass() throws ClassNotFoundException {
        TestPlugins.assertAvailable();
        DelegatingClassLoader classLoader = new DelegatingClassLoader(TestPlugins.pluginPath());
        classLoader.initLoaders();
        for (String pluginClassName : TestPlugins.pluginClasses()) {
            assertNotNull(classLoader.loadClass(pluginClassName));
            assertNotNull(classLoader.pluginClassLoader(pluginClassName));
        }
    }
}
