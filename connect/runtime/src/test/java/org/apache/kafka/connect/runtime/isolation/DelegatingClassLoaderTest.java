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

import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class DelegatingClassLoaderTest {

    public PluginClassLoader parent;
    public PluginClassLoader pluginLoader;
    public DelegatingClassLoader classLoader;
    public PluginDesc<SinkConnector> pluginDesc;
    public PluginScanResult scanResult;

    // Arbitrary values, their contents is not meaningful.
    public static final String ARBITRARY = "arbitrary";
    public static final Class<?> ARBITRARY_CLASS = org.mockito.Mockito.class;
    public static final URL ARBITRARY_URL;

    static {
        try {
            ARBITRARY_URL = new URL("jar:file://" + ARBITRARY + "!/" + ARBITRARY);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    @SuppressWarnings({"unchecked"})
    public void setUp() {
        parent = mock(PluginClassLoader.class);
        pluginLoader = mock(PluginClassLoader.class);
        classLoader = new DelegatingClassLoader(parent);
        SortedSet<PluginDesc<SinkConnector>> sinkConnectors = new TreeSet<>();
        // Lie to the DCL that this arbitrary class is a connector, since all real connector classes we have access to
        // are forced to be non-isolated by PluginUtils.shouldLoadInIsolation.
        when(pluginLoader.location()).thenReturn("some-location");
        pluginDesc = new PluginDesc<>((Class<? extends SinkConnector>) ARBITRARY_CLASS, null, PluginType.SINK, pluginLoader);
        assertTrue(PluginUtils.shouldLoadInIsolation(pluginDesc.className()));
        sinkConnectors.add(pluginDesc);
        scanResult = new PluginScanResult(
                sinkConnectors,
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                Collections.emptySortedSet()
        );
    }

    @Test
    public void testEmptyConnectorLoader() {
        assertSame(classLoader, classLoader.connectorLoader(ARBITRARY));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testEmptyLoadClass() throws ClassNotFoundException {
        when(parent.loadClass(ARBITRARY, false)).thenReturn((Class) ARBITRARY_CLASS);
        assertSame(ARBITRARY_CLASS, classLoader.loadClass(ARBITRARY, false));
    }

    @Test
    public void testEmptyGetResource() {
        when(parent.getResource(ARBITRARY)).thenReturn(ARBITRARY_URL);
        assertSame(ARBITRARY_URL, classLoader.getResource(ARBITRARY));
    }

    @Test
    public void testInitializedConnectorLoader() {
        classLoader.installDiscoveredPlugins(scanResult);
        assertSame(pluginLoader, classLoader.connectorLoader(PluginUtils.prunedName(pluginDesc)));
        assertSame(pluginLoader, classLoader.connectorLoader(PluginUtils.simpleName(pluginDesc)));
        assertSame(pluginLoader, classLoader.connectorLoader(pluginDesc.className()));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testInitializedLoadClass() throws ClassNotFoundException {
        classLoader.installDiscoveredPlugins(scanResult);
        String className = pluginDesc.className();
        when(pluginLoader.loadClass(className, false)).thenReturn((Class) ARBITRARY_CLASS);
        assertSame(ARBITRARY_CLASS, classLoader.loadClass(className, false));
    }
}
