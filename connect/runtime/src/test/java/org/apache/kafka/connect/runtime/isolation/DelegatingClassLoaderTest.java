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
import org.apache.kafka.connect.transforms.Transformation;

import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.apache.maven.artifact.versioning.VersionRange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class DelegatingClassLoaderTest {

    public PluginClassLoader parent;
    public PluginClassLoader pluginLoader;
    public PluginClassLoader pluginLoader2;
    public DelegatingClassLoader classLoader;
    public PluginDesc<SinkConnector> connectorPluginDesc;
    public PluginDesc<Transformation<?>> transformationPluginDesc;
    public PluginDesc<Transformation<?>> transformationPluginDesc2;
    public PluginDesc<Transformation<?>> transformationPluginDesc3;
    public PluginScanResult scanResult;
    public String version = "1.0";
    public VersionRange range;

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

    @BeforeEach
    @SuppressWarnings({"unchecked"})
    public void setUp() throws InvalidVersionSpecificationException {
        range = VersionRange.createFromVersionSpec("[" + version + "]");
        parent = mock(PluginClassLoader.class);
        pluginLoader = mock(PluginClassLoader.class);
        pluginLoader2 = mock(PluginClassLoader.class);
        classLoader = new DelegatingClassLoader(parent);
        SortedSet<PluginDesc<SinkConnector>> sinkConnectors = new TreeSet<>();
        // Lie to the DCL that this arbitrary class is a connector, since all real connector classes we have access to
        // are forced to be non-isolated by PluginUtils.shouldLoadInIsolation.
        when(pluginLoader.location()).thenReturn("some-location");
        when(pluginLoader2.location()).thenReturn("other-location");
        connectorPluginDesc = new PluginDesc<>((Class<? extends SinkConnector>) ARBITRARY_CLASS, null, PluginType.SINK, pluginLoader);
        assertTrue(PluginUtils.shouldLoadInIsolation(connectorPluginDesc.className()));
        sinkConnectors.add(connectorPluginDesc);
        SortedSet<PluginDesc<Transformation<?>>> transformations = new TreeSet<>();
        transformationPluginDesc = new PluginDesc<>((Class<? extends Transformation<?>>) ARBITRARY_CLASS, null, PluginType.TRANSFORMATION, pluginLoader);
        transformationPluginDesc2 = new PluginDesc<>((Class<? extends Transformation<?>>) ARBITRARY_CLASS, null, PluginType.TRANSFORMATION, pluginLoader2);
        transformationPluginDesc3 = new PluginDesc<>((Class<? extends Transformation<?>>) ARBITRARY_CLASS, version, PluginType.TRANSFORMATION, pluginLoader);
        transformations.add(transformationPluginDesc);
        transformations.add(transformationPluginDesc2);
        transformations.add(transformationPluginDesc3);
        scanResult = new PluginScanResult(
            sinkConnectors,
            new TreeSet<>(),
            new TreeSet<>(),
            new TreeSet<>(),
            transformations,
            new TreeSet<>(),
            new TreeSet<>(),
            new TreeSet<>(),
            new TreeSet<>()
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
        assertSame(pluginLoader, classLoader.connectorLoader(PluginUtils.prunedName(connectorPluginDesc)));
        assertSame(pluginLoader, classLoader.connectorLoader(PluginUtils.simpleName(connectorPluginDesc)));
        assertSame(pluginLoader, classLoader.connectorLoader(connectorPluginDesc.className()));
    }

    @Test
    public void testInitializedPluginLoader() {
        classLoader.installDiscoveredPlugins(scanResult);
        assertSame(pluginLoader, classLoader.pluginLoader(PluginUtils.prunedName(transformationPluginDesc), null, null));
        assertSame(pluginLoader, classLoader.pluginLoader(PluginUtils.simpleName(transformationPluginDesc), null, null));
        assertSame(pluginLoader, classLoader.pluginLoader(connectorPluginDesc.className(), null, null));
    }

    @Test
    public void testInitializedPluginLoaderWithClassLoader() {
        classLoader.installDiscoveredPlugins(scanResult);
        assertSame(pluginLoader, classLoader.pluginLoader(PluginUtils.prunedName(transformationPluginDesc), null, pluginLoader));
        assertSame(pluginLoader, classLoader.pluginLoader(PluginUtils.simpleName(transformationPluginDesc), null, pluginLoader));
        assertSame(pluginLoader, classLoader.pluginLoader(connectorPluginDesc.className(), null, pluginLoader));
        assertSame(pluginLoader2, classLoader.pluginLoader(PluginUtils.prunedName(transformationPluginDesc), null, pluginLoader2));
        assertSame(pluginLoader2, classLoader.pluginLoader(PluginUtils.simpleName(transformationPluginDesc), null, pluginLoader2));
        assertSame(pluginLoader2, classLoader.pluginLoader(connectorPluginDesc.className(), null, pluginLoader2));
    }

    @Test
    public void testInitializedPluginLoaderWithVersion() {
        classLoader.installDiscoveredPlugins(scanResult);
        assertSame(pluginLoader, classLoader.pluginLoader(PluginUtils.prunedName(transformationPluginDesc), range, pluginLoader));
        assertSame(pluginLoader, classLoader.pluginLoader(PluginUtils.simpleName(transformationPluginDesc), range, pluginLoader));
        assertSame(pluginLoader, classLoader.pluginLoader(connectorPluginDesc.className(), range, pluginLoader));

        assertSame(pluginLoader, classLoader.pluginLoader(PluginUtils.prunedName(transformationPluginDesc), range, pluginLoader2));
        assertSame(pluginLoader, classLoader.pluginLoader(PluginUtils.simpleName(transformationPluginDesc), range, pluginLoader2));
        assertSame(pluginLoader, classLoader.pluginLoader(connectorPluginDesc.className(), range, pluginLoader2));

        assertThrows(VersionedPluginLoadingException.class, () -> classLoader.pluginLoader(PluginUtils.prunedName(transformationPluginDesc), VersionRange.createFromVersion("[123]"), pluginLoader));
        assertThrows(VersionedPluginLoadingException.class, () -> classLoader.pluginLoader(PluginUtils.simpleName(transformationPluginDesc), VersionRange.createFromVersion("[123]"), pluginLoader));
        assertThrows(VersionedPluginLoadingException.class, () -> classLoader.pluginLoader(transformationPluginDesc.className(), VersionRange.createFromVersion("[123]"), pluginLoader));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testInitializedLoadClass() throws ClassNotFoundException {
        classLoader.installDiscoveredPlugins(scanResult);
        String className = connectorPluginDesc.className();
        when(pluginLoader.loadClass(className, false)).thenReturn((Class) ARBITRARY_CLASS);
        assertSame(ARBITRARY_CLASS, classLoader.loadClass(className, false));
    }
}
