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
import org.apache.kafka.connect.transforms.Cast;
import org.apache.kafka.connect.transforms.Filter;
import org.apache.kafka.connect.transforms.Transformation;

import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.apache.maven.artifact.versioning.VersionRange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
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
    public PluginClassLoader pluginLoader3;
    public PluginClassLoader pluginLoader4;
    public DelegatingClassLoader classLoader;
    public PluginDesc<SinkConnector> connectorPluginDesc;
    public PluginDesc<SinkConnector> connectorPluginDesc2;
    public PluginDesc<Transformation<?>> cast;
    public PluginDesc<Transformation<?>> castV1Loader2;
    public PluginDesc<Transformation<?>> castV1Loader3;
    public PluginDesc<Transformation<?>> castV2;
    public PluginDesc<Transformation<?>> filter;
    public PluginScanResult scanResult;
    public String version1 = "1.0";
    public String version2 = "2.0";
    public VersionRange range1;
    public VersionRange range1And2;
    public VersionRange range2;
    public VersionRange range123;

    // Arbitrary values, their contents is not meaningful.
    public static final String ARBITRARY = "arbitrary";
    public static final Class<?> CONN = Mockito.class;
    public static final Class<?> CAST = mock(Cast.class).getClass();
    public static final Class<?> FILTER = mock(Filter.class).getClass();
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
        range1 = VersionRange.createFromVersionSpec("[" + version1 + "]");
        range1And2 = VersionRange.createFromVersionSpec("[" + version1 + "," + version2 + "]");
        range2 = VersionRange.createFromVersionSpec("[" + version2 + "]");
        range123 = VersionRange.createFromVersionSpec("[123]");
        parent = mock(PluginClassLoader.class);
        pluginLoader = mock(PluginClassLoader.class);
        pluginLoader2 = mock(PluginClassLoader.class);
        pluginLoader3 = mock(PluginClassLoader.class);
        pluginLoader4 = mock(PluginClassLoader.class);
        classLoader = new DelegatingClassLoader(parent);
        SortedSet<PluginDesc<SinkConnector>> sinkConnectors = new TreeSet<>();
        // Lie to the DCL that this arbitrary class is a connector, since all real connector classes we have access to
        // are forced to be non-isolated by PluginUtils.shouldLoadInIsolation.
        when(pluginLoader.location()).thenReturn("some-location");
        when(pluginLoader2.location()).thenReturn("some-location2");
        when(pluginLoader3.location()).thenReturn("some-location3");
        when(pluginLoader4.location()).thenReturn("some-location4");
        connectorPluginDesc = new PluginDesc<>((Class<? extends SinkConnector>) CONN, null, PluginType.SINK, pluginLoader);
        connectorPluginDesc2 = new PluginDesc<>((Class<? extends SinkConnector>) CONN, version1, PluginType.SINK, pluginLoader2);
        assertTrue(PluginUtils.shouldLoadInIsolation(connectorPluginDesc.className()));
        assertTrue(PluginUtils.shouldLoadInIsolation(connectorPluginDesc2.className()));
        sinkConnectors.add(connectorPluginDesc);
        sinkConnectors.add(connectorPluginDesc2);
        SortedSet<PluginDesc<Transformation<?>>> transformations = new TreeSet<>();
        cast = new PluginDesc<>((Class<? extends Transformation<?>>) CAST, null, PluginType.TRANSFORMATION, pluginLoader);
        castV1Loader2 = new PluginDesc<>((Class<? extends Transformation<?>>) CAST, version1, PluginType.TRANSFORMATION, pluginLoader2);
        castV1Loader3 = new PluginDesc<>((Class<? extends Transformation<?>>) CAST, version1, PluginType.TRANSFORMATION, pluginLoader3);
        castV2 = new PluginDesc<>((Class<? extends Transformation<?>>) CAST, version2, PluginType.TRANSFORMATION, pluginLoader4);
        filter = new PluginDesc<>((Class<? extends Transformation<?>>) FILTER, null, PluginType.TRANSFORMATION, pluginLoader4);
        transformations.add(cast);
        transformations.add(castV1Loader2);
        transformations.add(castV1Loader3);
        transformations.add(castV2);
        transformations.add(filter);
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
        when(parent.loadClass(ARBITRARY, false)).thenReturn((Class) CAST);
        assertSame(CAST, classLoader.loadClass(ARBITRARY, false));
    }

    @Test
    public void testEmptyGetResource() {
        when(parent.getResource(ARBITRARY)).thenReturn(ARBITRARY_URL);
        assertSame(ARBITRARY_URL, classLoader.getResource(ARBITRARY));
    }

    @Test
    public void testInitializedConnectorLoader() {
        classLoader.installDiscoveredPlugins(scanResult);
        ClassLoader expectedLoader = scanResult.sinkConnectors().last().loader();
        assertSame(expectedLoader, classLoader.connectorLoader(connectorPluginDesc.className()));
    }

    @Test
    public void testInitializedConnectorLoaderWithVersion() {
        classLoader.installDiscoveredPlugins(scanResult);
        // connector v1 is only in pluginLoader2
        assertSame(pluginLoader2, classLoader.connectorLoader(connectorPluginDesc.className(), range1));

        // connector v123 cannot be found
        assertThrows(VersionedPluginLoadingException.class, () -> classLoader.connectorLoader(cast.className(), range123));
    }

    @Test
    public void testInitializedPluginLoader() {
        classLoader.installDiscoveredPlugins(scanResult);
        // without a loader or version, the last loader that has the plugin is picked
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.prunedName(cast), null, null));
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.simpleName(cast), null, null));
    }

    @Test
    public void testInitializedPluginLoaderWithClassLoader() {
        classLoader.installDiscoveredPlugins(scanResult);
        // when range is not provided return our classloader if it has the plugin
        assertSame(pluginLoader, classLoader.pluginLoader(PluginUtils.prunedName(cast), null, pluginLoader));
        assertSame(pluginLoader, classLoader.pluginLoader(PluginUtils.simpleName(cast), null, pluginLoader));
        assertSame(pluginLoader3, classLoader.pluginLoader(PluginUtils.prunedName(cast), null, pluginLoader3));
        assertSame(pluginLoader3, classLoader.pluginLoader(PluginUtils.simpleName(cast), null, pluginLoader3));
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.prunedName(filter), null, pluginLoader4));
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.simpleName(filter), null, pluginLoader4));

        // when range is not provided return the classloader which has the plugin if it's no in our classloader
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.prunedName(filter), null, pluginLoader));
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.simpleName(filter), null, pluginLoader));
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.prunedName(filter), null, pluginLoader3));
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.simpleName(filter), null, pluginLoader3));

        // when range is not provided return the last classloader which has the plugin if it's no in our classloader
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.prunedName(cast), null, pluginLoader4));
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.simpleName(cast), null, pluginLoader4));
    }

    @Test
    public void testInitializedPluginLoaderWithVersion() {
        classLoader.installDiscoveredPlugins(scanResult);
        // cast v1 is in both pluginLoader2 and pluginLoader3, we prefer the specified loader
        assertSame(pluginLoader2, classLoader.pluginLoader(PluginUtils.prunedName(cast), range1, pluginLoader2));
        assertSame(pluginLoader2, classLoader.pluginLoader(PluginUtils.simpleName(cast), range1, pluginLoader2));
        assertSame(pluginLoader3, classLoader.pluginLoader(PluginUtils.prunedName(cast), range1, pluginLoader3));
        assertSame(pluginLoader3, classLoader.pluginLoader(PluginUtils.simpleName(cast), range1, pluginLoader3));

        // cast v1 is in both pluginLoader2 and pluginLoader3, we prefer the last loader
        assertSame(pluginLoader3, classLoader.pluginLoader(PluginUtils.prunedName(cast), range1, pluginLoader));
        assertSame(pluginLoader3, classLoader.pluginLoader(PluginUtils.simpleName(cast), range1, pluginLoader));
        assertSame(pluginLoader3, classLoader.pluginLoader(PluginUtils.prunedName(cast), range1, pluginLoader4));
        assertSame(pluginLoader3, classLoader.pluginLoader(PluginUtils.simpleName(cast), range1, pluginLoader4));

        // both cast v1 and v2 match the range, we prefer the specified loader
        assertSame(pluginLoader2, classLoader.pluginLoader(PluginUtils.prunedName(cast), range1And2, pluginLoader2));
        assertSame(pluginLoader2, classLoader.pluginLoader(PluginUtils.simpleName(cast), range1And2, pluginLoader2));
        assertSame(pluginLoader3, classLoader.pluginLoader(PluginUtils.prunedName(cast), range1And2, pluginLoader3));
        assertSame(pluginLoader3, classLoader.pluginLoader(PluginUtils.simpleName(cast), range1And2, pluginLoader3));

        // both cast v1 and v2 match the range, we prefer the last loader
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.prunedName(cast), range1And2, pluginLoader));
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.simpleName(cast), range1And2, pluginLoader));
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.prunedName(cast), range1And2, pluginLoader4));
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.simpleName(cast), range1And2, pluginLoader4));

        // cast v2 is only in pluginLoader4
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.prunedName(cast), range2, pluginLoader));
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.simpleName(cast), range2, pluginLoader));
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.prunedName(cast), range2, pluginLoader3));
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.simpleName(cast), range2, pluginLoader3));
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.prunedName(cast), range2, pluginLoader4));
        assertSame(pluginLoader4, classLoader.pluginLoader(PluginUtils.simpleName(cast), range2, pluginLoader4));

        // cast v123 cannot be found
        assertThrows(VersionedPluginLoadingException.class, () -> classLoader.pluginLoader(PluginUtils.prunedName(cast), range123, pluginLoader));
        assertThrows(VersionedPluginLoadingException.class, () -> classLoader.pluginLoader(PluginUtils.simpleName(cast), range123, pluginLoader));
        assertThrows(VersionedPluginLoadingException.class, () -> classLoader.pluginLoader(PluginUtils.prunedName(cast), range123, pluginLoader2));
        assertThrows(VersionedPluginLoadingException.class, () -> classLoader.pluginLoader(PluginUtils.simpleName(cast), range123, pluginLoader2));
        assertThrows(VersionedPluginLoadingException.class, () -> classLoader.pluginLoader(PluginUtils.prunedName(cast), range123, pluginLoader3));
        assertThrows(VersionedPluginLoadingException.class, () -> classLoader.pluginLoader(PluginUtils.simpleName(cast), range123, pluginLoader3));
        assertThrows(VersionedPluginLoadingException.class, () -> classLoader.pluginLoader(PluginUtils.prunedName(cast), range123, pluginLoader4));
        assertThrows(VersionedPluginLoadingException.class, () -> classLoader.pluginLoader(PluginUtils.simpleName(cast), range123, pluginLoader4));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testInitializedLoadClass() throws ClassNotFoundException {
        classLoader.installDiscoveredPlugins(scanResult);
        String className = connectorPluginDesc.className();
        // Use the last loader that has CONN
        when(pluginLoader2.loadClass(className, false)).thenReturn((Class) CONN);
        assertSame(CONN, classLoader.loadClass(className, false));
    }
}
