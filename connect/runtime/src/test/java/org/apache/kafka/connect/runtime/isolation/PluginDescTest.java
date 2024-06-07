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

import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.config.provider.FileConfigProvider;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PluginDescTest {
    private final ClassLoader systemLoader = ClassLoader.getSystemClassLoader();
    private final String regularVersion = "1.0.0";
    private final String newerVersion = "1.0.1";
    private final String snapshotVersion = "1.0.0-SNAPSHOT";
    private final String noVersion = "undefined";
    private PluginClassLoader pluginLoader;
    private PluginClassLoader otherPluginLoader;

    @Before
    public void setUp() throws Exception {
        // Fairly simple use case, thus no need to create a random directory here yet.
        URL location = Paths.get("/tmp").toUri().toURL();
        URL otherLocation = Paths.get("/tmp-other").toUri().toURL();
        // Normally parent will be a DelegatingClassLoader.
        pluginLoader = new PluginClassLoader(location, new URL[0], systemLoader);
        otherPluginLoader = new PluginClassLoader(otherLocation, new URL[0], systemLoader);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testRegularPluginDesc() {
        PluginDesc<SinkConnector> connectorDesc = new PluginDesc<>(
                SinkConnector.class,
                regularVersion,
                PluginType.SINK,
                pluginLoader
        );

        assertPluginDesc(connectorDesc, SinkConnector.class, regularVersion, PluginType.SINK, pluginLoader.location());

        PluginDesc<Converter> converterDesc = new PluginDesc<>(
                Converter.class,
                snapshotVersion,
                PluginType.CONVERTER,
                pluginLoader
        );

        assertPluginDesc(converterDesc, Converter.class, snapshotVersion, PluginType.CONVERTER, pluginLoader.location());

        PluginDesc<Transformation> transformDesc = new PluginDesc<>(
                Transformation.class,
                noVersion,
                PluginType.TRANSFORMATION,
                pluginLoader
        );

        assertPluginDesc(transformDesc, Transformation.class, noVersion, PluginType.TRANSFORMATION, pluginLoader.location());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testPluginDescWithSystemClassLoader() {
        String location = "classpath";
        PluginDesc<SinkConnector> connectorDesc = new PluginDesc<>(
                SinkConnector.class,
                regularVersion,
                PluginType.SINK,
                systemLoader
        );

        assertPluginDesc(connectorDesc, SinkConnector.class, regularVersion, PluginType.SINK, location);

        PluginDesc<Converter> converterDesc = new PluginDesc<>(
                Converter.class,
                snapshotVersion,
                PluginType.CONVERTER,
                systemLoader
        );

        assertPluginDesc(converterDesc, Converter.class, snapshotVersion, PluginType.CONVERTER, location);

        PluginDesc<Transformation> transformDesc = new PluginDesc<>(
                Transformation.class,
                noVersion,
                PluginType.TRANSFORMATION,
                systemLoader
        );

        assertPluginDesc(transformDesc, Transformation.class, noVersion, PluginType.TRANSFORMATION, location);
    }

    @Test
    public void testPluginDescWithNullVersion() {
        String nullVersion = "null";
        PluginDesc<SourceConnector> connectorDesc = new PluginDesc<>(
                SourceConnector.class,
                null,
                PluginType.SOURCE,
                pluginLoader
        );

        assertPluginDesc(
                connectorDesc,
                SourceConnector.class,
                nullVersion,
                PluginType.SOURCE,
                pluginLoader.location()
        );

        String location = "classpath";
        PluginDesc<Converter> converterDesc = new PluginDesc<>(
                Converter.class,
                null,
                PluginType.CONVERTER,
                systemLoader
        );

        assertPluginDesc(converterDesc, Converter.class, nullVersion, PluginType.CONVERTER, location);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testPluginDescEquality() {
        PluginDesc<SinkConnector> connectorDescPluginPath = new PluginDesc<>(
                SinkConnector.class,
                snapshotVersion,
                PluginType.SINK,
                pluginLoader
        );

        PluginDesc<SinkConnector> connectorDescClasspath = new PluginDesc<>(
                SinkConnector.class,
                snapshotVersion,
                PluginType.SINK,
                systemLoader
        );

        assertEquals(connectorDescPluginPath, connectorDescClasspath);
        assertEquals(connectorDescPluginPath.hashCode(), connectorDescClasspath.hashCode());

        PluginDesc<Converter> converterDescPluginPath = new PluginDesc<>(
                Converter.class,
                noVersion,
                PluginType.CONVERTER,
                pluginLoader
        );

        PluginDesc<Converter> converterDescClasspath = new PluginDesc<>(
                Converter.class,
                noVersion,
                PluginType.CONVERTER,
                systemLoader
        );

        assertEquals(converterDescPluginPath, converterDescClasspath);
        assertEquals(converterDescPluginPath.hashCode(), converterDescClasspath.hashCode());

        PluginDesc<Transformation> transformDescPluginPath = new PluginDesc<>(
                Transformation.class,
                null,
                PluginType.TRANSFORMATION,
                pluginLoader
        );

        PluginDesc<Transformation> transformDescClasspath = new PluginDesc<>(
                Transformation.class,
                noVersion,
                PluginType.TRANSFORMATION,
                pluginLoader
        );

        assertNotEquals(transformDescPluginPath, transformDescClasspath);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testPluginDescComparison() {
        PluginDesc<SinkConnector> connectorDescPluginPath = new PluginDesc<>(
                SinkConnector.class,
                regularVersion,
                PluginType.SINK,
                pluginLoader
        );

        PluginDesc<SinkConnector> connectorDescClasspath = new PluginDesc<>(
                SinkConnector.class,
                newerVersion,
                PluginType.SINK,
                systemLoader
        );

        assertNewer(connectorDescPluginPath, connectorDescClasspath);

        PluginDesc<Converter> converterDescPluginPath = new PluginDesc<>(
                Converter.class,
                noVersion,
                PluginType.CONVERTER,
                pluginLoader
        );

        PluginDesc<Converter> converterDescClasspath = new PluginDesc<>(
                Converter.class,
                snapshotVersion,
                PluginType.CONVERTER,
                systemLoader
        );

        assertNewer(converterDescPluginPath, converterDescClasspath);

        PluginDesc<Transformation> transformDescPluginPath = new PluginDesc<>(
                Transformation.class,
                null,
                PluginType.TRANSFORMATION,
                pluginLoader
        );

        PluginDesc<Transformation> transformDescClasspath = new PluginDesc<>(
                Transformation.class,
                regularVersion,
                PluginType.TRANSFORMATION,
                systemLoader
        );

        assertNewer(transformDescPluginPath, transformDescClasspath);

        PluginDesc<Predicate> predicateDescPluginPath = new PluginDesc<>(
                Predicate.class,
                regularVersion,
                PluginType.PREDICATE,
                pluginLoader
        );

        PluginDesc<Predicate> predicateDescClasspath = new PluginDesc<>(
                Predicate.class,
                regularVersion,
                PluginType.PREDICATE,
                systemLoader
        );

        assertNewer(predicateDescPluginPath, predicateDescClasspath);

        PluginDesc<ConfigProvider> configProviderDescPluginPath = new PluginDesc<>(
                FileConfigProvider.class,
                regularVersion,
                PluginType.CONFIGPROVIDER,
                pluginLoader
        );

        PluginDesc<ConfigProvider> configProviderDescOtherPluginLoader = new PluginDesc<>(
                FileConfigProvider.class,
                regularVersion,
                PluginType.CONFIGPROVIDER,
                otherPluginLoader
        );

        assertTrue("Different plugin loaders should have an ordering",
                configProviderDescPluginPath.compareTo(configProviderDescOtherPluginLoader) != 0);


        PluginDesc<Converter> jsonConverterPlugin = new PluginDesc<>(
                JsonConverter.class,
                regularVersion,
                PluginType.CONVERTER,
                systemLoader
        );

        PluginDesc<HeaderConverter> jsonHeaderConverterPlugin = new PluginDesc<>(
                JsonConverter.class,
                regularVersion,
                PluginType.HEADER_CONVERTER,
                systemLoader
        );

        assertNewer(jsonConverterPlugin, jsonHeaderConverterPlugin);
    }

    @Test
    public void testNullArguments() {
        // Null version is acceptable
        PluginDesc<SinkConnector> sink = new PluginDesc<>(SinkConnector.class, null, PluginType.SINK, systemLoader);
        assertEquals("null", sink.version());

        // Direct nulls are not acceptable for other arguments
        assertThrows(NullPointerException.class, () -> new PluginDesc<>(null, regularVersion, PluginType.SINK, systemLoader));
        assertThrows(NullPointerException.class, () -> new PluginDesc<>(SinkConnector.class, regularVersion, null, systemLoader));
        assertThrows(NullPointerException.class, () -> new PluginDesc<>(SinkConnector.class, regularVersion, PluginType.SINK, null));

        // PluginClassLoaders must have non-null locations
        PluginClassLoader nullLocationLoader = mock(PluginClassLoader.class);
        when(nullLocationLoader.location()).thenReturn(null);
        assertThrows(NullPointerException.class, () -> new PluginDesc<>(SinkConnector.class, regularVersion, PluginType.SINK, nullLocationLoader));
    }

    private static <T> void assertPluginDesc(
            PluginDesc<T> desc,
            Class<? extends T> klass,
            String version,
            PluginType type,
            String location
    ) {
        assertEquals(desc.pluginClass(), klass);
        assertEquals(desc.className(), klass.getName());
        assertEquals(desc.version(), version);
        assertEquals(desc.type(), type);
        assertEquals(desc.typeName(), type.toString());
        assertEquals(desc.location(), location);
    }

    private static void assertNewer(PluginDesc<?> older, PluginDesc<?> newer) {
        assertTrue(newer + " should be newer than " + older, older.compareTo(newer) < 0);
    }
}
