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

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class PluginDescTest {
    private final ClassLoader systemLoader = ClassLoader.getSystemClassLoader();
    private final String regularVersion = "1.0.0";
    private final String newerVersion = "1.0.1";
    private final String snaphotVersion = "1.0.0-SNAPSHOT";
    private final String noVersion = "undefined";
    private PluginClassLoader pluginLoader;

    @Before
    public void setUp() throws Exception {
        // Fairly simple use case, thus no need to create a random directory here yet.
        URL location = Paths.get("/tmp").toUri().toURL();
        // Normally parent will be a DelegatingClassLoader.
        pluginLoader = new PluginClassLoader(location, new URL[0], systemLoader);
    }

    @Test
    public void testRegularPluginDesc() throws Exception {
        PluginDesc<Connector> connectorDesc = new PluginDesc<>(
                Connector.class,
                regularVersion,
                pluginLoader
        );

        assertPluginDesc(connectorDesc, Connector.class, regularVersion, pluginLoader.location());

        PluginDesc<Converter> converterDesc = new PluginDesc<>(
                Converter.class,
                snaphotVersion,
                pluginLoader
        );

        assertPluginDesc(converterDesc, Converter.class, snaphotVersion, pluginLoader.location());

        PluginDesc<Transformation> transformDesc = new PluginDesc<>(
                Transformation.class,
                noVersion,
                pluginLoader
        );

        assertPluginDesc(transformDesc, Transformation.class, noVersion, pluginLoader.location());
    }

    @Test
    public void testPluginDescWithSystemClassLoader() throws Exception {
        String location = "classpath";
        PluginDesc<SinkConnector> connectorDesc = new PluginDesc<>(
                SinkConnector.class,
                regularVersion,
                systemLoader
        );

        assertPluginDesc(connectorDesc, SinkConnector.class, regularVersion, location);

        PluginDesc<Converter> converterDesc = new PluginDesc<>(
                Converter.class,
                snaphotVersion,
                systemLoader
        );

        assertPluginDesc(converterDesc, Converter.class, snaphotVersion, location);

        PluginDesc<Transformation> transformDesc = new PluginDesc<>(
                Transformation.class,
                noVersion,
                systemLoader
        );

        assertPluginDesc(transformDesc, Transformation.class, noVersion, location);
    }

    @Test
    public void testPluginDescWithNullVersion() throws Exception {
        String nullVersion = "null";
        PluginDesc<SourceConnector> connectorDesc = new PluginDesc<>(
                SourceConnector.class,
                null,
                pluginLoader
        );

        assertPluginDesc(
                connectorDesc,
                SourceConnector.class,
                nullVersion,
                pluginLoader.location()
        );

        String location = "classpath";
        PluginDesc<Converter> converterDesc = new PluginDesc<>(
                Converter.class,
                null,
                systemLoader
        );

        assertPluginDesc(converterDesc, Converter.class, nullVersion, location);
    }

    @Test
    public void testPluginDescEquality() throws Exception {
        PluginDesc<Connector> connectorDescPluginPath = new PluginDesc<>(
                Connector.class,
                snaphotVersion,
                pluginLoader
        );

        PluginDesc<Connector> connectorDescClasspath = new PluginDesc<>(
                Connector.class,
                snaphotVersion,
                systemLoader
        );

        assertEquals(connectorDescPluginPath, connectorDescClasspath);
        assertEquals(connectorDescPluginPath.hashCode(), connectorDescClasspath.hashCode());

        PluginDesc<Converter> converterDescPluginPath = new PluginDesc<>(
                Converter.class,
                noVersion,
                pluginLoader
        );

        PluginDesc<Converter> converterDescClasspath = new PluginDesc<>(
                Converter.class,
                noVersion,
                systemLoader
        );

        assertEquals(converterDescPluginPath, converterDescClasspath);
        assertEquals(converterDescPluginPath.hashCode(), converterDescClasspath.hashCode());

        PluginDesc<Transformation> transformDescPluginPath = new PluginDesc<>(
                Transformation.class,
                null,
                pluginLoader
        );

        PluginDesc<Transformation> transformDescClasspath = new PluginDesc<>(
                Transformation.class,
                noVersion,
                pluginLoader
        );

        assertNotEquals(transformDescPluginPath, transformDescClasspath);
    }

    @Test
    public void testPluginDescComparison() throws Exception {
        PluginDesc<Connector> connectorDescPluginPath = new PluginDesc<>(
                Connector.class,
                regularVersion,
                pluginLoader
        );

        PluginDesc<Connector> connectorDescClasspath = new PluginDesc<>(
                Connector.class,
                newerVersion,
                systemLoader
        );

        assertNewer(connectorDescPluginPath, connectorDescClasspath);

        PluginDesc<Converter> converterDescPluginPath = new PluginDesc<>(
                Converter.class,
                noVersion,
                pluginLoader
        );

        PluginDesc<Converter> converterDescClasspath = new PluginDesc<>(
                Converter.class,
                snaphotVersion,
                systemLoader
        );

        assertNewer(converterDescPluginPath, converterDescClasspath);

        PluginDesc<Transformation> transformDescPluginPath = new PluginDesc<>(
                Transformation.class,
                null,
                pluginLoader
        );

        PluginDesc<Transformation> transformDescClasspath = new PluginDesc<>(
                Transformation.class,
                regularVersion,
                systemLoader
        );

        assertNewer(transformDescPluginPath, transformDescClasspath);
    }

    private static <T> void assertPluginDesc(
            PluginDesc<T> desc,
            Class<? extends T> klass,
            String version,
            String location
    ) {
        assertEquals(desc.pluginClass(), klass);
        assertEquals(desc.className(), klass.getName());
        assertEquals(desc.version(), version);
        assertEquals(desc.type(), PluginType.from(klass));
        assertEquals(desc.typeName(), PluginType.from(klass).toString());
        assertEquals(desc.location(), location);
    }

    private static <T> void assertNewer(PluginDesc<T> older, PluginDesc<T> newer) {
        assertTrue(newer + " should be newer than " + older, older.compareTo(newer) < 0);
    }
}
