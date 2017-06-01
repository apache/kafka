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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PluginUtilsTest {
    @Rule
    public TemporaryFolder rootDir = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testJavaLibraryClasses() throws Exception {
        assertFalse(PluginUtils.shouldLoadInIsolation("java."));
        assertFalse(PluginUtils.shouldLoadInIsolation("java.lang.Object"));
        assertFalse(PluginUtils.shouldLoadInIsolation("java.lang.String"));
        assertFalse(PluginUtils.shouldLoadInIsolation("java.util.HashMap$Entry"));
        assertFalse(PluginUtils.shouldLoadInIsolation("java.io.Serializable"));
        assertFalse(PluginUtils.shouldLoadInIsolation("javax.rmi."));
        assertFalse(PluginUtils.shouldLoadInIsolation(
                "javax.management.loading.ClassLoaderRepository")
        );
        assertFalse(PluginUtils.shouldLoadInIsolation("org.omg.CORBA."));
        assertFalse(PluginUtils.shouldLoadInIsolation("org.omg.CORBA.Object"));
        assertFalse(PluginUtils.shouldLoadInIsolation("org.w3c.dom."));
        assertFalse(PluginUtils.shouldLoadInIsolation("org.w3c.dom.traversal.TreeWalker"));
        assertFalse(PluginUtils.shouldLoadInIsolation("org.xml.sax."));
        assertFalse(PluginUtils.shouldLoadInIsolation("org.xml.sax.EntityResolver"));
    }

    @Test
    public void testThirdPartyClasses() throws Exception {
        assertFalse(PluginUtils.shouldLoadInIsolation("org.slf4j."));
        assertFalse(PluginUtils.shouldLoadInIsolation("org.slf4j.LoggerFactory"));
    }

    @Test
    public void testConnectFrameworkClasses() throws Exception {
        assertFalse(PluginUtils.shouldLoadInIsolation("org.apache.kafka.common."));
        assertFalse(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.common.config.AbstractConfig")
        );
        assertFalse(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.common.config.ConfigDef$Type")
        );
        assertFalse(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.common.serialization.Deserializer")
        );
        assertFalse(PluginUtils.shouldLoadInIsolation("org.apache.kafka.connect."));
        assertFalse(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.connector.Connector")
        );
        assertFalse(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.source.SourceConnector")
        );
        assertFalse(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.sink.SinkConnector")
        );
        assertFalse(PluginUtils.shouldLoadInIsolation("org.apache.kafka.connect.connector.Task"));
        assertFalse(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.source.SourceTask")
        );
        assertFalse(PluginUtils.shouldLoadInIsolation("org.apache.kafka.connect.sink.SinkTask"));
        assertFalse(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.transforms.Transformation")
        );
        assertFalse(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.storage.Converter")
        );
        assertFalse(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.storage.OffsetBackingStore")
        );
    }

    @Test
    public void testAllowedConnectFrameworkClasses() throws Exception {
        assertTrue(PluginUtils.shouldLoadInIsolation("org.apache.kafka.connect.transforms."));
        assertTrue(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.transforms.ExtractField")
        );
        assertTrue(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.transforms.ExtractField$Key")
        );
        assertTrue(PluginUtils.shouldLoadInIsolation("org.apache.kafka.connect.json."));
        assertTrue(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.json.JsonConverter")
        );
        assertTrue(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.json.JsonConverter$21")
        );
        assertTrue(PluginUtils.shouldLoadInIsolation("org.apache.kafka.connect.file."));
        assertTrue(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.file.FileStreamSourceTask")
        );
        assertTrue(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.file.FileStreamSinkConnector")
        );
        assertTrue(PluginUtils.shouldLoadInIsolation("org.apache.kafka.connect.converters."));
        assertTrue(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.converters.ByteArrayConverter")
        );
        assertTrue(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.storage.StringConverter")
        );
    }
}
