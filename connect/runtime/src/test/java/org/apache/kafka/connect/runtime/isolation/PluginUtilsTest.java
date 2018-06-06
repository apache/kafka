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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PluginUtilsTest {
    @Rule
    public TemporaryFolder rootDir = new TemporaryFolder();
    private Path pluginPath;

    @Before
    public void setUp() throws Exception {
        pluginPath = rootDir.newFolder("plugins").toPath().toRealPath();
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
        assertFalse(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.clients.producer.ProducerConfig")
        );
        assertFalse(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.clients.consumer.ConsumerConfig")
        );
        assertFalse(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.clients.admin.KafkaAdminClient")
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

    @Test
    public void testClientConfigProvider() throws Exception {
        assertTrue(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.common.config.FileConfigProvider")
        );
        assertTrue(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.common.config.FutureConfigProvider")
        );
    }

    @Test
    public void testEmptyPluginUrls() throws Exception {
        assertEquals(Collections.<Path>emptyList(), PluginUtils.pluginUrls(pluginPath));
    }

    @Test
    public void testEmptyStructurePluginUrls() throws Exception {
        createBasicDirectoryLayout();
        assertEquals(Collections.<Path>emptyList(), PluginUtils.pluginUrls(pluginPath));
    }

    @Test
    public void testPluginUrlsWithJars() throws Exception {
        createBasicDirectoryLayout();

        List<Path> expectedUrls = createBasicExpectedUrls();

        assertUrls(expectedUrls, PluginUtils.pluginUrls(pluginPath));
    }

    @Test
    public void testOrderOfPluginUrlsWithJars() throws Exception {
        createBasicDirectoryLayout();
        // Here this method is just used to create the files. The result is not used.
        createBasicExpectedUrls();

        List<Path> actual = PluginUtils.pluginUrls(pluginPath);
        // 'simple-transform.jar' is created first. In many cases, without sorting within the
        // PluginUtils, this jar will be placed before 'another-transform.jar'. However this is
        // not guaranteed because a DirectoryStream does not maintain a certain order in its
        // results. Besides this test case, sorted order in every call to assertUrls below.
        int i = Arrays.toString(actual.toArray()).indexOf("another-transform.jar");
        int j = Arrays.toString(actual.toArray()).indexOf("simple-transform.jar");
        assertTrue(i < j);
    }

    @Test
    public void testPluginUrlsWithZips() throws Exception {
        createBasicDirectoryLayout();

        List<Path> expectedUrls = new ArrayList<>();
        expectedUrls.add(Files.createFile(pluginPath.resolve("connectorA/my-sink.zip")));
        expectedUrls.add(Files.createFile(pluginPath.resolve("connectorB/a-source.zip")));
        expectedUrls.add(Files.createFile(pluginPath.resolve("transformC/simple-transform.zip")));
        expectedUrls.add(Files.createFile(
                pluginPath.resolve("transformC/deps/another-transform.zip"))
        );

        assertUrls(expectedUrls, PluginUtils.pluginUrls(pluginPath));
    }

    @Test
    public void testPluginUrlsWithClasses() throws Exception {
        Files.createDirectories(pluginPath.resolve("org/apache/kafka/converters"));
        Files.createDirectories(pluginPath.resolve("com/mycompany/transforms"));
        Files.createDirectories(pluginPath.resolve("edu/research/connectors"));
        Files.createFile(pluginPath.resolve("org/apache/kafka/converters/README.txt"));
        Files.createFile(pluginPath.resolve("org/apache/kafka/converters/AlienFormat.class"));
        Files.createDirectories(pluginPath.resolve("com/mycompany/transforms/Blackhole.class"));
        Files.createDirectories(pluginPath.resolve("edu/research/connectors/HalSink.class"));

        List<Path> expectedUrls = new ArrayList<>();
        expectedUrls.add(pluginPath);

        assertUrls(expectedUrls, PluginUtils.pluginUrls(pluginPath));
    }

    @Test
    public void testPluginUrlsWithAbsoluteSymlink() throws Exception {
        createBasicDirectoryLayout();

        Path anotherPath = rootDir.newFolder("moreplugins").toPath().toRealPath();
        Files.createDirectories(anotherPath.resolve("connectorB-deps"));
        Files.createSymbolicLink(
                pluginPath.resolve("connectorB/deps/symlink"),
                anotherPath.resolve("connectorB-deps")
        );

        List<Path> expectedUrls = createBasicExpectedUrls();
        expectedUrls.add(Files.createFile(anotherPath.resolve("connectorB-deps/converter.jar")));

        assertUrls(expectedUrls, PluginUtils.pluginUrls(pluginPath));
    }

    @Test
    public void testPluginUrlsWithRelativeSymlinkBackwards() throws Exception {
        createBasicDirectoryLayout();

        Path anotherPath = rootDir.newFolder("moreplugins").toPath().toRealPath();
        Files.createDirectories(anotherPath.resolve("connectorB-deps"));
        Files.createSymbolicLink(
                pluginPath.resolve("connectorB/deps/symlink"),
                Paths.get("../../../moreplugins/connectorB-deps")
        );

        List<Path> expectedUrls = createBasicExpectedUrls();
        expectedUrls.add(Files.createFile(anotherPath.resolve("connectorB-deps/converter.jar")));

        assertUrls(expectedUrls, PluginUtils.pluginUrls(pluginPath));
    }

    @Test
    public void testPluginUrlsWithRelativeSymlinkForwards() throws Exception {
        // Since this test case defines a relative symlink within an already included path, the main
        // assertion of this test is absence of exceptions and correct resolution of paths.
        createBasicDirectoryLayout();
        Files.createDirectories(pluginPath.resolve("connectorB/deps/more"));
        Files.createSymbolicLink(
                pluginPath.resolve("connectorB/deps/symlink"),
                Paths.get("more")
        );

        List<Path> expectedUrls = createBasicExpectedUrls();
        expectedUrls.add(
                Files.createFile(pluginPath.resolve("connectorB/deps/more/converter.jar"))
        );

        assertUrls(expectedUrls, PluginUtils.pluginUrls(pluginPath));
    }

    private void createBasicDirectoryLayout() throws IOException {
        Files.createDirectories(pluginPath.resolve("connectorA"));
        Files.createDirectories(pluginPath.resolve("connectorB/deps"));
        Files.createDirectories(pluginPath.resolve("transformC/deps"));
        Files.createDirectories(pluginPath.resolve("transformC/more-deps"));
        Files.createFile(pluginPath.resolve("transformC/more-deps/README.txt"));
    }

    private List<Path> createBasicExpectedUrls() throws IOException {
        List<Path> expectedUrls = new ArrayList<>();
        expectedUrls.add(Files.createFile(pluginPath.resolve("connectorA/my-sink.jar")));
        expectedUrls.add(Files.createFile(pluginPath.resolve("connectorB/a-source.jar")));
        expectedUrls.add(Files.createFile(pluginPath.resolve("transformC/simple-transform.jar")));
        expectedUrls.add(Files.createFile(
                pluginPath.resolve("transformC/deps/another-transform.jar"))
        );
        return expectedUrls;
    }

    private void assertUrls(List<Path> expected, List<Path> actual) {
        Collections.sort(expected);
        // not sorting 'actual' because it should be returned sorted from withing the PluginUtils.
        assertEquals(expected, actual);
    }
}
