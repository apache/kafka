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
    public void testJavaLibraryClasses() {
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
    public void testThirdPartyClasses() {
        assertFalse(PluginUtils.shouldLoadInIsolation("org.slf4j."));
        assertFalse(PluginUtils.shouldLoadInIsolation("org.slf4j.LoggerFactory"));
    }

    @Test
    public void testKafkaDependencyClasses() {
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
    public void testConnectApiClasses() {
        List<String> apiClasses = Arrays.asList(
            // Enumerate all packages and classes
            "org.apache.kafka.connect.",
            "org.apache.kafka.connect.components.",
            "org.apache.kafka.connect.components.Versioned",
            //"org.apache.kafka.connect.connector.policy.", isolated by default
            "org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy",
            "org.apache.kafka.connect.connector.policy.ConnectorClientConfigRequest",
            "org.apache.kafka.connect.connector.policy.ConnectorClientConfigRequest$ClientType",
            "org.apache.kafka.connect.connector.",
            "org.apache.kafka.connect.connector.Connector",
            "org.apache.kafka.connect.connector.ConnectorContext",
            "org.apache.kafka.connect.connector.ConnectRecord",
            "org.apache.kafka.connect.connector.Task",
            "org.apache.kafka.connect.data.",
            "org.apache.kafka.connect.data.ConnectSchema",
            "org.apache.kafka.connect.data.Date",
            "org.apache.kafka.connect.data.Decimal",
            "org.apache.kafka.connect.data.Field",
            "org.apache.kafka.connect.data.Schema",
            "org.apache.kafka.connect.data.SchemaAndValue",
            "org.apache.kafka.connect.data.SchemaBuilder",
            "org.apache.kafka.connect.data.SchemaProjector",
            "org.apache.kafka.connect.data.Struct",
            "org.apache.kafka.connect.data.Time",
            "org.apache.kafka.connect.data.Timestamp",
            "org.apache.kafka.connect.data.Values",
            "org.apache.kafka.connect.errors.",
            "org.apache.kafka.connect.errors.AlreadyExistsException",
            "org.apache.kafka.connect.errors.ConnectException",
            "org.apache.kafka.connect.errors.DataException",
            "org.apache.kafka.connect.errors.IllegalWorkerStateException",
            "org.apache.kafka.connect.errors.NotFoundException",
            "org.apache.kafka.connect.errors.RetriableException",
            "org.apache.kafka.connect.errors.SchemaBuilderException",
            "org.apache.kafka.connect.errors.SchemaProjectorException",
            "org.apache.kafka.connect.header.",
            "org.apache.kafka.connect.header.ConnectHeader",
            "org.apache.kafka.connect.header.ConnectHeaders",
            "org.apache.kafka.connect.header.Header",
            "org.apache.kafka.connect.header.Headers",
            "org.apache.kafka.connect.health.",
            "org.apache.kafka.connect.health.AbstractState",
            "org.apache.kafka.connect.health.ConnectClusterDetails",
            "org.apache.kafka.connect.health.ConnectClusterState",
            "org.apache.kafka.connect.health.ConnectorHealth",
            "org.apache.kafka.connect.health.ConnectorState",
            "org.apache.kafka.connect.health.ConnectorType",
            "org.apache.kafka.connect.health.TaskState",
            "org.apache.kafka.connect.rest.",
            "org.apache.kafka.connect.rest.ConnectRestExtension",
            "org.apache.kafka.connect.rest.ConnectRestExtensionContext",
            "org.apache.kafka.connect.sink.",
            "org.apache.kafka.connect.sink.SinkConnector",
            "org.apache.kafka.connect.sink.SinkRecord",
            "org.apache.kafka.connect.sink.SinkTask",
            "org.apache.kafka.connect.sink.SinkTaskContext",
            "org.apache.kafka.connect.sink.ErrantRecordReporter",
            "org.apache.kafka.connect.source.",
            "org.apache.kafka.connect.source.SourceConnector",
            "org.apache.kafka.connect.source.SourceRecord",
            "org.apache.kafka.connect.source.SourceTask",
            "org.apache.kafka.connect.source.SourceTaskContext",
            "org.apache.kafka.connect.storage.",
            "org.apache.kafka.connect.storage.Converter",
            "org.apache.kafka.connect.storage.ConverterConfig",
            "org.apache.kafka.connect.storage.ConverterType",
            "org.apache.kafka.connect.storage.HeaderConverter",
            "org.apache.kafka.connect.storage.OffsetStorageReader",
            //"org.apache.kafka.connect.storage.SimpleHeaderConverter", explicitly isolated
            //"org.apache.kafka.connect.storage.StringConverter", explicitly isolated
            "org.apache.kafka.connect.storage.StringConverterConfig",
            //"org.apache.kafka.connect.transforms.", isolated by default
            "org.apache.kafka.connect.transforms.Transformation",
            "org.apache.kafka.connect.transforms.predicates.Predicate",
            "org.apache.kafka.connect.util.",
            "org.apache.kafka.connect.util.ConnectorUtils"
        );
        // Classes in the API should never be loaded in isolation.
        for (String clazz : apiClasses) {
            assertFalse(
                clazz + " from 'api' is loaded in isolation but should not be",
                PluginUtils.shouldLoadInIsolation(clazz)
            );
        }
    }

    @Test
    public void testConnectRuntimeClasses() {
        // Only list packages, because there are too many classes.
        List<String> runtimeClasses = Arrays.asList(
            "org.apache.kafka.connect.cli.",
            //"org.apache.kafka.connect.connector.policy.", isolated by default
            //"org.apache.kafka.connect.converters.", isolated by default
            "org.apache.kafka.connect.runtime.",
            "org.apache.kafka.connect.runtime.distributed",
            "org.apache.kafka.connect.runtime.errors",
            "org.apache.kafka.connect.runtime.health",
            "org.apache.kafka.connect.runtime.isolation",
            "org.apache.kafka.connect.runtime.rest.",
            "org.apache.kafka.connect.runtime.rest.entities.",
            "org.apache.kafka.connect.runtime.rest.errors.",
            "org.apache.kafka.connect.runtime.rest.resources.",
            "org.apache.kafka.connect.runtime.rest.util.",
            "org.apache.kafka.connect.runtime.standalone.",
            "org.apache.kafka.connect.runtime.rest.",
            "org.apache.kafka.connect.storage.",
            "org.apache.kafka.connect.tools.",
            "org.apache.kafka.connect.util."
        );
        for (String clazz : runtimeClasses) {
            assertFalse(
                clazz + " from 'runtime' is loaded in isolation but should not be",
                PluginUtils.shouldLoadInIsolation(clazz)
            );
        }
    }

    @Test
    public void testAllowedRuntimeClasses() {
        List<String> jsonConverterClasses = Arrays.asList(
            "org.apache.kafka.connect.connector.policy.",
            "org.apache.kafka.connect.connector.policy.AbstractConnectorClientConfigOverridePolicy",
            "org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy",
            "org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy",
            "org.apache.kafka.connect.connector.policy.PrincipalConnectorClientConfigOverridePolicy",
            "org.apache.kafka.connect.converters.",
            "org.apache.kafka.connect.converters.ByteArrayConverter",
            "org.apache.kafka.connect.converters.DoubleConverter",
            "org.apache.kafka.connect.converters.FloatConverter",
            "org.apache.kafka.connect.converters.IntegerConverter",
            "org.apache.kafka.connect.converters.LongConverter",
            "org.apache.kafka.connect.converters.NumberConverter",
            "org.apache.kafka.connect.converters.NumberConverterConfig",
            "org.apache.kafka.connect.converters.ShortConverter",
            //"org.apache.kafka.connect.storage.", not isolated by default
            "org.apache.kafka.connect.storage.StringConverter",
            "org.apache.kafka.connect.storage.SimpleHeaderConverter"
        );
        for (String clazz : jsonConverterClasses) {
            assertTrue(
                clazz + " from 'runtime' is not loaded in isolation but should be",
                PluginUtils.shouldLoadInIsolation(clazz)
            );
        }
    }

    @Test
    public void testTransformsClasses() {
        List<String> transformsClasses = Arrays.asList(
            "org.apache.kafka.connect.transforms.",
            "org.apache.kafka.connect.transforms.util.",
            "org.apache.kafka.connect.transforms.util.NonEmptyListValidator",
            "org.apache.kafka.connect.transforms.util.RegexValidator",
            "org.apache.kafka.connect.transforms.util.Requirements",
            "org.apache.kafka.connect.transforms.util.SchemaUtil",
            "org.apache.kafka.connect.transforms.util.SimpleConfig",
            "org.apache.kafka.connect.transforms.Cast",
            "org.apache.kafka.connect.transforms.Cast$Key",
            "org.apache.kafka.connect.transforms.Cast$Value",
            "org.apache.kafka.connect.transforms.ExtractField",
            "org.apache.kafka.connect.transforms.ExtractField$Key",
            "org.apache.kafka.connect.transforms.ExtractField$Value",
            "org.apache.kafka.connect.transforms.Flatten",
            "org.apache.kafka.connect.transforms.Flatten$Key",
            "org.apache.kafka.connect.transforms.Flatten$Value",
            "org.apache.kafka.connect.transforms.HoistField",
            "org.apache.kafka.connect.transforms.HoistField$Key",
            "org.apache.kafka.connect.transforms.HoistField$Key",
            "org.apache.kafka.connect.transforms.InsertField",
            "org.apache.kafka.connect.transforms.InsertField$Key",
            "org.apache.kafka.connect.transforms.InsertField$Value",
            "org.apache.kafka.connect.transforms.MaskField",
            "org.apache.kafka.connect.transforms.MaskField$Key",
            "org.apache.kafka.connect.transforms.MaskField$Value",
            "org.apache.kafka.connect.transforms.RegexRouter",
            "org.apache.kafka.connect.transforms.ReplaceField",
            "org.apache.kafka.connect.transforms.ReplaceField$Key",
            "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "org.apache.kafka.connect.transforms.SetSchemaMetadata",
            "org.apache.kafka.connect.transforms.SetSchemaMetadata$Key",
            "org.apache.kafka.connect.transforms.SetSchemaMetadata$Value",
            "org.apache.kafka.connect.transforms.TimestampConverter",
            "org.apache.kafka.connect.transforms.TimestampConverter$Key",
            "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "org.apache.kafka.connect.transforms.TimestampRouter",
            "org.apache.kafka.connect.transforms.TimestampRouter$Key",
            "org.apache.kafka.connect.transforms.TimestampRouter$Value",
            "org.apache.kafka.connect.transforms.ValueToKey",
            "org.apache.kafka.connect.transforms.predicates.",
            "org.apache.kafka.connect.transforms.predicates.HasHeaderKey",
            "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone",
            "org.apache.kafka.connect.transforms.predicates.TopicNameMatches"
        );
        for (String clazz : transformsClasses) {
            assertTrue(
                clazz + " from 'transforms' is not loaded in isolation but should be",
                PluginUtils.shouldLoadInIsolation(clazz)
            );
        }
    }

    @Test
    public void testAllowedJsonConverterClasses() {
        List<String> jsonConverterClasses = Arrays.asList(
            "org.apache.kafka.connect.json.",
            "org.apache.kafka.connect.json.DecimalFormat",
            "org.apache.kafka.connect.json.JsonConverter",
            "org.apache.kafka.connect.json.JsonConverterConfig",
            "org.apache.kafka.connect.json.JsonDeserializer",
            "org.apache.kafka.connect.json.JsonSchema",
            "org.apache.kafka.connect.json.JsonSerializer"
        );
        for (String clazz : jsonConverterClasses) {
            assertTrue(
                clazz + " from 'json' is not loaded in isolation but should be",
                PluginUtils.shouldLoadInIsolation(clazz)
            );
        }
    }

    @Test
    public void testAllowedFileConnectors() {
        List<String> jsonConverterClasses = Arrays.asList(
            "org.apache.kafka.connect.file.",
            "org.apache.kafka.connect.file.FileStreamSinkConnector",
            "org.apache.kafka.connect.file.FileStreamSinkTask",
            "org.apache.kafka.connect.file.FileStreamSourceConnector",
            "org.apache.kafka.connect.file.FileStreamSourceTask"
        );
        for (String clazz : jsonConverterClasses) {
            assertTrue(
                clazz + " from 'file' is not loaded in isolation but should be",
                PluginUtils.shouldLoadInIsolation(clazz)
            );
        }
    }

    @Test
    public void testAllowedBasicAuthExtensionClasses() {
        List<String> basicAuthExtensionClasses = Arrays.asList(
            "org.apache.kafka.connect.rest.basic.auth.extension.BasicAuthSecurityRestExtension"
            //"org.apache.kafka.connect.rest.basic.auth.extension.JaasBasicAuthFilter", TODO fix?
            //"org.apache.kafka.connect.rest.basic.auth.extension.PropertyFileLoginModule" TODO fix?
        );
        for (String clazz : basicAuthExtensionClasses) {
            assertTrue(
                clazz + " from 'basic-auth-extension' is not loaded in isolation but should be",
                PluginUtils.shouldLoadInIsolation(clazz)
            );
        }
    }

    @Test
    public void testMirrorClasses() {
        assertTrue(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.mirror.MirrorSourceTask")
        );
        assertTrue(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.connect.mirror.MirrorSourceConnector")
        );
    }

    @Test
    public void testClientConfigProvider() {
        assertFalse(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.common.config.provider.ConfigProvider")
        );
        assertTrue(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.common.config.provider.FileConfigProvider")
        );
        assertTrue(PluginUtils.shouldLoadInIsolation(
                "org.apache.kafka.common.config.provider.FutureConfigProvider")
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
