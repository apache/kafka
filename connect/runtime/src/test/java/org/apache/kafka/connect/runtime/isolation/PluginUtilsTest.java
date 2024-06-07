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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.tools.MockSinkConnector;
import org.apache.kafka.connect.tools.MockSourceConnector;
import org.apache.kafka.connect.transforms.Transformation;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

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
        assertEquals(Collections.emptyList(), PluginUtils.pluginUrls(pluginPath));
    }

    @Test
    public void testEmptyStructurePluginUrls() throws Exception {
        createBasicDirectoryLayout();
        assertEquals(Collections.emptyList(), PluginUtils.pluginUrls(pluginPath));
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

    @Test
    public void testNonCollidingAliases() {
        SortedSet<PluginDesc<SinkConnector>> sinkConnectors = new TreeSet<>();
        sinkConnectors.add(new PluginDesc<>(MockSinkConnector.class, null, PluginType.SINK, MockSinkConnector.class.getClassLoader()));
        SortedSet<PluginDesc<SourceConnector>> sourceConnectors = new TreeSet<>();
        sourceConnectors.add(new PluginDesc<>(MockSourceConnector.class, null, PluginType.SOURCE, MockSourceConnector.class.getClassLoader()));
        SortedSet<PluginDesc<Converter>> converters = new TreeSet<>();
        converters.add(new PluginDesc<>(CollidingConverter.class, null, PluginType.CONVERTER, CollidingConverter.class.getClassLoader()));
        PluginScanResult result = new PluginScanResult(
                sinkConnectors,
                sourceConnectors,
                converters,
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                Collections.emptySortedSet()
        );
        Map<String, String> aliases = PluginUtils.computeAliases(result);
        Map<String, String> actualAliases = PluginUtils.computeAliases(result);
        Map<String, String> expectedAliases = new HashMap<>();
        expectedAliases.put("MockSinkConnector", MockSinkConnector.class.getName());
        expectedAliases.put("MockSink", MockSinkConnector.class.getName());
        expectedAliases.put("MockSourceConnector", MockSourceConnector.class.getName());
        expectedAliases.put("MockSource", MockSourceConnector.class.getName());
        expectedAliases.put("CollidingConverter", CollidingConverter.class.getName());
        expectedAliases.put("Colliding", CollidingConverter.class.getName());
        assertEquals(expectedAliases, actualAliases);
    }

    @Test
    public void testMultiVersionAlias() {
        SortedSet<PluginDesc<SinkConnector>> sinkConnectors = new TreeSet<>();
        // distinct versions don't cause an alias collision (the class name is the same)
        sinkConnectors.add(new PluginDesc<>(MockSinkConnector.class, null, PluginType.SINK, MockSinkConnector.class.getClassLoader()));
        sinkConnectors.add(new PluginDesc<>(MockSinkConnector.class, "1.0", PluginType.SINK, MockSinkConnector.class.getClassLoader()));
        assertEquals(2, sinkConnectors.size());
        PluginScanResult result = new PluginScanResult(
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
        Map<String, String> actualAliases = PluginUtils.computeAliases(result);
        Map<String, String> expectedAliases = new HashMap<>();
        expectedAliases.put("MockSinkConnector", MockSinkConnector.class.getName());
        expectedAliases.put("MockSink", MockSinkConnector.class.getName());
        assertEquals(expectedAliases, actualAliases);
    }

    @Test
    public void testCollidingPrunedAlias() {
        SortedSet<PluginDesc<Converter>> converters = new TreeSet<>();
        converters.add(new PluginDesc<>(CollidingConverter.class, null, PluginType.CONVERTER, CollidingConverter.class.getClassLoader()));
        SortedSet<PluginDesc<HeaderConverter>> headerConverters = new TreeSet<>();
        headerConverters.add(new PluginDesc<>(CollidingHeaderConverter.class, null, PluginType.HEADER_CONVERTER, CollidingHeaderConverter.class.getClassLoader()));
        PluginScanResult result = new PluginScanResult(
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                converters,
                headerConverters,
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                Collections.emptySortedSet()
        );
        Map<String, String> actualAliases = PluginUtils.computeAliases(result);
        Map<String, String> expectedAliases = new HashMap<>();
        expectedAliases.put("CollidingConverter", CollidingConverter.class.getName());
        expectedAliases.put("CollidingHeaderConverter", CollidingHeaderConverter.class.getName());
        assertEquals(expectedAliases, actualAliases);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCollidingSimpleAlias() {
        SortedSet<PluginDesc<Converter>> converters = new TreeSet<>();
        converters.add(new PluginDesc<>(CollidingConverter.class, null, PluginType.CONVERTER, CollidingConverter.class.getClassLoader()));
        SortedSet<PluginDesc<Transformation<?>>> transformations = new TreeSet<>();
        transformations.add(new PluginDesc<>((Class<? extends Transformation<?>>) (Class<?>) Colliding.class, null, PluginType.TRANSFORMATION, Colliding.class.getClassLoader()));
        PluginScanResult result = new PluginScanResult(
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                converters,
                Collections.emptySortedSet(),
                transformations,
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                Collections.emptySortedSet(),
                Collections.emptySortedSet()
        );
        Map<String, String> actualAliases = PluginUtils.computeAliases(result);
        Map<String, String> expectedAliases = new HashMap<>();
        expectedAliases.put("CollidingConverter", CollidingConverter.class.getName());
        assertEquals(expectedAliases, actualAliases);
    }

    public static class CollidingConverter implements Converter, Versioned {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] fromConnectData(String topic, Schema schema, Object value) {
            return new byte[0];
        }

        @Override
        public SchemaAndValue toConnectData(String topic, byte[] value) {
            return null;
        }

        @Override
        public String version() {
            return "1.0";
        }
    }

    public static class CollidingHeaderConverter implements HeaderConverter, Versioned {

        @Override
        public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
            return null;
        }

        @Override
        public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
            return new byte[0];
        }

        @Override
        public ConfigDef config() {
            return null;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public String version() {
            return "1.0";
        }
    }

    public static class Colliding<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public R apply(R record) {
            return null;
        }

        @Override
        public ConfigDef config() {
            return null;
        }

        @Override
        public void close() {
        }
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
        // not sorting 'actual' because it should be returned sorted from within the PluginUtils.
        assertEquals(expected, actual);
    }
}
